"""Step decorator, Step class, and _FnStep.

Step.run() returns StepResult — never raises. Validation, retry, and timeout
are handled internally. Step authors throw to signal failure; run() catches
and wraps in StepFailure.

_FnStep is used internally by combinators (parallel, choice, map_step, etc.)
to create Runnable objects backed by async functions.
"""

from __future__ import annotations

import asyncio
import builtins
import inspect
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any, Literal, cast

from pydantic import BaseModel, ValidationError

from runsheet._errors import (
    ProvidesValidationError,
    RequiresValidationError,
    RetryExhaustedError,
    TimeoutError,
)
from runsheet._internal import (
    step_failure,
    step_meta,
    step_success,
    to_ctx_dict,
    to_error,
)
from runsheet._result import RollbackCallback, StepResult


async def _call_rollback_fn(fn: RollbackFn, ctx: object, output: object) -> None:
    """Call a rollback function (sync or async)."""
    result = fn(ctx, output)
    if inspect.isawaitable(result):
        await result


@dataclass(frozen=True)
class RetryPolicy:
    count: int
    delay: float = 0.0
    backoff: Literal["linear", "exponential"] = "linear"
    retry_if: Callable[[list[Exception]], bool] | None = None


# Type aliases for step functions
RunFn = Callable[..., Any]
RollbackFn = Callable[..., Coroutine[Any, Any, None] | None]


class Step:
    """A pipeline step with metadata, run function, and optional rollback.

    Step.run() returns StepResult — it never raises. Validation, retry,
    and timeout are handled internally.
    """

    def __init__(
        self,
        *,
        name: str,
        requires: type[BaseModel] | None,
        provides: type[BaseModel] | None,
        run_fn: RunFn,
        retry: RetryPolicy | None = None,
        timeout: float | None = None,
    ) -> None:
        self.name = name
        self.requires = requires
        self.provides = provides
        self.retry = retry
        self.timeout = timeout
        self._run_fn = run_fn
        self._run_is_async = inspect.iscoroutinefunction(run_fn)
        self._rollback_fn: RollbackFn | None = None

    def __repr__(self) -> str:
        parts = [f"name={self.name!r}"]
        if self.requires is not None:
            parts.append(f"requires={self.requires.__name__}")
        if self.provides is not None:
            parts.append(f"provides={self.provides.__name__}")
        return f"Step({', '.join(parts)})"

    async def run(self, ctx: object) -> StepResult[dict[str, Any]]:
        """Execute the step. Never raises — always returns StepResult."""
        step_input: object = ctx  # capture before isinstance narrows
        frozen_args = to_ctx_dict(ctx)
        meta = step_meta(self.name, frozen_args)

        # 1. Validate requires
        if self.requires is not None:
            try:
                step_input = self.requires.model_validate(frozen_args)
            except ValidationError as e:
                return step_failure(
                    RequiresValidationError(
                        f"Step '{self.name}' requires validation failed: {e}"
                    ),
                    meta,
                    self.name,
                )

        # 2. Execute with retry and timeout
        try:
            raw_output = await self._execute_with_retry_and_timeout(step_input)
        except Exception as e:
            return step_failure(to_error(e), meta, self.name)

        # 3. Validate provides and normalize output
        try:
            output = self._validate_provides(raw_output)
        except Exception as e:
            return step_failure(to_error(e), meta, self.name)

        return step_success(output, meta)

    def _validate_provides(self, output: object) -> dict[str, Any]:
        """Validate step output against provides schema."""
        if self.provides is None:
            if isinstance(output, BaseModel):
                return output.model_dump()
            if isinstance(output, dict):
                return cast(dict[str, Any], output)
            return {}

        if not isinstance(output, self.provides):
            raise ProvidesValidationError(
                f"Step '{self.name}' must return {self.provides.__name__}, "
                f"got {type(output).__name__}"
            )
        return output.model_dump()

    async def _call_run_fn(self, step_input: object) -> object:
        """Call the raw run function (sync or async)."""
        if self._run_is_async:
            return await self._run_fn(step_input)
        return self._run_fn(step_input)

    async def _run_once(self, step_input: object) -> object:
        """Run once, with timeout if configured."""
        if self.timeout is not None:
            try:
                async with asyncio.timeout(self.timeout):
                    return await self._call_run_fn(step_input)
            except builtins.TimeoutError as e:
                raise TimeoutError(
                    f"Step '{self.name}' timed out after {self.timeout}s",
                    timeout_seconds=self.timeout,
                ) from e
        return await self._call_run_fn(step_input)

    async def _execute_with_retry_and_timeout(self, step_input: object) -> object:
        """Execute with retry and/or timeout. Raises on failure."""
        if self.retry is None:
            return await self._run_once(step_input)

        errors: list[Exception] = []
        total_attempts = 1 + self.retry.count

        for attempt in range(total_attempts):
            try:
                return await self._run_once(step_input)
            except Exception as e:
                errors.append(e)
                if attempt < total_attempts - 1:
                    retry_if = self.retry.retry_if
                    if retry_if is not None and not retry_if(errors):
                        break
                    if self.retry.delay > 0:
                        if self.retry.backoff == "exponential":
                            delay = self.retry.delay * (2**attempt)
                        else:
                            delay = self.retry.delay
                        await asyncio.sleep(delay)

        last_error = errors[-1] if errors else Exception("Unknown retry failure")
        raise RetryExhaustedError(
            f"Step '{self.name}' failed after {len(errors)} attempt(s)",
            attempts=len(errors),
            last_error=last_error,
        )

    async def run_rollback(self, ctx: object, output: object) -> None:
        """Execute the rollback function if one exists."""
        if self._rollback_fn is None:
            return
        await _call_rollback_fn(self._rollback_fn, ctx, output)

    def make_rollback(
        self, pre_ctx: dict[str, Any], output: dict[str, Any]
    ) -> RollbackCallback | None:
        """Create a rollback callback capturing this execution's state.

        Called by Pipeline immediately after run() succeeds. The returned
        callback is stored and invoked during rollback if a later step fails.
        """
        if self._rollback_fn is None:
            return None
        fn = self._rollback_fn

        async def cb() -> None:
            await _call_rollback_fn(fn, pre_ctx, output)

        return cb

    def rollback(self, fn: RollbackFn) -> RollbackFn:
        """Decorator to attach a rollback handler to this step."""
        self._rollback_fn = fn
        return fn

    @property
    def has_rollback(self) -> bool:
        return self._rollback_fn is not None


# ---------------------------------------------------------------------------
# _FnStep — combinator backing
# ---------------------------------------------------------------------------

# Type for the make_rollback callback that combinators can provide
MakeRollbackFn = Callable[[dict[str, Any], dict[str, Any]], RollbackCallback | None]


class _FnStep:  # pyright: ignore[reportUnusedClass]
    """Runnable backed by async functions. Used by combinators.

    Unlike Step (which validates schemas, retries, times out), _FnStep is a
    thin wrapper that delegates directly to the provided functions. It
    satisfies the Runnable protocol without monkey-patching.
    """

    def __init__(
        self,
        *,
        name: str,
        run_fn: Callable[[object], Any],
        make_rollback_fn: MakeRollbackFn | None = None,
        requires: type[BaseModel] | None = None,
        provides: type[BaseModel] | None = None,
    ) -> None:
        self.name = name
        self.requires = requires
        self.provides = provides
        self._run_fn = run_fn
        self._make_rollback_fn = make_rollback_fn

    def __repr__(self) -> str:
        return f"_FnStep(name={self.name!r})"

    @property
    def has_rollback(self) -> bool:
        return self._make_rollback_fn is not None

    async def run(self, ctx: object) -> StepResult[dict[str, Any]]:
        return await self._run_fn(ctx)  # type: ignore[no-any-return]

    async def run_rollback(self, ctx: object, output: object) -> None:
        pass

    def make_rollback(
        self, pre_ctx: dict[str, Any], output: dict[str, Any]
    ) -> RollbackCallback | None:
        if self._make_rollback_fn is not None:
            return self._make_rollback_fn(pre_ctx, output)
        return None


def step(
    *,
    requires: type[BaseModel] | None = None,
    provides: type[BaseModel] | None = None,
    name: str | None = None,
    retry: RetryPolicy | None = None,
    timeout: float | None = None,
) -> Callable[[RunFn], Step]:
    """Decorator to create a Step from a function."""

    def decorator(fn: RunFn) -> Step:
        step_name = name if name is not None else fn.__name__
        return Step(
            name=step_name,
            requires=requires,
            provides=provides,
            run_fn=fn,
            retry=retry,
            timeout=timeout,
        )

    return decorator
