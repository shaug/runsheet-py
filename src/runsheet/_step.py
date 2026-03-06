"""Step decorator and Step class.

@step(requires=X, provides=Y) wraps an async (or sync) function into a Step
object with metadata for pipeline execution.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel


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

    Two-phase construction: create the step, then optionally attach a
    rollback handler via the .rollback decorator. No immutability theater —
    we're all adults here.
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
        self._rollback_is_async = False

    def __repr__(self) -> str:
        parts = [f"name={self.name!r}"]
        if self.requires is not None:
            parts.append(f"requires={self.requires.__name__}")
        if self.provides is not None:
            parts.append(f"provides={self.provides.__name__}")
        return f"Step({', '.join(parts)})"

    async def run(self, ctx: Any) -> Any:
        """Execute the step's run function.

        Sync/async is decided once at construction time, not per-call.
        For blocking I/O, users should use asyncio.to_thread themselves.
        """
        if self._run_is_async:
            return await self._run_fn(ctx)
        return self._run_fn(ctx)

    async def run_rollback(self, ctx: Any, output: Any) -> None:
        """Execute the rollback function if one exists."""
        if self._rollback_fn is None:
            return
        result = self._rollback_fn(ctx, output)
        if self._rollback_is_async:
            await result  # type: ignore[misc]

    def rollback(self, fn: RollbackFn) -> RollbackFn:
        """Decorator to attach a rollback handler to this step."""
        self._rollback_fn = fn
        self._rollback_is_async = inspect.iscoroutinefunction(fn)
        return fn

    @property
    def has_rollback(self) -> bool:
        return self._rollback_fn is not None


def step(
    *,
    requires: type[BaseModel] | None = None,
    provides: type[BaseModel] | None = None,
    name: str | None = None,
    retry: RetryPolicy | None = None,
    timeout: float | None = None,
) -> Callable[[RunFn], Step]:
    """Decorator to create a Step from a function.

    Usage:
        @step(requires=OrderInput, provides=ChargeOutput)
        async def charge_payment(ctx: OrderInput) -> ChargeOutput:
            ...
    """

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
