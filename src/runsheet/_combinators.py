"""Combinators: when, parallel, choice.

All combinators return Runnable instances. parallel() and choice() return
StepResult with AggregateMeta for steps_executed tracking. Combinator inner
steps do NOT go through middleware — middleware wraps the composite
combinator step as a whole.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable
from typing import Any, cast

from runsheet._errors import ChoiceNoMatchError, PredicateError, RollbackError
from runsheet._internal import (
    aggregate_meta,
    call_predicate,
    collapse_errors,
    partition_settled,
    step_failure,
    step_meta,
    step_success,
    to_ctx_dict,
    to_error,
)
from runsheet._result import RollbackCallback, StepFailure, StepResult
from runsheet._step import Step, _FnStep

# ---------------------------------------------------------------------------
# when() — conditional step wrapper
# ---------------------------------------------------------------------------


class _ConditionalStep(Step):
    """Step wrapper that evaluates a predicate before running.

    Pipeline treats _ConditionalStep like any other Runnable — no special-casing.
    When the predicate is False, run() returns an empty StepSuccess and
    make_rollback() returns None (the skipped step does not participate in
    rollback).
    """

    def __init__(
        self,
        predicate: Callable[[dict[str, Any]], object],
        inner_step: Step,
    ) -> None:
        super().__init__(
            name=f"when({inner_step.name})",
            requires=inner_step.requires,
            provides=inner_step.provides,
            run_fn=lambda _: None,  # type: ignore[arg-type]
        )
        self.predicate = predicate
        self.inner_step = inner_step
        self._skipped = True
        if inner_step.has_rollback:
            self._rollback_fn = inner_step._rollback_fn
            self._rollback_is_async = inner_step._rollback_is_async

    async def run(self, ctx: object) -> StepResult[dict[str, Any]]:
        self._skipped = True
        ctx_dict = to_ctx_dict(ctx)
        meta = step_meta(self.name, ctx_dict)

        try:
            should_run = await call_predicate(self.predicate, ctx_dict)
        except Exception as e:
            cause = to_error(e)
            inner = self.inner_step.name
            error = PredicateError(
                f"Predicate for '{inner}' raised: {cause}"
            )
            error.__cause__ = cause
            return step_failure(error, meta, self.name)

        if not should_run:
            return step_success({}, meta)

        self._skipped = False
        return await self.inner_step.run(ctx)

    def make_rollback(
        self, pre_ctx: dict[str, Any], output: dict[str, Any]
    ) -> RollbackCallback | None:
        if self._skipped:
            return None
        return super().make_rollback(pre_ctx, output)


def when(
    predicate: Callable[[dict[str, Any]], object],
    inner_step: Step,
) -> _ConditionalStep:
    """Conditional step — only runs when predicate returns True."""
    return _ConditionalStep(predicate, inner_step)


# ---------------------------------------------------------------------------
# parallel() — concurrent step composition
# ---------------------------------------------------------------------------


def parallel(*steps: Step) -> _FnStep:
    """Run inner steps concurrently, merge outputs in list order.

    Returns a StepResult with AggregateMeta tracking steps_executed.
    On partial failure, succeeded inner steps are rolled back
    in reverse order. Uses return_exceptions=True so all steps
    complete before errors are collected.
    """
    names = ", ".join(s.name for s in steps)
    step_name = f"parallel({names})"
    inner_steps = steps

    # Mutable state captured by make_rollback immediately after run.
    _last_succeeded: list[tuple[Step, dict[str, Any]]] = []

    async def run_fn(ctx: object) -> StepResult[dict[str, Any]]:
        ctx_dict = to_ctx_dict(ctx)
        pre_ctx: dict[str, Any] = dict(ctx_dict)

        async def run_one(
            s: Step,
        ) -> tuple[Step, dict[str, Any] | None, Exception | None]:
            result = await s.run(ctx_dict)
            if isinstance(result, StepFailure):
                return (s, None, result.error)
            return (s, result.data, None)

        raw = await asyncio.gather(
            *(run_one(s) for s in inner_steps),
            return_exceptions=True,
        )
        settled, gather_errors = partition_settled(raw)

        errors: list[Exception] = list(gather_errors)
        succeeded: list[tuple[Step, dict[str, Any]]] = []
        executed: list[str] = []

        for item in cast(
            list[tuple[Step, dict[str, Any] | None, Exception | None]],
            settled,
        ):
            s, output, err = item
            if err is not None:
                errors.append(err)
            else:
                assert output is not None
                succeeded.append((s, output))
                executed.append(s.name)

        if errors:
            for s, output in reversed(succeeded):
                if s.has_rollback:
                    with contextlib.suppress(Exception):
                        await s.run_rollback(pre_ctx, output)
            error = collapse_errors(
                errors, f"{step_name}: {len(errors)} step(s) failed"
            )
            meta = aggregate_meta(step_name, pre_ctx, tuple(executed))
            return step_failure(error, meta, step_name)

        # Store for make_rollback to capture
        _last_succeeded.clear()
        _last_succeeded.extend(succeeded)

        merged: dict[str, Any] = {}
        for _, output in succeeded:
            merged.update(output)

        meta = aggregate_meta(step_name, pre_ctx, tuple(executed))
        return step_success(merged, meta)

    def make_rb(
        pre_ctx: dict[str, Any], output: dict[str, Any]
    ) -> RollbackCallback | None:
        captured = list(_last_succeeded)
        _last_succeeded.clear()
        rollbackable = [(s, out) for s, out in captured if s.has_rollback]
        if not rollbackable:
            return None

        async def cb() -> None:
            errors: list[Exception] = []
            for s, out in reversed(rollbackable):
                try:
                    await s.run_rollback(pre_ctx, out)
                except Exception as e:
                    errors.append(to_error(e))
            if errors:
                raise RollbackError(
                    f"{step_name}: {len(errors)} rollback(s) failed",
                    causes=tuple(errors),
                )

        return cb

    return _FnStep(name=step_name, run_fn=run_fn, make_rollback_fn=make_rb)


# ---------------------------------------------------------------------------
# choice() — branching
# ---------------------------------------------------------------------------


def choice(
    *branches: tuple[Callable[[dict[str, Any]], object], Step] | Step,
) -> _FnStep:
    """Branching: predicates evaluated in order, first match wins.

    Returns a StepResult with AggregateMeta tracking which branch ran.
    Bare step as last arg = default. No match = ChoiceNoMatchError.
    Only the matched branch participates in rollback.
    """
    step_name = "choice"

    # Mutable state captured by make_rollback immediately after run.
    _last_matched: list[Step | None] = [None]

    async def run_fn(ctx: object) -> StepResult[dict[str, Any]]:
        ctx_dict = to_ctx_dict(ctx)
        _last_matched[0] = None

        for branch in branches:
            if isinstance(branch, Step):
                result = await branch.run(ctx)
                if isinstance(result, StepFailure):
                    meta = aggregate_meta(
                        step_name, ctx_dict, (branch.name,)
                    )
                    return step_failure(result.error, meta, step_name)
                _last_matched[0] = branch
                meta = aggregate_meta(
                    step_name, ctx_dict, (branch.name,)
                )
                return step_success(result.data, meta)

            predicate, inner_step = branch
            try:
                should_run = await call_predicate(predicate, ctx_dict)
            except Exception as e:
                cause = to_error(e)
                error = PredicateError(
                    f"Choice predicate for "
                    f"'{inner_step.name}' raised: {cause}"
                )
                error.__cause__ = cause
                meta = aggregate_meta(step_name, ctx_dict, ())
                return step_failure(error, meta, step_name)

            if should_run:
                result = await inner_step.run(ctx)
                if isinstance(result, StepFailure):
                    meta = aggregate_meta(
                        step_name, ctx_dict, (inner_step.name,)
                    )
                    return step_failure(result.error, meta, step_name)
                _last_matched[0] = inner_step
                meta = aggregate_meta(
                    step_name, ctx_dict, (inner_step.name,)
                )
                return step_success(result.data, meta)

        meta = aggregate_meta(step_name, ctx_dict, ())
        return step_failure(
            ChoiceNoMatchError("No branch matched in choice step"),
            meta,
            step_name,
        )

    def make_rb(
        pre_ctx: dict[str, Any], output: dict[str, Any]
    ) -> RollbackCallback | None:
        ms = _last_matched[0]
        _last_matched[0] = None
        if ms is None or not ms.has_rollback:
            return None

        async def cb() -> None:
            try:
                await ms.run_rollback(pre_ctx, output)
            except Exception as e:
                raise RollbackError(
                    f"{step_name}: 1 rollback(s) failed",
                    causes=(to_error(e),),
                ) from e

        return cb

    return _FnStep(name=step_name, run_fn=run_fn, make_rollback_fn=make_rb)
