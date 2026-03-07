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

from runsheet._errors import PredicateError, RollbackError
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
from runsheet._result import RollbackCallback, Runnable, StepFailure, StepResult
from runsheet._step import _FnStep

# ---------------------------------------------------------------------------
# when() — conditional step wrapper
# ---------------------------------------------------------------------------


def when(
    predicate: Callable[[dict[str, Any]], object],
    inner_step: Runnable,
) -> _FnStep:
    """Conditional step — only runs when predicate returns True."""
    step_name = inner_step.name

    # Per-call-site state: whether the inner step ran.
    # make_rb atomically consumes this — safe because Pipeline calls
    # run() then make_rollback() with no await in between.
    _ran: list[bool] = [False]

    async def run_fn(ctx: object) -> StepResult[dict[str, Any]]:
        _ran[0] = False
        ctx_dict = to_ctx_dict(ctx)
        meta = step_meta(step_name, ctx_dict)

        try:
            should_run = await call_predicate(predicate, ctx_dict)
        except Exception as e:
            cause = to_error(e)
            inner = inner_step.name
            error = PredicateError(f"Predicate for '{inner}' raised: {cause}")
            error.__cause__ = cause
            return step_failure(error, meta, step_name)

        if not should_run:
            return step_success({}, meta)

        _ran[0] = True
        return await inner_step.run(ctx_dict)

    def make_rb(
        pre_ctx: dict[str, Any], output: dict[str, Any]
    ) -> RollbackCallback | None:
        ran = _ran[0]
        _ran[0] = False
        if not ran:
            return None
        return inner_step.make_rollback(pre_ctx, output)

    return _FnStep(
        name=step_name,
        run_fn=run_fn,
        make_rollback_fn=make_rb,
        requires=inner_step.requires,
        provides=inner_step.provides,
    )


# ---------------------------------------------------------------------------
# parallel() — concurrent step composition
# ---------------------------------------------------------------------------


def parallel(*steps: Runnable) -> _FnStep:
    """Run inner steps concurrently, merge outputs in list order.

    Returns a StepResult with AggregateMeta tracking steps_executed.
    On partial failure, succeeded inner steps are rolled back
    in reverse order. Uses return_exceptions=True so all steps
    complete before errors are collected.
    """
    names = ", ".join(s.name for s in steps)
    step_name = f"parallel({names})"
    inner_steps = steps

    # Per-call-site state captured by make_rollback immediately after run.
    # Safe because Pipeline calls run() then make_rollback() with no await
    # in between (asyncio cooperative scheduling guarantees no interleaving).
    _last_succeeded: list[tuple[Runnable, dict[str, Any]]] = []

    async def run_fn(ctx: object) -> StepResult[dict[str, Any]]:
        ctx_dict = to_ctx_dict(ctx)
        pre_ctx: dict[str, Any] = dict(ctx_dict)

        async def run_one(
            s: Runnable,
        ) -> tuple[Runnable, dict[str, Any] | None, Exception | None]:
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
        succeeded: list[tuple[Runnable, dict[str, Any]]] = []
        executed: list[str] = []

        for item in cast(
            list[tuple[Runnable, dict[str, Any] | None, Exception | None]],
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
            for s, out in reversed(succeeded):
                rb = s.make_rollback(pre_ctx, out)
                if rb is not None:
                    with contextlib.suppress(Exception):
                        await rb()
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
        # Capture rollback callbacks for each succeeded step
        callbacks: list[RollbackCallback] = []
        for s, out in captured:
            rb = s.make_rollback(pre_ctx, out)
            if rb is not None:
                callbacks.append(rb)
        if not callbacks:
            return None

        async def cb() -> None:
            errors: list[Exception] = []
            for rb_cb in reversed(callbacks):
                try:
                    await rb_cb()
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
    *branches: tuple[Callable[[dict[str, Any]], object], Runnable] | Runnable,
) -> _FnStep:
    """Branching: predicates evaluated in order, first match wins.

    Returns a StepResult with AggregateMeta tracking which branch ran.
    Bare step as last arg = default. No match = empty success (no-op).
    Only the matched branch participates in rollback.
    """
    step_name = "choice"

    # Normalize bare steps to (always-true predicate, step) tuples.
    # A bare step (default branch) is only valid as the last argument.
    normalized: list[tuple[Callable[[dict[str, Any]], object], Runnable]] = []
    for i, b in enumerate(branches):
        if isinstance(b, tuple):
            normalized.append(b)
        elif i == len(branches) - 1:
            normalized.append((lambda _: True, b))
        else:
            raise ValueError(
                f"Bare step '{b.name}' must be the last argument (default branch)"
            )

    # Per-call-site state captured by make_rollback immediately after run.
    _last_matched: list[tuple[Runnable, dict[str, Any]] | None] = [None]

    async def run_fn(ctx: object) -> StepResult[dict[str, Any]]:
        ctx_dict = to_ctx_dict(ctx)
        _last_matched[0] = None

        for predicate, inner_step in normalized:
            try:
                should_run = await call_predicate(predicate, ctx_dict)
            except Exception as e:
                cause = to_error(e)
                error = PredicateError(
                    f"Choice predicate for '{inner_step.name}' raised: {cause}"
                )
                error.__cause__ = cause
                meta = aggregate_meta(step_name, ctx_dict, ())
                return step_failure(error, meta, step_name)

            if should_run:
                result = await inner_step.run(ctx_dict)
                if isinstance(result, StepFailure):
                    meta = aggregate_meta(step_name, ctx_dict, (inner_step.name,))
                    return step_failure(result.error, meta, step_name)
                _last_matched[0] = (inner_step, result.data)
                meta = aggregate_meta(step_name, ctx_dict, (inner_step.name,))
                return step_success(result.data, meta)

        meta = aggregate_meta(step_name, ctx_dict, ())
        return step_success({}, meta)

    def make_rb(
        pre_ctx: dict[str, Any], output: dict[str, Any]
    ) -> RollbackCallback | None:
        matched = _last_matched[0]
        _last_matched[0] = None
        if matched is None:
            return None
        ms, ms_output = matched
        return ms.make_rollback(pre_ctx, ms_output)

    return _FnStep(name=step_name, run_fn=run_fn, make_rollback_fn=make_rb)
