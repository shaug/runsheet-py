"""Combinators: when, parallel, choice.

Combinator inner steps do NOT go through middleware. Middleware wraps the
composite combinator step as a whole, not the individual inner steps.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable
from typing import Any

from runsheet._combinator_base import CombinatorResult
from runsheet._errors import ChoiceNoMatchError, PredicateError
from runsheet._internal import call_predicate, execute_step
from runsheet._rollback import ExecutedStep
from runsheet._step import Step


class _WhenStep:
    """Conditional step wrapper. Skipped steps produce no snapshot and no rollback."""

    def __init__(
        self,
        predicate: Callable[..., Any],
        inner_step: Step,
    ) -> None:
        self.name = f"when({inner_step.name})"
        self._predicate = predicate
        self._inner_step = inner_step

    async def execute(self, ctx: dict[str, Any]) -> CombinatorResult:
        try:
            should_run = await call_predicate(self._predicate, ctx)
        except Exception as e:
            raise PredicateError(
                f"Predicate for '{self._inner_step.name}' raised: {e}"
            ) from e

        if not should_run:
            return CombinatorResult(skipped=[self._inner_step.name])

        pre_ctx = dict(ctx)
        output = await execute_step(self._inner_step, ctx)
        return CombinatorResult(
            output=output,
            executed=[
                ExecutedStep(
                    step=self._inner_step,
                    pre_ctx=pre_ctx,
                    output=output,
                )
            ],
        )


def when(predicate: Callable[..., Any], inner_step: Step) -> _WhenStep:
    """Create a conditional step that only runs when predicate returns True.

    If predicate raises, pipeline treats it as a step failure and triggers rollback.
    Skipped steps are tracked in metadata but produce no rollback entry.
    """
    return _WhenStep(predicate, inner_step)


class _ParallelStep:
    """Concurrent step composition via asyncio.gather."""

    def __init__(self, *steps: Step) -> None:
        names = ", ".join(s.name for s in steps)
        self.name = f"parallel({names})"
        self._steps = steps

    async def execute(self, ctx: dict[str, Any]) -> CombinatorResult:
        # All inner steps receive the same pre-parallel context
        pre_ctx = dict(ctx)

        async def run_one(
            s: Step,
        ) -> tuple[Step, dict[str, Any], Exception | None]:
            try:
                output = await execute_step(s, dict(ctx))
                return (s, output, None)
            except Exception as e:
                return (s, {}, e)

        results = await asyncio.gather(*(run_one(s) for s in self._steps))

        # Check for failures
        errors: list[Exception] = []
        succeeded: list[tuple[Step, dict[str, Any]]] = []
        for s, output, error in results:
            if error is not None:
                errors.append(error)
            else:
                succeeded.append((s, output))

        if errors:
            # Rollback succeeded inner steps in reverse order
            for s, output in reversed(succeeded):
                if s.has_rollback:
                    with contextlib.suppress(Exception):
                        await s.run_rollback(pre_ctx, output)
            raise errors[0]

        # Merge outputs in list order
        merged: dict[str, Any] = {}
        executed: list[ExecutedStep] = []
        for s, output in succeeded:
            merged.update(output)
            executed.append(ExecutedStep(step=s, pre_ctx=pre_ctx, output=output))

        return CombinatorResult(output=merged, executed=executed)


def parallel(*steps: Step) -> _ParallelStep:
    """Create a parallel step that runs inner steps concurrently.

    All inner steps receive the same pre-parallel context.
    Outputs merge in list order. On partial failure, succeeded inner
    steps are rolled back in reverse order.
    """
    return _ParallelStep(*steps)


class _ChoiceStep:
    """Branching: predicates evaluated in order, first match wins."""

    def __init__(
        self,
        branches: tuple[tuple[Callable[..., Any], Step] | Step, ...],
    ) -> None:
        self.name = "choice"
        self._branches = branches

    async def execute(self, ctx: dict[str, Any]) -> CombinatorResult:
        for branch in self._branches:
            if isinstance(branch, Step):
                # Bare step = default branch
                pre_ctx = dict(ctx)
                output = await execute_step(branch, ctx)
                return CombinatorResult(
                    output=output,
                    executed=[
                        ExecutedStep(
                            step=branch,
                            pre_ctx=pre_ctx,
                            output=output,
                        )
                    ],
                )
            else:
                predicate, inner_step = branch
                try:
                    should_run = await call_predicate(predicate, ctx)
                except Exception as e:
                    raise PredicateError(
                        f"Choice predicate for '{inner_step.name}' raised: {e}"
                    ) from e

                if should_run:
                    pre_ctx = dict(ctx)
                    output = await execute_step(inner_step, ctx)
                    return CombinatorResult(
                        output=output,
                        executed=[
                            ExecutedStep(
                                step=inner_step,
                                pre_ctx=pre_ctx,
                                output=output,
                            )
                        ],
                    )

        raise ChoiceNoMatchError("No branch matched in choice step")


def choice(
    *branches: tuple[Callable[..., Any], Step] | Step,
) -> _ChoiceStep:
    """Create a branching step.

    Each branch is either a (predicate, step) tuple or a bare step (default).
    Predicates evaluated in order, first match wins.
    Bare step as last arg = default. No match = ChoiceNoMatchError.
    Only the matched branch participates in rollback.
    """
    return _ChoiceStep(branches)
