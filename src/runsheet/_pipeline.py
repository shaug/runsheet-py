"""Pipeline class and execution engine.

Pipeline is a Runnable — it has a run() method returning StepResult with
AggregateMeta. Pipelines compose into other pipelines' step arrays for
nested execution with rollback propagation.
"""

from __future__ import annotations

from collections.abc import Sequence
from types import MappingProxyType
from typing import Any

from pydantic import BaseModel, ValidationError

from runsheet._errors import ArgsValidationError, RollbackError, StrictOverlapError
from runsheet._internal import (
    aggregate_meta,
    step_failure,
    step_success,
    to_ctx_dict,
)
from runsheet._middleware import StepInfo, StepMiddleware, compose_middleware
from runsheet._result import (
    AggregateMeta,
    RollbackCallback,
    Runnable,
    StepFailure,
    StepResult,
)
from runsheet._rollback import ExecutedStep, do_rollback


class Pipeline:
    """Pipeline that sequences steps with context accumulation.

    Pipeline satisfies the Runnable protocol — it has name, run(),
    has_rollback, run_rollback(), and make_rollback(). When used as a
    nested step in an outer pipeline, failure of a later outer step
    triggers rollback of this pipeline's inner steps via make_rollback().
    """

    def __init__(
        self,
        *,
        name: str,
        steps: Sequence[Runnable],
        args_schema: type[BaseModel] | None = None,
        middleware: list[StepMiddleware] | None = None,
        strict: bool = False,
    ) -> None:
        self._name = name
        self._requires: type[BaseModel] | None = args_schema
        self._provides: type[BaseModel] | None = None
        self._steps = tuple(steps)
        self._middleware = tuple(middleware or [])
        self._strict = strict

        # Captured execution state for rollback when used as nested step.
        # make_rollback() captures and clears this immediately after run().
        self._captured_state: list[ExecutedStep] | None = None

        if strict:
            self._check_strict_overlaps()

    def __repr__(self) -> str:
        return f"Pipeline(name={self._name!r}, steps={len(self._steps)})"

    @property
    def name(self) -> str:
        return self._name

    @property
    def requires(self) -> type[BaseModel] | None:
        return self._requires

    @property
    def provides(self) -> type[BaseModel] | None:
        return self._provides

    @property
    def has_rollback(self) -> bool:
        return True  # Pipelines always support rollback

    def _check_strict_overlaps(self) -> None:
        """Check for provides key collisions at build time."""
        seen: dict[str, str] = {}

        for s in self._steps:
            provides = s.provides
            if provides is not None and issubclass(provides, BaseModel):
                step_name = s.name
                for field_name in provides.model_fields:
                    if field_name in seen:
                        raise StrictOverlapError(
                            f"Field '{field_name}' is provided by both "
                            f"'{seen[field_name]}' and '{step_name}'",
                            key=field_name,
                            steps=(seen[field_name], step_name),
                        )
                    seen[field_name] = step_name

    async def run(
        self, ctx: object = None,
    ) -> StepResult[dict[str, Any]]:
        """Execute the pipeline. Never raises — always returns StepResult."""
        self._captured_state = None
        steps_executed: list[str] = []
        executed: list[ExecutedStep] = []

        # Normalize input to dict
        ctx_dict = to_ctx_dict(ctx) if ctx is not None else {}
        if isinstance(ctx, (dict, BaseModel)):
            ctx_dict = dict(ctx_dict)  # ensure a fresh copy

        args_snapshot: dict[str, Any] = dict(ctx_dict)

        def make_meta() -> AggregateMeta:
            return aggregate_meta(
                self._name,
                MappingProxyType(dict(args_snapshot)),
                tuple(steps_executed),
            )

        # Validate pipeline args if schema provided
        if self._requires is not None:
            try:
                self._requires.model_validate(ctx_dict)
            except ValidationError as e:
                return step_failure(
                    ArgsValidationError(f"Args validation failed: {e}"),
                    make_meta(),
                    "<args>",
                )

        # Unified execution loop — all Runnables treated uniformly
        for pipeline_step in self._steps:
            # Snapshot pre-step context
            pre_ctx = dict(ctx_dict)

            # Execute through middleware
            result = await self._run_step_with_middleware(pipeline_step, ctx_dict)

            if isinstance(result, StepFailure):
                rollback_report = await do_rollback(executed)
                return step_failure(
                    result.error,
                    make_meta(),
                    pipeline_step.name,
                    rollback_report,
                )

            # Track and accumulate
            output: dict[str, Any] = dict(result.data)
            rollback_cb = pipeline_step.make_rollback(pre_ctx, output)
            executed.append(
                ExecutedStep(name=pipeline_step.name, rollback=rollback_cb)
            )
            steps_executed.append(pipeline_step.name)
            ctx_dict = {**ctx_dict, **output}

        # Capture state for nested rollback
        self._captured_state = executed

        return step_success(dict(ctx_dict), make_meta())

    async def run_rollback(self, ctx: object = None, output: object = None) -> None:
        """Rollback this pipeline's executed steps.

        Called by an outer pipeline when a later step fails.
        Raises RollbackError if any inner rollbacks fail.
        """
        if self._captured_state:
            state = self._captured_state
            self._captured_state = None
            report = await do_rollback(state)
            if report.failed:
                causes = tuple(f.error for f in report.failed)
                raise RollbackError(
                    f"Pipeline '{self._name}': {len(causes)} rollback(s) failed",
                    causes=causes,
                )

    def make_rollback(
        self, pre_ctx: dict[str, Any], output: dict[str, Any]
    ) -> RollbackCallback | None:
        """Capture rollback state for this execution.

        Called by an outer pipeline immediately after run() succeeds.
        Captures and clears _captured_state, making this safe for
        reentrant use.
        """
        state = self._captured_state
        self._captured_state = None
        if not state:
            return None

        async def cb() -> None:
            report = await do_rollback(state)
            if report.failed:
                causes = tuple(f.error for f in report.failed)
                raise RollbackError(
                    f"Pipeline '{self._name}': {len(causes)} rollback(s) failed",
                    causes=causes,
                )

        return cb

    async def _run_step_with_middleware(
        self, s: Runnable, ctx: dict[str, Any]
    ) -> StepResult[dict[str, Any]]:
        """Run a step through middleware, delegating to step.run()."""

        async def core(c: dict[str, Any]) -> StepResult[dict[str, Any]]:
            return await s.run(c)

        if self._middleware:
            wrapped = compose_middleware(
                list(self._middleware),
                StepInfo(
                    name=s.name,
                    requires=s.requires,
                    provides=s.provides,
                ),
                core,
            )
            return await wrapped(ctx)
        return await core(ctx)
