"""Pipeline class and execution engine.

The Pipeline is frozen/immutable after construction. pipeline.run() never raises —
it always returns a PipelineResult.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, cast

from pydantic import BaseModel, ValidationError

from runsheet._combinator_base import CombinatorResult, CombinatorStep
from runsheet._errors import (
    ArgsValidationError,
    StrictOverlapError,
)
from runsheet._internal import execute_step, freeze_context, to_error
from runsheet._middleware import StepInfo, StepMiddleware, compose_middleware
from runsheet._result import (
    PipelineExecutionMeta,
    PipelineFailure,
    PipelineResult,
    PipelineSuccess,
    RollbackReport,
)
from runsheet._rollback import ExecutedStep, do_rollback
from runsheet._step import Step

# Union of things that can appear in a steps list
PipelineStep = Step | CombinatorStep


class Pipeline:
    """Pipeline that sequences steps with context accumulation.

    Frozen after construction by convention, not enforcement.
    We're all adults here.
    """

    def __init__(
        self,
        *,
        name: str,
        steps: list[PipelineStep],
        args_schema: type[BaseModel] | None = None,
        middleware: list[StepMiddleware] | None = None,
        strict: bool = False,
    ) -> None:
        self.name = name
        self._steps = tuple(steps)
        self._args_schema = args_schema
        self._middleware = tuple(middleware or [])
        self._strict = strict

        if strict:
            self._check_strict_overlaps()

    def __repr__(self) -> str:
        return f"Pipeline(name={self.name!r}, steps={len(self._steps)})"

    def _check_strict_overlaps(self) -> None:
        """Check for provides key collisions at build time."""
        seen: dict[str, str] = {}  # field_name -> step_name

        for s in self._steps:
            if isinstance(s, Step) and s.provides is not None:
                step_name = s.name
                for field_name in s.provides.model_fields:
                    if field_name in seen:
                        raise StrictOverlapError(
                            f"Field '{field_name}' is provided by both "
                            f"'{seen[field_name]}' and '{step_name}'",
                            key=field_name,
                            steps=(seen[field_name], step_name),
                        )
                    seen[field_name] = step_name

    async def run(self, args: Any = None) -> PipelineResult[MappingProxyType[str, Any]]:
        """Execute the pipeline. Never raises — always returns PipelineResult."""
        steps_executed: list[str] = []
        steps_skipped: list[str] = []
        executed: list[ExecutedStep] = []

        # Validate args
        ctx: dict[str, Any]
        if args is not None:
            if isinstance(args, BaseModel):
                ctx = args.model_dump()
            elif isinstance(args, dict):
                ctx = cast(dict[str, Any], args)
            else:
                ctx = {"__args__": args}
        else:
            ctx = {}

        args_snapshot: dict[str, Any] = dict(ctx)

        if self._args_schema is not None:
            try:
                self._args_schema.model_validate(ctx)
            except ValidationError as e:
                return PipelineFailure(
                    errors=(ArgsValidationError(f"Args validation failed: {e}"),),
                    meta=PipelineExecutionMeta(
                        pipeline=self.name,
                        args=freeze_context(args_snapshot),
                        steps_executed=tuple(steps_executed),
                        steps_skipped=tuple(steps_skipped),
                    ),
                    failed_step="<args>",
                    rollback=RollbackReport(),
                )

        try:
            for pipeline_step in self._steps:
                match pipeline_step:
                    case Step() as s:
                        pre_ctx: dict[str, Any] = dict(ctx)
                        try:
                            output = await self._run_step_with_middleware(s, ctx)
                            executed.append(
                                ExecutedStep(
                                    step=s,
                                    pre_ctx=pre_ctx,
                                    output=output,
                                )
                            )
                            steps_executed.append(s.name)
                            ctx = {**ctx, **output}
                        except Exception as e:
                            error = to_error(e)
                            rollback_report = await do_rollback(executed)
                            return PipelineFailure(
                                errors=(error,),
                                meta=PipelineExecutionMeta(
                                    pipeline=self.name,
                                    args=freeze_context(args_snapshot),
                                    steps_executed=tuple(steps_executed),
                                    steps_skipped=tuple(steps_skipped),
                                ),
                                failed_step=s.name,
                                rollback=rollback_report,
                            )

                    case combo:
                        # Combinator step (when, parallel, choice, etc.)
                        try:
                            result = await self._run_combinator_with_middleware(
                                combo,
                                ctx,  # type: ignore[arg-type]
                            )
                            executed.extend(result.executed)
                            steps_executed.append(combo.name)
                            steps_skipped.extend(result.skipped)
                            ctx = {**ctx, **result.output}
                        except Exception as e:
                            error = to_error(e)
                            rollback_report = await do_rollback(executed)
                            return PipelineFailure(
                                errors=(error,),
                                meta=PipelineExecutionMeta(
                                    pipeline=self.name,
                                    args=freeze_context(args_snapshot),
                                    steps_executed=tuple(steps_executed),
                                    steps_skipped=tuple(steps_skipped),
                                ),
                                failed_step=combo.name,
                                rollback=rollback_report,
                            )

            return PipelineSuccess(
                data=freeze_context(ctx),
                meta=PipelineExecutionMeta(
                    pipeline=self.name,
                    args=freeze_context(args_snapshot),
                    steps_executed=tuple(steps_executed),
                    steps_skipped=tuple(steps_skipped),
                ),
            )
        except Exception as e:
            # Catch-all safety net — should not normally be reached
            error = to_error(e)
            rollback_report = await do_rollback(executed)
            return PipelineFailure(
                errors=(error,),
                meta=PipelineExecutionMeta(
                    pipeline=self.name,
                    args=freeze_context(args_snapshot),
                    steps_executed=tuple(steps_executed),
                    steps_skipped=tuple(steps_skipped),
                ),
                failed_step="<unknown>",
                rollback=rollback_report,
            )

    async def _run_step_with_middleware(
        self, s: Step, ctx: dict[str, Any]
    ) -> dict[str, Any]:
        """Run a step through middleware, then execute_step."""

        async def core(c: dict[str, Any]) -> dict[str, Any]:
            return await execute_step(s, c)

        if self._middleware:
            wrapped = compose_middleware(
                list(self._middleware), StepInfo(name=s.name), core
            )
            result = await wrapped(ctx)
            if isinstance(result, dict):
                return cast(dict[str, Any], result)
            return result  # type: ignore[return-value]
        return await core(ctx)

    async def _run_combinator_with_middleware(
        self, combo: CombinatorStep, ctx: dict[str, Any]
    ) -> CombinatorResult:
        """Run a combinator step through middleware."""
        # Middleware wraps the composite combinator step, not inner steps
        result_holder: list[CombinatorResult] = []

        async def core(c: dict[str, Any]) -> dict[str, Any]:
            cr = await combo.execute(c)
            result_holder.append(cr)
            return cr.output

        if self._middleware:
            wrapped = compose_middleware(
                list(self._middleware), StepInfo(name=combo.name), core
            )
            await wrapped(ctx)
        else:
            await core(ctx)

        if result_holder:
            return result_holder[0]
        return CombinatorResult()  # pragma: no cover
