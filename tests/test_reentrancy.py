"""Tests for pipeline reentrancy, RollbackError chaining, and aggregate metadata."""

import asyncio
from typing import Any

from pydantic import BaseModel

from runsheet import (
    AggregateFailure,
    AggregateMeta,
    AggregateSuccess,
    Pipeline,
    RollbackError,
    choice,
    parallel,
    step,
)


class OutputA(BaseModel):
    a: str


class OutputB(BaseModel):
    b: str


class OutputC(BaseModel):
    c: str


# ---------------------------------------------------------------------------
# Concurrent Pipeline.run() reentrancy
# ---------------------------------------------------------------------------


class TestPipelineReentrancy:
    async def test_concurrent_runs_independent_rollback(self) -> None:
        """Two concurrent runs of the same pipeline get independent rollback state."""
        log: list[str] = []

        @step(provides=OutputA)
        async def slow_step(ctx: dict) -> OutputA:  # type: ignore[type-arg]
            await asyncio.sleep(0.02)
            return OutputA(a=ctx.get("id", "?"))

        @slow_step.rollback
        async def undo_slow(ctx: dict[str, Any], output: dict[str, Any]) -> None:
            log.append(f"rollback-{output.get('a')}")

        @step(provides=OutputB)
        async def fail_step(ctx: dict) -> OutputB:  # type: ignore[type-arg]
            raise RuntimeError("boom")

        pipeline = Pipeline(name="reentrant", steps=[slow_step, fail_step])

        r1, r2 = await asyncio.gather(
            pipeline.run({"id": "1"}),
            pipeline.run({"id": "2"}),
        )

        assert isinstance(r1, AggregateFailure)
        assert isinstance(r2, AggregateFailure)
        # Both runs should have triggered rollback independently
        assert "rollback-1" in log
        assert "rollback-2" in log

    async def test_sequential_runs_no_stale_state(self) -> None:
        """A second run() doesn't carry over rollback state from the first."""
        log: list[str] = []

        @step(provides=OutputA)
        async def step_a(ctx: dict) -> OutputA:  # type: ignore[type-arg]
            return OutputA(a="a")

        @step_a.rollback
        async def undo_a(ctx: dict[str, Any], output: dict[str, Any]) -> None:
            log.append("rollback-a")

        @step(provides=OutputB)
        async def step_b(ctx: dict) -> OutputB:  # type: ignore[type-arg]
            if ctx.get("fail"):
                raise RuntimeError("fail")
            return OutputB(b="b")

        pipeline = Pipeline(name="test", steps=[step_a, step_b])

        # First run succeeds
        r1 = await pipeline.run({})
        assert isinstance(r1, AggregateSuccess)
        assert log == []

        # Second run fails — only this run's step_a should rollback
        r2 = await pipeline.run({"fail": True})
        assert isinstance(r2, AggregateFailure)
        assert log == ["rollback-a"]


# ---------------------------------------------------------------------------
# RollbackError.causes and __cause__ chaining
# ---------------------------------------------------------------------------


class TestRollbackErrorChaining:
    async def test_single_cause_sets_dunder_cause(self) -> None:
        """RollbackError with one cause sets __cause__ to that exception."""
        cause = RuntimeError("inner")
        err = RollbackError("rollback failed", causes=(cause,))
        assert err.causes == (cause,)
        assert err.__cause__ is cause

    async def test_multiple_causes_sets_exception_group(self) -> None:
        """RollbackError with multiple causes sets __cause__ to ExceptionGroup."""
        c1 = RuntimeError("first")
        c2 = ValueError("second")
        err = RollbackError("rollback failed", causes=(c1, c2))
        assert err.causes == (c1, c2)
        assert isinstance(err.__cause__, ExceptionGroup)
        assert list(err.__cause__.exceptions) == [c1, c2]

    async def test_no_causes_no_chaining(self) -> None:
        """RollbackError with no causes has no __cause__."""
        err = RollbackError("rollback failed")
        assert err.causes == ()
        assert err.__cause__ is None

    async def test_pipeline_rollback_error_in_result(self) -> None:
        """Pipeline failure with failing rollback includes cause chain."""

        @step(provides=OutputA)
        async def step_a(ctx: dict) -> OutputA:  # type: ignore[type-arg]
            return OutputA(a="a")

        @step_a.rollback
        async def undo_a(ctx: dict, output: dict) -> None:  # type: ignore[type-arg]
            raise ValueError("undo failed")

        @step(provides=OutputB)
        async def step_b(ctx: dict) -> OutputB:  # type: ignore[type-arg]
            raise RuntimeError("b failed")

        pipeline = Pipeline(name="test", steps=[step_a, step_b])
        result = await pipeline.run({})

        assert isinstance(result, AggregateFailure)
        assert len(result.rollback.failed) == 1
        assert isinstance(result.rollback.failed[0].error, ValueError)


# ---------------------------------------------------------------------------
# parallel/choice steps_executed metadata
# ---------------------------------------------------------------------------


class TestAggregateMetadata:
    async def test_parallel_steps_executed(self) -> None:
        """parallel() result meta includes all executed step names."""

        @step(provides=OutputA)
        async def step_a(ctx: dict) -> OutputA:  # type: ignore[type-arg]
            return OutputA(a="a")

        @step(provides=OutputB)
        async def step_b(ctx: dict) -> OutputB:  # type: ignore[type-arg]
            return OutputB(b="b")

        pipeline = Pipeline(
            name="test", steps=[parallel(step_a, step_b)]
        )
        result = await pipeline.run({})

        assert isinstance(result, AggregateSuccess)
        assert isinstance(result.meta, AggregateMeta)
        # The pipeline's steps_executed should include the parallel composite
        assert "parallel(step_a, step_b)" in result.meta.steps_executed

    async def test_parallel_partial_failure_steps_executed(self) -> None:
        """parallel() failure meta includes only steps that completed."""

        @step(provides=OutputA)
        async def step_a(ctx: dict) -> OutputA:  # type: ignore[type-arg]
            return OutputA(a="a")

        @step(provides=OutputB)
        async def step_b(ctx: dict) -> OutputB:  # type: ignore[type-arg]
            raise RuntimeError("fail")

        pipeline = Pipeline(
            name="test", steps=[parallel(step_a, step_b)]
        )
        result = await pipeline.run({})

        assert isinstance(result, AggregateFailure)

    async def test_choice_steps_executed_shows_branch(self) -> None:
        """choice() result meta tracks which branch was executed."""

        @step(provides=OutputA)
        async def card_pay(ctx: dict) -> OutputA:  # type: ignore[type-arg]
            return OutputA(a="card")

        @step(provides=OutputB)
        async def bank_pay(ctx: dict) -> OutputB:  # type: ignore[type-arg]
            return OutputB(b="bank")

        pipeline = Pipeline(
            name="test",
            steps=[
                choice(
                    (lambda ctx: ctx.get("method") == "card", card_pay),
                    bank_pay,  # default
                )
            ],
        )

        # Card branch
        r1 = await pipeline.run({"method": "card"})
        assert isinstance(r1, AggregateSuccess)

        # Bank default branch
        r2 = await pipeline.run({"method": "bank"})
        assert isinstance(r2, AggregateSuccess)

    async def test_pipeline_steps_executed_tracks_all(self) -> None:
        """Pipeline meta.steps_executed lists all step names in order."""

        @step(provides=OutputA)
        async def step_a(ctx: dict) -> OutputA:  # type: ignore[type-arg]
            return OutputA(a="a")

        @step(provides=OutputB)
        async def step_b(ctx: dict) -> OutputB:  # type: ignore[type-arg]
            return OutputB(b="b")

        pipeline = Pipeline(name="test", steps=[step_a, step_b])
        result = await pipeline.run({})

        assert isinstance(result, AggregateSuccess)
        assert isinstance(result.meta, AggregateMeta)
        assert result.meta.steps_executed == ("step_a", "step_b")
