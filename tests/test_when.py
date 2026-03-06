"""Tests for when (conditional steps)."""

from pydantic import BaseModel

from runsheet import (
    Pipeline,
    PipelineFailure,
    PipelineSuccess,
    PredicateError,
    step,
    when,
)


class Output(BaseModel):
    notified: bool


class TestWhen:
    async def test_when_true_runs_step(self) -> None:
        @step(provides=Output)
        async def notify(ctx: dict) -> Output:  # type: ignore[type-arg]
            return Output(notified=True)

        pipeline = Pipeline(
            name="test",
            steps=[when(lambda ctx: True, notify)],
        )
        result = await pipeline.run({})
        assert isinstance(result, PipelineSuccess)
        assert result.data["notified"] is True

    async def test_when_false_skips_step(self) -> None:
        @step(provides=Output)
        async def notify(ctx: dict) -> Output:  # type: ignore[type-arg]
            return Output(notified=True)

        pipeline = Pipeline(
            name="test",
            steps=[when(lambda ctx: False, notify)],
        )
        result = await pipeline.run({})
        assert isinstance(result, PipelineSuccess)
        assert "notified" not in result.data
        assert result.meta.steps_skipped == ("notify",)

    async def test_when_uses_context(self) -> None:
        @step(provides=Output)
        async def notify(ctx: dict) -> Output:  # type: ignore[type-arg]
            return Output(notified=True)

        pipeline = Pipeline(
            name="test",
            steps=[when(lambda ctx: ctx.get("amount", 0) > 100, notify)],
        )

        result = await pipeline.run({"amount": 50})
        assert isinstance(result, PipelineSuccess)
        assert "notified" not in result.data

        result = await pipeline.run({"amount": 200})
        assert isinstance(result, PipelineSuccess)
        assert result.data["notified"] is True

    async def test_when_predicate_error_triggers_rollback(self) -> None:
        rollback_called = False

        class InitOutput(BaseModel):
            initialized: bool

        @step(provides=InitOutput)
        async def init_step(ctx: dict) -> InitOutput:  # type: ignore[type-arg]
            return InitOutput(initialized=True)

        @init_step.rollback
        async def undo_init(ctx: dict, output: dict) -> None:  # type: ignore[type-arg]
            nonlocal rollback_called
            rollback_called = True

        @step(provides=Output)
        async def notify(ctx: dict) -> Output:  # type: ignore[type-arg]
            return Output(notified=True)

        def bad_predicate(ctx: dict) -> bool:  # type: ignore[type-arg]
            raise RuntimeError("predicate exploded")

        pipeline = Pipeline(
            name="test",
            steps=[init_step, when(bad_predicate, notify)],
        )
        result = await pipeline.run({})
        assert isinstance(result, PipelineFailure)
        assert isinstance(result.errors[0], PredicateError)
        assert rollback_called

    async def test_skipped_step_no_rollback(self) -> None:
        """Skipped steps don't participate in rollback."""
        rollback_log: list[str] = []

        @step(provides=Output)
        async def notify(ctx: dict) -> Output:  # type: ignore[type-arg]
            return Output(notified=True)

        @notify.rollback
        async def undo_notify(ctx: dict, output: dict) -> None:  # type: ignore[type-arg]
            rollback_log.append("notify")

        class FailOutput(BaseModel):
            done: bool

        @step(provides=FailOutput)
        async def failing(ctx: dict) -> FailOutput:  # type: ignore[type-arg]
            raise RuntimeError("boom")

        pipeline = Pipeline(
            name="test",
            steps=[
                when(lambda ctx: False, notify),
                failing,
            ],
        )
        result = await pipeline.run({})
        assert isinstance(result, PipelineFailure)
        # Notify was skipped, so its rollback should NOT run
        assert rollback_log == []

    async def test_when_with_async_predicate(self) -> None:
        @step(provides=Output)
        async def notify(ctx: dict) -> Output:  # type: ignore[type-arg]
            return Output(notified=True)

        async def async_pred(ctx: dict) -> bool:  # type: ignore[type-arg]
            return True

        pipeline = Pipeline(
            name="test",
            steps=[when(async_pred, notify)],
        )
        result = await pipeline.run({})
        assert isinstance(result, PipelineSuccess)
        assert result.data["notified"] is True
