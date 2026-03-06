"""Tests for map_step."""

from typing import Any

from pydantic import BaseModel

from runsheet import (
    AggregateFailure,
    AggregateSuccess,
    Pipeline,
    RollbackError,
    map_step,
    step,
)


class TestMapFunctionForm:
    async def test_basic_map(self) -> None:
        pipeline = Pipeline(
            name="test",
            steps=[
                map_step(
                    "doubled",
                    lambda ctx: ctx.get("numbers", []),
                    lambda item, ctx: item * 2,
                ),
            ],
        )
        result = await pipeline.run({"numbers": [1, 2, 3]})
        assert isinstance(result, AggregateSuccess)
        assert result.data["doubled"] == [2, 4, 6]

    async def test_async_mapper(self) -> None:
        async def double(item: int, ctx: dict[str, Any]) -> int:
            return item * 2

        pipeline = Pipeline(
            name="test",
            steps=[
                map_step("doubled", lambda ctx: ctx.get("numbers", []), double),
            ],
        )
        result = await pipeline.run({"numbers": [1, 2, 3]})
        assert isinstance(result, AggregateSuccess)
        assert result.data["doubled"] == [2, 4, 6]

    async def test_map_with_context(self) -> None:
        pipeline = Pipeline(
            name="test",
            steps=[
                map_step(
                    "results",
                    lambda ctx: ctx.get("items", []),
                    lambda item, ctx: f"{item}_{ctx.get('prefix', '')}",
                ),
            ],
        )
        result = await pipeline.run({"items": ["a", "b"], "prefix": "x"})
        assert isinstance(result, AggregateSuccess)
        assert result.data["results"] == ["a_x", "b_x"]

    async def test_map_empty_collection(self) -> None:
        pipeline = Pipeline(
            name="test",
            steps=[
                map_step(
                    "results",
                    lambda ctx: [],
                    lambda item, ctx: item,
                ),
            ],
        )
        result = await pipeline.run({})
        assert isinstance(result, AggregateSuccess)
        assert result.data["results"] == []


class TestMapStepForm:
    async def test_step_form(self) -> None:
        class ItemOutput(BaseModel):
            processed: bool

        @step(provides=ItemOutput)
        async def process_item(ctx: dict) -> ItemOutput:  # type: ignore[type-arg]
            return ItemOutput(processed=True)

        pipeline = Pipeline(
            name="test",
            steps=[
                map_step(
                    "results",
                    lambda ctx: ctx.get("items", []),
                    process_item,
                ),
            ],
        )
        result = await pipeline.run({"items": [{"id": 1}, {"id": 2}]})
        assert isinstance(result, AggregateSuccess)
        assert len(result.data["results"]) == 2
        assert all(r["processed"] for r in result.data["results"])

    async def test_step_form_rollback_on_partial_failure(self) -> None:
        rollback_count = 0

        class ItemOutput(BaseModel):
            item_id: int

        call_count = 0

        @step(provides=ItemOutput)
        async def process_item(ctx: dict[str, Any]) -> ItemOutput:
            nonlocal call_count
            call_count += 1
            item_id: int = ctx.get("id", 0)
            if item_id == 2:
                raise RuntimeError("item 2 failed")
            return ItemOutput(item_id=item_id)

        @process_item.rollback
        async def undo_process(ctx: dict, output: dict) -> None:  # type: ignore[type-arg]
            nonlocal rollback_count
            rollback_count += 1

        pipeline = Pipeline(
            name="test",
            steps=[
                map_step(
                    "results",
                    lambda ctx: ctx.get("items", []),
                    process_item,
                ),
            ],
        )
        result = await pipeline.run({"items": [{"id": 1}, {"id": 2}, {"id": 3}]})
        assert isinstance(result, AggregateFailure)
        # Succeeded items should be rolled back
        assert rollback_count > 0

    async def test_step_form_external_rollback(self) -> None:
        """When a later step fails, map_step rolls back all its items."""
        rolled_back: list[int] = []

        class ItemOutput(BaseModel):
            item_id: int

        @step(provides=ItemOutput)
        async def process_item(ctx: dict[str, Any]) -> ItemOutput:
            return ItemOutput(item_id=ctx.get("id", 0))

        @process_item.rollback
        async def undo_process(  # pyright: ignore[reportUnusedFunction]
            ctx: dict[str, Any], output: dict[str, Any]
        ) -> None:
            rolled_back.append(output["item_id"])

        class DoneOutput(BaseModel):
            done: bool

        @step(provides=DoneOutput)
        async def later_fails(ctx: dict[str, Any]) -> DoneOutput:
            raise RuntimeError("later fail")

        pipeline = Pipeline(
            name="test",
            steps=[
                map_step(
                    "results",
                    lambda ctx: ctx.get("items", []),
                    process_item,
                ),
                later_fails,
            ],
        )
        result = await pipeline.run({"items": [{"id": 1}, {"id": 2}]})
        assert isinstance(result, AggregateFailure)
        assert sorted(rolled_back) == [1, 2]

    async def test_step_form_external_rollback_error_reported(self) -> None:
        """Rollback errors from map_step items are reported."""

        class ItemOutput(BaseModel):
            item_id: int

        @step(provides=ItemOutput)
        async def process_item(ctx: dict[str, Any]) -> ItemOutput:
            return ItemOutput(item_id=ctx.get("id", 0))

        @process_item.rollback
        async def undo_process(  # pyright: ignore[reportUnusedFunction]
            ctx: dict[str, Any], output: dict[str, Any]
        ) -> None:
            raise RuntimeError("rollback boom")

        class DoneOutput(BaseModel):
            done: bool

        @step(provides=DoneOutput)
        async def later_fails(ctx: dict[str, Any]) -> DoneOutput:
            raise RuntimeError("later fail")

        pipeline = Pipeline(
            name="test",
            steps=[
                map_step(
                    "results",
                    lambda ctx: ctx.get("items", []),
                    process_item,
                ),
                later_fails,
            ],
        )
        result = await pipeline.run({"items": [{"id": 1}]})
        assert isinstance(result, AggregateFailure)
        assert len(result.rollback.failed) == 1
        assert isinstance(result.rollback.failed[0].error, RollbackError)

    async def test_step_form_rollback_receives_item_context(self) -> None:
        """Rollback handler receives per-item context, not base context."""
        rollback_contexts: list[dict[str, Any]] = []

        class ItemOutput(BaseModel):
            item_id: int

        @step(provides=ItemOutput)
        async def process_item(ctx: dict[str, Any]) -> ItemOutput:
            return ItemOutput(item_id=ctx.get("id", 0))

        @process_item.rollback
        async def undo_process(  # pyright: ignore[reportUnusedFunction]
            ctx: dict[str, Any], output: dict[str, Any]
        ) -> None:
            rollback_contexts.append(dict(ctx))

        call_count = 0

        @step(provides=ItemOutput)
        async def flaky_item(ctx: dict[str, Any]) -> ItemOutput:
            nonlocal call_count
            call_count += 1
            item_id = ctx.get("id", 0)
            if item_id == 2:
                raise RuntimeError("item 2 failed")
            return ItemOutput(item_id=item_id)

        @flaky_item.rollback
        async def undo_flaky(  # pyright: ignore[reportUnusedFunction]
            ctx: dict[str, Any], output: dict[str, Any]
        ) -> None:
            rollback_contexts.append(dict(ctx))

        pipeline = Pipeline(
            name="test",
            steps=[
                map_step(
                    "results",
                    lambda ctx: ctx.get("items", []),
                    flaky_item,
                ),
            ],
        )
        result = await pipeline.run(
            {"items": [{"id": 1}, {"id": 2}, {"id": 3}], "base_key": "value"}
        )
        assert isinstance(result, AggregateFailure)
        # Each rollback context should have the item's "id" merged in
        for ctx in rollback_contexts:
            assert "id" in ctx
            assert "base_key" in ctx
