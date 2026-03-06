"""Tests for map_step."""

from pydantic import BaseModel

from runsheet import (
    Pipeline,
    PipelineFailure,
    PipelineSuccess,
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
        assert isinstance(result, PipelineSuccess)
        assert result.data["doubled"] == [2, 4, 6]

    async def test_async_mapper(self) -> None:
        async def double(item: int, ctx: dict) -> int:  # type: ignore[type-arg]
            return item * 2

        pipeline = Pipeline(
            name="test",
            steps=[
                map_step("doubled", lambda ctx: ctx.get("numbers", []), double),
            ],
        )
        result = await pipeline.run({"numbers": [1, 2, 3]})
        assert isinstance(result, PipelineSuccess)
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
        assert isinstance(result, PipelineSuccess)
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
        assert isinstance(result, PipelineSuccess)
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
        assert isinstance(result, PipelineSuccess)
        assert len(result.data["results"]) == 2
        assert all(r["processed"] for r in result.data["results"])

    async def test_step_form_rollback_on_partial_failure(self) -> None:
        rollback_count = 0

        class ItemOutput(BaseModel):
            item_id: int

        call_count = 0

        @step(provides=ItemOutput)
        async def process_item(ctx: dict) -> ItemOutput:  # type: ignore[type-arg]
            nonlocal call_count
            call_count += 1
            item_id = ctx.get("id", 0)
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
        assert isinstance(result, PipelineFailure)
        # Succeeded items should be rolled back
        assert rollback_count > 0
