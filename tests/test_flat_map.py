"""Tests for flat_map."""

from typing import Any

from runsheet import AggregateSuccess, Pipeline, flat_map


class TestFlatMap:
    async def test_basic_flat_map(self) -> None:
        pipeline = Pipeline(
            name="test",
            steps=[
                flat_map(
                    "items",
                    lambda ctx: ctx.get("orders", []),
                    lambda order, ctx: order.get("items", []),
                ),
            ],
        )
        result = await pipeline.run(
            {
                "orders": [
                    {"items": ["a", "b"]},
                    {"items": ["c"]},
                    {"items": ["d", "e", "f"]},
                ]
            }
        )
        assert isinstance(result, AggregateSuccess)
        assert result.data["items"] == ["a", "b", "c", "d", "e", "f"]

    async def test_async_mapper(self) -> None:
        async def get_items(order: dict[str, Any], ctx: dict[str, Any]) -> list[Any]:
            return order.get("items", [])

        pipeline = Pipeline(
            name="test",
            steps=[
                flat_map("items", lambda ctx: ctx.get("orders", []), get_items),
            ],
        )
        result = await pipeline.run({"orders": [{"items": [1, 2]}, {"items": [3]}]})
        assert isinstance(result, AggregateSuccess)
        assert result.data["items"] == [1, 2, 3]

    async def test_empty_results(self) -> None:
        pipeline = Pipeline(
            name="test",
            steps=[
                flat_map(
                    "items",
                    lambda ctx: ctx.get("orders", []),
                    lambda order, ctx: [],
                ),
            ],
        )
        result = await pipeline.run({"orders": [{"id": 1}, {"id": 2}]})
        assert isinstance(result, AggregateSuccess)
        assert result.data["items"] == []

    async def test_empty_collection(self) -> None:
        pipeline = Pipeline(
            name="test",
            steps=[
                flat_map("items", lambda ctx: [], lambda item, ctx: [item]),
            ],
        )
        result = await pipeline.run({})
        assert isinstance(result, AggregateSuccess)
        assert result.data["items"] == []

    async def test_single_level_flatten(self) -> None:
        """Only flattens one level."""
        pipeline = Pipeline(
            name="test",
            steps=[
                flat_map(
                    "items",
                    lambda ctx: ctx.get("data", []),
                    lambda item, ctx: [[item, item]],
                ),
            ],
        )
        result = await pipeline.run({"data": [1, 2]})
        assert isinstance(result, AggregateSuccess)
        # Should flatten one level: [[1,1], [2,2]] not [1,1,2,2]
        assert result.data["items"] == [[1, 1], [2, 2]]

    async def test_mapper_receives_ctx(self) -> None:
        """flat_map callback receives both item and pipeline context."""
        pipeline = Pipeline(
            name="test",
            steps=[
                flat_map(
                    "results",
                    lambda ctx: ctx.get("items", []),
                    lambda item, ctx: [f"{item}_{ctx.get('suffix', '')}"],
                ),
            ],
        )
        result = await pipeline.run({"items": ["a", "b"], "suffix": "x"})
        assert isinstance(result, AggregateSuccess)
        assert result.data["results"] == ["a_x", "b_x"]
