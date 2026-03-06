"""Tests for filter_step."""

from typing import Any

from runsheet import AggregateSuccess, Pipeline, filter_step


class TestFilter:
    async def test_basic_filter(self) -> None:
        pipeline = Pipeline(
            name="test",
            steps=[
                filter_step(
                    "evens",
                    lambda ctx: ctx.get("numbers", []),
                    lambda n, _ctx: n % 2 == 0,
                ),
            ],
        )
        result = await pipeline.run({"numbers": [1, 2, 3, 4, 5, 6]})
        assert isinstance(result, AggregateSuccess)
        assert result.data["evens"] == [2, 4, 6]

    async def test_async_predicate(self) -> None:
        async def is_even(n: int, _ctx: dict[str, Any]) -> bool:
            return n % 2 == 0

        pipeline = Pipeline(
            name="test",
            steps=[
                filter_step(
                    "evens",
                    lambda ctx: ctx.get("numbers", []),
                    is_even,
                ),
            ],
        )
        result = await pipeline.run({"numbers": [1, 2, 3, 4]})
        assert isinstance(result, AggregateSuccess)
        assert result.data["evens"] == [2, 4]

    async def test_preserves_order(self) -> None:
        pipeline = Pipeline(
            name="test",
            steps=[
                filter_step(
                    "big",
                    lambda ctx: ctx.get("numbers", []),
                    lambda n, _ctx: n > 3,
                ),
            ],
        )
        result = await pipeline.run({"numbers": [5, 1, 4, 2, 6, 3]})
        assert isinstance(result, AggregateSuccess)
        assert result.data["big"] == [5, 4, 6]

    async def test_empty_result(self) -> None:
        pipeline = Pipeline(
            name="test",
            steps=[
                filter_step(
                    "filtered",
                    lambda ctx: ctx.get("items", []),
                    lambda _x, _ctx: False,
                ),
            ],
        )
        result = await pipeline.run({"items": [1, 2, 3]})
        assert isinstance(result, AggregateSuccess)
        assert result.data["filtered"] == []

    async def test_all_pass(self) -> None:
        pipeline = Pipeline(
            name="test",
            steps=[
                filter_step(
                    "all",
                    lambda ctx: ctx.get("items", []),
                    lambda _x, _ctx: True,
                ),
            ],
        )
        result = await pipeline.run({"items": [1, 2, 3]})
        assert isinstance(result, AggregateSuccess)
        assert result.data["all"] == [1, 2, 3]

    async def test_empty_collection(self) -> None:
        pipeline = Pipeline(
            name="test",
            steps=[
                filter_step(
                    "filtered",
                    lambda ctx: [],
                    lambda _x, _ctx: True,
                ),
            ],
        )
        result = await pipeline.run({})
        assert isinstance(result, AggregateSuccess)
        assert result.data["filtered"] == []

    async def test_predicate_receives_context(self) -> None:
        """Predicate receives (item, ctx) — ctx includes pipeline context."""
        received_ctx: dict[str, Any] = {}

        def check(item: int, ctx: dict[str, Any]) -> bool:
            received_ctx.update(ctx)
            return item > ctx.get("threshold", 0)

        pipeline = Pipeline(
            name="test",
            steps=[
                filter_step(
                    "above",
                    lambda ctx: ctx.get("numbers", []),
                    check,
                ),
            ],
        )
        result = await pipeline.run({"numbers": [1, 5, 10], "threshold": 4})
        assert isinstance(result, AggregateSuccess)
        assert result.data["above"] == [5, 10]
        assert received_ctx["threshold"] == 4
