"""Tests for middleware."""

import time
from typing import Any

from pydantic import BaseModel

from runsheet import (
    AggregateFailure,
    AggregateSuccess,
    Pipeline,
    StepInfo,
    step,
)
from runsheet._middleware import NextFn
from runsheet._result import StepResult


class Output(BaseModel):
    value: int


class TestMiddleware:
    async def test_middleware_wraps_step(self) -> None:
        log: list[str] = []

        async def logging_mw(
            step_info: StepInfo, next_fn: NextFn, ctx: dict[str, Any]
        ) -> StepResult[Any]:
            log.append(f"before:{step_info.name}")
            result = await next_fn(ctx)
            log.append(f"after:{step_info.name}")
            return result

        @step(provides=Output)
        async def my_step(ctx: dict) -> Output:  # type: ignore[type-arg]
            return Output(value=1)

        pipeline = Pipeline(
            name="test",
            steps=[my_step],
            middleware=[logging_mw],
        )
        result = await pipeline.run({})

        assert isinstance(result, AggregateSuccess)
        assert log == ["before:my_step", "after:my_step"]

    async def test_middleware_order(self) -> None:
        """First in list = outermost wrapper."""
        log: list[str] = []

        async def outer(
            step_info: StepInfo, next_fn: NextFn, ctx: dict[str, Any]
        ) -> StepResult[Any]:
            log.append("outer_before")
            result = await next_fn(ctx)
            log.append("outer_after")
            return result

        async def inner(
            step_info: StepInfo, next_fn: NextFn, ctx: dict[str, Any]
        ) -> StepResult[Any]:
            log.append("inner_before")
            result = await next_fn(ctx)
            log.append("inner_after")
            return result

        @step(provides=Output)
        async def my_step(ctx: dict) -> Output:  # type: ignore[type-arg]
            return Output(value=1)

        pipeline = Pipeline(
            name="test",
            steps=[my_step],
            middleware=[outer, inner],
        )
        result = await pipeline.run({})

        assert isinstance(result, AggregateSuccess)
        assert log == [
            "outer_before",
            "inner_before",
            "inner_after",
            "outer_after",
        ]

    async def test_middleware_can_short_circuit(self) -> None:
        from runsheet._internal import step_meta, step_success

        async def blocker(
            step_info: StepInfo, next_fn: NextFn, ctx: dict[str, Any]
        ) -> StepResult[Any]:
            # Short-circuit by returning a StepSuccess directly
            return step_success({"value": 42}, step_meta(step_info.name, ctx))

        @step(provides=Output)
        async def my_step(ctx: dict) -> Output:  # type: ignore[type-arg]
            raise RuntimeError("should not be called")

        pipeline = Pipeline(
            name="test",
            steps=[my_step],
            middleware=[blocker],
        )
        result = await pipeline.run({})
        assert isinstance(result, AggregateSuccess)
        assert result.data["value"] == 42

    async def test_middleware_timing(self) -> None:
        times: dict[str, float] = {}

        async def timing_mw(
            step_info: StepInfo, next_fn: NextFn, ctx: dict[str, Any]
        ) -> StepResult[Any]:
            start = time.perf_counter()
            result = await next_fn(ctx)
            times[step_info.name] = time.perf_counter() - start
            return result

        @step(provides=Output)
        async def my_step(ctx: dict) -> Output:  # type: ignore[type-arg]
            return Output(value=1)

        pipeline = Pipeline(
            name="test",
            steps=[my_step],
            middleware=[timing_mw],
        )
        await pipeline.run({})
        assert "my_step" in times
        assert times["my_step"] >= 0

    async def test_middleware_wraps_multiple_steps(self) -> None:
        call_count = 0

        async def counter_mw(
            step_info: StepInfo, next_fn: NextFn, ctx: dict[str, Any]
        ) -> StepResult[Any]:
            nonlocal call_count
            call_count += 1
            return await next_fn(ctx)

        @step(provides=Output)
        async def step_a(ctx: dict) -> Output:  # type: ignore[type-arg]
            return Output(value=1)

        class Output2(BaseModel):
            other: str

        @step(provides=Output2)
        async def step_b(ctx: dict) -> Output2:  # type: ignore[type-arg]
            return Output2(other="hello")

        pipeline = Pipeline(
            name="test",
            steps=[step_a, step_b],
            middleware=[counter_mw],
        )
        await pipeline.run({})
        assert call_count == 2

    async def test_middleware_sees_step_failure(self) -> None:
        """Middleware receives StepResult failures (not exceptions)."""
        saw_failure = False

        async def error_observer(
            step_info: StepInfo, next_fn: NextFn, ctx: dict[str, Any]
        ) -> StepResult[Any]:
            nonlocal saw_failure
            result = await next_fn(ctx)
            if not result.success:
                saw_failure = True
            return result

        @step(provides=Output)
        async def failing(ctx: dict) -> Output:  # type: ignore[type-arg]
            raise RuntimeError("boom")

        pipeline = Pipeline(
            name="test",
            steps=[failing],
            middleware=[error_observer],
        )
        result = await pipeline.run({})
        assert isinstance(result, AggregateFailure)
        assert saw_failure
