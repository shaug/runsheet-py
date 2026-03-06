"""Tests for step decorator and Step class."""

from pydantic import BaseModel

from runsheet import RetryPolicy, Step, step


class Input(BaseModel):
    value: int


class Output(BaseModel):
    result: int


class TestStepDecorator:
    async def test_basic_async_step(self) -> None:
        @step(requires=Input, provides=Output)
        async def double(ctx: Input) -> Output:
            return Output(result=ctx.value * 2)

        assert isinstance(double, Step)
        assert double.name == "double"
        assert double.requires is Input
        assert double.provides is Output

    async def test_sync_step(self) -> None:
        @step(requires=Input, provides=Output)
        def double(ctx: Input) -> Output:
            return Output(result=ctx.value * 2)

        result = await double.run(Input(value=5))
        assert isinstance(result, Output)
        assert result.result == 10

    async def test_async_step_run(self) -> None:
        @step(requires=Input, provides=Output)
        async def double(ctx: Input) -> Output:
            return Output(result=ctx.value * 2)

        result = await double.run(Input(value=5))
        assert result.result == 10

    def test_name_override(self) -> None:
        @step(name="custom_name", provides=Output)
        async def my_step(ctx: dict) -> Output:  # type: ignore[type-arg]
            return Output(result=1)

        assert my_step.name == "custom_name"

    def test_name_inferred_from_function(self) -> None:
        @step(provides=Output)
        async def compute(ctx: dict) -> Output:  # type: ignore[type-arg]
            return Output(result=1)

        assert compute.name == "compute"


class TestRollback:
    async def test_rollback_decorator(self) -> None:
        @step(requires=Input, provides=Output)
        async def charge(ctx: Input) -> Output:
            return Output(result=ctx.value)

        rollback_called = False

        @charge.rollback
        async def undo_charge(ctx: Input, output: Output) -> None:
            nonlocal rollback_called
            rollback_called = True

        assert charge.has_rollback is True
        await charge.run_rollback(Input(value=1), Output(result=1))
        assert rollback_called

    async def test_no_rollback(self) -> None:
        @step(provides=Output)
        async def simple(ctx: dict) -> Output:  # type: ignore[type-arg]
            return Output(result=1)

        assert simple.has_rollback is False
        await simple.run_rollback({}, {})  # should be a no-op

    async def test_sync_rollback(self) -> None:
        @step(requires=Input, provides=Output)
        async def charge(ctx: Input) -> Output:
            return Output(result=ctx.value)

        rollback_called = False

        @charge.rollback
        def undo_charge(ctx: Input, output: Output) -> None:
            nonlocal rollback_called
            rollback_called = True

        await charge.run_rollback(Input(value=1), Output(result=1))
        assert rollback_called


class TestRetryPolicy:
    def test_defaults(self) -> None:
        policy = RetryPolicy(count=3)
        assert policy.count == 3
        assert policy.delay == 0.0
        assert policy.backoff == "linear"
        assert policy.retry_if is None

    def test_custom(self) -> None:
        policy = RetryPolicy(
            count=5,
            delay=0.1,
            backoff="exponential",
            retry_if=lambda errors: True,
        )
        assert policy.count == 5
        assert policy.delay == 0.1
        assert policy.backoff == "exponential"
        assert policy.retry_if is not None

    def test_step_with_retry(self) -> None:
        @step(
            requires=Input,
            provides=Output,
            retry=RetryPolicy(count=3, delay=0.1),
        )
        async def flaky(ctx: Input) -> Output:
            return Output(result=ctx.value)

        assert flaky.retry is not None
        assert flaky.retry.count == 3

    def test_step_with_timeout(self) -> None:
        @step(requires=Input, provides=Output, timeout=5.0)
        async def slow(ctx: Input) -> Output:
            return Output(result=ctx.value)

        assert slow.timeout == 5.0
