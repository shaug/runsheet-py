"""Tests for Pipeline class and execution engine."""

import asyncio
from typing import Any

import pytest
from pydantic import BaseModel

from runsheet import (
    AggregateFailure,
    AggregateMeta,
    AggregateSuccess,
    ArgsValidationError,
    Pipeline,
    ProvidesValidationError,
    RequiresValidationError,
    RetryExhaustedError,
    RetryPolicy,
    StepSuccess,
    StrictOverlapError,
    TimeoutError,
    step,
)


class OrderInput(BaseModel):
    order_id: str
    amount: float


class ValidatedOrder(BaseModel):
    validated: bool


class ChargeOutput(BaseModel):
    charge_id: str


class EmailOutput(BaseModel):
    email_sent: bool


@step(requires=OrderInput, provides=ValidatedOrder)
async def validate_order(ctx: OrderInput) -> ValidatedOrder:
    if ctx.amount <= 0:
        raise ValueError("Invalid amount")
    return ValidatedOrder(validated=True)


@step(requires=OrderInput, provides=ChargeOutput)
async def charge_payment(ctx: OrderInput) -> ChargeOutput:
    return ChargeOutput(charge_id=f"ch_{ctx.order_id}")


@step(provides=EmailOutput)
async def send_email(ctx: dict) -> EmailOutput:  # type: ignore[type-arg]
    return EmailOutput(email_sent=True)


class TestPipelineBasic:
    async def test_successful_pipeline(self) -> None:
        pipeline = Pipeline(
            name="checkout",
            steps=[validate_order, charge_payment, send_email],
            args_schema=OrderInput,
        )

        result = await pipeline.run(OrderInput(order_id="123", amount=50.0))
        assert isinstance(result, AggregateSuccess)
        assert result.success is True
        assert result.data["validated"] is True
        assert result.data["charge_id"] == "ch_123"
        assert result.data["email_sent"] is True
        assert result.meta.name == "checkout"
        assert isinstance(result.meta, AggregateMeta)
        assert result.meta.steps_executed == (
            "validate_order",
            "charge_payment",
            "send_email",
        )

    async def test_pipeline_never_raises(self) -> None:
        @step(requires=OrderInput, provides=ValidatedOrder)
        async def failing_step(ctx: OrderInput) -> ValidatedOrder:
            raise RuntimeError("boom")

        pipeline = Pipeline(
            name="test",
            steps=[failing_step],
            args_schema=OrderInput,
        )

        result = await pipeline.run(OrderInput(order_id="1", amount=10.0))
        assert isinstance(result, AggregateFailure)
        assert result.success is False
        assert isinstance(result.error, RuntimeError)

    async def test_args_persist_through_pipeline(self) -> None:
        """Initial args flow through the entire pipeline."""
        pipeline = Pipeline(
            name="test",
            steps=[validate_order, charge_payment],
            args_schema=OrderInput,
        )

        result = await pipeline.run(OrderInput(order_id="456", amount=99.0))
        assert isinstance(result, AggregateSuccess)
        assert result.data["order_id"] == "456"
        assert result.data["amount"] == 99.0

    async def test_context_accumulation(self) -> None:
        """Each step's output merges into context."""

        @step(provides=ValidatedOrder)
        async def step_a(ctx: dict) -> ValidatedOrder:  # type: ignore[type-arg]
            return ValidatedOrder(validated=True)

        @step(provides=ChargeOutput)
        async def step_b(ctx: dict) -> ChargeOutput:  # type: ignore[type-arg]
            return ChargeOutput(charge_id="ch_1")

        pipeline = Pipeline(name="test", steps=[step_a, step_b])
        result = await pipeline.run({})
        assert isinstance(result, AggregateSuccess)
        assert result.data["validated"] is True
        assert result.data["charge_id"] == "ch_1"

    async def test_empty_pipeline(self) -> None:
        pipeline = Pipeline(name="empty", steps=[])
        result = await pipeline.run({})
        assert isinstance(result, AggregateSuccess)

    async def test_no_args(self) -> None:
        @step(provides=ValidatedOrder)
        async def simple(ctx: dict) -> ValidatedOrder:  # type: ignore[type-arg]
            return ValidatedOrder(validated=True)

        pipeline = Pipeline(name="test", steps=[simple])
        result = await pipeline.run()
        assert isinstance(result, AggregateSuccess)


class TestArgsValidation:
    async def test_args_validation_failure(self) -> None:
        pipeline = Pipeline(
            name="test",
            steps=[validate_order],
            args_schema=OrderInput,
        )

        result = await pipeline.run({"bad": "data"})
        assert isinstance(result, AggregateFailure)
        assert isinstance(result.error, ArgsValidationError)
        assert result.failed_step == "<args>"


class TestRequiresValidation:
    async def test_requires_validation_failure(self) -> None:
        """Step fails if context doesn't match requires schema."""

        @step(requires=OrderInput, provides=ValidatedOrder)
        async def needs_order(ctx: OrderInput) -> ValidatedOrder:
            return ValidatedOrder(validated=True)

        pipeline = Pipeline(name="test", steps=[needs_order])
        result = await pipeline.run({"wrong_field": "data"})
        assert isinstance(result, AggregateFailure)
        assert isinstance(result.error, RequiresValidationError)


class TestProvidesValidation:
    async def test_provides_validation_failure(self) -> None:
        """Step fails if output doesn't match provides schema."""

        @step(provides=ChargeOutput)
        async def bad_output(ctx: dict[str, Any]) -> dict[str, Any]:
            return {"wrong_field": "data"}

        pipeline = Pipeline(name="test", steps=[bad_output])
        result = await pipeline.run({})
        assert isinstance(result, AggregateFailure)
        assert isinstance(result.error, ProvidesValidationError)


class TestRollback:
    async def test_rollback_on_failure(self) -> None:
        rollback_log: list[str] = []

        @step(provides=ValidatedOrder)
        async def step_a(ctx: dict) -> ValidatedOrder:  # type: ignore[type-arg]
            return ValidatedOrder(validated=True)

        @step_a.rollback
        async def undo_a(ctx: dict, output: dict) -> None:  # type: ignore[type-arg]
            rollback_log.append("a")

        @step(provides=ChargeOutput)
        async def step_b(ctx: dict) -> ChargeOutput:  # type: ignore[type-arg]
            return ChargeOutput(charge_id="ch_1")

        @step_b.rollback
        async def undo_b(ctx: dict, output: dict) -> None:  # type: ignore[type-arg]
            rollback_log.append("b")

        @step(provides=EmailOutput)
        async def step_c(ctx: dict) -> EmailOutput:  # type: ignore[type-arg]
            raise RuntimeError("step c failed")

        pipeline = Pipeline(name="test", steps=[step_a, step_b, step_c])
        result = await pipeline.run({})

        assert isinstance(result, AggregateFailure)
        assert result.failed_step == "step_c"
        # Rollback in reverse order: b, a
        assert rollback_log == ["b", "a"]
        assert result.rollback.completed == ("step_b", "step_a")

    async def test_rollback_best_effort(self) -> None:
        """If a rollback handler raises, remaining rollbacks still execute."""

        @step(provides=ValidatedOrder)
        async def step_a(ctx: dict) -> ValidatedOrder:  # type: ignore[type-arg]
            return ValidatedOrder(validated=True)

        rollback_a_called = False

        @step_a.rollback
        async def undo_a(ctx: dict, output: dict) -> None:  # type: ignore[type-arg]
            nonlocal rollback_a_called
            rollback_a_called = True

        @step(provides=ChargeOutput)
        async def step_b(ctx: dict) -> ChargeOutput:  # type: ignore[type-arg]
            return ChargeOutput(charge_id="ch_1")

        @step_b.rollback
        async def undo_b(ctx: dict, output: dict) -> None:  # type: ignore[type-arg]
            raise RuntimeError("rollback b failed")

        @step(provides=EmailOutput)
        async def step_c(ctx: dict) -> EmailOutput:  # type: ignore[type-arg]
            raise RuntimeError("step c failed")

        pipeline = Pipeline(name="test", steps=[step_a, step_b, step_c])
        result = await pipeline.run({})

        assert isinstance(result, AggregateFailure)
        assert rollback_a_called
        assert len(result.rollback.failed) == 1
        assert result.rollback.failed[0].step == "step_b"


class TestRetry:
    async def test_retry_on_failure(self) -> None:
        call_count = 0

        @step(
            provides=ChargeOutput,
            retry=RetryPolicy(count=2, delay=0),
        )
        async def flaky(ctx: dict) -> ChargeOutput:  # type: ignore[type-arg]
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise RuntimeError("transient")
            return ChargeOutput(charge_id="ch_1")

        pipeline = Pipeline(name="test", steps=[flaky])
        result = await pipeline.run({})
        assert isinstance(result, AggregateSuccess)
        assert call_count == 3

    async def test_retry_exhausted(self) -> None:
        @step(
            provides=ChargeOutput,
            retry=RetryPolicy(count=2, delay=0),
        )
        async def always_fails(ctx: dict) -> ChargeOutput:  # type: ignore[type-arg]
            raise RuntimeError("permanent")

        pipeline = Pipeline(name="test", steps=[always_fails])
        result = await pipeline.run({})
        assert isinstance(result, AggregateFailure)
        assert isinstance(result.error, RetryExhaustedError)
        assert result.error.attempts == 3

    async def test_retry_if_predicate(self) -> None:
        call_count = 0

        @step(
            provides=ChargeOutput,
            retry=RetryPolicy(
                count=5,
                delay=0,
                retry_if=lambda errors: "transient" in str(errors[-1]),
            ),
        )
        async def selective(ctx: dict) -> ChargeOutput:  # type: ignore[type-arg]
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("transient")
            raise RuntimeError("permanent")

        pipeline = Pipeline(name="test", steps=[selective])
        result = await pipeline.run({})
        assert isinstance(result, AggregateFailure)
        assert call_count == 2


class TestTimeout:
    async def test_timeout(self) -> None:
        @step(provides=ChargeOutput, timeout=0.05)
        async def slow(ctx: dict) -> ChargeOutput:  # type: ignore[type-arg]
            await asyncio.sleep(10)
            return ChargeOutput(charge_id="ch_1")

        pipeline = Pipeline(name="test", steps=[slow])
        result = await pipeline.run({})
        assert isinstance(result, AggregateFailure)
        assert isinstance(result.error, TimeoutError)
        assert result.error.timeout_seconds == 0.05

    async def test_timeout_per_retry_attempt(self) -> None:
        call_count = 0

        @step(
            provides=ChargeOutput,
            retry=RetryPolicy(count=1, delay=0),
            timeout=0.05,
        )
        async def slow_retry(ctx: dict) -> ChargeOutput:  # type: ignore[type-arg]
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(10)
            return ChargeOutput(charge_id="ch_1")

        pipeline = Pipeline(name="test", steps=[slow_retry])
        result = await pipeline.run({})
        assert isinstance(result, AggregateFailure)
        assert call_count == 2


class TestStrictMode:
    def test_strict_detects_overlap(self) -> None:
        @step(provides=ChargeOutput)
        async def step_a(ctx: dict) -> ChargeOutput:  # type: ignore[type-arg]
            return ChargeOutput(charge_id="a")

        @step(provides=ChargeOutput)
        async def step_b(ctx: dict) -> ChargeOutput:  # type: ignore[type-arg]
            return ChargeOutput(charge_id="b")

        with pytest.raises(StrictOverlapError) as exc_info:
            Pipeline(name="test", steps=[step_a, step_b], strict=True)
        assert exc_info.value.key == "charge_id"
        assert exc_info.value.steps == ("step_a", "step_b")

    async def test_non_strict_allows_overlap(self) -> None:
        @step(provides=ChargeOutput)
        async def step_a(ctx: dict) -> ChargeOutput:  # type: ignore[type-arg]
            return ChargeOutput(charge_id="a")

        @step(provides=ChargeOutput)
        async def step_b(ctx: dict) -> ChargeOutput:  # type: ignore[type-arg]
            return ChargeOutput(charge_id="b")

        pipeline = Pipeline(name="test", steps=[step_a, step_b], strict=False)
        result = await pipeline.run({})
        assert isinstance(result, AggregateSuccess)
        assert result.data["charge_id"] == "b"


class TestSyncSteps:
    async def test_sync_run_function(self) -> None:
        @step(provides=ValidatedOrder)
        def sync_step(ctx: dict) -> ValidatedOrder:  # type: ignore[type-arg]
            return ValidatedOrder(validated=True)

        pipeline = Pipeline(name="test", steps=[sync_step])
        result = await pipeline.run({})
        assert isinstance(result, AggregateSuccess)
        assert result.data["validated"] is True


class TestPipelineComposition:
    """Pipeline IS a step — it can be used in another pipeline's steps."""

    async def test_nested_pipeline_success(self) -> None:
        """Inner pipeline's outputs merge into outer context."""

        @step(provides=ValidatedOrder)
        async def step_a(ctx: dict) -> ValidatedOrder:  # type: ignore[type-arg]
            return ValidatedOrder(validated=True)

        @step(provides=ChargeOutput)
        async def step_b(ctx: dict) -> ChargeOutput:  # type: ignore[type-arg]
            return ChargeOutput(charge_id="ch_1")

        inner = Pipeline(name="inner", steps=[step_a, step_b])

        @step(provides=EmailOutput)
        async def step_c(ctx: dict[str, Any]) -> EmailOutput:
            assert ctx.get("validated") is True
            assert ctx.get("charge_id") == "ch_1"
            return EmailOutput(email_sent=True)

        outer = Pipeline(
            name="outer",
            steps=[inner, step_c],
        )
        result = await outer.run({})
        assert isinstance(result, AggregateSuccess)
        assert result.data["validated"] is True
        assert result.data["charge_id"] == "ch_1"
        assert result.data["email_sent"] is True

    async def test_nested_pipeline_rollback(self) -> None:
        """Outer step failure rolls back inner pipeline's steps."""
        rollback_log: list[str] = []

        @step(provides=ValidatedOrder)
        async def step_a(ctx: dict) -> ValidatedOrder:  # type: ignore[type-arg]
            return ValidatedOrder(validated=True)

        @step_a.rollback
        async def undo_a(ctx: dict, output: dict) -> None:  # type: ignore[type-arg]
            rollback_log.append("a")

        @step(provides=ChargeOutput)
        async def step_b(ctx: dict) -> ChargeOutput:  # type: ignore[type-arg]
            return ChargeOutput(charge_id="ch_1")

        @step_b.rollback
        async def undo_b(ctx: dict, output: dict) -> None:  # type: ignore[type-arg]
            rollback_log.append("b")

        inner = Pipeline(name="inner", steps=[step_a, step_b])

        @step(provides=EmailOutput)
        async def failing_step(ctx: dict) -> EmailOutput:  # type: ignore[type-arg]
            raise RuntimeError("outer step failed")

        outer = Pipeline(
            name="outer",
            steps=[inner, failing_step],
        )
        result = await outer.run({})
        assert isinstance(result, AggregateFailure)
        assert "b" in rollback_log
        assert "a" in rollback_log

    async def test_inner_pipeline_failure_propagates(self) -> None:
        """If inner pipeline fails, outer pipeline stops."""

        @step(provides=ValidatedOrder)
        async def step_a(ctx: dict) -> ValidatedOrder:  # type: ignore[type-arg]
            raise RuntimeError("inner failure")

        inner = Pipeline(name="inner", steps=[step_a])

        @step(provides=EmailOutput)
        async def step_b(ctx: dict) -> EmailOutput:  # type: ignore[type-arg]
            return EmailOutput(email_sent=True)

        outer = Pipeline(
            name="outer",
            steps=[inner, step_b],
        )
        result = await outer.run({})
        assert isinstance(result, AggregateFailure)


class TestTypedOutput:
    async def test_output_returns_model_instance(self) -> None:
        """Pipeline with output= returns a typed model, not a dict."""

        class CheckoutOutput(BaseModel):
            validated: bool
            charge_id: str
            email_sent: bool

        pipeline = Pipeline(
            name="checkout",
            steps=[validate_order, charge_payment, send_email],
            args_schema=OrderInput,
            output=CheckoutOutput,
        )
        result = await pipeline.run(OrderInput(order_id="123", amount=50.0))
        assert isinstance(result, StepSuccess)
        assert isinstance(result.data, CheckoutOutput)
        assert result.data.charge_id == "ch_123"
        assert result.data.email_sent is True
        assert result.data.validated is True

    async def test_output_validation_failure(self) -> None:
        """Pipeline with output= fails if accumulated context doesn't match."""

        class StrictOutput(BaseModel):
            nonexistent_field: str

        pipeline = Pipeline(
            name="test",
            steps=[validate_order],
            args_schema=OrderInput,
            output=StrictOutput,
        )
        result = await pipeline.run(OrderInput(order_id="1", amount=10.0))
        assert isinstance(result, AggregateFailure)
        assert isinstance(result.error, ProvidesValidationError)
        assert result.failed_step == "<output>"

    async def test_without_output_returns_dict(self) -> None:
        """Pipeline without output= still returns dict[str, Any]."""
        pipeline = Pipeline(
            name="test",
            steps=[validate_order, charge_payment],
            args_schema=OrderInput,
        )
        result = await pipeline.run(OrderInput(order_id="1", amount=10.0))
        assert isinstance(result, AggregateSuccess)
        assert isinstance(result.data, dict)
        assert result.data["charge_id"] == "ch_1"

    async def test_output_with_nested_pipeline(self) -> None:
        """Outer pipeline with output= validates after inner pipeline runs."""

        class FullOutput(BaseModel):
            validated: bool
            charge_id: str

        inner = Pipeline(name="inner", steps=[validate_order, charge_payment])
        outer = Pipeline(
            name="outer",
            steps=[inner],
            args_schema=OrderInput,
            output=FullOutput,
        )
        result = await outer.run(OrderInput(order_id="42", amount=100.0))
        assert isinstance(result, StepSuccess)
        assert isinstance(result.data, FullOutput)
        assert result.data.charge_id == "ch_42"
        assert result.data.validated is True
