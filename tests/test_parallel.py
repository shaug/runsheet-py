"""Tests for parallel steps."""

import asyncio
from typing import Any

from pydantic import BaseModel

from runsheet import (
    AggregateFailure,
    AggregateSuccess,
    Pipeline,
    parallel,
    step,
)


class ChargeOutput(BaseModel):
    charge_id: str


class ReservationOutput(BaseModel):
    reservation_id: str


class EmailOutput(BaseModel):
    email_sent: bool


class TestParallel:
    async def test_parallel_runs_concurrently(self) -> None:
        started: list[str] = []

        @step(provides=ChargeOutput)
        async def charge(ctx: dict) -> ChargeOutput:  # type: ignore[type-arg]
            started.append("charge")
            await asyncio.sleep(0.01)
            return ChargeOutput(charge_id="ch_1")

        @step(provides=ReservationOutput)
        async def reserve(ctx: dict) -> ReservationOutput:  # type: ignore[type-arg]
            started.append("reserve")
            await asyncio.sleep(0.01)
            return ReservationOutput(reservation_id="res_1")

        pipeline = Pipeline(
            name="test",
            steps=[parallel(charge, reserve)],
        )
        result = await pipeline.run({})

        assert isinstance(result, AggregateSuccess)
        assert result.data["charge_id"] == "ch_1"
        assert result.data["reservation_id"] == "res_1"
        # Both should have started (concurrency)
        assert len(started) == 2

    async def test_parallel_outputs_merge_in_order(self) -> None:
        @step(provides=ChargeOutput)
        async def charge(ctx: dict) -> ChargeOutput:  # type: ignore[type-arg]
            await asyncio.sleep(0.02)
            return ChargeOutput(charge_id="ch_1")

        @step(provides=ReservationOutput)
        async def reserve(ctx: dict) -> ReservationOutput:  # type: ignore[type-arg]
            return ReservationOutput(reservation_id="res_1")

        pipeline = Pipeline(
            name="test",
            steps=[parallel(charge, reserve)],
        )
        result = await pipeline.run({})
        assert isinstance(result, AggregateSuccess)
        assert result.data["charge_id"] == "ch_1"
        assert result.data["reservation_id"] == "res_1"

    async def test_parallel_partial_failure_rollback(self) -> None:
        """On partial failure, succeeded inner steps are rolled back."""
        rollback_log: list[str] = []

        @step(provides=ChargeOutput)
        async def charge(ctx: dict) -> ChargeOutput:  # type: ignore[type-arg]
            return ChargeOutput(charge_id="ch_1")

        @charge.rollback
        async def undo_charge(ctx: dict, output: dict) -> None:  # type: ignore[type-arg]
            rollback_log.append("charge")

        @step(provides=ReservationOutput)
        async def reserve(ctx: dict) -> ReservationOutput:  # type: ignore[type-arg]
            raise RuntimeError("reservation failed")

        pipeline = Pipeline(
            name="test",
            steps=[parallel(charge, reserve)],
        )
        result = await pipeline.run({})
        assert isinstance(result, AggregateFailure)
        # charge succeeded, so it should be rolled back
        assert "charge" in rollback_log

    async def test_parallel_all_receive_same_context(self) -> None:
        """All inner steps receive the pre-parallel context."""
        seen_ctxs: list[dict[str, Any]] = []

        @step(provides=ChargeOutput)
        async def charge(ctx: dict[str, Any]) -> ChargeOutput:
            seen_ctxs.append(dict(ctx))
            return ChargeOutput(charge_id="ch_1")

        @step(provides=ReservationOutput)
        async def reserve(ctx: dict[str, Any]) -> ReservationOutput:
            seen_ctxs.append(dict(ctx))
            return ReservationOutput(reservation_id="res_1")

        pipeline = Pipeline(
            name="test",
            steps=[parallel(charge, reserve)],
        )
        await pipeline.run({"order_id": "123"})
        # Both should see the same initial context
        assert all(c.get("order_id") == "123" for c in seen_ctxs)

    async def test_parallel_in_sequence(self) -> None:
        """Parallel step works within a larger sequential pipeline."""

        class InitOutput(BaseModel):
            initialized: bool

        @step(provides=InitOutput)
        async def init_step(ctx: dict) -> InitOutput:  # type: ignore[type-arg]
            return InitOutput(initialized=True)

        @step(provides=ChargeOutput)
        async def charge(ctx: dict) -> ChargeOutput:  # type: ignore[type-arg]
            return ChargeOutput(charge_id="ch_1")

        @step(provides=ReservationOutput)
        async def reserve(ctx: dict) -> ReservationOutput:  # type: ignore[type-arg]
            return ReservationOutput(reservation_id="res_1")

        @step(provides=EmailOutput)
        async def email(ctx: dict) -> EmailOutput:  # type: ignore[type-arg]
            return EmailOutput(email_sent=True)

        pipeline = Pipeline(
            name="test",
            steps=[
                init_step,
                parallel(charge, reserve),
                email,
            ],
        )
        result = await pipeline.run({})
        assert isinstance(result, AggregateSuccess)
        assert result.data["initialized"] is True
        assert result.data["charge_id"] == "ch_1"
        assert result.data["reservation_id"] == "res_1"
        assert result.data["email_sent"] is True
