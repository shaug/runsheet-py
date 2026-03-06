"""Tests for distribute (cross-product collection combinator)."""

import asyncio
from typing import Any

from pydantic import BaseModel

from runsheet import (
    AggregateFailure,
    AggregateSuccess,
    Pipeline,
    RollbackError,
    distribute,
    step,
)


class EmailOutput(BaseModel):
    email_id: str


class ReportOutput(BaseModel):
    key: str


class DoneOutput(BaseModel):
    done: bool


class OutOutput(BaseModel):
    out: str


class OkOutput(BaseModel):
    ok: bool


class SetupOutput(BaseModel):
    org_id: str
    account_ids: list[str]


# ---------------------------------------------------------------------------
# Single mapping
# ---------------------------------------------------------------------------


class TestSingleMapping:
    async def test_runs_step_once_per_item(self) -> None:
        @step(provides=EmailOutput)
        async def send_email(ctx: dict[str, Any]) -> EmailOutput:
            return EmailOutput(email_id=f"email-{ctx['account_id']}")

        pipeline = Pipeline(
            name="test",
            steps=[distribute("emails", {"account_ids": "account_id"}, send_email)],
        )

        result = await pipeline.run(
            {"org_id": "org-1", "account_ids": ["a1", "a2", "a3"]}
        )
        assert isinstance(result, AggregateSuccess)
        assert result.data["emails"] == [
            {"email_id": "email-a1"},
            {"email_id": "email-a2"},
            {"email_id": "email-a3"},
        ]

    async def test_passes_non_mapped_context(self) -> None:
        received: list[dict[str, Any]] = []

        @step(provides=OkOutput)
        async def capture(ctx: dict[str, Any]) -> OkOutput:
            received.append({"account_id": ctx["account_id"], "org_id": ctx["org_id"]})
            return OkOutput(ok=True)

        pipeline = Pipeline(
            name="test",
            steps=[distribute("results", {"account_ids": "account_id"}, capture)],
        )

        await pipeline.run({"org_id": "org-1", "account_ids": ["a1", "a2"]})
        assert sorted(received, key=lambda d: d["account_id"]) == [
            {"account_id": "a1", "org_id": "org-1"},
            {"account_id": "a2", "org_id": "org-1"},
        ]


# ---------------------------------------------------------------------------
# Cross-product
# ---------------------------------------------------------------------------


class TestCrossProduct:
    async def test_runs_step_once_per_combination(self) -> None:
        received: list[str] = []

        @step(provides=ReportOutput)
        async def report(ctx: dict[str, Any]) -> ReportOutput:
            key = f"{ctx['account_id']}-{ctx['region_id']}"
            received.append(key)
            return ReportOutput(key=key)

        pipeline = Pipeline(
            name="test",
            steps=[
                distribute(
                    "reports",
                    {"account_ids": "account_id", "region_ids": "region_id"},
                    report,
                )
            ],
        )

        result = await pipeline.run(
            {"account_ids": ["a1", "a2"], "region_ids": ["r1", "r2", "r3"]}
        )
        assert isinstance(result, AggregateSuccess)
        # 2 accounts x 3 regions = 6 combinations
        assert len(result.data["reports"]) == 6
        assert sorted(received) == [
            "a1-r1",
            "a1-r2",
            "a1-r3",
            "a2-r1",
            "a2-r2",
            "a2-r3",
        ]


# ---------------------------------------------------------------------------
# Empty collections
# ---------------------------------------------------------------------------


class TestEmptyCollections:
    async def test_empty_collection_returns_empty(self) -> None:
        @step(provides=EmailOutput)
        async def send_email(ctx: dict[str, Any]) -> EmailOutput:
            return EmailOutput(email_id=f"email-{ctx['account_id']}")

        pipeline = Pipeline(
            name="test",
            steps=[distribute("emails", {"account_ids": "account_id"}, send_email)],
        )

        result = await pipeline.run({"org_id": "org-1", "account_ids": []})
        assert isinstance(result, AggregateSuccess)
        assert result.data["emails"] == []

    async def test_any_empty_cross_product_returns_empty(self) -> None:
        @step(provides=ReportOutput)
        async def report(ctx: dict[str, Any]) -> ReportOutput:
            return ReportOutput(key=f"{ctx['account_id']}-{ctx['region_id']}")

        pipeline = Pipeline(
            name="test",
            steps=[
                distribute(
                    "reports",
                    {"account_ids": "account_id", "region_ids": "region_id"},
                    report,
                )
            ],
        )

        result = await pipeline.run({"account_ids": ["a1"], "region_ids": []})
        assert isinstance(result, AggregateSuccess)
        assert result.data["reports"] == []


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------


class TestConcurrency:
    async def test_runs_items_concurrently(self) -> None:
        running: list[str] = []
        max_concurrent = 0

        @step(provides=DoneOutput)
        async def slow(ctx: dict[str, Any]) -> DoneOutput:
            nonlocal max_concurrent
            running.append(ctx["account_id"])
            max_concurrent = max(max_concurrent, len(running))
            await asyncio.sleep(0.02)
            running.remove(ctx["account_id"])
            return DoneOutput(done=True)

        pipeline = Pipeline(
            name="test",
            steps=[distribute("results", {"account_ids": "account_id"}, slow)],
        )

        await pipeline.run({"account_ids": ["a1", "a2", "a3"]})
        assert max_concurrent > 1


# ---------------------------------------------------------------------------
# Partial failure rollback
# ---------------------------------------------------------------------------


class TestPartialFailureRollback:
    async def test_rolls_back_succeeded_items(self) -> None:
        rolled_back: list[str] = []

        @step(provides=OutOutput)
        async def flaky(ctx: dict[str, Any]) -> OutOutput:
            if ctx["account_id"] == "a2":
                raise RuntimeError("a2 boom")
            await asyncio.sleep(0.02)
            return OutOutput(out=ctx["account_id"])

        @flaky.rollback
        async def _undo_flaky(  # pyright: ignore[reportUnusedFunction]
            ctx: dict[str, Any], output: dict[str, Any]
        ) -> None:
            rolled_back.append(ctx["account_id"])

        pipeline = Pipeline(
            name="test",
            steps=[distribute("results", {"account_ids": "account_id"}, flaky)],
        )

        result = await pipeline.run({"account_ids": ["a1", "a2", "a3"]})
        assert isinstance(result, AggregateFailure)
        assert sorted(rolled_back) == ["a1", "a3"]


# ---------------------------------------------------------------------------
# External failure rollback
# ---------------------------------------------------------------------------


class TestExternalFailureRollback:
    async def test_rolls_back_all_items(self) -> None:
        rolled_back: list[str] = []

        @step(provides=OutOutput)
        async def dist_step(ctx: dict[str, Any]) -> OutOutput:
            return OutOutput(out=ctx["account_id"])

        @dist_step.rollback
        async def _undo_dist(  # pyright: ignore[reportUnusedFunction]
            ctx: dict[str, Any], output: dict[str, Any]
        ) -> None:
            rolled_back.append(ctx["account_id"])

        @step(provides=DoneOutput)
        async def later_fails(ctx: dict[str, Any]) -> DoneOutput:
            raise RuntimeError("later fail")

        pipeline = Pipeline(
            name="test",
            steps=[
                distribute("results", {"account_ids": "account_id"}, dist_step),
                later_fails,
            ],
        )

        result = await pipeline.run({"account_ids": ["a1", "a2", "a3"]})
        assert isinstance(result, AggregateFailure)
        assert sorted(rolled_back) == ["a1", "a2", "a3"]

    async def test_reports_rollback_errors(self) -> None:
        @step(provides=OutOutput)
        async def dist_step(ctx: dict[str, Any]) -> OutOutput:
            return OutOutput(out=ctx["account_id"])

        @dist_step.rollback
        async def _undo_dist(  # pyright: ignore[reportUnusedFunction]
            ctx: dict[str, Any], output: dict[str, Any]
        ) -> None:
            raise RuntimeError("rollback boom")

        @step(provides=DoneOutput)
        async def later_fails(ctx: dict[str, Any]) -> DoneOutput:
            raise RuntimeError("later fail")

        pipeline = Pipeline(
            name="test",
            steps=[
                distribute("results", {"account_ids": "account_id"}, dist_step),
                later_fails,
            ],
        )

        result = await pipeline.run({"account_ids": ["a1", "a2"]})
        assert isinstance(result, AggregateFailure)
        assert len(result.rollback.failed) == 1
        assert isinstance(result.rollback.failed[0].error, RollbackError)


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------


class TestMetadata:
    async def test_descriptive_step_name(self) -> None:
        @step(provides=EmailOutput)
        async def send_email(ctx: dict[str, Any]) -> EmailOutput:
            return EmailOutput(email_id=f"email-{ctx['account_id']}")

        pipeline = Pipeline(
            name="test",
            steps=[distribute("emails", {"account_ids": "account_id"}, send_email)],
        )

        result = await pipeline.run({"org_id": "org-1", "account_ids": ["a1"]})
        assert isinstance(result, AggregateSuccess)
        assert "distribute(emails, send_email)" in result.meta.steps_executed  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# Step without rollback
# ---------------------------------------------------------------------------


class TestNoRollback:
    async def test_succeeds_without_rollback(self) -> None:
        @step(provides=OutOutput)
        async def no_rollback(ctx: dict[str, Any]) -> OutOutput:
            return OutOutput(out=ctx["account_id"])

        pipeline = Pipeline(
            name="test",
            steps=[distribute("results", {"account_ids": "account_id"}, no_rollback)],
        )

        result = await pipeline.run({"account_ids": ["a1", "a2"]})
        assert isinstance(result, AggregateSuccess)
        assert result.data["results"] == [{"out": "a1"}, {"out": "a2"}]


# ---------------------------------------------------------------------------
# Pipeline integration
# ---------------------------------------------------------------------------


class TestPipelineIntegration:
    async def test_works_with_setup_step(self) -> None:
        @step(provides=SetupOutput)
        async def setup(ctx: dict[str, Any]) -> SetupOutput:
            return SetupOutput(org_id="org-1", account_ids=["a1", "a2"])

        @step(provides=EmailOutput)
        async def send_email(ctx: dict[str, Any]) -> EmailOutput:
            return EmailOutput(email_id=f"email-{ctx['account_id']}")

        pipeline = Pipeline(
            name="test",
            steps=[
                setup,
                distribute("emails", {"account_ids": "account_id"}, send_email),
            ],
        )

        result = await pipeline.run({})
        assert isinstance(result, AggregateSuccess)
        assert result.data["emails"] == [
            {"email_id": "email-a1"},
            {"email_id": "email-a2"},
        ]
