"""Tests for choice (branching)."""

from typing import Any

from pydantic import BaseModel

from runsheet import (
    AggregateFailure,
    AggregateSuccess,
    ChoiceNoMatchError,
    Pipeline,
    PredicateError,
    choice,
    step,
)


class CardOutput(BaseModel):
    charge_id: str
    method: str


class BankOutput(BaseModel):
    charge_id: str
    method: str


class DefaultOutput(BaseModel):
    charge_id: str
    method: str


class TestChoice:
    async def test_first_match_wins(self) -> None:
        @step(provides=CardOutput)
        async def charge_card(ctx: dict) -> CardOutput:  # type: ignore[type-arg]
            return CardOutput(charge_id="card_1", method="card")

        @step(provides=BankOutput)
        async def charge_bank(ctx: dict) -> BankOutput:  # type: ignore[type-arg]
            return BankOutput(charge_id="bank_1", method="bank")

        pipeline = Pipeline(
            name="test",
            steps=[
                choice(
                    (lambda ctx: ctx.get("method") == "card", charge_card),
                    (lambda ctx: ctx.get("method") == "bank", charge_bank),
                ),
            ],
        )

        result = await pipeline.run({"method": "card"})
        assert isinstance(result, AggregateSuccess)
        assert result.data["method"] == "card"
        assert result.data["charge_id"] == "card_1"

    async def test_second_branch_matches(self) -> None:
        @step(provides=CardOutput)
        async def charge_card(ctx: dict) -> CardOutput:  # type: ignore[type-arg]
            return CardOutput(charge_id="card_1", method="card")

        @step(provides=BankOutput)
        async def charge_bank(ctx: dict) -> BankOutput:  # type: ignore[type-arg]
            return BankOutput(charge_id="bank_1", method="bank")

        pipeline = Pipeline(
            name="test",
            steps=[
                choice(
                    (lambda ctx: ctx.get("method") == "card", charge_card),
                    (lambda ctx: ctx.get("method") == "bank", charge_bank),
                ),
            ],
        )

        result = await pipeline.run({"method": "bank"})
        assert isinstance(result, AggregateSuccess)
        assert result.data["method"] == "bank"

    async def test_default_branch(self) -> None:
        """Bare step as last arg = default."""

        @step(provides=CardOutput)
        async def charge_card(ctx: dict) -> CardOutput:  # type: ignore[type-arg]
            return CardOutput(charge_id="card_1", method="card")

        @step(provides=DefaultOutput)
        async def charge_default(ctx: dict) -> DefaultOutput:  # type: ignore[type-arg]
            return DefaultOutput(charge_id="default_1", method="default")

        pipeline = Pipeline(
            name="test",
            steps=[
                choice(
                    (lambda ctx: ctx.get("method") == "card", charge_card),
                    charge_default,  # bare step = default
                ),
            ],
        )

        result = await pipeline.run({"method": "crypto"})
        assert isinstance(result, AggregateSuccess)
        assert result.data["method"] == "default"

    async def test_no_match_error(self) -> None:
        @step(provides=CardOutput)
        async def charge_card(ctx: dict) -> CardOutput:  # type: ignore[type-arg]
            return CardOutput(charge_id="card_1", method="card")

        pipeline = Pipeline(
            name="test",
            steps=[
                choice(
                    (lambda ctx: ctx.get("method") == "card", charge_card),
                ),
            ],
        )

        result = await pipeline.run({"method": "crypto"})
        assert isinstance(result, AggregateFailure)
        assert isinstance(result.error, ChoiceNoMatchError)

    async def test_only_matched_branch_in_rollback(self) -> None:
        """Only the matched branch participates in rollback."""
        rollback_log: list[str] = []

        @step(provides=CardOutput)
        async def charge_card(ctx: dict) -> CardOutput:  # type: ignore[type-arg]
            return CardOutput(charge_id="card_1", method="card")

        @charge_card.rollback
        async def undo_card(ctx: dict, output: dict) -> None:  # type: ignore[type-arg]
            rollback_log.append("card")

        @step(provides=BankOutput)
        async def charge_bank(ctx: dict) -> BankOutput:  # type: ignore[type-arg]
            return BankOutput(charge_id="bank_1", method="bank")

        @charge_bank.rollback
        async def undo_bank(ctx: dict, output: dict) -> None:  # type: ignore[type-arg]
            rollback_log.append("bank")

        class FailOutput(BaseModel):
            done: bool

        @step(provides=FailOutput)
        async def failing(ctx: dict) -> FailOutput:  # type: ignore[type-arg]
            raise RuntimeError("boom")

        pipeline = Pipeline(
            name="test",
            steps=[
                choice(
                    (lambda ctx: ctx.get("method") == "card", charge_card),
                    (lambda ctx: ctx.get("method") == "bank", charge_bank),
                ),
                failing,
            ],
        )

        result = await pipeline.run({"method": "card"})
        assert isinstance(result, AggregateFailure)
        # Only card should be rolled back (it was the matched branch)
        assert rollback_log == ["card"]

    async def test_predicate_error(self) -> None:
        @step(provides=CardOutput)
        async def charge_card(ctx: dict) -> CardOutput:  # type: ignore[type-arg]
            return CardOutput(charge_id="card_1", method="card")

        def bad_pred(ctx: dict[str, Any]) -> bool:
            raise RuntimeError("pred exploded")

        pipeline = Pipeline(
            name="test",
            steps=[choice((bad_pred, charge_card))],
        )

        result = await pipeline.run({})
        assert isinstance(result, AggregateFailure)
        assert isinstance(result.error, PredicateError)
