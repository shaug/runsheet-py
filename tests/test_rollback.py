"""Tests for rollback behavior."""

from typing import Any

from pydantic import BaseModel

from runsheet import AggregateFailure, Pipeline, step


class OutputA(BaseModel):
    a: str


class OutputB(BaseModel):
    b: str


class OutputC(BaseModel):
    c: str


class TestRollbackOrder:
    async def test_reverse_order(self) -> None:
        """Rollback handlers execute in reverse order (N-1...0)."""
        log: list[str] = []

        @step(provides=OutputA)
        async def step_a(ctx: dict) -> OutputA:  # type: ignore[type-arg]
            return OutputA(a="a")

        @step_a.rollback
        async def undo_a(ctx: dict, output: dict) -> None:  # type: ignore[type-arg]
            log.append("a")

        @step(provides=OutputB)
        async def step_b(ctx: dict) -> OutputB:  # type: ignore[type-arg]
            return OutputB(b="b")

        @step_b.rollback
        async def undo_b(ctx: dict, output: dict) -> None:  # type: ignore[type-arg]
            log.append("b")

        @step(provides=OutputC)
        async def step_c(ctx: dict) -> OutputC:  # type: ignore[type-arg]
            raise RuntimeError("c failed")

        pipeline = Pipeline(name="test", steps=[step_a, step_b, step_c])
        result = await pipeline.run({})

        assert isinstance(result, AggregateFailure)
        assert log == ["b", "a"]

    async def test_rollback_receives_correct_snapshots(self) -> None:
        """Each handler receives pre-step context and step output."""
        snapshots: list[tuple[dict[str, Any], dict[str, Any]]] = []

        @step(provides=OutputA)
        async def step_a(ctx: dict) -> OutputA:  # type: ignore[type-arg]
            return OutputA(a="value_a")

        @step_a.rollback
        async def undo_a(ctx: dict[str, Any], output: dict[str, Any]) -> None:
            snapshots.append((dict(ctx), dict(output)))

        _ = undo_a  # registered via decorator

        @step(provides=OutputB)
        async def step_b(ctx: dict) -> OutputB:  # type: ignore[type-arg]
            return OutputB(b="value_b")

        @step_b.rollback
        async def undo_b(ctx: dict[str, Any], output: dict[str, Any]) -> None:
            snapshots.append((dict(ctx), dict(output)))

        _ = undo_b  # registered via decorator

        @step(provides=OutputC)
        async def step_c(ctx: dict) -> OutputC:  # type: ignore[type-arg]
            raise RuntimeError("c failed")

        pipeline = Pipeline(name="test", steps=[step_a, step_b, step_c])
        result = await pipeline.run({"init": "data"})

        assert isinstance(result, AggregateFailure)
        # step_b rollback: pre-ctx has init+a, output is b
        b_ctx, b_out = snapshots[0]
        assert b_ctx.get("init") == "data"
        assert b_ctx.get("a") == "value_a"
        assert b_out.get("b") == "value_b"
        # step_a rollback: pre-ctx has init only, output is a
        a_ctx, a_out = snapshots[1]
        assert a_ctx.get("init") == "data"
        assert "a" not in a_ctx  # pre-step_a context doesn't have a
        assert a_out.get("a") == "value_a"

    async def test_no_rollback_for_failed_step(self) -> None:
        """The failing step itself does NOT get rolled back."""
        log: list[str] = []

        @step(provides=OutputA)
        async def step_a(ctx: dict) -> OutputA:  # type: ignore[type-arg]
            raise RuntimeError("a failed")

        @step_a.rollback
        async def undo_a(ctx: dict, output: dict) -> None:  # type: ignore[type-arg]
            log.append("a")

        pipeline = Pipeline(name="test", steps=[step_a])
        result = await pipeline.run({})

        assert isinstance(result, AggregateFailure)
        assert log == []  # step_a never succeeded, so no rollback

    async def test_steps_without_rollback_are_skipped(self) -> None:
        """Steps that don't have rollback handlers are silently skipped."""
        log: list[str] = []

        @step(provides=OutputA)
        async def step_a(ctx: dict) -> OutputA:  # type: ignore[type-arg]
            return OutputA(a="a")

        @step_a.rollback
        async def undo_a(ctx: dict, output: dict) -> None:  # type: ignore[type-arg]
            log.append("a")

        @step(provides=OutputB)
        async def step_b(ctx: dict) -> OutputB:  # type: ignore[type-arg]
            return OutputB(b="b")

        # step_b has no rollback

        @step(provides=OutputC)
        async def step_c(ctx: dict) -> OutputC:  # type: ignore[type-arg]
            raise RuntimeError("c failed")

        pipeline = Pipeline(name="test", steps=[step_a, step_b, step_c])
        result = await pipeline.run({})

        assert isinstance(result, AggregateFailure)
        assert log == ["a"]  # Only step_a's rollback runs

    async def test_rollback_error_collection(self) -> None:
        """Rollback errors are collected, not swallowed."""

        @step(provides=OutputA)
        async def step_a(ctx: dict) -> OutputA:  # type: ignore[type-arg]
            return OutputA(a="a")

        @step_a.rollback
        async def undo_a(ctx: dict, output: dict) -> None:  # type: ignore[type-arg]
            raise RuntimeError("rollback a failed")

        @step(provides=OutputB)
        async def step_b(ctx: dict) -> OutputB:  # type: ignore[type-arg]
            raise RuntimeError("b failed")

        pipeline = Pipeline(name="test", steps=[step_a, step_b])
        result = await pipeline.run({})

        assert isinstance(result, AggregateFailure)
        assert len(result.rollback.failed) == 1
        assert result.rollback.failed[0].step == "step_a"
