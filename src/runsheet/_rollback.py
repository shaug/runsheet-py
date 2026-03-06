"""Rollback execution.

On failure at step N, rollback handlers for steps N-1...0 execute in reverse
order. Rollback is best-effort: if a handler raises, remaining rollbacks still
execute. All rollback errors are collected, never swallowed.
"""

from __future__ import annotations

from dataclasses import dataclass

from runsheet._result import RollbackCallback, RollbackFailure, RollbackReport


@dataclass(frozen=True)
class ExecutedStep:
    """Tracks a step that was executed, for rollback purposes.

    The rollback callback captures the pre-step context and output at
    creation time, making it safe for concurrent/reentrant pipelines.
    """

    name: str
    rollback: RollbackCallback | None = None


async def do_rollback(executed: list[ExecutedStep]) -> RollbackReport:
    """Execute rollback callbacks in reverse order. Best-effort."""
    completed: list[str] = []
    failed: list[RollbackFailure] = []

    for entry in reversed(executed):
        if entry.rollback is None:
            continue
        try:
            await entry.rollback()
            completed.append(entry.name)
        except Exception as e:
            failed.append(RollbackFailure(step=entry.name, error=e))

    return RollbackReport(
        completed=tuple(completed),
        failed=tuple(failed),
    )
