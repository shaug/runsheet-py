"""Rollback execution.

On failure at step N, rollback handlers for steps N-1...0 execute in reverse
order. Rollback is best-effort: if a handler raises, remaining rollbacks still
execute. All rollback errors are collected, never swallowed.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from runsheet._result import RollbackFailure, RollbackReport
from runsheet._step import Step


@dataclass(frozen=True)
class ExecutedStep:
    """Tracks a step that was executed, for rollback purposes."""

    step: Step
    pre_ctx: dict[str, Any]
    output: dict[str, Any]


async def do_rollback(executed: list[ExecutedStep]) -> RollbackReport:
    """Execute rollback handlers in reverse order. Best-effort."""
    completed: list[str] = []
    failed: list[RollbackFailure] = []

    for entry in reversed(executed):
        if not entry.step.has_rollback:
            continue
        try:
            await entry.step.run_rollback(entry.pre_ctx, entry.output)
            completed.append(entry.step.name)
        except Exception as e:
            failed.append(RollbackFailure(step=entry.step.name, error=e))

    return RollbackReport(
        completed=tuple(completed),
        failed=tuple(failed),
    )
