"""Combinator protocol and result type.

Defines the structural interface that all combinators (when, parallel,
choice, map_step, filter_step, flat_map) must satisfy — via Protocol,
not inheritance. Duck typing done right.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from runsheet._rollback import ExecutedStep


@dataclass(frozen=True)
class CombinatorResult:
    """Named result from a combinator's execute() method."""

    output: dict[str, Any] = field(default_factory=lambda: dict[str, Any]())
    executed: list[ExecutedStep] = field(default_factory=lambda: list[ExecutedStep]())
    skipped: list[str] = field(default_factory=lambda: list[str]())


@runtime_checkable
class CombinatorStep(Protocol):
    """Structural interface for combinator pseudo-steps.

    Combinators are not Step instances — they just need a name and
    an execute() method. Classes satisfy this protocol by shape alone.
    """

    name: str

    async def execute(self, ctx: dict[str, Any]) -> CombinatorResult: ...
