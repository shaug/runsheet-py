"""Result types for steps and pipelines.

StepResult is the core type — returned by all Runnable.run() implementations.
StepSuccess carries output data, StepFailure carries the error.

AggregateMeta extends StepMeta with orchestration detail (steps_executed).
Pipeline, parallel, and choice return StepSuccess/StepFailure with AggregateMeta.

AggregateSuccess, AggregateFailure, and AggregateResult are backward-compatible
aliases for StepSuccess, StepFailure, and StepResult.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Generic, Literal, Protocol, TypeVar, runtime_checkable

T = TypeVar("T")

# Callback returned by make_rollback() — captures rollback state for one execution.
RollbackCallback = Callable[[], Awaitable[None]]


@dataclass(frozen=True)
class RollbackFailure:
    step: str
    error: Exception


@dataclass(frozen=True)
class RollbackReport:
    completed: tuple[str, ...] = ()
    failed: tuple[RollbackFailure, ...] = ()


EMPTY_ROLLBACK = RollbackReport()


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class StepMeta:
    """Slim metadata for step results. Just name and args."""

    name: str
    args: Mapping[str, Any]


@dataclass(frozen=True)
class AggregateMeta(StepMeta):
    """Extended metadata for aggregate results (pipeline, parallel, choice).

    Adds orchestration detail — which inner steps executed.
    """

    steps_executed: tuple[str, ...] = ()


# ---------------------------------------------------------------------------
# Step results
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class StepSuccess(Generic[T]):
    data: T
    meta: StepMeta
    success: Literal[True] = field(default=True, init=False)


@dataclass(frozen=True)
class StepFailure:
    error: Exception
    meta: StepMeta
    failed_step: str
    rollback: RollbackReport = EMPTY_ROLLBACK
    success: Literal[False] = field(default=False, init=False)


StepResult = StepSuccess[T] | StepFailure


# ---------------------------------------------------------------------------
# Aggregate result aliases (backward compatibility)
# ---------------------------------------------------------------------------

AggregateSuccess = StepSuccess
AggregateFailure = StepFailure
AggregateResult = StepResult


# ---------------------------------------------------------------------------
# Runnable protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class Runnable(Protocol):
    """Protocol for anything that can be used as a pipeline step.

    Step, Pipeline, and combinators (parallel, choice, when, map_step, etc.)
    all satisfy this protocol.
    """

    @property
    def name(self) -> str: ...

    @property
    def requires(self) -> type | None: ...

    @property
    def provides(self) -> type | None: ...

    @property
    def has_rollback(self) -> bool: ...

    async def run(self, ctx: object) -> StepResult[dict[str, Any]]: ...

    async def run_rollback(self, ctx: object, output: object) -> None: ...

    def make_rollback(
        self, pre_ctx: dict[str, Any], output: dict[str, Any]
    ) -> RollbackCallback | None: ...
