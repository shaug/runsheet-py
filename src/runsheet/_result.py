"""Pipeline result types.

Hand-rolled Result dataclasses (decision: no dry-python/returns dependency).
The TS version only uses composable-functions for the container shape, not
map/bind methods. A simple dataclass is more faithful and avoids a dependency.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Generic, Literal, TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class RollbackFailure:
    step: str
    error: Exception


@dataclass(frozen=True)
class RollbackReport:
    completed: tuple[str, ...] = ()
    failed: tuple[RollbackFailure, ...] = ()


@dataclass(frozen=True)
class PipelineExecutionMeta:
    pipeline: str
    args: Mapping[str, Any]
    steps_executed: tuple[str, ...] = ()
    steps_skipped: tuple[str, ...] = ()


@dataclass(frozen=True)
class PipelineSuccess(Generic[T]):
    data: T
    meta: PipelineExecutionMeta
    success: Literal[True] = field(default=True, init=False)
    errors: tuple[()] = field(default=(), init=False)


@dataclass(frozen=True)
class PipelineFailure:
    errors: tuple[Exception, ...]
    meta: PipelineExecutionMeta
    failed_step: str
    rollback: RollbackReport
    success: Literal[False] = field(default=False, init=False)


PipelineResult = PipelineSuccess[T] | PipelineFailure
