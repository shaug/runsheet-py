"""Shared internal utilities.

Result constructors, error normalization, context helpers, and predicate calling.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable, Mapping, Sequence
from types import MappingProxyType
from typing import Any, TypeVar, cast

from pydantic import BaseModel

from runsheet._errors import UnknownError
from runsheet._result import (
    EMPTY_ROLLBACK,
    AggregateMeta,
    RollbackReport,
    StepFailure,
    StepMeta,
    StepSuccess,
)

_T = TypeVar("_T")


# ---------------------------------------------------------------------------
# Error utilities
# ---------------------------------------------------------------------------


def to_error(value: object) -> Exception:
    """Normalize any value to an Exception."""
    if isinstance(value, Exception):
        return value
    return UnknownError(f"Non-exception value raised: {value!r}", original_value=value)


def collapse_errors(errors: list[Exception], message: str) -> Exception:
    """Collapse a list of errors into a single error.

    Returns the sole error when there is exactly one, otherwise wraps
    them in an ExceptionGroup with the given message.

    Raises ValueError if the list is empty.
    """
    if not errors:
        raise ValueError("collapse_errors requires at least one error")
    if len(errors) == 1:
        return errors[0]
    return ExceptionGroup(message, errors)


# ---------------------------------------------------------------------------
# Context utilities
# ---------------------------------------------------------------------------


def to_ctx_dict(ctx: object) -> dict[str, Any]:
    """Normalize ctx to dict[str, Any]."""
    if isinstance(ctx, dict):
        return cast(dict[str, Any], ctx)
    if isinstance(ctx, BaseModel):
        return ctx.model_dump()
    return {}


def partition_settled(
    results: Sequence[object],
) -> tuple[list[object], list[Exception]]:
    """Partition gather(return_exceptions=True) results into values and errors.

    BaseExceptions that aren't Exceptions are wrapped via to_error().
    Returns (values, errors) where values are the non-exception results.
    """
    values: list[object] = []
    errors: list[Exception] = []
    for r in results:
        if isinstance(r, BaseException):
            errors.append(r if isinstance(r, Exception) else to_error(r))
        else:
            values.append(r)
    return values, errors


# ---------------------------------------------------------------------------
# Result constructors
# ---------------------------------------------------------------------------


def step_meta(name: str, args: Mapping[str, Any]) -> StepMeta:
    """Create StepMeta with a frozen copy of args."""
    return StepMeta(name=name, args=MappingProxyType(dict(args)))


def step_success(data: _T, meta: StepMeta) -> StepSuccess[_T]:
    return StepSuccess(data=data, meta=meta)


def step_failure(
    error: Exception,
    meta: StepMeta,
    failed_step: str,
    rollback: RollbackReport = EMPTY_ROLLBACK,
) -> StepFailure:
    return StepFailure(
        error=error, meta=meta, failed_step=failed_step, rollback=rollback
    )


def aggregate_meta(
    name: str,
    args: Mapping[str, Any],
    steps_executed: tuple[str, ...],
) -> AggregateMeta:
    return AggregateMeta(
        name=name,
        args=args,
        steps_executed=steps_executed,
    )


# ---------------------------------------------------------------------------
# Predicate helper
# ---------------------------------------------------------------------------


async def call_predicate(
    predicate: Callable[[dict[str, Any]], object],
    ctx: dict[str, Any],
) -> bool:
    """Call a predicate function (sync or async) with context."""
    result = predicate(ctx)
    if inspect.isawaitable(result):
        return bool(await result)
    return bool(result)
