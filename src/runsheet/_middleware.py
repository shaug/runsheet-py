"""Middleware types and composition.

Middleware wraps the full step lifecycle including schema validation.
First in list = outermost wrapper. Composition via functools.reduce (right fold).

NextFn and StepMiddleware operate on StepResult — matching the TS version
where middleware receives and returns StepResult values.
"""

from __future__ import annotations

import functools
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any

from runsheet._result import StepResult

# Middleware signature: next_fn receives context, returns StepResult
NextFn = Callable[
    [dict[str, Any]],
    Coroutine[Any, Any, StepResult[dict[str, Any]]],
]
StepMiddleware = Callable[
    ["StepInfo", NextFn, dict[str, Any]],
    Coroutine[Any, Any, StepResult[dict[str, Any]]],
]


@dataclass(frozen=True)
class StepInfo:
    """Metadata about the step being executed, passed to middleware."""

    name: str
    requires: type | None = None
    provides: type | None = None


def compose_middleware(
    middleware: list[StepMiddleware],
    step_info: StepInfo,
    core_fn: NextFn,
) -> NextFn:
    """Compose middleware around a core execution function.

    First middleware in the list is the outermost wrapper.
    """

    def make_next(next_fn: NextFn, mw: StepMiddleware) -> NextFn:
        async def wrapped(ctx: dict[str, Any]) -> StepResult[dict[str, Any]]:
            return await mw(step_info, next_fn, ctx)

        return wrapped

    return functools.reduce(make_next, reversed(middleware), core_fn)
