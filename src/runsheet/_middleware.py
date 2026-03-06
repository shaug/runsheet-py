"""Middleware types and composition.

Middleware wraps the full step lifecycle including schema validation.
First in list = outermost wrapper. Composition via functools.reduce (right fold).
"""

from __future__ import annotations

import functools
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class StepInfo:
    """Metadata about the step being executed, passed to middleware."""

    name: str


# Middleware signature: (step_info, next_fn, ctx) -> result
NextFn = Callable[[dict[str, Any]], Coroutine[Any, Any, Any]]
StepMiddleware = Callable[[StepInfo, NextFn, dict[str, Any]], Coroutine[Any, Any, Any]]


def compose_middleware(
    middleware: list[StepMiddleware],
    step_info: StepInfo,
    core_fn: NextFn,
) -> NextFn:
    """Compose middleware around a core execution function.

    First middleware in the list is the outermost wrapper.
    Uses right-fold (reduceRight equivalent) so the first middleware
    wraps everything else.
    """

    def make_next(next_fn: NextFn, mw: StepMiddleware) -> NextFn:
        async def wrapped(ctx: dict[str, Any]) -> Any:
            return await mw(step_info, next_fn, ctx)

        return wrapped

    return functools.reduce(make_next, reversed(middleware), core_fn)
