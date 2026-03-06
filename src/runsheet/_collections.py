"""Collection combinators: map_step, filter_step, flat_map.

Items run concurrently via asyncio.gather.

Open question decision: naming for map/filter.
Chose ``map_step`` / ``filter_step`` — avoids shadowing the Python builtins
``map`` and ``filter`` while staying descriptive.  Alternatives considered:
``map_each``, ``for_each``, ``keep``, ``where``, ``select``.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
from collections.abc import Callable
from typing import Any

from pydantic import BaseModel

from runsheet._combinator_base import CombinatorResult
from runsheet._internal import execute_step
from runsheet._rollback import ExecutedStep
from runsheet._step import Step


class _MapStep:
    """Collection iteration — function form or step form.

    Function form: apply a function to each item, collect results under a key.
    Step form: run an existing step for each item (item merged into context).
    """

    def __init__(
        self,
        key: str,
        collection_fn: Callable[..., Any],
        mapper: Callable[..., Any] | Step,
    ) -> None:
        self.name = f"map({key})"
        self._key = key
        self._collection_fn = collection_fn
        self._mapper = mapper

    async def execute(self, ctx: dict[str, Any]) -> CombinatorResult:
        items = self._collection_fn(ctx)
        if inspect.isawaitable(items):
            items = await items

        if isinstance(self._mapper, Step):
            return await self._execute_step_form(ctx, list(items))
        return await self._execute_fn_form(ctx, list(items))

    async def _execute_fn_form(
        self, ctx: dict[str, Any], items: list[Any]
    ) -> CombinatorResult:
        mapper = self._mapper
        assert callable(mapper) and not isinstance(mapper, Step)

        async def run_one(item: Any) -> Any:
            result = mapper(item, ctx)
            if inspect.isawaitable(result):
                return await result
            return result

        results = await asyncio.gather(*(run_one(item) for item in items))
        return CombinatorResult(output={self._key: list(results)})

    async def _execute_step_form(
        self, ctx: dict[str, Any], items: list[Any]
    ) -> CombinatorResult:
        step = self._mapper
        assert isinstance(step, Step)
        pre_ctx = dict(ctx)

        async def run_one(
            item: Any,
        ) -> tuple[dict[str, Any], Exception | None]:
            # Merge item into context so the step sees both
            item_ctx: dict[str, Any]
            if isinstance(item, dict):
                item_ctx = {**ctx, **item}
            elif isinstance(item, BaseModel):
                item_ctx = {**ctx, **item.model_dump()}
            else:
                item_ctx = dict(ctx)
            try:
                output = await execute_step(step, item_ctx)
                return output, None
            except Exception as e:
                return {}, e

        results = await asyncio.gather(*(run_one(item) for item in items))

        errors: list[Exception] = []
        succeeded: list[dict[str, Any]] = []
        for output, error in results:
            if error is not None:
                errors.append(error)
            else:
                succeeded.append(output)

        if errors:
            # Rollback succeeded items in reverse order
            for output in reversed(succeeded):
                if step.has_rollback:
                    with contextlib.suppress(Exception):
                        await step.run_rollback(pre_ctx, output)
            raise errors[0]

        executed = [
            ExecutedStep(step=step, pre_ctx=pre_ctx, output=output)
            for output in succeeded
        ]

        return CombinatorResult(output={self._key: succeeded}, executed=executed)


def map_step(
    key: str,
    collection_fn: Callable[..., Any],
    mapper: Callable[..., Any] | Step,
) -> _MapStep:
    """Create a map step that iterates over a collection.

    Function form: mapper(item, ctx) -> result
    Step form: mapper is a Step, each item merged into context.
    Items run concurrently. On partial failure (step form), succeeded items
    are rolled back.
    """
    return _MapStep(key, collection_fn, mapper)


class _FilterStep:
    """Collection filtering — keep items where predicate returns True.

    Predicates run concurrently. Original order preserved. No rollback.
    Supports sync and async predicates.
    """

    def __init__(
        self,
        key: str,
        collection_fn: Callable[..., Any],
        predicate: Callable[..., Any],
    ) -> None:
        self.name = f"filter({key})"
        self._key = key
        self._collection_fn = collection_fn
        self._predicate = predicate

    async def execute(self, ctx: dict[str, Any]) -> CombinatorResult:
        items = self._collection_fn(ctx)
        if inspect.isawaitable(items):
            items = await items

        items_list = list(items)

        async def check_one(item: Any) -> bool:
            result = self._predicate(item, ctx)
            if inspect.isawaitable(result):
                return bool(await result)
            return bool(result)

        results = await asyncio.gather(*(check_one(item) for item in items_list))
        filtered = [
            item for item, keep in zip(items_list, results, strict=False) if keep
        ]

        return CombinatorResult(output={self._key: filtered})


def filter_step(
    key: str,
    collection_fn: Callable[..., Any],
    predicate: Callable[..., Any],
) -> _FilterStep:
    """Create a filter step that keeps items where predicate returns True.

    Predicate signature: predicate(item, ctx) -> bool.
    Predicates run concurrently. Original order preserved. No rollback (pure).
    Supports sync and async predicates.
    """
    return _FilterStep(key, collection_fn, predicate)


class _FlatMapStep:
    """Collection expansion — map each item to a list, flatten one level.

    Function-only (no step form). Callbacks run concurrently. No rollback.
    """

    def __init__(
        self,
        key: str,
        collection_fn: Callable[..., Any],
        mapper: Callable[..., Any],
    ) -> None:
        self.name = f"flat_map({key})"
        self._key = key
        self._collection_fn = collection_fn
        self._mapper = mapper

    async def execute(self, ctx: dict[str, Any]) -> CombinatorResult:
        items = self._collection_fn(ctx)
        if inspect.isawaitable(items):
            items = await items

        async def run_one(item: Any) -> list[Any]:
            result = self._mapper(item, ctx)
            if inspect.isawaitable(result):
                return list(await result)
            return list(result)

        results = await asyncio.gather(*(run_one(item) for item in items))
        # Flatten one level
        flattened = [x for sublist in results for x in sublist]

        return CombinatorResult(output={self._key: flattened})


def flat_map(
    key: str,
    collection_fn: Callable[..., Any],
    mapper: Callable[..., Any],
) -> _FlatMapStep:
    """Create a flat_map step that maps each item to a list and flattens one level.

    Function-only (no step form). Callbacks run concurrently. No rollback (pure).
    """
    return _FlatMapStep(key, collection_fn, mapper)
