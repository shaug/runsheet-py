"""Collection combinators: map_step, filter_step, flat_map.

Items run concurrently via asyncio.gather with return_exceptions=True
so all items complete before errors are collected. All return _FnStep
instances whose run() returns StepResult.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
from collections.abc import Callable
from typing import Any, cast

from pydantic import BaseModel

from runsheet._internal import (
    collapse_errors,
    partition_settled,
    step_failure,
    step_meta,
    step_success,
    to_ctx_dict,
    to_error,
)
from runsheet._result import StepFailure, StepMeta, StepResult
from runsheet._step import Step, _FnStep

# ---------------------------------------------------------------------------
# map_step
# ---------------------------------------------------------------------------


def map_step(
    key: str,
    collection_fn: Callable[[dict[str, Any]], Any],
    mapper: Callable[[Any, dict[str, Any]], Any] | Step,
) -> _FnStep:
    """Map over a collection. Function form or step form."""
    step_name = f"map({key})"

    async def run_fn(ctx: object) -> StepResult[dict[str, Any]]:
        ctx_dict = to_ctx_dict(ctx)
        meta = step_meta(step_name, ctx_dict)

        try:
            items = collection_fn(ctx_dict)
            if inspect.isawaitable(items):
                items = await items
            items_list: list[object] = list(items)
        except Exception as e:
            return step_failure(to_error(e), meta, step_name)

        if isinstance(mapper, Step):
            return await _map_step_form(key, mapper, ctx_dict, items_list, meta)
        return await _map_fn_form(key, mapper, ctx_dict, items_list, meta)

    return _FnStep(name=step_name, run_fn=run_fn)


async def _map_fn_form(
    key: str,
    mapper: Callable[[Any, dict[str, Any]], Any],
    ctx: dict[str, Any],
    items: list[object],
    meta: StepMeta,
) -> StepResult[dict[str, Any]]:
    async def run_one(item: object) -> object:
        result = mapper(item, ctx)
        if inspect.isawaitable(result):
            return await result
        return result

    raw = await asyncio.gather(
        *(run_one(item) for item in items),
        return_exceptions=True,
    )
    values, errors = partition_settled(raw)

    if errors:
        return step_failure(
            collapse_errors(errors, f"map({key}): {len(errors)} item(s) failed"),
            meta,
            f"map({key})",
        )

    return step_success({key: values}, meta)


async def _map_step_form(
    key: str,
    inner_step: Step,
    ctx: dict[str, Any],
    items: list[object],
    meta: StepMeta,
) -> StepResult[dict[str, Any]]:
    pre_ctx: dict[str, Any] = dict(ctx)

    async def run_one(
        item: object,
    ) -> tuple[dict[str, Any] | None, Exception | None]:
        item_ctx: dict[str, Any]
        if isinstance(item, dict):
            item_ctx = {**ctx, **item}
        elif isinstance(item, BaseModel):
            item_ctx = {**ctx, **item.model_dump()}
        else:
            item_ctx = dict(ctx)

        result = await inner_step.run(item_ctx)
        if isinstance(result, StepFailure):
            return None, result.error
        return result.data, None

    raw = await asyncio.gather(
        *(run_one(item) for item in items),
        return_exceptions=True,
    )
    settled, gather_errors = partition_settled(raw)

    errors: list[Exception] = list(gather_errors)
    succeeded: list[dict[str, Any]] = []
    for item in cast(list[tuple[dict[str, Any] | None, Exception | None]], settled):
        output, err = item
        if err is not None:
            errors.append(err)
        else:
            assert output is not None
            succeeded.append(output)

    if errors:
        for output in reversed(succeeded):
            if inner_step.has_rollback:
                with contextlib.suppress(Exception):
                    await inner_step.run_rollback(pre_ctx, output)
        return step_failure(
            collapse_errors(errors, f"map({key}): {len(errors)} item(s) failed"),
            meta,
            f"map({key})",
        )

    return step_success({key: succeeded}, meta)


# ---------------------------------------------------------------------------
# filter_step
# ---------------------------------------------------------------------------


def filter_step(
    key: str,
    collection_fn: Callable[[dict[str, Any]], Any],
    predicate: Callable[[Any, dict[str, Any]], Any],
) -> _FnStep:
    """Keep items where predicate(item, ctx) returns True."""
    step_name = f"filter({key})"

    async def run_fn(ctx: object) -> StepResult[dict[str, Any]]:
        ctx_dict = to_ctx_dict(ctx)
        meta = step_meta(step_name, ctx_dict)

        try:
            items = collection_fn(ctx_dict)
            if inspect.isawaitable(items):
                items = await items
            items_list: list[object] = list(items)

            async def check_one(item: object) -> bool:
                result = predicate(item, ctx_dict)
                if inspect.isawaitable(result):
                    return bool(await result)
                return bool(result)

            raw = await asyncio.gather(
                *(check_one(item) for item in items_list),
                return_exceptions=True,
            )
            values, errors = partition_settled(raw)

            if errors:
                return step_failure(
                    collapse_errors(
                        errors,
                        f"{step_name}: {len(errors)} predicate(s) failed",
                    ),
                    meta,
                    step_name,
                )

            keep_flags: list[bool] = [bool(v) for v in values]
            filtered = [
                item for item, keep in zip(items_list, keep_flags, strict=True) if keep
            ]
        except Exception as e:
            return step_failure(to_error(e), meta, step_name)

        return step_success({key: filtered}, meta)

    return _FnStep(name=step_name, run_fn=run_fn)


# ---------------------------------------------------------------------------
# flat_map
# ---------------------------------------------------------------------------


def flat_map(
    key: str,
    collection_fn: Callable[[dict[str, Any]], Any],
    mapper: Callable[[Any, dict[str, Any]], Any],
) -> _FnStep:
    """Map each item to a list and flatten one level."""
    step_name = f"flat_map({key})"

    async def run_fn(ctx: object) -> StepResult[dict[str, Any]]:
        ctx_dict = to_ctx_dict(ctx)
        meta = step_meta(step_name, ctx_dict)

        try:
            items = collection_fn(ctx_dict)
            if inspect.isawaitable(items):
                items = await items

            async def run_one(item: object) -> list[object]:
                result = mapper(item, ctx_dict)
                if inspect.isawaitable(result):
                    return list(await result)
                return list(result)

            raw = await asyncio.gather(
                *(run_one(item) for item in items),
                return_exceptions=True,
            )
            values, errors = partition_settled(raw)

            if errors:
                return step_failure(
                    collapse_errors(
                        errors,
                        f"{step_name}: {len(errors)} callback(s) failed",
                    ),
                    meta,
                    step_name,
                )

            flattened: list[object] = [
                x for sublist in cast(list[list[object]], values) for x in sublist
            ]
        except Exception as e:
            return step_failure(to_error(e), meta, step_name)

        return step_success({key: flattened}, meta)

    return _FnStep(name=step_name, run_fn=run_fn)
