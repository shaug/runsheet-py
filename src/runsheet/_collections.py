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

from runsheet._errors import RollbackError
from runsheet._internal import (
    collapse_errors,
    partition_settled,
    step_failure,
    step_meta,
    step_success,
    to_ctx_dict,
    to_error,
)
from runsheet._result import (
    RollbackCallback,
    Runnable,
    StepFailure,
    StepMeta,
    StepResult,
)
from runsheet._step import _FnStep

# ---------------------------------------------------------------------------
# map_step
# ---------------------------------------------------------------------------


def map_step(
    key: str,
    collection_fn: Callable[[dict[str, Any]], Any],
    mapper: Callable[[Any, dict[str, Any]], Any] | Runnable,
) -> _FnStep:
    """Map over a collection. Function form or step form."""
    step_name = f"map({key})"

    # Per-call-site state for external rollback (step form only).
    # Captured by make_rb immediately after run — safe because Pipeline
    # calls run() then make_rollback() with no await in between.
    _last_execution: list[
        tuple[Runnable, list[tuple[dict[str, Any], dict[str, Any]]]]
    ] = []

    is_step = isinstance(mapper, Runnable)

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

        if isinstance(mapper, Runnable):
            return await _map_step_form(
                key, step_name, mapper, ctx_dict, items_list, meta, _last_execution
            )
        _last_execution.clear()
        assert callable(mapper)
        return await _map_fn_form(key, mapper, ctx_dict, items_list, meta)

    def make_rb(
        pre_ctx: dict[str, Any], output: dict[str, Any]
    ) -> RollbackCallback | None:
        if not _last_execution:
            return None
        inner_step, item_pairs = _last_execution[0]
        _last_execution.clear()

        if not inner_step.has_rollback or not item_pairs:
            return None

        async def cb() -> None:
            errors: list[Exception] = []
            for item_ctx, item_output in reversed(item_pairs):
                try:
                    await inner_step.run_rollback(item_ctx, item_output)
                except Exception as e:
                    errors.append(to_error(e))
            if errors:
                raise RollbackError(
                    f"{step_name}: {len(errors)} rollback(s) failed",
                    causes=tuple(errors),
                )

        return cb

    return _FnStep(
        name=step_name,
        run_fn=run_fn,
        make_rollback_fn=make_rb if is_step else None,
    )


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
    step_name: str,
    inner_step: Runnable,
    ctx: dict[str, Any],
    items: list[object],
    meta: StepMeta,
    last_execution: list[tuple[Runnable, list[tuple[dict[str, Any], dict[str, Any]]]]],
) -> StepResult[dict[str, Any]]:
    async def run_one(
        item: object,
    ) -> tuple[dict[str, Any], dict[str, Any] | None, Exception | None]:
        item_ctx: dict[str, Any]
        if isinstance(item, dict):
            item_ctx = {**ctx, **item}
        elif isinstance(item, BaseModel):
            item_ctx = {**ctx, **item.model_dump()}
        else:
            item_ctx = dict(ctx)

        result = await inner_step.run(item_ctx)
        if isinstance(result, StepFailure):
            return item_ctx, None, result.error
        return item_ctx, result.data, None

    raw = await asyncio.gather(
        *(run_one(item) for item in items),
        return_exceptions=True,
    )
    settled, gather_errors = partition_settled(raw)

    errors: list[Exception] = list(gather_errors)
    succeeded: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for item in cast(
        list[tuple[dict[str, Any], dict[str, Any] | None, Exception | None]],
        settled,
    ):
        item_ctx, output, err = item
        if err is not None:
            errors.append(err)
        else:
            assert output is not None
            succeeded.append((item_ctx, output))

    if errors:
        # Partial failure: roll back succeeded items (best-effort).
        # Errors are swallowed here because the primary error is the step
        # failure itself; the pipeline's RollbackReport covers the external
        # rollback path.
        if inner_step.has_rollback:
            for item_ctx, output in reversed(succeeded):
                with contextlib.suppress(Exception):
                    await inner_step.run_rollback(item_ctx, output)
        return step_failure(
            collapse_errors(errors, f"map({key}): {len(errors)} item(s) failed"),
            meta,
            f"map({key})",
        )

    # Store for make_rollback to capture
    last_execution.clear()
    last_execution.append((inner_step, succeeded))

    results = [output for _, output in succeeded]
    return step_success({key: results}, meta)


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
