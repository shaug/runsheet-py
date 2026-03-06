"""distribute — cross-product collection combinator.

Runs a step once per combination (cross product) of items from multiple
collections, concurrently. Similar to AWS Step Functions Map state but
with declarative key mapping and cross-product support.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any, cast

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
from runsheet._result import RollbackCallback, StepFailure, StepResult
from runsheet._step import Step, _FnStep


def _cross_product(
    mapping: dict[str, str], ctx: dict[str, Any]
) -> list[dict[str, Any]]:
    """Compute the cross product of all mapped collections.

    For mapping ``{"accountIds": "accountId", "regionIds": "regionId"}``
    and context ``{"accountIds": ["a1", "a2"], "regionIds": ["r1", "r2"]}``,
    produces::

        [
            {"accountId": "a1", "regionId": "r1"},
            {"accountId": "a1", "regionId": "r2"},
            {"accountId": "a2", "regionId": "r1"},
            {"accountId": "a2", "regionId": "r2"},
        ]

    If any mapped collection is empty, the result is an empty list.
    """
    combinations: list[dict[str, Any]] = [{}]

    for context_key, step_key in mapping.items():
        raw = ctx.get(context_key)
        if not isinstance(raw, list):
            return []
        items = cast(list[Any], raw)
        if len(items) == 0:
            return []
        next_combos: list[dict[str, Any]] = []
        for combo in combinations:
            for item in items:
                next_combos.append({**combo, step_key: item})
        combinations = next_combos

    return combinations


def distribute(
    key: str,
    mapping: dict[str, str],
    inner_step: Step,
) -> _FnStep:
    """Distribute collections across a step via cross-product, concurrently.

    Runs ``inner_step`` once per combination of items from the mapped
    collections.  Non-mapped context keys pass through unchanged.

    On partial failure, succeeded items are rolled back in reverse order
    (best-effort).  On external failure (a later pipeline step fails),
    all items are rolled back via ``make_rollback``.

    Args:
        key: Output key under which results are collected.
        mapping: Maps context array keys to step scalar keys.
            E.g. ``{"accountIds": "accountId"}`` means the context
            field ``accountIds`` (a list) is fanned out so that each
            invocation receives a single ``accountId``.
        inner_step: The step to execute for each combination.

    Returns:
        A ``_FnStep`` whose output is ``{key: [result, ...]}``.
    """
    step_name = f"distribute({key}, {inner_step.name})"
    array_keys = set(mapping.keys())

    # Per-call-site state captured by make_rollback immediately after run.
    # Safe because Pipeline calls run() then make_rollback() with no await
    # in between (asyncio cooperative scheduling).
    _last_execution: list[
        tuple[list[dict[str, Any]], dict[str, Any], list[dict[str, Any]]]
    ] = []

    async def run_fn(ctx: object) -> StepResult[dict[str, Any]]:
        ctx_dict = to_ctx_dict(ctx)
        meta = step_meta(step_name, ctx_dict)

        combinations = _cross_product(mapping, ctx_dict)

        # Base context: everything except the array keys
        base_ctx: dict[str, Any] = {
            k: v for k, v in ctx_dict.items() if k not in array_keys
        }

        if not combinations:
            data: dict[str, Any] = {key: []}
            _last_execution.clear()
            _last_execution.append((combinations, base_ctx, []))
            return step_success(data, meta)

        # Run step for each combination concurrently
        async def run_one(
            combo: dict[str, Any],
        ) -> tuple[dict[str, Any] | None, Exception | None]:
            item_ctx = {**base_ctx, **combo}
            result = await inner_step.run(item_ctx)
            if isinstance(result, StepFailure):
                return None, result.error
            return result.data, None

        raw = await asyncio.gather(
            *(run_one(c) for c in combinations),
            return_exceptions=True,
        )
        settled, gather_errors = partition_settled(raw)

        errors: list[Exception] = list(gather_errors)
        succeeded: list[tuple[int, dict[str, Any]]] = []

        for i, item in enumerate(
            cast(
                list[tuple[dict[str, Any] | None, Exception | None]],
                settled,
            )
        ):
            output, err = item
            if err is not None:
                errors.append(err)
            else:
                assert output is not None
                succeeded.append((i, output))

        if errors:
            # Roll back succeeded items in reverse order (best-effort)
            if inner_step.has_rollback:
                for idx, output in reversed(succeeded):
                    combo = combinations[idx]
                    item_ctx = {**base_ctx, **combo}
                    with contextlib.suppress(Exception):
                        await inner_step.run_rollback(item_ctx, output)
            return step_failure(
                collapse_errors(
                    errors,
                    f"{step_name}: {len(errors)} item(s) failed",
                ),
                meta,
                step_name,
            )

        # Collect results in cross-product order
        results = [output for _, output in succeeded]

        # Store for make_rollback to capture
        _last_execution.clear()
        _last_execution.append((combinations, base_ctx, results))

        return step_success({key: results}, meta)

    def make_rb(
        pre_ctx: dict[str, Any], output: dict[str, Any]
    ) -> RollbackCallback | None:
        if not _last_execution:
            return None
        combinations, base_ctx, results = _last_execution[0]
        _last_execution.clear()

        if not inner_step.has_rollback or not results:
            return None

        async def cb() -> None:
            errors: list[Exception] = []
            for i in range(len(results) - 1, -1, -1):
                combo = combinations[i]
                item_ctx = {**base_ctx, **combo}
                try:
                    await inner_step.run_rollback(item_ctx, results[i])
                except Exception as e:
                    errors.append(to_error(e))
            if errors:
                raise RollbackError(
                    f"{step_name}: {len(errors)} rollback(s) failed",
                    causes=tuple(errors),
                )

        return cb

    return _FnStep(name=step_name, run_fn=run_fn, make_rollback_fn=make_rb)
