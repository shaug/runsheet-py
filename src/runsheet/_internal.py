"""Shared internal utilities.

Centralizes error normalization, schema validation, and inner step execution
to avoid duplication across pipeline, combinators, and collections.
"""

from __future__ import annotations

import asyncio
import builtins
import inspect
from types import MappingProxyType
from typing import Any, cast

from pydantic import BaseModel, ValidationError

from runsheet._errors import (
    ProvidesValidationError,
    RequiresValidationError,
    RetryExhaustedError,
    TimeoutError,
    UnknownError,
)
from runsheet._step import Step


def to_error(value: object) -> Exception:
    """Normalize any value to an Exception.

    Application exceptions pass through as-is.
    Non-Exception values get wrapped in UnknownError.
    """
    if isinstance(value, Exception):
        return value
    return UnknownError(f"Non-exception value raised: {value!r}", original_value=value)


def validate_requires(step: Step, ctx: dict[str, Any]) -> BaseModel | None:
    """Validate context against a step's requires schema.

    Returns the validated model instance, or None if no requires schema.
    Raises RequiresValidationError on validation failure.
    """
    if step.requires is None:
        return None
    try:
        return step.requires.model_validate(ctx)
    except ValidationError as e:
        raise RequiresValidationError(
            f"Step '{step.name}' requires validation failed: {e}"
        ) from e


def validate_provides(step: Step, output: Any) -> dict[str, Any]:
    """Validate step output against its provides schema.

    If a step declares provides, output MUST be an instance of that model.
    If no provides, accept a dict or BaseModel and dump it.
    """
    if step.provides is None:
        if isinstance(output, BaseModel):
            return output.model_dump()
        if isinstance(output, dict):
            return cast(dict[str, Any], output)
        return {}

    if not isinstance(output, step.provides):
        raise ProvidesValidationError(
            f"Step '{step.name}' must return {step.provides.__name__}, "
            f"got {type(output).__name__}"
        )
    return output.model_dump()


def freeze_context(ctx: dict[str, Any]) -> MappingProxyType[str, Any]:
    """Create a read-only view of the context dict."""
    return MappingProxyType(ctx)


async def run_with_timeout(coro: Any, timeout_seconds: float, step_name: str) -> Any:
    """Execute a coroutine with a timeout. Uses asyncio.timeout (Python 3.11+)."""
    try:
        async with asyncio.timeout(timeout_seconds):
            return await coro
    except builtins.TimeoutError as e:
        raise TimeoutError(
            f"Step '{step_name}' timed out after {timeout_seconds}s",
            timeout_seconds=timeout_seconds,
        ) from e


async def _run_step_once(step: Step, step_input: Any) -> Any:
    """Run a step once, with timeout if configured."""
    if step.timeout is not None:
        return await run_with_timeout(step.run(step_input), step.timeout, step.name)
    return await step.run(step_input)


async def _execute_with_retry_and_timeout(step: Step, step_input: Any) -> Any:
    """Execute a step with retry and/or timeout. Returns raw output."""
    if step.retry is None:
        return await _run_step_once(step, step_input)

    errors: list[Exception] = []
    total_attempts = 1 + step.retry.count

    for attempt in range(total_attempts):
        try:
            return await _run_step_once(step, step_input)
        except Exception as e:
            errors.append(e)
            if attempt < total_attempts - 1:
                if step.retry.retry_if is not None and not step.retry.retry_if(errors):
                    break
                if step.retry.delay > 0:
                    if step.retry.backoff == "exponential":
                        delay = step.retry.delay * (2**attempt)
                    else:
                        delay = step.retry.delay
                    await asyncio.sleep(delay)

    last_error = errors[-1] if errors else Exception("Unknown retry failure")
    raise RetryExhaustedError(
        f"Step '{step.name}' failed after {len(errors)} attempt(s)",
        attempts=len(errors),
        last_error=last_error,
    )


async def execute_step(step: Step, ctx: dict[str, Any]) -> dict[str, Any]:
    """Execute a single step with validation, retry, and timeout.

    This is the shared inner step lifecycle used by the pipeline engine
    and all combinators.
    """
    # 1. Validate requires
    validated_input = validate_requires(step, ctx)
    step_input = validated_input if validated_input is not None else ctx

    # 2. Execute with retry and/or timeout
    raw_output: Any = await _execute_with_retry_and_timeout(step, step_input)

    # 3. Validate provides
    return validate_provides(step, raw_output)


async def call_predicate(predicate: Any, ctx: dict[str, Any]) -> bool:
    """Call a predicate function (sync or async) with context.

    Open question decision: context type for predicate lambdas.
    Predicates receive a plain ``dict[str, Any]`` — the most flexible option,
    matching the TS version's plain object.  Alternatives considered:
    ``MappingProxyType`` (read-only but no attribute access),
    ``SimpleNamespace`` (attribute access but no type safety),
    Pydantic model (typed but requires knowing accumulated shape).
    """
    result = predicate(ctx)
    if inspect.isawaitable(result):
        return bool(await result)
    return bool(result)
