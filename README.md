# runsheet

[![PyPI version](https://img.shields.io/pypi/v/runsheet)](https://pypi.org/project/runsheet/)
[![Python versions](https://img.shields.io/pypi/pyversions/runsheet)](https://pypi.org/project/runsheet/)
[![CI](https://github.com/shaug/runsheet-py/actions/workflows/ci.yml/badge.svg)](https://github.com/shaug/runsheet-py/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Type-safe, composable business logic pipelines for Python.

## Why runsheet

Business logic has a way of growing into tangled, hard-to-test code. A checkout flow starts as one function, then gains validation, payment processing, inventory reservation, email notifications — each with its own failure modes and cleanup logic. Before long you're staring at a 300-line function with nested try/except blocks and no clear way to reuse any of it.

runsheet gives that logic structure. You break work into small, focused steps with explicit inputs and outputs, then compose them into pipelines. Each step is independently testable. The pipeline handles context passing, rollback on failure, and schema validation at every boundary. Immutable data semantics mean steps can't accidentally interfere with each other.

It's an organizational layer for business logic that encourages reuse, testability, type safety, and immutable data flow — without the overhead of a full effect system or workflow engine.

The name takes its inspiration from the world of stage productions and live broadcast events. A runsheet is the document that sequences every cue, handoff, and contingency so the show runs smoothly. Same idea here: you define the steps, and runsheet makes sure they execute in order with clear contracts between them.

## What this is

Args persist and outputs accumulate. That's the core model — initial arguments flow through the entire pipeline, each step's output merges into the context, and every step sees the full picture of everything before it.

A pipeline orchestration library with:

- **Typed steps** — each step declares `requires` and `provides` as Pydantic models. Your IDE shows exact input and output shapes. Both sync and async functions are supported.
- **Validated boundaries** — Pydantic validates context against `requires` before each step and validates output against `provides` after. Mismatches fail fast with clear errors.
- **Immutable step boundaries** — context is frozen between steps via `MappingProxyType`. Each step receives a snapshot and returns only what it adds.
- **Rollback with snapshots** — on failure, rollback handlers execute in reverse order. Each receives the pre-step context and the step's output.
- **Middleware** — cross-cutting concerns (logging, timing, metrics) wrap the full step lifecycle.
- **Standalone** — no framework dependencies beyond Pydantic. Works anywhere Python runs.

**runsheet is NOT** a workflow engine (not Temporal, not Airflow, not Prefect). No persistence, no cross-process coordination. Strictly local, single-call orchestration. That said, it's complementary — you could use a runsheet pipeline as a step within a Temporal workflow or an Inngest function.

## Install

```bash
pip install runsheet
# or
uv add runsheet
```

## Quick start

### Define steps

Each step declares what it reads from context (`requires`) and what it adds (`provides`). The `@step` decorator wraps a function with metadata for pipeline execution.

Step functions can be sync or async — both are supported.

```python
from pydantic import BaseModel
from runsheet import step, RetryPolicy


class OrderInput(BaseModel):
    order_id: str
    amount: float


class ValidatedOrder(BaseModel):
    validated: bool


class ChargeOutput(BaseModel):
    charge_id: str


@step(requires=OrderInput, provides=ValidatedOrder)
async def validate_order(ctx: OrderInput) -> ValidatedOrder:
    if ctx.amount <= 0:
        raise ValueError("Invalid amount")
    return ValidatedOrder(validated=True)


@step(
    requires=OrderInput,
    provides=ChargeOutput,
    retry=RetryPolicy(count=3, delay=0.2, backoff="exponential"),
    timeout=5.0,
)
async def charge_payment(ctx: OrderInput) -> ChargeOutput:
    charge = await stripe.charges.create(amount=ctx.amount)
    return ChargeOutput(charge_id=charge.id)


@charge_payment.rollback
async def undo_charge(ctx: OrderInput, output: ChargeOutput) -> None:
    await stripe.refunds.create(charge=output.charge_id)
```

Each step is fully typed — your IDE can see its exact input and output shapes, and Pydantic validates at every boundary.

### Build and run a pipeline

```python
import asyncio
from runsheet import Pipeline

checkout = Pipeline(
    name="checkout",
    steps=[validate_order, charge_payment, send_confirmation],
    args_schema=OrderInput,
)

result = await checkout.run(OrderInput(order_id="123", amount=50.0))

if result.success:
    print(result.data["charge_id"])   # accumulated context
    print(result.data["email_sent"])
else:
    print(result.errors)              # what went wrong
    print(result.rollback)            # { completed: [...], failed: [...] }
```

`pipeline.run()` never raises. It always returns a `PipelineResult` — either `PipelineSuccess` or `PipelineFailure`.

## Retry and timeout

Steps can declare retry policies and timeouts directly:

```python
@step(
    provides=ApiResponse,
    retry=RetryPolicy(count=3, delay=0.2, backoff="exponential"),
    timeout=5.0,  # seconds
)
async def call_external_api(ctx) -> ApiResponse:
    ...
```

**Retry** re-executes the step on failure. The `retry_if` predicate lets you inspect errors and decide whether to retry:

```python
RetryPolicy(
    count=3,
    retry_if=lambda errors: any("ECONNRESET" in str(e) for e in errors),
)
```

**Timeout** cancels `run` via `asyncio.timeout`. If the step exceeds the limit, it fails with a `TimeoutError`. When both are set, each retry attempt gets its own timeout.

## Conditional steps

```python
from runsheet import when

checkout = Pipeline(
    name="checkout",
    steps=[
        validate_order,
        charge_payment,
        when(lambda ctx: ctx.get("amount", 0) > 10000, notify_manager),
        send_confirmation,
    ],
)
```

Skipped steps produce no snapshot, no rollback entry. The pipeline result tracks which steps were skipped in `result.meta.steps_skipped`.

## Parallel steps

Run steps concurrently with `parallel()`. Outputs merge in array order:

```python
from runsheet import parallel

checkout = Pipeline(
    name="checkout",
    steps=[
        validate_order,
        parallel(reserve_inventory, charge_payment),
        send_confirmation,
    ],
)
```

On partial failure, succeeded inner steps are rolled back before the error propagates. Inner steps retain their own `requires`/`provides` validation, `retry`, and `timeout` behavior.

## Choice (branching)

Execute the first branch whose predicate returns `True` — like an AWS Step Functions Choice state:

```python
from runsheet import choice

checkout = Pipeline(
    name="checkout",
    steps=[
        validate_order,
        choice(
            (lambda ctx: ctx.get("method") == "card", charge_card),
            (lambda ctx: ctx.get("method") == "bank", charge_bank),
            charge_default,  # bare step = default
        ),
        send_confirmation,
    ],
)
```

Predicates are evaluated in order — first match wins. A bare step (without a tuple) can be passed as the last argument to serve as a default. If no predicate matches, the step fails with a `CHOICE_NO_MATCH` error. Only the matched branch participates in rollback.

## Collection combinators

### Map (collection iteration)

Iterate over a collection and run a function or step per item, concurrently:

```python
from runsheet import map_step

# Function form — mapper(item, ctx) -> result
pipeline = Pipeline(
    name="notify",
    steps=[
        map_step(
            "emails",
            lambda ctx: ctx.get("users", []),
            async lambda user, ctx: {
                "email": user["email"],
                "sent_at": await send_email(user["email"]),
            },
        ),
    ],
)

# Step form — reuse existing steps
pipeline = Pipeline(
    name="process",
    steps=[map_step("results", lambda ctx: ctx.get("items", []), process_item)],
)
```

Items run concurrently via `asyncio.gather`. Results are collected into a list under the given key. In step form, each item is spread into the pipeline context (`{**ctx, **item}`) so the step sees both pipeline-level and per-item values. On partial failure, succeeded items are rolled back (step form only).

### Filter (collection filtering)

```python
from runsheet import filter_step

pipeline = Pipeline(
    name="notify",
    steps=[
        filter_step(
            "eligible",
            lambda ctx: ctx.get("users", []),
            lambda user, ctx: user["opted_in"],
        ),
        map_step("emails", lambda ctx: ctx.get("eligible", []), send_email),
    ],
)

# Async predicate
filter_step(
    "valid",
    lambda ctx: ctx.get("orders", []),
    async lambda order, ctx: (await check_inventory(order["sku"])).available >= order["quantity"],
)
```

Predicates receive `(item, ctx)` and run concurrently. Original order is preserved. If any predicate throws, the step fails. No rollback (filtering is a pure operation).

### FlatMap (collection expansion)

```python
from runsheet import flat_map

pipeline = Pipeline(
    name="process",
    steps=[
        flat_map(
            "line_items",
            lambda ctx: ctx.get("orders", []),
            lambda order, ctx: order["items"],
        ),
    ],
)
```

Maps each item to a list, then flattens one level. Callbacks receive `(item, ctx)` and run concurrently. No rollback (pure operation).

## Dependency injection

No special mechanism needed — pass dependencies as pipeline args and they're available to every step through the accumulated context:

```python
from pydantic import BaseModel, ConfigDict


class Dependencies(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    order_id: str
    stripe: StripeClient
    db: Database


@step(requires=Dependencies, provides=ChargeOutput)
async def charge_payment(ctx: Dependencies) -> ChargeOutput:
    charge = await ctx.stripe.charges.create(amount=ctx.order.total)
    return ChargeOutput(charge_id=charge.id)


pipeline = Pipeline(
    name="checkout",
    steps=[validate_order, charge_payment, send_confirmation],
    args_schema=Dependencies,
)

await pipeline.run(Dependencies(
    order_id="123",
    stripe=stripe_client,
    db=db_client,
))
```

Args persist through the entire pipeline without any step needing to `provides` them. Pydantic validates at every boundary that each step's `requires` are satisfied by the accumulated context. For testing, swap in mocks at the call site.

## Rollback

When a step fails, rollback handlers for all previously completed steps execute in reverse order. Each handler receives the pre-step context snapshot and the step's output:

```python
@step(requires=OrderInput, provides=ReservationOutput)
async def reserve_inventory(ctx: OrderInput) -> ReservationOutput:
    reservation = await inventory.reserve(ctx.order_items)
    return ReservationOutput(reservation_id=reservation.id)


@reserve_inventory.rollback
async def undo_reservation(ctx: OrderInput, output: ReservationOutput) -> None:
    await inventory.release(output.reservation_id)
```

Rollback is best-effort: if a rollback handler raises, remaining rollbacks still execute. The result includes a structured report:

```python
if not result.success:
    result.rollback.completed   # ("charge_payment", "reserve_inventory")
    result.rollback.failed      # (RollbackFailure(step="send_notification", error=...),)
```

## Pipeline result

Every pipeline returns a `PipelineResult` with execution metadata:

```python
# Success
result.success          # True
result.data             # MappingProxyType — accumulated context, read-only
result.meta.pipeline    # "checkout"
result.meta.args        # MappingProxyType — original args snapshot
result.meta.steps_executed  # ("validate_order", "charge_payment", "send_confirmation")
result.meta.steps_skipped   # ()

# Failure
result.success          # False
result.errors           # tuple of exceptions
result.failed_step      # "charge_payment"
result.rollback         # RollbackReport(completed=..., failed=...)
result.meta             # same execution metadata
```

## Middleware

Middleware wraps the full step lifecycle including schema validation:

```python
import time
from runsheet import StepMiddleware


async def timing(step_info, next_fn, ctx):
    start = time.perf_counter()
    result = await next_fn(ctx)
    elapsed = time.perf_counter() - start
    print(f"{step_info.name}: {elapsed:.3f}s")
    return result


async def logging_mw(step_info, next_fn, ctx):
    print(f"-> {step_info.name}")
    result = await next_fn(ctx)
    print(f"<- {step_info.name}")
    return result


pipeline = Pipeline(
    name="checkout",
    steps=[validate_order, charge_payment, send_confirmation],
    middleware=[logging_mw, timing],
)
```

## Error hierarchy

All library errors are `RunsheetError` subclasses with a `code` discriminator. Application exceptions pass through as-is.

```python
from runsheet import RunsheetError, TimeoutError, RetryExhaustedError

if not result.success:
    for error in result.errors:
        if isinstance(error, TimeoutError):
            print(f"Timed out after {error.timeout_seconds}s")
        elif isinstance(error, RetryExhaustedError):
            print(f"Failed after {error.attempts} attempts")
        elif isinstance(error, RunsheetError):
            print(f"Library error: {error.code}")
        else:
            print(f"Application error: {error}")
```

## Strict mode

Detect `provides` key collisions at build time:

```python
pipeline = Pipeline(
    name="checkout",
    steps=[step_a, step_b],  # raises StrictOverlapError if both provide "charge_id"
    strict=True,
)
```

## API reference

### `@step(requires, provides, name, retry, timeout)`

Decorator to create a pipeline step. Returns a `Step` object with a `.rollback` sub-decorator.

| Option     | Type                  | Description                                       |
| ---------- | --------------------- | ------------------------------------------------- |
| `requires` | `type[BaseModel]`     | Optional Pydantic model for required context keys |
| `provides` | `type[BaseModel]`     | Optional Pydantic model for provided context keys |
| `name`     | `str`                 | Step name (defaults to function name)             |
| `retry`    | `RetryPolicy`         | Optional retry policy for transient failures      |
| `timeout`  | `float`               | Optional max duration in seconds                  |

### `Pipeline(name, steps, args_schema, middleware, strict)`

Construct a pipeline from a list of steps.

| Option        | Type                  | Description                                                       |
| ------------- | --------------------- | ----------------------------------------------------------------- |
| `name`        | `str`                 | Pipeline name                                                     |
| `steps`       | `list[PipelineStep]`  | Steps to execute in order                                         |
| `args_schema` | `type[BaseModel]`     | Optional Pydantic model for pipeline input validation             |
| `middleware`  | `list[StepMiddleware]` | Optional middleware                                              |
| `strict`      | `bool`                | Optional — raises at build time if two steps provide the same key |

### `when(predicate, step)`

Conditional execution. The step only runs when the predicate returns `True`.

### `parallel(*steps)`

Run steps concurrently and merge outputs. On partial failure, succeeded inner steps are rolled back.

### `choice(*branches)`

Execute the first branch whose predicate returns `True`. Each branch is a `(predicate, step)` tuple. A bare step as the last argument serves as a default.

### `map_step(key, collection_fn, mapper)`

Iterate over a collection. Mapper receives `(item, ctx)`. Accepts a function or a `Step` (items spread into context). Step form supports rollback on partial failure.

### `filter_step(key, collection_fn, predicate)`

Filter a collection. Predicate receives `(item, ctx)`. Keeps items where predicate returns `True`. Concurrent. No rollback.

### `flat_map(key, collection_fn, mapper)`

Map each item to a list, then flatten one level. Mapper receives `(item, ctx)`. Concurrent. No rollback.

### `StepMiddleware`

```python
async def my_middleware(step_info: StepInfo, next_fn, ctx: dict) -> dict:
    result = await next_fn(ctx)
    return result
```

## Origins

runsheet draws from [sunny/actor](https://github.com/sunny/actor) (Ruby) for the service-object pattern with declared I/O, sequential composition, and rollback, and [@fieldguide/pipeline](https://github.com/fieldguide/pipeline) (TypeScript) for the typed context flow and middleware design. The TypeScript original is [runsheet-js](https://github.com/shaug/runsheet-js).

## LLM-friendly docs

See [llms.txt](llms.txt) for a complete API reference designed for LLM consumption.

## License

MIT
