# runsheet

[![PyPI version](https://img.shields.io/pypi/v/runsheet)](https://pypi.org/project/runsheet/)
[![Python versions](https://img.shields.io/pypi/pyversions/runsheet)](https://pypi.org/project/runsheet/)
[![CI](https://github.com/shaug/runsheet-py/actions/workflows/ci.yml/badge.svg)](https://github.com/shaug/runsheet-py/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Type-safe, composable business logic pipelines for Python.

```python
from pydantic import BaseModel
from runsheet import Pipeline, step

class OrderInput(BaseModel):
    order_id: str
    amount: float

class ValidatedOrder(BaseModel):
    validated: bool

class ChargeOutput(BaseModel):
    charge_id: str

class CheckoutOutput(BaseModel):
    validated: bool
    charge_id: str
    email_sent: bool

@step(requires=OrderInput, provides=ValidatedOrder)
async def validate_order(ctx: OrderInput) -> ValidatedOrder:
    if ctx.amount <= 0:
        raise ValueError("Invalid amount")
    return ValidatedOrder(validated=True)

@step(requires=OrderInput, provides=ChargeOutput)
async def charge_payment(ctx: OrderInput) -> ChargeOutput:
    charge = await stripe.charges.create(amount=ctx.amount)
    return ChargeOutput(charge_id=charge.id)

@charge_payment.rollback
async def undo_charge(ctx: OrderInput, output: ChargeOutput) -> None:
    await stripe.refunds.create(charge=output.charge_id)

checkout = Pipeline(
    name="checkout",
    steps=[validate_order, charge_payment, send_confirmation],
    output=CheckoutOutput,
)

result = await checkout.run(OrderInput(order_id="123", amount=50.0))
# result.data.charge_id — fully typed, validated at pipeline end
```

## Why runsheet

Business logic has a way of growing into tangled, hard-to-test code. A checkout flow starts as one function, then gains validation, payment processing, inventory reservation, email notifications — each with its own failure modes and cleanup logic. Before long you're staring at a 300-line function with nested try/except blocks and no clear way to reuse any of it.

runsheet gives that logic structure. You break work into small, focused steps with explicit inputs and outputs, then compose them into pipelines. Each step is independently testable. The pipeline handles context accumulation — args persist, outputs merge — along with rollback on failure and Pydantic validation at step boundaries. Immutable context boundaries mean steps can't accidentally interfere with each other.

runsheet is for **in-process business logic orchestration** — multi-step flows inside an application service. It is not a distributed workflow engine, job queue, or durable execution runtime. That said, the two are complementary: a runsheet pipeline works well as the business logic inside a [Temporal](https://temporal.io/) activity or an [Inngest](https://www.inngest.com/) function handler.

The name takes its inspiration from the world of stage productions and live broadcast events. A runsheet is the document that sequences every cue, handoff, and contingency so the show runs smoothly. Same idea here: you define the steps, and runsheet makes sure they execute in order with clear contracts between them.

## When to use it

**Good fit:**

- Multi-step business flows — checkout, onboarding, provisioning, data import
- Operations that need compensating actions (rollback) on failure
- Reusable orchestration shared across handlers, jobs, and routes
- Logic where schema-checked boundaries between steps add confidence
- Anywhere you'd otherwise write a long imperative function with a growing number of intermediate variables and try/except blocks

**Not the right tool:**

- Trivial one-off functions that don't benefit from step decomposition
- Long-running durable workflows that survive process restarts — use [Temporal](https://temporal.io/) or [Inngest](https://www.inngest.com/)
- Cross-service event choreography — use a message bus
- Simple CRUD handlers with no orchestration complexity

## What you get

### Typed accumulated context

Args persist and outputs accumulate. Initial arguments flow through the entire pipeline, each step's output merges into the context, and every step sees the full picture of everything before it. Pass `output=` to a pipeline to get a validated Pydantic model as `result.data` with full attribute access and IDE autocomplete, instead of a plain dict.

### Immutable step boundaries

Context is frozen (`MappingProxyType`) at every step boundary — this is a guarantee, not an implementation detail. Each step receives a read-only snapshot and returns only what it adds. Steps cannot mutate shared pipeline state; the pipeline engine manages accumulation. This eliminates a class of bugs where step B accidentally corrupts step A's data, and it makes rollback reliable because each handler receives the exact snapshot from before the step ran.

### Pydantic validation at boundaries

Each step declares `requires` and `provides` as Pydantic models. Pydantic validates context against `requires` before each step and validates output against `provides` after. Mismatches fail fast with clear errors. Your IDE shows exact input and output shapes.

### Rollback with snapshots

On failure, rollback handlers execute in reverse order. Each handler receives the pre-step context snapshot and the step's output, so it knows exactly what to undo. Rollback is best-effort: if a handler raises, remaining rollbacks still execute.

### Compared to alternatives

| Capability                      | Plain functions | Ad hoc orchestration | runsheet                  |
| ------------------------------- | --------------- | -------------------- | ------------------------- |
| Reusable business steps         | Manual          | Manual               | Built-in                  |
| Typed accumulated context       | —               | Manual               | Built-in                  |
| Rollback / compensation         | Manual          | Manual               | Automatic, snapshot-based |
| Schema validation at boundaries | —               | Manual               | Pydantic at every step    |
| Middleware (logging, timing)    | Manual          | Manual               | Built-in                  |
| Control-flow combinators        | Manual          | Manual               | Built-in                  |
| Composable (nest pipelines)     | Manual          | Difficult            | Pipelines are steps       |
| Immutable context boundaries    | —               | Rarely               | Always (`MappingProxyType`) |

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

### Build and run a pipeline

```python
import asyncio
from runsheet import Pipeline, AggregateSuccess, AggregateFailure


class CheckoutOutput(BaseModel):
    validated: bool
    charge_id: str
    email_sent: bool


checkout = Pipeline(
    name="checkout",
    steps=[validate_order, charge_payment, send_confirmation],
    args_schema=OrderInput,
    output=CheckoutOutput,
)

result = await checkout.run(OrderInput(order_id="123", amount=50.0))

if result.success:
    print(result.data.charge_id)   # str — typed attribute access
    print(result.data.email_sent)  # bool
else:
    print(result.error)               # what went wrong
    print(result.rollback)            # RollbackReport(completed=..., failed=...)
```

`pipeline.run()` never raises. It always returns a result — either `AggregateSuccess` or `AggregateFailure`. When `output=` is provided, `result.data` is a validated Pydantic model instance. Without it, `result.data` is a `dict[str, Any]`.

### A second example: workspace onboarding

Steps compose across domains. The same patterns — typed inputs, rollback on failure, accumulated context — apply to any multi-step flow:

```python
from pydantic import BaseModel
from runsheet import Pipeline, step


class OnboardInput(BaseModel):
    owner_email: str
    plan: str


class WorkspaceOutput(BaseModel):
    workspace_id: str


class ResourceOutput(BaseModel):
    bucket_arn: str
    db_url: str


@step(requires=OnboardInput, provides=WorkspaceOutput)
async def create_workspace(ctx: OnboardInput) -> WorkspaceOutput:
    ws = await db.workspaces.create(owner=ctx.owner_email, plan=ctx.plan)
    return WorkspaceOutput(workspace_id=ws.id)


@create_workspace.rollback
async def undo_workspace(ctx: OnboardInput, output: WorkspaceOutput) -> None:
    await db.workspaces.delete(output.workspace_id)


@step(provides=ResourceOutput)
async def provision_resources(ctx: dict) -> ResourceOutput:
    infra = await provisioner.create(ctx["workspace_id"], ctx["plan"])
    return ResourceOutput(bucket_arn=infra.bucket_arn, db_url=infra.db_url)


@provision_resources.rollback
async def undo_resources(ctx: dict, output: ResourceOutput) -> None:
    await provisioner.teardown(output.bucket_arn, output.db_url)


onboard = Pipeline(
    name="onboard_workspace",
    steps=[create_workspace, provision_resources, send_welcome_email],
)

result = await onboard.run(OnboardInput(owner_email="alice@co.com", plan="team"))
```

If `send_welcome_email` fails, provisioned resources are torn down and the workspace is deleted — automatically, in reverse order.

### Pipeline composition

Pipelines are steps — use one pipeline as a step in another:

```python
checkout = Pipeline(
    name="checkout",
    steps=[validate_order, charge_payment, send_confirmation],
)

full_flow = Pipeline(
    name="full_flow",
    steps=[checkout, ship_order, notify_warehouse],
)
```

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

Skipped steps produce no snapshot, no rollback entry, and do not appear in `result.meta.steps_executed`.

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

On partial failure, succeeded inner steps are rolled back before the error propagates. Inner steps retain their own `requires`/`provides` validation, `retry`, and `timeout` behavior. Conditional steps (via `when()`) work inside `parallel()`.

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

### Distribute (collection distribution)

Fan out execution over one or more context collections, binding each item to the step's named scalar inputs. With multiple collections, `distribute` computes the cross product and runs the step once per combination.

```python
from runsheet import distribute

# Single collection — run send_email once per account_id
pipeline = Pipeline(
    name="notify",
    steps=[
        distribute("emails", {"account_ids": "account_id"}, send_email),
    ],
)
# Context: {"org_id": "org-1", "account_ids": ["a1", "a2"]}
# Output:  {"emails": [{"email_id": "email-a1"}, {"email_id": "email-a2"}]}

# Cross product — run once per (account_id, region_id) pair
pipeline = Pipeline(
    name="reports",
    steps=[
        distribute(
            "reports",
            {"account_ids": "account_id", "region_ids": "region_id"},
            generate_report,
        ),
    ],
)
# 2 accounts x 3 regions = 6 concurrent executions
```

The mapping dict connects context array keys to the step's scalar input keys. All non-mapped context keys pass through unchanged. Items run concurrently and support partial-failure rollback.

### Map (iteration)

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
            lambda user, ctx: {
                "email": user["email"],
                "sent_at": str(send_email(user["email"])),
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
```

Predicates receive `(item, ctx)` and run concurrently. Original order is preserved. If any predicate raises, the step fails. No rollback (filtering is a pure operation).

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

## Step result

Every `run()` returns a result with execution metadata:

```python
# Success (AggregateSuccess)
result.success          # True
result.data             # dict (or typed model if output= was set)
result.meta.name        # "checkout"
result.meta.args        # Mapping — original args snapshot
result.meta.steps_executed  # ("validate_order", "charge_payment", "send_confirmation")

# Failure (AggregateFailure)
result.success          # False
result.error            # Exception
result.failed_step      # "charge_payment"
result.rollback         # RollbackReport(completed=..., failed=...)
result.meta             # same execution metadata
```

## Middleware

Middleware wraps the full step lifecycle including schema validation:

```python
import time
from runsheet import StepInfo, StepMiddleware


async def timing(step_info: StepInfo, next_fn, ctx):
    start = time.perf_counter()
    result = await next_fn(ctx)
    elapsed = time.perf_counter() - start
    print(f"{step_info.name}: {elapsed:.3f}s")
    return result


async def logging_mw(step_info: StepInfo, next_fn, ctx):
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
    error = result.error
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

### `Pipeline(name, steps, output, args_schema, middleware, strict)`

Construct a pipeline from a list of steps.

| Option        | Type                   | Description                                                                  |
| ------------- | ---------------------- | ---------------------------------------------------------------------------- |
| `name`        | `str`                  | Pipeline name                                                                |
| `steps`       | `Sequence[Runnable]`   | Steps to execute in order                                                    |
| `output`      | `type[BaseModel]`      | Optional Pydantic model — validates accumulated context and types `result.data` |
| `args_schema` | `type[BaseModel]`      | Optional Pydantic model for pipeline input validation                        |
| `middleware`  | `list[StepMiddleware]` | Optional middleware                                                          |
| `strict`      | `bool`                 | Optional — raises at build time if two steps provide the same key            |

### `when(predicate, step)`

Conditional execution. The step only runs when the predicate returns `True`.

### `parallel(*steps)`

Run steps concurrently and merge outputs. On partial failure, succeeded inner steps are rolled back.

### `choice(*branches)`

Execute the first branch whose predicate returns `True`. Each branch is a `(predicate, step)` tuple. A bare step as the last argument serves as a default.

### `distribute(key, mapping, step)`

Distribute collections from context across a step. The mapping dict connects context array keys to the step's scalar input keys. Runs the step once per item (single mapping) or once per cross-product combination (multiple mappings). Non-mapped context keys pass through. Supports per-item rollback on partial and external failure.

### `map_step(key, collection_fn, mapper)`

Iterate over a collection. Mapper receives `(item, ctx)`. Accepts a function or a `Step` (items spread into context). Step form supports rollback on partial failure.

### `filter_step(key, collection_fn, predicate)`

Filter a collection. Predicate receives `(item, ctx)`. Keeps items where predicate returns `True`. Concurrent. No rollback.

### `flat_map(key, collection_fn, mapper)`

Map each item to a list, then flatten one level. Mapper receives `(item, ctx)`. Concurrent. No rollback.

### `StepMiddleware`

```python
async def my_middleware(step_info: StepInfo, next_fn, ctx: dict) -> StepResult:
    result = await next_fn(ctx)
    return result
```

## Origins

runsheet draws from [sunny/actor](https://github.com/sunny/actor) (Ruby) for the service-object pattern with declared I/O, sequential composition, and rollback, and [@fieldguide/pipeline](https://github.com/fieldguide/pipeline) (TypeScript) for the typed context flow and middleware design. The TypeScript original is [runsheet-js](https://github.com/shaug/runsheet-js).

## LLM-friendly docs

See [llms.txt](llms.txt) for a complete API reference designed for LLM consumption.

## License

MIT
