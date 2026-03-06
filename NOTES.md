# runsheet-py — Implementation Guide

This document is written by the developer of
[runsheet-js](https://github.com/shaug/runsheet-js), the TypeScript original, to
guide a Python implementation. It contains every design decision, lesson learned,
and recommendation for building a Pythonic equivalent. Read this entire document
before writing any code.

## Name and availability

The name `runsheet` is **available on PyPI** (confirmed 2026-03-05). Claim it.

The name comes from stage productions and live broadcast — a runsheet is the
document that sequences every cue, handoff, and fallback for a live event. That
metaphor maps perfectly: steps are cues, context is the shared state sheet,
rollback is the contingency plan.

## Why runsheet exists

Business logic has a way of growing into tangled, hard-to-test code. A checkout
flow starts as one function, then gains validation, payment processing, inventory
reservation, email notifications — each with its own failure modes and cleanup
logic. Before long you're staring at a 300-line function with nested try/except
blocks and no clear way to reuse any of it.

runsheet gives that logic structure. You break work into small, focused steps with
explicit inputs and outputs, then compose them into pipelines. Each step is
independently testable. The pipeline handles context passing, rollback on failure,
and schema validation at every boundary. Immutable data semantics mean steps can't
accidentally interfere with each other.

It's an organizational layer for business logic that encourages reuse, testability,
type safety, and immutable data flow — without the overhead of a full effect system
or workflow engine.

The Python README should lead with this motivating problem. Don't start with API
docs — start with *why this exists*.

## What runsheet is

An in-memory pipeline orchestration library. You define small, focused steps with
explicit inputs and outputs, then compose them into pipelines that handle context
passing, rollback on failure, and schema validation at every boundary.

**It is NOT:**

- A workflow engine (not [Temporal], not [Airflow], not [Prefect], not [Inngest],
  not [AWS Step Functions]). No persistence, no cross-process coordination. Strictly
  local, single-call orchestration.
- An actor system. No message passing, no mailboxes, no supervision trees.
- An effect system. Lighter than Effect-TS or dry-python/returns used alone. Just
  typed pipelines with rollback.

That said, it's **complementary** to those systems — you could use a runsheet
pipeline as a step within a Temporal workflow or an Inngest function.

[Temporal]: https://temporal.io/
[Airflow]: https://airflow.apache.org/
[Prefect]: https://www.prefect.io/
[Inngest]: https://www.inngest.com/
[AWS Step Functions]: https://aws.amazon.com/step-functions/

## Origins and inspirations

runsheet draws from two projects:

- **[sunny/actor]** (Ruby) — the service-object pattern with declared inputs/outputs,
  sequential composition via `play`, rollback on failure, and conditional execution.
  This is the primary design influence for the step model, I/O contracts, and
  rollback semantics.
- **[@fieldguide/pipeline]** (TypeScript) — the three-parameter Arguments → Context →
  Results execution model, the builder pattern for pipeline construction, named
  stages with rollback, and Express-style middleware. This shaped the context
  accumulation model and middleware design.

runsheet takes the best ideas from both: sunny/actor's explicit I/O declarations and
composition ergonomics, combined with fieldguide/pipeline's typed context flow and
middleware system — then rebuilds them on an immutable foundation.

The Python version should credit these inspirations in its docs. The Ruby lineage is
especially relevant — Python developers will relate to sunny/actor's service-object
pattern more directly than TypeScript developers did.

[sunny/actor]: https://github.com/sunny/actor
[@fieldguide/pipeline]: https://github.com/fieldguide/pipeline

## Core dependencies

### Pydantic (replaces Zod)

Pydantic is the schema validation layer. In runsheet-js, Zod is optional — steps
can use TypeScript generics alone for compile-time safety. In Python, **Pydantic
should be required, not optional.** Here's why:

- Python's type system can't enforce context accumulation at check time (no
  intersection types — see "Type system differences" below)
- Pydantic models are the standard way Python libraries express typed data
  contracts
- Pydantic v2 is fast (Rust core), mature, and universally adopted
- Making it optional creates two code paths for marginal benefit

Steps declare `requires` and `provides` as Pydantic model classes. The pipeline
engine validates at each boundary.

### dry-python/returns (replaces composable-functions)

The `returns` library provides the `Result` container type with railway-oriented
programming semantics. Use `Result[SuccessType, FailureType]` as the pipeline's
return type.

Key types from `returns`:

- `Result[T, E]` — the discriminated union (Success or Failure)
- `Success(value)` — wraps successful data
- `Failure(value)` — wraps error data

However, **do not over-couple to returns internally.** The TS version uses
composable-functions' `Result` type but wraps step `run` functions with
`composable()` to catch throws automatically. In Python, consider whether you
even need `returns` as a runtime dependency or just want to adopt its Result
pattern. The alternative is a simple hand-rolled `Result` dataclass:

```python
@dataclass(frozen=True)
class Success(Generic[T]):
    data: T
    errors: tuple[()] = ()
    success: Literal[True] = True

@dataclass(frozen=True)
class Failure:
    errors: tuple[Exception, ...]
    success: Literal[False] = False

PipelineResult = Success[T] | Failure
```

Evaluate both options. The hand-rolled approach avoids a dependency and maps more
directly to the TS version's shape. The `returns` approach provides `map`,
`bind`, `lash`, etc. for free. The TS version doesn't expose those composition
methods on its results, so the hand-rolled version may be more faithful. **Use
your judgment — if `returns` pulls its weight, use it. If it's just providing a
Result container, roll your own.**

## Feature set — complete list

Every feature below exists in runsheet-js and should be implemented in
runsheet-py. The semantics must match unless this document explicitly calls out a
Pythonic divergence.

### 1. defineStep → `@step` decorator

**TS version:** `defineStep({ name, requires, provides, run, rollback, retry, timeout })`

**Python version:** Use a decorator. This is the most natural Python idiom for
wrapping a function with metadata.

```python
from runsheet import step
from pydantic import BaseModel

class OrderInput(BaseModel):
    order_id: str
    amount: float

class ChargeOutput(BaseModel):
    charge_id: str

@step(requires=OrderInput, provides=ChargeOutput)
async def charge_payment(ctx: OrderInput) -> ChargeOutput:
    charge = await stripe.charges.create(amount=ctx.amount)
    return ChargeOutput(charge_id=charge.id)

# Rollback as a decorator on the step
@charge_payment.rollback
async def undo_charge(ctx: OrderInput, output: ChargeOutput) -> None:
    await stripe.refunds.create(charge=output.charge_id)
```

Key decisions:

- **`name` is inferred from the function name.** `charge_payment.__name__` gives
  you `"charge_payment"`. Allow override via `@step(name="custom_name")`.
- **`requires` and `provides` are Pydantic model classes**, not instances. The
  pipeline engine calls `RequiresModel.model_validate(ctx)` before `run` and
  `ProvidesModel.model_validate(output)` after.
- **`run` is the decorated function itself.** The decorator wraps it.
- **`rollback` is attached via a sub-decorator** on the step object, similar to
  Flask's `@app.route` / `@blueprint.before_request` pattern, or Click's
  `@group.command`.
- **Both sync and async `run` functions must be supported.** The pipeline engine
  should normalize sync functions to async internally (wrap in
  `asyncio.to_thread` or just `await` the result if it's not a coroutine).

### 2. Pipeline construction

**TS version:** Two APIs — `buildPipeline({ name, steps, middleware, argsSchema,
strict })` and the fluent builder `createPipeline(name).step(a).step(b).build()`.

**Python version:** The fluent builder doesn't buy much in Python because there's
no progressive type narrowing to leverage (see "Type system differences"). Offer
a single construction API:

```python
from runsheet import Pipeline

checkout = Pipeline(
    name="checkout",
    steps=[validate_order, charge_payment, send_email],
    args_schema=OrderInput,  # optional, Pydantic model class
    middleware=[timing, logging_mw],  # optional
    strict=True,  # optional, default False
)

result = await checkout.run(OrderInput(order_id="123", amount=50.0))
```

The `Pipeline` class is frozen/immutable after construction. Use
`@dataclass(frozen=True)` or `__slots__` + property descriptors.

If there's demand for a builder later, it can be added, but start without it.
The array form is more Pythonic than method chaining.

### 3. Context accumulation

This is the core execution model. It translates directly:

```
context = freeze(initial_args)  # MappingProxyType or frozen model

for step in steps:
    if step is conditional and not predicate(context):
        skip (record in steps_skipped)
    snapshot pre-step context
    validate step.requires against context
    result = await step.run(context)
    if result is failure:
        await rollback(executed_steps, snapshots, outputs)
        return failure with rollback report
    validate result against step.provides
    context = freeze(merge(context, result))

return success with final context
```

**Context representation:** In TS, context is a plain `Record<string, unknown>`.
In Python, use a `dict[str, Any]` internally, but expose it to steps as their
typed Pydantic model (validated via `model_validate`). The merge is
`{**context, **step_output.model_dump()}`.

**Immutability:** In TS, `Object.freeze` provides shallow immutability. In
Python, use `types.MappingProxyType` for the dict view passed to steps (prevents
mutation), or pass the validated Pydantic model directly (Pydantic models can be
frozen via `model_config = ConfigDict(frozen=True)`).

**Key invariant:** Args persist. Initial arguments flow through the entire
pipeline. No step needs to re-provide them. This is what makes dependency
injection free — pass infrastructure deps (DB clients, API clients) as args.

### 4. Rollback

Translates directly. On failure at step N, rollback handlers for steps
N-1...0 execute in reverse order. Each handler receives:

- `ctx`: the frozen context snapshot from *before* that step ran
- `output`: the frozen output that step produced

**Rollback is best-effort.** If a rollback handler raises, still attempt
remaining rollbacks. Collect all rollback errors. Never swallow rollback errors,
never abort remaining rollbacks.

```python
@dataclass(frozen=True)
class RollbackFailure:
    step: str
    error: Exception

@dataclass(frozen=True)
class RollbackReport:
    completed: tuple[str, ...]
    failed: tuple[RollbackFailure, ...]
```

### 5. Retry and timeout

**Retry:**

```python
@step(
    requires=ApiInput,
    provides=ApiOutput,
    retry=RetryPolicy(count=3, delay=0.2, backoff="exponential"),
)
async def call_api(ctx: ApiInput) -> ApiOutput:
    ...

# With retry_if
@step(
    retry=RetryPolicy(
        count=3,
        retry_if=lambda errors: any("ECONNRESET" in str(e) for e in errors),
    ),
)
```

`RetryPolicy` fields:

- `count: int` — max retry attempts (not counting initial)
- `delay: float` — base delay in seconds (default 0)
- `backoff: Literal["linear", "exponential"]` — default "linear"
- `retry_if: Callable[[list[Exception]], bool] | None` — return False to stop

**Timeout:**

```python
@step(timeout=5.0)  # seconds, not milliseconds (Python convention)
async def slow_step(ctx: Input) -> Output:
    ...
```

Use `asyncio.timeout()` (Python 3.11+) or `asyncio.wait_for()`. Unlike the TS
version which uses `Promise.race` (can't cancel the underlying work), Python's
`asyncio.timeout` actually cancels the task via `CancelledError`. This is
**better than the TS version** — document it as an advantage. Each retry attempt
gets its own timeout.

### 6. Parallel steps

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

Use `asyncio.gather(*tasks, return_exceptions=True)` as the equivalent of
`Promise.allSettled`. All inner steps receive the same pre-parallel context.
Outputs merge in list order. On partial failure, succeeded inner steps are rolled
back in reverse order before the error propagates.

Inner steps retain their own requires/provides validation, retry, and timeout.
Conditional steps work inside parallel.

### 7. Choice (branching)

```python
from runsheet import choice

checkout = Pipeline(
    name="checkout",
    steps=[
        validate_order,
        choice(
            (lambda ctx: ctx.method == "card", charge_card),
            (lambda ctx: ctx.method == "bank", charge_bank),
            charge_default,  # bare step = default
        ),
        send_receipt,
    ],
)
```

Predicates evaluated in order, first match wins. Bare step (no tuple) as last
arg = default. No match = `ChoiceNoMatchError`. Only the matched branch
participates in rollback.

**Implementation note from TS:** The TS version uses a `WeakMap` keyed on the
`result.data` object reference to track which branch ran for rollback. This is
fragile — it relies on the pipeline engine storing the exact reference. In
Python, consider a cleaner approach: store the branch index directly on the step
instance per-execution, or use a contextvars-based approach, or simply store
`(branch_index, output)` tuples instead of a WeakMap. The TS approach was a
necessary evil; Python can do better.

### 8. Map (collection iteration)

```python
from runsheet import map_step  # avoid shadowing builtin `map`

# Function form
pipeline = Pipeline(
    name="notify",
    steps=[
        map_step(
            "emails",
            lambda ctx: ctx.users,
            async lambda user, ctx: {"email": user.email, "sent_at": datetime.now()},
        ),
    ],
)

# Step form — reuse existing steps
pipeline = Pipeline(
    name="process",
    steps=[map_step("results", lambda ctx: ctx.items, process_item)],
)
```

**IMPORTANT:** Name this `map_step`, not `map`. Shadowing Python's built-in
`map` would be terrible. Or use `map_each`, `iterate`, or `for_each`. Pick
something that doesn't conflict.

Items run concurrently via `asyncio.gather`. In step form, each item is merged
into context (`{**ctx, **item}`) so the step sees both. On partial failure,
succeeded items are rolled back (step form only).

### 9. Filter (collection filtering)

```python
from runsheet import filter_step  # avoid shadowing builtin `filter`

pipeline = Pipeline(
    name="notify",
    steps=[
        filter_step(
            "eligible",
            lambda ctx: ctx.users,
            lambda user: user.opted_in,
        ),
    ],
)
```

Same naming concern as map. `filter_step`, `keep`, `where`, or `select`.

Predicates run concurrently. Items where predicate returns True are kept.
Original order preserved. No rollback (pure operation). Supports sync and async
predicates.

### 10. FlatMap (collection expansion)

```python
from runsheet import flat_map

pipeline = Pipeline(
    name="process",
    steps=[
        flat_map(
            "line_items",
            lambda ctx: ctx.orders,
            lambda order: order.items,
        ),
    ],
)
```

Map each item to a list, then flatten one level. Function-only (no step form —
map handles that case). Callbacks run concurrently. No rollback (pure operation).

### 11. When (conditional steps)

```python
from runsheet import when

pipeline = Pipeline(
    name="checkout",
    steps=[
        fetch_user,
        charge_payment,
        when(lambda ctx: ctx.amount > 10000, notify_manager),
        send_email,
    ],
)
```

Skipped steps produce no snapshot and no rollback entry. If the predicate raises,
the pipeline treats it as a step failure and triggers rollback.

### 12. Middleware

```python
from runsheet import StepMiddleware

async def timing(step_info, next_fn, ctx):
    start = time.perf_counter()
    result = await next_fn(ctx)
    print(f"{step_info.name}: {time.perf_counter() - start:.3f}s")
    return result

pipeline = Pipeline(
    name="checkout",
    steps=[fetch_user, charge_payment],
    middleware=[timing],
)
```

Middleware wraps the **full step lifecycle** including schema validation. This
means middleware can time validation, catch validation failures, log the full
lifecycle, or short-circuit before validation runs.

First in list = outermost wrapper. Composition via `functools.reduce` (right
fold), same as the TS version's `reduceRight`.

**Combinator inner steps do NOT go through middleware.** Middleware wraps the
composite combinator step as a whole, not the individual inner steps. This is an
explicit design decision — document it.

### 13. Error hierarchy

```python
from enum import StrEnum

class RunsheetErrorCode(StrEnum):
    REQUIRES_VALIDATION = "REQUIRES_VALIDATION"
    PROVIDES_VALIDATION = "PROVIDES_VALIDATION"
    ARGS_VALIDATION = "ARGS_VALIDATION"
    PREDICATE = "PREDICATE"
    TIMEOUT = "TIMEOUT"
    RETRY_EXHAUSTED = "RETRY_EXHAUSTED"
    STRICT_OVERLAP = "STRICT_OVERLAP"
    CHOICE_NO_MATCH = "CHOICE_NO_MATCH"
    ROLLBACK = "ROLLBACK"
    UNKNOWN = "UNKNOWN"

class RunsheetError(Exception):
    code: RunsheetErrorCode
    message: str

class RequiresValidationError(RunsheetError): ...
class ProvidesValidationError(RunsheetError): ...
class ArgsValidationError(RunsheetError): ...
class PredicateError(RunsheetError): ...
class TimeoutError(RunsheetError):
    timeout_seconds: float  # not ms — Python convention
class RetryExhaustedError(RunsheetError):
    attempts: int  # total attempts (initial + retries)
class StrictOverlapError(RunsheetError):
    key: str
    steps: tuple[str, str]
class ChoiceNoMatchError(RunsheetError): ...
class RollbackError(RunsheetError): ...
class UnknownError(RunsheetError):
    original_value: object  # the non-Exception value that was raised
```

**Key contract:** Application exceptions (raised by step `run` or `rollback`
functions) pass through as-is. `RunsheetError` means the library produced it.
`isinstance(e, RunsheetError)` distinguishes library errors from application
errors.

**Python-specific note:** Python allows raising non-Exception objects
(`raise "foo"` is a `TypeError` in Python 3), so the `UnknownError` case is less
common than in JS/TS where you can `throw "foo"`. But it still applies to
edge cases and should be handled via a `to_error()` utility.

### 14. Strict mode

`strict=True` on the pipeline detects provides key collisions at build time.
In TS, this inspects Zod schemas' `.shape` property. In Python, inspect Pydantic
models' `.model_fields` to extract field names.

```python
pipeline = Pipeline(
    name="checkout",
    steps=[step_a, step_b],
    strict=True,  # raises StrictOverlapError if two steps provide the same field
)
```

### 15. PipelineResult

```python
@dataclass(frozen=True)
class PipelineExecutionMeta:
    pipeline: str
    args: Mapping[str, Any]
    steps_executed: tuple[str, ...]
    steps_skipped: tuple[str, ...]

@dataclass(frozen=True)
class PipelineSuccess(Generic[T]):
    success: Literal[True]
    data: T
    errors: tuple[()]
    meta: PipelineExecutionMeta

@dataclass(frozen=True)
class PipelineFailure:
    success: Literal[False]
    errors: tuple[Exception, ...]
    meta: PipelineExecutionMeta
    failed_step: str
    rollback: RollbackReport

PipelineResult = PipelineSuccess[T] | PipelineFailure
```

**pipeline.run() never raises.** It always returns a `PipelineResult`. This is a
core contract. The discriminant is `result.success` (True/False).

### 16. Dependency injection

Same pattern as TS — pass infrastructure deps as pipeline args. They flow into
context and every step can require them:

```python
class Dependencies(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    order_id: str
    stripe: StripeClient
    db: Database

pipeline = Pipeline(
    name="place_order",
    steps=[validate_order, charge_payment],
    args_schema=Dependencies,
)

result = await pipeline.run(Dependencies(
    order_id="123",
    stripe=stripe_client,
    db=db_client,
))
```

For testing, swap in mocks at the call site.

## Type system differences — what translates and what doesn't

### What translates well

- **Pydantic model validation at step boundaries** — strictly better than
  optional Zod in some ways, because Pydantic models are classes with methods
  and inheritance.
- **`isinstance` checks for error discrimination** — works identically to TS.
- **`@dataclass(frozen=True)` for immutable result types** — direct equivalent
  of `Object.freeze`.
- **Async/await** — Python's `asyncio` maps directly to JS promises.
- **Generic types** — `PipelineResult[T]` works with mypy/pyright.

### What does NOT translate

- **Progressive type narrowing in the builder.** In TS,
  `.step(a).step(b).step(c)` produces a type that's the intersection of all step
  outputs. Python has NO intersection types. This is tracked as
  [python/typing#213](https://github.com/python/typing/issues/213) with no
  resolution in sight. **Do not try to replicate this.** Instead:
  - Have users declare the full output type as a Pydantic model or TypeVar
  - Rely on runtime Pydantic validation to catch mismatches
  - Accept that the builder pattern has less value without type narrowing
- **Phantom brands.** The TS version uses `unique symbol` brands on `TypedStep`
  for compile-time type tracking. Python has no equivalent. Steps are just
  objects with known attributes — no phantom types needed because you're not
  doing type-level accumulation.
- **Union-to-intersection type mapping.** `buildPipeline` in TS infers the full
  output type from the step array. Python can't do this. The output type is
  either explicitly declared or `dict[str, Any]`.
- **`as const` literal narrowing.** Python `Literal` types serve a similar
  purpose for discriminants (`success: Literal[True]`) but are less flexible.

### The pragmatic approach

Accept that Python runsheet will have **strong runtime validation** (Pydantic at
every boundary) but **weaker static analysis** than the TS version. This is fine.
The Python ecosystem generally leans on runtime checks more than TS does. Lean
into that strength rather than fighting for compile-time guarantees the type
system can't deliver.

Where you CAN provide good static typing:

- Step input/output types (via Pydantic model annotations)
- Pipeline result discrimination (`if result.success:`)
- Error hierarchy (`isinstance(e, TimeoutError)`)
- Middleware signatures

Where you should NOT try to force it:

- Accumulated context type across steps
- Progressive builder narrowing
- Compile-time "does this step's requires match prior provides"

## Pythonic idioms to adopt

### Decorators over factory functions

TS uses `defineStep({...})` because JS/TS doesn't have decorators that work this
way. Python does. `@step(requires=X, provides=Y)` is more natural and more
discoverable.

### Snake case everywhere

`defineStep` → `step` (decorator), `buildPipeline` → `Pipeline(...)`,
`chargePayment` → `charge_payment`, `retryIf` → `retry_if`,
`stepsExecuted` → `steps_executed`, `failedStep` → `failed_step`.

### Seconds, not milliseconds

Python conventions use seconds for timeouts and delays. `timeout=5.0` not
`timeout=5000`. `delay=0.2` not `delay=200`.

### asyncio.timeout over Promise.race

Python 3.11+ has `asyncio.timeout()` which actually cancels the task. This is
**superior to the TS version** which uses `Promise.race` and can't cancel the
underlying work. Document this as a Python advantage.

### asyncio.gather over Promise.allSettled

For parallel execution, `asyncio.gather(*tasks, return_exceptions=True)` gives
you the same "run all, collect results and errors" semantics.

### Context managers for resource cleanup

Consider whether `Pipeline` should support `async with` for pipelines that
manage resources. This is a natural Python extension that doesn't exist in TS.
Not required for v1, but worth considering.

### Enum over string union

Use `StrEnum` for `RunsheetErrorCode` instead of a string literal union. Python
enums are more powerful (iteration, membership testing).

### Type narrowing via isinstance

Python's `isinstance` is the equivalent of TS's type guard. Use it for result
discrimination:

```python
match result:
    case PipelineSuccess() as s:
        print(s.data)
    case PipelineFailure() as f:
        print(f.errors, f.rollback)
```

Or with structural pattern matching (Python 3.10+), which is even more Pythonic.

## Comparison to prior art (for Python docs)

The TS project's OVERVIEW.md has a comparison table. The Python version should
include an equivalent in its own docs, updated for the Python ecosystem:

| Feature | sunny/actor (Ruby) | runsheet (Python) | Prefect/Airflow |
|---|---|---|---|
| Declared I/O | `input`/`output` macros | Pydantic `requires`/`provides` | Task decorators |
| Sequential composition | `play A, B, C` | `Pipeline(steps=[...])` | DAG dependencies |
| Shared context | Mutable result object | Immutable accumulation | XComs / task results |
| Rollback | `def rollback` | Snapshot-verified rollback | Not built-in |
| Middleware | Not built-in | Built-in | Hooks/callbacks |
| Conditional steps | `if:`/`unless:` lambdas | `when(predicate, step)` | `@task.branch` |
| Branching | Not supported | `choice([pred, step], ...)` | Branch operator |
| Collection mapping | Not supported | `map_step(key, collection, fn)` | Dynamic task mapping |
| Scope | In-memory, single call | In-memory, single call | Distributed, persistent |

This table helps users understand where runsheet fits. The key differentiator from
Prefect/Airflow: runsheet is in-memory, single-call, no persistence. It's
complementary, not competitive.

## Architecture decisions that transfer directly

These decisions were hard-won in the TS version. Don't re-litigate them:

1. **Args persist and outputs accumulate.** Initial args flow through the entire
   pipeline. Each step's output merges into context.

2. **Steps signal failure by raising.** The pipeline catches exceptions and
   returns a Result. Steps should NOT return Result objects directly.

3. **Context is immutable at every step boundary.** Steps receive a readonly
   snapshot and return only what they add.

4. **Rollback is best-effort.** If a rollback handler raises, remaining rollbacks
   still execute. All rollback errors are collected.

5. **Last writer wins for context keys.** If two steps both provide `status`,
   the later step's value is used. `strict=True` detects this at build time.

6. **Middleware wraps the full step lifecycle** including schema validation.

7. **Skipped steps don't exist** in the rollback loop. No snapshot, no rollback
   entry. Tracked in metadata for debugging.

8. **Combinator inner steps bypass middleware.** Middleware wraps the composite
   step, not the individual inner steps.

9. **Error hierarchy with discriminated `code` property.** Application errors
   pass through as-is. Library errors are all `RunsheetError` subclasses.

10. **pipeline.run() never raises.** Always returns a result.

## Lessons learned during TS development

These are mistakes we made and corrected. Don't repeat them:

1. **Don't make the schema layer optional.** We started with Zod as optional in
   TS because TypeScript generics alone provide compile-time safety. Python
   doesn't have that luxury. Make Pydantic required.

2. **Error subclasses from day one.** We started with a single `RunsheetError`
   class and a `code` string. We later added subclasses
   (`TimeoutError`, `RetryExhaustedError`, etc.) with additional state
   (`timeout_ms`, `attempts`, etc.). Start with the full hierarchy.

3. **Centralize error normalization.** We had the equivalent of
   `err if isinstance(err, Exception) else Exception(str(err))` repeated in 8+
   places before extracting a `to_error()` utility. Write it once from the start.

4. **Shared combinator execution path.** parallel, choice, and map all need the
   same inner step lifecycle (validate requires → run → validate provides). We
   extracted `run_inner_step()` into an `_internal` module. Do this from the
   start.

5. **Document WeakMap/reference invariants.** The TS version uses WeakMaps in
   choice and map to track which branch/items ran for rollback, keyed on the
   exact `result.data` object reference. This is fragile. Python can do better —
   use a dict keyed on `id()`, or store the metadata directly on the execution
   state. Don't replicate the WeakMap pattern.

6. **Cast audit.** We did a pass removing all unnecessary type casts. In Python,
   the equivalent is: don't use `typing.cast()` or `# type: ignore` unless
   mypy/pyright actually fails. Start clean.

7. **Lint and format from day one.** Use `ruff` for both linting and formatting.
   Single tool, fast, covers everything. Don't use separate tools for lint vs
   format.

## Project structure

```
runsheet-py/
├── pyproject.toml          # project metadata, dependencies, tool config
├── src/
│   └── runsheet/
│       ├── __init__.py     # public API exports
│       ├── _step.py        # @step decorator and Step class
│       ├── _pipeline.py    # Pipeline class and execution engine
│       ├── _rollback.py    # rollback execution
│       ├── _middleware.py   # middleware types and composition
│       ├── _combinators.py # parallel, choice, when (or split into files)
│       ├── _collections.py # map_step, filter_step, flat_map
│       ├── _errors.py      # RunsheetError hierarchy
│       ├── _result.py      # PipelineResult, PipelineSuccess, PipelineFailure
│       ├── _internal.py    # shared utilities (to_error, validate_schema, etc.)
│       └── py.typed        # PEP 561 marker
├── tests/
│   ├── test_step.py
│   ├── test_pipeline.py
│   ├── test_rollback.py
│   ├── test_middleware.py
│   ├── test_parallel.py
│   ├── test_choice.py
│   ├── test_map.py
│   ├── test_filter.py
│   ├── test_flat_map.py
│   └── test_when.py
├── docs/
│   └── OVERVIEW.md
├── README.md
├── llms.txt
└── NOTES.md                # this file (can be removed before release)
```

Use the `src` layout. Prefix internal modules with `_` (Python convention for
"private, don't import directly"). Export everything public from `__init__.py`.

### Complete public API (`__init__.py` exports)

Everything users import comes from `runsheet`. Internal modules are never imported
directly. Here's the definitive list, mapped from the TS `index.ts`:

**Functions / decorators:**
- `step` — decorator (replaces `defineStep`)
- `Pipeline` — constructor (replaces `buildPipeline`)
- `when` — conditional step wrapper
- `parallel` — concurrent step composition
- `choice` — branching
- `map_step` — collection iteration (name TBD, see open questions)
- `filter_step` — collection filtering (name TBD)
- `flat_map` — collection expansion

**Error classes:**
- `RunsheetError` — base class
- `RequiresValidationError`
- `ProvidesValidationError`
- `ArgsValidationError`
- `PredicateError`
- `TimeoutError`
- `RetryExhaustedError`
- `StrictOverlapError`
- `ChoiceNoMatchError`
- `RollbackError`
- `UnknownError`
- `RunsheetErrorCode` — StrEnum

**Result types:**
- `PipelineResult` — union type
- `PipelineSuccess`
- `PipelineFailure`
- `PipelineExecutionMeta`
- `RollbackReport`
- `RollbackFailure`

**Step types:**
- `Step` — base protocol/type
- `RetryPolicy` — dataclass
- `StepMiddleware` — callable protocol

**Key principle:** Users should never need to import from internal modules or from
Pydantic/returns directly for runsheet-specific types. If the Python version uses
`dry-python/returns`, re-export `Result`, `Success`, `Failure` from `runsheet` so
users have a single import source. The TS version does exactly this with
`composable-functions`.

## Tooling

- **Python 3.11+** — required for `asyncio.TaskGroup`, `asyncio.timeout`,
  `StrEnum`, `ExceptionGroup`
- **uv** — package manager (fast, modern, replaces pip/poetry/pdm)
- **ruff** — linting and formatting (replaces flake8, black, isort)
- **pyright** — type checker (faster and stricter than mypy — don't offer both)
- **pytest** + **pytest-asyncio** + **pytest-cov** — testing and coverage
- **pydantic v2** — schema validation
- **hatchling** — build backend
- **pre-commit** — git hooks

See the "Packaging and distribution" section below for the complete tooling stack
table with rationale.

### pyproject.toml skeleton

See the complete pyproject.toml in the "Packaging and distribution" section below.
It includes build-system, classifiers, license, URLs, hatch config, and all tool
configuration. Use that version — it's production-ready.

## Target API — complete example

This is what the finished library should feel like to a user:

```python
import asyncio
from pydantic import BaseModel
from runsheet import (
    step, Pipeline, when, parallel, choice,
    map_step, filter_step, flat_map,
    PipelineResult, RunsheetError, TimeoutError,
    RetryPolicy,
)

# --- Models ---

class OrderInput(BaseModel):
    order_id: str
    amount: float
    method: str

class ValidatedOrder(BaseModel):
    validated: bool

class ChargeOutput(BaseModel):
    charge_id: str

class ReservationOutput(BaseModel):
    reservation_id: str

class EmailOutput(BaseModel):
    email_sent: bool

# --- Steps ---

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
async def charge_card(ctx: OrderInput) -> ChargeOutput:
    # ... charge logic
    return ChargeOutput(charge_id="ch_123")

@charge_card.rollback
async def undo_charge(ctx: OrderInput, output: ChargeOutput) -> None:
    # ... refund logic
    pass

@step(requires=OrderInput, provides=ChargeOutput)
async def charge_bank(ctx: OrderInput) -> ChargeOutput:
    return ChargeOutput(charge_id="bank_123")

@step(provides=ReservationOutput)
async def reserve_inventory(ctx) -> ReservationOutput:
    return ReservationOutput(reservation_id="res_123")

@step(provides=EmailOutput)
async def send_email(ctx) -> EmailOutput:
    return EmailOutput(email_sent=True)

@step(provides=EmailOutput)
async def notify_manager(ctx) -> EmailOutput:
    return EmailOutput(email_sent=True)

# --- Pipeline ---

checkout = Pipeline(
    name="checkout",
    steps=[
        validate_order,
        choice(
            (lambda ctx: ctx["method"] == "card", charge_card),
            (lambda ctx: ctx["method"] == "bank", charge_bank),
        ),
        parallel(reserve_inventory, send_email),
        when(lambda ctx: ctx["amount"] > 10000, notify_manager),
    ],
    args_schema=OrderInput,
)

# --- Execution ---

async def main():
    result = await checkout.run(
        OrderInput(order_id="123", amount=50.0, method="card")
    )

    if result.success:
        print(f"Charge: {result.data['charge_id']}")
        print(f"Steps: {result.meta.steps_executed}")
    else:
        print(f"Failed at: {result.failed_step}")
        print(f"Errors: {result.errors}")
        print(f"Rollback: {result.rollback}")

asyncio.run(main())
```

## What to build first

Suggested implementation order (each builds on the previous):

1. **Error hierarchy** (`_errors.py`) — no dependencies, used everywhere
2. **Result types** (`_result.py`) — PipelineResult, Success, Failure
3. **Step decorator** (`_step.py`) — `@step`, sync/async support, rollback
4. **Internal utilities** (`_internal.py`) — `to_error`, `validate_schema`,
   `run_inner_step`
5. **Pipeline engine** (`_pipeline.py`) — sequential execution, context
   accumulation, rollback, middleware
6. **When** — conditional steps (simplest combinator)
7. **Parallel** — concurrent execution
8. **Choice** — branching
9. **Map** — collection iteration (function + step forms)
10. **Filter** — collection filtering
11. **FlatMap** — collection expansion
12. **Middleware** — if not done as part of step 5

Write tests alongside each step. The TS test files are a direct guide for what
to test — the scenarios translate 1:1.

## Packaging and distribution

The TS project has mature packaging infrastructure. Here's the complete mapping to
Python equivalents. Get this right from day one — retrofitting packaging is painful.

### Tooling stack (best-of-breed, 2026)

| Concern | Tool | Why |
|---|---|---|
| Package manager | **uv** | Fastest Python package manager. Manages deps, lockfiles, Python versions, virtualenvs. Replaces pip, pip-tools, pyenv, virtualenv. |
| Type checker | **pyright** | Faster and stricter than mypy. Better inference, better error messages. Don't offer both — pick one. |
| Lint + format | **ruff** | Single Rust binary replaces black, isort, flake8, pyupgrade, and dozens of plugins. Sub-second on large codebases. |
| Testing | **pytest** + **pytest-asyncio** | pytest is the only serious choice. `asyncio_mode = "auto"` avoids `@pytest.mark.asyncio` on every test. |
| Coverage | **pytest-cov** | Wraps coverage.py. Enforce `fail_under = 90` in CI. For a library with rollback/combinator branches, coverage catches dead code. |
| Build backend | **hatchling** | Zero-config for src layout. Lightweight, maintained by PyPA. |
| Hooks | **pre-commit** | Standard Python hook manager. Replaces husky + lint-staged. |
| Multi-version testing | **uv** (CI matrix) | `uv run --python 3.12 pytest` auto-fetches interpreters. Let CI be the matrix authority (3.11/3.12/3.13). Skip nox/tox unless pain emerges. |

Intentionally absent:
- **mypy** — pyright is strictly better for this use case
- **black** — ruff format replaced it
- **tox/nox** — uv handles multi-version; CI is the real matrix
- **setuptools** — hatchling is lighter and modern
- **sphinx/mkdocs** — README + llms.txt until API surface warrants dedicated docs

### Complete pyproject.toml

The skeleton above is missing critical sections. Here's the full version:

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "runsheet"
version = "0.1.0"
description = "Type-safe, composable business logic pipelines for Python"
readme = "README.md"
license = "MIT"
requires-python = ">=3.11"
authors = [{ name = "Your Name", email = "you@example.com" }]
keywords = [
    "pipeline",
    "workflow",
    "orchestration",
    "typed",
    "pydantic",
    "rollback",
    "composition",
]
classifiers = [
    "Development Status :: 3 - Alpha",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: MIT License",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
    "Programming Language :: Python :: 3.13",
    "Typing :: Typed",
    "Framework :: Pydantic :: 2",
    "Topic :: Software Development :: Libraries",
]
dependencies = [
    "pydantic>=2.0",
]

[project.urls]
Homepage = "https://github.com/shaug/runsheet-py"
Documentation = "https://github.com/shaug/runsheet-py#readme"
Repository = "https://github.com/shaug/runsheet-py"
Issues = "https://github.com/shaug/runsheet-py/issues"
Changelog = "https://github.com/shaug/runsheet-py/blob/main/CHANGELOG.md"

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=0.24",
    "ruff>=0.8",
    "pyright>=1.1",
    "pre-commit>=4.0",
    "pytest-cov>=6.0",
]

[tool.hatch.build.targets.wheel]
packages = ["src/runsheet"]

[tool.ruff]
target-version = "py311"
src = ["src"]

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B", "SIM", "RUF"]

[tool.ruff.format]
docstring-code-format = true

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]

[tool.coverage.run]
source = ["runsheet"]

[tool.coverage.report]
fail_under = 90
show_missing = true
exclude_lines = [
    "pragma: no cover",
    "if TYPE_CHECKING:",
]

[tool.pyright]
pythonVersion = "3.11"
typeCheckingMode = "strict"
include = ["src"]
```

### Build backend: Hatchling

The TS project uses `tsup` (esbuild) to produce CJS + ESM bundles. Python doesn't
need a compile step, but it does need a build backend to create sdists and wheels.
**Hatchling** is the recommended choice:

- Zero config for src layout projects
- Fast, lightweight, maintained by the PyPA
- No setup.py or setup.cfg needed
- Alternatives: `setuptools` (heavier), `flit` (simpler but less flexible),
  `pdm-backend`

### PEP 561: py.typed marker

For type checkers (pyright, mypy) to recognize your package as typed, include an
empty marker file:

```
src/runsheet/py.typed    # empty file, must be present
```

This is the Python equivalent of the TS project's `"types"` field in package.json.
Without it, downstream users get no type checking for your library.

### What goes in the wheel

The TS project's package.json has:
```json
"files": ["dist", "llms.txt"]
```

Python equivalent via hatch config above (`packages = ["src/runsheet"]`). The wheel
will contain:
- `runsheet/` package directory (all `.py` files)
- `runsheet/py.typed` marker
- Package metadata

For the sdist (source distribution), hatch includes everything tracked by git by
default. To explicitly include `llms.txt` in the wheel, add:

```toml
[tool.hatch.build.targets.wheel.force-include]
"llms.txt" = "runsheet/llms.txt"
```

### License file

Create a `LICENSE` file at the project root with the MIT license text. The
`license = "MIT"` field in pyproject.toml references the SPDX identifier; the
actual license text goes in the file.

### CI/CD: GitHub Actions

The TS project has two workflows:

**1. CI (`ci.yml`)** — runs on every push/PR, matrix tests across Node 20/22/24/25.

Python equivalent:

```yaml
# .github/workflows/ci.yml
name: CI

on:
  push:
    branches: [main]
  pull_request:

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ["3.11", "3.12", "3.13"]

    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v5
      - run: uv python install ${{ matrix.python-version }}
      - run: uv sync --dev
      - run: uv run ruff check .
      - run: uv run ruff format --check .
      - run: uv run pyright
      - run: uv run pytest --cov --cov-report=term-missing

  # Optional: test on multiple OS
  # os: [ubuntu-latest, macos-latest, windows-latest]
```

**2. Release (`release.yml`)** — release-please + publish in a single workflow.

The TS version has both jobs in one file: release-please creates the GitHub release,
then publish runs only when a release was created. The publish job **re-runs lint,
typecheck, and tests** before building and publishing — this is a safety gate.

Python equivalent using trusted publishers (PyPI's recommended auth method — no
API tokens needed):

```yaml
# .github/workflows/release.yml
name: Release

on:
  push:
    branches: [main]

permissions:
  contents: write
  pull-requests: write
  id-token: write  # Required for trusted publishing

jobs:
  release-please:
    runs-on: ubuntu-latest
    outputs:
      release_created: ${{ steps.release.outputs.release_created }}
    steps:
      - uses: googleapis/release-please-action@v4
        id: release
        with:
          release-type: python

  publish:
    needs: release-please
    if: ${{ needs.release-please.outputs.release_created }}
    runs-on: ubuntu-latest
    environment: pypi
    permissions:
      id-token: write
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v5
      - run: uv sync --dev
      - run: uv run ruff check .
      - run: uv run ruff format --check .
      - run: uv run pyright
      - run: uv run pytest --cov --cov-report=term-missing
      - run: uv build
      - uses: pypa/gh-action-pypi-publish@release/v1
        # No credentials needed — uses OIDC trusted publishing
```

To set up trusted publishers, configure the PyPI project to trust the GitHub
repository + workflow + environment. This is done once in the PyPI project settings
and is more secure than API tokens.

### Pre-commit hooks

The TS project uses husky + lint-staged + commitlint. Python equivalent:

**pre-commit** (the standard Python hook manager):

```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.8.0
    hooks:
      - id: ruff        # lint
        args: [--fix]
      - id: ruff-format # format

  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v5.0.0
    hooks:
      - id: trailing-whitespace
      - id: end-of-file-fixer
      - id: check-yaml
      - id: check-toml

  # Optional: markdown linting (equivalent to markdownlint-cli2)
  - repo: https://github.com/markdownlint/markdownlint
    rev: v0.16.0
    hooks:
      - id: markdownlint

  # Optional: commit message linting (equivalent to commitlint)
  - repo: https://github.com/commitizen-tools/commitizen
    rev: v4.1.0
    hooks:
      - id: commitizen
```

Install with: `pre-commit install`

**Note:** The TS project's husky pre-commit hook runs lint-staged, typecheck, AND
the full test suite on every commit. This is aggressive but ensures nothing broken
gets committed. In Python, add a local hook to `.pre-commit-config.yaml` for the
same effect:

```yaml
  - repo: local
    hooks:
      - id: typecheck
        name: pyright
        entry: uv run pyright
        language: system
        pass_filenames: false
      - id: tests
        name: pytest
        entry: uv run pytest -x -q
        language: system
        pass_filenames: false
```

The TS project's `scripts/lint.sh` consolidates linting into one command. In Python,
ruff handles both linting and formatting, so the equivalent is just:

```bash
ruff check . && ruff format --check .
```

### Changelog

The TS project uses release-please to auto-generate `CHANGELOG.md` from
conventional commits. This works identically for Python projects — release-please
is language-agnostic. Use conventional commit messages (`feat:`, `fix:`, `chore:`,
etc.) and release-please generates the changelog automatically.

### README structure

Mirror the TS project's README structure:
1. One-line description + badges (PyPI version, Python versions, CI status, license)
2. Install command (`pip install runsheet` / `uv add runsheet`)
3. Quick example (30 lines max)
4. Core concepts (brief)
5. API reference (each function/decorator with examples)
6. Link to llms.txt for AI consumption

### llms.txt

The TS project maintains `llms.txt` — a plain-text file designed for LLM
consumption that summarizes the entire API surface. Create an equivalent for the
Python version. This file should:

- Be included in the published package (see wheel force-include above)
- Cover every public function, class, and type
- Include concise usage examples
- Be maintained alongside the code (update when the API changes)
- Be referenced from the README

### Package manager: uv

Use **uv** as the package manager (Python's equivalent of pnpm):

- `uv init` to bootstrap
- `uv add pydantic` for dependencies
- `uv add --dev pytest ruff pyright` for dev dependencies
- `uv sync` to install from lockfile
- `uv run pytest` to run in the virtualenv
- `uv build` to create sdist + wheel
- `uv publish` to upload to PyPI

uv generates a `uv.lock` lockfile (equivalent to `pnpm-lock.yaml`). Commit it —
it ensures reproducible installs in CI.

### Version management

Two options:

1. **Static version in pyproject.toml** — simplest. Release-please bumps it
   automatically via PR. This is what the TS project does with package.json.

2. **Dynamic version from git tags** — using `hatch-vcs` plugin. More complex,
   avoid unless you have a specific reason.

Recommend option 1 for simplicity.

### Files checklist

Before first publish, ensure these exist:

```
pyproject.toml          # complete (see above)
LICENSE                 # MIT license text
README.md               # with badges, install, quick example
CHANGELOG.md            # can start empty, release-please fills it
llms.txt                # LLM-friendly API reference
src/runsheet/__init__.py  # public API re-exports
src/runsheet/py.typed     # empty PEP 561 marker
.pre-commit-config.yaml   # hooks config
.github/workflows/ci.yml  # CI pipeline
.github/workflows/release.yml  # publish to PyPI
uv.lock                  # committed lockfile
```

## Open questions for the implementer

1. **`returns` or hand-rolled Result?** Evaluate whether `dry-python/returns`
   pulls its weight or if a simple dataclass Result is cleaner. The TS version
   uses composable-functions' Result but only for the container shape, not for
   its `map`/`bind` methods.

2. **Naming for map/filter.** `map_step`/`filter_step` avoid shadowing builtins
   but are verbose. `map_each`/`keep`/`for_each`/`where`/`select` are
   alternatives. Pick names that feel natural in pipeline step arrays.

3. **Context type for predicate lambdas.** In `when(lambda ctx: ctx.amount >
   10000, ...)`, what is `ctx`? A `dict[str, Any]`? A Pydantic model? In the TS
   version it's a plain object. In Python, a dict is most flexible but loses
   autocompletion. A Pydantic model is typed but requires knowing the accumulated
   shape. Consider a `MappingProxyType` or just a plain dict with attribute
   access (e.g., via `types.SimpleNamespace` or a thin wrapper).

4. **Sync step support.** The TS version supports both sync and async `run`
   functions. In Python, the pipeline engine is async. Sync steps need to be
   wrapped. `asyncio.to_thread` offloads to a thread pool (good for blocking
   I/O). For pure-compute sync steps, just calling them directly in the async
   context is fine. Decide the default behavior.
