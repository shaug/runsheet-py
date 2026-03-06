"""runsheet — Type-safe, composable business logic pipelines for Python.

Everything users import comes from this module. Internal modules (_prefixed)
are never imported directly.
"""

from runsheet._collections import filter_step, flat_map, map_step
from runsheet._combinators import choice, parallel, when
from runsheet._errors import (
    ArgsValidationError,
    ChoiceNoMatchError,
    PredicateError,
    ProvidesValidationError,
    RequiresValidationError,
    RetryExhaustedError,
    RollbackError,
    RunsheetError,
    RunsheetErrorCode,
    StrictOverlapError,
    TimeoutError,
    UnknownError,
)
from runsheet._middleware import StepInfo, StepMiddleware
from runsheet._pipeline import Pipeline
from runsheet._result import (
    EMPTY_ROLLBACK,
    AggregateFailure,
    AggregateMeta,
    AggregateResult,
    AggregateSuccess,
    RollbackCallback,
    RollbackFailure,
    RollbackReport,
    Runnable,
    StepFailure,
    StepMeta,
    StepResult,
    StepSuccess,
)
from runsheet._step import RetryPolicy, Step, step

__all__ = [
    "EMPTY_ROLLBACK",
    "AggregateFailure",
    "AggregateMeta",
    "AggregateResult",
    "AggregateSuccess",
    "ArgsValidationError",
    "ChoiceNoMatchError",
    "Pipeline",
    "PredicateError",
    "ProvidesValidationError",
    "RequiresValidationError",
    "RetryExhaustedError",
    "RetryPolicy",
    "RollbackCallback",
    "RollbackError",
    "RollbackFailure",
    "RollbackReport",
    "Runnable",
    "RunsheetError",
    "RunsheetErrorCode",
    "Step",
    "StepFailure",
    "StepInfo",
    "StepMeta",
    "StepMiddleware",
    "StepResult",
    "StepSuccess",
    "StrictOverlapError",
    "TimeoutError",
    "UnknownError",
    "choice",
    "filter_step",
    "flat_map",
    "map_step",
    "parallel",
    "step",
    "when",
]
