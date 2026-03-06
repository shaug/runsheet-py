"""Error hierarchy for runsheet.

All library-produced errors are RunsheetError subclasses.
Application exceptions (raised by step run/rollback functions) pass through as-is.
isinstance(e, RunsheetError) distinguishes library errors from application errors.
"""

from __future__ import annotations

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
    """Base class for all runsheet library errors."""

    code: RunsheetErrorCode
    message: str

    def __init__(self, code: RunsheetErrorCode, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(message)


class RequiresValidationError(RunsheetError):
    def __init__(self, message: str) -> None:
        super().__init__(RunsheetErrorCode.REQUIRES_VALIDATION, message)


class ProvidesValidationError(RunsheetError):
    def __init__(self, message: str) -> None:
        super().__init__(RunsheetErrorCode.PROVIDES_VALIDATION, message)


class ArgsValidationError(RunsheetError):
    def __init__(self, message: str) -> None:
        super().__init__(RunsheetErrorCode.ARGS_VALIDATION, message)


class PredicateError(RunsheetError):
    def __init__(self, message: str) -> None:
        super().__init__(RunsheetErrorCode.PREDICATE, message)


class TimeoutError(RunsheetError):
    timeout_seconds: float

    def __init__(self, message: str, *, timeout_seconds: float) -> None:
        self.timeout_seconds = timeout_seconds
        super().__init__(RunsheetErrorCode.TIMEOUT, message)


class RetryExhaustedError(RunsheetError):
    attempts: int
    last_error: Exception

    def __init__(self, message: str, *, attempts: int, last_error: Exception) -> None:
        self.attempts = attempts
        self.last_error = last_error
        super().__init__(RunsheetErrorCode.RETRY_EXHAUSTED, message)


class StrictOverlapError(RunsheetError):
    key: str
    steps: tuple[str, str]

    def __init__(self, message: str, *, key: str, steps: tuple[str, str]) -> None:
        self.key = key
        self.steps = steps
        super().__init__(RunsheetErrorCode.STRICT_OVERLAP, message)


class ChoiceNoMatchError(RunsheetError):
    def __init__(self, message: str) -> None:
        super().__init__(RunsheetErrorCode.CHOICE_NO_MATCH, message)


class RollbackError(RunsheetError):
    causes: tuple[Exception, ...]

    def __init__(
        self, message: str, causes: tuple[Exception, ...] = ()
    ) -> None:
        self.causes = causes
        super().__init__(RunsheetErrorCode.ROLLBACK, message)
        if len(causes) == 1:
            self.__cause__ = causes[0]
        elif len(causes) > 1:
            self.__cause__ = ExceptionGroup(message, list(causes))


class UnknownError(RunsheetError):
    """Wraps non-Exception values that were somehow raised."""

    original_value: object

    def __init__(self, message: str, *, original_value: object) -> None:
        self.original_value = original_value
        super().__init__(RunsheetErrorCode.UNKNOWN, message)
