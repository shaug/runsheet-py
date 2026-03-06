"""Tests for error hierarchy."""

from runsheet import (
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


class TestRunsheetErrorCode:
    def test_all_codes_are_str(self) -> None:
        for code in RunsheetErrorCode:
            assert isinstance(code, str)
            assert isinstance(code.value, str)

    def test_code_values(self) -> None:
        assert RunsheetErrorCode.REQUIRES_VALIDATION == "REQUIRES_VALIDATION"
        assert RunsheetErrorCode.TIMEOUT == "TIMEOUT"
        assert RunsheetErrorCode.UNKNOWN == "UNKNOWN"


class TestRunsheetError:
    def test_base_error_is_exception(self) -> None:
        err = RunsheetError(RunsheetErrorCode.UNKNOWN, "test")
        assert isinstance(err, Exception)
        assert err.code == RunsheetErrorCode.UNKNOWN
        assert err.message == "test"
        assert str(err) == "test"

    def test_isinstance_distinguishes_library_errors(self) -> None:
        lib_err = RequiresValidationError("bad input")
        app_err = ValueError("application error")
        assert isinstance(lib_err, RunsheetError)
        assert not isinstance(app_err, RunsheetError)


class TestErrorSubclasses:
    def test_requires_validation(self) -> None:
        err = RequiresValidationError("missing field")
        assert err.code == RunsheetErrorCode.REQUIRES_VALIDATION
        assert isinstance(err, RunsheetError)

    def test_provides_validation(self) -> None:
        err = ProvidesValidationError("bad output")
        assert err.code == RunsheetErrorCode.PROVIDES_VALIDATION

    def test_args_validation(self) -> None:
        err = ArgsValidationError("bad args")
        assert err.code == RunsheetErrorCode.ARGS_VALIDATION

    def test_predicate(self) -> None:
        err = PredicateError("pred failed")
        assert err.code == RunsheetErrorCode.PREDICATE

    def test_timeout(self) -> None:
        err = TimeoutError("timed out", timeout_seconds=5.0)
        assert err.code == RunsheetErrorCode.TIMEOUT
        assert err.timeout_seconds == 5.0

    def test_retry_exhausted(self) -> None:
        cause = ValueError("fail")
        err = RetryExhaustedError("retries done", attempts=3, last_error=cause)
        assert err.code == RunsheetErrorCode.RETRY_EXHAUSTED
        assert err.attempts == 3
        assert err.last_error is cause

    def test_strict_overlap(self) -> None:
        err = StrictOverlapError("overlap", key="status", steps=("step_a", "step_b"))
        assert err.code == RunsheetErrorCode.STRICT_OVERLAP
        assert err.key == "status"
        assert err.steps == ("step_a", "step_b")

    def test_choice_no_match(self) -> None:
        err = ChoiceNoMatchError("no match")
        assert err.code == RunsheetErrorCode.CHOICE_NO_MATCH

    def test_rollback(self) -> None:
        err = RollbackError("rollback failed")
        assert err.code == RunsheetErrorCode.ROLLBACK

    def test_unknown(self) -> None:
        err = UnknownError("weird", original_value="not an exception")
        assert err.code == RunsheetErrorCode.UNKNOWN
        assert err.original_value == "not an exception"
