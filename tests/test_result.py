"""Tests for result types."""

from types import MappingProxyType

from runsheet import (
    EMPTY_ROLLBACK,
    AggregateFailure,
    AggregateMeta,
    AggregateSuccess,
    RollbackFailure,
    RollbackReport,
    StepFailure,
    StepMeta,
    StepSuccess,
)


class TestRollbackTypes:
    def test_rollback_failure(self) -> None:
        err = ValueError("oops")
        rf = RollbackFailure(step="charge", error=err)
        assert rf.step == "charge"
        assert rf.error is err

    def test_rollback_report_defaults(self) -> None:
        rr = RollbackReport()
        assert rr.completed == ()
        assert rr.failed == ()

    def test_rollback_report_with_data(self) -> None:
        rf = RollbackFailure(step="charge", error=ValueError("oops"))
        rr = RollbackReport(completed=("step_a",), failed=(rf,))
        assert rr.completed == ("step_a",)
        assert len(rr.failed) == 1

    def test_empty_rollback_sentinel(self) -> None:
        assert EMPTY_ROLLBACK.completed == ()
        assert EMPTY_ROLLBACK.failed == ()


class TestStepMeta:
    def test_step_meta(self) -> None:
        meta = StepMeta(name="charge", args={"amount": 100})
        assert meta.name == "charge"
        assert meta.args == {"amount": 100}


class TestAggregateMeta:
    def test_meta_defaults(self) -> None:
        meta = AggregateMeta(
            name="test",
            args=MappingProxyType({}),
        )
        assert meta.name == "test"
        assert meta.steps_executed == ()

    def test_meta_with_steps(self) -> None:
        meta = AggregateMeta(
            name="test",
            args={},
            steps_executed=("a", "b"),
        )
        assert meta.steps_executed == ("a", "b")


class TestStepSuccess:
    def test_fields(self) -> None:
        meta = StepMeta(name="charge", args={})
        result = StepSuccess(data={"charge_id": "ch_1"}, meta=meta)
        assert result.success is True
        assert result.data == {"charge_id": "ch_1"}

    def test_frozen(self) -> None:
        meta = StepMeta(name="charge", args={})
        result = StepSuccess(data={}, meta=meta)
        try:
            result.success = False  # type: ignore[misc]
            raise AssertionError("Should have raised")
        except AttributeError:
            pass


class TestStepFailure:
    def test_fields(self) -> None:
        meta = StepMeta(name="charge", args={})
        err = ValueError("fail")
        result = StepFailure(error=err, meta=meta, failed_step="charge")
        assert result.success is False
        assert result.error is err
        assert result.failed_step == "charge"
        assert result.rollback is EMPTY_ROLLBACK


class TestAggregateSuccess:
    def test_success_fields(self) -> None:
        meta = AggregateMeta(
            name="test",
            args=MappingProxyType({}),
            steps_executed=("a", "b"),
        )
        result = AggregateSuccess(data={"charge_id": "ch_1"}, meta=meta)
        assert result.success is True
        assert result.data == {"charge_id": "ch_1"}
        assert result.meta.steps_executed == ("a", "b")

    def test_success_is_frozen(self) -> None:
        meta = AggregateMeta(name="test", args=MappingProxyType({}))
        result = AggregateSuccess(data={}, meta=meta)
        try:
            result.success = False  # type: ignore[misc]
            raise AssertionError("Should have raised")
        except AttributeError:
            pass


class TestAggregateFailure:
    def test_failure_fields(self) -> None:
        meta = AggregateMeta(
            name="test",
            args=MappingProxyType({}),
        )
        err = ValueError("fail")
        result = AggregateFailure(
            error=err,
            meta=meta,
            failed_step="charge",
            rollback=RollbackReport(),
        )
        assert result.success is False
        assert result.error is err
        assert result.failed_step == "charge"

    def test_failure_is_frozen(self) -> None:
        meta = AggregateMeta(name="test", args=MappingProxyType({}))
        result = AggregateFailure(
            error=ValueError("x"),
            meta=meta,
            failed_step="x",
            rollback=RollbackReport(),
        )
        try:
            result.success = True  # type: ignore[misc]
            raise AssertionError("Should have raised")
        except AttributeError:
            pass
