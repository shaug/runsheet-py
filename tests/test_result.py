"""Tests for result types."""

from types import MappingProxyType

from runsheet import (
    PipelineExecutionMeta,
    PipelineFailure,
    PipelineSuccess,
    RollbackFailure,
    RollbackReport,
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


class TestPipelineExecutionMeta:
    def test_meta_defaults(self) -> None:
        meta = PipelineExecutionMeta(
            pipeline="test",
            args=MappingProxyType({}),
        )
        assert meta.pipeline == "test"
        assert meta.steps_executed == ()
        assert meta.steps_skipped == ()


class TestPipelineSuccess:
    def test_success_fields(self) -> None:
        meta = PipelineExecutionMeta(
            pipeline="test",
            args=MappingProxyType({}),
            steps_executed=("a", "b"),
        )
        result = PipelineSuccess(data={"charge_id": "ch_1"}, meta=meta)
        assert result.success is True
        assert result.data == {"charge_id": "ch_1"}
        assert result.errors == ()
        assert result.meta.steps_executed == ("a", "b")

    def test_success_is_frozen(self) -> None:
        meta = PipelineExecutionMeta(pipeline="test", args=MappingProxyType({}))
        result = PipelineSuccess(data={}, meta=meta)
        try:
            result.success = False  # type: ignore[misc]
            raise AssertionError("Should have raised")
        except AttributeError:
            pass


class TestPipelineFailure:
    def test_failure_fields(self) -> None:
        meta = PipelineExecutionMeta(
            pipeline="test",
            args=MappingProxyType({}),
        )
        err = ValueError("fail")
        result = PipelineFailure(
            errors=(err,),
            meta=meta,
            failed_step="charge",
            rollback=RollbackReport(),
        )
        assert result.success is False
        assert result.errors == (err,)
        assert result.failed_step == "charge"

    def test_failure_is_frozen(self) -> None:
        meta = PipelineExecutionMeta(pipeline="test", args=MappingProxyType({}))
        result = PipelineFailure(
            errors=(ValueError("x"),),
            meta=meta,
            failed_step="x",
            rollback=RollbackReport(),
        )
        try:
            result.success = True  # type: ignore[misc]
            raise AssertionError("Should have raised")
        except AttributeError:
            pass
