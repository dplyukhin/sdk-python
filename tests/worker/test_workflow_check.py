from __future__ import annotations

import asyncio
import dataclasses
import json
from collections.abc import Callable, Sequence
from datetime import timedelta
from typing import Any

import pytest

import temporalio.activity
import temporalio.api.common.v1
import temporalio.api.history.v1
import temporalio.bridge.proto.workflow_activation
import temporalio.bridge.proto.workflow_completion
import temporalio.bridge.worker
import temporalio.client
import temporalio.common
import temporalio.converter
import temporalio.exceptions
import temporalio.testing
import temporalio.testing._workflow_check
import temporalio.worker
import temporalio.worker._workflow
import temporalio.workflow

with temporalio.workflow.unsafe.imports_passed_through():
    import hypothesis.strategies as st


@temporalio.workflow.defn
class TimerSignalRace:
    def __init__(self) -> None:
        self.value = ""

    @temporalio.workflow.run
    async def run(self, forbidden: str) -> None:
        try:
            await temporalio.workflow.wait_condition(
                lambda: bool(self.value), timeout=timedelta(hours=1)
            )
        except asyncio.TimeoutError:
            return
        assert self.value != forbidden, "signal won the race"

    @temporalio.workflow.signal
    def set_value(self, value: str) -> None:
        self.value = value


@temporalio.workflow.defn
class RepeatedSignal:
    def __init__(self) -> None:
        self.count = 0

    @temporalio.workflow.run
    async def run(self) -> None:
        await temporalio.workflow.wait_condition(lambda: self.count == 3)
        raise RuntimeError("third signal")

    @temporalio.workflow.signal
    def increment(self) -> None:
        self.count += 1


@temporalio.workflow.defn
class SignalTupleWorkflow:
    def __init__(self) -> None:
        self.received = False
        self.value = (0, "")

    @temporalio.workflow.run
    async def run(self) -> None:
        await temporalio.workflow.wait_condition(lambda: self.received)
        assert self.value != (7, "bad"), "generated signal tuple"

    @temporalio.workflow.signal
    def set_value(self, number: int, text: str) -> None:
        self.value = (number, text)
        self.received = True


@temporalio.workflow.defn
class ExpensiveTask:
    @temporalio.workflow.run
    async def run(self) -> int:
        return sum(range(10_000_000))


@temporalio.workflow.defn
class InitialFailure:
    @temporalio.workflow.run
    async def run(self, message: str) -> None:
        raise RuntimeError(message)


@temporalio.activity.defn
async def forbidden_activity() -> int:
    raise AssertionError("Activity implementations must not execute")


@temporalio.activity.defn
async def forbidden_none_activity() -> None:
    raise AssertionError("Activity implementations must not execute")


@temporalio.workflow.defn
class NoneActivityWorkflow:
    @temporalio.workflow.run
    async def run(self) -> None:
        await temporalio.workflow.execute_activity(
            forbidden_none_activity, start_to_close_timeout=timedelta(seconds=1)
        )
        raise RuntimeError("received generated None")


@temporalio.workflow.defn
class ActivityWorkflow:
    @temporalio.workflow.run
    async def run(self) -> None:
        value = await temporalio.workflow.execute_activity(
            forbidden_activity, start_to_close_timeout=timedelta(seconds=1)
        )
        assert value != 1, "generated activity result"


@temporalio.workflow.defn
class ActivityOrderWorkflow:
    @temporalio.workflow.run
    async def run(self) -> None:
        order: list[str] = []

        async def resolve(activity_id: str) -> None:
            await temporalio.workflow.execute_activity(
                forbidden_activity,
                activity_id=activity_id,
                start_to_close_timeout=timedelta(seconds=1),
            )
            order.append(activity_id)

        await asyncio.gather(resolve("first"), resolve("second"))
        assert order != ["second", "first"], "activity completion order"


@temporalio.workflow.defn
class OrderApprovalWorkflow:
    stage_attribute: temporalio.common.SearchAttributeKey[str] = (
        temporalio.common.SearchAttributeKey.for_keyword("OrderStage")
    )

    def __init__(self) -> None:
        self.revoked: bool = False

    def set_stage(self, stage: str) -> None:
        temporalio.workflow.upsert_memo({"stage": stage})
        temporalio.workflow.upsert_search_attributes(
            [self.stage_attribute.value_set(stage)]
        )
        assert (
            temporalio.workflow.info().typed_search_attributes[self.stage_attribute]
            == stage
        )

    @temporalio.workflow.run
    async def run(self, order_id: str) -> None:
        temporalio.workflow.deprecate_patch("legacy-approval")
        assert temporalio.workflow.patched("approval-v2")
        temporalio.workflow.upsert_memo({"order_id": order_id, "scratch": True})
        self.set_stage("pending")
        approved = await temporalio.workflow.execute_activity(
            forbidden_activity,
            start_to_close_timeout=timedelta(seconds=10),
        )
        if not approved or self.revoked:
            self.set_stage("rejected")
            return
        self.set_stage("approved")
        temporalio.workflow.upsert_memo({"scratch": None})
        assert temporalio.workflow.memo_value("order_id") == order_id
        assert temporalio.workflow.memo_value("scratch", None) is None
        await temporalio.workflow.sleep(timedelta(minutes=5))
        assert not self.revoked, "revoked order dispatched"
        self.set_stage("dispatched")

    @temporalio.workflow.signal
    def revoke(self) -> None:
        self.revoked = True
        self.set_stage("revoked")


@temporalio.workflow.defn
class CompensationRaceWorkflow:
    def __init__(self) -> None:
        self.reserved: bool = False
        self.payment_failed: bool = False

    @temporalio.workflow.run
    async def run(self) -> None:
        assert temporalio.workflow.patched("compensate-payment")

        async def reserve() -> None:
            await temporalio.workflow.execute_activity(
                forbidden_none_activity,
                activity_id="reserve",
                start_to_close_timeout=timedelta(seconds=10),
            )
            self.reserved = True

        async def pay() -> None:
            try:
                await temporalio.workflow.execute_activity(
                    forbidden_activity,
                    activity_id="pay",
                    start_to_close_timeout=timedelta(seconds=10),
                    retry_policy=temporalio.common.RetryPolicy(maximum_attempts=1),
                )
            except temporalio.exceptions.ActivityError as error:
                cause = error.cause
                if isinstance(cause, temporalio.exceptions.ApplicationError):
                    assert cause.type == "PaymentDeclined"
                    assert cause.details == ("payment-123",)
                else:
                    assert isinstance(cause, temporalio.exceptions.TimeoutError)
                    assert (
                        cause.type == temporalio.exceptions.TimeoutType.START_TO_CLOSE
                    )
                self.payment_failed = True
                self.reserved = False
                temporalio.workflow.upsert_memo({"payment": "failed"})

        await asyncio.gather(reserve(), pay())
        assert not (self.payment_failed and self.reserved), (
            "late reservation escaped compensation"
        )


@pytest.mark.parametrize(
    "error",
    [
        temporalio.exceptions.ApplicationError(
            "declined", "payment-123", type="PaymentDeclined"
        ),
        temporalio.exceptions.TimeoutError(
            "payment timed out",
            type=temporalio.exceptions.TimeoutType.START_TO_CLOSE,
            last_heartbeat_details=[],
        ),
    ],
    ids=["application", "start-to-close"],
)
async def test_check_workflow_finds_activity_compensation_race(
    error: Exception, monkeypatch: pytest.MonkeyPatch
) -> None:
    contexts: list[temporalio.converter.SerializationContext] = []
    converter = dataclasses.replace(
        temporalio.converter.DataConverter.default,
        payload_codec=_CheckingCodec(contexts),
        failure_converter_class=temporalio.converter.DefaultFailureConverterWithEncodedAttributes,
    )
    result = await temporalio.testing.WorkflowChecker(
        workflows=[CompensationRaceWorkflow], data_converter=converter
    ).check_workflow(
        CompensationRaceWorkflow.run,
        activities=[
            temporalio.testing.WorkflowCheckActivity(
                forbidden_none_activity, result=st.none()
            ),
            temporalio.testing.WorkflowCheckActivity(
                forbidden_activity, result=st.just(1), failure=st.just(error)
            ),
        ],
        max_depth=2,
        max_checks=20,
    )
    assert result.exception is not None
    assert "late reservation escaped compensation" in str(result.exception)
    assert result.history is not None
    failed = next(
        e
        for e in result.history.events
        if e.HasField("activity_task_failed_event_attributes")
        or e.HasField("activity_task_timed_out_event_attributes")
    )
    attributes = (
        failed.activity_task_timed_out_event_attributes
        if isinstance(error, temporalio.exceptions.TimeoutError)
        else failed.activity_task_failed_event_attributes
    )
    assert (
        attributes.retry_state
        == temporalio.exceptions.RetryState.MAXIMUM_ATTEMPTS_REACHED
    )
    scheduled = result.history.events[attributes.scheduled_event_id - 1]
    started = result.history.events[attributes.started_event_id - 1]
    assert scheduled.activity_task_scheduled_event_attributes.activity_id == "pay"
    assert (
        started.activity_task_started_event_attributes.scheduled_event_id
        == scheduled.event_id
    )
    assert (
        attributes.failure.encoded_attributes.metadata["encoding"] == b"checking-test"
    )
    if isinstance(error, temporalio.exceptions.TimeoutError):
        assert (
            failed.event_time.ToDatetime() - started.event_time.ToDatetime()
        ) == timedelta(seconds=10)
    completed = next(
        e
        for e in result.history.events
        if e.HasField("activity_task_completed_event_attributes")
    )
    assert failed.event_id < completed.event_id
    assert any(
        isinstance(context, temporalio.converter.ActivitySerializationContext)
        and context.activity_type == "forbidden_activity"
        and context.activity_id is None
        for context in contexts
    )

    failures: list[str] = []
    original = temporalio.bridge.worker.Worker.complete_workflow_activation

    async def record_completion(
        self: temporalio.bridge.worker.Worker,
        completion: temporalio.bridge.proto.workflow_completion.WorkflowActivationCompletion,
    ) -> None:
        if completion.HasField("failed"):
            decoded = await converter.with_context(
                temporalio.converter.WorkflowSerializationContext(
                    namespace="WorkflowCheckNamespace",
                    workflow_id=history.workflow_id,
                )
            ).decode_failure(completion.failed.failure)
            failures.append(str(decoded))
        await original(self, completion)

    def no_sampling(*_args: object) -> None:
        raise AssertionError("Replay must not generate new activity outcomes")

    monkeypatch.setattr(temporalio.testing._workflow_check, "_sample", no_sampling)
    monkeypatch.setattr(
        temporalio.bridge.worker.Worker,
        "complete_workflow_activation",
        record_completion,
    )
    history = temporalio.client.WorkflowHistory.from_json(
        result.history.workflow_id, result.history.to_json()
    )
    replay = await temporalio.worker.Replayer(
        workflows=[CompensationRaceWorkflow],
        namespace="WorkflowCheckNamespace",
        data_converter=converter,
    ).replay_workflow(history, raise_on_replay_failure=False)
    assert replay.replay_failure is None
    assert any(
        "late reservation escaped compensation" in failure for failure in failures
    )


async def test_check_workflow_activity_failures_require_terminal_outcomes() -> None:
    checker = temporalio.testing.WorkflowChecker(workflows=[ActivityWorkflow])
    with pytest.raises(RuntimeError, match="maximum_attempts = 1"):
        await checker.check_workflow(
            ActivityWorkflow.run,
            activities=[
                temporalio.testing.WorkflowCheckActivity(
                    forbidden_activity, failure=st.just(RuntimeError("retryable"))
                )
            ],
        )
    result = await checker.check_workflow(
        ActivityWorkflow.run,
        activities=[
            temporalio.testing.WorkflowCheckActivity(
                forbidden_activity,
                failure=st.just(
                    temporalio.exceptions.ApplicationError(
                        "terminal", non_retryable=True
                    )
                ),
            )
        ],
    )
    assert result.history is None
    assert result.exception is None


async def test_check_workflow_named_activity_requires_an_outcome_strategy() -> None:
    with pytest.raises(ValueError, match="result or failure"):
        await temporalio.testing.WorkflowChecker(
            workflows=[ActivityWorkflow]
        ).check_workflow(
            ActivityWorkflow.run,
            activities=[temporalio.testing.WorkflowCheckActivity("forbidden_activity")],
        )


async def test_check_workflow_failure_strategies_require_exceptions() -> None:
    with pytest.raises(TypeError, match="must generate exceptions"):
        await temporalio.testing.WorkflowChecker(
            workflows=[ActivityWorkflow]
        ).check_workflow(
            ActivityWorkflow.run,
            activities=[
                temporalio.testing.WorkflowCheckActivity(
                    forbidden_activity,
                    failure=st.just("not an exception"),  # type: ignore[arg-type]
                )
            ],
        )


@pytest.mark.parametrize(
    "failure",
    [
        temporalio.exceptions.CancelledError("cancelled"),
        temporalio.exceptions.TimeoutError(
            "heartbeat timeout",
            type=temporalio.exceptions.TimeoutType.HEARTBEAT,
            last_heartbeat_details=[],
        ),
    ],
    ids=["cancellation", "heartbeat"],
)
async def test_check_workflow_rejects_unsupported_activity_failures(
    failure: Exception,
) -> None:
    with pytest.raises(
        RuntimeError, match="only application failures and start-to-close"
    ):
        await temporalio.testing.WorkflowChecker(
            workflows=[ActivityWorkflow]
        ).check_workflow(
            ActivityWorkflow.run,
            activities=[
                temporalio.testing.WorkflowCheckActivity(
                    forbidden_activity, failure=st.just(failure)
                )
            ],
        )


@pytest.mark.parametrize("max_depth, finds_failure", [(2, False), (3, True)])
async def test_check_workflow_versioned_order_approval(
    max_depth: int, finds_failure: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = await temporalio.testing.WorkflowChecker(
        workflows=[OrderApprovalWorkflow]
    ).check_workflow(
        OrderApprovalWorkflow.run,
        st.just("order-123"),
        signals=[
            temporalio.testing.WorkflowCheckSignal(
                OrderApprovalWorkflow.revoke, args=st.tuples()
            )
        ],
        activities=[
            temporalio.testing.WorkflowCheckActivity(
                forbidden_activity, result=st.sampled_from([0, 1])
            )
        ],
        samples_per_strategy=10,
        max_depth=max_depth,
        max_checks=100,
    )
    if not finds_failure:
        assert result.history is None
        assert result.exception is None
        return
    assert result.exception is not None
    assert "revoked order dispatched" in str(result.exception)
    assert result.history is not None
    events = result.history.events
    markers = [
        json.loads(
            e.marker_recorded_event_attributes.details["patch-data"].payloads[0].data
        )
        for e in events
        if e.HasField("marker_recorded_event_attributes")
    ]
    assert markers == [
        {"id": "legacy-approval", "deprecated": True},
        {"id": "approval-v2", "deprecated": False},
    ]
    converter = temporalio.converter.DataConverter.default
    memo_stages = []
    indexed_stages = []
    choices = []
    for event in events:
        if event.HasField("workflow_properties_modified_event_attributes"):
            fields = (
                event.workflow_properties_modified_event_attributes.upserted_memo.fields
            )
            if "stage" in fields:
                memo_stages.extend(await converter.decode([fields["stage"]]))
        elif event.HasField("upsert_workflow_search_attributes_event_attributes"):
            fields = event.upsert_workflow_search_attributes_event_attributes.search_attributes.indexed_fields
            if "OrderStage" in fields:
                indexed_stages.extend(await converter.decode([fields["OrderStage"]]))
        elif event.HasField("activity_task_completed_event_attributes"):
            choices.append("approved")
            assert await converter.decode(
                event.activity_task_completed_event_attributes.result.payloads
            ) == [1]
        elif event.HasField("workflow_execution_signaled_event_attributes"):
            choices.append("revoked")
        elif event.HasField("timer_fired_event_attributes"):
            choices.append("deadline")
    assert choices == ["approved", "revoked", "deadline"]
    assert memo_stages == indexed_stages == ["pending", "approved", "revoked"]

    failures: list[str] = []
    original = temporalio.bridge.worker.Worker.complete_workflow_activation

    async def record_completion(
        self: temporalio.bridge.worker.Worker,
        completion: temporalio.bridge.proto.workflow_completion.WorkflowActivationCompletion,
    ) -> None:
        if completion.HasField("failed"):
            failures.append(completion.failed.failure.message)
        await original(self, completion)

    monkeypatch.setattr(
        temporalio.bridge.worker.Worker,
        "complete_workflow_activation",
        record_completion,
    )
    history = temporalio.client.WorkflowHistory.from_json(
        result.history.workflow_id, result.history.to_json()
    )
    replay = await temporalio.worker.Replayer(
        workflows=[OrderApprovalWorkflow]
    ).replay_workflow(history, raise_on_replay_failure=False)
    assert replay.replay_failure is None
    assert any("revoked order dispatched" in failure for failure in failures)


async def test_check_workflow_backtracks_and_returns_reproducible_history(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = await temporalio.client.Client.connect("127.0.0.1:1", lazy=True)
    signal = temporalio.testing.WorkflowCheckSignal(
        TimerSignalRace.set_value, args=st.tuples(st.just("bad"))
    )
    # Initial task and timer-first branch succeed; the third frontier is its signal-first sibling.
    limited = await client.check_workflow(
        TimerSignalRace.run,
        st.just("bad"),
        workflows=[TimerSignalRace],
        signals=[signal],
        max_checks=2,
    )
    assert limited.history is None
    result = await client.check_workflow(
        TimerSignalRace.run,
        st.just("bad"),
        workflows=[TimerSignalRace],
        signals=[signal],
        max_checks=3,
    )
    assert result.history is not None
    assert result.exception is not None
    assert "signal won the race" in str(result.exception)
    assert result.history.events[-1].HasField("workflow_task_started_event_attributes")
    assert not any(
        e.HasField("timer_fired_event_attributes") for e in result.history.events
    )

    failures: list[str] = []
    original = temporalio.bridge.worker.Worker.complete_workflow_activation

    async def record_completion(
        self: temporalio.bridge.worker.Worker,
        completion: temporalio.bridge.proto.workflow_completion.WorkflowActivationCompletion,
    ) -> None:
        if completion.HasField("failed"):
            failures.append(completion.failed.failure.message)
        await original(self, completion)

    monkeypatch.setattr(
        temporalio.bridge.worker.Worker,
        "complete_workflow_activation",
        record_completion,
    )
    await temporalio.worker.Replayer(workflows=[TimerSignalRace]).replay_workflow(
        result.history, raise_on_replay_failure=False
    )
    assert any("signal won the race" in failure for failure in failures)


@pytest.mark.parametrize("max_depth, finds_failure", [(2, False), (3, True)])
async def test_check_workflow_allows_repeated_signals(
    max_depth: int, finds_failure: bool
) -> None:
    result = await temporalio.testing.WorkflowChecker(
        workflows=[RepeatedSignal]
    ).check_workflow(
        RepeatedSignal.run,
        signals=[
            temporalio.testing.WorkflowCheckSignal(
                RepeatedSignal.increment, args=st.tuples()
            )
        ],
        max_depth=max_depth,
    )
    assert (result.exception is not None) == finds_failure
    if result.history:
        assert (
            sum(
                e.HasField("workflow_execution_signaled_event_attributes")
                for e in result.history.events
            )
            == 3
        )


async def test_check_workflow_requires_activity_results_without_running_them() -> None:
    with pytest.raises(
        RuntimeError, match="no sampled results or failures for activity type"
    ):
        await temporalio.testing.WorkflowChecker(
            workflows=[ActivityWorkflow]
        ).check_workflow(ActivityWorkflow.run)


async def test_check_workflow_samples_signal_arguments() -> None:
    result = await temporalio.testing.WorkflowChecker(
        workflows=[TimerSignalRace]
    ).check_workflow(
        TimerSignalRace.run,
        st.just("bad"),
        signals=[
            temporalio.testing.WorkflowCheckSignal(
                TimerSignalRace.set_value,
                args=st.tuples(st.sampled_from(["good", "bad"])),
            )
        ],
        samples_per_strategy=10,
        max_depth=1,
        max_checks=4,
    )
    assert result.exception is not None
    assert "signal won the race" in str(result.exception)
    assert result.history is not None
    [signal] = [
        e.workflow_execution_signaled_event_attributes
        for e in result.history.events
        if e.HasField("workflow_execution_signaled_event_attributes")
    ]
    assert await temporalio.converter.DataConverter.default.decode(
        signal.input.payloads
    ) == ["bad"]


async def test_check_workflow_generates_signal_argument_tuples() -> None:
    result = await temporalio.testing.WorkflowChecker(
        workflows=[SignalTupleWorkflow]
    ).check_workflow(
        SignalTupleWorkflow.run,
        signals=[
            temporalio.testing.WorkflowCheckSignal(
                SignalTupleWorkflow.set_value,
                args=st.tuples(st.just(7), st.just("bad")),
            )
        ],
    )
    assert result.exception is not None
    assert "generated signal tuple" in str(result.exception)


@pytest.mark.parametrize("activity", [forbidden_activity, "forbidden_activity"])
async def test_check_workflow_samples_activity_results_and_replays(
    activity: str | Callable[..., Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = await temporalio.client.Client.connect("127.0.0.1:1", lazy=True)
    result = await client.check_workflow(
        ActivityWorkflow.run,
        workflows=[ActivityWorkflow],
        activities=[
            temporalio.testing.WorkflowCheckActivity(
                activity, result=st.integers(min_value=0, max_value=1)
            )
        ],
        samples_per_strategy=10,
        max_depth=1,
        max_checks=3,
    )
    assert result.exception is not None
    assert "generated activity result" in str(result.exception)
    assert result.history is not None
    scheduled = next(
        e
        for e in result.history.events
        if e.HasField("activity_task_scheduled_event_attributes")
    )
    started = next(
        e
        for e in result.history.events
        if e.HasField("activity_task_started_event_attributes")
    )
    completed = next(
        e
        for e in result.history.events
        if e.HasField("activity_task_completed_event_attributes")
    )
    assert (
        started.activity_task_started_event_attributes.scheduled_event_id
        == scheduled.event_id
    )
    assert started.activity_task_started_event_attributes.attempt == 1
    assert (
        completed.activity_task_completed_event_attributes.scheduled_event_id
        == scheduled.event_id
    )
    assert (
        completed.activity_task_completed_event_attributes.started_event_id
        == started.event_id
    )
    assert await temporalio.converter.DataConverter.default.decode(
        completed.activity_task_completed_event_attributes.result.payloads
    ) == [1]

    def no_sampling(*_args: object) -> None:
        raise AssertionError("Replay must use recorded payloads, not generator draws")

    monkeypatch.setattr(temporalio.testing._workflow_check, "_sample", no_sampling)
    failures: list[str] = []
    original = temporalio.bridge.worker.Worker.complete_workflow_activation

    async def record_completion(
        self: temporalio.bridge.worker.Worker,
        completion: temporalio.bridge.proto.workflow_completion.WorkflowActivationCompletion,
    ) -> None:
        if completion.HasField("failed"):
            failures.append(completion.failed.failure.message)
        await original(self, completion)

    monkeypatch.setattr(
        temporalio.bridge.worker.Worker,
        "complete_workflow_activation",
        record_completion,
    )
    await temporalio.worker.Replayer(workflows=[ActivityWorkflow]).replay_workflow(
        result.history, raise_on_replay_failure=False
    )
    assert any("generated activity result" in failure for failure in failures)


async def test_check_workflow_records_none_activity_result() -> None:
    result = await temporalio.testing.WorkflowChecker(
        workflows=[NoneActivityWorkflow]
    ).check_workflow(
        NoneActivityWorkflow.run,
        activities=[
            temporalio.testing.WorkflowCheckActivity(
                forbidden_none_activity, result=st.none()
            )
        ],
    )
    assert result.exception is not None
    assert "received generated None" in str(result.exception)
    assert result.history is not None
    completed = next(
        e.activity_task_completed_event_attributes
        for e in result.history.events
        if e.HasField("activity_task_completed_event_attributes")
    )
    assert await temporalio.converter.DataConverter.default.decode(
        completed.result.payloads
    ) == [None]


class _CheckingCodec(
    temporalio.converter.PayloadCodec, temporalio.converter.WithSerializationContext
):
    def __init__(
        self,
        contexts: list[temporalio.converter.SerializationContext],
        context: temporalio.converter.SerializationContext | None = None,
    ) -> None:
        self.contexts = contexts
        self.context = context

    def with_context(
        self, context: temporalio.converter.SerializationContext
    ) -> _CheckingCodec:
        return _CheckingCodec(self.contexts, context)

    async def encode(
        self, payloads: Sequence[temporalio.api.common.v1.Payload]
    ) -> list[temporalio.api.common.v1.Payload]:
        assert self.context is not None
        self.contexts.append(self.context)
        return [
            temporalio.api.common.v1.Payload(
                metadata={"encoding": b"checking-test"}, data=p.SerializeToString()
            )
            for p in payloads
        ]

    async def decode(
        self, payloads: Sequence[temporalio.api.common.v1.Payload]
    ) -> list[temporalio.api.common.v1.Payload]:
        assert self.context is not None
        return [temporalio.api.common.v1.Payload.FromString(p.data) for p in payloads]


async def test_check_workflow_encodes_generated_activity_results_with_context() -> None:
    contexts: list[temporalio.converter.SerializationContext] = []
    result = await temporalio.testing.WorkflowChecker(
        workflows=[ActivityWorkflow],
        namespace="generated-namespace",
        data_converter=dataclasses.replace(
            temporalio.converter.DataConverter.default,
            payload_codec=_CheckingCodec(contexts),
        ),
    ).check_workflow(
        ActivityWorkflow.run,
        activities=[
            temporalio.testing.WorkflowCheckActivity(
                forbidden_activity, result=st.just(1)
            )
        ],
    )
    assert result.exception is not None
    assert "generated activity result" in str(result.exception)
    assert result.history is not None
    assert any(
        isinstance(context, temporalio.converter.ActivitySerializationContext)
        and context.namespace == "generated-namespace"
        and context.workflow_id == result.history.workflow_id
        and context.workflow_type == "ActivityWorkflow"
        and context.activity_type == "forbidden_activity"
        and not context.is_local
        for context in contexts
    )


async def test_check_workflow_backtracks_activity_completion_order() -> None:
    result = await temporalio.testing.WorkflowChecker(
        workflows=[ActivityOrderWorkflow]
    ).check_workflow(
        ActivityOrderWorkflow.run,
        activities=[
            temporalio.testing.WorkflowCheckActivity(
                forbidden_activity, result=st.just(0)
            )
        ],
        max_depth=2,
        max_checks=5,
    )
    assert result.exception is not None
    assert "activity completion order" in str(result.exception)
    assert result.history is not None
    schedules = {
        e.event_id: e.activity_task_scheduled_event_attributes.activity_id
        for e in result.history.events
        if e.HasField("activity_task_scheduled_event_attributes")
    }
    order = [
        schedules[e.activity_task_completed_event_attributes.scheduled_event_id]
        for e in result.history.events
        if e.HasField("activity_task_completed_event_attributes")
    ]
    assert order == ["second", "first"]


async def test_check_workflow_rejects_non_tuple_signal_arguments() -> None:
    with pytest.raises(TypeError, match="must generate tuples"):
        await temporalio.testing.WorkflowChecker(
            workflows=[RepeatedSignal]
        ).check_workflow(
            RepeatedSignal.run,
            signals=[
                temporalio.testing.WorkflowCheckSignal(
                    RepeatedSignal.increment,
                    args=st.just([]),  # type: ignore[arg-type]
                )
            ],
        )


async def test_check_workflow_requires_positive_sample_count() -> None:
    with pytest.raises(ValueError, match="samples_per_strategy must be positive"):
        await temporalio.testing.WorkflowChecker(
            workflows=[RepeatedSignal]
        ).check_workflow(RepeatedSignal.run, samples_per_strategy=0)


async def test_check_workflow_task_timeout_is_a_counterexample() -> None:
    result = await temporalio.testing.WorkflowChecker(
        workflows=[ExpensiveTask]
    ).check_workflow(
        ExpensiveTask.run,
        task_timeout=timedelta(milliseconds=1),
    )
    assert result.history is not None
    assert result.exception is not None
    assert "timed out" in str(result.exception)


async def test_check_workflow_overall_timeout_is_not_a_counterexample() -> None:
    result = await temporalio.testing.WorkflowChecker(
        workflows=[ExpensiveTask]
    ).check_workflow(ExpensiveTask.run, timeout=timedelta(milliseconds=1))
    assert result.exception is None
    assert result.history is None


async def test_check_workflow_checks_the_initial_task_at_depth_zero() -> None:
    result = await temporalio.testing.WorkflowChecker(
        workflows=[InitialFailure]
    ).check_workflow(
        InitialFailure.run, st.just("initial failure"), max_depth=0, max_checks=1
    )
    assert result.exception is not None
    assert "initial failure" in str(result.exception)
    assert result.history is not None
    assert len(result.history.events) == 3


async def test_check_workflow_cancellation_cleans_up(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    original = temporalio.worker._workflow._WorkflowWorker._handle_activation

    async def paused_activation(
        self: temporalio.worker._workflow._WorkflowWorker,
        activation: temporalio.bridge.proto.workflow_activation.WorkflowActivation,
    ) -> None:
        entered.set()
        await release.wait()
        await original(self, activation)

    monkeypatch.setattr(
        temporalio.worker._workflow._WorkflowWorker,
        "_handle_activation",
        paused_activation,
    )
    task = asyncio.create_task(
        temporalio.testing.WorkflowChecker(workflows=[RepeatedSignal]).check_workflow(
            RepeatedSignal.run
        )
    )
    await entered.wait()
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task


@temporalio.workflow.defn
class GeneratedStartWorkflow:
    @temporalio.workflow.run
    async def run(self, fail: bool) -> None:
        await temporalio.workflow.sleep(1)
        assert not fail, "generated start failed"


@temporalio.workflow.defn
class UntypedStartWorkflow:
    @temporalio.workflow.run
    async def run(self, value) -> None:
        assert value != "bad", "explicit untyped input"


@temporalio.activity.defn
async def forbidden_bool_activity() -> bool:
    raise AssertionError("Activity implementations must not execute")


@temporalio.workflow.defn
class InferredOutcomeWorkflow:
    def __init__(self) -> None:
        self.signal_value: bool | None = None

    @temporalio.workflow.run
    async def run(self) -> None:
        result = await temporalio.workflow.execute_activity(
            forbidden_bool_activity, start_to_close_timeout=timedelta(seconds=1)
        )
        await temporalio.workflow.wait_condition(lambda: self.signal_value is not None)
        assert not (result and self.signal_value), "inferred activity and signal"

    @temporalio.workflow.signal
    def set_value(self, value: bool) -> None:
        self.signal_value = value


@pytest.mark.parametrize("mode", ["inferred", "name", "arg", "args"])
async def test_check_workflow_explores_generated_starts(
    mode: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    starts: list[bool] = []
    original = temporalio.testing.WorkflowChecker._check_start

    async def record_start(
        self: temporalio.testing.WorkflowChecker,
        workflow_id: str,
        start: bytes,
        *args: Any,
    ) -> temporalio.testing.WorkflowCheckResult:
        attributes = temporalio.api.history.v1.WorkflowExecutionStartedEventAttributes.FromString(
            start
        )
        [value] = await temporalio.converter.DataConverter.default.decode(
            attributes.input.payloads
        )
        starts.append(value)
        return await original(self, workflow_id, start, *args)

    monkeypatch.setattr(
        temporalio.testing.WorkflowChecker, "_check_start", record_start
    )
    checker = temporalio.testing.WorkflowChecker(workflows=[GeneratedStartWorkflow])
    options: dict[str, Any] = {}
    if mode == "arg":
        options["arg"] = st.booleans()
    elif mode == "args":
        options["args"] = st.tuples(st.booleans())
    result = await checker.check_workflow(
        "GeneratedStartWorkflow" if mode == "name" else GeneratedStartWorkflow.run,
        samples_per_strategy=10,
        max_depth=1,
        max_checks=2,
        **options,
    )
    assert starts == [False, True]
    assert result.exception is not None
    assert "generated start failed" in str(result.exception)
    assert result.history is not None
    [value] = await temporalio.converter.DataConverter.default.decode(
        result.history.events[
            0
        ].workflow_execution_started_event_attributes.input.payloads
    )
    assert value is True
    replay = await temporalio.worker.Replayer(
        workflows=[GeneratedStartWorkflow]
    ).replay_workflow(result.history, raise_on_replay_failure=False)
    assert replay.replay_failure is None


async def test_check_workflow_deduplicates_starts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = temporalio.testing._workflow_check._sample
    calls = 0

    def duplicates(strategy: st.SearchStrategy[Any], count: int) -> list[Any]:
        return original(strategy, count) * 3

    original_check = temporalio.testing.WorkflowChecker._check_start

    async def record_check(
        self: temporalio.testing.WorkflowChecker, *args: Any
    ) -> temporalio.testing.WorkflowCheckResult:
        nonlocal calls
        calls += 1
        return await original_check(self, *args)

    monkeypatch.setattr(temporalio.testing._workflow_check, "_sample", duplicates)
    monkeypatch.setattr(
        temporalio.testing.WorkflowChecker, "_check_start", record_check
    )
    result = await temporalio.testing.WorkflowChecker(
        workflows=[GeneratedStartWorkflow]
    ).check_workflow(GeneratedStartWorkflow.run, st.just(False))
    assert result.exception is None
    assert calls == 1


async def test_check_workflow_infers_signal_and_activity_strategies() -> None:
    signal_result = await temporalio.testing.WorkflowChecker(
        workflows=[RepeatedSignal]
    ).check_workflow(RepeatedSignal.run)
    assert signal_result.exception is not None
    assert "third signal" in str(signal_result.exception)

    disabled = await temporalio.testing.WorkflowChecker(
        workflows=[RepeatedSignal]
    ).check_workflow(RepeatedSignal.run, signals=[])
    assert disabled.exception is None

    activity_result = await temporalio.testing.WorkflowChecker(
        workflows=[NoneActivityWorkflow]
    ).check_workflow(
        NoneActivityWorkflow.run,
        activities=[temporalio.testing.WorkflowCheckActivity(forbidden_none_activity)],
    )
    assert activity_result.exception is not None
    assert "received generated None" in str(activity_result.exception)


async def test_check_workflow_infers_typed_signal_arguments_and_activity_results() -> (
    None
):
    result = await temporalio.testing.WorkflowChecker(
        workflows=[InferredOutcomeWorkflow]
    ).check_workflow(
        InferredOutcomeWorkflow.run,
        activities=[temporalio.testing.WorkflowCheckActivity(forbidden_bool_activity)],
        samples_per_strategy=10,
        max_depth=2,
    )
    assert result.exception is not None
    assert "inferred activity and signal" in str(result.exception)


async def test_check_workflow_inference_requires_types_but_allows_overrides() -> None:
    checker = temporalio.testing.WorkflowChecker(workflows=[UntypedStartWorkflow])
    with pytest.raises(ValueError, match="missing type annotation.*explicit strategy"):
        await checker.check_workflow(UntypedStartWorkflow.run)
    result = await checker.check_workflow(UntypedStartWorkflow.run, st.just("bad"))
    assert result.exception is not None
    assert "explicit untyped input" in str(result.exception)


def test_check_workflow_unsupported_annotation_requires_an_override() -> None:
    with pytest.raises(ValueError, match="Cannot infer.*explicit strategy"):
        temporalio.testing._workflow_check._type_strategy(Any, "test input")


async def test_check_workflow_rejects_concrete_or_conflicting_start_arguments() -> None:
    checker = temporalio.testing.WorkflowChecker(workflows=[GeneratedStartWorkflow])
    with pytest.raises(TypeError, match="Hypothesis strategy"):
        await checker.check_workflow(
            GeneratedStartWorkflow.run,
            False,  # type: ignore[arg-type]
        )
    with pytest.raises(TypeError, match="must generate tuples"):
        await checker.check_workflow(
            GeneratedStartWorkflow.run,
            args=st.booleans(),  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="both arg and args"):
        await checker.check_workflow(
            GeneratedStartWorkflow.run, st.just(False), args=st.tuples()
        )
