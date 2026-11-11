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
import temporalio.bridge.proto.workflow_activation
import temporalio.bridge.proto.workflow_completion
import temporalio.bridge.worker
import temporalio.client
import temporalio.common
import temporalio.converter
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


@pytest.mark.parametrize("max_depth, finds_failure", [(2, False), (3, True)])
async def test_check_workflow_versioned_order_approval(
    max_depth: int, finds_failure: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = await temporalio.testing.WorkflowChecker(
        workflows=[OrderApprovalWorkflow]
    ).check_workflow(
        OrderApprovalWorkflow.run,
        "order-123",
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
        "bad",
        workflows=[TimerSignalRace],
        signals=[signal],
        max_checks=2,
    )
    assert limited.history is None
    result = await client.check_workflow(
        TimerSignalRace.run,
        "bad",
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
    with pytest.raises(RuntimeError, match="no sampled results for activity type"):
        await temporalio.testing.WorkflowChecker(
            workflows=[ActivityWorkflow]
        ).check_workflow(ActivityWorkflow.run)


async def test_check_workflow_samples_signal_arguments() -> None:
    result = await temporalio.testing.WorkflowChecker(
        workflows=[TimerSignalRace]
    ).check_workflow(
        TimerSignalRace.run,
        "bad",
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
    ).check_workflow(InitialFailure.run, "initial failure", max_depth=0, max_checks=1)
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
