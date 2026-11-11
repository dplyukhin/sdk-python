from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

import temporalio.activity
import temporalio.bridge.proto.workflow_completion
import temporalio.bridge.worker
import temporalio.client
import temporalio.exceptions
import temporalio.testing
import temporalio.worker
import temporalio.workflow

with temporalio.workflow.unsafe.imports_passed_through():
    import hypothesis.strategies as st


@temporalio.activity.defn
async def cancellation_activity() -> int:
    raise AssertionError("Checking must not execute activity implementations")


@temporalio.workflow.defn
class ActivityCancellationRace:
    def __init__(self) -> None:
        self.handle: temporalio.workflow.ActivityHandle[int] | None = None
        self.cancel_requested: bool = False

    @temporalio.workflow.run
    async def run(self, cancellation_type: int, fail_on_ack: bool) -> None:
        self.handle = temporalio.workflow.start_activity(
            cancellation_activity,
            start_to_close_timeout=timedelta(minutes=1),
            cancellation_type=temporalio.workflow.ActivityCancellationType(
                cancellation_type
            ),
        )
        deadline = asyncio.create_task(temporalio.workflow.sleep(timedelta(seconds=5)))
        result = None
        try:
            result = await self.handle
        except temporalio.exceptions.ActivityError as error:
            assert isinstance(error.cause, temporalio.exceptions.CancelledError)
            if fail_on_ack:
                raise RuntimeError("activity cancellation reached workflow") from error
        await deadline
        if not fail_on_ack:
            assert not (self.cancel_requested and result is not None), (
                "activity completed after cancellation was requested"
            )

    @temporalio.workflow.signal
    def cancel_activity(self) -> None:
        if self.handle is not None and not self.handle.done():
            self.cancel_requested = True
            self.handle.cancel()


@temporalio.workflow.defn
class WorkflowCancellationRace:
    def __init__(self) -> None:
        self.release_cleanup: bool = False

    @temporalio.workflow.run
    async def run(self) -> None:
        handle = temporalio.workflow.start_activity(
            cancellation_activity,
            start_to_close_timeout=timedelta(minutes=1),
            cancellation_type=temporalio.workflow.ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
        )
        try:
            await asyncio.shield(handle)
        except asyncio.CancelledError:
            handle.cancel()
            completed = False
            try:
                await handle
                completed = True
            except temporalio.exceptions.ActivityError as error:
                assert isinstance(error.cause, temporalio.exceptions.CancelledError)
            try:
                await temporalio.workflow.wait_condition(
                    lambda: self.release_cleanup, timeout=timedelta(seconds=30)
                )
            except asyncio.TimeoutError:
                raise asyncio.CancelledError()
            assert not completed, "cancelled order committed an activity result"
            raise

    @temporalio.workflow.signal
    def finish_cleanup(self) -> None:
        self.release_cleanup = True


@temporalio.workflow.defn
class WorkflowCancellationOutcome:
    @temporalio.workflow.run
    async def run(self, suppress: bool) -> None:
        try:
            await temporalio.workflow.sleep(timedelta(hours=1))
        except asyncio.CancelledError:
            if not suppress:
                raise
            await temporalio.workflow.sleep(timedelta(seconds=1))


async def _assert_replays(
    result: temporalio.testing.WorkflowCheckResult,
    workflow: type,
    message: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert result.history is not None
    assert result.exception is not None
    assert message in str(result.exception)
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
        workflows=[workflow], namespace="WorkflowCheckNamespace"
    ).replay_workflow(history, raise_on_replay_failure=False)
    assert replay.replay_failure is None
    assert any(message in failure for failure in failures)


@pytest.mark.parametrize(
    "cancellation_type", list(temporalio.workflow.ActivityCancellationType)
)
async def test_check_activity_cancellation_races_completion_and_timer(
    cancellation_type: temporalio.workflow.ActivityCancellationType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = await temporalio.testing.WorkflowChecker(
        workflows=[ActivityCancellationRace]
    ).check_workflow(
        ActivityCancellationRace.run,
        args=st.just((int(cancellation_type), False)),
        activities=[
            temporalio.testing.WorkflowCheckActivity(
                cancellation_activity, result=st.just(1)
            )
        ],
        signals=[
            temporalio.testing.WorkflowCheckSignal(
                ActivityCancellationRace.cancel_activity, args=st.tuples()
            )
        ],
        max_depth=3,
        max_checks=200,
    )
    if (
        cancellation_type
        != temporalio.workflow.ActivityCancellationType.WAIT_CANCELLATION_COMPLETED
    ):
        assert result.history is None
        assert result.exception is None
        return
    assert result.history is not None
    events = result.history.events
    assert any(e.HasField("timer_fired_event_attributes") for e in events)
    request = next(
        e
        for e in events
        if e.HasField("activity_task_cancel_requested_event_attributes")
    )
    completed = next(
        e for e in events if e.HasField("activity_task_completed_event_attributes")
    )
    assert request.event_id < completed.event_id
    assert not any(
        e.HasField("activity_task_canceled_event_attributes") for e in events
    )
    await _assert_replays(
        result,
        ActivityCancellationRace,
        "activity completed after cancellation was requested",
        monkeypatch,
    )


@pytest.mark.parametrize(
    "cancellation_type", list(temporalio.workflow.ActivityCancellationType)
)
async def test_check_activity_cancellation_acknowledgments(
    cancellation_type: temporalio.workflow.ActivityCancellationType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = await temporalio.testing.WorkflowChecker(
        workflows=[ActivityCancellationRace]
    ).check_workflow(
        ActivityCancellationRace.run,
        args=st.just((int(cancellation_type), True)),
        activities=[
            temporalio.testing.WorkflowCheckActivity(
                cancellation_activity, result=st.just(1)
            )
        ],
        signals=[
            temporalio.testing.WorkflowCheckSignal(
                ActivityCancellationRace.cancel_activity, args=st.tuples()
            )
        ],
        max_depth=3,
        max_checks=200,
    )
    assert result.history is not None
    waits = (
        cancellation_type
        == temporalio.workflow.ActivityCancellationType.WAIT_CANCELLATION_COMPLETED
    )
    assert (
        any(
            e.HasField("activity_task_cancel_requested_event_attributes")
            for e in result.history.events
        )
        == waits
    )
    assert (
        any(
            e.HasField("activity_task_canceled_event_attributes")
            for e in result.history.events
        )
        == waits
    )
    await _assert_replays(
        result,
        ActivityCancellationRace,
        "activity cancellation reached workflow",
        monkeypatch,
    )


@pytest.mark.parametrize("workflow_cancellation", [False, True])
async def test_check_workflow_cancellation_races_activity_signal_and_timer(
    workflow_cancellation: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = await temporalio.client.Client.connect("127.0.0.1:1", lazy=True)
    result = await client.check_workflow(
        WorkflowCancellationRace.run,
        workflows=[WorkflowCancellationRace],
        workflow_cancellation=workflow_cancellation,
        activities=[
            temporalio.testing.WorkflowCheckActivity(
                cancellation_activity, result=st.just(1)
            )
        ],
        signals=[
            temporalio.testing.WorkflowCheckSignal(
                WorkflowCancellationRace.finish_cleanup, args=st.tuples()
            )
        ],
        max_depth=3,
        max_checks=200,
    )
    if not workflow_cancellation:
        assert result.history is None
        assert result.exception is None
        return
    assert result.history is not None
    events = result.history.events
    assert (
        sum(
            e.HasField("workflow_execution_cancel_requested_event_attributes")
            for e in events
        )
        == 1
    )
    assert any(
        e.HasField("activity_task_cancel_requested_event_attributes") for e in events
    )
    assert any(
        e.HasField("workflow_execution_signaled_event_attributes") for e in events
    )
    assert any(e.HasField("timer_started_event_attributes") for e in events)
    assert not any(e.HasField("timer_fired_event_attributes") for e in events)
    await _assert_replays(
        result,
        WorkflowCancellationRace,
        "cancelled order committed an activity result",
        monkeypatch,
    )


@pytest.mark.parametrize("suppress", [False, True])
async def test_check_workflow_cancellation_is_not_a_counterexample(
    suppress: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    cancellations: list[object] = []
    original = temporalio.bridge.worker.Worker.complete_workflow_activation

    async def record_completion(
        self: temporalio.bridge.worker.Worker,
        completion: temporalio.bridge.proto.workflow_completion.WorkflowActivationCompletion,
    ) -> None:
        if completion.HasField("successful"):
            cancellations.extend(
                command
                for command in completion.successful.commands
                if command.HasField("cancel_workflow_execution")
            )
        await original(self, completion)

    monkeypatch.setattr(
        temporalio.bridge.worker.Worker,
        "complete_workflow_activation",
        record_completion,
    )
    result = await temporalio.testing.WorkflowChecker(
        workflows=[WorkflowCancellationOutcome]
    ).check_workflow(
        WorkflowCancellationOutcome.run,
        st.just(suppress),
        workflow_cancellation=True,
        max_depth=3,
    )
    assert result.history is None
    assert result.exception is None
    assert bool(cancellations) == (not suppress)
