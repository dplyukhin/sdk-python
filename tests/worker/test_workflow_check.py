import asyncio
from datetime import timedelta

import pytest

import temporalio.activity
import temporalio.bridge.proto.workflow_activation
import temporalio.bridge.proto.workflow_completion
import temporalio.bridge.worker
import temporalio.client
import temporalio.testing
import temporalio.worker
import temporalio.worker._workflow
import temporalio.workflow


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
async def forbidden_activity() -> None:
    raise AssertionError("Activity implementations must not execute")


@temporalio.workflow.defn
class ActivityWorkflow:
    @temporalio.workflow.run
    async def run(self) -> None:
        await temporalio.workflow.execute_activity(
            forbidden_activity, start_to_close_timeout=timedelta(seconds=1)
        )


async def test_check_workflow_backtracks_and_returns_reproducible_history(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = await temporalio.client.Client.connect("127.0.0.1:1", lazy=True)
    signal = temporalio.testing.WorkflowCheckSignal(
        TimerSignalRace.set_value, args=["bad"]
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
        signals=[temporalio.testing.WorkflowCheckSignal(RepeatedSignal.increment)],
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


async def test_check_workflow_rejects_activities_without_running_them() -> None:
    with pytest.raises(
        RuntimeError, match="unsupported workflow command: ScheduleActivityTask"
    ):
        await temporalio.testing.WorkflowChecker(
            workflows=[ActivityWorkflow]
        ).check_workflow(ActivityWorkflow.run)


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
