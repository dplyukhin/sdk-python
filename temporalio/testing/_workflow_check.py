"""Experimental, serverless exploration of timer and signal schedules."""

from __future__ import annotations

import asyncio
import math
import uuid
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import temporalio.api.common.v1
import temporalio.api.failure.v1
import temporalio.api.history.v1
import temporalio.api.taskqueue.v1
import temporalio.bridge.proto.workflow_activation
import temporalio.bridge.worker
import temporalio.client
import temporalio.common
import temporalio.converter
import temporalio.runtime
import temporalio.worker
import temporalio.worker.workflow_sandbox
import temporalio.workflow


@dataclass(frozen=True)
class WorkflowCheckSignal:
    """A signal input eligible repeatedly at every open choice point."""

    signal: str | Callable
    """Signal name or decorated handler."""

    args: Sequence[Any] = ()
    """Concrete argument values, encoded once and reused on replay."""


@dataclass(frozen=True)
class WorkflowCheckResult:
    """The first counterexample, or empty fields if none was found within the bounds.

    An empty result does not prove correctness. Search may stop at its depth,
    task-count, or wall-clock limit. Only singleton event batches are explored.
    """

    history: temporalio.client.WorkflowHistory | None = None
    """Input history ending at the start of the failing workflow task."""

    exception: BaseException | None = None
    """Decoded task failure or start-to-close timeout."""


class WorkflowChecker:
    """Experimental workflow checking without a Temporal server.

    Uses the normal Core and Python workflow runners, with a fresh replay for
    each explored frontier. Supports timers, signals, and terminal commands.
    Activities, updates, child workflows, and continue-as-new are unsupported.
    Workflow failures are terminal outcomes; workflow-task failures and timeouts
    are counterexamples. Python execution uses the normal deadlock detector.
    """

    def __init__(
        self,
        *,
        workflows: Sequence[type],
        namespace: str = "WorkflowCheckNamespace",
        data_converter: temporalio.converter.DataConverter = temporalio.converter.DataConverter.default,
        workflow_runner: temporalio.worker.WorkflowRunner = temporalio.worker.workflow_sandbox.SandboxedWorkflowRunner(),
        runtime: temporalio.runtime.Runtime | None = None,
    ) -> None:
        """Configure registered implementations and the same converter used in production."""
        self._namespace = namespace
        self._data_converter = data_converter
        self._replayer = temporalio.worker.Replayer(
            workflows=workflows,
            namespace=namespace,
            data_converter=data_converter,
            workflow_runner=workflow_runner,
            runtime=runtime,
        )

    async def check_workflow(
        self,
        workflow: str | Callable[..., Awaitable[Any]],
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = (),
        signals: Sequence[WorkflowCheckSignal] = (),
        max_depth: int = 20,
        max_checks: int = 1_000,
        timeout: timedelta = timedelta(seconds=30),
        task_timeout: timedelta = timedelta(seconds=10),
    ) -> WorkflowCheckResult:
        """Explore schedules for these exact start arguments.

        Args:
            workflow: Registered workflow name or decorated run method.
            arg: Single start argument, mutually exclusive with args.
            args: Multiple start arguments.
            signals: Concrete signal inputs, each eligible repeatedly.
            max_depth: Maximum extensions after the initial task, which has depth zero.
            max_checks: Maximum new task frontiers, including the initial task.
            timeout: Overall search budget, including replay and eviction,
                rounded up to milliseconds.
            task_timeout: Wall-clock limit for one task, including replay,
                rounded up to milliseconds.

        Returns:
            History and exception for the first task failure or timeout. Otherwise,
            an empty result. Cleanup may outlast the search budget; this does not
            forcibly terminate uninterruptible Python code.
        """
        if max_depth < 0 or max_checks <= 0:
            raise ValueError("max_depth must be nonnegative and max_checks positive")
        timeout_millis = math.ceil(timeout.total_seconds() * 1_000)
        task_timeout_millis = math.ceil(task_timeout.total_seconds() * 1_000)
        if timeout < timedelta(milliseconds=1) or task_timeout < timedelta(
            milliseconds=1
        ):
            raise ValueError(
                "timeout and task_timeout must be at least one millisecond"
            )
        name, _ = temporalio.workflow._Definition.get_name_and_result_type(workflow)
        workflow_id = f"check-{uuid.uuid4()}"
        converter = self._data_converter.with_context(
            temporalio.converter.WorkflowSerializationContext(
                namespace=self._namespace, workflow_id=workflow_id
            )
        )
        start = temporalio.api.history.v1.WorkflowExecutionStartedEventAttributes(
            workflow_type=temporalio.api.common.v1.WorkflowType(name=name),
            workflow_id=workflow_id,
            original_execution_run_id=str(uuid.uuid4()),
            task_queue=temporalio.api.taskqueue.v1.TaskQueue(name="workflow-check"),
            input=temporalio.api.common.v1.Payloads(
                payloads=await converter.encode(
                    temporalio.common._arg_or_args(arg, args)
                )
            ),
        )
        start.workflow_task_timeout.FromTimedelta(task_timeout)
        encoded_signals = []
        for signal in signals:
            encoded_signals.append(
                temporalio.api.history.v1.WorkflowExecutionSignaledEventAttributes(
                    signal_name=temporalio.workflow._SignalDefinition.must_name_from_fn_or_str(
                        signal.signal
                    ),
                    input=temporalio.api.common.v1.Payloads(
                        payloads=await converter.encode(signal.args)
                    ),
                ).SerializeToString()
            )

        def on_eviction(
            _run_id: str,
            _job: temporalio.bridge.proto.workflow_activation.RemoveFromCache,
        ) -> None:
            controller.notify_eviction()

        workflow_worker, bridge_worker, controller = self._replayer._create_workers(
            on_eviction, temporalio.bridge.worker.Worker.for_checking
        )
        worker_task = asyncio.create_task(workflow_worker.run())
        check_task = None
        try:
            check_task = asyncio.ensure_future(
                controller.check(
                    workflow_id,
                    start.SerializeToString(),
                    encoded_signals,
                    max_depth,
                    max_checks,
                    timeout_millis,
                    task_timeout_millis,
                )
            )
            await asyncio.wait(
                [worker_task, check_task], return_when=asyncio.FIRST_COMPLETED
            )
            if worker_task.done():
                await worker_task
            result = await check_task
            if result is None:
                return WorkflowCheckResult()
            history, failure = result
            return WorkflowCheckResult(
                history=temporalio.client.WorkflowHistory(
                    workflow_id,
                    temporalio.api.history.v1.History.FromString(history).events,
                ),
                exception=await converter.decode_failure(
                    temporalio.api.failure.v1.Failure.FromString(failure)
                ),
            )
        finally:
            if check_task is not None:
                check_task.cancel()
                await asyncio.gather(check_task, return_exceptions=True)
            bridge_worker.initiate_shutdown()
            await asyncio.gather(worker_task, return_exceptions=True)
            await bridge_worker.finalize_shutdown()
