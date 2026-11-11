from __future__ import annotations

import asyncio
import json
import subprocess
import sys
from pathlib import Path

import pytest

import temporalio.client
import temporalio.testing
import temporalio.testing.workflow_check
import temporalio.worker
import temporalio.workflow

with temporalio.workflow.unsafe.imports_passed_through():
    import hypothesis.strategies as st


@temporalio.workflow.defn
class ExportWorkflow:
    @temporalio.workflow.run
    async def run(self, value: bool) -> bool:
        await asyncio.gather(temporalio.workflow.sleep(1), temporalio.workflow.sleep(2))
        return value


@temporalio.workflow.defn(name="ExportWorkflow")
class ChangedExportWorkflow:
    @temporalio.workflow.run
    async def run(self, value: bool) -> bool:
        return value


@temporalio.workflow.defn
class UnsupportedExportWorkflow:
    @temporalio.workflow.run
    async def run(self) -> None:
        temporalio.workflow.continue_as_new()


@temporalio.workflow.defn
class FailingExportWorkflow:
    @temporalio.workflow.run
    async def run(self) -> None:
        raise RuntimeError("exported counterexample")


checks = [
    temporalio.testing.WorkflowCheckSpec(ExportWorkflow, arg=st.just(False)),
    temporalio.testing.WorkflowCheckSpec(ExportWorkflow, arg=st.just(True)),
]
failing_check = temporalio.testing.WorkflowCheckSpec(FailingExportWorkflow)


async def test_check_workflow_collects_all_explored_and_completed_histories() -> None:
    histories: list[temporalio.client.WorkflowHistory] = []
    result = await temporalio.testing.WorkflowChecker(
        workflows=[ExportWorkflow]
    ).check_workflow(
        ExportWorkflow.run,
        samples_per_strategy=10,
        max_depth=2,
        max_checks=10,
        on_history=histories.append,
    )
    assert result.exception is None
    assert len(histories) == 20
    terminal = [
        history
        for history in histories
        if history.events[-1].HasField("workflow_execution_completed_event_attributes")
    ]
    assert len(terminal) == 4
    assert (
        sum(
            history.events[-1].HasField("workflow_task_started_event_attributes")
            for history in histories
        )
        == 10
    )
    replayer = temporalio.worker.Replayer(workflows=[ExportWorkflow])
    for history in histories:
        restored = temporalio.client.WorkflowHistory.from_json(
            history.workflow_id, history.to_json()
        )
        replay = await replayer.replay_workflow(restored)
        assert replay.replay_failure is None

    replay = await temporalio.worker.Replayer(
        workflows=[ChangedExportWorkflow]
    ).replay_workflow(terminal[0], raise_on_replay_failure=False)
    assert replay.replay_failure is not None


async def test_check_workflow_exports_at_bounds_and_through_client() -> None:
    histories: list[temporalio.client.WorkflowHistory] = []
    client = await temporalio.client.Client.connect("127.0.0.1:1", lazy=True)
    result = await client.check_workflow(
        ExportWorkflow.run,
        st.just(False),
        workflows=[ExportWorkflow],
        max_checks=1,
        on_history=histories.append,
    )
    assert result.exception is None
    assert len(histories) == 2
    assert histories[-1].events[-1].HasField("timer_started_event_attributes")
    replay = await temporalio.worker.Replayer(
        workflows=[ExportWorkflow]
    ).replay_workflow(histories[-1])
    assert replay.replay_failure is None


async def test_check_workflow_exports_prefix_before_model_error() -> None:
    histories: list[temporalio.client.WorkflowHistory] = []
    with pytest.raises(RuntimeError, match="unsupported workflow command"):
        await temporalio.testing.WorkflowChecker(
            workflows=[UnsupportedExportWorkflow]
        ).check_workflow(UnsupportedExportWorkflow.run, on_history=histories.append)
    assert len(histories) == 1
    assert histories[0].events[-1].HasField("workflow_task_started_event_attributes")


async def test_check_workflow_callback_failure_cleans_up() -> None:
    def fail(_history: temporalio.client.WorkflowHistory) -> None:
        raise ValueError("writer failed")

    checker = temporalio.testing.WorkflowChecker(workflows=[ExportWorkflow])
    with pytest.raises(ValueError, match="writer failed"):
        await checker.check_workflow(
            ExportWorkflow.run, st.just(False), max_checks=1, on_history=fail
        )
    result = await checker.check_workflow(
        ExportWorkflow.run, st.just(False), max_checks=1
    )
    assert result.exception is None


async def test_export_histories_manifest_preserves_errors_and_counterexamples(
    tmp_path: Path,
) -> None:
    output = tmp_path / "corpus"
    exit_code = await temporalio.testing.workflow_check.export_histories(
        [
            temporalio.testing.WorkflowCheckSpec(UnsupportedExportWorkflow),
            temporalio.testing.WorkflowCheckSpec(FailingExportWorkflow),
            checks[0],
        ],
        output,
        max_depth=2,
    )
    assert exit_code == 2
    manifest = json.loads((output / "manifest.json").read_text())
    assert [entry["status"] for entry in manifest["checks"]] == [
        "error",
        "counterexample",
        "no_counterexample",
    ]
    assert manifest["bounds_per_start"]["max_depth"] == 2
    assert [len(entry["histories"]) for entry in manifest["checks"]] == [1, 1, 10]
    counterexample = manifest["checks"][1]
    assert "exported counterexample" in counterexample["failure"]
    assert counterexample["counterexample"] == counterexample["histories"][0]["path"]
    for entry in manifest["checks"]:
        for record in entry["histories"]:
            history = temporalio.client.WorkflowHistory.from_json(
                record["workflow_id"], (output / record["path"]).read_text()
            )
            assert history.events
    with pytest.raises(FileExistsError):
        await temporalio.testing.workflow_check.export_histories(checks, output)
    assert json.loads((output / "manifest.json").read_text()) == manifest


@pytest.mark.parametrize(
    "target,code",
    [
        ("tests.worker.test_workflow_check_export:checks", 0),
        ("tests.worker.test_workflow_check_export:failing_check", 1),
        ("tests.worker.test_workflow_check_export:missing", 2),
    ],
)
def test_workflow_check_export_command(tmp_path: Path, target: str, code: int) -> None:
    output = tmp_path / "corpus"
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "temporalio.testing.workflow_check",
            target,
            "--output",
            str(output),
            "--max-depth",
            "2",
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == code, completed.stdout + completed.stderr
    if code == 0:
        manifest = json.loads((output / "manifest.json").read_text())
        assert len(manifest["checks"]) == 2
        assert all(
            entry["status"] == "no_counterexample" for entry in manifest["checks"]
        )
        assert all(len(entry["histories"]) == 10 for entry in manifest["checks"])
    elif code == 1:
        manifest = json.loads((output / "manifest.json").read_text())
        [entry] = manifest["checks"]
        assert entry["status"] == "counterexample"
        assert (output / entry["counterexample"]).is_file()
    else:
        assert not output.exists()
