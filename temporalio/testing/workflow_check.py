"""Export generated workflow histories: ``python -m temporalio.testing.workflow_check``."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import json
from collections.abc import Sequence
from datetime import timedelta
from pathlib import Path
from typing import Any

import temporalio.client
import temporalio.testing
import temporalio.workflow


async def export_histories(
    specs: Sequence[temporalio.testing.WorkflowCheckSpec],
    output: Path,
    *,
    samples_per_strategy: int = 3,
    max_depth: int = 20,
    max_checks: int = 1_000,
    timeout: timedelta = timedelta(seconds=30),
    task_timeout: timedelta = timedelta(seconds=10),
) -> int:
    """Check specs and write a new JSON history corpus without overwriting files.

    Each spec gets a numbered directory. ``manifest.json`` records the bounds,
    workflow types, history paths and workflow IDs needed by Replayer. Status
    ``no_counterexample`` means only that no failure was found within the bounds.
    Model errors preserve histories collected before the error and do not prevent
    subsequent specs from running.

    Returns 0 for no counterexamples, 1 for counterexamples, or 2 for errors.
    Histories contain generated input and result payloads; protect them like
    production histories when using strategies containing sensitive values.
    """
    if not specs:
        raise ValueError("Supply at least one workflow check spec")
    if samples_per_strategy <= 0 or max_checks <= 0 or max_depth < 0:
        raise ValueError(
            "Sample and check counts must be positive; depth must be nonnegative"
        )
    if timeout < timedelta(milliseconds=1) or task_timeout < timedelta(milliseconds=1):
        raise ValueError("Timeouts must be at least one millisecond")
    output.mkdir(parents=True, exist_ok=False)
    manifest: dict[str, Any] = {
        "schema_version": 1,
        "bounds_per_start": {
            "samples_per_strategy": samples_per_strategy,
            "max_depth": max_depth,
            "max_checks": max_checks,
            "timeout_seconds": timeout.total_seconds(),
            "task_timeout_seconds": task_timeout.total_seconds(),
        },
        "checks": [],
    }
    manifest_path = output / "manifest.json"

    def save_manifest() -> None:
        manifest_path.write_text(
            json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
        )

    save_manifest()
    exit_code = 0
    for index, spec in enumerate(specs):
        directory = output / f"{index:04d}"
        directory.mkdir()
        entry: dict[str, Any] = {
            "implementation": f"{spec.workflow.__module__}:{spec.workflow.__qualname__}",
            "namespace": spec.namespace,
            "status": "running",
            "histories": [],
        }
        manifest["checks"].append(entry)
        save_manifest()

        def write_history(history: temporalio.client.WorkflowHistory) -> None:
            path = directory / f"{len(entry['histories']):06d}.json"
            path.write_text(history.to_json() + "\n", encoding="utf-8")
            entry["histories"].append(
                {
                    "path": path.relative_to(output).as_posix(),
                    "workflow_id": history.workflow_id,
                }
            )

        try:
            definition = temporalio.workflow._Definition.must_from_class(spec.workflow)
            entry["workflow_type"] = definition.name
            result = await temporalio.testing.WorkflowChecker(
                workflows=[spec.workflow],
                namespace=spec.namespace,
                data_converter=spec.data_converter,
            ).check_workflow(
                definition.run_fn,
                spec.arg,
                args=spec.args,
                signals=spec.signals,
                activities=spec.activities,
                workflow_cancellation=spec.workflow_cancellation,
                samples_per_strategy=samples_per_strategy,
                max_depth=max_depth,
                max_checks=max_checks,
                timeout=timeout,
                task_timeout=task_timeout,
                on_history=write_history,
            )
            entry["status"] = (
                "counterexample"
                if result.exception is not None
                else "no_counterexample"
            )
            if result.exception is not None:
                entry["failure"] = str(result.exception)
                entry["counterexample"] = entry["histories"][-1]["path"]
                exit_code = max(exit_code, 1)
        except Exception as err:
            entry["status"] = "error"
            entry["error"] = f"{type(err).__name__}: {err}"
            exit_code = 2
        finally:
            save_manifest()
        print(
            f"{entry.get('workflow_type', entry['implementation'])}: "
            f"{entry['status']} ({len(entry['histories'])} histories)"
        )
    print(f"History manifest: {manifest_path}")
    return exit_code


def _load_specs(target: str) -> list[temporalio.testing.WorkflowCheckSpec]:
    module_name, separator, attribute = target.partition(":")
    if not separator or not module_name or not attribute:
        raise ValueError(f"Expected module:attribute, got {target!r}")
    value = getattr(importlib.import_module(module_name), attribute)
    if isinstance(value, temporalio.testing.WorkflowCheckSpec):
        return [value]
    if not isinstance(value, Sequence) or not all(
        isinstance(spec, temporalio.testing.WorkflowCheckSpec) for spec in value
    ):
        raise ValueError(
            f"{target!r} must reference a WorkflowCheckSpec or a sequence of specs"
        )
    return list(value)


def main() -> int:
    """Run the history-export command."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "specs", nargs="+", help="Python module:attribute containing check specs"
    )
    parser.add_argument(
        "--output", required=True, type=Path, help="New corpus directory"
    )
    parser.add_argument("--samples-per-strategy", type=int, default=3)
    parser.add_argument("--max-depth", type=int, default=20)
    parser.add_argument("--max-checks", type=int, default=1_000)
    parser.add_argument(
        "--timeout", type=float, default=30, help="Seconds per sampled start"
    )
    parser.add_argument(
        "--task-timeout", type=float, default=10, help="Seconds per task"
    )
    options = parser.parse_args()
    try:
        specs = [spec for target in options.specs for spec in _load_specs(target)]
        return asyncio.run(
            export_histories(
                specs,
                options.output,
                samples_per_strategy=options.samples_per_strategy,
                max_depth=options.max_depth,
                max_checks=options.max_checks,
                timeout=timedelta(seconds=options.timeout),
                task_timeout=timedelta(seconds=options.task_timeout),
            )
        )
    except (ValueError, OSError, ImportError, AttributeError) as err:
        parser.error(str(err))


if __name__ == "__main__":
    raise SystemExit(main())
