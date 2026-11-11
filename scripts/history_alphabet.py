"""Print history symbols or visualize their exact observed language as DFAs.

Usage::

    uv run python scripts/history_alphabet.py history-corpus/
    uv run python scripts/history_alphabet.py first.json second.json
    uv run --with 'aalpy>=1.6,<2' python scripts/history_alphabet.py \
        history-corpus/ --dfa-dir build/history-dfas

Text histories are separated by a blank line, without filename headers on stdout.
Symbols preserve event kinds, workflow/activity types, signal names, and marker
names. Activity outcomes refer to the scheduled activity's type, not its ID.
Arguments, results, failure details, timestamps, timer IDs, and other metadata
are discarded, as are workflow_task_scheduled, workflow_task_started, and
workflow_task_completed events. Other event kinds
are represented by their event type alone; this is a deliberately lossy alphabet,
not an equivalence or compatibility check.

DFA mode requires AALpy and Graphviz's ``dot`` executable. It writes an SVG and
event-node DOT diagram per workflow, plus the underlying edge-labeled DFA DOT
and an HTML index. Diagram event nodes carry the alphabet labels; arrows are
unlabeled. Blank synthetic nodes preserve shared continuation states, with
single-incoming synthetic nodes collapsed. Histories are grouped by workflow type
and implementation when a corpus manifest supplies it, so unrelated sample
classes sharing a Temporal type name are not combined. Equivalent suffixes are
merged without learning or generalizing new traces. Missing transitions reject
implicitly; the rejecting sink is omitted from diagrams. Prefix endpoints and
acceptance outlines are hidden; counterexamples and terminal outcomes remain
annotated. Endpoint classifications are preserved internally during merging.
Search truncation and the lossy alphabet still limit coverage. Use ``--overwrite``
to update an existing visualization directory without deleting unrelated files.
"""

from __future__ import annotations

import argparse
import html
import json
import re
import shutil
import subprocess
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import temporalio.api.enums.v1
import temporalio.client

if TYPE_CHECKING:
    import pydot
    from aalpy.automata import Dfa


@dataclass(frozen=True)
class HistoryInput:
    """History location and optional corpus annotations."""

    path: Path
    implementation: str = ""
    counterexample: bool = False


@dataclass(frozen=True, order=True)
class Trace:
    """Projected history and its observed endpoint classification."""

    symbols: tuple[str, ...]
    ending: str = "prefix"


@dataclass
class WorkflowTraces:
    """Histories grouped by Temporal type and registered implementation."""

    workflow_type: str
    implementation: str
    traces: set[Trace] = field(default_factory=set)
    history_count: int = 0


def history_paths(source: Path) -> Iterator[Path]:
    """Yield files in corpus manifest order, or one standalone history file."""
    for record in history_inputs(source):
        yield record.path


def history_inputs(source: Path) -> Iterator[HistoryInput]:
    """Read locations and endpoint annotations without trusting manifest paths."""
    path = source / "manifest.json" if source.is_dir() else source
    document = json.loads(path.read_text(encoding="utf-8"))
    if "events" in document:
        yield HistoryInput(path)
        return
    if document.get("schema_version") != 1 or "checks" not in document:
        raise ValueError(f"{path}: expected a history or a version 1 corpus manifest")
    root = path.parent.resolve()
    for check in document["checks"]:
        for record in check["histories"]:
            history_path = (root / record["path"]).resolve()
            if not history_path.is_relative_to(root):
                raise ValueError(f"{path}: history path escapes the corpus directory")
            yield HistoryInput(
                history_path,
                check.get("implementation", ""),
                record["path"] == check.get("counterexample"),
            )


def alphabet(history: temporalio.client.WorkflowHistory) -> Iterator[str]:
    """Yield event symbols in recorded order without decoding payloads."""
    activity_types = {
        event.event_id: event.activity_task_scheduled_event_attributes.activity_type.name
        for event in history.events
        if event.HasField("activity_task_scheduled_event_attributes")
    }
    for event in history.events:
        kind = (
            temporalio.api.enums.v1.EventType.Name(event.event_type)
            .removeprefix("EVENT_TYPE_")
            .lower()
        )
        if kind in {
            "workflow_task_scheduled",
            "workflow_task_started",
            "workflow_task_completed",
        }:
            continue
        symbol = kind.replace("workflow_execution_", "workflow_", 1).replace(
            "activity_task_", "activity_", 1
        )
        name: str | None = None
        if kind == "activity_task_scheduled":
            name = activity_types[event.event_id]
        elif kind.startswith("activity_task_"):
            attributes_name = event.WhichOneof("attributes")
            if attributes_name is None:
                raise ValueError(f"Activity event {event.event_id} has no attributes")
            scheduled_id = getattr(event, attributes_name).scheduled_event_id
            if scheduled_id not in activity_types:
                raise ValueError(
                    f"Activity event {event.event_id} references missing schedule {scheduled_id}"
                )
            name = activity_types[scheduled_id]
        elif kind == "workflow_execution_started":
            name = event.workflow_execution_started_event_attributes.workflow_type.name
        elif kind == "workflow_execution_signaled":
            symbol = "signal_received"
            name = event.workflow_execution_signaled_event_attributes.signal_name
        elif kind == "marker_recorded":
            name = event.marker_recorded_event_attributes.marker_name
        yield symbol if name is None else f"{symbol}({json.dumps(name)})"


def workflow_traces(sources: Sequence[Path]) -> list[WorkflowTraces]:
    """Load and deduplicate projected traces, preserving endpoint classifications."""
    groups: dict[tuple[str, str], WorkflowTraces] = {}
    terminal_kinds = {
        "workflow_execution_completed_event_attributes": "completed",
        "workflow_execution_failed_event_attributes": "failed",
        "workflow_execution_canceled_event_attributes": "cancelled",
        "workflow_execution_terminated_event_attributes": "terminated",
        "workflow_execution_timed_out_event_attributes": "timed_out",
        "workflow_execution_continued_as_new_event_attributes": "continued_as_new",
    }
    for source in sources:
        for record in history_inputs(source):
            history = temporalio.client.WorkflowHistory.from_json(
                record.path.stem, record.path.read_text(encoding="utf-8")
            )
            if not history.events or not history.events[0].HasField(
                "workflow_execution_started_event_attributes"
            ):
                raise ValueError(
                    f"{record.path}: history must begin with workflow start"
                )
            name = history.events[
                0
            ].workflow_execution_started_event_attributes.workflow_type.name
            key = (name, record.implementation)
            group = groups.setdefault(key, WorkflowTraces(*key))
            ending = terminal_kinds.get(
                history.events[-1].WhichOneof("attributes") or "", "prefix"
            )
            if record.counterexample:
                ending = "counterexample"
            group.traces.add(Trace(tuple(alphabet(history)), ending))
            group.history_count += 1
    return [groups[key] for key in sorted(groups)]


def build_dfa(traces: Iterable[Trace]) -> tuple[Dfa, dict[str, tuple[str, ...]]]:
    """Build an AALpy prefix acceptor, merging suffixes with identical annotations.

    Only exact observed symbol sequences are accepted. Endpoint classifications
    prevent a truncated or counterexample endpoint from merging with a terminal
    endpoint even if their residual languages happen to be identical.
    """
    from aalpy.automata import Dfa, DfaState

    children: list[dict[str, int]] = [{}]
    endings: list[set[str]] = [set()]
    for trace in sorted(set(traces)):
        node = 0
        for symbol in trace.symbols:
            if symbol not in children[node]:
                children[node][symbol] = len(children)
                children.append({})
                endings.append(set())
            node = children[node][symbol]
        endings[node].add(trace.ending)

    canonical: dict[int, int] = {}
    signatures: dict[tuple, int] = {}
    merged: list[tuple[tuple[str, ...], tuple[tuple[str, int], ...]]] = []
    # Trie children always have higher indexes, so reverse order avoids recursion
    # limits on long histories while computing identical residual languages.
    for node in reversed(range(len(children))):
        signature = (
            tuple(sorted(endings[node])),
            tuple(
                (symbol, canonical[child])
                for symbol, child in sorted(children[node].items())
            ),
        )
        if signature not in signatures:
            signatures[signature] = len(merged)
            merged.append(signature)
        canonical[node] = signatures[signature]

    root = canonical[0]
    states = {root: DfaState("q0", is_accepting=bool(merged[root][0]))}
    pending = [root]
    for node in pending:
        for symbol, child in merged[node][1]:
            if child not in states:
                states[child] = DfaState(
                    f"q{len(states)}", is_accepting=bool(merged[child][0])
                )
                pending.append(child)
            states[node].transitions[symbol] = states[child]
    return Dfa(states[root], list(states.values())), {
        state.state_id: merged[node][0] for node, state in states.items()
    }


def event_graph(dfa: Dfa, endings: dict[str, tuple[str, ...]]) -> pydot.Dot:
    """Project DFA edges onto event nodes, preserving shared states as blank phi nodes."""
    import pydot

    graph = pydot.Dot("observed_histories", graph_type="digraph")
    incoming: dict[str, list[str]] = {state.state_id: [] for state in dfa.states}
    transitions: list[tuple[str, str, str, str]] = []
    for state in dfa.states:
        for symbol, target in state.transitions.items():
            event_id = f"event{len(transitions)}"
            transitions.append((state.state_id, event_id, symbol, target.state_id))
            incoming[target.state_id].append(event_id)
            # Graphviz treats backslash sequences in labels as layout escapes.
            graph.add_node(
                pydot.Node(event_id, label=symbol.replace("\\", "\\\\"), shape="box")
            )

    anchors: dict[str, str] = {}
    for state in dfa.states:
        state_id = state.state_id
        if state is dfa.initial_state or len(incoming[state_id]) != 1:
            anchors[state_id] = state_id
            node = pydot.Node(
                state_id,
                label="",
                shape="circle",
                fixedsize="true",
                width="0.18",
                height="0.18",
            )
            graph.add_node(node)
        else:
            anchors[state_id] = incoming[state_id][0]
            [node] = graph.get_node(anchors[state_id])
        if endings[state_id]:
            node.set("observed_endpoint", "true")
            visible_endings = [kind for kind in endings[state_id] if kind != "prefix"]
            if visible_endings:
                node.set("xlabel", ", ".join(visible_endings))
    for source, event_id, _, target in transitions:
        graph.add_edge(pydot.Edge(anchors[source], event_id))
        if anchors[target] != event_id:
            graph.add_edge(pydot.Edge(event_id, anchors[target]))
    graph.add_node(pydot.Node("__start0", shape="none", label=""))
    graph.add_edge(pydot.Edge("__start0", anchors[dfa.initial_state.state_id]))
    return graph


def visualize_dfas(
    groups: Sequence[WorkflowTraces], output: Path, *, overwrite: bool = False
) -> None:
    """Write diagrams and AALpy models, optionally refreshing an existing output."""
    try:
        from aalpy.utils import save_automaton_to_file
    except ImportError as err:
        raise ValueError(
            "DFA visualization requires AALpy; run uv run --with 'aalpy>=1.6,<2' python scripts/history_alphabet.py"
        ) from err
    if shutil.which("dot") is None:
        raise ValueError("DFA visualization requires Graphviz's dot executable on PATH")
    if not groups:
        raise ValueError("No histories found")
    if overwrite and output.exists() and not (output / "index.json").is_file():
        raise ValueError("Overwriting requires an existing DFA visualization directory")
    output.mkdir(parents=True, exist_ok=overwrite)
    models = []
    links = []
    for index, group in enumerate(groups):
        dfa, endings = build_dfa(group.traces)
        graph = event_graph(dfa, endings)
        # AALpy quotes DOT labels without escaping their contents. Escape only
        # this visualization model so quoted names remain literal graph labels.
        for state in dfa.states:
            state.transitions = {
                symbol.replace("\\", "\\\\").replace('"', '\\"'): target
                for symbol, target in state.transitions.items()
            }
        dot_source = save_automaton_to_file(dfa, file_type="string")
        if dot_source is None:
            raise ValueError(f"AALpy did not produce DOT for {group.workflow_type}")
        graph.set(
            "label",
            f"{group.workflow_type}\n{group.implementation}\n"
            "Event labels are nodes; blank circles are synthetic states; arrows are unlabeled.\n"
            "Missing transition = unobserved/reject; no speculative generalization.",
        )
        graph.set("labelloc", "t")
        slug = re.sub(r"[^a-zA-Z0-9_-]+", "-", group.workflow_type)[:80] or "workflow"
        stem = f"{index:04d}-{slug}"
        dot_path = output / f"{stem}.dot"
        dfa_path = output / f"{stem}.dfa.dot"
        svg_path = output / f"{stem}.svg"
        dot_path.write_text(graph.to_string(), encoding="utf-8")
        dfa_path.write_text(dot_source, encoding="utf-8")
        subprocess.run(
            ["dot", "-Tsvg", str(dot_path), "-o", str(svg_path)],
            check=True,
            capture_output=True,
            text=True,
        )
        model = {
            "workflow_type": group.workflow_type,
            "implementation": group.implementation,
            "histories": group.history_count,
            "distinct_traces": len({trace.symbols for trace in group.traces}),
            "states": len(dfa.states),
            "dot": dot_path.name,
            "dfa_dot": dfa_path.name,
            "svg": svg_path.name,
        }
        models.append(model)
        links.append(
            f'<li><a href="{svg_path.name}">{html.escape(group.workflow_type)}</a>'
            f" — {html.escape(group.implementation)}"
            f" ({model['distinct_traces']} traces, {model['states']} DFA states)"
            f' · <a href="{dot_path.name}">Diagram DOT</a>'
            f' · <a href="{dfa_path.name}">DFA DOT</a></li>'
        )
        print(
            f"{group.workflow_type}: {model['distinct_traces']} traces, {model['states']} states → {svg_path}"
        )
    (output / "index.json").write_text(
        json.dumps({"models": models}, indent=2) + "\n", encoding="utf-8"
    )
    (output / "index.html").write_text(
        '<!doctype html><html lang="en"><meta charset="utf-8">'
        "<title>Workflow history DFAs</title><h1>Workflow history DFAs</h1>"
        "<p>Event labels are nodes, arrows are unlabeled, and blank circles are "
        "synthetic continuation states. Workflow-task scheduled, started, and completed "
        "events are omitted. Exact observed traces after alphabet projection; missing "
        "edges reject implicitly. Recorded endpoints are not necessarily completed "
        "workflows. Counterexamples and terminal outcomes are annotated.</p>"
        "<ul>" + "".join(links) + "</ul></html>\n",
        encoding="utf-8",
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Print event symbols or export per-workflow DFAs for selected histories."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "paths",
        nargs="+",
        type=Path,
        help="Corpus directories, manifests, or history JSON files",
    )
    parser.add_argument(
        "--dfa-dir",
        type=Path,
        help="Write exact AALpy DFAs and SVG diagrams to a new directory instead of printing symbols",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Update files in an existing DFA visualization directory",
    )
    options = parser.parse_args(argv)
    if options.overwrite and options.dfa_dir is None:
        parser.error("--overwrite requires --dfa-dir")
    first = True
    try:
        if options.dfa_dir is not None:
            visualize_dfas(
                workflow_traces(options.paths),
                options.dfa_dir,
                overwrite=options.overwrite,
            )
            return 0
        for source in options.paths:
            for path in history_paths(source):
                history = temporalio.client.WorkflowHistory.from_json(
                    path.stem, path.read_text(encoding="utf-8")
                )
                if not first:
                    print()
                first = False
                try:
                    for symbol in alphabet(history):
                        print(symbol)
                except ValueError as err:
                    raise ValueError(f"{path}: {err}") from err
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as err:
        parser.error(str(err))
    except subprocess.CalledProcessError as err:
        parser.error(f"Graphviz failed: {err.stderr}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
