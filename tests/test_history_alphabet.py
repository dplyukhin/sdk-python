import json
import shutil
import xml.etree.ElementTree as ET
from itertools import product
from pathlib import Path

import pytest

import temporalio.client
from scripts.history_alphabet import (
    Trace,
    alphabet,
    build_dfa,
    event_graph,
    history_paths,
    main,
    workflow_traces,
)


def _history(activity_id: str = "activity-1") -> temporalio.client.WorkflowHistory:
    return temporalio.client.WorkflowHistory.from_json(
        "workflow-id",
        {
            "events": [
                {
                    "eventId": "1",
                    "eventType": "WorkflowExecutionStarted",
                    "workflowExecutionStartedEventAttributes": {
                        "workflowType": {"name": "Orders"}
                    },
                },
                {
                    "eventId": "2",
                    "eventType": "ActivityTaskScheduled",
                    "activityTaskScheduledEventAttributes": {
                        "activityId": activity_id,
                        "activityType": {"name": "charge"},
                    },
                },
                {
                    "eventId": "3",
                    "eventType": "ActivityTaskCompleted",
                    "activityTaskCompletedEventAttributes": {"scheduledEventId": "2"},
                },
                {
                    "eventId": "4",
                    "eventType": "WorkflowExecutionSignaled",
                    "workflowExecutionSignaledEventAttributes": {
                        "signalName": "cancel\norder"
                    },
                },
                {
                    "eventId": "5",
                    "eventType": "TimerFired",
                    "timerFiredEventAttributes": {"timerId": "timer-1"},
                },
                {
                    "eventId": "6",
                    "eventType": "WorkflowExecutionCompleted",
                    "workflowExecutionCompletedEventAttributes": {},
                },
            ]
        },
    )


def test_alphabet_preserves_types_and_order_but_not_ids() -> None:
    expected = [
        'workflow_started("Orders")',
        'activity_scheduled("charge")',
        'activity_completed("charge")',
        'signal_received("cancel\\norder")',
        "timer_fired",
        "workflow_completed",
    ]
    assert list(alphabet(_history())) == expected
    assert list(alphabet(_history("different-id"))) == expected


def test_alphabet_rejects_missing_activity_schedule() -> None:
    history = _history()
    history.events[2].activity_task_completed_event_attributes.scheduled_event_id = 999
    with pytest.raises(ValueError, match="missing schedule 999"):
        list(alphabet(history))


def test_alphabet_omits_only_the_three_workflow_task_bookkeeping_events() -> None:
    document = _history().to_json_dict()
    document["events"][1:1] = [
        {"eventType": kind}
        for kind in [
            "WorkflowTaskScheduled",
            "WorkflowTaskStarted",
            "WorkflowTaskCompleted",
            "WorkflowTaskFailed",
            "WorkflowTaskTimedOut",
        ]
    ]
    projected = list(
        alphabet(temporalio.client.WorkflowHistory.from_json("id", document))
    )
    assert projected == [
        'workflow_started("Orders")',
        "workflow_task_failed",
        "workflow_task_timed_out",
        *list(alphabet(_history()))[1:],
    ]


def test_manifest_and_cli_preserve_history_boundaries(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    history_path = tmp_path / "history.json"
    history_path.write_text(_history().to_json())
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "checks": [
                    {"histories": [{"path": "history.json"}, {"path": "history.json"}]}
                ],
            }
        )
    )
    assert list(history_paths(tmp_path)) == [history_path, history_path]
    assert main([str(manifest_path)]) == 0
    trace = "\n".join(alphabet(_history()))
    assert capsys.readouterr().out == f"{trace}\n\n{trace}\n"
    assert main([str(history_path)]) == 0
    assert capsys.readouterr().out == f"{trace}\n"


def test_manifest_rejects_paths_outside_corpus(tmp_path: Path) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "checks": [{"histories": [{"path": "../outside.json"}]}],
            }
        )
    )
    with pytest.raises(ValueError, match="escapes"):
        list(history_paths(manifest_path))


def test_dfa_merges_suffixes_without_generalizing_the_language() -> None:
    pytest.importorskip("aalpy")
    words = {("a", "x"), ("b", "x")}
    dfa, _ = build_dfa(Trace(word) for word in words)
    assert len(dfa.states) == 3
    assert dfa.initial_state.transitions["a"] is dfa.initial_state.transitions["b"]
    for length in range(4):
        for word in product(("a", "b", "x"), repeat=length):
            state = dfa.initial_state
            for symbol in word:
                if symbol not in state.transitions:
                    accepted = False
                    break
                state = state.transitions[symbol]
            else:
                accepted = state.is_accepting
            assert accepted == (word in words)


def test_dfa_preserves_endpoint_classifications() -> None:
    pytest.importorskip("aalpy")
    dfa, endings = build_dfa(
        [
            Trace(("a", "x"), "prefix"),
            Trace(("b", "x"), "completed"),
            Trace(("c", "x"), "counterexample"),
        ]
    )
    assert len(dfa.states) == 7
    assert set(endings.values()) == {
        (),
        ("prefix",),
        ("completed",),
        ("counterexample",),
    }


@pytest.mark.parametrize(
    "words",
    [
        {("A", "X"), ("B", "X"), ("C", "X")},
        {("A", "K", "X"), ("B", "K", "Y")},
        {("A",), ("A", "X"), ("B", "X")},
        {(), ("A",)},
    ],
)
def test_event_nodes_and_blank_phi_nodes_preserve_exact_paths(
    words: set[tuple[str, ...]],
) -> None:
    pytest.importorskip("aalpy")
    dfa, endings = build_dfa(Trace(word) for word in words)
    graph = event_graph(dfa, endings)
    nodes = {node.get_name(): node for node in graph.get_nodes()}
    assert all(node.get("peripheries") is None for node in nodes.values())
    assert all(node.get("xlabel") is None for node in nodes.values())
    edges: dict[str, list[str]] = {name: [] for name in nodes}
    for edge in graph.get_edges():
        assert edge.get("label") is None
        source, target = edge.get_source(), edge.get_destination()
        assert isinstance(source, str) and isinstance(target, str)
        edges[source].append(target)
    observed = set()
    pending: list[tuple[str, tuple[str, ...]]] = [("__start0", ())]
    while pending:
        name, path = pending.pop()
        label = nodes[name].get("label")
        if label:
            path += (label,)
        if nodes[name].get("observed_endpoint") == "true":
            observed.add(path)
        pending.extend((target, path) for target in edges[name])
    assert observed == words
    if len(words) == 3 and ("C", "X") in words:
        event_nodes = {
            node.get("label"): name for name, node in nodes.items() if node.get("label")
        }
        [join] = edges[event_nodes["A"]]
        assert edges[event_nodes["B"]] == edges[event_nodes["C"]] == [join]
        assert nodes[join].get("label") == ""
        assert edges[join] == [event_nodes["X"]]


def test_dfa_accepts_only_recorded_endpoints_even_on_long_traces() -> None:
    pytest.importorskip("aalpy")
    symbols = ("event",) * 1_100
    dfa, _ = build_dfa([Trace(symbols), Trace(symbols)])
    assert len(dfa.states) == 1_101
    state = dfa.initial_state
    for symbol in symbols:
        assert not state.is_accepting
        state = state.transitions[symbol]
    assert state.is_accepting
    assert not state.transitions


def test_dfa_groups_implementations_and_marks_manifest_counterexamples(
    tmp_path: Path,
) -> None:
    prefix = temporalio.client.WorkflowHistory("id", _history().events[:-1])
    (tmp_path / "prefix.json").write_text(prefix.to_json())
    (tmp_path / "complete.json").write_text(_history().to_json())
    (tmp_path / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "checks": [
                    {
                        "implementation": "one:Orders",
                        "histories": [
                            {"path": "prefix.json"},
                            {"path": "complete.json"},
                        ],
                    },
                    {
                        "implementation": "two:Orders",
                        "histories": [{"path": "prefix.json"}],
                        "counterexample": "prefix.json",
                    },
                    {
                        "implementation": "one:Orders",
                        "histories": [{"path": "prefix.json"}],
                    },
                ],
            }
        )
    )
    groups = workflow_traces([tmp_path])
    assert len(groups) == 2
    assert groups[0].workflow_type == groups[1].workflow_type == "Orders"
    assert groups[0].history_count == 3
    assert {trace.ending for trace in groups[0].traces} == {"prefix", "completed"}
    assert len(groups[0].traces) == 2
    assert {trace.ending for trace in groups[1].traces} == {"counterexample"}


def test_dfa_cli_writes_dot_svg_and_index_without_overwriting(tmp_path: Path) -> None:
    pytest.importorskip("aalpy")
    if shutil.which("dot") is None:
        pytest.skip("Graphviz is not installed")
    history_path = tmp_path / "history.json"
    history_path.write_text(_history().to_json())
    output = tmp_path / "dfas"
    assert main([str(history_path), "--dfa-dir", str(output)]) == 0
    [model] = json.loads((output / "index.json").read_text())["models"]
    assert model["workflow_type"] == "Orders"
    assert model["distinct_traces"] == 1
    assert model["histories"] == 1
    assert (output / model["dot"]).is_file()
    svg = ET.parse(output / model["svg"])
    assert svg.getroot().tag == "{http://www.w3.org/2000/svg}svg"
    text = " ".join(svg.getroot().itertext())
    assert 'activity_scheduled("charge")' in text
    assert 'signal_received("cancel\\norder")' in text
    assert "completed" in text
    assert model["svg"] in (output / "index.html").read_text()
    with pytest.raises(SystemExit) as err:
        main([str(history_path), "--dfa-dir", str(output)])
    assert err.value.code == 2
    sentinel = output / "notes.txt"
    sentinel.write_text("keep this")
    assert main([str(history_path), "--dfa-dir", str(output), "--overwrite"]) == 0
    assert sentinel.read_text() == "keep this"
