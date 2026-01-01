from __future__ import annotations

from functools import partial
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

from langgraph.graph import END, StateGraph

from lib.config.form5500_config import CalculatedFieldsConfig, IngestConfig, ScriptConfig
from lib.models.form5500_state import CalcState, IngestState
from .nodes import calc as calc_nodes
from .nodes import ingest as ingest_nodes
from .nodes import script_runner


NODE_ORDER = [
    "prepare_files",
    "load_headers",
    "open_connection",
    "inspect_schema",
    "open_connection",
    "inspect_schema",
    "load_headers",
    "sync_schema",
    "stage_csv",
    "validate",
    "upsert",
    "apply_calculated_fields",
    "finalize",
]


def build_ingest_graph(config: IngestConfig):
    graph = StateGraph(IngestState)

    graph.add_node("prepare_files", partial(ingest_nodes.prepare_files, config=config))
    graph.add_node("open_connection", partial(ingest_nodes.open_connection, config=config))
    graph.add_node("inspect_schema", partial(ingest_nodes.inspect_schema, config=config))
    graph.add_node("load_headers", partial(ingest_nodes.load_headers, config=config))
    graph.add_node("sync_schema", partial(ingest_nodes.sync_schema, config=config))
    graph.add_node("stage_csv", partial(ingest_nodes.stage_csv, config=config))
    graph.add_node("validate", partial(ingest_nodes.validate, config=config))
    graph.add_node("upsert", partial(ingest_nodes.upsert, config=config))
    graph.add_node("apply_calculated_fields", partial(ingest_nodes.apply_calculated_fields, config=config))
    graph.add_node("finalize", partial(ingest_nodes.finalize, config=config))

    graph.set_entry_point("prepare_files")
    for left, right in zip(NODE_ORDER, NODE_ORDER[1:]):
        graph.add_edge(left, right)
    graph.add_edge("finalize", END)

    return graph.compile()


def run_ingest(config: IngestConfig) -> Dict[str, Any]:
    graph = build_ingest_graph(config)
    initial_state: IngestState = {}
    final_state = graph.invoke(initial_state)
    return final_state


def build_script_graph(config: ScriptConfig):
    graph = StateGraph(dict)  # simple dict state

    graph.add_node("validate_script", partial(script_runner.validate_script, config=config))
    graph.add_node("run_script", partial(script_runner.run_script, config=config))
    graph.add_node("finalize", partial(script_runner.finalize, config=config))

    graph.set_entry_point("validate_script")
    graph.add_edge("validate_script", "run_script")
    graph.add_edge("run_script", "finalize")
    graph.add_edge("finalize", END)

    return graph.compile()


def run_script(config: ScriptConfig) -> Dict[str, Any]:
    graph = build_script_graph(config)
    return graph.invoke({})


def build_calc_graph(config: CalculatedFieldsConfig):
    graph = StateGraph(CalcState)
    graph.add_node("prepare", partial(calc_nodes.prepare, config=config))
    graph.add_node("run_scripts", partial(calc_nodes.run_scripts, config=config))
    graph.add_node("finalize", partial(calc_nodes.finalize, config=config))

    graph.set_entry_point("prepare")
    graph.add_edge("prepare", "run_scripts")
    graph.add_edge("run_scripts", "finalize")
    graph.add_edge("finalize", END)
    return graph.compile()


def run_calc(config: CalculatedFieldsConfig) -> Dict[str, Any]:
    graph = build_calc_graph(config)
    return graph.invoke({})


def run_legacy_script(script_path: Path, args: Sequence[str] | None = None) -> Dict[str, Any]:
    """Helper to execute a legacy Python script via LangGraph script runner."""
    config = ScriptConfig(script_path=script_path, args=args)
    return run_script(config)
