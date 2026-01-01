from __future__ import annotations

import logging
import subprocess
from pathlib import Path
from typing import Iterable, List, Optional

from lib.config.form5500_config import ScriptConfig
from lib.models.form5500_state import ScriptState

logger = logging.getLogger(__name__)


class ScriptRunnerError(RuntimeError):
    pass


def validate_script(state: ScriptState, config: ScriptConfig) -> ScriptState:
    script_path = config.script_path
    if not script_path.exists():
        raise ScriptRunnerError(f"Script not found: {script_path}")
    if not script_path.is_file():
        raise ScriptRunnerError(f"Expected a file to execute, got: {script_path}")

    state["script_path"] = script_path
    state["script_args"] = list(config.args or [])
    return state


def run_script(state: ScriptState, _: ScriptConfig) -> ScriptState:
    script_path: Path = state["script_path"]
    args: List[str] = state.get("script_args", [])

    cmd = ["python", str(script_path), *args]
    logger.info("Running legacy script: %s", " ".join(cmd))

    try:
        proc = subprocess.run(cmd, check=True, capture_output=True, text=True)
        state["stdout"] = proc.stdout
        state["stderr"] = proc.stderr
    except subprocess.CalledProcessError as exc:
        raise ScriptRunnerError(
            f"Script {script_path} failed with exit code {exc.returncode}.\nSTDOUT:\n{exc.stdout}\nSTDERR:\n{exc.stderr}"
        ) from exc
    return state


def finalize(state: ScriptState, _: ScriptConfig) -> ScriptState:
    # No resources to clean up for script runner
    return state
