"""Namespace version switching commands."""

from __future__ import annotations

from core_state import current_namespace, switch_namespace


FIXED_ARITY: dict[str, int] = {
    "SWITCHVER": 2,
    "CURRENTVER": 1,
}


def execute_version_command(command_name: str, command: list[str]) -> dict | None:
    if command_name == "SWITCHVER":
        switch_namespace(command[1])
        return {"type": "simple_string", "value": "OK"}

    if command_name == "CURRENTVER":
        return {"type": "bulk_string", "value": current_namespace()}

    return None
