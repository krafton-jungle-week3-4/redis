"""Common key commands shared by all data types."""

from __future__ import annotations

from core_state import delete_key_everywhere, key_exists, key_type


def execute_common_key_command(command_name: str, command: list[str]) -> dict | None:
    if command_name == "DEL":
        key = command[1]
        deleted = 1 if key_exists(key) else 0
        delete_key_everywhere(key)
        return {"type": "integer", "value": deleted}

    if command_name == "EXISTS":
        key = command[1]
        return {"type": "integer", "value": 1 if key_exists(key) else 0}

    if command_name == "TYPE":
        key = command[1]
        return {"type": "bulk_string", "value": key_type(key)}

    return None
