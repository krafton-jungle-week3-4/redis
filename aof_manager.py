"""AOF(Append Only File) logging and replay helpers."""

from __future__ import annotations

import json
from pathlib import Path


DEFAULT_AOF_PATH = Path("appendonly.aof")
_active_aof_path = DEFAULT_AOF_PATH


def set_aof_path(path: str | Path) -> Path:
    """현재 프로세스가 사용할 AOF 경로를 바꿉니다."""
    global _active_aof_path
    _active_aof_path = Path(path)
    return _active_aof_path


def get_aof_path() -> Path:
    return _active_aof_path


def reset_aof(path: str | Path | None = None) -> Path:
    """기존 AOF 파일 내용을 비워서 새 로그를 시작합니다."""
    target = Path(path) if path is not None else _active_aof_path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("", encoding="utf-8")
    return target


def append_aof_command(command: list[str], path: str | Path | None = None) -> Path:
    """성공한 쓰기 명령을 JSON Lines 형식으로 AOF 파일 끝에 추가합니다."""
    target = Path(path) if path is not None else _active_aof_path
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(command, ensure_ascii=False) + "\n")
    return target


def load_aof_commands(path: str | Path | None = None) -> list[list[str]]:
    """AOF 파일을 읽어 replay 순서 그대로 명령 목록으로 복원합니다."""
    target = Path(path) if path is not None else _active_aof_path
    if not target.exists():
        return []

    commands: list[list[str]] = []
    for line_number, raw_line in enumerate(target.read_text(encoding="utf-8").splitlines(), start=1):
        stripped = raw_line.strip()
        if not stripped:
            continue
        payload = json.loads(stripped)
        if not isinstance(payload, list) or not all(isinstance(item, str) for item in payload):
            raise ValueError(f"invalid AOF entry at line {line_number}")
        commands.append(payload)
    return commands


def replay_aof(path: str | Path | None = None, delay_sec: float = 0.0) -> Path:
    """AOF 파일 내용을 메모리에 다시 적용해 장애 후 상태를 복구합니다."""
    target = Path(path) if path is not None else _active_aof_path
    commands = load_aof_commands(target)

    from redis import replay_from_aof_commands

    replay_from_aof_commands(commands, delay_sec=delay_sec)
    return target
