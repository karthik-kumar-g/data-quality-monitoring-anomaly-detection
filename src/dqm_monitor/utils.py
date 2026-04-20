"""Utility helpers shared across scripts and modules."""

from __future__ import annotations

from pathlib import Path
import re


def ensure_directories(paths: list[Path]) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def numeric_batch_id(name: str) -> int:
    stem = Path(name).stem
    match = re.search(r"batch_(\d+)", stem)
    if match:
        return int(match.group(1))
    digits = re.findall(r"\d+", stem)
    if not digits:
        raise ValueError(f"Could not extract batch id from {name}")
    return int(digits[0])


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
