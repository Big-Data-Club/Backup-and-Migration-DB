#!/usr/bin/env python3
"""Update local CoreApplication DB hosts and credentials without printing them."""

from __future__ import annotations

import os
import re
import sys
import tempfile
from pathlib import Path
from urllib.parse import unquote, urlsplit


HOST_KEYS = (
    "POSTGRES_HOST",
    "LMS_POSTGRES_HOST",
    "LAB_POSTGRES_HOST",
    "CHAT_POSTGRES_HOST",
    "AI_POSTGRES_HOST",
    "DUTYLOG_POSTGRES_HOST",
)
USER_KEYS = (
    "POSTGRES_USER",
    "LMS_POSTGRES_USER",
    "LAB_POSTGRES_USER",
    "CHAT_POSTGRES_USER",
    "AI_POSTGRES_USER",
    "DUTYLOG_POSTGRES_USER",
)
PASSWORD_KEYS = (
    "POSTGRES_PASSWORD",
    "LMS_POSTGRES_PASSWORD",
    "LAB_POSTGRES_PASSWORD",
    "CHAT_POSTGRES_PASSWORD",
    "AI_POSTGRES_PASSWORD",
    "DUTYLOG_POSTGRES_PASSWORD",
)


def atomic_write(path: Path, content: str, mode: int) -> None:
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(fd, mode)
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(content)
        os.replace(temporary_name, path)
    except BaseException:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def update_env(path: Path, replacements: dict[str, str]) -> None:
    original = path.read_text(encoding="utf-8")
    output = original.splitlines(keepends=True)
    last_index: dict[str, int] = {}
    for index, line in enumerate(output):
        match = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)=", line)
        if match and match.group(1) in replacements:
            last_index[match.group(1)] = index
    for key, index in last_index.items():
        ending = "\n" if output[index].endswith("\n") else ""
        output[index] = f"{key}={replacements[key]}{ending}"
    missing = sorted(set(replacements) - set(last_index))
    if missing:
        suffix = "" if original.endswith("\n") else "\n"
        output.append(suffix + "\n".join(f"{key}={replacements[key]}" for key in missing) + "\n")
    atomic_write(path, "".join(output), path.stat().st_mode & 0o777)


def update_configmap(path: Path, host: str) -> None:
    content = path.read_text(encoding="utf-8")
    for key in HOST_KEYS:
        pattern = re.compile(rf'^(\s*{re.escape(key)}:\s*)"[^"]*"\s*$', re.MULTILINE)
        content, count = pattern.subn(rf'\1"{host}"', content)
        if count != 1:
            raise RuntimeError(f"Expected exactly one {key} in {path}, found {count}")
    atomic_write(path, content, path.stat().st_mode & 0o777)


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: update_core_config.py <CoreApplication-dir>", file=sys.stderr)
        return 2
    destination = urlsplit(os.environ["DESTINATION_URL"])
    if destination.scheme not in {"postgresql", "postgres"}:
        raise SystemExit("Destination must be a PostgreSQL URL")
    if not destination.hostname or destination.username is None or destination.password is None:
        raise SystemExit("Destination URL must include host, user, and password")
    if not destination.path.lstrip("/"):
        raise SystemExit("Destination URL must include an administrative database path")
    if destination.port not in (None, 5432):
        raise SystemExit("Core production manifests currently require PostgreSQL port 5432")

    core_dir = Path(sys.argv[1]).resolve()
    env_path = core_dir / ".env"
    configmap_path = core_dir / "k3s" / "base" / "configmap.yaml"
    if not env_path.is_file() or not configmap_path.is_file():
        raise SystemExit(f"Invalid CoreApplication directory: {core_dir}")

    replacements = {key: destination.hostname for key in HOST_KEYS}
    replacements.update({key: unquote(destination.username) for key in USER_KEYS})
    replacements.update({key: unquote(destination.password) for key in PASSWORD_KEYS})
    update_env(env_path, replacements)
    update_configmap(configmap_path, destination.hostname)
    print(f"Updated local Core DB configuration in {core_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
