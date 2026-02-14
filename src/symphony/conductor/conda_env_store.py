from __future__ import annotations

import json
import time
from typing import Optional

from symphony.conductor.models import CondaEnvCreate, CondaEnvResponse, CondaEnvUpdate
from symphony.interface.sqlite import SQLiteAsyncDB

sqlite_db_conn = SQLiteAsyncDB()


def _now_ms() -> int:
    return int(time.time() * 1000)


def _row_to_out(row) -> CondaEnvResponse:
    packages_raw = row["packages"]
    packages = (
        json.loads(packages_raw)
        if isinstance(packages_raw, str)
        else (packages_raw or [])
    )
    return CondaEnvResponse(
        name=row["name"],
        python_version=row["python_version"],
        packages=packages,
        custom_script=row["custom_script"] if "custom_script" in row.keys() else "",
        created_at_ms=row["created_at_ms"],
        updated_at_ms=row["updated_at_ms"],
    )


async def create(data: CondaEnvCreate) -> CondaEnvResponse:
    now = _now_ms()
    packages_json = json.dumps(
        data.packages, separators=(",", ":"), ensure_ascii=False
    )

    await sqlite_db_conn.execute(
        """
        INSERT INTO conda_envs (
            name, python_version, packages, custom_script,
            created_at_ms, updated_at_ms
        ) VALUES (?, ?, json(?), ?, ?, ?)
        """,
        (
            data.name,
            data.python_version,
            packages_json,
            data.custom_script,
            now,
            now,
        ),
    )

    row = await sqlite_db_conn.fetchone(
        "SELECT * FROM conda_envs WHERE name = ?", (data.name,)
    )
    assert row is not None
    return _row_to_out(row)


async def get(name: str) -> Optional[CondaEnvResponse]:
    row = await sqlite_db_conn.fetchone(
        "SELECT * FROM conda_envs WHERE name = ?", (name,)
    )
    return _row_to_out(row) if row else None


async def list(limit: int = 100, offset: int = 0) -> list[CondaEnvResponse]:
    rows = await sqlite_db_conn.fetchall(
        """
        SELECT * FROM conda_envs
        ORDER BY created_at_ms DESC
        LIMIT ? OFFSET ?
        """,
        (limit, offset),
    )
    return [_row_to_out(r) for r in rows]


async def list_all() -> list[CondaEnvResponse]:
    rows = await sqlite_db_conn.fetchall(
        """
        SELECT * FROM conda_envs
        ORDER BY created_at_ms DESC
        """
    )
    return [_row_to_out(r) for r in rows]


async def delete(name: str) -> bool:
    existing = await get(name)
    if not existing:
        return False
    await sqlite_db_conn.execute("DELETE FROM conda_envs WHERE name = ?", (name,))
    return True


async def update(name: str, data: CondaEnvUpdate) -> Optional[CondaEnvResponse]:
    existing = await get(name)
    if not existing:
        return None

    patch = data.model_dump(exclude_none=True)
    if not patch:
        return existing

    updates = []
    params = []

    if "packages" in patch:
        packages_json = json.dumps(
            patch["packages"], separators=(",", ":"), ensure_ascii=False
        )
        updates.append("packages = json(?)")
        params.append(packages_json)

    if "custom_script" in patch:
        updates.append("custom_script = ?")
        params.append(patch["custom_script"])

    updates.append("updated_at_ms = ?")
    params.append(_now_ms())
    params.append(name)

    await sqlite_db_conn.execute(
        f"UPDATE conda_envs SET {', '.join(updates)} WHERE name = ?",
        tuple(params),
    )

    row = await sqlite_db_conn.fetchone("SELECT * FROM conda_envs WHERE name = ?", (name,))
    assert row is not None
    return _row_to_out(row)
