from __future__ import annotations

import json
import time
import uuid
from typing import Any, Optional

from symphony.conductor.models import (
    DeploymentCreate,
    DeploymentResponse,
    DeploymentUpdate,
)
from symphony.interface.sqlite import SQLiteAsyncDB

sqlite_db_conn = SQLiteAsyncDB()


def _now_ms() -> int:
    return int(time.time() * 1000)


def _row_to_out(row) -> DeploymentResponse:
    spec_raw = row["specification"]
    spec = json.loads(spec_raw) if isinstance(spec_raw, str) else (spec_raw or {})
    return DeploymentResponse(
        id=row["id"],
        name=row["name"],
        desired_state=row["desired_state"],
        current_state=row["current_state"] if "current_state" in row else None,
        kind=row["kind"],
        specification=spec,
        created_at_ms=row["created_at_ms"],
        updated_at_ms=row["updated_at_ms"],
    )


async def create(data: DeploymentCreate) -> DeploymentResponse:
    dep_id = uuid.uuid4().hex
    now = _now_ms()

    spec_json = json.dumps(
        data.specification, separators=(",", ":"), ensure_ascii=False
    )

    await sqlite_db_conn.execute(
        """
        INSERT INTO deployments (
            id, name, desired_state, kind,
            specification, created_at_ms, updated_at_ms
        ) VALUES (?, ?, ?, ?, json(?), ?, ?)
        """,
        (
            dep_id,
            data.name,
            data.desired_state.value,
            data.kind.value,
            spec_json,
            now,
            now,
        ),
    )

    row = await sqlite_db_conn.fetchone(
        "SELECT * FROM deployments WHERE id = ?", (dep_id,)
    )
    assert row is not None
    return _row_to_out(row)


async def get(dep_id: str) -> Optional[DeploymentResponse]:
    row = await sqlite_db_conn.fetchone(
        "SELECT * FROM deployments WHERE id = ?", (dep_id,)
    )
    return _row_to_out(row) if row else None


async def list(limit: int = 100, offset: int = 0) -> list[DeploymentResponse]:
    rows = await sqlite_db_conn.fetchall(
        """
        SELECT * FROM deployments
        ORDER BY created_at_ms DESC
        LIMIT ? OFFSET ?
        """,
        (limit, offset),
    )
    return [_row_to_out(r) for r in rows]


async def list_all() -> list[DeploymentResponse]:
    rows = await sqlite_db_conn.fetchall(
        """
        SELECT * FROM deployments
        ORDER BY created_at_ms DESC
        """
    )
    return [_row_to_out(r) for r in rows]


async def update(dep_id: str, patch: DeploymentUpdate) -> Optional[DeploymentResponse]:
    # Build dynamic SET clause
    sets: list[str] = []
    params: list[Any] = []

    if patch.name is not None:
        sets.append("name = ?")
        params.append(patch.name)
    if patch.desired_state is not None:
        sets.append("desired_state = ?")
        params.append(patch.desired_state.value)
    if patch.specification is not None:
        spec_json = json.dumps(
            patch.specification, separators=(",", ":"), ensure_ascii=False
        )
        sets.append("specification = json(?)")
        params.append(spec_json)

    if not sets:
        return await get(dep_id)

    sets.append("updated_at_ms = ?")
    params.append(_now_ms())

    params.append(dep_id)

    await sqlite_db_conn.execute(
        f"UPDATE deployments SET {', '.join(sets)} WHERE id = ?",
        tuple(params),
    )

    return await get(dep_id)


async def delete(dep_id: str) -> bool:
    existing = await get(dep_id)
    if not existing:
        return False

    await sqlite_db_conn.execute("DELETE FROM deployments WHERE id = ?", (dep_id,))
    return True
