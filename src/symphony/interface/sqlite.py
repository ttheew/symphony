from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional, Sequence, TypeVar

import aiosqlite

T = TypeVar("T")

CREATE_DEPLOYMENTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS deployments (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,

    desired_state   TEXT NOT NULL,
    kind            TEXT NOT NULL,

    specification   JSON NOT NULL
        CHECK (json_valid(specification)),

    created_at_ms   INTEGER NOT NULL,
    updated_at_ms   INTEGER NOT NULL
);
"""

CREATE_DEPLOYMENTS_INDEXES_SQL = [
    """
    CREATE INDEX IF NOT EXISTS idx_deployments_name
        ON deployments (name);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_deployments_state
        ON deployments (desired_state);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_deployments_kind
        ON deployments (kind);
    """,
]

CREATE_CONDA_ENVS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS conda_envs (
    name            TEXT PRIMARY KEY,
    python_version  TEXT NOT NULL,
    packages        JSON NOT NULL
        CHECK (json_valid(packages)),
    custom_script   TEXT NOT NULL DEFAULT '',
    created_at_ms   INTEGER NOT NULL,
    updated_at_ms   INTEGER NOT NULL
);
"""

CREATE_CONDA_ENVS_INDEXES_SQL = [
    """
    CREATE INDEX IF NOT EXISTS idx_conda_envs_name
        ON conda_envs (name);
    """,
]


@dataclass(frozen=True)
class DBConfig:
    path: str
    timeout_sec: float = 10.0
    busy_timeout_ms: int = 8000
    pragmas: tuple[tuple[str, str], ...] = (
        ("journal_mode", "WAL"),
        ("synchronous", "NORMAL"),
        ("foreign_keys", "ON"),
        ("temp_store", "MEMORY"),
        ("cache_size", "-20000"),
        ("busy_timeout", "8000"),
    )


class SQLiteAsyncDB:
    _instance: Optional[SQLiteAsyncDB] = None
    _init_done: bool = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if self.__class__._init_done:
            return
        self._cfg = DBConfig("storage/app.db")
        self._conn: Optional[aiosqlite.Connection] = None
        self._op_lock = asyncio.Lock()
        self._conn_lock = asyncio.Lock()
        self._closed = False

        self.__class__._init_done = True

    async def create_tables(self):
        await self._conn.execute(CREATE_DEPLOYMENTS_TABLE_SQL)

        for sql in CREATE_DEPLOYMENTS_INDEXES_SQL:
            await self._conn.execute(sql)

        await self._conn.execute(CREATE_CONDA_ENVS_TABLE_SQL)
        await self._ensure_conda_envs_columns()

        for sql in CREATE_CONDA_ENVS_INDEXES_SQL:
            await self._conn.execute(sql)

    async def _ensure_conda_envs_columns(self) -> None:
        async with self._conn.execute("PRAGMA table_info(conda_envs)") as cur:
            rows = await cur.fetchall()
        existing = {str(row[1]) for row in rows}
        if "custom_script" not in existing:
            await self._conn.execute(
                "ALTER TABLE conda_envs ADD COLUMN custom_script TEXT NOT NULL DEFAULT ''"
            )

    async def close(self) -> None:
        self._closed = True
        async with self._conn_lock:
            if self._conn is not None:
                try:
                    await self._conn.close()
                finally:
                    self._conn = None

    async def connect(self) -> None:
        async with self._conn_lock:
            if self._conn is not None:
                return

            self._conn = await aiosqlite.connect(
                self._cfg.path,
                timeout=self._cfg.timeout_sec,
            )
            self._conn.row_factory = aiosqlite.Row

            for key, val in self._cfg.pragmas:
                await self._conn.execute(f"PRAGMA {key}={val};")

            await self._conn.execute(
                f"PRAGMA busy_timeout={self._cfg.busy_timeout_ms};"
            )
            await self._conn.commit()

    async def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        async with self._op_lock:
            await self._conn.execute(sql, params)
            await self._conn.commit()

    async def fetchone(
        self, sql: str, params: Sequence[Any] = ()
    ) -> Optional[aiosqlite.Row]:
        async with self._op_lock:
            async with self._conn.execute(sql, params) as cur:
                return await cur.fetchone()

    async def fetchall(
        self, sql: str, params: Sequence[Any] = ()
    ) -> list[aiosqlite.Row]:
        async with self._op_lock:
            async with self._conn.execute(sql, params) as cur:
                rows = await cur.fetchall()
                return list(rows)

    async def transaction(
        self, work: Callable[[aiosqlite.Connection], Awaitable[T]]
    ) -> T:
        async with self._op_lock:
            try:
                await self._conn.execute("BEGIN;")
                res = await work(self._conn)
                await self._conn.execute("COMMIT;")
                return res
            except Exception:
                await self._conn.execute("ROLLBACK;")
                raise
