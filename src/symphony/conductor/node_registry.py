from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional


class NodeAlreadyRegisteredError(Exception):
    """
    Raised when a node with a duplicate node_id attempts to register.
    """


@dataclass
class NodeStaticResources:

    cpu: Dict[str, Any] = field(default_factory=dict)
    memory: Dict[str, Any] = field(default_factory=dict)  # e.g. {"total_bytes": ...}
    storage_mounts: List[Dict[str, Any]] = field(default_factory=list)
    gpus: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class NodeDynamicResources:
    """
    Dynamic with Heartbeat.
    """

    timestamp_unix_ms: int = 0
    total_capacities_used: Dict[str, int] = field(default_factory=dict)
    cpu: Dict[str, Any] = field(default_factory=dict)
    memory: Dict[str, Any] = field(default_factory=dict)
    storage_mounts: List[Dict[str, Any]] = field(default_factory=list)
    gpus: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class NodeRecord:
    node_id: str
    groups: List[str]
    capacities_total: Dict[str, int]
    static: NodeStaticResources = field(default_factory=NodeStaticResources)
    dynamic: NodeDynamicResources = field(default_factory=NodeDynamicResources)
    last_heartbeat: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    conda_envs: List[str] = field(default_factory=list)


class NodeRegistry:
    """
    In-memory registry of connected nodes.
    """

    _instance: Optional[NodeRegistry] = None
    _init_done: bool = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if self.__class__._init_done:
            return
        self._nodes: Dict[str, NodeRecord] = {}
        self._lock = asyncio.Lock()
        self.__class__._init_done = True

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _norm_index(item: Mapping[str, Any]) -> int:
        try:
            if "index" not in item or item.get("index") is None:
                return 0
            return int(item.get("index") or 0)
        except Exception:
            return 0

    @staticmethod
    def _index_map(items: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
        out: Dict[int, Dict[str, Any]] = {}
        for it in items:
            idx = NodeRegistry._norm_index(it)
            out[idx] = dict(it)
        return out

    @staticmethod
    def _merge_gpu(
        static_gpus: List[Dict[str, Any]], dyn_gpus: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        s_by_idx = NodeRegistry._index_map(static_gpus)
        d_by_idx = NodeRegistry._index_map(dyn_gpus)

        merged: List[Dict[str, Any]] = []
        all_idxs = sorted(set(s_by_idx.keys()) | set(d_by_idx.keys()))
        for idx in all_idxs:
            s = s_by_idx.get(idx, {})
            d = d_by_idx.get(idx, {})
            m = {"index": idx, **s, **d}
            if "name" not in m or not m.get("name"):
                m["name"] = s.get("name", "")
            merged.append(m)
        return merged

    @staticmethod
    def _merge_mounts(
        static_mounts: List[Dict[str, Any]], dyn_mounts: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        s_by_mp = {
            m.get("mount_point"): dict(m) for m in static_mounts if m.get("mount_point")
        }
        d_by_mp = {
            m.get("mount_point"): dict(m) for m in dyn_mounts if m.get("mount_point")
        }

        merged: List[Dict[str, Any]] = []
        all_mps = sorted(set(s_by_mp.keys()) | set(d_by_mp.keys()))
        for mp in all_mps:
            s = s_by_mp.get(mp, {})
            d = d_by_mp.get(mp, {})
            merged.append({"mount_point": mp, **s, **d})
        return merged

    @staticmethod
    def _combined_view(rec: NodeRecord) -> Dict[str, Any]:
        return {
            "node_id": rec.node_id,
            "groups": list(rec.groups),
            "capacities_total": dict(rec.capacities_total),
            "total_capacities_used": dict(rec.dynamic.total_capacities_used),
            "last_heartbeat": rec.last_heartbeat.isoformat(),
            "dynamic_timestamp_unix_ms": rec.dynamic.timestamp_unix_ms,
            "conda_envs": list(rec.conda_envs),
            "cpu": {
                "static": dict(rec.static.cpu),
                "dynamic": dict(rec.dynamic.cpu),
            },
            "memory": {
                "static": dict(rec.static.memory),
                "dynamic": dict(rec.dynamic.memory),
            },
            "storage_mounts": NodeRegistry._merge_mounts(
                rec.static.storage_mounts, rec.dynamic.storage_mounts
            ),
            "gpus": NodeRegistry._merge_gpu(rec.static.gpus, rec.dynamic.gpus),
        }

    async def node_hello(
        self,
        *,
        node_id: str,
        groups: List[str],
        capacities_total: Mapping[str, int],
        static_cpu: Optional[Mapping[str, Any]] = None,
        static_memory: Optional[Mapping[str, Any]] = None,
        static_storage_mounts: Optional[List[Mapping[str, Any]]] = None,
        static_gpus: Optional[List[Mapping[str, Any]]] = None,
    ) -> None:
        now = self._now()
        async with self._lock:
            rec = self._nodes.get(node_id)
            if rec is not None:
                raise NodeAlreadyRegisteredError(
                    f"Node with id {node_id!r} is already registered"
                )

            rec = NodeRecord(
                node_id=node_id,
                groups=list(groups),
                capacities_total=dict(capacities_total),
                last_heartbeat=now,
            )
            self._nodes[node_id] = rec

            if static_cpu is not None:
                rec.static.cpu = dict(static_cpu)
            if static_memory is not None:
                rec.static.memory = dict(static_memory)
            if static_storage_mounts is not None:
                rec.static.storage_mounts = [dict(m) for m in static_storage_mounts]
            if static_gpus is not None:
                rec.static.gpus = [dict(g) for g in static_gpus]

            rec.last_heartbeat = now

    async def heartbeat(
        self,
        *,
        node_id: str,
        timestamp_unix_ms: int,
        total_capacities_used: Optional[Mapping[str, int]] = None,
        dyn_cpu: Optional[Mapping[str, Any]] = None,
        dyn_memory: Optional[Mapping[str, Any]] = None,
        dyn_storage_mounts: Optional[List[Mapping[str, Any]]] = None,
        dyn_gpus: Optional[List[Mapping[str, Any]]] = None,
    ) -> None:
        now = self._now()
        async with self._lock:
            rec = self._nodes.get(node_id)
            if rec is None:
                # If heartbeat arrives before hello, create record with empty static.
                rec = NodeRecord(
                    node_id=node_id,
                    groups=[],
                    capacities_total={},
                    last_heartbeat=now,
                )
                self._nodes[node_id] = rec

            rec.last_heartbeat = now
            rec.dynamic.timestamp_unix_ms = int(timestamp_unix_ms or 0)

            if total_capacities_used is not None:
                rec.dynamic.total_capacities_used = dict(total_capacities_used)
            if dyn_cpu is not None:
                rec.dynamic.cpu = dict(dyn_cpu)
            if dyn_memory is not None:
                rec.dynamic.memory = dict(dyn_memory)
            if dyn_storage_mounts is not None:
                rec.dynamic.storage_mounts = [dict(m) for m in dyn_storage_mounts]
            if dyn_gpus is not None:
                rec.dynamic.gpus = [dict(g) for g in dyn_gpus]

    async def update_conda_envs(self, *, node_id: str, env_names: List[str]) -> None:
        now = self._now()
        async with self._lock:
            rec = self._nodes.get(node_id)
            if rec is None:
                rec = NodeRecord(
                    node_id=node_id,
                    groups=[],
                    capacities_total={},
                    last_heartbeat=now,
                )
                self._nodes[node_id] = rec
            rec.last_heartbeat = now
            rec.conda_envs = list(env_names)

    async def snapshot_records(self) -> Dict[str, NodeRecord]:
        async with self._lock:
            return dict(self._nodes)

    async def combined_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        async with self._lock:
            rec = self._nodes.get(node_id)
            if rec is None:
                return None
            return self._combined_view(rec)

    async def combined_snapshot(self) -> Dict[str, Dict[str, Any]]:
        async with self._lock:
            return {nid: self._combined_view(rec) for nid, rec in self._nodes.items()}

    async def delete_node(self, node_id: str):
        async with self._lock:
            self._nodes.pop(node_id, None)
