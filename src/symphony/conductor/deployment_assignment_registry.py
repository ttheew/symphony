from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Dict, List, Optional, Set


@dataclass(frozen=True)
class DeploymentStatus:
    exec_id: str
    desired_state: str
    status: str
    pid: int
    started_at_ms: int
    restart_policy: str
    max_restarts: int
    restart_window_sec: int


@dataclass
class DeploymentInfo:
    node_id: str
    status: DeploymentStatus


class DeploymentAssignmentRegistry:
    """
    In-memory deployment <-> node assignment registry.
    """

    _instance: Optional[DeploymentAssignmentRegistry] = None
    _init_done: bool = False

    def __new__(cls) -> DeploymentAssignmentRegistry:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if self.__class__._init_done:
            return
        self.__class__._init_done = True

        self._lock = asyncio.Lock()
        self._deployments: Dict[str, DeploymentInfo] = {}
        self._node_to_deployments: Dict[str, Set[str]] = {}

    async def update(
        self,
        *,
        node_id: str,
        status: DeploymentStatus,
    ) -> None:
        """
        Update node assignment and status from data recieved from nodes
        """
        exec_id = status.exec_id

        async with self._lock:
            old_info = self._deployments.get(exec_id)
            old_node = old_info.node_id if old_info else None

            if old_node is not None and old_node != node_id:
                s = self._node_to_deployments.get(old_node)
                if s:
                    s.discard(exec_id)
                    if not s:
                        self._node_to_deployments.pop(old_node, None)

            self._deployments[exec_id] = DeploymentInfo(node_id=node_id, status=status)
            self._node_to_deployments.setdefault(node_id, set()).add(exec_id)

    async def remove_deployment(self, exec_id: str) -> None:
        async with self._lock:
            info = self._deployments.pop(exec_id, None)
            if info is None:
                return

            s = self._node_to_deployments.get(info.node_id)
            if s:
                s.discard(exec_id)
                if not s:
                    self._node_to_deployments.pop(info.node_id, None)

    async def get_node(self, exec_id: str) -> Optional[str]:
        async with self._lock:
            info = self._deployments.get(exec_id)
            return info.node_id if info else None

    async def get_deployments(self, node_id: str) -> List[str]:
        async with self._lock:
            return sorted(self._node_to_deployments.get(node_id, set()))

    async def get_status(self, exec_id: str) -> Optional[DeploymentStatus]:
        async with self._lock:
            info = self._deployments.get(exec_id)
            return info.status if info else None

    async def list_statuses(self) -> List[DeploymentStatus]:
        async with self._lock:
            return [info.status for info in self._deployments.values()]

    async def list_statuses_by_node(self, node_id: str) -> List[DeploymentStatus]:
        async with self._lock:
            ids = sorted(self._node_to_deployments.get(node_id, set()))
            out: List[DeploymentStatus] = []
            for exec_id in ids:
                info = self._deployments.get(exec_id)
                if info:
                    out.append(info.status)
            return out
