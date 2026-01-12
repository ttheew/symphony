import asyncio
import random
from datetime import datetime, timedelta, timezone

import grpc
from loguru import logger

from symphony.conductor.deployment_assignment_registry import (
    DeploymentAssignmentRegistry,
)
from symphony.conductor.deployment_store import list_all
from symphony.conductor.node_registry import NodeRecord, NodeRegistry
from symphony.conductor.service import ConductorService
from symphony.v1 import protocol_pb2


class NodeScheduler:
    """
    Periodically sweeps the in-memory NodeRegistry and removes
    nodes that have not sent a heartbeat for a configured TTL.
    Assigns Deployments to suitable nodes.
    """

    def __init__(
        self,
        ttl_seconds: float = 60.0,
        check_interval_seconds: float = 5.0,
    ) -> None:
        self._registry = NodeRegistry()
        self._deploy_ass_registry = DeploymentAssignmentRegistry()
        self._svc = ConductorService()
        self._ttl = timedelta(seconds=float(ttl_seconds))
        self._check_interval = float(check_interval_seconds)
        self._stopped = asyncio.Event()

    async def stop(self) -> None:
        self._stopped.set()

    async def run(self) -> None:
        logger.info(
            "Starting NodeHealthScheduler ttl={} interval={}",
            self._ttl.total_seconds(),
            self._check_interval,
        )

        try:
            while not self._stopped.is_set():
                try:
                    await self._sweep_once()
                    await self.assign_deployment()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Error while scheduling")

                try:
                    await asyncio.wait_for(
                        self._stopped.wait(), timeout=self._check_interval
                    )
                except asyncio.TimeoutError:
                    continue
        finally:
            logger.info("NodeHealthScheduler stopped")

    async def assign_deployment(self) -> None:
        all_deployments = await list_all()
        for deployment in all_deployments:
            deployment_id = deployment.id
            node_id = await self._deploy_ass_registry.get_node(deployment_id)
            if node_id:
                continue
            snapshot_nodes = await self._registry.snapshot_records()
            if not snapshot_nodes:
                logger.warning(f"No nodes found for deployments")
                return
            spec = (deployment.specification or {}).get("spec") or {}
            capacity_request = spec.get("capacity_requests") or {}

            if not capacity_request:
                chosen_node_id = random.choice(list(snapshot_nodes.keys()))
                logger.info(
                    "Sending Deployment Request to {} deployment_id {}",
                    chosen_node_id,
                    deployment_id,
                )
                await self.send_message(chosen_node_id, deployment.model_dump_json())
                continue

            eligible_nodes: list[str] = []

            for nid, rec in snapshot_nodes.items():
                capacity_total = rec.capacities_total or {}
                used = getattr(rec.dynamic, "total_capacities_used", None) or {}

                ok = True
                for cap_id, req_amount in capacity_request.items():
                    total = int(capacity_total.get(cap_id, 0))
                    used_amt = int(used.get(cap_id, 0))
                    available = total - used_amt

                    if available < int(req_amount):
                        ok = False
                        break

                if ok:
                    eligible_nodes.append(nid)

            if not eligible_nodes:
                logger.warning(
                    "Cannot assign node for deployment {} with capacity request {}",
                    deployment_id,
                    capacity_request,
                )
                continue

            chosen_node_id = random.choice(eligible_nodes)
            logger.info(
                "Sending Deployment Request to {} deployment_id {} (req={})",
                chosen_node_id,
                deployment_id,
                capacity_request,
            )
            await self.send_message(chosen_node_id, deployment.model_dump_json())

    async def send_message(self, node_id: str, message: str) -> bool:
        if self._svc is None:
            logger.warning(
                "Cannot send message to node id={}: no ConductorService instance",
                node_id,
            )
            return False

        await self._svc.send_message(
            node_id,
            protocol_pb2.ConductorToNode(
                deployment_req=protocol_pb2.DeploymentReq(specification=message)
            ),
        )
        return True

    async def _sweep_once(self) -> None:
        """
        Remove nodes whose last_heartbeat is older than ttl.
        """
        now = datetime.now(timezone.utc)
        snapshot_nodes = await self._registry.snapshot_records()

        for node_id, rec in snapshot_nodes.items():
            if self._is_stale(rec, now):
                logger.warning(
                    "Removing stale node id={} last_heartbeat={}",
                    node_id,
                    rec.last_heartbeat.isoformat(),
                )

                disconnected = False
                if self._svc is not None:
                    try:
                        disconnected = await self._svc.disconnect_node(
                            node_id,
                            code=grpc.StatusCode.UNAVAILABLE,
                            reason="Node heartbeat stale; closing connection",
                        )
                    except Exception:
                        logger.exception(
                            "Error while aborting gRPC stream for stale node id={}",
                            node_id,
                        )

                if not disconnected:
                    await self._registry.delete_node(node_id)

    def _is_stale(self, rec: NodeRecord, now: datetime) -> bool:
        return now - rec.last_heartbeat > self._ttl
