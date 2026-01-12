from __future__ import annotations

import asyncio
from typing import AsyncIterator, Dict, Optional

import grpc
from loguru import logger

from symphony.conductor.deployment_assignment_registry import (
    DeploymentAssignmentRegistry,
)
from symphony.conductor.node_registry import NodeAlreadyRegisteredError, NodeRegistry
from symphony.v1 import protocol_pb2, protocol_pb2_grpc


class ConductorService(protocol_pb2_grpc.ConductorServiceServicer):
    _instance: Optional[ConductorService] = None
    _init_done: bool = False

    def __new__(cls) -> ConductorService:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if self.__class__._init_done:
            return
        self._registry = NodeRegistry()
        self._streams_lock = asyncio.Lock()
        self._streams: Dict[str, grpc.aio.ServicerContext] = {}
        self._out_msg_queue: Dict[str, asyncio.Queue[str]] = {}
        self._deploy_ass_registry = DeploymentAssignmentRegistry()

    async def Connect(
        self,
        request_iterator: AsyncIterator[protocol_pb2.NodeToConductor],
        context: grpc.aio.ServicerContext,
    ) -> AsyncIterator[protocol_pb2.ConductorToNode]:
        """
        Handle a bidirectional stream from nodes.
        """
        node_id: str | None = None
        consumer_task = None

        async def consumer(c_node_id):
            self._out_msg_queue[c_node_id] = asyncio.Queue()
            try:
                while True:
                    msg = await self._out_msg_queue[c_node_id].get()

                    await context.write(msg)
            except asyncio.CancelledError:
                pass

        try:
            async for msg in request_iterator:
                kind = msg.WhichOneof("msg")

                if kind == "hello":
                    hello = msg.hello
                    node_id = hello.node_id
                    consumer_task = asyncio.create_task(consumer(node_id))
                    try:
                        await self._registry.node_hello(
                            node_id=hello.node_id,
                            groups=list(hello.groups),
                            capacities_total=dict(hello.capacities_total),
                            static_cpu={
                                "logical_cores": hello.cpu.logical_cores,
                                "max_millicores_total": hello.cpu.max_millicores_total,
                            },
                            static_memory={"total_bytes": hello.memory.total_bytes},
                            static_storage_mounts=[
                                {
                                    "mount_point": m.mount_point,
                                    "fs_type": m.fs_type,
                                    "total_bytes": m.total_bytes,
                                }
                                for m in hello.storage_mounts
                            ],
                            static_gpus=[
                                {
                                    "index": g.index,
                                    "name": g.name,
                                    "mem_total_bytes": g.mem_total_bytes,
                                }
                                for g in hello.gpus
                            ],
                        )
                    except NodeAlreadyRegisteredError:
                        logger.warning(
                            "Rejecting NodeHello for already-registered node id=%s",
                            hello.node_id,
                        )
                        await context.abort(
                            grpc.StatusCode.ALREADY_EXISTS,
                            f"Node with id {hello.node_id!r} is already registered",
                        )

                    await self._register_stream(node_id, context)

                    logger.info(
                        "Node registered id={} groups={} capacities={}",
                        hello.node_id,
                        list(hello.groups),
                        dict(hello.capacities_total),
                    )
                    yield protocol_pb2.ConductorToNode(
                        ack=protocol_pb2.Ack(message=f"hello {hello.node_id}")
                    )

                elif kind == "heartbeat":
                    hb = msg.heartbeat
                    node_id = node_id or hb.node_id
                    await self._registry.heartbeat(
                        node_id=hb.node_id,
                        timestamp_unix_ms=hb.timestamp_unix_ms,
                        total_capacities_used=dict(hb.total_capacities_used),
                        dyn_cpu={
                            "total_percent": hb.cpu.total_percent,
                            "per_core": [
                                {"core_id": c.core_id, "used_percent": c.used_percent}
                                for c in hb.cpu.per_core
                            ],
                        },
                        dyn_memory={
                            "used_bytes": hb.memory.used_bytes,
                            "available_bytes": hb.memory.available_bytes,
                            "used_percent": hb.memory.used_percent,
                            "free_bytes": hb.memory.free_bytes,
                            "buffers_bytes": hb.memory.buffers_bytes,
                            "cached_bytes": hb.memory.cached_bytes,
                        },
                        dyn_storage_mounts=[
                            {
                                "mount_point": m.mount_point,
                                "used_bytes": m.used_bytes,
                                "available_bytes": m.available_bytes,
                                "used_percent": m.used_percent,
                            }
                            for m in hb.storage_mounts
                        ],
                        dyn_gpus=[
                            {
                                "index": g.index,
                                "util_percent": g.util_percent,
                                "mem_util_percent": g.mem_util_percent,
                                "mem_used_bytes": g.mem_used_bytes,
                                "mem_free_bytes": g.mem_free_bytes,
                                "temperature_c": g.temperature_c,
                                "power_w": g.power_w,
                            }
                            for g in hb.gpus
                        ],
                    )
                    await self._registry.combined_node(node_id)
                    logger.debug("Heartbeat from node id={}", hb.node_id)
                elif kind == "deployment_status_list":
                    for deployment_status in msg.deployment_status_list.deployments:
                        await self._deploy_ass_registry.update(
                            node_id=node_id, status=deployment_status
                        )

        finally:
            if consumer_task:
                consumer_task.cancel()
                try:
                    await consumer_task
                except asyncio.CancelledError:
                    pass
            if node_id is not None:
                await self._unregister_stream(node_id)
                logger.info("Connection from node id={} closed and removed", node_id)
                if node_id in self._out_msg_queue:
                    self._out_msg_queue.pop(node_id)
                await self._registry.delete_node(node_id)
                deployments = await self._deploy_ass_registry.get_deployments(node_id)
                for deployment in deployments:
                    await self._deploy_ass_registry.remove_deployment(deployment)
            else:
                logger.info("Connection from node with unknown id closed")

    async def _register_stream(
        self, node_id: str, context: grpc.aio.ServicerContext
    ) -> None:
        async with self._streams_lock:
            self._streams[node_id] = context

    async def _unregister_stream(self, node_id: str) -> None:
        async with self._streams_lock:
            self._streams.pop(node_id, None)

    async def send_message(self, node_id: str, message: str) -> None:
        if not node_id in self._out_msg_queue:
            logger.warning(f"Adding message to {node_id} que failed")
        else:
            await self._out_msg_queue[node_id].put(message)

    async def send_deployment_change(
        self, node_id, deployment_id, kind: str, change: str
    ):
        if kind == "desired_state":
            await self.send_message(
                node_id,
                protocol_pb2.ConductorToNode(
                    deployment_update=protocol_pb2.DeploymentUpdate(
                        status=change, deployment_id=deployment_id
                    )
                ),
            )

    async def disconnect_node(
        self,
        node_id: str,
        *,
        code: grpc.StatusCode = grpc.StatusCode.UNAVAILABLE,
        reason: str = "Node marked unhealthy; closing connection",
    ) -> bool:
        async with self._streams_lock:
            ctx: Optional[grpc.aio.ServicerContext] = self._streams.get(node_id)

        if ctx is None:
            return False

        try:
            await ctx.abort(code, reason)
            return True
        except Exception as exc:
            logger.debug(
                "Stream for node id=%s already aborted or closed: %s", node_id, exc
            )
            return True
