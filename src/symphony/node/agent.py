import asyncio
import json
from datetime import datetime, timezone
from typing import AsyncIterator

from loguru import logger

from symphony.config import NodeConfig
from symphony.node.conda_env import CondaEnvManager
from symphony.node.runner_exec import RunnerExec
from symphony.transport.grpc_client import create_channel
from symphony.util.backoff import backoff
from symphony.util.resource_monitoring.monitor import Monitor
from symphony.v1 import protocol_pb2, protocol_pb2_grpc


class NodeAgent:
    """
    Node-side client that maintains a persistent streaming
    connection to the Conductor with automatic reconnect.
    """

    def __init__(self, cfg: NodeConfig) -> None:
        self._cfg = cfg
        self._stopped = asyncio.Event()
        self.r_monitor = Monitor(
            mount_points=["/"], sample_interval=1.0, space_interval=10.0
        )
        self.total_capacities_used = {}
        self.r_monitor.start()
        self.runner_exec = RunnerExec()
        self.cap_usage_lock = asyncio.Lock()
        self._log_subscriptions: dict[str, dict] = {}
        self._log_subscriptions_lock = asyncio.Lock()
        self._conda_env_manager = CondaEnvManager()
        self._conda_env_names: list[str] = []
        self._extra_outgoing: asyncio.Queue[protocol_pb2.NodeToConductor] = (
            asyncio.Queue()
        )

    async def _build_heartbeat(self, snap: dict) -> protocol_pb2.Heartbeat:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        hb = protocol_pb2.Heartbeat(
            node_id=self._cfg.node_id,
            timestamp_unix_ms=now_ms,
        )

        cpu = snap.get("cpu") or {}
        hb.cpu.total_percent = float(cpu.get("total_percent") or 0.0)

        per_core = cpu.get("per_core_percent") or {}
        for k, pct in per_core.items():
            try:
                core_id = int(str(k).replace("cpu", ""))
            except Exception:
                continue
            core_msg = hb.cpu.per_core.add()
            core_msg.core_id = core_id
            core_msg.used_percent = float(pct or 0.0)

        ram = snap.get("ram") or {}
        hb.memory.used_bytes = int(ram.get("used_bytes") or 0)
        hb.memory.available_bytes = int(ram.get("available_bytes") or 0)

        if "used_percent" in ram:
            hb.memory.used_percent = float(ram.get("used_percent") or 0.0)
        if "free_bytes" in ram:
            hb.memory.free_bytes = int(ram.get("free_bytes") or 0)
        if "buffers_bytes" in ram:
            hb.memory.buffers_bytes = int(ram.get("buffers_bytes") or 0)
        if "cached_bytes" in ram:
            hb.memory.cached_bytes = int(ram.get("cached_bytes") or 0)

        ds = snap.get("disk_space") or {}
        mounts = ds.get("mounts") or []
        for m in mounts:
            mp = m.get("path")
            if not mp:
                continue
            sm = hb.storage_mounts.add()
            sm.mount_point = str(mp)
            sm.used_bytes = int(m.get("used_bytes") or 0)
            sm.available_bytes = int(m.get("available_bytes") or 0)
            # Optional (if in proto)
            if "used_percent" in m:
                sm.used_percent = float(m.get("used_percent") or 0.0)

        gpus = snap.get("gpus") or []
        for g in gpus:
            gm = hb.gpus.add()
            gm.index = int(g.get("index") or 0)
            gm.util_percent = float(g.get("util_percent") or 0.0)
            gm.mem_util_percent = float(g.get("mem_util_percent") or 0.0)
            gm.mem_used_bytes = int(g.get("mem_used_bytes") or 0)
            gm.mem_free_bytes = int(g.get("mem_free_bytes") or 0)

            temp = g.get("temperature_c")
            if temp is not None:
                gm.temperature_c = int(temp)
            power = g.get("power_w")
            if power is not None:
                gm.power_w = float(power)
        async with self.cap_usage_lock:
            hb.total_capacities_used.update(self.total_capacities_used)
        return hb

    def _build_node_hello_from_snapshot(self, snap: dict) -> protocol_pb2.NodeHello:
        hello = protocol_pb2.NodeHello(
            node_id=self._cfg.node_id,
            groups=list(self._cfg.groups),
            capacities_total=self._cfg.capacities_total,
        )
        per_core = snap.get("cpu", {}).get("per_core_percent", {})
        logical_cores = len(per_core) or 1

        hello.cpu.logical_cores = logical_cores
        hello.cpu.max_millicores_total = logical_cores * 1000

        ram = snap.get("ram", {})
        hello.memory.total_bytes = int(ram.get("total_bytes") or 0)

        for m in snap.get("disk_space", {}).get("mounts", []):
            sm = hello.storage_mounts.add()
            sm.mount_point = m.get("path", "")
            sm.total_bytes = int(m.get("total_bytes") or 0)

        for g in snap.get("gpus", []):
            gm = hello.gpus.add()
            gm.index = 0
            gm.name = g.get("name", "")
            gm.mem_total_bytes = int(g.get("mem_total_bytes") or 0)

        return hello

    async def start(self) -> None:
        """
        Run until stopped, reconnecting on errors with backoff.
        """
        backoff_iter = backoff()
        while not self._stopped.is_set():
            try:
                await self._connect_once()
                backoff_iter = backoff()
            except asyncio.CancelledError:
                raise
            except Exception:
                delay = next(backoff_iter)
                logger.exception(
                    "Connection to conductor failed, retrying in %.1fs", delay
                )
                try:
                    await asyncio.wait_for(self._stopped.wait(), timeout=5)
                except asyncio.TimeoutError:
                    continue

    async def stop(self) -> None:
        self._stopped.set()

    async def _build_deployment_status(self):
        deployment_ids = await self.runner_exec.list_ids()
        deployment_status_list = []
        total_capacities_used = {}
        allowed_fields = set(protocol_pb2.DeploymentStatus.DESCRIPTOR.fields_by_name)
        for d_id in deployment_ids:
            deployment_status = await self.runner_exec.status(d_id)
            capacity_req = deployment_status["capacity_requests"]
            deployment_status.pop("capacity_requests")
            deployment_status = {
                key: value
                for key, value in deployment_status.items()
                if key in allowed_fields
            }
            deployment_status_list.append(
                protocol_pb2.DeploymentStatus(**deployment_status)
            )
            for cap_key in capacity_req:
                total_capacities_used[cap_key] = total_capacities_used.get(
                    cap_key, 0
                ) + capacity_req.get(cap_key, 0)
        async with self.cap_usage_lock:
            self.total_capacities_used = dict(total_capacities_used)

        return protocol_pb2.DeploymentStatusList(deployments=deployment_status_list)

    async def _refresh_conda_envs(self) -> None:
        try:
            self._conda_env_names = await self._conda_env_manager.list_env_names()
        except Exception as exc:
            logger.warning("Failed to list conda envs: {}", exc)

    def _build_conda_env_report(self) -> protocol_pb2.CondaEnvReport:
        return protocol_pb2.CondaEnvReport(env_names=list(self._conda_env_names))

    async def _enqueue_conda_report(self) -> None:
        await self._extra_outgoing.put(
            protocol_pb2.NodeToConductor(
                conda_env_report=self._build_conda_env_report()
            )
        )

    async def _build_deployment_log_messages(self) -> list[protocol_pb2.NodeToConductor]:
        async with self._log_subscriptions_lock:
            items = list(self._log_subscriptions.items())

        messages: list[protocol_pb2.NodeToConductor] = []
        for deployment_id, sub in items:
            try:
                logs = await self.runner_exec.logs(
                    deployment_id,
                    since_ms=sub.get("since_ms"),
                    tail=sub.get("tail"),
                    streams=sub.get("streams"),
                )
            except KeyError:
                async with self._log_subscriptions_lock:
                    self._log_subscriptions.pop(deployment_id, None)
                continue
            except Exception as e:
                logger.debug(
                    "Skipping log stream deployment_id={} reason={}",
                    deployment_id,
                    e,
                )
                continue

            entries = []
            for ts_ms, stream, line in logs:
                entries.append(
                    protocol_pb2.LogEntry(
                        timestamp_unix_ms=int(ts_ms),
                        stream=str(stream),
                        line=str(line),
                    )
                )

            if not entries:
                continue

            async with self._log_subscriptions_lock:
                current = self._log_subscriptions.get(deployment_id)
                if current is None:
                    continue
                current["since_ms"] = int(entries[-1].timestamp_unix_ms) + 1
                current["tail"] = None

            messages.append(
                protocol_pb2.NodeToConductor(
                    deployment_logs=protocol_pb2.DeploymentLogs(
                        deployment_id=deployment_id,
                        entries=entries,
                    )
                )
            )

        return messages

    async def _outgoing(
        self,
    ) -> AsyncIterator[protocol_pb2.NodeToConductor]:
        """
        Yield hello once, then periodic heartbeats.
        """
        try:
            max_wait = 10.0
            poll_interval = 0.1
            waited = 0.0
            snap = self.r_monitor.snapshot()
            while (
                snap.get("timestamp_unix") is None
                and not self._stopped.is_set()
                and waited < max_wait
            ):
                await asyncio.sleep(poll_interval)
                waited += poll_interval
                snap = self.r_monitor.snapshot()

            if self._stopped.is_set():
                return

            hello = self._build_node_hello_from_snapshot(snap)
            yield protocol_pb2.NodeToConductor(hello=hello)
            await self._refresh_conda_envs()
            await self._enqueue_conda_report()

            while not self._stopped.is_set():
                while True:
                    try:
                        msg = self._extra_outgoing.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    else:
                        yield msg
                snap = self.r_monitor.snapshot()
                try:
                    hb = await self._build_heartbeat(snap)
                    yield protocol_pb2.NodeToConductor(heartbeat=hb)
                except Exception as e:
                    logger.exception(f"Failed to get heartbeat data {e}")
                try:
                    d_stat = await self._build_deployment_status()
                    yield protocol_pb2.NodeToConductor(deployment_status_list=d_stat)
                except Exception as e:
                    logger.exception(f"Failed to get deployment status data {e}")

                try:
                    log_messages = await self._build_deployment_log_messages()
                    for message in log_messages:
                        yield message
                except Exception as e:
                    logger.exception(f"Failed to stream deployment logs {e}")

                try:
                    await asyncio.sleep(self._cfg.heartbeat_sec)
                except asyncio.CancelledError:
                    break
        except asyncio.CancelledError:
            return

    async def _connect_once(self) -> None:
        """
        Open a gRPC stream, send hello + heartbeats, and
        log any acknowledgements from the conductor.
        """
        logger.info("Connecting to conductor at {}", self._cfg.conductor_addr)
        channel = create_channel(self._cfg.conductor_addr, self._cfg.tls)
        stub = protocol_pb2_grpc.ConductorServiceStub(channel)

        try:
            call = stub.Connect(self._outgoing())
            async for msg in call:
                kind = msg.WhichOneof("msg")
                if kind == "ack":
                    logger.info("Conductor ack: {}", msg.ack.message)
                elif kind == "deployment_update":
                    d_update_kind = msg.deployment_update.WhichOneof("msg")
                    deployment_id = msg.deployment_update.deployment_id
                    logger.info(f"deployment update: {d_update_kind}")
                    if d_update_kind == "status":
                        current_status = await self.runner_exec.status(deployment_id)
                        if msg.deployment_update.status != current_status["status"]:
                            if msg.deployment_update.status == "STOPPED":
                                await self.runner_exec.stop(deployment_id)
                            elif msg.deployment_update.status == "RUNNING":
                                await self.runner_exec.start(deployment_id)
                elif kind == "deployment_req":
                    logger.info("deployment ack: {}", msg.deployment_req.specification)
                    deployment_dict = json.loads(msg.deployment_req.specification)
                    deployment_id = deployment_dict["id"]
                    deployment_status = await self.runner_exec.status(deployment_id)
                    deployment_spec = deployment_dict["specification"]["spec"]
                    if deployment_status:
                        logger.info("Updating deployment spec on node id={}", deployment_id)
                    else:
                        logger.info("Creating deployment on node id={}", deployment_id)
                    await self.runner_exec.add_exec(
                        deployment_id,
                        deployment_spec,
                    )
                    if deployment_dict["desired_state"] == "RUNNING":
                        await self.runner_exec.start(deployment_id)
                    else:
                        await self.runner_exec.stop(deployment_id)
                elif kind == "deployment_logs_request":
                    req = msg.deployment_logs_request
                    if req.enable:
                        async with self._log_subscriptions_lock:
                            self._log_subscriptions[req.deployment_id] = {
                                "since_ms": int(req.since_ms or 0) or None,
                                "tail": int(req.tail or 200),
                                "streams": list(req.streams) if req.streams else None,
                            }
                    else:
                        async with self._log_subscriptions_lock:
                            self._log_subscriptions.pop(req.deployment_id, None)
                elif kind == "conda_env_ensure":
                    try:
                        self._conda_env_names = await self._conda_env_manager.ensure_envs(
                            msg.conda_env_ensure.envs
                        )
                        await self._enqueue_conda_report()
                    except Exception as exc:
                        logger.warning("Failed to ensure conda envs: {}", exc)
        finally:
            await channel.close()
