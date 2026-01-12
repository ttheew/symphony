import asyncio
import os
import signal
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Tuple

from loguru import logger


@dataclass
class RestartEvent:
    ts_ms: int
    reason: str
    exit_code: Optional[int] = None


@dataclass
class ExecRuntime:
    exec_id: str
    spec: Dict[str, Any]
    capacity_requests: Dict[str, int]

    process: Optional[asyncio.subprocess.Process] = None
    stdout_task: Optional[asyncio.Task] = None
    stderr_task: Optional[asyncio.Task] = None
    waiter_task: Optional[asyncio.Task] = None

    desired_state: str = "STOPPED"  # "RUNNING" | "STOPPED"
    status: str = (
        "STARTING"  # "STARTING" | "RUNNING" | "STOPPING" | "STARTING" | "CRASHED" | "EXITED"
    )
    last_exit_code: Optional[int] = None
    started_at_ms: Optional[int] = None
    stopped_at_ms: Optional[int] = None

    log_limit_lines: int = 5000
    _logs: List[Tuple[int, str, str]] = field(default_factory=list)
    _log_lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    restart_policy: str = "on-failure"  # "never" | "always" | "on-failure"
    max_restarts: int = 10
    restart_window_sec: int = 300
    _restart_times: List[float] = field(default_factory=list)
    restart_history: List[RestartEvent] = field(default_factory=list)

    _state_lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def _now_ms(self) -> int:
        return int(time.time() * 1000)

    async def append_log(self, stream: str, line: str) -> None:
        ts = self._now_ms()
        async with self._log_lock:
            self._logs.append((ts, stream, line))
            if len(self._logs) > self.log_limit_lines:
                over = len(self._logs) - self.log_limit_lines
                del self._logs[:over]

    async def get_logs(
        self,
        *,
        since_ms: Optional[int] = None,
        tail: Optional[int] = 200,
        streams: Optional[List[str]] = None,  # ["stdout","stderr"]
    ) -> List[Tuple[int, str, str]]:
        async with self._log_lock:
            items = self._logs
            if streams:
                s = set(streams)
                items = [x for x in items if x[1] in s]
            if since_ms is not None:
                items = [x for x in items if x[0] >= since_ms]
            if tail is not None and tail >= 0:
                items = items[-tail:]
            return list(items)


class RunnerExec:
    """
    Async singleton runner for exec-type deployments.
    """

    _instance: Optional["RunnerExec"] = None

    def __new__(cls) -> "RunnerExec":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init_done = False  # type: ignore[attr-defined]
        return cls._instance

    def __init__(self) -> None:
        if getattr(self, "_init_done", False):
            return
        self._init_done = True
        self._runtimes: Dict[str, ExecRuntime] = {}
        self._lock = asyncio.Lock()

    async def add_exec(self, exec_id: str, specification: Mapping[str, Any]) -> None:
        spec = dict(specification)

        cmd = spec.get("cmd")
        if (
            not isinstance(cmd, list)
            or not cmd
            or not all(isinstance(x, str) for x in cmd)
        ):
            raise ValueError("spec['cmd'] must be a non-empty list[str]")

        logger.debug(
            "Add exec process exec_id={} cmd={} cwd={} env_keys={} restart_policy={} max_restarts={} restart_window_sec={} log_limit_lines={}",
            exec_id,
            cmd,
            spec.get("cwd"),
            list((spec.get("env") or {}).keys()),
            spec.get("restart_policy"),
            spec.get("max_restarts"),
            spec.get("restart_window_sec"),
            spec.get("log_limit_lines"),
        )

        async with self._lock:
            rt = self._runtimes.get(exec_id)
            if rt is None:
                logger.info("RunnerExec creating new runtime exec_id={}", exec_id)
                capacity_requests = (
                    spec.get("capacity_requests") if "capacity_requests" in spec else {}
                )
                rt = ExecRuntime(
                    exec_id=exec_id, spec=spec, capacity_requests=capacity_requests
                )
                self._apply_spec(rt, spec)
                self._runtimes[exec_id] = rt
            else:
                # Update spec and runtime parameters
                logger.info("RunnerExec updating existing runtime exec_id={}", exec_id)
                rt.spec = spec
                self._apply_spec(rt, spec)

    async def remove(self, exec_id: str, *, stop: bool = True) -> None:
        async with self._lock:
            rt = self._runtimes.get(exec_id)
        if rt is None:
            logger.warning("Runner Exec unknown exec_id={}", exec_id)
            return
        if stop:
            await self.stop(exec_id)
        async with self._lock:
            self._runtimes.pop(exec_id, None)
        logger.info("Remove completed exec_id={}", exec_id)

    async def list_ids(self) -> List[str]:
        async with self._lock:
            return list(self._runtimes.keys())

    async def get_spec(self, exec_id: str) -> Dict[str, Any]:
        rt = await self._get_runtime(exec_id)
        return dict(rt.spec)

    async def start(self, exec_id: str) -> None:
        rt = await self._get_runtime(exec_id)
        async with rt._state_lock:
            rt.desired_state = "RUNNING"
            if rt.process and rt.status in ("STARTING", "RUNNING"):
                logger.debug(
                    "Already running exec_id={} status={}",
                    exec_id,
                    rt.status,
                )
                return
            await self._spawn(rt)
        logger.info("Start completed exec_id={}", exec_id)

    async def stop(self, exec_id: str) -> None:
        rt = await self._get_runtime(exec_id)
        async with rt._state_lock:
            rt.desired_state = "STOPPED"
            await self._stop(rt, reason="stop requested")
        logger.info("Stop completed exec_id={}", exec_id)

    async def restart(self, exec_id: str, *, reason: str = "manual restart") -> None:
        rt = await self._get_runtime(exec_id)
        async with rt._state_lock:
            rt.desired_state = "RUNNING"
            await self._stop(rt, reason=reason)
            await self._spawn(rt)
        logger.info("Restart completed exec_id={}", exec_id)

    async def status(self, exec_id: str) -> Dict[str, Any]:
        try:
            rt = await self._get_runtime(exec_id)
        except Exception:
            return None
        async with rt._state_lock:
            pid = rt.process.pid if rt.process else None
            status = {
                "exec_id": rt.exec_id,
                "desired_state": rt.desired_state,
                "status": rt.status,
                "pid": pid,
                "started_at_ms": rt.started_at_ms,
                "stopped_at_ms": rt.stopped_at_ms,
                "last_exit_code": rt.last_exit_code,
                "restart_policy": rt.restart_policy,
                "max_restarts": rt.max_restarts,
                "restart_window_sec": rt.restart_window_sec,
                "restart_count_window": self._restart_count_in_window(rt),
                "capacity_requests": rt.capacity_requests,
            }
            return status

    async def logs(
        self,
        exec_id: str,
        *,
        since_ms: Optional[int] = None,
        tail: Optional[int] = 200,
        streams: Optional[List[str]] = None,
    ) -> List[Tuple[int, str, str]]:
        rt = await self._get_runtime(exec_id)
        return await rt.get_logs(since_ms=since_ms, tail=tail, streams=streams)

    async def get_restart_history(
        self, exec_id: str, *, tail: int = 50
    ) -> List[Dict[str, Any]]:
        rt = await self._get_runtime(exec_id)
        async with rt._state_lock:
            items = (
                rt.restart_history[-tail:] if tail >= 0 else list(rt.restart_history)
            )
            return [
                {"ts_ms": e.ts_ms, "reason": e.reason, "exit_code": e.exit_code}
                for e in items
            ]

    async def _get_runtime(self, exec_id: str) -> ExecRuntime:
        async with self._lock:
            rt = self._runtimes.get(exec_id)
        if rt is None:
            raise KeyError(f"unknown exec_id: {exec_id}")
        return rt

    def _apply_spec(self, rt: ExecRuntime, spec: Dict[str, Any]) -> None:
        rt.log_limit_lines = int(
            spec.get("log_limit_lines", rt.log_limit_lines or 5000)
        )
        rt.restart_policy = str(
            spec.get("restart_policy", rt.restart_policy or "on-failure")
        )
        rt.max_restarts = int(spec.get("max_restarts", rt.max_restarts or 10))
        rt.restart_window_sec = int(
            spec.get("restart_window_sec", rt.restart_window_sec or 300)
        )

    async def _spawn(self, rt: ExecRuntime) -> None:
        cmd: List[str] = rt.spec["cmd"]
        cwd = rt.spec.get("cwd")
        env = self._build_env(rt.spec.get("env"))

        logger.info(
            "Starting exec_id={} cmd={} cwd={} env_keys={}",
            rt.exec_id,
            cmd,
            cwd,
            list((rt.spec.get("env") or {}).keys()),
        )

        rt.status = "STARTING"
        rt.started_at_ms = rt._now_ms()
        rt.stopped_at_ms = None
        rt.last_exit_code = None

        await rt.append_log("system", f"Starting: {cmd}")

        rt.process = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=cwd,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        logger.info(
            "Started exec_id={} pid={}",
            rt.exec_id,
            rt.process.pid,
        )

        rt.status = "RUNNING"

        rt.stdout_task = asyncio.create_task(
            self._pump_stream(rt, "stdout", rt.process.stdout)
        )
        rt.stderr_task = asyncio.create_task(
            self._pump_stream(rt, "stderr", rt.process.stderr)
        )
        rt.waiter_task = asyncio.create_task(self._wait_process(rt))

    async def _stop(self, rt: ExecRuntime, *, reason: str) -> None:
        proc = rt.process
        if proc is None or rt.status in ("STOPPING", "STOPPED"):
            rt.status = "STOPPED"
            rt.process = None
            return

        logger.info(
            "Stop exec_id={} pid={} reason={} status={}",
            rt.exec_id,
            proc.pid,
            reason,
            rt.status,
        )

        rt.status = "STOPPING"
        await rt.append_log("system", f"Stopping ({reason})...")

        stop_signal_name = str(rt.spec.get("stop_signal", "SIGTERM"))
        timeout_sec = float(rt.spec.get("stop_timeout_sec", 10))

        sig = getattr(signal, stop_signal_name, signal.SIGTERM)

        try:
            proc.send_signal(sig)
        except ProcessLookupError:
            pass

        try:
            await asyncio.wait_for(proc.wait(), timeout=timeout_sec)
        except asyncio.TimeoutError:
            await rt.append_log(
                "system", f"Stop timeout after {timeout_sec}s, killing..."
            )
            try:
                proc.kill()
            except ProcessLookupError:
                pass
            try:
                await proc.wait()
            except Exception as e:
                logger.error(
                    "Error waiting for killed process exec_id={} pid={} err={}",
                    rt.exec_id,
                    proc.pid,
                    e,
                )

        for t in (rt.stdout_task, rt.stderr_task, rt.waiter_task):
            if t and not t.done():
                t.cancel()

        rt.stdout_task = None
        rt.stderr_task = None
        rt.waiter_task = None

        rt.last_exit_code = proc.returncode
        rt.process = None
        rt.stopped_at_ms = rt._now_ms()
        rt.status = "STOPPED"

        await rt.append_log("system", f"Stopped (exit_code={rt.last_exit_code})")

    async def _pump_stream(
        self,
        rt: ExecRuntime,
        stream_name: str,
        stream: Optional[asyncio.StreamReader],
    ) -> None:
        if stream is None:
            return
        try:
            while True:
                line = await stream.readline()
                if not line:
                    break
                txt = line.decode(errors="replace").rstrip("\n")
                await rt.append_log(stream_name, txt)
        except asyncio.CancelledError:
            return
        except Exception as e:
            await rt.append_log("system", f"log pump error ({stream_name}): {e!r}")

    async def _wait_process(self, rt: ExecRuntime) -> None:
        proc = rt.process
        if proc is None:
            return

        try:
            code = await proc.wait()
        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.error(
                "Wait error exec_id={} pid={} err={}",
                rt.exec_id,
                proc.pid if proc else None,
                e,
            )
            async with rt._state_lock:
                rt.status = "CRASHED"
                rt.last_exit_code = None
                rt.stopped_at_ms = rt._now_ms()
            await rt.append_log("system", f"wait error: {e!r}")
            return

        async with rt._state_lock:
            logger.info(
                "Exited exec_id={} pid={} code={}",
                rt.exec_id,
                proc.pid,
                code,
            )
            rt.last_exit_code = code
            rt.process = None
            rt.stdout_task = None
            rt.stderr_task = None
            rt.waiter_task = None
            rt.stopped_at_ms = rt._now_ms()

            if rt.desired_state == "RUNNING":
                rt.status = "CRASHED" if code != 0 else "EXITED"
            else:
                rt.status = "STOPPED"

        await rt.append_log("system", f"Process exited (code={code})")

        if await self._should_restart(rt, exit_code=code):
            await self._record_restart(rt, reason="auto-restart", exit_code=code)
            await asyncio.sleep(0.5)
            async with rt._state_lock:
                if rt.desired_state == "RUNNING":
                    logger.info(
                        "Auto-restarting exec_id={}",
                        rt.exec_id,
                    )
                    await self._spawn(rt)

    async def _should_restart(self, rt: ExecRuntime, *, exit_code: int) -> bool:
        async with rt._state_lock:
            if rt.desired_state != "RUNNING":
                logger.debug(
                    "Desired_state!=RUNNING exec_id={} desired_state={}",
                    rt.exec_id,
                    rt.desired_state,
                )
                return False

            policy = rt.restart_policy
            if policy == "never":
                logger.debug("Policy=never exec_id={}", rt.exec_id)
                return False
            if policy == "on-failure" and exit_code == 0:
                logger.debug(
                    "On-failure & exit_code=0 exec_id={}",
                    rt.exec_id,
                )
                return False

            now = time.monotonic()
            window = float(rt.restart_window_sec)
            rt._restart_times = [t for t in rt._restart_times if (now - t) <= window]
            if len(rt._restart_times) >= int(rt.max_restarts):
                rt.status = "CRASHED"
                await rt.append_log(
                    "system",
                    f"Restart suppressed: max_restarts={rt.max_restarts} in window={rt.restart_window_sec}s",
                )
                return False

            rt._restart_times.append(now)
            logger.info(
                "Allowed exec_id={} exit_code={} restart_count_window={}",
                rt.exec_id,
                exit_code,
                len(rt._restart_times),
            )
            return True

    async def _record_restart(
        self, rt: ExecRuntime, *, reason: str, exit_code: Optional[int]
    ) -> None:
        async with rt._state_lock:
            rt.restart_history.append(
                RestartEvent(ts_ms=rt._now_ms(), reason=reason, exit_code=exit_code)
            )
            if len(rt.restart_history) > 2000:
                rt.restart_history = rt.restart_history[-2000:]
            logger.info(
                "Restart exec_id={} reason={} exit_code={} history_len={}",
                rt.exec_id,
                reason,
                exit_code,
                len(rt.restart_history),
            )

    def _restart_count_in_window(self, rt: ExecRuntime) -> int:
        now = time.monotonic()
        window = float(rt.restart_window_sec)
        return sum(1 for t in rt._restart_times if (now - t) <= window)

    def _build_env(self, env_override: Any) -> Dict[str, str]:
        env = dict(os.environ)
        if isinstance(env_override, Mapping):
            for k, v in env_override.items():
                env[str(k)] = str(v)
        return env
