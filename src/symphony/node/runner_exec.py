import asyncio
import base64
import os
import shlex
import signal
import sys
import time
from pathlib import Path
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Optional, Tuple
from zoneinfo import ZoneInfo

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
    hc_task: Optional[asyncio.Task] = None
    auto_restart_task: Optional[asyncio.Task] = None

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
    restart_backoff_seconds: float = 0.5
    max_restarts: int = 10
    restart_window_sec: int = 300
    _restart_times: List[float] = field(default_factory=list)
    restart_history: List[RestartEvent] = field(default_factory=list)
    auto_restart_cron: Optional[str] = None
    auto_restart_timezone: Optional[str] = None

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
        self._cron_minute_horizon = 2 * 366 * 24 * 60

    async def _restartable_task(
        self,
        *,
        name: str,
        rt: ExecRuntime,
        coro_factory,
        restart_delay: float = 5,
    ):
        """
        Runs a task, restarts it if it crashes.
        Stops cleanly on cancellation or when exec stops.
        """
        while True:
            try:
                await coro_factory()
                # Normal exit → do not restart
                return

            except asyncio.CancelledError:
                # Explicit shutdown
                raise

            except Exception as e:
                logger.error(
                    "Task crashed exec_id={} task={} err={}",
                    rt.exec_id,
                    name,
                    e,
                )
                await rt.append_log(
                    "system", f"Task {name} crashed: {e!r}, restarting..."
                )

                # If process is gone or stopping, do NOT restart
                async with rt._state_lock:
                    if rt.process is None or rt.status in ("STOPPING", "STOPPED"):
                        logger.info(
                            "Not restarting task exec_id={} task={} (process stopped)",
                            rt.exec_id,
                            name,
                        )
                        return

                await asyncio.sleep(restart_delay)

    async def add_exec(self, exec_id: str, specification: Mapping[str, Any]) -> None:
        spec = dict(specification)

        logger.debug(
            "Adding exec process exec_id={} specification={}",
            exec_id,
            self._sanitize_spec_for_log(spec),
        )
        config = spec["config"]
        cmd = config.get("command")
        if (
            not isinstance(cmd, list)
            or not cmd
            or not all(isinstance(x, str) for x in cmd)
        ):
            raise ValueError("spec['cmd'] must be a non-empty list[str]")

        old_spec: Optional[Dict[str, Any]] = None
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
                old_spec = dict(rt.spec)
                rt.spec = spec
                rt.capacity_requests = (
                    spec.get("capacity_requests") if "capacity_requests" in spec else {}
                )
                self._apply_spec(rt, spec)

        if rt is not None and old_spec is not None:
            await self._reconcile_spec_update(rt, old_spec=old_spec, new_spec=spec)

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
                "restart_backoff_seconds": rt.restart_backoff_seconds,
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
        restart_policy_spec = spec.get("restart_policy")
        if isinstance(restart_policy_spec, Mapping):
            policy_type = restart_policy_spec.get("type", rt.restart_policy or "on-failure")
            rt.restart_policy = str(policy_type).strip().lower()
            try:
                rt.restart_backoff_seconds = max(
                    0.0,
                    float(
                        restart_policy_spec.get(
                            "backoff_seconds", rt.restart_backoff_seconds or 0.5
                        )
                    ),
                )
            except (TypeError, ValueError):
                rt.restart_backoff_seconds = 0.5
        else:
            rt.restart_policy = str(
                spec.get("restart_policy", rt.restart_policy or "on-failure")
            ).strip().lower()

        rt.max_restarts = int(spec.get("max_restarts", rt.max_restarts or 10))
        rt.restart_window_sec = int(
            spec.get("restart_window_sec", rt.restart_window_sec or 300)
        )
        auto_restart = spec.get("auto_restart")
        if isinstance(auto_restart, Mapping) and auto_restart.get("enabled") is True:
            cron = auto_restart.get("cron")
            tz_name = auto_restart.get("timezone")
            if isinstance(cron, str) and cron.strip() and isinstance(tz_name, str) and tz_name.strip():
                rt.auto_restart_cron = cron.strip()
                rt.auto_restart_timezone = tz_name.strip()
                return
        rt.auto_restart_cron = None
        rt.auto_restart_timezone = None

    async def _spawn(self, rt: ExecRuntime) -> None:
        cmd: List[str] = rt.spec["config"]["command"]
        run_cmd = self._with_conda_env_if_needed(cmd, rt.spec)
        env = self._build_env(rt.spec.get("env"))

        rt.status = "STARTING"
        rt.started_at_ms = rt._now_ms()
        rt.stopped_at_ms = None
        rt.last_exit_code = None

        repo_workdir = None
        try:
            if self._extract_repo_config(rt.spec)[0]:
                if not shutil.which("git"):
                    raise RuntimeError("git is required to pull git repo but was not found")
                repo_workdir = await self._prepare_repo(rt)
                if repo_workdir:
                    await rt.append_log(
                        "system",
                        f"Git repo prepared at {repo_workdir}",
                    )
        except Exception as e:
            rt.status = "CRASHED"
            rt.stopped_at_ms = rt._now_ms()
            await rt.append_log("system", f"Git repo prep failed: {e!r}")
            logger.error(
                "Git repo prep failed exec_id={} err={}",
                rt.exec_id,
                e,
            )
            return

        cwd = repo_workdir

        logger.info(
            "Starting exec_id={} cmd={} cwd={} env_keys={}",
            rt.exec_id,
            cmd,
            cwd,
            list((rt.spec.get("env") or {}).keys()),
        )

        await rt.append_log("system", f"Starting: {cmd}")

        try:
            rt.process = await asyncio.create_subprocess_exec(
                *run_cmd,
                cwd=cwd,
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except Exception as e:
            rt.status = "CRASHED"
            rt.stopped_at_ms = rt._now_ms()
            await rt.append_log("system", f"Failed to start process: {e!r}")
            logger.error(
                "Failed to start process exec_id={} err={}",
                rt.exec_id,
                e,
            )
            return

        logger.info(
            "Started exec_id={} pid={}",
            rt.exec_id,
            rt.process.pid,
        )

        rt.status = "RUNNING"

        rt.stdout_task = asyncio.create_task(
            self._restartable_task(
                name="stdout_pump",
                rt=rt,
                coro_factory=lambda: self._pump_stream(
                    rt, "stdout", rt.process.stdout
                ),
            )
        )

        rt.stderr_task = asyncio.create_task(
            self._restartable_task(
                name="stderr_pump",
                rt=rt,
                coro_factory=lambda: self._pump_stream(
                    rt, "stderr", rt.process.stderr
                ),
            )
        )

        rt.waiter_task = asyncio.create_task(
            self._restartable_task(
                name="process waiter",
                rt=rt,
                coro_factory=lambda: self._wait_process(rt),
            )
        )

        health_check = rt.spec.get("health_check")
        if health_check:
            rt.hc_task = asyncio.create_task(
                self._restartable_task(
                    name="health_check",
                    rt=rt,
                    coro_factory=lambda: self._run_health_check(rt),
                    restart_delay=2.0,
                )
            )
        self._start_auto_restart_task(rt)

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

        current_task = asyncio.current_task()
        for t in (
            rt.stdout_task,
            rt.stderr_task,
            rt.waiter_task,
            rt.hc_task,
            rt.auto_restart_task,
        ):
            if t and t is not current_task and not t.done():
                t.cancel()

        rt.stdout_task = None
        rt.stderr_task = None
        rt.waiter_task = None
        rt.hc_task = None
        rt.auto_restart_task = None

        rt.last_exit_code = proc.returncode
        rt.process = None
        rt.stopped_at_ms = rt._now_ms()
        rt.status = "STOPPED"

        await rt.append_log("system", f"Stopped (exit_code={rt.last_exit_code})")

    async def _reconcile_spec_update(
        self, rt: ExecRuntime, *, old_spec: Dict[str, Any], new_spec: Dict[str, Any]
    ) -> None:
        old_cmd = (old_spec.get("config") or {}).get("command")
        new_cmd = (new_spec.get("config") or {}).get("command")
        old_repo = (old_spec.get("config") or {}).get("git_repo")
        new_repo = (new_spec.get("config") or {}).get("git_repo")
        old_env = old_spec.get("env")
        new_env = new_spec.get("env")
        old_hc = old_spec.get("health_check")
        new_hc = new_spec.get("health_check")
        old_auto_restart = old_spec.get("auto_restart")
        new_auto_restart = new_spec.get("auto_restart")

        process_config_changed = (
            old_cmd != new_cmd or old_repo != new_repo or old_env != new_env
        )
        health_check_changed = old_hc != new_hc
        auto_restart_changed = old_auto_restart != new_auto_restart

        if process_config_changed:
            async with rt._state_lock:
                should_restart = (
                    rt.desired_state == "RUNNING"
                    and rt.process is not None
                    and rt.status in ("STARTING", "RUNNING")
                )
            if should_restart:
                await rt.append_log(
                    "system", "Spec updated; restarting process to apply new config"
                )
                await self._record_restart(
                    rt, reason="spec-updated-restart", exit_code=None
                )
                await self.restart(rt.exec_id, reason="spec updated")
            return

        if health_check_changed:
            async with rt._state_lock:
                running = rt.process is not None and rt.status in ("STARTING", "RUNNING")
                old_task = rt.hc_task
                rt.hc_task = None
                if old_task and not old_task.done():
                    old_task.cancel()
                if running and rt.desired_state == "RUNNING" and new_hc:
                    rt.hc_task = asyncio.create_task(
                        self._restartable_task(
                            name="health_check",
                            rt=rt,
                            coro_factory=lambda: self._run_health_check(rt),
                            restart_delay=2.0,
                        )
                    )

            await rt.append_log("system-hc", "Health check config updated and reloaded")

        if auto_restart_changed:
            async with rt._state_lock:
                running = rt.process is not None and rt.status in ("STARTING", "RUNNING")
                old_task = rt.auto_restart_task
                rt.auto_restart_task = None
                if old_task and not old_task.done():
                    old_task.cancel()
                if running and rt.desired_state == "RUNNING":
                    self._start_auto_restart_task(rt)

            await rt.append_log("system-ar", "Auto restart config updated and reloaded")


    async def _run_health_check(
        self,
        rt: ExecRuntime,
    ):
        hc_spec = rt.spec["health_check"]
        initial_delay_seconds = hc_spec["initial_delay_seconds"]
        period_seconds = hc_spec["period_seconds"]
        timeout_seconds = float(hc_spec.get("timeout_seconds", period_seconds))
        repo, _, _ = self._extract_repo_config(rt.spec)
        hc_cwd = str(self._repo_workdir(rt.exec_id)) if repo else os.getcwd()
        raw_command = hc_spec.get("command")

        if isinstance(raw_command, list) and all(
            isinstance(part, str) for part in raw_command
        ):
            cmd = raw_command
        elif isinstance(raw_command, str) and raw_command.strip():
            cmd = shlex.split(raw_command)
        else:
            await rt.append_log(
                "system-hc", "Health check misconfigured: invalid command"
            )
            return

        if len(cmd) == 1 and cmd[0].endswith(".py"):
            cmd = [sys.executable, cmd[0]]

        await rt.append_log(
            "system-hc", f"Waiting initial_delay_seconds - {initial_delay_seconds}"
        )
        await asyncio.sleep(initial_delay_seconds)
        await rt.append_log("system-hc", f"Starting periodic health check")
        try:
            while True:
                if rt.process is None:
                    break
                await asyncio.sleep(period_seconds)
                if rt.process is None:
                    break

                logger.debug("Running health check exec_id={} pid={}",
                    rt.exec_id,
                    rt.process.pid,
                )

                healthy = False
                detail = ""
                try:
                    proc = await asyncio.create_subprocess_exec(
                        *self._with_conda_env_if_needed(cmd, rt.spec),
                        cwd=hc_cwd,
                        env=self._build_env(rt.spec.get("env")),
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                    )
                    try:
                        out, err = await asyncio.wait_for(
                            proc.communicate(), timeout=timeout_seconds
                        )
                    except asyncio.TimeoutError:
                        proc.kill()
                        out, err = await proc.communicate()
                        detail = f"timed out after {timeout_seconds}s"
                    else:
                        healthy = proc.returncode == 0
                        if not healthy:
                            detail = f"exit_code={proc.returncode}"

                    if not healthy:
                        stdout_text = out.decode(errors="replace").strip() if out else ""
                        stderr_text = err.decode(errors="replace").strip() if err else ""
                        if stdout_text:
                            detail = f"{detail}; stdout={stdout_text}"
                        if stderr_text:
                            detail = f"{detail}; stderr={stderr_text}"
                except Exception as e:
                    detail = f"exception={e!r}"

                if not healthy:
                    await rt.append_log(
                        "system-hc",
                        f"Health check failed ({detail or 'unknown failure'}), requesting restart",
                    )
                    await self._record_restart(
                        rt, reason="health-check-failed", exit_code=None
                    )
                    await self.restart(rt.exec_id, reason="health check failed")
                    return
        except asyncio.CancelledError:
            return

    def _start_auto_restart_task(self, rt: ExecRuntime) -> None:
        if not rt.auto_restart_cron or not rt.auto_restart_timezone:
            return
        if rt.auto_restart_task and not rt.auto_restart_task.done():
            rt.auto_restart_task.cancel()
        rt.auto_restart_task = asyncio.create_task(
            self._restartable_task(
                name="auto_restart_scheduler",
                rt=rt,
                coro_factory=lambda: self._run_auto_restart(rt),
                restart_delay=30.0,
            )
        )

    async def _run_auto_restart(self, rt: ExecRuntime) -> None:
        cron_expr = rt.auto_restart_cron
        tz_name = rt.auto_restart_timezone
        if not cron_expr or not tz_name:
            return
        try:
            tz = ZoneInfo(tz_name)
            parsed = self._parse_cron_expr(cron_expr)
        except Exception as e:
            await rt.append_log(
                "system-ar",
                f"Auto restart disabled due to invalid config: {e}",
            )
            return

        await rt.append_log(
            "system-ar",
            f"Auto restart enabled (cron='{cron_expr}' timezone='{tz_name}')",
        )

        while True:
            now_utc = datetime.now(timezone.utc)
            try:
                next_run_utc = self._next_cron_match_utc(
                    parsed=parsed,
                    tz=tz,
                    from_utc=now_utc,
                )
            except Exception as e:
                await rt.append_log("system-ar", f"Failed to calculate next restart: {e}")
                return

            sleep_sec = max(0.0, (next_run_utc - now_utc).total_seconds())
            await asyncio.sleep(sleep_sec)

            if rt.desired_state != "RUNNING":
                return

            await rt.append_log(
                "system-ar",
                "Scheduled restart triggered",
            )
            await self._record_restart(
                rt,
                reason="scheduled-auto-restart",
                exit_code=None,
            )
            await self.restart(rt.exec_id, reason="scheduled auto-restart")
            return

    def _parse_cron_expr(self, expr: str) -> Dict[str, Any]:
        parts = expr.split()
        if len(parts) != 5:
            raise ValueError("cron must have 5 fields: minute hour day month weekday")

        fields = [
            self._parse_cron_field(parts[0], 0, 59, "minute"),
            self._parse_cron_field(parts[1], 0, 23, "hour"),
            self._parse_cron_field(parts[2], 1, 31, "day"),
            self._parse_cron_field(parts[3], 1, 12, "month"),
            self._parse_cron_field(parts[4], 0, 7, "weekday"),
        ]
        weekday_values = set()
        for v in fields[4]["values"]:
            weekday_values.add(0 if v == 7 else v)
        fields[4]["values"] = weekday_values
        return {
            "minute": fields[0],
            "hour": fields[1],
            "day": fields[2],
            "month": fields[3],
            "weekday": fields[4],
        }

    def _parse_cron_field(
        self, raw: str, min_value: int, max_value: int, label: str
    ) -> Dict[str, Any]:
        token = raw.strip()
        wildcard = token == "*"
        values: set[int] = set()
        for piece in token.split(","):
            piece = piece.strip()
            if not piece:
                raise ValueError(f"invalid {label} field '{raw}'")
            for value in self._expand_cron_piece(piece, min_value, max_value, label):
                values.add(value)
        if not values:
            raise ValueError(f"empty {label} field '{raw}'")
        return {"wildcard": wildcard, "values": values}

    def _expand_cron_piece(
        self, piece: str, min_value: int, max_value: int, label: str
    ) -> List[int]:
        if "/" in piece:
            base, step_raw = piece.split("/", 1)
            try:
                step = int(step_raw)
            except Exception as e:
                raise ValueError(f"invalid {label} step '{step_raw}'") from e
            if step <= 0:
                raise ValueError(f"invalid {label} step '{step_raw}'")

            if base == "*":
                start, end = min_value, max_value
            elif "-" in base:
                start_raw, end_raw = base.split("-", 1)
                start, end = self._parse_cron_range(
                    start_raw, end_raw, min_value, max_value, label
                )
            else:
                try:
                    start = int(base)
                except Exception as e:
                    raise ValueError(f"invalid {label} value '{base}'") from e
                if start < min_value or start > max_value:
                    raise ValueError(f"{label} value out of range: {start}")
                end = max_value

            return list(range(start, end + 1, step))

        if piece == "*":
            return list(range(min_value, max_value + 1))

        if "-" in piece:
            start_raw, end_raw = piece.split("-", 1)
            start, end = self._parse_cron_range(
                start_raw, end_raw, min_value, max_value, label
            )
            return list(range(start, end + 1))

        try:
            value = int(piece)
        except Exception as e:
            raise ValueError(f"invalid {label} value '{piece}'") from e
        if value < min_value or value > max_value:
            raise ValueError(f"{label} value out of range: {value}")
        return [value]

    def _parse_cron_range(
        self,
        start_raw: str,
        end_raw: str,
        min_value: int,
        max_value: int,
        label: str,
    ) -> Tuple[int, int]:
        try:
            start = int(start_raw)
            end = int(end_raw)
        except Exception as e:
            raise ValueError(f"invalid {label} range '{start_raw}-{end_raw}'") from e
        if start > end:
            raise ValueError(f"invalid {label} range '{start}-{end}'")
        if start < min_value or end > max_value:
            raise ValueError(f"{label} range out of bounds: '{start}-{end}'")
        return start, end

    def _next_cron_match_utc(
        self, *, parsed: Dict[str, Any], tz: ZoneInfo, from_utc: datetime
    ) -> datetime:
        cursor = from_utc.astimezone(tz).replace(second=0, microsecond=0) + timedelta(
            minutes=1
        )
        for _ in range(self._cron_minute_horizon):
            if self._cron_matches_local(parsed, cursor):
                return cursor.astimezone(timezone.utc)
            cursor += timedelta(minutes=1)
        raise ValueError("no matching schedule time found in horizon")

    def _cron_matches_local(self, parsed: Dict[str, Any], dt_local: datetime) -> bool:
        if dt_local.minute not in parsed["minute"]["values"]:
            return False
        if dt_local.hour not in parsed["hour"]["values"]:
            return False
        if dt_local.month not in parsed["month"]["values"]:
            return False

        day_match = dt_local.day in parsed["day"]["values"]
        cron_dow = (dt_local.weekday() + 1) % 7
        dow_match = cron_dow in parsed["weekday"]["values"]
        day_is_wildcard = parsed["day"]["wildcard"]
        dow_is_wildcard = parsed["weekday"]["wildcard"]

        if day_is_wildcard and dow_is_wildcard:
            return True
        if day_is_wildcard:
            return dow_match
        if dow_is_wildcard:
            return day_match
        return day_match or dow_match

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
                if rt.process is None:
                    break
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
            old_hc_task = rt.hc_task
            rt.last_exit_code = code
            rt.process = None
            rt.stdout_task = None
            rt.stderr_task = None
            rt.waiter_task = None
            rt.hc_task = None
            rt.stopped_at_ms = rt._now_ms()

            if rt.desired_state == "RUNNING":
                rt.status = "CRASHED" if code != 0 else "EXITED"
            else:
                rt.status = "STOPPED"

        if old_hc_task and not old_hc_task.done():
            old_hc_task.cancel()

        await rt.append_log("system", f"Process exited (code={code})")

        if await self._should_restart(rt, exit_code=code):
            await self._record_restart(rt, reason="auto-restart", exit_code=code)
            backoff_seconds = self._resolve_restart_backoff_seconds(rt)
            if backoff_seconds > 0:
                await rt.append_log(
                    "system",
                    f"Waiting restart backoff: {backoff_seconds}s",
                )
                await asyncio.sleep(backoff_seconds)
            async with rt._state_lock:
                if rt.desired_state == "RUNNING":
                    logger.info(
                        "Auto-restarting exec_id={} backoff_seconds={}",
                        rt.exec_id,
                        backoff_seconds,
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

    def _sanitize_spec_for_log(self, spec: Mapping[str, Any]) -> Dict[str, Any]:
        safe = dict(spec)
        config = safe.get("config")
        if isinstance(config, Mapping):
            config = dict(config)
            if "token" in config:
                config["token"] = "***"
            safe["config"] = config
        return safe

    def _extract_repo_config(
        self, spec: Mapping[str, Any]
    ) -> tuple[Optional[str], Optional[str], Optional[str]]:
        config = spec.get("config") if isinstance(spec, Mapping) else None
        if not isinstance(config, Mapping):
            return None, None, None
        repo = config.get("git_repo")
        token = config.get("token")
        ref = config.get("git_ref")
        repo_str = repo.strip() if isinstance(repo, str) else None
        token_str = token.strip() if isinstance(token, str) else None
        ref_str = ref.strip() if isinstance(ref, str) else None
        return (repo_str or None), (token_str or None), (ref_str or None)

    async def _run_subprocess(
        self,
        cmd: List[str],
        *,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
    ) -> tuple[int, str, str]:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=cwd,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out_b, err_b = await proc.communicate()
        out = out_b.decode(errors="replace") if out_b else ""
        err = err_b.decode(errors="replace") if err_b else ""
        return proc.returncode or 0, out, err

    def _repo_workdir(self, exec_id: str) -> Path:
        return Path("/tmp/symphony/repos") / exec_id

    async def _prepare_repo(self, rt: ExecRuntime) -> Optional[str]:
        repo, token, ref = self._extract_repo_config(rt.spec)
        if not repo:
            return None

        dest = self._repo_workdir(rt.exec_id)
        await asyncio.to_thread(dest.parent.mkdir, parents=True, exist_ok=True)

        git_env = dict(os.environ)
        # Hard-disable interactive prompts so git fails fast instead of blocking.
        git_env["GIT_TERMINAL_PROMPT"] = "0"
        git_env["GIT_ASKPASS"] = "/bin/false"
        git_env["SSH_ASKPASS"] = "/bin/false"

        git_cmd_prefix: List[str] = ["git"]
        if token and repo.startswith(("http://", "https://")):
            # Use an auth header so we don't mutate the URL.
            # Git over HTTPS typically expects Basic auth, e.g. "x-access-token:<token>".
            basic = base64.b64encode(f"x-access-token:{token}".encode()).decode()
            git_cmd_prefix = [
                "git",
                "-c",
                f"http.extraHeader=Authorization: Basic {basic}",
            ]

        def _raise_git_error(op: str, err: str) -> None:
            msg = err.strip() or "unknown git error"
            lowered = msg.lower()
            auth_related = (
                "authentication failed" in lowered
                or "could not read username" in lowered
                or "terminal prompts disabled" in lowered
                or "401" in lowered
                or "403" in lowered
            )
            if auth_related:
                if token:
                    raise RuntimeError(
                        f"git {op} failed: invalid/unauthorized token for {repo}"
                    )
                raise RuntimeError(
                    f"git {op} failed: authentication required for {repo}"
                )
            raise RuntimeError(f"git {op} failed: {msg}")

        async def _checkout_ref() -> None:
            if not ref:
                return
            # Prefer remote branch if it exists, otherwise treat as tag/commit.
            rc, _, _ = await self._run_subprocess(
                git_cmd_prefix + ["rev-parse", "--verify", f"refs/remotes/origin/{ref}"],
                cwd=str(dest),
                env=git_env,
            )
            if rc == 0:
                rc, _, err = await self._run_subprocess(
                    git_cmd_prefix + ["checkout", "-B", ref, f"origin/{ref}"],
                    cwd=str(dest),
                    env=git_env,
                )
                if rc != 0:
                    _raise_git_error("checkout", err)
                rc, _, err = await self._run_subprocess(
                    git_cmd_prefix + ["reset", "--hard", f"origin/{ref}"],
                    cwd=str(dest),
                    env=git_env,
                )
                if rc != 0:
                    _raise_git_error("reset", err)
                return

            rc, _, err = await self._run_subprocess(
                git_cmd_prefix + ["checkout", ref],
                cwd=str(dest),
                env=git_env,
            )
            if rc != 0:
                _raise_git_error("checkout", err)

        if dest.exists():
            git_dir = dest / ".git"
            if not git_dir.exists():
                await asyncio.to_thread(shutil.rmtree, dest, ignore_errors=True)
            else:
                # Update existing repo to the latest remote HEAD.
                rc, _, err = await self._run_subprocess(
                    git_cmd_prefix + ["remote", "set-url", "origin", repo],
                    cwd=str(dest),
                    env=git_env,
                )
                if rc != 0:
                    _raise_git_error("remote set-url", err)

                rc, _, err = await self._run_subprocess(
                    git_cmd_prefix + ["fetch", "origin", "--prune", "--tags"],
                    cwd=str(dest),
                    env=git_env,
                )
                if rc != 0:
                    _raise_git_error("fetch", err)

                if ref:
                    await _checkout_ref()
                else:
                    rc, _, err = await self._run_subprocess(
                        git_cmd_prefix + ["reset", "--hard", "origin/HEAD"],
                        cwd=str(dest),
                        env=git_env,
                    )
                    if rc != 0:
                        _raise_git_error("reset", err)

                rc, _, err = await self._run_subprocess(
                    git_cmd_prefix + ["clean", "-fd"],
                    cwd=str(dest),
                    env=git_env,
                )
                if rc != 0:
                    _raise_git_error("clean", err)

                return str(dest)

        clone_cmd = git_cmd_prefix + ["clone", "--depth", "1"]
        if ref:
            clone_cmd += ["--branch", ref]
        clone_cmd += [repo, str(dest)]
        rc, _, err = await self._run_subprocess(
            clone_cmd,
            env=git_env,
        )
        if rc != 0:
            _raise_git_error("clone", err)

        if ref:
            await _checkout_ref()

        return str(dest)

    def _resolve_restart_backoff_seconds(self, rt: ExecRuntime) -> float:
        restart_policy_spec = rt.spec.get("restart_policy")
        if isinstance(restart_policy_spec, Mapping):
            raw_backoff = restart_policy_spec.get("backoff_seconds")
            if raw_backoff is not None:
                try:
                    return max(0.0, float(raw_backoff))
                except (TypeError, ValueError):
                    pass
        return max(0.0, float(rt.restart_backoff_seconds or 0.0))

    def _with_conda_env_if_needed(
        self, cmd: List[str], spec: Mapping[str, Any]
    ) -> List[str]:
        config = spec.get("config") if isinstance(spec, Mapping) else None
        env_name = None
        if isinstance(config, Mapping):
            candidate = config.get("env_name")
            if isinstance(candidate, str) and candidate.strip():
                env_name = candidate.strip()

        if not env_name:
            return cmd

        quoted_env_name = shlex.quote(env_name)
        quoted_cmd = " ".join(shlex.quote(part) for part in cmd)
        shell_cmd = (
            "eval \"$(conda shell.bash hook)\" "
            f"&& conda activate {quoted_env_name} "
            f"&& exec {quoted_cmd}"
        )
        return ["bash", "-lc", shell_cmd]
