import threading
from typing import Any, Dict, List, Optional

from symphony.util.resource_monitoring.cpu import (
    cpu_percent,
    parse_cpu_times_from_proc_stat,
)
from symphony.util.resource_monitoring.disk import space_for_mount
from symphony.util.resource_monitoring.models import CpuTimes
from symphony.util.resource_monitoring.nvidia import Nvml
from symphony.util.resource_monitoring.ram import ram_snapshot
from symphony.util.resource_monitoring.utils import now


class Monitor:
    """
    Resource monitor with a background sampler thread.
    """

    def __init__(
        self,
        mount_points: Optional[List[str]] = None,
        sample_interval: float = 1.0,
        space_interval: float = 30.0,
        disk_devices: Optional[List[str]] = None,
    ) -> None:
        self.mount_points = mount_points or ["/"]
        self.sample_interval = float(sample_interval)
        self.space_interval = float(space_interval)
        self.disk_devices = disk_devices

        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._stop_evt = threading.Event()

        self._nvml = Nvml()

        self._cpu_prev: Optional[CpuTimes] = None
        self._per_core_prev: Optional[Dict[str, CpuTimes]] = None
        self._t_prev: Optional[float] = None
        self._t_space_prev: float = 0.0

        self._state: Dict[str, Any] = {
            "timestamp_unix": None,
            "cpu": None,
            "ram": None,
            "disk_space": None,
            "gpus": None,
        }

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_evt.clear()
        self._thread = threading.Thread(
            target=self._run, name="LightMonitor", daemon=True
        )
        self._thread.start()

    def stop(self, timeout: float = 2.0) -> None:
        self._stop_evt.set()
        if self._thread:
            self._thread.join(timeout=timeout)
        self._nvml.shutdown()

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return dict(self._state)

    def _run(self) -> None:
        try:
            self._cpu_prev, self._per_core_prev = parse_cpu_times_from_proc_stat()
        except Exception:
            self._cpu_prev, self._per_core_prev = None, None

        self._t_prev = now()
        self._t_space_prev = 0.0

        while not self._stop_evt.is_set():
            t0 = now()
            dt = (
                (t0 - self._t_prev)
                if self._t_prev is not None
                else self.sample_interval
            )
            if dt <= 0:
                dt = self.sample_interval

            cpu_block = self._sample_cpu()
            ram_block = self._sample_ram()
            gpus_block = self._sample_gpus()

            disk_space_block = None
            if (
                t0 - self._t_space_prev
            ) >= self.space_interval or self._t_space_prev == 0.0:
                disk_space_block = self._sample_disk_space()
                self._t_space_prev = t0

            with self._lock:
                self._state["timestamp_unix"] = int(t0)
                if cpu_block is not None:
                    self._state["cpu"] = cpu_block
                if ram_block is not None:
                    self._state["ram"] = ram_block
                if disk_space_block is not None:
                    self._state["disk_space"] = disk_space_block
                if gpus_block is not None:
                    self._state["gpus"] = gpus_block

            self._t_prev = t0

            elapsed = now() - t0
            sleep_for = self.sample_interval - elapsed
            if sleep_for > 0:
                self._stop_evt.wait(timeout=sleep_for)

    def _sample_cpu(self) -> Optional[Dict[str, Any]]:
        try:
            cur_global, cur_cores = parse_cpu_times_from_proc_stat()
        except Exception:
            return None

        out: Dict[str, Any] = {}

        if self._cpu_prev is not None:
            out["total_percent"] = cpu_percent(self._cpu_prev, cur_global)
        else:
            out["total_percent"] = 0.0

        per_core_out: Dict[str, float] = {}
        if self._per_core_prev is not None:
            for core, cur in cur_cores.items():
                prev = self._per_core_prev.get(core)
                if prev is None:
                    continue
                per_core_out[core] = cpu_percent(prev, cur)
        out["per_core_percent"] = per_core_out

        self._cpu_prev = cur_global
        self._per_core_prev = cur_cores
        return out

    def _sample_ram(self) -> Optional[Dict[str, Any]]:
        try:
            return ram_snapshot()
        except Exception:
            return None

    def _sample_disk_space(self) -> Dict[str, Any]:
        mounts: List[Dict[str, Any]] = []
        for mp in self.mount_points:
            try:
                mounts.append(space_for_mount(mp))
            except Exception:
                mounts.append({"path": mp, "error": "statvfs_failed"})
        return {"mounts": mounts}

    def _sample_gpus(self) -> Optional[List[Dict[str, Any]]]:
        if not self._nvml.ok():
            return []
        return self._nvml.snapshot()
