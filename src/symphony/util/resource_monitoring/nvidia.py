from typing import Any, Dict, List


class Nvml:
    def __init__(self) -> None:
        self._ok = False
        self._pynvml = None
        try:
            import pynvml

            self._pynvml = pynvml
            pynvml.nvmlInit()
            self._ok = True
        except Exception:
            self._ok = False

    def ok(self) -> bool:
        return self._ok

    def shutdown(self) -> None:
        if not self._ok or self._pynvml is None:
            return
        try:
            self._pynvml.nvmlShutdown()
        except Exception:
            pass

    def snapshot(self) -> List[Dict[str, Any]]:
        if not self._ok or self._pynvml is None:
            return []

        nvml = self._pynvml
        gpus: List[Dict[str, Any]] = []
        try:
            n = nvml.nvmlDeviceGetCount()
        except Exception:
            return []

        for i in range(n):
            try:
                h = nvml.nvmlDeviceGetHandleByIndex(i)
                name = (
                    nvml.nvmlDeviceGetName(h).decode("utf-8", "replace")
                    if hasattr(nvml.nvmlDeviceGetName(h), "decode")
                    else str(nvml.nvmlDeviceGetName(h))
                )
                util = nvml.nvmlDeviceGetUtilizationRates(h)
                mem = nvml.nvmlDeviceGetMemoryInfo(h)
                temp_c = None
                power_w = None
                try:
                    temp_c = nvml.nvmlDeviceGetTemperature(h, nvml.NVML_TEMPERATURE_GPU)
                except Exception:
                    pass
                try:
                    power_w = nvml.nvmlDeviceGetPowerUsage(h) / 1000.0
                except Exception:
                    pass

                gpus.append(
                    {
                        "index": i,
                        "name": name,
                        "util_percent": float(getattr(util, "gpu", 0)),
                        "mem_util_percent": float(getattr(util, "memory", 0)),
                        "mem_total_bytes": int(getattr(mem, "total", 0)),
                        "mem_used_bytes": int(getattr(mem, "used", 0)),
                        "mem_free_bytes": int(getattr(mem, "free", 0)),
                        "temperature_c": temp_c,
                        "power_w": power_w,
                    }
                )
            except Exception:
                continue
        return gpus
