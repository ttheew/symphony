import asyncio
import json
import os
import shlex
from typing import Iterable, List

from loguru import logger


class CondaEnvManager:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()

    async def list_env_names(self) -> List[str]:
        result = await self._run_conda_cmd("conda env list --json")
        if result is None:
            return []
        try:
            payload = json.loads(result)
            env_paths = payload.get("envs") or []
            names = []
            for path in env_paths:
                name = os.path.basename(str(path))
                if name:
                    names.append(name)
            return sorted(set(names))
        except Exception as exc:
            logger.warning("Failed to parse conda env list output: {}", exc)
            return []

    async def ensure_envs(self, envs: Iterable) -> List[str]:
        async with self._lock:
            current = set(await self.list_env_names())
            for env in envs:
                name = str(getattr(env, "name", "") or "").strip()
                python_version = str(getattr(env, "python_version", "") or "").strip()
                packages = list(getattr(env, "packages", []) or [])
                packages = [str(p).strip() for p in packages if str(p).strip()]
                if not name:
                    continue
                if name in current:
                    continue
                if not python_version:
                    logger.warning("Skipping conda env {}: missing python_version", name)
                    continue
                ok = await self._create_env(name, python_version, packages)
                if ok:
                    current.add(name)
            return sorted(current)

    async def _create_env(
        self, name: str, python_version: str, packages: List[str]
    ) -> bool:
        quoted_name = shlex.quote(name)
        quoted_python = shlex.quote(f"python={python_version}")
        quoted_packages = " ".join(shlex.quote(p) for p in packages)
        cmd = f"conda create -y -n {quoted_name} {quoted_python}"
        if quoted_packages:
            cmd = f"{cmd} {quoted_packages}"
        logger.info("Creating conda env {} (python={})", name, python_version)
        result = await self._run_conda_cmd(cmd)
        if result is None:
            logger.warning("Conda env creation failed for {}", name)
            return False
        return True

    async def _run_conda_cmd(self, cmd: str) -> str | None:
        try:
            proc = await asyncio.create_subprocess_exec(
                "bash",
                "-lc",
                cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except Exception as exc:
            logger.warning("Failed to start conda command: {} err={}", cmd, exc)
            return None

        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            logger.warning(
                "Conda command failed rc={} cmd={} stderr={}",
                proc.returncode,
                cmd,
                (stderr or b"").decode(errors="ignore").strip(),
            )
            return None
        return (stdout or b"").decode(errors="ignore").strip()
