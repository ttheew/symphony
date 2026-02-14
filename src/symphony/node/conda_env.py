import asyncio
import json
import os
import shlex
from typing import Iterable, List

from loguru import logger


FORCE_RECREATE_MARKER = "__SYMPHONY_FORCE_RECREATE__"


class CondaEnvManager:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._failed_specs: dict[str, str] = {}
        self._conda_path = str(os.getenv("CONDA_PATH", "conda") or "").strip() or "conda"

    async def list_env_names(self) -> List[str]:
        result = await self._run_cmd(self._build_conda_cmd("env", "list", "--json"))
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
                raw_custom_script = str(getattr(env, "custom_script", "") or "").strip()
                force_recreate, custom_script = self._parse_custom_script(raw_custom_script)
                if not name:
                    continue
                if not python_version:
                    logger.warning("Skipping conda env {}: missing python_version", name)
                    continue
                spec_key = self._build_spec_key(
                    python_version=python_version,
                    packages=packages,
                    custom_script=custom_script,
                )
                if name in current:
                    self._failed_specs.pop(name, None)
                    if not force_recreate:
                        continue
                    logger.info("Force recreating existing conda env {}", name)
                    removed = await self._remove_env(name)
                    if not removed:
                        logger.warning("Failed to remove existing conda env {}", name)
                        continue
                    current.discard(name)
                elif not force_recreate and self._failed_specs.get(name) == spec_key:
                    logger.info(
                        "Skipping conda env {} retry; same spec failed previously",
                        name,
                    )
                    continue
                ok = await self._create_env(name, python_version, packages, custom_script)
                if ok:
                    current.add(name)
                    self._failed_specs.pop(name, None)
                else:
                    self._failed_specs[name] = spec_key
            return sorted(current)

    def _parse_custom_script(self, custom_script: str) -> tuple[bool, str]:
        if not custom_script:
            return False, ""
        lines = custom_script.splitlines()
        if lines and lines[0].strip() == FORCE_RECREATE_MARKER:
            return True, "\n".join(lines[1:]).strip()
        return False, custom_script

    def _build_spec_key(
        self, *, python_version: str, packages: List[str], custom_script: str
    ) -> str:
        return json.dumps(
            {
                "python_version": python_version,
                "packages": list(packages),
                "custom_script": custom_script,
            },
            sort_keys=True,
        )

    async def _create_env(
        self, name: str, python_version: str, packages: List[str], custom_script: str
    ) -> bool:
        logger.info("Creating conda env {} (python={})", name, python_version)
        result = await self._run_cmd(
            self._build_conda_cmd("create", "-y", "-n", name, f"python={python_version}")
        )
        if result is None:
            logger.warning("Conda env creation failed for {}", name)
            return False

        if custom_script:
            logger.info("Running custom script for env {}", name)
            result = await self._run_cmd(custom_script)
            if result is None:
                logger.warning("Custom script failed for env {}", name)
                await self._cleanup_failed_env(name)
                return False

        if packages:
            upgrade_pip_cmd = self._build_conda_cmd(
                "run", "-n", name, "python", "-m", "pip", "install", "--upgrade", "pip"
            )
            logger.info("Upgrading pip in conda env {}", name)
            result = await self._run_cmd(upgrade_pip_cmd)
            if result is None:
                logger.warning("Pip upgrade failed for {}", name)
                await self._cleanup_failed_env(name)
                return False

            install_cmd = self._build_conda_cmd("run", "-n", name, "pip", "install", *packages)
            logger.info(
                "Installing {} packages with pip in conda env {}", len(packages), name
            )
            result = await self._run_cmd(install_cmd)
            if result is None:
                logger.warning("Pip package install failed for {}", name)
                await self._cleanup_failed_env(name)
                return False
        return True

    async def _remove_env(self, name: str) -> bool:
        logger.info("Removing conda env {}", name)
        result = await self._run_cmd(self._build_conda_cmd("env", "remove", "-y", "-n", name))
        return result is not None

    def _build_conda_cmd(self, *args: str) -> str:
        return " ".join(shlex.quote(part) for part in [self._conda_path, *args])

    async def _cleanup_failed_env(self, name: str) -> None:
        logger.info("Cleaning up partially created conda env {}", name)
        removed = await self._remove_env(name)
        if not removed:
            logger.warning("Failed to clean up partially created conda env {}", name)

    async def _run_cmd(self, cmd: str) -> str | None:
        try:
            proc = await asyncio.create_subprocess_exec(
                "bash",
                "-lc",
                cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except Exception as exc:
            logger.warning("Failed to start command: {} err={}", cmd, exc)
            return None

        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            logger.warning(
                "command failed rc={} cmd={} stderr={}",
                proc.returncode,
                cmd,
                (stderr or b"").decode(errors="ignore").strip(),
            )
            return None
        return (stdout or b"").decode(errors="ignore").strip()
