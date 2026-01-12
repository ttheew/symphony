from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

import yaml

Mode = Literal["conductor", "node"]


@dataclass(frozen=True)
class LoggingConfig:
    level: str = "INFO"
    json: bool = False


@dataclass(frozen=True)
class TlsConfig:
    cert_file: str = None
    key_file: str = None
    ca_file: str = None


@dataclass(frozen=True)
class ConductorConfig:
    listen: str = "0.0.0.0:8080"
    server: Optional[str] = None
    cert_path: str = None


@dataclass(frozen=True)
class NodeConfig:
    node_id: str
    conductor_addr: str
    groups: List[str]
    capacities_total: Dict[str, int]
    heartbeat_sec: float = 3.0
    tls: TlsConfig = None


@dataclass(frozen=True)
class AppConfig:
    mode: Mode
    logging: LoggingConfig
    conductor: ConductorConfig
    node: Optional[NodeConfig] = None


def _require(d: Dict[str, Any], key: str) -> Any:
    if key not in d:
        raise ValueError(f"Missing required config key: {key}")
    return d[key]


def load_config(path: str) -> AppConfig:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    raw = yaml.safe_load(p.read_text()) or {}
    mode: Mode = raw.get("mode", "conductor")

    log_raw = raw.get("logging") or {}
    logging_cfg = LoggingConfig(
        level=str(log_raw.get("level", "INFO")).upper(),
        json=bool(log_raw.get("json", False)),
    )

    def _opt_str(v: Any) -> Optional[str]:
        if v is None:
            return None
        s = str(v)
        return s if s else None

    node_cfg: Optional[NodeConfig] = None
    conductor_cfg: Optional[ConductorConfig] = None
    if mode == "node":
        nraw = raw.get("node") or {}
        node_tls_raw = nraw.get("tls")
        node_tls = TlsConfig(
            cert_file=_opt_str(node_tls_raw.get("cert_file")),
            key_file=_opt_str(node_tls_raw.get("key_file")),
            ca_file=_opt_str(node_tls_raw.get("ca_file")),
        )

        node_cfg = NodeConfig(
            node_id=str(_require(nraw, "node_id")),
            conductor_addr=str(_require(nraw, "conductor_addr")),
            groups=list(_require(nraw, "groups")),
            capacities_total=dict(_require(nraw, "capacities_total")),
            heartbeat_sec=float(nraw.get("heartbeat_sec", 3.0)),
            tls=node_tls,
        )
    else:
        cond_raw = raw.get("conductor") or {}
        cond_tls_raw = cond_raw.get("tls")

        conductor_cfg = ConductorConfig(
            listen=str(cond_raw.get("listen", "0.0.0.0:8080")),
            server=_opt_str(cond_raw.get("server")),
            cert_path=cond_tls_raw.get("cert_path"),
        )

    return AppConfig(
        mode=mode,
        logging=logging_cfg,
        conductor=conductor_cfg,
        node=node_cfg,
    )
