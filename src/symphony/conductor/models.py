from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


class DesiredState(str, Enum):
    running = "RUNNING"
    stopped = "STOPPED"


class CurrentState(str, Enum):
    pending = "PENDING"
    running = "RUNNING"
    stopped = "STOPPED"
    failed = "FAILED"


class DeployKind(str, Enum):
    exec = "EXEC"
    docker = "DOCKER"


class DeploymentCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    desired_state: DesiredState
    kind: DeployKind
    specification: Dict[str, Any] = Field(default_factory=dict)


class DeploymentUpdate(BaseModel):
    name: Optional[str] = Field(default=None, min_length=1, max_length=200)
    desired_state: Optional[DesiredState] = None
    specification: Optional[Dict[str, Any]] = None


class DeploymentResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    name: str
    desired_state: DesiredState
    current_state: Optional[CurrentState] = None
    kind: DeployKind
    specification: Dict[str, Any]
    created_at_ms: int
    updated_at_ms: int
    assigned_node_id: Optional[str] = None
    assignment_reason: Optional[str] = None


class CondaEnvCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    python_version: str = Field(min_length=1, max_length=20)
    packages: List[str] = Field(default_factory=list)
    custom_script: str = Field(default="", max_length=2000)

    @field_validator("packages", mode="before")
    @classmethod
    def _normalize_packages(cls, value):
        if value is None:
            return []
        if isinstance(value, str):
            parts = [p.strip() for p in value.split(",")]
            return [p for p in parts if p]
        return value

    @field_validator("packages")
    @classmethod
    def _validate_packages(cls, value: List[str]) -> List[str]:
        cleaned = [str(p).strip() for p in value if str(p).strip()]
        return cleaned

    @field_validator("custom_script")
    @classmethod
    def _validate_custom_script(cls, value: str) -> str:
        return str(value or "").strip()


class CondaEnvUpdate(BaseModel):
    packages: Optional[List[str]] = None
    custom_script: Optional[str] = Field(default=None, max_length=2000)

    @field_validator("packages", mode="before")
    @classmethod
    def _normalize_packages(cls, value):
        if value is None:
            return None
        if isinstance(value, str):
            parts = [p.strip() for p in value.split(",")]
            return [p for p in parts if p]
        return value

    @field_validator("packages")
    @classmethod
    def _validate_packages(cls, value: Optional[List[str]]) -> Optional[List[str]]:
        if value is None:
            return None
        cleaned = [str(p).strip() for p in value if str(p).strip()]
        return cleaned

    @field_validator("custom_script")
    @classmethod
    def _validate_custom_script(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return str(value).strip()


class CondaEnvResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    name: str
    python_version: str
    packages: List[str]
    custom_script: str
    created_at_ms: int
    updated_at_ms: int


class CpuCoreUsage(BaseModel):
    core_id: int
    used_percent: float


class CpuStatic(BaseModel):
    logical_cores: int
    max_millicores_total: int


class CpuDynamic(BaseModel):
    total_percent: float
    per_core: List[CpuCoreUsage]


class CpuInfo(BaseModel):
    static: CpuStatic
    dynamic: CpuDynamic


class MemoryStatic(BaseModel):
    total_bytes: int


class MemoryDynamic(BaseModel):
    used_bytes: int
    available_bytes: int
    used_percent: float
    free_bytes: int
    buffers_bytes: int
    cached_bytes: int


class MemoryInfo(BaseModel):
    static: MemoryStatic
    dynamic: MemoryDynamic


class StorageMount(BaseModel):
    mount_point: str
    fs_type: str
    total_bytes: int
    used_bytes: int
    available_bytes: int
    used_percent: float


class GpuInfo(BaseModel):
    index: int
    name: str
    mem_total_bytes: int
    util_percent: float
    mem_util_percent: float
    mem_used_bytes: int
    mem_free_bytes: int
    temperature_c: int
    power_w: float


class AssignedDeployment(BaseModel):
    id: str
    name: str


class NodeSnapshot(BaseModel):
    node_id: str
    groups: List[str]

    capacities_total: Dict[str, int]
    total_capacities_used: Dict[str, int]

    last_heartbeat: datetime
    dynamic_timestamp_unix_ms: int

    cpu: CpuInfo
    memory: MemoryInfo
    storage_mounts: List[StorageMount]
    gpus: Optional[List[GpuInfo]] = None
    assigned_deployments: List[AssignedDeployment] = Field(default_factory=list)
    conda_envs: List[str] = Field(default_factory=list)
    schedulable: bool = True
    missing_conda_envs: List[str] = Field(default_factory=list)


class NodesResponse(BaseModel):
    nodes: Dict[str, NodeSnapshot]


class CondaEnvsResponse(BaseModel):
    envs: List[CondaEnvResponse]
