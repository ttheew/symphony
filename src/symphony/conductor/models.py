from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


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


class NodesResponse(BaseModel):
    nodes: Dict[str, NodeSnapshot]
