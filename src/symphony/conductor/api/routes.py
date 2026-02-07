from __future__ import annotations

import asyncio

from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect, status
from loguru import logger

from symphony.conductor import conda_env_store, deployment_store
from symphony.conductor.deployment_assignment_registry import (
    DeploymentAssignmentRegistry,
)
from symphony.conductor.models import (
    CurrentState,
    CondaEnvCreate,
    CondaEnvResponse,
    DeploymentCreate,
    DeploymentResponse,
    DeploymentUpdate,
    NodesResponse,
)
from symphony.conductor.node_registry import NodeRegistry
from symphony.conductor.service import ConductorService
from symphony.v1 import protocol_pb2

node_registry = NodeRegistry()
deployment_ass_registry = DeploymentAssignmentRegistry()
router_deployment = APIRouter(prefix="/deployments", tags=["deployments"])
router_nodes = APIRouter(prefix="/nodes", tags=["nodes"])
router_conda_envs = APIRouter(prefix="/conda-envs", tags=["conda-envs"])
router_stream = APIRouter(tags=["stream"])

svc = ConductorService()


@router_deployment.post(
    "", response_model=DeploymentResponse, status_code=status.HTTP_201_CREATED
)
async def create_deployment(payload: DeploymentCreate) -> DeploymentResponse:
    return await deployment_store.create(payload)


@router_deployment.get("", response_model=list[DeploymentResponse])
async def list_deployments(
    limit: int = 100, offset: int = 0
) -> list[DeploymentResponse]:
    return await _deployment_snapshot(limit=limit, offset=offset)


async def _deployment_snapshot(
    *, limit: int = 100, offset: int = 0
) -> list[DeploymentResponse]:
    limit = max(1, min(limit, 500))
    offset = max(0, offset)
    deployment_list = await deployment_store.list(limit=limit, offset=offset)
    node_snapshot = await node_registry.snapshot_records()
    conda_envs = await conda_env_store.list_all()
    required_names = {env.name for env in conda_envs}
    for deployment in deployment_list:
        deployment.assigned_node_id = await deployment_ass_registry.get_node(deployment.id)
        deployment.current_state = CurrentState.pending
        try:
            deployment_status = await deployment_ass_registry.get_status(deployment.id)
            deployment.current_state = CurrentState(deployment_status.status)
        except Exception:
            pass
        if not deployment.assigned_node_id:
            deployment.assignment_reason = _compute_assignment_reason(
                deployment,
                node_snapshot=node_snapshot,
                required_names=required_names,
            )
    return deployment_list


def _compute_assignment_reason(
    deployment: DeploymentResponse,
    *,
    node_snapshot: dict,
    required_names: set[str],
) -> str:
    if not node_snapshot:
        return "No Node"

    spec = (deployment.specification or {}).get("spec") or {}
    spec_config = spec.get("config") if isinstance(spec, dict) else None
    env_name = None
    if isinstance(spec_config, dict):
        candidate = spec_config.get("env_name")
        if isinstance(candidate, str) and candidate.strip():
            env_name = candidate.strip()

    required_for_deployment = set(required_names)
    if env_name:
        if required_names and env_name not in required_names:
            return "No Env"
        if not required_names:
            required_for_deployment.add(env_name)

    if required_for_deployment:
        has_env_node = any(
            required_for_deployment.issubset(set(rec.conda_envs or []))
            for rec in node_snapshot.values()
        )
        if not has_env_node:
            return "No Env"

    capacity_request = spec.get("capacity_requests") or {}
    if capacity_request:
        for rec in node_snapshot.values():
            capacity_total = rec.capacities_total or {}
            used = getattr(rec.dynamic, "total_capacities_used", None) or {}
            ok = True
            for cap_id, req_amount in capacity_request.items():
                total = int(capacity_total.get(cap_id, 0))
                used_amt = int(used.get(cap_id, 0))
                available = total - used_amt
                if available < int(req_amount):
                    ok = False
                    break
            if ok:
                return "Pending"
        return "No Capacity"

    return "Pending"


async def _nodes_snapshot() -> dict:
    snapshot = await node_registry.combined_snapshot()
    deployments = await deployment_store.list_all()
    conda_envs = await conda_env_store.list_all()
    required_env_names = [env.name for env in conda_envs]
    deployment_names = {dep.id: dep.name for dep in deployments}
    for node_id, node in snapshot.items():
        deployment_ids = await deployment_ass_registry.get_deployments(node_id)
        node["assigned_deployments"] = [
            {"id": dep_id, "name": deployment_names.get(dep_id, dep_id)}
            for dep_id in deployment_ids
        ]
        node_envs = set(node.get("conda_envs") or [])
        missing_envs = [name for name in required_env_names if name not in node_envs]
        node["missing_conda_envs"] = missing_envs
        node["schedulable"] = len(missing_envs) == 0
    return snapshot


@router_deployment.get("/{deployment_id}", response_model=DeploymentResponse)
async def get_deployment(deployment_id: str) -> DeploymentResponse:
    dep = await deployment_store.get(deployment_id)
    if not dep:
        raise HTTPException(status_code=404, detail="Deployment not found")
    return dep


@router_deployment.patch("/{deployment_id}", response_model=DeploymentResponse)
async def update_deployment(
    deployment_id: str, patch: DeploymentUpdate
) -> DeploymentResponse:
    dep = await deployment_store.update(deployment_id, patch)
    patch_json = patch.model_dump(exclude_none=True)
    if not dep:
        raise HTTPException(status_code=404, detail="Deployment not found")
    try:
        node_id = await deployment_ass_registry.get_node(deployment_id)
    except Exception:
        print("Change not sent to node")
        return dep
    if not node_id:
        return dep
    if "desired_state" in patch_json:
        await svc.send_deployment_change(
            node_id, deployment_id, "desired_state", patch_json["desired_state"]
        )
    if "specification" in patch_json:
        await svc.send_message(
            node_id,
            protocol_pb2.ConductorToNode(
                deployment_req=protocol_pb2.DeploymentReq(
                    specification=dep.model_dump_json()
                )
            ),
        )
    return dep


@router_deployment.delete("/{deployment_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_deployment(deployment_id: str) -> None:
    ok = await deployment_store.delete(deployment_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Deployment not found")


@router_nodes.get(
    "",
    response_model=NodesResponse,
    summary="List connected nodes with full resource snapshot",
)
async def list_nodes():
    return {"nodes": await _nodes_snapshot()}


@router_conda_envs.post(
    "",
    response_model=CondaEnvResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_conda_env(payload: CondaEnvCreate) -> CondaEnvResponse:
    try:
        env = await conda_env_store.create(payload)
    except Exception as exc:
        if "UNIQUE constraint failed" in str(exc):
            raise HTTPException(
                status_code=409, detail="Conda env with that name already exists"
            ) from exc
        raise
    await svc.ensure_envs_on_all_nodes([env])
    return env


@router_conda_envs.get("", response_model=list[CondaEnvResponse])
async def list_conda_envs(
    limit: int = 100, offset: int = 0
) -> list[CondaEnvResponse]:
    limit = max(1, min(limit, 500))
    offset = max(0, offset)
    return await conda_env_store.list(limit=limit, offset=offset)


@router_conda_envs.delete("/{env_name}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_conda_env(env_name: str) -> None:
    ok = await conda_env_store.delete(env_name)
    if not ok:
        raise HTTPException(status_code=404, detail="Conda env not found")


@router_stream.websocket("/ws/updates")
async def stream_updates(websocket: WebSocket) -> None:
    await websocket.accept()
    logger.info("Websocket client connected for updates")
    try:
        while True:
            deployments = await _deployment_snapshot(limit=500, offset=0)
            nodes = await _nodes_snapshot()
            await websocket.send_json(
                {
                    "type": "snapshot",
                    "deployments": [d.model_dump(mode="json") for d in deployments],
                    "nodes": nodes,
                }
            )
            await asyncio.sleep(1.0)
    except asyncio.CancelledError:
        logger.info("Websocket updates stream cancelled")
        return
    except WebSocketDisconnect:
        logger.info("Websocket client disconnected from updates stream")


@router_stream.websocket("/ws/deployments/{deployment_id}/logs")
async def stream_deployment_logs(websocket: WebSocket, deployment_id: str) -> None:
    await websocket.accept()
    node_id = await deployment_ass_registry.get_node(deployment_id)
    if not node_id:
        await websocket.send_json(
            {"deployment_id": deployment_id, "entries": [], "error": "Deployment not assigned"}
        )
        await websocket.close(code=1008)
        return

    query = websocket.query_params
    try:
        tail = int(query.get("tail", "200"))
    except Exception:
        tail = 200
    streams_param = query.get("streams")
    streams = [x.strip() for x in streams_param.split(",") if x.strip()] if streams_param else []

    queue = await svc.subscribe_deployment_logs(
        node_id=node_id,
        deployment_id=deployment_id,
        since_ms=0,
        tail=max(0, tail),
        streams=streams,
    )

    logger.info("Started log stream deployment_id={} node_id={}", deployment_id, node_id)
    try:
        while True:
            payload = await queue.get()
            await websocket.send_json(payload)
    except asyncio.CancelledError:
        logger.info("Deployment logs websocket cancelled deployment_id={}", deployment_id)
    except WebSocketDisconnect:
        logger.info("Deployment logs websocket disconnected deployment_id={}", deployment_id)
    finally:
        await svc.unsubscribe_deployment_logs(
            node_id=node_id,
            deployment_id=deployment_id,
            queue=queue,
        )
