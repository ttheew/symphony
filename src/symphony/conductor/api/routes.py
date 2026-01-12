from __future__ import annotations

from fastapi import APIRouter, HTTPException, status

from symphony.conductor import deployment_store
from symphony.conductor.deployment_assignment_registry import (
    DeploymentAssignmentRegistry,
)
from symphony.conductor.models import (
    CurrentState,
    DeploymentCreate,
    DeploymentResponse,
    DeploymentUpdate,
    NodesResponse,
)
from symphony.conductor.node_registry import NodeRegistry
from symphony.conductor.service import ConductorService

node_registry = NodeRegistry()
deployment_ass_registry = DeploymentAssignmentRegistry()
router_deployment = APIRouter(prefix="/deployments", tags=["deployments"])
router_nodes = APIRouter(prefix="/nodes", tags=["nodes"])

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
    limit = max(1, min(limit, 500))
    offset = max(0, offset)
    deployment_list = await deployment_store.list(limit=limit, offset=offset)
    for deployment in deployment_list:
        deployment.current_state = CurrentState.pending
        try:
            deployment_status = await deployment_ass_registry.get_status(deployment.id)
            deployment.current_state = CurrentState(deployment_status.status)
        except Exception:
            pass
    return deployment_list


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
    if "desired_state" in patch_json:
        await svc.send_deployment_change(
            node_id, deployment_id, "desired_state", patch_json["desired_state"]
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
    snapshot = await node_registry.combined_snapshot()

    return {"nodes": snapshot}
