from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from symphony.conductor.api.routes import router_deployment, router_nodes
from symphony.conductor.ui.ui_router import router_ui


def create_app() -> FastAPI:
    app = FastAPI(
        title="Symphony Conductor",
    )
    origins = [
        "http://localhost:8080",
    ]

    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(router_deployment)
    app.include_router(router_nodes)
    app.include_router(router_ui)
    return app
