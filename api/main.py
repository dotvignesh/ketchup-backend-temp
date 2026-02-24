"""FastAPI application entry point."""

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api.routes import (
    auth,
    availability,
    availability_group,
    feedback,
    groups,
    internal_analytics,
    plans,
    users,
)
from analytics.bootstrap import ensure_analytics_schema
from agents.planning import close_planner_client, init_planner_client
from config import get_settings
from database import db
from services.errors import ServiceError
from utils.invite_expiry import expire_stale_invites_loop


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.connect()
    await ensure_analytics_schema()
    await init_planner_client()

    expiry_task = asyncio.create_task(expire_stale_invites_loop())

    yield

    expiry_task.cancel()
    try:
        await expiry_task
    except asyncio.CancelledError:
        pass
    await close_planner_client()
    await db.disconnect()


app = FastAPI(
    title="Ketchup API",
    description="Group planning backend API",
    version="0.1.0",
    lifespan=lifespan,
)

settings = get_settings()
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router)
app.include_router(users.router)
app.include_router(groups.router)
app.include_router(plans.router)
app.include_router(availability.router)
app.include_router(availability_group.router)
app.include_router(feedback.router)
app.include_router(internal_analytics.router)


@app.exception_handler(ServiceError)
async def handle_service_error(_: Request, exc: ServiceError) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/")
async def root():
    return {"message": "Ketchup API", "docs": "/docs"}
