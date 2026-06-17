from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from app.db.database import close_db, connect_db
from app.routers import auth, users, shifts, shifts_db, common, clients, recruitments, criteria, shift_users, staff, sequences, outreach, user_types
from app.routers.user_types import county_router

limiter = Limiter(key_func=get_remote_address)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await connect_db()
    yield
    await close_db()


app = FastAPI(
    title="XpressHealth User API",
    openapi_url="/openapi.json",
    docs_url="/docs",
    redoc_url="/redoc",
    root_path="/xnapi",
    root_path_in_servers=False,
    servers=[
        {"url": "https://uat.expresshealth.ie/xnapi", "description": "UAT Server"},
        {"url": "http://127.0.0.1:8050", "description": "Local Server"},
    ],
    description=(
        "XpressHealth User Management API\n\n"
        "## Authentication\n"
        "- **API Key**: `Authorization: Bearer <api-key>`\n"
        "- **JWT** (`/auth/`): Login via `/auth/login`\n"
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router)
app.include_router(users.router)
app.include_router(shifts.router)
app.include_router(shifts_db.router)
app.include_router(common.router)
app.include_router(clients.router)
app.include_router(recruitments.router)
app.include_router(criteria.router)
app.include_router(shift_users.router)
app.include_router(staff.router)
app.include_router(sequences.router)
app.include_router(outreach.router)
app.include_router(user_types.router)
app.include_router(county_router)


@app.get("/", tags=["Health"])
async def root():
    return {"status": "ok", "message": "XpressHealth User API — xpress_health_uat"}
