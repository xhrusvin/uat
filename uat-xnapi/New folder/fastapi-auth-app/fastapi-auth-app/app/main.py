from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.db.database import close_db, connect_db
from app.routers import auth, users


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
        "- **API Key** (`/users/` endpoints): `Authorization: Bearer <api-key>`\n"
        "- **JWT** (`/auth/` endpoints): Login via `/auth/login`, then use the returned token\n"
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router)
app.include_router(users.router)


@app.get("/", tags=["Health"])
async def root():
    return {"status": "ok", "message": "XpressHealth User API — xpress_health_uat"}
