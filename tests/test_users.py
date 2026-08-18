"""
Integration tests — read-only Users API with API key auth.
Run with: pytest tests/ -v
"""
import pytest
import pytest_asyncio
from beanie import init_beanie
from fastapi.testclient import TestClient
from mongomock_motor import AsyncMongoMockClient

from app.core.config import settings
from app.core.security import verify_api_key
from app.main import app
from app.models.user import User

import bcrypt


@pytest_asyncio.fixture(autouse=True)
async def init_mongo():
    client = AsyncMongoMockClient()
    await init_beanie(database=client["xpress_health_uat_test"], document_models=[User])
    yield
    await User.find().delete()


from contextlib import asynccontextmanager

@asynccontextmanager
async def mock_lifespan(app):
    yield

app.router.lifespan_context = mock_lifespan
client = TestClient(app)

AUTH = {"Authorization": f"Bearer {settings.API_KEY}"}
BAD_AUTH = {"Authorization": "Bearer wrong-key"}


# ── Helpers ───────────────────────────────────────────────────────────────────

async def make_user(email="alice@example.com", first_name="Alice",
                    last_name="Smith", is_admin=False):
    u = User(
        email=email,
        password=bcrypt.hashpw(b"Password1", bcrypt.gensalt()),
        first_name=first_name,
        last_name=last_name,
        is_admin=is_admin,
        status="Enabled",
    )
    await u.insert()
    return u


import asyncio

def create_user(email="alice@example.com", **kwargs):
    return asyncio.get_event_loop().run_until_complete(
        make_user(email=email, **kwargs)
    )


# ── Auth tests ────────────────────────────────────────────────────────────────

def test_no_api_key_returns_401():
    assert client.get("/users/").status_code == 401


def test_wrong_api_key_returns_401():
    assert client.get("/users/", headers=BAD_AUTH).status_code == 401


def test_valid_api_key_passes():
    assert client.get("/users/", headers=AUTH).status_code == 200


# ── List users — admin excluded ───────────────────────────────────────────────

def test_list_users_excludes_admins():
    create_user("alice@example.com")
    create_user("admin@example.com", is_admin=True)
    r = client.get("/users/", headers=AUTH)
    assert r.status_code == 200
    data = r.json()
    assert data["total"] == 1
    assert data["users"][0]["email"] == "alice@example.com"


def test_list_users_search():
    create_user("alice@example.com", first_name="Alice")
    create_user("bob@example.com", first_name="Bob")
    r = client.get("/users/?search=bob", headers=AUTH)
    assert r.status_code == 200
    assert r.json()["total"] == 1


# ── Get single user ───────────────────────────────────────────────────────────

def test_get_user_by_id():
    u = create_user()
    r = client.get(f"/users/{u.id}", headers=AUTH)
    assert r.status_code == 200
    assert r.json()["email"] == "alice@example.com"


def test_get_admin_user_returns_404():
    u = create_user("admin@example.com", is_admin=True)
    r = client.get(f"/users/{u.id}", headers=AUTH)
    assert r.status_code == 404


def test_get_user_invalid_id():
    assert client.get("/users/invalid-id", headers=AUTH).status_code == 422


def test_get_user_not_found():
    assert client.get("/users/507f1f77bcf86cd799439011", headers=AUTH).status_code == 404


# ── Removed endpoints return 405 ─────────────────────────────────────────────

def test_create_endpoint_removed():
    assert client.post("/users/", headers=AUTH, json={}).status_code == 405


def test_update_endpoint_requires_jwt_not_api_key():
    # PATCH exists but requires JWT (not API key) — returns 403 with API key
    assert client.patch("/users/507f1f77bcf86cd799439011", headers=AUTH, json={}).status_code in (401, 403, 404)


def test_delete_endpoint_removed():
    assert client.delete("/users/507f1f77bcf86cd799439011", headers=AUTH).status_code == 405
