"""
Integration tests — API key Bearer auth + mongomock-motor.
Run with: pytest tests/ -v
"""
import pytest
import pytest_asyncio
from beanie import init_beanie
from fastapi.testclient import TestClient
from mongomock_motor import AsyncMongoMockClient

from app.core.config import settings
from app.main import app
from app.models.user import User

# ── In-memory MongoDB ─────────────────────────────────────────────────────────

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

# Valid auth header using the test API key
AUTH = {"Authorization": f"Bearer {settings.API_KEY}"}
BAD_AUTH = {"Authorization": "Bearer wrong-key"}


# ── Helpers ───────────────────────────────────────────────────────────────────

def create_user(email="alice@example.com", password="Password1",
                first_name="Alice", last_name="Smith"):
    return client.post("/users/", json={
        "email": email, "password": password,
        "first_name": first_name, "last_name": last_name,
    }, headers=AUTH)


# ── Auth tests ────────────────────────────────────────────────────────────────

def test_no_api_key_returns_401():
    r = client.get("/users/")
    assert r.status_code == 401


def test_wrong_api_key_returns_401():
    r = client.get("/users/", headers=BAD_AUTH)
    assert r.status_code == 401


def test_valid_api_key_passes():
    r = client.get("/users/", headers=AUTH)
    assert r.status_code == 200


# ── CRUD tests ────────────────────────────────────────────────────────────────

def test_create_user():
    r = create_user()
    assert r.status_code == 201
    data = r.json()
    assert data["email"] == "alice@example.com"
    assert data["first_name"] == "Alice"
    assert "password" not in data


def test_duplicate_email():
    create_user()
    assert create_user().status_code == 409


def test_list_users():
    create_user()
    create_user("bob@example.com", first_name="Bob", last_name="Jones")
    r = client.get("/users/", headers=AUTH)
    assert r.status_code == 200
    assert r.json()["total"] == 2


def test_list_users_search():
    create_user()
    create_user("bob@example.com", first_name="Bob", last_name="Jones")
    r = client.get("/users/?search=bob", headers=AUTH)
    assert r.status_code == 200
    assert r.json()["total"] == 1


def test_get_user_by_id():
    user_id = create_user().json()["id"]
    r = client.get(f"/users/{user_id}", headers=AUTH)
    assert r.status_code == 200
    assert r.json()["email"] == "alice@example.com"


def test_get_user_invalid_id():
    r = client.get("/users/invalid-id", headers=AUTH)
    assert r.status_code == 422


def test_get_user_not_found():
    r = client.get("/users/507f1f77bcf86cd799439011", headers=AUTH)
    assert r.status_code == 404


def test_update_user():
    user_id = create_user().json()["id"]
    r = client.patch(f"/users/{user_id}", json={"first_name": "Alicia"}, headers=AUTH)
    assert r.status_code == 200
    assert r.json()["first_name"] == "Alicia"


def test_delete_user():
    user_id = create_user().json()["id"]
    assert client.delete(f"/users/{user_id}", headers=AUTH).status_code == 204
    assert client.get(f"/users/{user_id}", headers=AUTH).status_code == 404
