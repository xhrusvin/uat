# User Management API — FastAPI + MongoDB + Docker

A production-ready REST API with JWT authentication and full CRUD for users,
backed by **MongoDB** (`xpress_health_uat`), containerised with Docker Desktop for Windows.

---

## Stack

| Layer | Technology |
|-------|-----------|
| Framework | FastAPI 0.115 |
| ODM | Beanie (async MongoDB ODM) |
| Driver | Motor (async Motor driver) |
| Database | MongoDB 7.0 (`xpress_health_uat`) |
| Auth | JWT via python-jose + bcrypt |
| Container | Docker + Docker Compose |

---

## Prerequisites (Windows)

| Tool | Download |
|------|----------|
| Docker Desktop 4.x+ | https://www.docker.com/products/docker-desktop/ |
| WSL 2 (recommended) | Enable in Docker Desktop → Settings → General |

---

## Quick start (Windows PowerShell)

```powershell
# 1. Unzip and enter the project
cd fastapi-auth-app

# 2. Start MongoDB + API (builds image on first run)
docker compose up --build

# 3. Open Swagger UI
start http://localhost:8000/docs
```

The API runs on **http://localhost:8000**  
MongoDB is exposed on **localhost:27017** (connect with MongoDB Compass or mongosh)

---

## Common Docker commands

```powershell
docker compose up -d            # run in background
docker compose logs -f api      # live API logs
docker compose logs -f mongo    # live MongoDB logs
docker compose down             # stop (data kept in volume)
docker compose down -v          # stop + wipe MongoDB data
docker compose up --build       # rebuild after a code change
```

---

## API endpoints

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `POST` | `/users/` | — | Register a new user |
| `POST` | `/auth/login` | — | Login → JWT token |
| `GET` | `/auth/me` | ✅ | Current user profile |
| `GET` | `/users/` | ✅ | List users (paginated + search) |
| `GET` | `/users/{id}` | ✅ | Get user by ObjectId |
| `PATCH` | `/users/{id}` | ✅ | Update user |
| `DELETE` | `/users/{id}` | ✅ | Delete user |

### Try it in Swagger

1. `POST /users/` — register
2. `POST /auth/login` — copy `access_token`
3. Click **Authorize 🔒** → enter `Bearer <token>`
4. Call any protected endpoint

---

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MONGODB_URI` | `mongodb://mongo:27017` | MongoDB connection string |
| `MONGODB_DB` | `xpress_health_uat` | Database name |
| `SECRET_KEY` | *(set in .env.docker)* | JWT signing secret |
| `ALGORITHM` | `HS256` | JWT algorithm |
| `ACCESS_TOKEN_EXPIRE_MINUTES` | `30` | Token lifetime |

---

## Project structure

```
fastapi-auth-app/
├── app/
│   ├── main.py              # FastAPI app, lifespan (MongoDB connect/disconnect)
│   ├── core/
│   │   ├── config.py        # Settings — reads MONGODB_URI, MONGODB_DB, SECRET_KEY
│   │   └── security.py      # bcrypt hashing + JWT helpers
│   ├── db/
│   │   └── database.py      # Motor client + Beanie init
│   ├── models/
│   │   └── user.py          # Beanie Document (maps to `users` collection)
│   ├── schemas/
│   │   └── user.py          # Pydantic request / response schemas
│   └── routers/
│       ├── auth.py          # POST /auth/login, GET /auth/me
│       └── users.py         # CRUD /users/
├── tests/
│   └── test_users.py        # 12 tests — uses mongomock-motor (no real DB needed)
├── Dockerfile               # Multi-stage, non-root user
├── docker-compose.yml       # api + mongo services
├── .env                     # Local dev env vars
├── .env.docker              # Docker env vars template
├── pytest.ini
└── requirements.txt
```
