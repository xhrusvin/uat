from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # ── MongoDB ───────────────────────────────────────────────────────────────
    MONGODB_URI: str = "mongodb://localhost:27017"
    MONGODB_DB: str = "xpress_health_uat"

    # ── Auth ──────────────────────────────────────────────────────────────────
    API_KEY: str = "xh-uat-9f4a2c8b1d6e3f7a0b5c9d2e4f8a1b3c"
    SECRET_KEY: str = "xh-jwt-secret-7f3a9c2d1e8b4f6a0c5d3e7b9f2a4c8d"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60

    # ── Ports ─────────────────────────────────────────────────────────────────
    API_PORT: int = 8050
    ADMIN_PORT: int = 8051

    # ── User API ──────────────────────────────────────────────────────────────
    USER_API_URL: str = "https://uat.user-xpresshealth.webc.in/api/"
    USER_INTERNAL_API_KEY: str = "HGvB67Hju7TGV8KA8Qah678HIkNH7M"
    USER_EXTERNAL_API_KEY: str = ""
    APP_COUNTRY: str = "ie"

    # ── Shift API ─────────────────────────────────────────────────────────────
    SHIFT_URL: str = "https://uat.shift-xpresshealth.webc.in/api/"
    SHIFT_INTERNAL_API_KEY: str = "ZDGnj76YJDfg56Ij7YNBkm7Yvfh67B"


settings = Settings()
