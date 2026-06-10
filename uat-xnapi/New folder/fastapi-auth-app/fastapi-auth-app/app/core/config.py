from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # API key auth (for external consumers)
    API_KEY: str = "xh-uat-9f4a2c8b1d6e3f7a0b5c9d2e4f8a1b3c"

    # JWT auth (for admin panel)
    SECRET_KEY: str = "xh-jwt-secret-7f3a9c2d1e8b4f6a0c5d3e7b9f2a4c8d"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60

    MONGODB_URI: str = "mongodb://localhost:27017"
    MONGODB_DB: str = "xpress_health_uat"


settings = Settings()
