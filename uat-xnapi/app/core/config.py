from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    API_KEY: str = "change-me-in-production"

    MONGODB_URI: str = "mongodb://localhost:27017"
    MONGODB_DB: str = "xpress_health_uat"


settings = Settings()
