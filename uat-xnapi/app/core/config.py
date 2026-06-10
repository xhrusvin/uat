from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    API_KEY: str = "xh-uat-9f4a2c8b1d6e3f7a0b5c9d2e4f8a1b3c"

    MONGODB_URI: str = "mongodb://localhost:27017"
    MONGODB_DB: str = "xpress_health_uat"


settings = Settings()
