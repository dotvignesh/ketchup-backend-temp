"""Application settings from environment variables."""

from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = "postgresql://postgres:postgres@localhost:5433/appdb"
    vllm_base_url: str = "http://localhost:8080/v1"
    vllm_model: str = "Qwen/Qwen3-4B-Instruct-2507"
    vllm_api_key: str = "EMPTY"
    vllm_connect_timeout_seconds: float = 10.0
    vllm_read_timeout_seconds: float = 180.0
    vllm_write_timeout_seconds: float = 20.0
    vllm_pool_timeout_seconds: float = 10.0
    vllm_max_connections: int = 100
    vllm_max_keepalive_connections: int = 20
    planner_novelty_target_generate: float = 0.7
    planner_novelty_target_refine: float = 0.35
    planner_fallback_enabled: bool = False
    backend_internal_api_key: str = ""
    google_maps_api_key: str = ""
    tavily_api_key: str = ""
    cors_origins: list[str] = [
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:3001",
        "http://127.0.0.1:3001",
    ]

    smtp_host: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""
    smtp_from_email: str | None = None

    frontend_url: str = "http://localhost:3001"



@lru_cache
def get_settings() -> Settings:
    return Settings()
