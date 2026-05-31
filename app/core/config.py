from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # MegaETH / RPC connection
    ws_url: str = "ws://127.0.0.1:8545"  # Anvil default; set to wss://carrot.megaeth.com/ws for testnet
    chain_id: int = 31337  # Anvil default; set to 6343 for testnet
    subscription_type: Literal["auto", "miniBlocks", "newHeads"] = "auto"
    """Which RPC subscription to use.
    - auto: try miniBlocks (MegaETH), fall back to newHeads (standard EVM)
    - miniBlocks: force MegaETH mini-block subscription
    - newHeads: force standard new block header subscription
    """

    # Infrastructure
    redis_url: str = "redis://127.0.0.1:6379/0"
    postgres_url: str = "postgresql+asyncpg://megaeth:megaeth@127.0.0.1:5432/megaeth"
    qdrant_url: str = "http://127.0.0.1:6333"
    cohere_embed_url: str = "http://127.0.0.1:8000"

    # Redis Stream
    stream_name: str = "megaeth:raw:miniBlocks"
    stream_maxlen: int = 500_000
    consumer_group_postgres: str = "megaeth:workers:postgres"
    consumer_group_qdrant: str = "megaeth:workers:qdrant"
    dead_letter_stream: str = "megaeth:raw:miniBlocks:dead"

    # Daemon
    keepalive_interval_sec: int = 30
    reconnect_backoff_base: float = 1.0
    reconnect_backoff_cap: float = 16.0

    # Runtime
    log_level: str = "INFO"
    docker_env: bool = False

    # Embeddings
    embedding_dimensions: int = 1024
    embedding_model: str = "embed-english-v3.0"


settings = Settings()
