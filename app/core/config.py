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

    # ------------------------------------------------------------------
    # Phase 2: Execution & Balance Tracking
    # ------------------------------------------------------------------

    rpc_url: str = "http://127.0.0.1:8545"
    """HTTP RPC endpoint for transaction broadcasting (not derivable from WS_URL)."""

    private_key: str = ""
    """Private key for signing transactions (0x-prefixed, 64 hex chars). Validated on worker startup."""

    megaeth_historical_graphql_url: str = ""
    """GraphQL endpoint for historical on-chain data (reserved for Phase 3)."""

    execution_stream: str = "megaeth:stream:executions"
    """Redis Stream for agent execution plans."""

    execution_consumer_group: str = "megaeth:workers:executors"
    """Consumer group for the execution worker pool."""

    execution_dead_letter: str = "megaeth:stream:executions:dead"
    """Dead-letter stream for failed execution plans."""

    worker_pool_count: int = 1
    """Number of worker pool instances. Constrained to 1 in Phase 2 to avoid nonce collisions."""

    max_concurrent_tx: int = 10
    """Maximum concurrent transactions per worker pool."""

    balance_consumer_group: str = "megaeth:workers:balances"
    """Consumer group for the balance tracker worker on the main miniBlocks stream."""


settings = Settings()
