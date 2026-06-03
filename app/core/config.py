from typing import Literal

from pydantic import field_validator
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

    # ------------------------------------------------------------------
    # Phase 3: LangGraph Agent Configuration
    # ------------------------------------------------------------------

    opencode_go_api_key: str = ""
    """API key for the OpenCode Go LLM endpoint."""

    llm_model: str = "deepseek-v4-flash"
    """LLM model identifier for the agent."""

    llm_base_url: str = "https://opencode.ai/zen/go/v1"
    """Base URL for the LLM API (langchain appends /chat/completions)."""

    llm_temperature: float = 0.0
    """LLM temperature for agent reasoning."""

    llm_max_tokens: int = 4096
    """Maximum tokens per LLM response."""

    subgraph_type: Literal["uniswap_v3", "envio_hyperindex"] = "uniswap_v3"
    """Which subgraph backend to query for historical data."""

    agent_poll_interval_sec: int = 60
    """Polling interval when the agent is actively producing payloads."""

    agent_poll_idle_backoff_sec: int = 300
    """Polling interval when the agent has been idle (no payloads)."""

    agent_max_tool_calls: int = 10
    """Maximum tool calls per agent invocation (recursion guard)."""

    agent_calldata_allowlist_csv: str = "0xa9059cbb,0x095ea7b3,0x38ed1739"
    """Comma-separated known-safe function selectors allowed for dispatch (transfer, approve, swap)."""

    default_eth_price: float = 3000.0
    """Fallback ETH/USD price when oracle is unavailable."""

    default_usdc_price: float = 1.0
    """Fallback USDC/USD price (stablecoin peg)."""

    # ------------------------------------------------------------------
    # Validators
    # ------------------------------------------------------------------

    @field_validator("subgraph_type")
    @classmethod
    def _validate_subgraph_type(cls, v: str) -> str:
        """Ensure subgraph_type is in the allowed set."""
        allowed = {"uniswap_v3", "envio_hyperindex"}
        if v not in allowed:
            raise ValueError(
                f"subgraph_type must be one of {allowed}, got {v!r}"
            )
        return v

    @property
    def agent_calldata_allowlist(self) -> list[str]:
        """Parse the CSV ``agent_calldata_allowlist_csv`` into a list of hex selectors."""
        return [s.strip() for s in self.agent_calldata_allowlist_csv.split(",") if s.strip()]

    @field_validator("agent_poll_interval_sec")
    @classmethod
    def _validate_agent_poll_interval(cls, v: int) -> int:
        """Ensure poll interval is at least 10 seconds."""
        if v < 10:
            raise ValueError(
                f"agent_poll_interval_sec must be >= 10, got {v}"
            )
        return v


settings = Settings()
