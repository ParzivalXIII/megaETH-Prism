
import httpx

from app.core.config import settings
from app.core.logging import get_logger
import app.core.metrics as metrics_

logger = get_logger("megaeth.vector.embedder")


class CohereEmbedder:
    """Client for the cohere-embed self-hosted Docker service.

    Uses a manual dict-based cache (NOT lru_cache on async functions).
    """

    def __init__(self, base_url: str | None = None):
        self._base_url = (base_url or settings.cohere_embed_url).rstrip("/")
        self._cache: dict[str, list[float]] = {}
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=30.0)
        return self._client

    async def embed(self, text: str) -> list[float]:
        """Embed a text string, returning a 1024-dim vector.

        Uses a local dict-based cache for deterministic summaries.
        """
        # Check cache first
        if text in self._cache:
            metrics_.embeddings_cached += 1
            return self._cache[text]

        metrics_.embeddings_computed += 1

        # Call cohere-embed service
        client = await self._get_client()

        # Try known embedding endpoints in order
        # The actual endpoint may differ; we try the most common ones
        endpoints_to_try = [
            {"path": "/v1/embeddings", "json": {"model": settings.embedding_model, "texts": [text], "input_type": "search_document"}},
            {"path": "/embeddings", "json": {"model": settings.embedding_model, "texts": [text], "input_type": "search_document"}},
            {"path": "/embed", "json": {"texts": [text]}},
        ]

        last_error: Exception | None = None
        result = None
        success_path = None
        for attempt in endpoints_to_try:
            try:
                response = await client.post(
                    f"{self._base_url}{attempt['path']}",
                    json=attempt["json"],
                )
                response.raise_for_status()
                result = response.json()
                success_path = attempt["path"]
                break
            except Exception as e:
                last_error = e
                continue

        if result is None:
            raise RuntimeError("embedding failed on all endpoints") from last_error

        if success_path != endpoints_to_try[0]["path"]:
            logger.debug("embedding endpoint selected", path=success_path)

        # Handle both Cohere and OpenAI-compatible response formats
        if "data" in result:
            # OpenAI-compatible: {"data": [{"embedding": [...], "index": 0}]}
            embedding = result["data"][0]["embedding"]
        elif "embeddings" in result:
            # Cohere-compatible: {"embeddings": [[...]]}
            embedding = result["embeddings"][0]
        elif "embedding" in result:
            # Single embedding response: {"embedding": [...]}
            embedding = result["embedding"]
        else:
            raise ValueError(f"Unknown embed response format: {list(result.keys())}")

        # Validate dimension
        if len(embedding) != settings.embedding_dimensions:
            logger.warning(
                "unexpected embedding dimension",
                expected=settings.embedding_dimensions,
                got=len(embedding),
                model=settings.embedding_model,
            )

        # Cache and return
        vec = [float(v) for v in embedding]
        self._cache[text] = vec
        return vec

    async def health_check(self) -> bool:
        """Check if the embed service is available."""
        try:
            client = await self._get_client()
            resp = await client.get(f"{self._base_url}/health", timeout=5.0)
            return resp.status_code == 200
        except Exception:
            return False

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None


async def check_embedding_endpoint(embedder: CohereEmbedder) -> str | None:
    """Discover the correct embedding endpoint path.

    Tries common endpoints and returns the one that works.
    """
    client = await embedder._get_client()
    endpoints = ["/v1/embeddings", "/embeddings", "/embed", "/api/embeddings", "/api/v1/embeddings"]
    test_text = "health check embedding endpoint"

    for endpoint in endpoints:
        try:
            resp = await client.post(
                f"{embedder._base_url}{endpoint}",
                json={"texts": [test_text], "model": settings.embedding_model},
                timeout=5.0,
            )
            if resp.status_code in (200, 201):
                logger.info("embedding endpoint discovered", endpoint=endpoint)
                return endpoint
        except Exception:
            continue

    logger.warning("no embedding endpoint found, using default")
    return "/v1/embeddings"
