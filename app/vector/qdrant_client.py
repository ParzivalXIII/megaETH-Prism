from qdrant_client import QdrantClient as SyncQdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams

from app.core.config import settings
from app.core.logging import get_logger
from app.schemas.vector import VectorPayload

logger = get_logger("megaeth.vector.qdrant")


class QdrantManager:
    """Manages Qdrant collection lifecycle and upsert operations."""

    COLLECTION_NAME = "megaeth_blocks"

    def __init__(self):
        self._client: SyncQdrantClient | None = None

    def _get_client(self) -> SyncQdrantClient:
        if self._client is None:
            self._client = SyncQdrantClient(
                url=settings.qdrant_url,
                timeout=30,
            )
        return self._client

    def initialize(self) -> None:
        """Create the collection if it doesn't exist (recreate=False to prevent data loss)."""
        client = self._get_client()
        collections = [c.name for c in client.get_collections().collections]

        if self.COLLECTION_NAME not in collections:
            client.create_collection(
                collection_name=self.COLLECTION_NAME,
                vectors_config=VectorParams(
                    size=settings.embedding_dimensions,
                    distance=Distance.COSINE,
                ),
            )
            logger.info(
                "collection created",
                collection=self.COLLECTION_NAME,
                dimensions=settings.embedding_dimensions,
            )
        else:
            logger.debug("collection already exists", collection=self.COLLECTION_NAME)

    def upsert(self, payload: VectorPayload) -> None:
        """Upsert a vector point. Idempotent — same payload_id overwrites."""
        client = self._get_client()
        point = PointStruct(
            id=payload.payload_id,
            vector=payload.embedding,
            payload={
                "block_number": payload.block_number,
                "summary": payload.summary,
                **payload.metadata,
            },
        )
        client.upsert(
            collection_name=self.COLLECTION_NAME,
            points=[point],
        )

    def search(self, vector: list[float], limit: int = 10) -> list:
        """Search for similar mini-block vectors using cosine similarity."""
        client = self._get_client()
        results = client.search(  # type: ignore[attr-defined]
            collection_name=self.COLLECTION_NAME,
            query_vector=vector,
            limit=limit,
        )
        return results

    def count(self) -> int:
        """Get the number of points in the collection."""
        client = self._get_client()
        result = client.count(collection_name=self.COLLECTION_NAME)
        return result.count

    def close(self) -> None:
        if self._client:
            self._client.close()
            self._client = None
