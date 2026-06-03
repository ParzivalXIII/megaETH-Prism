"""LangGraph tool for semantic search over cognitive memory (Qdrant vector DB).

The ``search_cognitive_memory`` tool embeds a text query via CohereEmbedder,
searches Qdrant for similar mini-block summaries, and returns structured results.

Qdrant is a **sync client** — all Qdrant calls are wrapped in ``asyncio.to_thread()``
to avoid blocking the event loop.
"""

from __future__ import annotations

import asyncio
from typing import Any

from langchain_core.tools import tool

from app.core import metrics
from app.core.logging import get_logger

logger = get_logger("megaeth.agents.tools.memory")


@tool
async def search_cognitive_memory(
    query: str,
    limit: int = 10,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Search cognitive memory (Qdrant vector DB) for semantically similar content.

    Embeds the query text using CohereEmbedder, then searches the Qdrant vector
    collection for the most similar mini-block summaries.

    Parameters
    ----------
    query : str
        Natural language query text to search for.
    limit : int, optional
        Maximum number of results (default 10).

    Returns
    -------
    dict
        ``{"query": ..., "results": [{id, score, summary, block_number, ...}], "count": N}``.
        On error: ``{"error": "..."}``.
    """
    metrics.agent_tool_call_total += 1

    # Resolve dependencies from runtime context
    if not config or "configurable" not in config:
        return {"error": "runtime context (configurable) not available"}

    embedder = config["configurable"].get("embedder")
    qdrant_client = config["configurable"].get("qdrant_client")

    if embedder is None or qdrant_client is None:
        return {"error": "embedder or qdrant_client not available in context"}

    try:
        # Step 1: Embed the query (async)
        try:
            vector = await embedder.embed(query)
        except Exception as e:
            logger.warning("embedding service unavailable", error=str(e))
            return {"error": "embedding service unavailable"}

        if not vector or len(vector) == 0:
            return {"error": "embedding produced empty vector"}

        # Step 2: Search Qdrant (sync client → run in thread)
        try:
            results = await asyncio.to_thread(
                qdrant_client.search,
                vector=vector,
                limit=limit,
            )
        except Exception as e:
            logger.error("Qdrant search failed", error=str(e))
            return {"error": f"vector search failed: {e}"}

        # Step 3: Format results
        formatted = []
        for r in results:
            entry = {
                "id": str(r.id),
                "score": float(r.score),
                "summary": r.payload.get("summary", ""),
                "block_number": r.payload.get("block_number", 0),
            }
            # Include any additional metadata
            for key, val in r.payload.items():
                if key not in ("summary", "block_number"):
                    entry[key] = val
            formatted.append(entry)

        return {
            "query": query,
            "results": formatted,
            "count": len(formatted),
        }

    except Exception as e:
        logger.error("search_cognitive_memory failed", error=str(e), exc_info=True)
        return {"error": f"cognitive memory search failed: {e}"}
