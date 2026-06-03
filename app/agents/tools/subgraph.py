"""SubgraphClient & query_historical_subgraph tool.

Provides async access to on-chain historical data via a subgraph (Uniswap V3
or Envio HyperIndex).  Fixed named queries — NOT generic GraphQL — so that
schema changes are explicit and auditable.

Usage::

    client = SubgraphClient("https://...")
    prices = await client.fetch_token_price_history("0xabc...", days=7)
    await client.close()
"""

from __future__ import annotations

from typing import Any

from gql import gql
from gql.client import Client as GqlClient
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.exceptions import TransportQueryError
from langchain_core.tools import tool

from app.core import metrics
from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger("megaeth.agents.tools.subgraph")


class SubgraphClient:
    """Async GraphQL client for on-chain historical data.

    Parameters
    ----------
    url : str
        The subgraph endpoint URL.
    """

    def __init__(self, url: str | None = None) -> None:
        self._url = url or settings.megaeth_historical_graphql_url
        self._client: GqlClient | None = None
        self._transport: AIOHTTPTransport | None = None

    async def _get_client(self) -> GqlClient:
        """Lazy-initialise the GraphQL client."""
        if self._client is None:
            self._transport = AIOHTTPTransport(url=self._url)
            self._client = GqlClient(
                transport=self._transport,
                fetch_schema_from_transport=True,
                execute_timeout=30,
            )
            await self._client.__aenter__()
        return self._client

    async def _execute(self, query_str: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
        """Execute a GraphQL query with error handling."""
        client = await self._get_client()
        try:
            result: dict[str, Any] = await client.execute(  # type: ignore[misc]
                gql(query_str),
                variable_values=variables or {},
            )
            return result
        except TransportQueryError as e:
            logger.error("subgraph query failed", query_type=query_str[:80], error=str(e))
            return {"error": f"subgraph query failed: {e}"}
        except Exception as e:
            logger.error("subgraph client error", error=str(e), exc_info=True)
            return {"error": f"subgraph client error: {e}"}

    # ------------------------------------------------------------------
    # Fixed named queries
    # ------------------------------------------------------------------

    UNISWAP_V3_QUERIES: dict[str, str] = {
        "token_price_history": """
            query tokenPriceHistory($token: String!, $days: Int!) {
                tokenDayDatas(
                    first: $days
                    orderBy: date
                    orderDirection: desc
                    where: { token: $token }
                ) {
                    date
                    priceUSD
                    open
                    high
                    low
                    close
                    volumeUSD
                    totalValueLockedUSD
                }
            }
        """,
        "recent_swaps": """
            query recentSwaps($pool: String!, $limit: Int!) {
                swaps(
                    first: $limit
                    orderBy: timestamp
                    orderDirection: desc
                    where: { pool: $pool }
                ) {
                    id
                    timestamp
                    sender
                    recipient
                    amount0
                    amount1
                    amountUSD
                    sqrtPriceX96
                }
            }
        """,
        "pool_stats": """
            query poolStats($pool: String!) {
                pool(id: $pool) {
                    id
                    feeTier
                    liquidity
                    sqrtPrice
                    token0 { id symbol decimals }
                    token1 { id symbol decimals }
                    totalValueLockedUSD
                    volumeUSD
                    feesUSD
                }
            }
        """,
    }

    ENVIO_QUERIES: dict[str, str] = {
        "token_price_history": """
            query tokenPriceHistory($token: String!, $days: Int!) {
                TokenDayData(
                    limit: $days
                    orderBy: DATE_DESC
                    where: { token_id: { _eq: $token } }
                ) {
                    date
                    price_usd
                    open
                    high
                    low
                    close
                    volume_usd
                    tvl_usd
                }
            }
        """,
        "recent_swaps": """
            query recentSwaps($pool: String!, $limit: Int!) {
                Swap(
                    limit: $limit
                    orderBy: TIMESTAMP_DESC
                    where: { pool_id: { _eq: $pool } }
                ) {
                    id
                    timestamp
                    sender
                    recipient
                    amount0
                    amount1
                    amount_usd
                    sqrt_price_x96
                }
            }
        """,
        "pool_stats": """
            query poolStats($pool: String!) {
                Pool(id: $pool) {
                    id
                    fee_tier
                    liquidity
                    sqrt_price
                    token0 { id symbol decimals }
                    token1 { id symbol decimals }
                    tvl_usd
                    volume_usd
                    fees_usd
                }
            }
        """,
    }

    @property
    def _queries(self) -> dict[str, str]:
        """Return the query set for the configured subgraph type."""
        if settings.subgraph_type == "envio_hyperindex":
            return self.ENVIO_QUERIES
        return self.UNISWAP_V3_QUERIES

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    async def fetch_token_price_history(self, token_address: str, days: int = 7) -> dict[str, Any]:
        """Fetch daily OHLCV price data for a token over N days."""
        query = self._queries.get("token_price_history", "")
        if not query:
            return {"error": f"no query for subgraph_type={settings.subgraph_type}, token_price_history"}
        return await self._execute(query, {"token": token_address.lower(), "days": days})

    async def fetch_recent_swaps(self, pool_address: str, limit: int = 100) -> dict[str, Any]:
        """Fetch recent swap events for a pool."""
        query = self._queries.get("recent_swaps", "")
        if not query:
            return {"error": f"no query for subgraph_type={settings.subgraph_type}, recent_swaps"}
        return await self._execute(query, {"pool": pool_address.lower(), "limit": limit})

    async def fetch_pool_stats(self, pool_address: str) -> dict[str, Any]:
        """Fetch TVL, volume, and fee data for a pool."""
        query = self._queries.get("pool_stats", "")
        if not query:
            return {"error": f"no query for subgraph_type={settings.subgraph_type}, pool_stats"}
        return await self._execute(query, {"pool": pool_address.lower()})

    async def close(self) -> None:
        """Close the underlying HTTP session."""
        if self._client is not None:
            await self._client.__aexit__(None, None, None)
            self._client = None
            self._transport = None


# ---------------------------------------------------------------------------
# LangGraph Tool
# ---------------------------------------------------------------------------


@tool
async def query_historical_subgraph(query_type: str, params: dict[str, Any]) -> dict[str, Any]:
    """Query on-chain historical data via subgraph.

    Supports three query types:
    - ``token_price_history``: daily OHLCV for a token. Params: ``token_address``, ``days`` (default 7).
    - ``recent_swaps``: recent swap events for a pool. Params: ``pool_address``, ``limit`` (default 100).
    - ``pool_stats``: TVL, volume, fees for a pool. Params: ``pool_address``.

    Parameters
    ----------
    query_type : str
        One of ``token_price_history``, ``recent_swaps``, ``pool_stats``.
    params : dict
        Query-specific parameters (see above).

    Returns
    -------
    dict
        Query results or error message.
    """
    metrics.agent_tool_call_total += 1

    client = SubgraphClient()

    try:
        if query_type == "token_price_history":
            result = await client.fetch_token_price_history(
                params.get("token_address", ""),
                days=params.get("days", 7),
            )
        elif query_type == "recent_swaps":
            result = await client.fetch_recent_swaps(
                params.get("pool_address", ""),
                limit=params.get("limit", 100),
            )
        elif query_type == "pool_stats":
            result = await client.fetch_pool_stats(
                params.get("pool_address", ""),
            )
        else:
            result = {"error": f"unknown query_type: {query_type}"}
    finally:
        await client.close()

    return result if result else {"error": "no data returned"}
