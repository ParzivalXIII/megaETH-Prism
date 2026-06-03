"""LangGraph tools for querying local PostgreSQL state (portfolios and balances).

Both tools manage their own ``AsyncSession`` lifecycle — acquiring a session
from the factory provided in the runtime context, querying, and closing.

Address inputs are auto-lowercased to match the database convention.
"""

from __future__ import annotations

from typing import Any

from langchain_core.tools import tool

from app.core import metrics
from app.core.logging import get_logger
from app.state.balance_repository import AssetBalanceRepository

logger = get_logger("megaeth.agents.tools.local_state")


@tool
async def query_portfolio(
    user_address: str,
    token_symbol: str = "",
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Query a user's portfolio from local PostgreSQL state.

    Returns all asset balances for the given user, optionally filtered by
    token symbol.  Each balance includes the human-readable amount.

    Parameters
    ----------
    user_address : str
        0x-prefixed user address (will be lowercased).
    token_symbol : str, optional
        Filter by token symbol (e.g. "USDM", "WETH").  Empty string = all.

    Returns
    -------
    dict
        ``{"user_address": ..., "balances": [...], "count": N}`` or
        ``{"error": "..."}`` on failure.
    """
    metrics.agent_tool_call_total += 1

    # Resolve DB session factory from runtime context
    if not config or "configurable" not in config:
        return {"error": "runtime context (configurable) not available"}

    session_factory = config["configurable"].get("db_session_factory")
    if session_factory is None:
        return {"error": "db_session_factory not available in context"}

    user_address = user_address.lower()

    try:
        async with session_factory() as session:
            repo = AssetBalanceRepository(session)
            balances = await repo.get_portfolio(user_address)

        # Convert to serializable dicts
        balance_list = []
        for bal in balances:
            entry = {
                "token_address": bal.token_address,
                "token_symbol": bal.token_symbol,
                "balance_raw": str(bal.balance_raw),
                "balance_human": str(bal.balance_human) if hasattr(bal, "balance_human") else "",
                "decimals": bal.decimals,
                "block_number": bal.block_number,
            }
            # Apply optional symbol filter
            if token_symbol and bal.token_symbol.upper() != token_symbol.upper():
                continue
            balance_list.append(entry)

        return {
            "user_address": user_address,
            "balances": balance_list,
            "count": len(balance_list),
        }

    except Exception as e:
        logger.error("query_portfolio failed", user_address=user_address, error=str(e))
        return {"error": f"portfolio query failed: {e}"}


@tool
async def query_balance(
    user_address: str,
    token_address: str,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Query a single token balance from local PostgreSQL state.

    Parameters
    ----------
    user_address : str
        0x-prefixed user address (will be lowercased).
    token_address : str
        0x-prefixed token contract address (will be lowercased).

    Returns
    -------
    dict
        ``{"user_address": ..., "token_address": ..., "balance": ..., "symbol": ...}``
        or ``{"error": "not found"}`` or ``{"error": "..."}`` on failure.
    """
    metrics.agent_tool_call_total += 1

    # Resolve DB session factory from runtime context
    if not config or "configurable" not in config:
        return {"error": "runtime context (configurable) not available"}

    session_factory = config["configurable"].get("db_session_factory")
    if session_factory is None:
        return {"error": "db_session_factory not available in context"}

    user_address = user_address.lower()
    token_address = token_address.lower()

    try:
        async with session_factory() as session:
            repo = AssetBalanceRepository(session)
            balance = await repo.get_balance(user_address, token_address)

        if balance is None:
            return {"error": "not found", "user_address": user_address, "token_address": token_address}

        return {
            "user_address": user_address,
            "token_address": token_address,
            "token_symbol": balance.token_symbol,
            "balance_raw": str(balance.balance_raw),
            "balance_human": str(balance.balance_human) if hasattr(balance, "balance_human") else "",
            "decimals": balance.decimals,
            "block_number": balance.block_number,
        }

    except Exception as e:
        logger.error("query_balance failed", user_address=user_address, token_address=token_address, error=str(e))
        return {"error": f"balance query failed: {e}"}
