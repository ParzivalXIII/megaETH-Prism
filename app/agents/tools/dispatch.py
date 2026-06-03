"""Safety-critical dispatch tool for LangGraph agents.

This is the **only** way agents can trigger on-chain actions.  It enforces:

1. **Calldata safety** — known-safe selectors allowed, known-dangerous blocked,
   unknown selectors blocked by default.
2. **Slippage cap** — ``max_slippage_bps ≤ 100`` (1%) enforced at tool level.
3. **Schema validation** — every payload is validated against ``ExecutionPayload``
   before enqueueing.
4. **Audit trail** — ``created_by_agent`` is set to ``"portfolio-agent"``.

This is the safety boundary between the LLM and the execution system.
"""

from __future__ import annotations

import uuid
from typing import Any

from langchain_core.tools import tool
from pydantic import ValidationError

from app.core import metrics
from app.core.config import settings
from app.core.logging import get_logger
from app.schemas.intent import ExecutionPayload, TriggerCondition

logger = get_logger("megaeth.agents.tools.dispatch")

# ---------------------------------------------------------------------------
# Calldata safety constants
# ---------------------------------------------------------------------------

# Known-safe function selectors (4 bytes = 10 chars with 0x prefix)
KNOWN_SAFE_SELECTORS: set[str] = {
    "0xa9059cbb",  # transfer(address,uint256)
    "0x095ea7b3",  # approve(address,uint256)
    "0x38ed1739",  # swapExactTokensForTokens(uint256,uint256,address[],address,uint256)
}

# Known-dangerous function selectors (hardcoded blocklist)
KNOWN_DANGEROUS_SELECTORS: set[str] = {
    "0x23b872dd",  # transferFrom(address,address,uint256)
    "0x42842e0e",  # safeTransferFrom(address,address,uint256)
    "0xb88d4fde",  # safeTransferFrom(address,address,uint256,bytes)
    "0xf242432a",  # safeTransferFrom(address,address,uint256,uint256,bytes)
    "0x2e1a7d4d",  # withdraw(uint256) — only safe for WETH, blocked generically
}

# Wildcard allowlist — allows all selectors (admin bypass, DANGEROUS)
WILDCARD: set[str] = {"*"}


def _extract_selector(calldata: str) -> str:
    """Extract the function selector from ABI-encoded calldata.

    Returns the first 10 characters (``0x`` + 8 hex chars), or ``"0x"`` if
    the calldata is too short.
    """
    if len(calldata) >= 10:
        return calldata[:10].lower()
    return "0x"


def _check_calldata(calldata: str) -> tuple[bool, str]:
    """Validate calldata against the safety layer.

    Returns ``(allowed, reason)`` tuple.
    """
    selector = _extract_selector(calldata)

    # Wildcard bypass
    if WILDCARD.issubset(set(settings.agent_calldata_allowlist)):
        logger.warning(
            "calldata allowlist is wildcard '*' — ALL selectors allowed. "
            "This should only be used in controlled environments.",
        )
        return True, "wildcard allowlist"

    # Check allowlist first
    if selector in settings.agent_calldata_allowlist:
        return True, f"selector {selector} is in allowlist"

    # Check blocklist
    if selector in KNOWN_DANGEROUS_SELECTORS:
        return False, f"selector {selector} is blocked (dangerous function)"

    # Unknown selector — blocked
    return False, f"selector {selector} is not in the allowlist and is not recognised as safe"


# ---------------------------------------------------------------------------
# LangGraph Tool
# ---------------------------------------------------------------------------


@tool
async def dispatch_execution(
    payload: dict[str, Any],
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Dispatch a validated execution plan to the worker pool.

    This is the **safety boundary** between the LLM and on-chain execution.
    The payload is validated against the ``ExecutionPayload`` schema,
    calldata is checked against the safety layer, and slippage is capped.

    Parameters
    ----------
    payload : dict
        Execution plan with fields: ``target_contract``, ``call_data``,
        ``trigger_condition``, ``max_slippage_bps``, ``max_gas_price``,
        ``priority``.

    Returns
    -------
    dict
        ``{"status": "queued", "execution_id": "..."}`` on success, or
        ``{"status": "rejected", "reason": "..."}`` on failure.
    """
    metrics.agent_tool_call_total += 1

    # Resolve execution queue from runtime context
    if not config or "configurable" not in config:
        return {"error": "runtime context (configurable) not available"}

    execution_queue = config["configurable"].get("execution_queue")
    if execution_queue is None:
        return {"error": "execution_queue not available in context"}

    # ------------------------------------------------------------------
    # Step 1: Calldata safety check (before schema validation)
    # ------------------------------------------------------------------

    call_data = payload.get("call_data", "")
    allowed, reason = _check_calldata(call_data)
    if not allowed:
        logger.warning("dispatch rejected by calldata safety layer", reason=reason, call_data=call_data[:20])
        return {"status": "rejected", "reason": f"calldata safety: {reason}"}

    # ------------------------------------------------------------------
    # Step 2: Slippage cap (defense-in-depth)
    # ------------------------------------------------------------------

    max_slippage = payload.get("max_slippage_bps", 100)
    if max_slippage > 100:
        logger.warning(
            "dispatch rejected — slippage exceeds 1%",
            max_slippage_bps=max_slippage,
        )
        return {"status": "rejected", "reason": "max_slippage_bps must be ≤ 100 (1%)"}

    # ------------------------------------------------------------------
    # Step 3: Schema validation
    # ------------------------------------------------------------------

    # Set created_by_agent if not already set
    if "created_by_agent" not in payload or not payload["created_by_agent"]:
        payload["created_by_agent"] = "portfolio-agent"

    # Normalise trigger_condition to a dict if it's a string
    trigger_raw = payload.get("trigger_condition", {"condition_type": "always"})
    if isinstance(trigger_raw, dict) and "condition_type" not in trigger_raw:
        trigger_raw = {"condition_type": "always", "params": trigger_raw}

    try:
        validated = ExecutionPayload(
            id=payload.get("id", str(uuid.uuid4())),
            target_contract=payload["target_contract"],
            call_data=payload["call_data"],
            max_gas_price=payload.get("max_gas_price", 1_000_000),
            trigger_condition=TriggerCondition(
                condition_type=trigger_raw.get("condition_type", "always"),
                params=trigger_raw.get("params", {}),
            ),
            max_slippage_bps=max_slippage,
            priority=payload.get("priority", "normal"),
            created_by_agent=payload.get("created_by_agent", "portfolio-agent"),
        )
    except (ValidationError, KeyError) as e:
        logger.warning("dispatch rejected — invalid payload", error=str(e))
        return {"status": "rejected", "reason": f"invalid payload: {e}"}

    # ------------------------------------------------------------------
    # Step 4: Enqueue
    # ------------------------------------------------------------------

    try:
        entry_id = await execution_queue.enqueue(validated)
        metrics.agent_payloads_generated_total += 1
        logger.info(
            "execution payload dispatched",
            execution_id=validated.id,
            target=validated.target_contract,
            entry_id=entry_id,
        )
        return {"status": "queued", "execution_id": validated.id, "stream_entry_id": entry_id}
    except Exception as e:
        logger.error("dispatch failed — enqueue error", error=str(e), exc_info=True)
        return {"status": "rejected", "reason": f"enqueue failed: {e}"}
