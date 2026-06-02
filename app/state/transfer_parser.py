"""ERC-20 Transfer event extractor — pure function, no I/O.

Extracts ``Transfer(address,address,uint256)`` events from mini-block receipt
logs, following the standard Ethereum event log format.

Design:
- Pure functions (synchronous, no I/O) — easy to unit test.
- Defensive parsing: skips malformed logs with WARNING, never crashes.
- Handles edge cases: failed transactions, zero-address mint/burn,
  self-transfers, zero-value transfers.
- All addresses normalised to lowercase 0x-prefixed.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from app.core.logging import get_logger
from app.schemas.mini_block import MiniBlockPayload

logger = get_logger("megaeth.state.transfer_parser")

# keccak256("Transfer(address,address,uint256)")
TRANSFER_TOPIC: str = (
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)


@dataclass
class TransferDelta:
    """A single ERC-20 Transfer event extracted from a receipt log."""

    token_address: str
    """ERC-20 contract address (lowercase 0x-prefixed)."""

    from_address: str
    """Sender address (lowercase 0x-prefixed)."""

    to_address: str
    """Recipient address (lowercase 0x-prefixed)."""

    amount: int
    """Transfer amount (uint256 raw value)."""

    block_number: int
    """Block containing this transfer."""

    tx_index: int
    """Transaction index within the block."""

    log_index: int
    """Log index within the receipt."""


def _normalise_address(hex_str: str) -> str:
    """Extract and normalise a 40-char hex address from a topic or data field.

    Handles both zero-padded 64-char hex (topic) and bare addresses.
    """
    hex_str = hex_str.strip().lower()
    if hex_str.startswith("0x"):
        hex_str = hex_str[2:]
    # Take last 40 hex chars (topics are 32-byte padded)
    hex_str = hex_str[-40:] if len(hex_str) >= 40 else hex_str.zfill(40)
    return "0x" + hex_str


def extract_transfers(
    payload: MiniBlockPayload,
    on_skip: Callable[[], object] | None = None,
) -> list[TransferDelta]:
    """Extract ERC-20 Transfer events from a mini-block payload.

    Iterates all receipts in the mini-block, matches ``Transfer`` event
    topics, and decodes the sender, recipient, and amount.

    Args:
        payload: A validated ``MiniBlockPayload``.
        on_skip: Optional zero-arg callable invoked on each skipped log
            (e.g. a metric counter increment).  Keeps ``extract_transfers``
            a pure function while allowing instrumentation.

    Returns:
        A list of ``TransferDelta`` objects (empty list if none found).
    """
    deltas: list[TransferDelta] = []
    receipts = payload.receipts or []

    for tx_idx, receipt in enumerate(receipts):
        # Defensive: skip non-dict receipts
        if not isinstance(receipt, dict):
            logger.warning("skipping non-dict receipt", tx_index=tx_idx)
            if on_skip:
                on_skip()
            continue

        # Skip failed transactions
        status = receipt.get("status")
        if status is not None:
            # Accept both hex string and int forms
            if isinstance(status, str) and status not in ("0x1", "1"):
                continue
            if isinstance(status, int) and status != 1:
                continue

        logs = receipt.get("logs", [])
        if not isinstance(logs, list):
            continue

        for log_idx, log in enumerate(logs):
            if not isinstance(log, dict):
                logger.warning(
                    "skipping non-dict log",
                    tx_index=tx_idx,
                    log_index=log_idx,
                )
                if on_skip:
                    on_skip()
                continue

            topics = log.get("topics", [])
            if not isinstance(topics, list) or len(topics) < 3:
                logger.warning(
                    "skipping log with insufficient topics",
                    tx_index=tx_idx,
                    log_index=log_idx,
                    topic_count=len(topics),
                )
                if on_skip:
                    on_skip()
                continue

            # Match Transfer topic (case-insensitive)
            topic0 = str(topics[0]).lower()
            if topic0 != TRANSFER_TOPIC:
                if on_skip:
                    on_skip()
                continue

            # Decode addresses from topics[1] and topics[2]
            try:
                from_address = _normalise_address(str(topics[1]))
                to_address = _normalise_address(str(topics[2]))
            except (IndexError, ValueError) as e:
                logger.warning(
                    "failed to decode addresses from topics",
                    tx_index=tx_idx,
                    log_index=log_idx,
                    error=str(e),
                )
                continue

            # Decode amount from data field (uint256)
            data = log.get("data", "0x0")
            if not isinstance(data, str):
                data = "0x0"
            try:
                amount = int(data, 16)
            except (ValueError, TypeError) as e:
                logger.warning(
                    "failed to decode amount from log data",
                    tx_index=tx_idx,
                    log_index=log_idx,
                    error=str(e),
                )
                continue

            # Derive token address from log address field if available
            token_address = log.get("address", "")
            if not token_address:
                # Fallback: not available in some formats — consumer must supply
                token_address = "0x0000000000000000000000000000000000000000"
            token_address = token_address.lower()
            if not token_address.startswith("0x"):
                token_address = "0x" + token_address

            deltas.append(
                TransferDelta(
                    token_address=token_address,
                    from_address=from_address,
                    to_address=to_address,
                    amount=amount,
                    block_number=payload.block_number,
                    tx_index=tx_idx,
                    log_index=log_idx,
                )
            )

    return deltas


def apply_deltas(
    deltas: list[TransferDelta],
) -> list[tuple[str, str, int]]:
    """Aggregate TransferDelta objects into ``(address, token, net_change)`` tuples.

    Self-transfers (``from == to``) produce net-zero deltas and are skipped.
    Positive for recipients, negative for senders.

    Args:
        deltas: List of ``TransferDelta`` objects.

    Returns:
        List of ``(user_address, token_address, net_change)`` tuples.
    """
    net: dict[tuple[str, str], int] = {}

    for d in deltas:
        # Self-transfer: delta would be zero
        if d.from_address.lower() == d.to_address.lower():
            continue

        key_from = (d.from_address.lower(), d.token_address.lower())
        key_to = (d.to_address.lower(), d.token_address.lower())

        net[key_from] = net.get(key_from, 0) - d.amount
        net[key_to] = net.get(key_to, 0) + d.amount

    # Filter out zero net deltas
    return [(addr, token, delta) for (addr, token), delta in net.items() if delta != 0]
