"""Unit tests for the summarizer.

Tests ``build_summary()`` edge cases: empty blocks, normal blocks,
500-character boundary, large transaction counts.
"""

from app.schemas.mini_block import MiniBlockPayload
from app.schemas.vector import VectorPayload
from app.vector.summarizer import build_summary


def _make_payload(
    block_number: int = 1,
    block_timestamp: int = 1700000000,
    tx_count: int = 0,
    gas_used: int = 0,
) -> MiniBlockPayload:
    return MiniBlockPayload(
        block_number=block_number,
        block_timestamp=block_timestamp,
        index=0,
        gas_used=gas_used,
        transactions=[f"0xtxhash{i}" for i in range(tx_count)],
        receipts=[f"0xreceipthash{i}" for i in range(tx_count)],
    )


def test_empty_block() -> None:
    """Zero transactions → "Block #{n}: empty"."""
    payload = _make_payload(block_number=42, tx_count=0, gas_used=0)
    result = build_summary(payload)
    assert result == "Block #42: empty"


def test_normal_block() -> None:
    """Normal block with transactions and gas."""
    payload = _make_payload(block_number=100, tx_count=3, gas_used=21000)
    result = build_summary(payload)
    assert "Block #100" in result
    assert "3 transactions" in result
    assert "21000 gas used" in result
    assert "timestamp" in result


def test_never_exceeds_500_chars() -> None:
    """Summary output must never exceed 500 characters for any valid input."""
    payload = _make_payload(
        block_number=999999999,
        block_timestamp=9999999999,
        tx_count=10000,
        gas_used=999999999999,
    )
    result = build_summary(payload)
    assert len(result) <= 500
    # Verify it's accepted by VectorPayload validation
    VectorPayload(
        payload_id="0xtest",
        block_number=payload.block_number,
        summary=result,
        embedding=[0.0] * 1024,
        metadata={},
    )


def test_large_tx_count_produces_valid_summary() -> None:
    """Blocks with many transactions produce parseable summaries."""
    payload = _make_payload(block_number=1, tx_count=500, gas_used=1000000)
    result = build_summary(payload)
    assert "500 transactions" in result
    assert len(result) <= 500
