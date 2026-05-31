"""Unit tests for the ingestion daemon utilities.

Tests the ``_normalize_block_data`` static method that converts
``newHeads`` (standard EVM) block headers into the unified
``MiniBlockPayload``-compatible format.
"""

from typing import Any

from app.ingestion.daemon import MegaETHIngestionDaemon
from app.schemas import MiniBlockPayload


def test_normalize_mini_blocks_passthrough() -> None:
    """miniBlocks data should pass through unchanged (it's already in format)."""
    raw: dict[str, Any] = {
        "block_number": "0x138cb41",
        "block_timestamp": "0x6a1bd839",
        "index": "0x9",
        "gas_used": "0x0",
        "transactions": [],
        "receipts": [],
        "mini_block_number": "0x7273fb41",
    }
    normalized = MegaETHIngestionDaemon._normalize_block_data(raw, "miniBlocks")
    assert normalized["block_number"] == "0x138cb41"
    assert normalized["block_timestamp"] == "0x6a1bd839"
    assert normalized["index"] == "0x9"
    # Verify it produces a valid MiniBlockPayload
    payload = MiniBlockPayload.model_validate(normalized)
    assert payload.block_number == 0x138CB41


def test_normalize_new_heads_converts_fields() -> None:
    """newHeads data should be mapped to MiniBlockPayload-compatible format."""
    raw: dict[str, Any] = {
        "number": "0x1b4",
        "hash": "0xdead",
        "timestamp": "0x6a1bd839",
        "gasUsed": "0x5208",
        "transactions": ["0xtxhash1", "0xtxhash2"],
        "transactionsRoot": "0xroot1",
        "receiptsRoot": "0xroot2",
        "parentHash": "0xbeef",
        "miner": "0x0000",
    }
    normalized = MegaETHIngestionDaemon._normalize_block_data(raw, "newHeads")

    # Field mapping
    assert normalized["block_number"] == "0x1b4"
    assert normalized["block_timestamp"] == "0x6a1bd839"
    assert normalized["gas_used"] == 0x5208  # int-converted
    assert normalized["index"] == 0
    assert normalized["transactions"] == ["0xtxhash1", "0xtxhash2"]
    assert normalized["receipts"] == []  # newHeads has no receipts
    assert normalized["transaction_root"] == "0xroot1"
    assert normalized["receipt_root"] == "0xroot2"

    # Verify it produces a valid MiniBlockPayload
    payload = MiniBlockPayload.model_validate(normalized)
    assert payload.block_number == 0x1B4
    assert payload.block_timestamp == 0x6A1BD839
    assert payload.gas_used == 0x5208
    assert payload.tx_count == 2
    assert payload.transaction_root == "0xroot1"
    assert payload.receipt_root == "0xroot2"


def test_normalize_new_heads_empty_transactions() -> None:
    """Empty blocks from newHeads should produce tx_count=0."""
    raw: dict[str, Any] = {
        "number": "0x1",
        "timestamp": "0x0",
        "gasUsed": "0x0",
        "transactions": [],
    }
    normalized = MegaETHIngestionDaemon._normalize_block_data(raw, "newHeads")
    payload = MiniBlockPayload.model_validate(normalized)
    assert payload.tx_count == 0
    assert payload.receipts == []


def test_normalize_new_heads_missing_optional_fields() -> None:
    """Missing optional fields should not break normalization."""
    raw: dict[str, Any] = {
        "number": "0x1",
        "timestamp": "0x0",
        "gasUsed": "0x0",
        "transactions": [],
    }
    normalized = MegaETHIngestionDaemon._normalize_block_data(raw, "newHeads")
    payload = MiniBlockPayload.model_validate(normalized)
    assert payload.transaction_root is None
    assert payload.receipt_root is None
