"""Unit tests for the app.schemas contract layer.

Covers:
- Happy-path validation for MiniBlockPayload, StreamEntry, VectorPayload
- Derived property tx_count
- ValidationError on missing fields
- JSON Schema output from MiniBlockRecord
- Stream constant values
- VectorPayload embedding dimension validation
- Hex string acceptance on MiniBlockPayload numeric fields
"""

import json
from datetime import datetime
from typing import Any

import pytest
from pydantic import ValidationError

from app.schemas import (
    STREAM_FIELD_BLOCK_NUMBER,
    STREAM_FIELD_CORRELATION_ID,
    STREAM_FIELD_INGESTED_AT,
    STREAM_FIELD_RAW_PAYLOAD,
    MiniBlockPayload,
    MiniBlockRecord,
    StreamEntry,
    VectorPayload,
)

# ---------------------------------------------------------------------------
# Fixtures — shared valid payloads matching the actual MegaETH testnet API
# ---------------------------------------------------------------------------


@pytest.fixture
def valid_mini_block_data() -> dict[str, Any]:
    return {
        "block_number": "0x138cb41",  # hex string from wire
        "block_timestamp": "0x6a1bd839",
        "index": "0x9",
        "gas_used": "0x0",
        "transactions": [
            {"hash": "0xtx1", "type": "0x2", "from": "0xabc"},
            {"hash": "0xtx2", "type": "0x0", "from": "0xdef"},
            {"hash": "0xtx3", "type": "0x2", "from": "0x123"},
        ],
        "receipts": [
            {"status": "0x1", "gasUsed": "0x5208"},
            {"status": "0x1", "gasUsed": "0x5208"},
            {"status": "0x1", "gasUsed": "0x5208"},
        ],
        "mini_block_number": "0x7273fb41",
        "mini_block_timestamp": "0x653175ffded30",
    }


@pytest.fixture
def valid_stream_entry_data() -> dict[str, Any]:
    return {
        "block_number": 100,
        "raw_payload": '{"block_number":"0x138cb41","block_timestamp":"0x6a1bd839","index":"0x9","gas_used":"0x0","transactions":[],"receipts":[]}',
        "ingested_at": 1700000000.123,
        "correlation_id": "corr-001",
    }


@pytest.fixture
def valid_vector_payload_data() -> dict[str, Any]:
    return {
        "payload_id": "0xabc123",
        "block_number": 100,
        "summary": "Mini-block with 3 transfers",
        "embedding": [0.0] * 1024,
        "metadata": {"source": "megaeth-mainnet"},
    }


# ========================= MiniBlockPayload tests =========================


class TestMiniBlockPayload:
    def test_happy_path_native_ints(self) -> None:
        """Accept native integers (local/dev RPC may return them)."""
        payload = MiniBlockPayload.model_validate({
            "block_number": 20447297,
            "block_timestamp": 1782345785,
            "index": 9,
            "gas_used": 21000,
            "transactions": [{"hash": "0xtx1", "from": "0xabc"}],
            "receipts": [{"status": "0x1"}],
        })
        assert payload.block_number == 20447297
        assert payload.index == 9

    def test_happy_path_hex_strings(self, valid_mini_block_data: dict[str, Any]) -> None:
        """Accept hex-encoded strings (testnet API format)."""
        payload = MiniBlockPayload.model_validate(valid_mini_block_data)
        assert payload.block_number == 0x138cb41
        assert payload.index == 9
        assert payload.block_timestamp == 0x6a1bd839
        assert payload.gas_used == 0

    def test_derived_tx_count(self, valid_mini_block_data: dict[str, Any]) -> None:
        payload = MiniBlockPayload.model_validate(valid_mini_block_data)
        assert payload.tx_count == len(payload.transactions)
        assert payload.tx_count == 3

    def test_tx_count_zero_when_no_transactions(self) -> None:
        data: dict[str, Any] = {
            "block_number": 20447298,
            "block_timestamp": 1782345786,
            "index": 0,
            "gas_used": 0,
            "transactions": [],
            "receipts": [],
        }
        payload = MiniBlockPayload.model_validate(data)
        assert payload.tx_count == 0

    def test_immutable(self, valid_mini_block_data: dict[str, Any]) -> None:
        payload = MiniBlockPayload.model_validate(valid_mini_block_data)
        with pytest.raises(ValidationError):
            payload.block_number = 999  # type: ignore[misc]

    def test_missing_required_field_block_number(self) -> None:
        data: dict[str, Any] = {
            "block_timestamp": 1782345786,
            "index": 0,
            "gas_used": 0,
            "transactions": [],
            "receipts": [],
        }
        with pytest.raises(ValidationError) as excinfo:
            MiniBlockPayload.model_validate(data)
        assert "block_number" in str(excinfo.value)

    def test_extra_fields_ignored(self, valid_mini_block_data: dict[str, Any]) -> None:
        """Extra fields like block_hash or unknown RPC fields are silently ignored."""
        data = {**valid_mini_block_data, "block_hash": "0xdeadbeef", "unknown_field": 42}
        payload = MiniBlockPayload.model_validate(data)
        assert not hasattr(payload, "block_hash")

    def test_mixed_hex_and_int(self) -> None:
        """Accept mixed hex strings and integers."""
        payload = MiniBlockPayload.model_validate({
            "block_number": "0x138cb41",  # hex
            "block_timestamp": 1782345785,  # int
            "index": 9,
            "gas_used": "0x0",
            "transactions": [],
            "receipts": [],
        })
        assert payload.block_number == 0x138cb41
        assert payload.block_timestamp == 1782345785

    def test_optional_fields(self) -> None:
        """Optional fields should be None if not provided."""
        payload = MiniBlockPayload.model_validate({
            "block_number": 1,
            "block_timestamp": 1000,
            "index": 0,
            "gas_used": 0,
            "transactions": [],
            "receipts": [],
        })
        assert payload.mini_block_number is None
        assert payload.mini_block_timestamp is None
        assert payload.transaction_root is None


# ========================= StreamEntry tests ================================


class TestStreamEntry:
    def test_happy_path(self, valid_stream_entry_data: dict[str, Any]) -> None:
        entry = StreamEntry.model_validate(valid_stream_entry_data)
        assert entry.block_number == 100
        assert entry.correlation_id == "corr-001"
        assert isinstance(entry.ingested_at, float)
        assert isinstance(entry.raw_payload, str)

    def test_missing_required_field(self) -> None:
        with pytest.raises(ValidationError) as excinfo:
            StreamEntry.model_validate(
                {"block_number": 100}  # missing raw_payload, ingested_at, correlation_id
            )
        assert "raw_payload" in str(excinfo.value)

    def test_extra_fields_rejected(self) -> None:
        data: dict[str, Any] = {
            "block_number": 100,
            "raw_payload": "{}",
            "ingested_at": 1.0,
            "correlation_id": "c",
            "bogus": "nope",
        }
        with pytest.raises(ValidationError):
            StreamEntry.model_validate(data)


# ========================= Stream constants ================================


class TestStreamConstants:
    def test_values(self) -> None:
        assert STREAM_FIELD_BLOCK_NUMBER == "block_number"
        assert STREAM_FIELD_RAW_PAYLOAD == "raw_payload"
        assert STREAM_FIELD_INGESTED_AT == "ingested_at"
        assert STREAM_FIELD_CORRELATION_ID == "correlation_id"

    def test_no_duplicates(self) -> None:
        """All four constants should be distinct."""
        values = {
            STREAM_FIELD_BLOCK_NUMBER,
            STREAM_FIELD_RAW_PAYLOAD,
            STREAM_FIELD_INGESTED_AT,
            STREAM_FIELD_CORRELATION_ID,
        }
        assert len(values) == 4


# ========================= MiniBlockRecord tests ============================


class TestMiniBlockRecord:
    def test_model_json_schema_is_valid(self) -> None:
        schema = MiniBlockRecord.model_json_schema()
        assert isinstance(schema, dict)
        assert schema.get("title") == "MiniBlockRecord"
        assert "properties" in schema
        props = schema["properties"]
        assert "block_number" in props
        assert "index" in props
        assert "block_timestamp" in props
        assert "gas_used" in props
        assert "tx_count" in props
        assert "raw_jsonb" in props
        assert "ingested_at" in props
        assert "mini_block_number" in props

    def test_json_schema_valid_json(self) -> None:
        schema = MiniBlockRecord.model_json_schema()
        json.dumps(schema)
        assert True

    def test_defaults(self) -> None:
        record = MiniBlockRecord(
            block_number=42,
            index=0,
            tx_count=5,
        )
        assert record.block_timestamp == 0
        assert record.gas_used == 0
        assert record.raw_jsonb == {}
        assert isinstance(record.ingested_at, datetime)
        assert record.mini_block_number is None

    def test_mini_block_number_unique(self) -> None:
        record = MiniBlockRecord(
            block_number=42,
            index=0,
            mini_block_number=0x7273fb41,
            tx_count=5,
        )
        assert record.mini_block_number == 0x7273fb41


# ========================= VectorPayload tests ==============================


class TestVectorPayload:
    def test_happy_path(self, valid_vector_payload_data: dict[str, Any]) -> None:
        vp = VectorPayload.model_validate(valid_vector_payload_data)
        assert vp.payload_id == "0xabc123"
        assert vp.block_number == 100
        assert vp.summary == "Mini-block with 3 transfers"
        assert len(vp.embedding) == 1024

    def test_wrong_embedding_dimension(self, valid_vector_payload_data: dict[str, Any]) -> None:
        data = {**valid_vector_payload_data, "embedding": [0.0] * 512}
        with pytest.raises(ValidationError) as excinfo:
            VectorPayload.model_validate(data)
        assert "1024" in str(excinfo.value)

    def test_empty_embedding_rejected(self, valid_vector_payload_data: dict[str, Any]) -> None:
        data = {**valid_vector_payload_data, "embedding": []}
        with pytest.raises(ValidationError):
            VectorPayload.model_validate(data)

    def test_summary_max_length(self, valid_vector_payload_data: dict[str, Any]) -> None:
        data = {**valid_vector_payload_data, "summary": "x" * 501}
        with pytest.raises(ValidationError):
            VectorPayload.model_validate(data)

    def test_extra_fields_rejected(self, valid_vector_payload_data: dict[str, Any]) -> None:
        data = {**valid_vector_payload_data, "unexpected": "field"}
        with pytest.raises(ValidationError):
            VectorPayload.model_validate(data)
