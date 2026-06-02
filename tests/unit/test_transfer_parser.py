"""Unit tests for the ERC-20 Transfer event parser.

Covers:
- Empty block / no transactions
- Single Transfer event
- Multiple Transfer events across different tokens
- Non-Transfer topics (e.g. Approval)
- Failed transactions (status=0)
- Malformed logs (insufficient topics)
- Burn events (to=0x0)
- Mint events (from=0x0)
- Self-transfers (from == to)
- Address normalisation (uppercase → lowercase)
- Delta aggregation across multiple events
"""

from __future__ import annotations

from app.schemas.mini_block import MiniBlockPayload
from app.state.transfer_parser import (
    TRANSFER_TOPIC,
    TransferDelta,
    apply_deltas,
    extract_transfers,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TOKEN_A = "0x1111111111111111111111111111111111111111"
TOKEN_B = "0x2222222222222222222222222222222222222222"
ADDR_ALICE = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
ADDR_BOB = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
ADDR_CAROL = "0xcccccccccccccccccccccccccccccccccccccccc"
ZERO_ADDR = "0x0000000000000000000000000000000000000000"


def _make_receipt(
    status: str = "0x1",
    logs: list | None = None,
) -> dict:
    return {"status": status, "logs": logs or []}


def _make_transfer_log(
    token_address: str = TOKEN_A,
    from_addr: str = ADDR_ALICE,
    to_addr: str = ADDR_BOB,
    amount_hex: str = "0x64",  # 100
) -> dict:
    # Topics: topic0=Transfer, topic1=from (padded to 32 bytes), topic2=to
    from_padded = "0x" + "00" * 12 + from_addr[2:]
    to_padded = "0x" + "00" * 12 + to_addr[2:]
    return {
        "topics": [TRANSFER_TOPIC, from_padded, to_padded],
        "data": amount_hex,
        "address": token_address,
    }


def _make_mini_block(block_number: int = 1, receipts: list | None = None) -> MiniBlockPayload:
    return MiniBlockPayload(
        block_number=block_number,
        block_timestamp=1704067200 + block_number,
        index=0,
        gas_used=21000,
        transactions=["0xtx1"] if receipts else [],
        receipts=receipts or [],
    )


class TestExtractTransfers:
    def test_empty_block(self) -> None:
        """No transactions → empty list."""
        payload = _make_mini_block(block_number=1, receipts=[])
        assert extract_transfers(payload) == []

    def test_single_transfer(self) -> None:
        """1 Transfer log → 1 delta with correct addresses and amount."""
        payload = _make_mini_block(receipts=[
            _make_receipt(logs=[_make_transfer_log()]),
        ])
        deltas = extract_transfers(payload)
        assert len(deltas) == 1
        d = deltas[0]
        assert d.token_address == TOKEN_A
        assert d.from_address == ADDR_ALICE
        assert d.to_address == ADDR_BOB
        assert d.amount == 100
        assert d.block_number == 1

    def test_multiple_transfers(self) -> None:
        """3 Transfer logs across different tokens → 3 deltas."""
        payload = _make_mini_block(receipts=[
            _make_receipt(logs=[
                _make_transfer_log(token_address=TOKEN_A),
                _make_transfer_log(token_address=TOKEN_B),
                _make_transfer_log(
                    token_address=TOKEN_A,
                    from_addr=ADDR_BOB,
                    to_addr=ADDR_CAROL,
                    amount_hex="0x32",  # 50
                ),
            ]),
        ])
        deltas = extract_transfers(payload)
        assert len(deltas) == 3

    def test_non_transfer_topic(self) -> None:
        """Approval log → skipped, no error."""
        payload = _make_mini_block(receipts=[
            _make_receipt(logs=[
                {
                    "topics": [
                        "0x8f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f",  # not Transfer
                        "0x" + "00" * 12 + "ab" * 20,
                        "0x" + "00" * 12 + "cd" * 20,
                    ],
                    "data": "0x01",
                    "address": TOKEN_A,
                },
                _make_transfer_log(),  # This one is a Transfer
            ]),
        ])
        deltas = extract_transfers(payload)
        assert len(deltas) == 1
        assert deltas[0].from_address == ADDR_ALICE

    def test_failed_transaction(self) -> None:
        """Receipt with status=0x0 → all logs skipped."""
        payload = _make_mini_block(receipts=[
            _make_receipt(
                status="0x0",
                logs=[_make_transfer_log()],
            ),
        ])
        assert extract_transfers(payload) == []

    def test_failed_transaction_int_status(self) -> None:
        """Receipt with status=0 (int) → all logs skipped."""
        payload = _make_mini_block(receipts=[
            {"status": 0, "logs": [_make_transfer_log()]},
        ])
        assert extract_transfers(payload) == []

    def test_malformed_log_two_topics(self) -> None:
        """Log with <3 topics → skipped gracefully."""
        payload = _make_mini_block(receipts=[
            _make_receipt(logs=[
                {"topics": [TRANSFER_TOPIC], "data": "0x01", "address": TOKEN_A},
            ]),
        ])
        assert extract_transfers(payload) == []

    def test_burn_event(self) -> None:
        """Transfer to zero address → delta produced."""
        payload = _make_mini_block(receipts=[
            _make_receipt(logs=[
                _make_transfer_log(to_addr=ZERO_ADDR),
            ]),
        ])
        deltas = extract_transfers(payload)
        assert len(deltas) == 1
        assert deltas[0].to_address == ZERO_ADDR

    def test_mint_event(self) -> None:
        """Transfer from zero address → delta produced."""
        payload = _make_mini_block(receipts=[
            _make_receipt(logs=[
                _make_transfer_log(from_addr=ZERO_ADDR),
            ]),
        ])
        deltas = extract_transfers(payload)
        assert len(deltas) == 1
        assert deltas[0].from_address == ZERO_ADDR

    def test_self_transfer(self) -> None:
        """from == to → delta still extracted (aggregation handles it)."""
        payload = _make_mini_block(receipts=[
            _make_receipt(logs=[
                _make_transfer_log(from_addr=ADDR_ALICE, to_addr=ADDR_ALICE),
            ]),
        ])
        deltas = extract_transfers(payload)
        assert len(deltas) == 1
        assert deltas[0].from_address == deltas[0].to_address

    def test_address_normalisation(self) -> None:
        """Uppercase addresses in log → lowercase in output."""
        payload = _make_mini_block(receipts=[
            _make_receipt(logs=[{
                "topics": [
                    TRANSFER_TOPIC,
                    "0x" + "00" * 12 + "AA" * 20,  # uppercase
                    "0x" + "00" * 12 + "BB" * 20,  # uppercase
                ],
                "data": "0x64",
                "address": TOKEN_A.upper(),
            }]),
        ])
        deltas = extract_transfers(payload)
        assert deltas[0].from_address == "0x" + "aa" * 20
        assert deltas[0].to_address == "0x" + "bb" * 20
        assert deltas[0].token_address == TOKEN_A


class TestApplyDeltas:
    def test_simple_transfer(self) -> None:
        """Alice sends 100 to Bob → Alice -100, Bob +100."""
        deltas = [
            TransferDelta(TOKEN_A, ADDR_ALICE, ADDR_BOB, 100, 1, 0, 0),
        ]
        result = apply_deltas(deltas)
        net = {(a, t): d for a, t, d in result}
        assert net[(ADDR_ALICE, TOKEN_A)] == -100
        assert net[(ADDR_BOB, TOKEN_A)] == 100

    def test_multiple_transfers_aggregation(self) -> None:
        """Multiple events for same (user, token) net correctly."""
        deltas = [
            TransferDelta(TOKEN_A, ADDR_ALICE, ADDR_BOB, 100, 1, 0, 0),
            TransferDelta(TOKEN_A, ADDR_ALICE, ADDR_BOB, 50, 1, 1, 0),
            TransferDelta(TOKEN_A, ADDR_BOB, ADDR_ALICE, 30, 1, 2, 0),
        ]
        result = apply_deltas(deltas)
        net = {(a, t): d for a, t, d in result}
        assert net[(ADDR_ALICE, TOKEN_A)] == -120  # -100 - 50 + 30
        assert net[(ADDR_BOB, TOKEN_A)] == 120     # +100 + 50 - 30

    def test_self_transfer_skipped(self) -> None:
        """Self-transfer → net zero, skipped."""
        deltas = [
            TransferDelta(TOKEN_A, ADDR_ALICE, ADDR_ALICE, 100, 1, 0, 0),
        ]
        result = apply_deltas(deltas)
        assert len(result) == 0

    def test_multiple_tokens(self) -> None:
        """Different tokens are tracked independently."""
        deltas = [
            TransferDelta(TOKEN_A, ADDR_ALICE, ADDR_BOB, 100, 1, 0, 0),
            TransferDelta(TOKEN_B, ADDR_ALICE, ADDR_BOB, 200, 1, 1, 0),
        ]
        result = apply_deltas(deltas)
        net = {(a, t): d for a, t, d in result}
        assert net[(ADDR_ALICE, TOKEN_A)] == -100
        assert net[(ADDR_ALICE, TOKEN_B)] == -200
        assert net[(ADDR_BOB, TOKEN_A)] == 100
        assert net[(ADDR_BOB, TOKEN_B)] == 200
