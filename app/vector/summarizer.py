from app.schemas.mini_block import MiniBlockPayload


def build_summary(payload: MiniBlockPayload) -> str:
    """Build a human-readable text summary of a mini-block.

    Template: "Block #{block_number}: N transactions, X gas used, timestamp T"
    Max 500 characters. No transaction-level decoding.
    """
    tx_count = len(payload.transactions)
    if tx_count == 0:
        return f"Block #{payload.block_number}: empty"

    return (
        f"Block #{payload.block_number}: {tx_count} transactions, "
        f"{payload.gas_used} gas used, timestamp {payload.block_timestamp}"
    )[:500]
