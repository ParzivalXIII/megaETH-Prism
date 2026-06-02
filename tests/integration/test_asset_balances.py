"""Integration tests for AssetBalanceRepository (T6).

Tests require a running PostgreSQL instance reachable via
``settings.postgres_url``.

Start with::

    docker compose up -d postgres
"""

from __future__ import annotations

import pytest
import pytest_asyncio
from sqlalchemy import text as truncate_text
from sqlalchemy.ext.asyncio import AsyncSession

from app.state.balance_repository import AssetBalanceRepository
from app.state.database import create_async_engine_pool, create_sync_engine
from app.state.models import AssetBalance

pytestmark = pytest.mark.asyncio

# Test addresses
ADDR_A = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
ADDR_B = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
TOKEN_T1 = "0x1111111111111111111111111111111111111111"
TOKEN_T2 = "0x2222222222222222222222222222222222222222"
TOKEN_T3 = "0x3333333333333333333333333333333333333333"


@pytest_asyncio.fixture
async def db_session() -> AsyncSession:
    """Create a test database session with auto-cleanup."""
    # Initialize tables
    sync_engine = create_sync_engine()
    AssetBalance.metadata.create_all(sync_engine)
    sync_engine.dispose()

    engine, session_factory = create_async_engine_pool()

    # Truncate any leftover data
    try:
        async with session_factory() as truncate_session:
            await truncate_session.execute(
                truncate_text(
                    "TRUNCATE TABLE asset_balances RESTART IDENTITY CASCADE"
                )
            )
            await truncate_session.commit()
    except Exception:
        pass

    async with session_factory() as session:
        yield session

    # Cleanup
    try:
        async with session_factory() as truncate_session:
            await truncate_session.execute(
                truncate_text(
                    "TRUNCATE TABLE asset_balances RESTART IDENTITY CASCADE"
                )
            )
            await truncate_session.commit()
    except Exception:
        pass

    await engine.dispose()


def _balance(
    user_address: str,
    token_address: str,
    balance_raw: int = 100,
    decimals: int = 18,
    block_number: int = 1,
    token_symbol: str = "TEST",
) -> AssetBalance:
    return AssetBalance(
        user_address=user_address,
        token_address=token_address,
        balance_raw=balance_raw,
        decimals=decimals,
        block_number=block_number,
        token_symbol=token_symbol,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_upsert_new_balance(db_session: AsyncSession) -> None:
    """New (user, token) pair → 1 row created."""
    repo = AssetBalanceRepository(db_session)
    bal = _balance(ADDR_A, TOKEN_T1, balance_raw=1000)
    await repo.upsert(bal)

    result = await repo.get_balance(ADDR_A, TOKEN_T1)
    assert result is not None
    assert result.balance_raw == 1000
    assert result.decimals == 18


async def test_upsert_idempotent(db_session: AsyncSession) -> None:
    """Same pair upserted twice → 1 row, second overwrites."""
    repo = AssetBalanceRepository(db_session)
    bal1 = _balance(ADDR_A, TOKEN_T1, balance_raw=1000)
    await repo.upsert(bal1)

    bal2 = _balance(ADDR_A, TOKEN_T1, balance_raw=2000)
    await repo.upsert(bal2)

    result = await repo.get_balance(ADDR_A, TOKEN_T1)
    assert result is not None
    assert result.balance_raw == 2000

    # Only one row
    portfolio = await repo.get_portfolio(ADDR_A)
    assert len(portfolio) == 1


async def test_get_portfolio(db_session: AsyncSession) -> None:
    """Upsert 3 tokens for user → get_portfolio returns 3 rows."""
    repo = AssetBalanceRepository(db_session)
    for token, raw in [(TOKEN_T1, 100), (TOKEN_T2, 200), (TOKEN_T3, 300)]:
        await repo.upsert(_balance(ADDR_A, token, balance_raw=raw))

    portfolio = await repo.get_portfolio(ADDR_A)
    assert len(portfolio) == 3
    raw_values = {b.token_address: b.balance_raw for b in portfolio}
    assert raw_values[TOKEN_T1] == 100
    assert raw_values[TOKEN_T2] == 200
    assert raw_values[TOKEN_T3] == 300


async def test_get_balance_single(db_session: AsyncSession) -> None:
    """Upsert 1 → get_balance returns it; unknown pair returns None."""
    repo = AssetBalanceRepository(db_session)
    await repo.upsert(_balance(ADDR_A, TOKEN_T1, balance_raw=42))

    result = await repo.get_balance(ADDR_A, TOKEN_T1)
    assert result is not None
    assert result.balance_raw == 42

    unknown = await repo.get_balance(ADDR_A, TOKEN_T2)
    assert unknown is None


async def test_bulk_upsert(db_session: AsyncSession) -> None:
    """Bulk upsert 30 rows → count=30, all queryable."""
    repo = AssetBalanceRepository(db_session)
    balances = [
        _balance(
            f"0x{i:040x}", TOKEN_T1, balance_raw=i * 10, block_number=1
        )
        for i in range(30)
    ]
    count = await repo.bulk_upsert(balances)
    assert count == 30

    # Query one
    result = await repo.get_balance(f"0x{'00' * 19}01", TOKEN_T1)
    assert result is not None


async def test_delta_application(db_session: AsyncSession) -> None:
    """Apply deltas of +100 and -50 for same (user, token) → net +50, balance 150."""
    repo = AssetBalanceRepository(db_session)
    # Start with balance 100
    await repo.upsert(_balance(ADDR_A, TOKEN_T1, balance_raw=100))

    # Apply deltas: +100 and -50 (net = +50)
    deltas = [
        (ADDR_A, TOKEN_T1, 100),
        (ADDR_A, TOKEN_T1, -50),
    ]
    count = await repo.apply_deltas_and_upsert(deltas, block_number=2)
    assert count == 1

    result = await repo.get_balance(ADDR_A, TOKEN_T1)
    assert result is not None
    assert result.balance_raw == 150  # 100 + 100 - 50 = 150


async def test_address_case_normalization(db_session: AsyncSession) -> None:
    """Upsert with uppercase → query with lowercase → row found."""
    repo = AssetBalanceRepository(db_session)
    upper_addr = "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    await repo.upsert(
        _balance(upper_addr, TOKEN_T1, balance_raw=999)
    )

    result = await repo.get_balance(ADDR_A, TOKEN_T1)
    assert result is not None
    assert result.balance_raw == 999


async def test_negative_balance_clamped(db_session: AsyncSession) -> None:
    """Delta that would make balance negative → clamped to 0 with warning."""
    repo = AssetBalanceRepository(db_session)
    # Start with balance 50
    await repo.upsert(_balance(ADDR_A, TOKEN_T1, balance_raw=50))

    # Apply delta that would go negative: -100
    deltas = [(ADDR_A, TOKEN_T1, -100)]
    count = await repo.apply_deltas_and_upsert(deltas, block_number=2)
    assert count == 1

    result = await repo.get_balance(ADDR_A, TOKEN_T1)
    assert result is not None
    assert result.balance_raw == 0  # Clamped, not -50
