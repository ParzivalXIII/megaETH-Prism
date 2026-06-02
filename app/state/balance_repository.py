"""AssetBalanceRepository — upsert and query operations for asset balances.

Follows the Phase 1 ``StateRepository`` pattern with async session management.
Uses PostgreSQL ``INSERT ... ON CONFLICT DO UPDATE`` for idempotent upserts.

Design:
- Composite PK ``(user_address, token_address)``.
- ``bulk_upsert`` uses raw SQL for performance (no ORM overhead).
- ``apply_deltas_and_upsert`` reads current balances, applies net changes,
  and writes back.  Negative balances are clamped to 0 with a WARNING.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import get_logger
from app.core.metrics import (
    asset_balances_queried_total,
    asset_balances_upserted_total,
)
from app.state.models import AssetBalance

logger = get_logger("megaeth.state.balance_repository")


class AssetBalanceRepository:
    """Repository for asset balance upserts and queries.

    Parameters
    ----------
    session : AsyncSession
        SQLAlchemy async session (from session factory).
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert(self, balance: AssetBalance) -> None:
        """Idempotent upsert using PostgreSQL ``ON CONFLICT DO UPDATE``.

        If a row with the same ``(user_address, token_address)`` exists,
        the ``balance_raw``, ``token_symbol``, ``block_number``, and
        ``updated_at`` fields are overwritten.
        """
        global asset_balances_upserted_total
        stmt = pg_insert(AssetBalance).values(
            user_address=balance.user_address.lower(),
            token_address=balance.token_address.lower(),
            token_symbol=balance.token_symbol,
            balance_raw=balance.balance_raw,
            decimals=balance.decimals,
            block_number=balance.block_number,
        ).on_conflict_do_update(
            index_elements=["user_address", "token_address"],
            set_={
                "balance_raw": balance.balance_raw,
                "token_symbol": balance.token_symbol,
                "block_number": balance.block_number,
                "updated_at": datetime.utcnow(),
            },
        )
        await self._session.execute(stmt)
        await self._session.commit()
        asset_balances_upserted_total += 1

    async def get_portfolio(
        self, user_address: str
    ) -> list[AssetBalance]:
        """Get all asset balances for a user.

        Returns all rows where ``user_address`` matches.
        """
        global asset_balances_queried_total
        user_address = user_address.lower()
        stmt = select(AssetBalance).where(
            AssetBalance.user_address == user_address  # type: ignore[arg-type]
        )
        result = await self._session.execute(stmt)
        asset_balances_queried_total += 1
        return list(result.scalars().all())

    async def get_balance(
        self, user_address: str, token_address: str
    ) -> AssetBalance | None:
        """Get a single asset balance by composite PK.

        Returns ``None`` if the row does not exist.
        """
        global asset_balances_queried_total
        user_address = user_address.lower()
        token_address = token_address.lower()
        stmt = select(AssetBalance).where(
            AssetBalance.user_address == user_address,  # type: ignore[arg-type]
            AssetBalance.token_address == token_address,  # type: ignore[arg-type]
        )
        result = await self._session.execute(stmt)
        asset_balances_queried_total += 1
        return result.scalar_one_or_none()

    async def bulk_upsert(self, balances: list[AssetBalance]) -> int:
        """Efficient bulk upsert using raw SQL.

        Uses a single ``INSERT ... ON CONFLICT DO UPDATE`` statement
        for all balances, avoiding per-row ORM overhead.

        Returns the count of upserted rows.
        """
        global asset_balances_upserted_total
        if not balances:
            return 0

        # Build VALUES clause using parameterised placeholders
        value_rows = []
        params: dict[str, str | int] = {}
        for i, b in enumerate(balances):
            prefix = f"b{i}"
            value_rows.append(
                f"(:{prefix}_ua, :{prefix}_ta, :{prefix}_ts, :{prefix}_br, "
                f":{prefix}_dc, :{prefix}_bn, NOW())"
            )
            params.update(
                {
                    f"{prefix}_ua": b.user_address.lower(),
                    f"{prefix}_ta": b.token_address.lower(),
                    f"{prefix}_ts": b.token_symbol,
                    f"{prefix}_br": b.balance_raw,
                    f"{prefix}_dc": b.decimals,
                    f"{prefix}_bn": b.block_number,
                }
            )

        sql = text(
            f"""
            INSERT INTO asset_balances
                (user_address, token_address, token_symbol,
                 balance_raw, decimals, block_number, updated_at)
            VALUES {', '.join(value_rows)}
            ON CONFLICT (user_address, token_address) DO UPDATE SET
                balance_raw = EXCLUDED.balance_raw,
                token_symbol = EXCLUDED.token_symbol,
                block_number = EXCLUDED.block_number,
                updated_at = EXCLUDED.updated_at
            """
        )

        await self._session.execute(sql, params)
        await self._session.commit()
        asset_balances_upserted_total += len(balances)
        return len(balances)

    async def apply_deltas_and_upsert(
        self,
        deltas: list[tuple[str, str, int]],
        block_number: int,
    ) -> int:
        """Apply net Transfer deltas to current balances and upsert.

        Reads current balances for each ``(user, token)`` pair, applies
        net changes, and writes back.  Negative balances are clamped to 0
        with a WARNING (the delta is partially applied up to the available
        balance; the remaining deficit is discarded).

        Args:
            deltas: List of ``(user_address, token_address, net_change)``
                    tuples from ``apply_deltas()``.
            block_number: The block number to record on updated rows.

        Returns:
            Number of rows upserted.
        """
        global asset_balances_upserted_total
        if not deltas:
            return 0

        # Step 1: Normalise addresses to lowercase
        normalized: list[tuple[str, str, int]] = []
        for addr, token, change in deltas:
            normalized.append((addr.lower(), token.lower(), change))

        # Step 2: Aggregate deltas with same (user, token) pair
        # This avoids PostgreSQL CardinalityViolationError from
        # duplicate PKs in a single INSERT ... ON CONFLICT statement.
        aggregated: dict[tuple[str, str], int] = {}
        for addr, token, change in normalized:
            key = (addr, token)
            aggregated[key] = aggregated.get(key, 0) + change

        if not aggregated:
            return 0

        # Step 3: Read current balances (preserving token_symbol and decimals)
        current_map: dict[tuple[str, str], AssetBalance | None] = {}
        for user_addr, token_addr in aggregated:
            bal = await self.get_balance(user_addr, token_addr)
            current_map[(user_addr, token_addr)] = bal

        # Step 4: Apply aggregated deltas
        updated_balances: list[AssetBalance] = []
        for (user_addr, token_addr), net_change in aggregated.items():
            existing = current_map.get((user_addr, token_addr))
            current_raw = existing.balance_raw if existing else 0
            new_raw = current_raw + net_change

            if new_raw < 0:
                logger.warning(
                    "balance would go negative, clamping to 0",
                    user_address=user_addr,
                    token_address=token_addr,
                    current_raw=current_raw,
                    net_change=net_change,
                )
                new_raw = 0

            # Preserve existing token metadata when applying deltas
            updated_balances.append(
                AssetBalance(
                    user_address=user_addr,
                    token_address=token_addr,
                    token_symbol=existing.token_symbol if existing else "",
                    decimals=existing.decimals if existing else 18,
                    balance_raw=new_raw,
                    block_number=block_number,
                )
            )

        # Step 5: Bulk upsert
        return await self.bulk_upsert(updated_balances)
