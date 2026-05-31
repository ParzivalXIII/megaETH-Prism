"""StateRepository — PostgreSQL upsert operations for mini-block records.

Provides a repository layer over the ``mini_block_records`` table with
idempotent upsert semantics via ``session.merge()``.
"""

import json
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.schemas.mini_block import MiniBlockPayload
from app.schemas.state import MiniBlockRecord


class StateRepository:
    """Repository for PostgreSQL state operations.

    Handles upsert of mini-block records with ON CONFLICT DO UPDATE
    semantics via SQLAlchemy's ``merge()``.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert(self, record: MiniBlockRecord) -> None:
        """Upsert a mini-block record.

        Uses ``session.merge()`` which translates to
        ``ON CONFLICT (block_number, index) DO UPDATE`` — overwriting
        state modifications rapidly, preventing deadlocks and reducing
        storage requirements.

        Parameters
        ----------
        record : MiniBlockRecord
            The record to insert or update.
        """
        await self._session.merge(record)
        await self._session.commit()

    @staticmethod
    def record_from_payload(
        payload: MiniBlockPayload,
        raw_json: str,
    ) -> MiniBlockRecord:
        """Convert a validated :class:`MiniBlockPayload` into a :class:`MiniBlockRecord`.

        Parameters
        ----------
        payload : MiniBlockPayload
            The parsed mini-block payload.
        raw_json : str
            The original JSON string received from the stream.

        Returns
        -------
        MiniBlockRecord
            A record ready for upsert into the database.
        """
        return MiniBlockRecord(
            block_number=payload.block_number,
            index=payload.index,
            mini_block_number=payload.mini_block_number,
            block_timestamp=payload.block_timestamp,
            gas_used=payload.gas_used,
            tx_count=payload.tx_count,
            raw_jsonb=json.loads(raw_json) if isinstance(raw_json, str) else raw_json,
            ingested_at=datetime.utcnow(),
        )

    async def get_latest_block(self) -> int | None:
        """Get the highest block_number in the database.

        Returns
        -------
        int | None
            The maximum block number, or ``None`` if the table is empty.
        """
        result = await self._session.execute(
            text("SELECT MAX(block_number) FROM mini_block_records"),
        )
        return result.scalar()

    async def count(self) -> int:
        """Count total records in the mini_block_records table.

        Returns
        -------
        int
            The number of rows.
        """
        result = await self._session.execute(
            text("SELECT COUNT(*) FROM mini_block_records"),
        )
        value = result.scalar()
        # COUNT(*) always returns an integer, never None
        return value if value is not None else 0
