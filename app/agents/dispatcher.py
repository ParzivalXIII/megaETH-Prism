"""TransactionExecutor & WorkerPool — low-latency transaction dispatch daemon.

Consumes execution plans from the Redis STREAM (``ExecutionQueue``) and
broadcasts them to MegaETH via HTTP RPC.

Architecture:
- ``TransactionExecutor`` — signs and broadcasts a single transaction.
- ``WorkerPool`` — manages async workers that consume from the execution
  stream and delegate to the executor.

Key design decisions:
- Single-worker (``worker_count=1``) in Phase 2 to avoid nonce collisions
  from a single ``PRIVATE_KEY``.
- ``asyncio.Semaphore`` for concurrency control within the worker.
- ``asyncio.Event`` for graceful shutdown (matching Phase 1 pattern).
- ``eth_sendRawTransactionSync`` (EIP-7966) preferred; falls back to
  standard ``eth_sendRawTransaction`` if sync variant is unavailable.
- Hardcoded gas limits per MegaETH gas model (0.001 gwei base fee).
"""

from __future__ import annotations

import asyncio
from typing import Any

import redis.asyncio as redis

from app.agents.dispatch import ExecutionQueue
from app.core.config import settings
from app.core.logging import get_logger
from app.core.metrics import (
    dispatcher_intents_executed_total,
    dispatcher_intents_failed_total,
    dispatcher_intents_received_total,
    dispatcher_intents_rejected_total,
)
from app.schemas.intent import ExecutionPayload, TriggerCondition

logger = get_logger("megaeth.agents.dispatcher")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

HARDCODED_GAS_LIMIT: int = 500_000
"""Gas limit for transactions (MegaETH: base fee ~0.001 gwei, intrinsic ~60K)."""


# ---------------------------------------------------------------------------
# TransactionExecutor
# ---------------------------------------------------------------------------


class TransactionExecutor:
    """Signs and broadcasts a single transaction to MegaETH.

    Usage:
        executor = TransactionExecutor()
        result = await executor.execute(payload)
    """

    def __init__(self, web3_provider: Any | None = None) -> None:
        self._w3: Any | None = web3_provider
        self._nonce_lock = asyncio.Lock()
        self._nonce: int | None = None
        self._account: Any | None = None

    # ------------------------------------------------------------------
    # Web3 connection (lazy)
    # ------------------------------------------------------------------

    def _get_w3(self) -> Any:
        """Lazy-initialise the AsyncWeb3 client."""
        if self._w3 is None:
            from web3 import AsyncWeb3
            from web3.providers.rpc import AsyncHTTPProvider

            provider = AsyncHTTPProvider(settings.rpc_url)
            self._w3 = AsyncWeb3(provider)
        return self._w3

    def _get_account(self) -> Any:
        """Lazy-load the account from the private key."""
        if self._account is None:
            from web3 import Account

            pk = settings.private_key
            if not pk:
                raise ValueError(
                    "PRIVATE_KEY is not set. Set the PRIVATE_KEY environment variable."
                )
            pk = pk.strip()
            if not pk.startswith("0x"):
                pk = "0x" + pk
            if len(pk) != 66:
                raise ValueError(
                    f"PRIVATE_KEY must be 64 hex characters (0x-prefixed), "
                    f"got {len(pk)} chars"
                )
            try:
                int(pk, 16)
            except ValueError:
                raise ValueError("PRIVATE_KEY is not a valid hex string")

            acc = Account.from_key(pk)
            self._account = acc
            logger.info(
                "executor account loaded",
                address=acc.address,
            )
        return self._account

    # ------------------------------------------------------------------
    # Nonce management
    # ------------------------------------------------------------------

    async def _get_nonce(self, w3: Any) -> int:
        """Get the next nonce, protected by an asyncio lock.

        On first call per session, fetches the on-chain transaction count.
        Subsequent calls increment the counter (avoids repeated RPC calls).
        """
        async with self._nonce_lock:
            if self._nonce is None:
                account = self._get_account()
                self._nonce = await w3.eth.get_transaction_count(
                    account.address
                )
                logger.debug("initial nonce fetched", nonce=self._nonce)
            else:
                self._nonce += 1
            return self._nonce

    # ------------------------------------------------------------------
    # Condition checking
    # ------------------------------------------------------------------

    async def _check_conditions(self, condition: TriggerCondition) -> bool:
        """Validate trigger conditions.

        Phase 2 only evaluates ``condition_type="always"`` → ``True``.
        ``price_threshold`` and ``time_bound`` return ``False`` with a
        WARNING log (stubbed — price oracle integration deferred to Phase 3).
        """
        if condition.condition_type == "always":
            return True

        logger.warning(
            "trigger condition not evaluated in Phase 2",
            condition_type=condition.condition_type,
        )
        return False

    # ------------------------------------------------------------------
    # Transaction assembly, signing, and broadcast
    # ------------------------------------------------------------------

    async def _sign_transaction(self, tx: dict[str, Any], w3: Any) -> str:
        """Sign a transaction dict and return the raw signed hex."""
        account = self._get_account()
        signed = account.sign_transaction(tx)
        return str(signed.raw_transaction.hex())

    async def _broadcast(self, signed_tx_hex: str, w3: Any) -> str:
        """Broadcast a signed transaction and return the tx hash.

        Broadcast strategy (tried in order):
        1. ``eth_sendRawTransactionSync`` (EIP-7966) via web3.py 7.x direct API
        2. ``eth_sendRawTransactionSync`` via JSON-RPC ``make_request`` fallback
        3. Standard ``eth_sendRawTransaction`` (produces a pending tx hash,
           caller must poll for receipt)
        """
        # Attempt 1: EIP-7966 via web3.py 7.x direct API
        if hasattr(w3.eth, "send_raw_transaction_sync"):
            try:
                sync_hash = await w3.eth.send_raw_transaction_sync(signed_tx_hex)
                return sync_hash.hex() if hasattr(sync_hash, "hex") else str(sync_hash)
            except Exception:
                logger.debug("send_raw_transaction_sync failed, trying make_request")

        # Attempt 2: EIP-7966 via JSON-RPC make_request
        try:
            rpc_result = await w3.provider.make_request(
                "eth_sendRawTransactionSync", [signed_tx_hex]
            )
            rpc_tx_hash: str = str(rpc_result.get("result", ""))
            if rpc_tx_hash:
                return rpc_tx_hash
        except Exception:
            logger.debug(
                "eth_sendRawTransactionSync via make_request failed, "
                "falling back to standard send_raw_transaction"
            )

        # Attempt 3: Standard broadcast (polling required)
        tx_hash = await w3.eth.send_raw_transaction(signed_tx_hex)
        return tx_hash.hex() if hasattr(tx_hash, "hex") else str(tx_hash)

    # ------------------------------------------------------------------
    # Execute
    # ------------------------------------------------------------------

    async def execute(
        self, payload: ExecutionPayload
    ) -> dict[str, Any]:
        """Execute a single transaction plan.

        Pipeline:
        1. Check trigger conditions.
        2. Assemble transaction dict.
        3. Get nonce.
        4. Sign and broadcast.

        Returns:
            ``{"status": "success", "tx_hash": "0x..."}`` on success, or
            ``{"status": "failed"/"rejected"/"skipped", "reason": "..."}``
            on failure.
        """
        global dispatcher_intents_received_total
        global dispatcher_intents_executed_total
        global dispatcher_intents_failed_total
        global dispatcher_intents_rejected_total
        dispatcher_intents_received_total += 1

        # Step 1: Check trigger conditions
        if not await self._check_conditions(payload.trigger_condition):
            dispatcher_intents_rejected_total += 1
            return {
                "status": "skipped",
                "reason": f"trigger condition '{payload.trigger_condition.condition_type}' not met",
            }

        try:
            w3 = self._get_w3()
            account = self._get_account()

            # Step 2: Assemble transaction
            nonce = await self._get_nonce(w3)
            tx: dict[str, Any] = {
                "from": account.address,
                "to": payload.target_contract,
                "data": payload.call_data,
                "value": 0,  # No ETH transfer in Phase 2
                "gasPrice": payload.max_gas_price,
                "gas": HARDCODED_GAS_LIMIT,
                "nonce": nonce,
                "chainId": settings.chain_id,
            }

            # Step 3: Sign
            signed_hex = await self._sign_transaction(tx, w3)

            # Step 4: Broadcast
            tx_hash = await self._broadcast(signed_hex, w3)

            dispatcher_intents_executed_total += 1
            logger.info(
                "transaction executed",
                target=payload.target_contract,
                tx_hash=tx_hash,
                nonce=nonce,
            )
            return {"status": "success", "tx_hash": tx_hash}

        except Exception as e:
            dispatcher_intents_failed_total += 1
            logger.error(
                "transaction execution failed",
                target=payload.target_contract,
                error=str(e),
                exc_info=True,
            )
            return {"status": "failed", "reason": str(e)}


# ---------------------------------------------------------------------------
# WorkerPool
# ---------------------------------------------------------------------------


class WorkerPool:
    """Pool of async workers consuming from the Redis execution stream.

    Phase 2 constrains to a **single worker** (``worker_count=1``) to avoid
    nonce collisions from a single ``PRIVATE_KEY``.  Multi-worker dispatch
    is deferred to a future phase with a nonce manager.

    Usage::

        pool = WorkerPool()
        await pool.start()
        # ... workers run in background ...
        await pool.stop()
    """

    def __init__(
        self,
        worker_count: int | None = None,
        max_concurrent_tx: int | None = None,
    ) -> None:
        self.worker_count = worker_count or settings.worker_pool_count
        self.max_concurrent_tx = (
            max_concurrent_tx or settings.max_concurrent_tx
        )
        self._shutdown_event = asyncio.Event()
        self._semaphore = asyncio.Semaphore(self.max_concurrent_tx)
        self._tasks: list[asyncio.Task[Any]] = []
        self._queue: ExecutionQueue | None = None
        self._executor = TransactionExecutor()
        self._redis_conn: redis.Redis | None = None

    @property
    def shutdown_event(self) -> asyncio.Event:
        """Public shutdown event for supervisor integration."""
        return self._shutdown_event

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def _get_redis(self) -> redis.Redis:
        """Lazy Redis connection."""
        if self._redis_conn is None:
            self._redis_conn = redis.from_url(  # type: ignore[no-untyped-call]
                settings.redis_url, decode_responses=True
            )
            await self._redis_conn.ping()
        return self._redis_conn

    async def start(self) -> None:
        """Start the worker pool: initialise queue and spawn workers.

        Validates ``PRIVATE_KEY`` on startup to fail fast on misconfiguration.
        """
        # Validate PRIVATE_KEY on startup (fail fast)
        try:
            self._executor._get_account()
        except ValueError as e:
            logger.error(
                "PRIVATE_KEY validation failed on startup",
                error=str(e),
            )
            raise

        r = await self._get_redis()
        self._queue = ExecutionQueue(redis_client=r)
        await self._queue.initialize()

        # Claim orphans on startup
        await self._claim_orphans()

        # Spawn workers (single worker in Phase 2)
        for i in range(self.worker_count):
            task = asyncio.create_task(
                self._worker(i), name=f"executor-{i}"
            )
            self._tasks.append(task)

        logger.info(
            "worker pool started",
            worker_count=self.worker_count,
            max_concurrent_tx=self.max_concurrent_tx,
        )

    async def stop(self) -> None:
        """Gracefully stop all workers.

        Sets the shutdown event, waits for in-flight tasks with 30s timeout,
        then cancels remaining tasks and closes Redis.
        """
        logger.info("worker pool stopping")
        self._shutdown_event.set()

        if self._tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._tasks, return_exceptions=True),
                    timeout=30.0,
                )
            except asyncio.TimeoutError:
                logger.warning("worker pool shutdown timed out, cancelling")
                for t in self._tasks:
                    t.cancel()

        if self._redis_conn:
            await self._redis_conn.close()

        logger.info("worker pool stopped")

    # ------------------------------------------------------------------
    # Worker loop
    # ------------------------------------------------------------------

    async def _worker(self, worker_id: int) -> None:
        """Single worker loop: consume → execute → set_result → ack."""
        queue = self._queue
        assert queue is not None

        logger.debug("worker started", worker_id=worker_id)

        while not self._shutdown_event.is_set():
            try:
                # Consume (blocking, with 1s timeout for shutdown responsiveness)
                async for entry_id, payload in queue.consume(block_ms=1000):
                    if self._shutdown_event.is_set():
                        break

                    # Throttle concurrent executions
                    async with self._semaphore:
                        result = await self._executor.execute(payload)

                        # Write result
                        if result["status"] == "success":
                            await queue.set_result(
                                payload.id,
                                "completed",
                                tx_hash=result.get("tx_hash", ""),
                            )
                        elif result["status"] in ("rejected", "skipped"):
                            await queue.set_result(
                                payload.id,
                                result["status"],
                                error=result.get("reason", ""),
                            )
                        else:
                            await queue.set_result(
                                payload.id,
                                "failed",
                                error=result.get("reason", ""),
                            )

                        # Ack the stream entry
                        await queue.ack(entry_id)

                        logger.debug(
                            "intent processed",
                            worker_id=worker_id,
                            execution_id=payload.id,
                            status=result["status"],
                        )

            except Exception as e:
                logger.error(
                    "worker error",
                    worker_id=worker_id,
                    error=str(e),
                    exc_info=True,
                )
                # Backoff on persistent errors
                await asyncio.sleep(1.0)

        logger.debug("worker stopped", worker_id=worker_id)

    # ------------------------------------------------------------------
    # Orphan recovery
    # ------------------------------------------------------------------

    async def _claim_orphans(self) -> None:
        """Reclaim orphaned messages on startup.

        Messages pending for >60s are claimed via XAUTOCLAIM.
        """
        queue = self._queue
        if queue is None:
            return
        try:
            claimed = await queue.claim_pending(min_idle_ms=60000)
            if claimed:
                logger.info(
                    "claimed orphaned execution intents", count=len(claimed)
                )
                for entry_id, payload in claimed:
                    result = await self._executor.execute(payload)
                    await queue.set_result(
                        payload.id,
                        "completed" if result["status"] == "success" else "failed",
                        tx_hash=result.get("tx_hash", ""),
                        error=result.get("reason", ""),
                    )
                    await queue.ack(entry_id)
        except Exception as e:
            logger.warning(
                "claim_orphans failed on startup",
                error=str(e),
            )
