"""WebSocket ingestion daemon for MegaETH mini-blocks.

Connects to MegaETH's Realtime API, subscribes to miniBlocks,
and pushes raw payloads into a Redis Stream.

Zero business logic -- no transaction parsing, no decoding,
no validation beyond structural JSON well-formedness.
"""

from __future__ import annotations

import asyncio
import json
import signal
import time
from typing import Any

import redis.asyncio as redis
import websockets.exceptions
from websockets.asyncio.client import connect as ws_connect

from app.core import metrics
from app.core.config import settings
from app.core.logging import bind_correlation_id, get_logger
from app.ingestion.backoff import ExponentialBackoff
from app.schemas import (
    STREAM_FIELD_BLOCK_NUMBER,
    STREAM_FIELD_CORRELATION_ID,
    STREAM_FIELD_INGESTED_AT,
    STREAM_FIELD_RAW_PAYLOAD,
    MiniBlockPayload,
)

logger = get_logger("megaeth.ingestion")


class MegaETHIngestionDaemon:
    """WebSocket ingestion daemon for MegaETH mini-blocks.

    Lifecycle
    ---------
    1. ``connect()``  -- open Redis + WebSocket, subscribe to miniBlocks.
    2. ``run()``      -- message loop with automatic reconnection.
    3. ``shutdown()`` -- graceful stop via ``asyncio.Event``.

    Thread-safety
    -------------
    Not thread-safe.  Designed to run in a single asyncio event loop.
    """

    def __init__(self) -> None:
        self._redis: redis.Redis | None = None
        self._ws: Any = None
        self._shutdown_event = asyncio.Event()
        self._backoff = ExponentialBackoff(
            base=settings.reconnect_backoff_base,
            cap=settings.reconnect_backoff_cap,
        )
        self._block_counter = 0
        self._start_time = time.monotonic()
        self._subscription_method: str | None = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _connect_redis(self) -> redis.Redis:
        """Create async Redis connection and verify it's live."""
        r = redis.from_url(settings.redis_url, decode_responses=True)
        await r.ping()
        logger.info("connected to redis")
        return r

    async def _subscribe(self, method: str | None = None) -> str:
        """Subscribe to the blockchain event stream.

        Tries the configured subscription method, with fallback:
        - ``"miniBlocks"`` → MegaETH real-time mini-blocks
        - ``"newHeads"``  → Standard EVM new block headers (Anvil/Geth)
        - ``"auto"``      → detect backend via chain ID, then subscribe

        Returns the method that was successfully subscribed.
        """
        methods_to_try: list[str]
        configured = settings.subscription_type

        if method:
            methods_to_try = [method]
        elif configured == "auto":
            # Check chain ID first to avoid blind trial-and-error
            if settings.chain_id == 31337:
                # Default Anvil chain ID — skip miniBlocks attempt
                methods_to_try = ["newHeads"]
            else:
                # Try miniBlocks first (MegaETH), fall back to newHeads
                methods_to_try = ["miniBlocks", "newHeads"]
        else:
            methods_to_try = [configured]

        last_error: Exception | None = None
        for sub_method in methods_to_try:
            try:
                subscribe_msg: dict[str, Any] = {
                    "jsonrpc": "2.0",
                    "method": "eth_subscribe",
                    "params": [sub_method],
                    "id": 1,
                }
                await self._ws.send(json.dumps(subscribe_msg))

                # Wait for the subscription confirmation response
                response = await asyncio.wait_for(
                    self._ws.recv(), timeout=5.0
                )
                resp_data = json.loads(response)
                if "result" in resp_data or resp_data.get("error") is None:
                    logger.info(
                        "subscribed to %s",
                        sub_method,
                        result=resp_data.get("result"),
                    )
                    self._subscription_method = sub_method

                    # Warn if using newHeads — data limitations
                    if sub_method == "newHeads":
                        logger.warning(
                            "Using newHeads subscription: transactions will be "
                            "hashes only (not full objects), receipts unavailable. "
                            "Use a MegaETH endpoint (chain_id 4326/6343) with "
                            "subscription_type=miniBlocks for full block data."
                        )

                    return sub_method
                else:
                    last_error = Exception(
                        f"subscribe to {sub_method} failed: {resp_data.get('error')}"
                    )
                    logger.warning("subscribe failed", sub_method=sub_method)
            except Exception as e:
                last_error = e
                logger.warning(
                    "subscribe to %s failed, trying next", sub_method, error=str(e)
                )

        raise ConnectionError(
            f"Failed to subscribe to any method: {methods_to_try}. "
            f"Last error: {last_error}"
        ) from last_error

    async def _keepalive_loop(self) -> None:
        """Send eth_chainId every ``keepalive_interval_sec`` seconds.

        Keepalive uses a separate async task so it does not block the
        message handler.  If sending fails the loop breaks and the
        caller will detect the connection drop via the message handler.
        """
        while not self._shutdown_event.is_set():
            try:
                keepalive_msg: dict[str, Any] = {
                    "jsonrpc": "2.0",
                    "method": "eth_chainId",
                    "params": [],
                    "id": int(time.time() * 1000),
                }
                await self._ws.send(json.dumps(keepalive_msg))
                metrics.ws_keepalive_sent_total += 1
                logger.debug("keepalive sent")
            except Exception:
                metrics.ws_keepalive_errors_total += 1
                logger.warning("keepalive send failed, breaking loop")
                break

            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=settings.keepalive_interval_sec,
                )
            except asyncio.TimeoutError:
                continue  # Timeout is expected -- loop back and send again

    @staticmethod
    def _normalize_block_data(
        result: dict[str, Any], method: str
    ) -> dict[str, Any]:
        """Normalize block data from different subscription formats.

        ``newHeads`` (standard EVM) uses different field names than
        ``miniBlocks`` (MegaETH).  This method normalizes to the
        ``MiniBlockPayload``-compatible format.
        """
        if method == "newHeads":
            # Standard EVM block header format
            return {
                "block_number": result.get("number", "0x0"),
                "block_timestamp": result.get("timestamp", "0x0"),
                "index": 0,
                "gas_used": int(result.get("gasUsed", "0x0"), 16),
                "transactions": result.get("transactions", []),
                "receipts": result.get("receipts", []),
                "mini_block_number": int(result.get("number", "0x0"), 16),
                "transaction_root": result.get("transactionsRoot"),
                "receipt_root": result.get("receiptsRoot"),
            }
        # miniBlocks — return as-is (already in the right format for
        # MiniBlockPayload which accepts hex strings for numeric fields)
        return result

    async def _message_handler(self) -> None:
        """Process incoming WebSocket messages.

        Filters for ``eth_subscription`` messages only.  Keepalive
        responses (JSON-RPC replies without ``method == "eth_subscription"``)
        are silently ignored.
        """
        async for message in self._ws:
            if self._shutdown_event.is_set():
                break

            # -- Parse JSON -------------------------------------------------
            try:
                parsed: dict[str, Any] = json.loads(message)
            except json.JSONDecodeError:
                metrics.ws_subscription_errors_total += 1
                logger.warning("malformed JSON from WS, skipping")
                continue

            # -- Filter: only eth_subscription notifications are miniBlocks --
            if parsed.get("method") != "eth_subscription":
                logger.debug(
                    "ignoring non-subscription message",
                    msg_id=parsed.get("id"),
                )
                continue

            # -- Extract and validate the block payload ---------------------
            try:
                params = parsed.get("params", {})
                result: dict[str, Any] = params.get("result", {})

                # Normalize if from standard EVM subscription
                sub_method = self._subscription_method or "miniBlocks"
                normalized = self._normalize_block_data(result, sub_method)

                # Structural validation via Pydantic
                try:
                    MiniBlockPayload.model_validate(normalized)
                except Exception as ve:
                    metrics.ws_subscription_errors_total += 1
                    logger.warning(
                        "block failed structural validation, skipping",
                        sub_method=sub_method,
                        error=str(ve),
                    )
                    continue

                # _redis is guaranteed to be set before _message_handler runs
                assert self._redis is not None

                # Store the NORMALIZED payload so downstream consumers
                # always get MiniBlockPayload-compatible JSON regardless
                # of whether the source was miniBlocks or newHeads.
                raw_json = json.dumps(normalized)
                correlation_id = bind_correlation_id()
                ingested_at = str(time.time())
                block_number = str(normalized["block_number"])

                stream_data: dict[str, str] = {
                    STREAM_FIELD_BLOCK_NUMBER: block_number,
                    STREAM_FIELD_RAW_PAYLOAD: raw_json,
                    STREAM_FIELD_INGESTED_AT: ingested_at,
                    STREAM_FIELD_CORRELATION_ID: correlation_id,
                }

                start = time.monotonic()
                await self._redis.xadd(
                    settings.stream_name,
                    stream_data,  # type: ignore[arg-type]
                    maxlen=settings.stream_maxlen,
                    approximate=True,
                )
                elapsed_ms = (time.monotonic() - start) * 1000  # ms

                metrics.blocks_ingested_total += 1
                self._block_counter += 1

                logger.info(
                    "miniBlock ingested",
                    block_number=result.get("block_number"),
                    stream_id=correlation_id,
                    xadd_ms=round(elapsed_ms, 2),
                )

            except Exception as e:
                logger.error(
                    "failed to process miniBlock",
                    error=str(e),
                    exc_info=True,
                )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Connect to the RPC WebSocket and Redis.

        Detects the subscription type if set to ``"auto"``.
        """
        self._redis = await self._connect_redis()

        logger.info("connecting to RPC WebSocket", url=settings.ws_url)

        self._ws = await ws_connect(
            settings.ws_url,
            ping_interval=None,  # We handle keepalive ourselves
            ping_timeout=None,
            close_timeout=5,
        )

        sub_method = await self._subscribe()
        self._backoff.record_success()
        logger.info(
            "connected to RPC WebSocket",
            url=settings.ws_url,
            subscription=sub_method,
        )

    async def _reconnect(self) -> None:
        """Reconnect to WebSocket with exponential backoff."""
        delay = self._backoff.delay()
        self._backoff.record_attempt()
        logger.warning(
            "disconnected, reconnecting",
            attempt=self._backoff.attempt,
            delay_sec=delay,
        )

        await asyncio.sleep(delay)

        try:
            if self._ws is not None:
                await self._ws.close()
        except Exception:
            pass

        await self.connect()

    async def run(self) -> None:
        """Main async loop.

        Connects to Redis + WebSocket, starts the keepalive task, then
        enters the message-processing loop.  On disconnection, applies
        exponential backoff and reconnects automatically.

        Returns when ``shutdown()`` is called.
        """
        logger.info(
            "ingestion daemon starting",
            ws_url=settings.ws_url,
            stream=settings.stream_name,
            subscription=settings.subscription_type,
        )

        await self.connect()

        keepalive_task = asyncio.create_task(self._keepalive_loop())

        while not self._shutdown_event.is_set():
            try:
                await self._message_handler()
            except websockets.exceptions.ConnectionClosed:
                metrics.ws_reconnects_total += 1
                logger.warning("WS connection closed")
                await self._reconnect()
                continue
            except Exception as e:
                metrics.ws_reconnects_total += 1
                logger.error(
                    "unexpected error in message loop",
                    error=str(e),
                    exc_info=True,
                )
                await self._reconnect()
                continue

        # -- Graceful shutdown ------------------------------------------
        keepalive_task.cancel()
        try:
            await keepalive_task
        except asyncio.CancelledError:
            pass

        if self._ws is not None:
            await self._ws.close()
        if self._redis is not None:
            await self._redis.close()

        uptime = time.monotonic() - self._start_time
        logger.info(
            "ingestion daemon stopped",
            blocks_ingested=self._block_counter,
            uptime_sec=round(uptime, 2),
        )

    async def shutdown(self) -> None:
        """Trigger graceful shutdown by setting the shutdown event."""
        logger.info("shutdown requested")
        self._shutdown_event.set()


# ======================================================================
# Supervisor entrypoint
# ======================================================================


async def main() -> None:
    """Entrypoint for the ingestion daemon.

    Called by the Wave 4 supervisor (``app.api.main``).
    Sets up signal handlers and runs the daemon.
    """
    daemon = MegaETHIngestionDaemon()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(
            sig,
            lambda: asyncio.create_task(daemon.shutdown()),
        )

    await daemon.run()


if __name__ == "__main__":
    asyncio.run(main())
