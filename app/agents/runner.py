"""AgentRunner — async poll loop that invokes the LangGraph agent on a schedule.

Follows the Phase 1/2 worker pattern exactly:
- ``asyncio.Event`` for graceful shutdown
- ``asyncio.Lock`` for single-invocation guarantee
- ``asyncio.create_task`` for background execution
- ``asyncio.wait_for`` for controlled shutdown timeout

The runner alternates between two poll intervals:
- ``interval_sec`` (default 60s): when the last run produced an execution payload
- ``idle_backoff_sec`` (default 300s): when the last run produced nothing (NO_ACTION)
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

from langgraph.graph.state import CompiledStateGraph

from app.core import metrics
from app.core.logging import get_logger

logger = get_logger("megaeth.agents.runner")


class AgentRunner:
    """Background poll loop that periodically invokes the LangGraph agent.

    Parameters
    ----------
    graph : CompiledStateGraph
        The compiled LangGraph to invoke.
    context : dict
        Runtime context passed as ``config["configurable"]``.
    interval_sec : int, optional
        Active poll interval (default: from settings).
    idle_backoff_sec : int, optional
        Idle poll interval (default: from settings).
    """

    def __init__(
        self,
        graph: CompiledStateGraph,
        context: dict[str, Any],
        interval_sec: int | None = None,
        idle_backoff_sec: int | None = None,
    ) -> None:
        from app.core.config import settings as cfg

        self._graph = graph
        self._context = context
        self._interval_sec = interval_sec or cfg.agent_poll_interval_sec
        self._idle_backoff_sec = idle_backoff_sec or cfg.agent_poll_idle_backoff_sec

        self._shutdown_event = asyncio.Event()
        self._invocation_lock = asyncio.Lock()
        self._task: asyncio.Task[Any] | None = None
        self._last_had_payload = False

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def shutdown_event(self) -> asyncio.Event:
        """Public shutdown event for supervisor integration."""
        return self._shutdown_event

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the background poll loop as an asyncio task."""
        if self._task is not None:
            logger.warning("AgentRunner already started")
            return
        self._task = asyncio.create_task(
            self._poll_loop(),
            name="agent-runner",
        )
        logger.info(
            "AgentRunner started",
            interval_sec=self._interval_sec,
            idle_backoff_sec=self._idle_backoff_sec,
        )

    async def stop(self) -> None:
        """Gracefully stop the poll loop.

        Sets the shutdown event and waits up to 30 seconds for the
        poll task to finish its current invocation.
        """
        logger.info("AgentRunner stopping")
        self._shutdown_event.set()

        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=30.0)
            except asyncio.TimeoutError:
                logger.warning("AgentRunner shutdown timed out, cancelling task")
                self._task.cancel()
                try:
                    await self._task
                except (asyncio.CancelledError, Exception):
                    pass

        self._task = None
        logger.info("AgentRunner stopped")

    # ------------------------------------------------------------------
    # Poll loop
    # ------------------------------------------------------------------

    async def _poll_loop(self) -> None:
        """Main poll loop: wait → acquire lock → invoke graph → update metrics.

        Uses the idle backoff interval when the last invocation did not
        produce a payload (NO_ACTION path).  Switches to the active
        interval when a payload was produced.
        """
        logger.debug("poll loop started")

        while not self._shutdown_event.is_set():
            # Determine wait interval
            wait_sec = self._idle_backoff_sec if not self._last_had_payload else self._interval_sec

            try:
                # Wait for interval or shutdown
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=wait_sec,
                )
                # Shutdown was signalled
                break
            except asyncio.TimeoutError:
                # Timeout is normal — proceed to poll
                pass

            # Acquire lock (single invocation guarantee)
            if self._invocation_lock.locked():
                logger.debug("previous invocation still running, skipping")
                continue

            async with self._invocation_lock:
                await self._invoke_graph()

        logger.debug("poll loop stopped")

    async def _invoke_graph(self) -> None:
        """Invoke the LangGraph agent and update metrics."""
        metrics.agent_invocations_total += 1
        start_time = time.monotonic()

        try:
            # Build initial state
            initial_state = {
                "messages": [],
                "analysis_result": None,
                "execution_payload": None,
                "user_address": self._context.get("user_address", ""),
                "error": None,
                "current_phase": "market_analysis",
            }

            # Invoke graph
            result = await self._graph.ainvoke(
                initial_state,
                config={"configurable": self._context},
            )

            # Check if a payload was produced
            payload = result.get("execution_payload")
            self._last_had_payload = payload is not None

            if payload:
                logger.info(
                    "agent produced execution payload",
                    payload_id=payload.get("id", "") if isinstance(payload, dict) else str(payload),
                )

            metrics.agent_invocations_success_total += 1

        except Exception as e:
            metrics.agent_invocations_error_total += 1
            self._last_had_payload = False
            logger.error("agent invocation failed", error=str(e), exc_info=True)

        finally:
            metrics.agent_loop_duration_seconds = time.monotonic() - start_time
