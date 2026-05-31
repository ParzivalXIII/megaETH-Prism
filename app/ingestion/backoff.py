import time


class ExponentialBackoff:
    """Exponential backoff for WebSocket reconnection.

    Resets to base delay after any successful connection that lasts
    longer than ``stable_threshold`` seconds. On repeated rapid
    disconnects, continues exponential progression.
    """

    def __init__(
        self,
        base: float = 1.0,
        cap: float = 16.0,
        stable_threshold: float = 10.0,
    ):
        self.base = base
        self.cap = cap
        self.stable_threshold = stable_threshold
        self._attempt = 0
        self._last_connect_time: float | None = None

    def delay(self) -> float:
        """Return the current backoff delay in seconds."""
        return min(self.base * (2**self._attempt), self.cap)

    def record_attempt(self) -> None:
        """Call after a failed reconnection attempt to increase backoff."""
        self._attempt += 1

    def record_success(self) -> None:
        """Call after a successful connection.

        If the connection was stable (lasted > stable_threshold),
        reset the backoff.
        """
        now = time.monotonic()
        if self._last_connect_time is not None:
            duration = now - self._last_connect_time
            if duration >= self.stable_threshold:
                self._attempt = 0
        self._last_connect_time = now

    def reset(self) -> None:
        """Manually reset backoff to base."""
        self._attempt = 0

    @property
    def attempt(self) -> int:
        return self._attempt
