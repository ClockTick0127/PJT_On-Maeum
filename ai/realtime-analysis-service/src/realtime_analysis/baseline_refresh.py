"""baseline 주기적 조회"""

from __future__ import annotations

import logging
from threading import Event, Thread

from realtime_analysis.baseline import SqlAlchemyBaselineRepository


logger = logging.getLogger(__name__)


class BaselineCacheRefresher:
    def __init__(
        self,
        repository: SqlAlchemyBaselineRepository,
        interval_seconds: float,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be greater than zero")

        self._repository = repository
        self._interval_seconds = interval_seconds
        self._stop_event = Event()
        self._thread: Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("Baseline cache refresher is already running")

        self._stop_event.clear()
        self._thread = Thread(
            target=self._run,
            name="baseline-cache-refresh",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()

        thread = self._thread
        if thread is None:
            return

        thread.join(timeout=5)

        if thread.is_alive():
            logger.warning("Baseline refresh is still finishing")
            return

        self._thread = None

    def refresh_once(self) -> bool:
        try:
            self._repository.refresh()
        except Exception:
            logger.exception(
                "Baseline refresh failed; keeping the previous cache"
            )
            return False

        return True

    def _run(self) -> None:
        while not self._stop_event.wait(self._interval_seconds):
            self.refresh_once()