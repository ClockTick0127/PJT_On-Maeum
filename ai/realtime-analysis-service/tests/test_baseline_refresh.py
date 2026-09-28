from unittest.mock import Mock

from realtime_analysis.baseline import SqlAlchemyBaselineRepository
from realtime_analysis.baseline_refresh import BaselineCacheRefresher


def test_refresh_failure_keeps_refresher_available_for_retry() -> None:
    repository = Mock(spec=SqlAlchemyBaselineRepository)
    repository.refresh.side_effect = [RuntimeError("temporary failure"), None]
    refresher = BaselineCacheRefresher(repository, interval_seconds=60)

    assert refresher.refresh_once() is False
    assert refresher.refresh_once() is True
    assert repository.refresh.call_count == 2


def test_stop_interrupts_the_interval_wait() -> None:
    repository = Mock(spec=SqlAlchemyBaselineRepository)
    refresher = BaselineCacheRefresher(repository, interval_seconds=3600)

    refresher.start()
    refresher.stop()

    repository.refresh.assert_not_called()
