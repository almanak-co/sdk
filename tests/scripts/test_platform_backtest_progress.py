from unittest.mock import Mock

import requests

from almanak.framework.backtesting.pnl.progress import BacktestProgress
from scripts.platform_backtest_progress import ProgressReporter


def test_coalesces_ticks_and_only_sends_new_observations(monkeypatch):
    post = Mock()
    monkeypatch.setattr("scripts.platform_backtest_progress.requests.post", post)
    reporter = ProgressReporter("https://platform/internal/backtest/run/progress", "secret")
    for tick in range(100):
        reporter.observe(BacktestProgress("simulating", tick, 100, tick * 1000))
    post.assert_not_called()
    reporter._send_latest()
    reporter._send_latest()
    post.assert_called_once()
    payload = post.call_args.kwargs["json"]
    assert payload["completed_ticks"] == 99
    assert payload["simulation_elapsed_ms"] == 99000
    assert payload["observed_at"].endswith("+00:00")
    assert post.call_args.kwargs["timeout"] == (2, 2)


def test_old_backend_or_network_failure_does_not_fail_run(monkeypatch):
    post = Mock(side_effect=requests.HTTPError("404"))
    monkeypatch.setattr("scripts.platform_backtest_progress.requests.post", post)
    reporter = ProgressReporter("https://platform/progress", "secret")
    reporter.observe(BacktestProgress("preparing"))
    reporter._send_latest()
    post.side_effect = None
    reporter.observe(BacktestProgress("saving_results"))
    reporter._send_latest()
    assert post.call_count == 2


def test_reporter_shutdown_is_nonblocking(monkeypatch):
    from threading import Event

    entered, release = Event(), Event()

    def blocked_post(*args, **kwargs):
        entered.set()
        release.wait(5)
        return Mock()

    monkeypatch.setattr("scripts.platform_backtest_progress.requests.post", blocked_post)
    reporter = ProgressReporter("https://platform/progress", "secret", interval=0.001)
    try:
        with reporter:
            reporter.observe(BacktestProgress("simulating", 1, 100))
            assert entered.wait(2)
        assert reporter._stop.is_set()
        assert reporter._thread.is_alive()
    finally:
        release.set()
        reporter._thread.join(2)
    assert not reporter._thread.is_alive()
