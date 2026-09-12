"""Failed client cleanup cannot replace or delay submitted-transaction evidence."""

import asyncio
from unittest.mock import AsyncMock, patch

import pytest
from web3 import AsyncHTTPProvider

from almanak.framework.execution.interfaces import SubmissionError
from almanak.framework.execution.submitter.public import PublicMempoolSubmitter
from almanak.gateway.utils.async_web3_cleanup import drain_failed_client_cleanup, schedule_failed_client_cleanup
from almanak.gateway.utils.rpc_provider import create_async_web3

TX = "0x" + "12" * 32


@pytest.mark.asyncio
async def test_disconnect_error_does_not_replace_original_initialization_error(caplog):
    provider = AsyncHTTPProvider("https://unused.invalid")
    provider.make_request = AsyncMock(side_effect=OSError("original transport failure"))
    with (
        patch("web3.AsyncHTTPProvider", return_value=provider),
        patch.object(provider, "disconnect", side_effect=RuntimeError("cleanup failure")),
        pytest.raises(OSError, match="original transport failure"),
    ):
        await create_async_web3("https://unused.invalid")
    await drain_failed_client_cleanup()
    assert "initialization cleanup failed error_type=RuntimeError" in caplog.text
    assert "cleanup failure" not in caplog.text


@pytest.mark.asyncio
async def test_slow_disconnect_cannot_extend_submitted_receipt_deadline():
    provider = AsyncHTTPProvider("https://unused.invalid")
    cleanup_started = asyncio.Event()
    cleanup_release = asyncio.Event()
    cleanup_finished = asyncio.Event()

    async def request(method, params):
        await asyncio.sleep(5)

    async def disconnect():
        cleanup_started.set()
        await cleanup_release.wait()
        cleanup_finished.set()

    provider.make_request = AsyncMock(side_effect=request)
    with (
        patch("web3.AsyncHTTPProvider", return_value=provider),
        patch.object(provider, "disconnect", side_effect=disconnect),
    ):
        task = asyncio.create_task(PublicMempoolSubmitter("https://unused.invalid").get_receipt(TX, timeout=0.02))
        try:
            await asyncio.wait_for(cleanup_started.wait(), timeout=1)
            await asyncio.sleep(0.05)
            assert task.done(), "Receipt timeout is waiting on transport cleanup"
            with pytest.raises(SubmissionError, match="Timeout waiting") as caught:
                await task
            assert caught.value.tx_hash == TX
            assert caught.value.recoverable is True
            assert not cleanup_finished.is_set()
        finally:
            cleanup_release.set()
            await asyncio.gather(task, return_exceptions=True)
            await asyncio.wait_for(cleanup_finished.wait(), timeout=1)


@pytest.mark.asyncio
async def test_cleanup_retains_provider_until_disconnect_finishes():
    import gc
    import weakref

    started = asyncio.Event()
    release = asyncio.Event()
    finished = asyncio.Event()

    class Provider:
        async def disconnect(self):
            started.set()
            await release.wait()
            finished.set()

    provider = Provider()
    reference = weakref.ref(provider)
    schedule_failed_client_cleanup(provider)
    del provider
    await started.wait()
    gc.collect()
    assert reference() is not None
    release.set()
    await drain_failed_client_cleanup()
    assert finished.is_set()
    gc.collect()
    assert reference() is None


@pytest.mark.asyncio
async def test_disconnect_timeout_is_observable_and_cancels_cleanup(caplog):
    started = asyncio.Event()
    cancelled = asyncio.Event()

    class Provider:
        async def disconnect(self):
            started.set()
            try:
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                cancelled.set()
                raise

    with patch("almanak.gateway.utils.async_web3_cleanup._DISCONNECT_TIMEOUT_SECONDS", 0.02):
        schedule_failed_client_cleanup(Provider())
        await started.wait()
        await drain_failed_client_cleanup()
    assert cancelled.is_set()
    assert "initialization cleanup failed error_type=TimeoutError" in caplog.text


@pytest.mark.asyncio
async def test_cancelled_shutdown_drain_keeps_owned_cleanup_alive():
    started = asyncio.Event()
    release = asyncio.Event()
    finished = asyncio.Event()

    class Provider:
        async def disconnect(self):
            started.set()
            await release.wait()
            finished.set()

    schedule_failed_client_cleanup(Provider())
    await started.wait()
    drain = asyncio.create_task(drain_failed_client_cleanup())
    await asyncio.sleep(0)
    drain.cancel()
    with pytest.raises(asyncio.CancelledError):
        await drain
    assert not finished.is_set()
    release.set()
    await drain_failed_client_cleanup()
    assert finished.is_set()


@pytest.mark.asyncio
async def test_short_shutdown_budget_reports_retained_cleanup(caplog):
    release = asyncio.Event()

    class Provider:
        async def disconnect(self):
            await release.wait()

    schedule_failed_client_cleanup(Provider())
    await drain_failed_client_cleanup(timeout=0)
    assert "initialization cleanup still pending at shutdown count=1" in caplog.text
    release.set()
    await drain_failed_client_cleanup()


def test_loop_shutdown_reports_unfinished_cleanup(caplog):
    async def run():
        started = asyncio.Event()

        class Provider:
            async def disconnect(self):
                started.set()
                await asyncio.sleep(5)

        schedule_failed_client_cleanup(Provider())
        await started.wait()

    asyncio.run(run())
    assert "initialization cleanup cancelled before completion" in caplog.text


@pytest.mark.asyncio
async def test_real_never_connected_provider_disconnects():
    provider = AsyncHTTPProvider("https://unused.invalid")
    schedule_failed_client_cleanup(provider)
    await drain_failed_client_cleanup()
    assert len(provider._request_session_manager.session_cache) == 0
