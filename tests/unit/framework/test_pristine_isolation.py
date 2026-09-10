"""Unit tests for the inter-module pristine-revert isolation helpers (VIB-3059).

These tests mock the Web3 provider's `make_request` interface to validate the
snapshot/revert/purge semantics of `_ensure_pristine_and_rearm` and
`reset_fork_to_pristine` without needing a live Anvil fork.
"""

from unittest.mock import MagicMock

import pytest

from tests.intents import conftest as intents_conftest
from tests.intents.conftest import (
    _ensure_pristine_and_rearm,
    _module_baselines,
    _PristineTransportError,
    _session_pristine,
    reset_fork_to_pristine,
)


def _make_fake_web3(snapshot_ids: list[str], revert_results: list[bool]):
    """Build a web3 mock whose provider returns the supplied snapshot IDs / revert results in order."""
    fake = MagicMock()
    fake.eth.chain_id = 8453
    fake.provider.endpoint_uri = "http://127.0.0.1:8545"

    snap_iter = iter(snapshot_ids)
    revert_iter = iter(revert_results)

    def _make_request(method: str, params: list) -> dict:
        if method == "evm_snapshot":
            return {"result": next(snap_iter)}
        if method == "evm_revert":
            return {"result": next(revert_iter)}
        raise AssertionError(f"unexpected RPC {method!r}")

    fake.provider.make_request.side_effect = _make_request
    return fake


def _reset_module_state() -> None:
    intents_conftest._session_pristine.clear()
    intents_conftest._module_baselines.clear()


def test_first_call_captures_pristine_without_reverting() -> None:
    _reset_module_state()
    web3 = _make_fake_web3(snapshot_ids=["0x1"], revert_results=[])

    ok = _ensure_pristine_and_rearm(web3, chain_id=8453)

    assert ok is True
    assert _session_pristine[(8453, "http://127.0.0.1:8545")] == "0x1"
    # Only evm_snapshot should have been called; no revert on first call.
    methods = [call.args[0] for call in web3.provider.make_request.call_args_list]
    assert methods == ["evm_snapshot"]


def test_second_call_reverts_and_rearms_pristine() -> None:
    _reset_module_state()
    _session_pristine[(8453, "http://127.0.0.1:8545")] = "0x1"
    _module_baselines[(8453, "/fake/module_a.py", "http://127.0.0.1:8545")] = "0x5"
    _module_baselines[(1, "/fake/ethereum_module.py", "http://127.0.0.1:8546")] = "0x6"

    web3 = _make_fake_web3(snapshot_ids=["0x2"], revert_results=[True])

    ok = _ensure_pristine_and_rearm(web3, chain_id=8453)

    assert ok is True
    assert _session_pristine[(8453, "http://127.0.0.1:8545")] == "0x2"
    # Module A baseline on chain 8453 must be purged; ethereum baseline untouched.
    assert (8453, "/fake/module_a.py", "http://127.0.0.1:8545") not in _module_baselines
    assert _module_baselines[(1, "/fake/ethereum_module.py", "http://127.0.0.1:8546")] == "0x6"

    calls = [(c.args[0], c.args[1]) for c in web3.provider.make_request.call_args_list]
    assert calls == [("evm_revert", ["0x1"]), ("evm_snapshot", [])]


def test_failed_revert_recaptures_and_returns_false() -> None:
    _reset_module_state()
    _session_pristine[(8453, "http://127.0.0.1:8545")] = "0x1"
    _module_baselines[(8453, "/fake/module.py", "http://127.0.0.1:8545")] = "0x5"

    web3 = _make_fake_web3(snapshot_ids=["0x9"], revert_results=[False])

    ok = _ensure_pristine_and_rearm(web3, chain_id=8453)

    # Returning False signals degraded isolation but pristine was recaptured so
    # later modules can still attempt a revert.
    assert ok is False
    assert _session_pristine[(8453, "http://127.0.0.1:8545")] == "0x9"
    # Stale baselines are still purged even when revert fails, because the
    # snapshot ids they referenced are no longer guaranteed valid.
    assert (8453, "/fake/module.py", "http://127.0.0.1:8545") not in _module_baselines


def test_failed_rearm_returns_false_and_clears_pristine() -> None:
    """If revert succeeds but post-revert snapshot fails, we must surface failure.

    Otherwise the next module would run without a valid pristine anchor and
    could silently inherit this module's residue.
    """
    _reset_module_state()
    _session_pristine[(8453, "http://127.0.0.1:8545")] = "0x1"

    fake = MagicMock()
    fake.eth.chain_id = 8453
    fake.provider.endpoint_uri = "http://127.0.0.1:8545"
    calls = []

    def _make_request(method: str, params: list) -> dict:
        calls.append(method)
        if method == "evm_revert":
            return {"result": True}
        if method == "evm_snapshot":
            # Simulate RPC returning nothing from the post-revert snapshot attempt.
            return {}
        raise AssertionError(f"unexpected RPC {method!r}")

    fake.provider.make_request.side_effect = _make_request

    ok = _ensure_pristine_and_rearm(fake, chain_id=8453)

    assert ok is False
    # Pristine is cleared so the NEXT caller will recapture fresh (first-call path).
    assert (8453, "http://127.0.0.1:8545") not in _session_pristine
    assert calls == ["evm_revert", "evm_snapshot"]


def test_reset_fork_to_pristine_reads_chain_id_from_web3() -> None:
    _reset_module_state()
    web3 = _make_fake_web3(snapshot_ids=["0xA"], revert_results=[])
    web3.eth.chain_id = 42161

    ok = reset_fork_to_pristine(web3)

    assert ok is True
    assert _session_pristine[(42161, "http://127.0.0.1:8545")] == "0xA"


def test_reset_fork_to_pristine_strict_raises_on_chain_id_read_failure() -> None:
    _reset_module_state()
    web3 = MagicMock()
    type(web3.eth).chain_id = property(lambda self: (_ for _ in ()).throw(RuntimeError("rpc down")))

    with pytest.raises(RuntimeError, match="could not determine chain_id"):
        reset_fork_to_pristine(web3)
    assert _session_pristine == {}


def test_reset_fork_to_pristine_non_strict_returns_false_on_chain_id_failure() -> None:
    _reset_module_state()
    web3 = MagicMock()
    type(web3.eth).chain_id = property(lambda self: (_ for _ in ()).throw(RuntimeError("rpc down")))

    ok = reset_fork_to_pristine(web3, strict=False)

    assert ok is False
    assert _session_pristine == {}


def test_reset_fork_to_pristine_strict_raises_when_pristine_fails() -> None:
    """Post-revert snapshot failure must abort in strict mode."""
    _reset_module_state()
    _session_pristine[(8453, "http://127.0.0.1:8545")] = "0x1"

    fake = MagicMock()
    fake.eth.chain_id = 8453
    fake.provider.endpoint_uri = "http://127.0.0.1:8545"

    def _make_request(method: str, params: list) -> dict:
        if method == "evm_revert":
            return {"result": True}
        if method == "evm_snapshot":
            return {}  # post-revert snapshot returns no result
        raise AssertionError(f"unexpected RPC {method!r}")

    fake.provider.make_request.side_effect = _make_request

    with pytest.raises(RuntimeError, match="pristine reset could not be established"):
        reset_fork_to_pristine(fake)


def test_ensure_pristine_raises_transport_error_on_initial_capture_flake() -> None:
    """A transport exception on the initial snapshot capture must surface as
    `_PristineTransportError` so the retry loop in `reset_fork_to_pristine`
    can absorb it."""
    _reset_module_state()

    fake = MagicMock()
    fake.eth.chain_id = 8453
    fake.provider.endpoint_uri = "http://127.0.0.1:8545"
    fake.provider.make_request.side_effect = ConnectionError("rpc read timeout")

    with pytest.raises(_PristineTransportError, match="initial pristine snapshot"):
        _ensure_pristine_and_rearm(fake, chain_id=8453)
    # No state should have been recorded on a transport failure.
    assert (8453, "http://127.0.0.1:8545") not in _session_pristine


def test_ensure_pristine_raises_transport_error_on_revert_flake() -> None:
    """A transport exception raised from `evm_revert` must surface as
    `_PristineTransportError` (not silently degrade to `False`), so retries
    can actually engage."""
    _reset_module_state()
    _session_pristine[(8453, "http://127.0.0.1:8545")] = "0x1"

    fake = MagicMock()
    fake.eth.chain_id = 8453
    fake.provider.endpoint_uri = "http://127.0.0.1:8545"

    def _make_request(method: str, params: list) -> dict:
        if method == "evm_revert":
            raise ConnectionError("rpc read timeout on revert")
        raise AssertionError(f"unexpected RPC {method!r}")

    fake.provider.make_request.side_effect = _make_request

    with pytest.raises(_PristineTransportError, match="pristine revert"):
        _ensure_pristine_and_rearm(fake, chain_id=8453)


def test_ensure_pristine_raises_transport_error_on_post_revert_recapture_flake() -> None:
    """A transport exception raised from the post-revert recapture must surface
    as `_PristineTransportError`; on retry the now-stale snap id will
    deterministically fall into the best-effort recapture branch."""
    _reset_module_state()
    _session_pristine[(8453, "http://127.0.0.1:8545")] = "0x1"

    fake = MagicMock()
    fake.eth.chain_id = 8453
    fake.provider.endpoint_uri = "http://127.0.0.1:8545"
    call_log: list[str] = []

    def _make_request(method: str, params: list) -> dict:
        call_log.append(method)
        if method == "evm_revert":
            return {"result": True}
        if method == "evm_snapshot":
            raise ConnectionError("rpc read timeout on recapture")
        raise AssertionError(f"unexpected RPC {method!r}")

    fake.provider.make_request.side_effect = _make_request

    with pytest.raises(_PristineTransportError, match="post-revert pristine recapture"):
        _ensure_pristine_and_rearm(fake, chain_id=8453)
    # Revert already succeeded, so the retry path relies on the stale id
    # being left in `_session_pristine` to drive deterministic fall-through
    # on the next attempt.
    assert call_log == ["evm_revert", "evm_snapshot"]


def test_reset_fork_to_pristine_retries_on_transport_error_and_succeeds() -> None:
    """The retry-with-backoff loop must absorb a transient transport error
    and succeed on a subsequent attempt — this is the whole reason #1739
    exists. Attempts-1 transport flakes followed by a real success means the
    loop returns `True`, with no sleep on the final attempt."""
    _reset_module_state()

    fake = MagicMock()
    fake.eth.chain_id = 8453
    fake.provider.endpoint_uri = "http://127.0.0.1:8545"

    call_count = {"evm_snapshot": 0}

    def _make_request(method: str, params: list) -> dict:
        if method == "evm_snapshot":
            call_count["evm_snapshot"] += 1
            if call_count["evm_snapshot"] < 2:
                raise ConnectionError("transient rpc read timeout")
            return {"result": "0xfresh"}
        raise AssertionError(f"unexpected RPC {method!r}")

    fake.provider.make_request.side_effect = _make_request

    ok = reset_fork_to_pristine(fake, attempts=3, backoff_s=0.0)

    assert ok is True
    assert _session_pristine[(8453, "http://127.0.0.1:8545")] == "0xfresh"
    assert call_count["evm_snapshot"] == 2


def test_reset_fork_to_pristine_does_not_retry_on_definitive_false() -> None:
    """A definitive `False` return (e.g. post-revert snapshot returns no result)
    must short-circuit the retry loop so strict mode raises on the first attempt
    — retrying a `False` verdict risks silently upgrading a lost-isolation
    outcome into an accidental `True`."""
    _reset_module_state()
    _session_pristine[(8453, "http://127.0.0.1:8545")] = "0x1"

    fake = MagicMock()
    fake.eth.chain_id = 8453
    fake.provider.endpoint_uri = "http://127.0.0.1:8545"
    calls: list[str] = []

    def _make_request(method: str, params: list) -> dict:
        calls.append(method)
        if method == "evm_revert":
            return {"result": True}
        if method == "evm_snapshot":
            return {}  # post-revert snapshot returns no result
        raise AssertionError(f"unexpected RPC {method!r}")

    fake.provider.make_request.side_effect = _make_request

    with pytest.raises(RuntimeError, match="pristine reset could not be established"):
        reset_fork_to_pristine(fake, attempts=3, backoff_s=0.0)
    # Only one attempt happened — no retry on definitive False.
    assert calls == ["evm_revert", "evm_snapshot"]


def test_same_chain_forks_never_reuse_or_purge_each_others_snapshots():
    _reset_module_state()
    first = _make_fake_web3(["0x1", "0x2"], [True])
    second = _make_fake_web3(["0x1", "0x3"], [True])
    second.provider.endpoint_uri = "http://127.0.0.1:8546"
    assert _ensure_pristine_and_rearm(first, 8453)
    assert _ensure_pristine_and_rearm(second, 8453)
    assert [call.args[0] for call in second.provider.make_request.call_args_list] == ["evm_snapshot"]
    first_key = (8453, "/same/module.py", first.provider.endpoint_uri)
    second_key = (8453, "/same/module.py", second.provider.endpoint_uri)
    _module_baselines[first_key] = "0x5"
    _module_baselines[second_key] = "0x6"
    assert _ensure_pristine_and_rearm(first, 8453)
    assert first_key not in _module_baselines
    assert _module_baselines[second_key] == "0x6"
    assert _session_pristine[(8453, second.provider.endpoint_uri)] == "0x1"
    assert _ensure_pristine_and_rearm(second, 8453)
    assert _session_pristine[(8453, first.provider.endpoint_uri)] == "0x2"


def test_snapshot_identity_refuses_an_unknown_endpoint():
    _reset_module_state()
    web3 = _make_fake_web3(["0x1"], [])
    web3.provider.endpoint_uri = None
    with pytest.raises(RuntimeError, match="explicit provider endpoint"):
        _ensure_pristine_and_rearm(web3, 8453)
    web3.provider.make_request.assert_not_called()
