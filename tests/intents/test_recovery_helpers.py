"""Unit tests for intent test recovery helpers in tests/intents/conftest.py."""

from types import SimpleNamespace

import pytest

import tests.intents.conftest as intents_conftest


def test_seed_wallet_state_with_recovery_timeout_then_success(monkeypatch):
    """Timeout on first seed attempt should trigger restart and retry."""
    old_web3 = object()
    new_web3 = object()
    seen_web3_ids: list[int] = []

    def seed_wallet_state(web3, rpc_url):
        seen_web3_ids.append(id(web3))
        if len(seen_web3_ids) == 1:
            raise TimeoutError("timed out")
        assert rpc_url == "http://127.0.0.1:9999"
        return "0xabc"

    def force_restart(anvil_instance, chain_name, attempt):
        assert chain_name == "optimism"
        assert attempt == 1
        return (True, "http://127.0.0.1:9999")

    monkeypatch.setattr(intents_conftest, "_force_restart_anvil", force_restart)
    monkeypatch.setattr(intents_conftest, "make_intent_test_web3", lambda rpc_url: new_web3)

    result = intents_conftest.seed_wallet_state_with_recovery(
        seed_wallet_state=seed_wallet_state,
        web3=old_web3,
        rpc_url="http://127.0.0.1:1234",
        anvil_instance=SimpleNamespace(),
        chain_name="optimism",
    )

    assert result == "0xabc"
    assert seen_web3_ids == [id(old_web3), id(new_web3)]


def test_seed_wallet_state_with_recovery_exhausts_restarts(monkeypatch):
    """Recovery should fail fast after fixed restart budget is exhausted."""
    restart_attempts: list[int] = []

    def seed_wallet_state(web3, rpc_url):
        raise TimeoutError("timed out")

    def force_restart(anvil_instance, chain_name, attempt):
        restart_attempts.append(attempt)
        return (False, "")

    monkeypatch.setattr(intents_conftest, "_force_restart_anvil", force_restart)

    with pytest.raises(RuntimeError, match="failed after 2 forced restart attempts"):
        intents_conftest.seed_wallet_state_with_recovery(
            seed_wallet_state=seed_wallet_state,
            web3=object(),
            rpc_url="http://127.0.0.1:1234",
            anvil_instance=SimpleNamespace(),
            chain_name="optimism",
        )

    assert restart_attempts == [1, 2]


def test_seed_wallet_state_with_recovery_reraises_non_timeout(monkeypatch):
    """Non-timeout exceptions should bubble up immediately."""
    restart_calls: list[int] = []

    def seed_wallet_state(web3, rpc_url):
        raise ValueError("bad input")

    def force_restart(anvil_instance, chain_name, attempt):
        restart_calls.append(attempt)
        return (True, "http://127.0.0.1:9999")

    monkeypatch.setattr(intents_conftest, "_force_restart_anvil", force_restart)

    with pytest.raises(ValueError, match="bad input"):
        intents_conftest.seed_wallet_state_with_recovery(
            seed_wallet_state=seed_wallet_state,
            web3=object(),
            rpc_url="http://127.0.0.1:1234",
            anvil_instance=SimpleNamespace(),
            chain_name="optimism",
        )

    assert restart_calls == []


def test_fund_native_token_skips_setbalance_when_balance_already_sufficient(monkeypatch):
    """fund_native_token should avoid anvil_setBalance when top-up is unnecessary."""
    make_request_calls: list[tuple[str, list[str]]] = []

    class FakeEth:
        @staticmethod
        def get_balance(wallet):
            return 200

    class FakeProvider:
        @staticmethod
        def make_request(method, params):
            make_request_calls.append((method, params))
            return {"result": None}

    fake_web3 = SimpleNamespace(eth=FakeEth(), provider=FakeProvider())
    monkeypatch.setattr(intents_conftest, "make_intent_test_web3", lambda rpc_url: fake_web3)

    intents_conftest.fund_native_token(
        wallet=intents_conftest.TEST_WALLET,
        amount_wei=100,
        rpc_url="http://127.0.0.1:1234",
    )

    assert make_request_calls == []


def test_fund_native_token_sets_balance_on_top_up(monkeypatch):
    """fund_native_token should call anvil_setBalance when current balance is below target."""
    make_request_calls: list[tuple[str, list[str]]] = []

    class FakeEth:
        @staticmethod
        def get_balance(wallet):
            return 1

    class FakeProvider:
        @staticmethod
        def make_request(method, params):
            make_request_calls.append((method, params))
            return {"result": None}

    fake_web3 = SimpleNamespace(eth=FakeEth(), provider=FakeProvider())
    monkeypatch.setattr(intents_conftest, "make_intent_test_web3", lambda rpc_url: fake_web3)

    intents_conftest.fund_native_token(
        wallet=intents_conftest.TEST_WALLET,
        amount_wei=100,
        rpc_url="http://127.0.0.1:1234",
    )

    assert make_request_calls == [("anvil_setBalance", [intents_conftest.TEST_WALLET, hex(100)])]
