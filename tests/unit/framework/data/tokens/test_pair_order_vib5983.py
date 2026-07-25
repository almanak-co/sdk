"""VIB-5983 — unit tests for realign_token_pair_by_address.

Required under tests/unit/ for the new module
``almanak/framework/data/tokens/pair_order.py`` (repo unit-test policy).
Producer integration lives in
``tests/framework/observability/test_position_events_v3_token_order_vib5983.py``.
"""

from __future__ import annotations

from types import SimpleNamespace

from almanak.framework.data.tokens.pair_order import realign_token_pair_by_address


def _addr(byte: str) -> str:
    return "0x" + byte * 20


def _patch_resolver(monkeypatch, book: dict[str, str]):
    """Return a fake resolver that records every resolve() call's kwargs.

    Production ``realign_token_pair_by_address`` must pass
    ``skip_gateway=True`` (no network). Tests assert that contract so a
    regression that drops the flag fails the suite (CodeRabbit PR #3422).
    """
    calls: list[dict] = []

    class _Fake:
        def resolve(self, key, **kwargs):  # noqa: ANN001
            calls.append({"key": key, **kwargs})
            up = str(key).upper()
            if up in book:
                return SimpleNamespace(symbol=up, address=book[up], decimals=18)
            return None

    monkeypatch.setattr(
        "almanak.framework.data.tokens.resolver.get_token_resolver",
        lambda: _Fake(),
    )
    return calls


def _assert_offline_contract(calls: list[dict], *, chain: str) -> None:
    assert len(calls) == 2, f"expected 2 resolve calls, got {len(calls)}: {calls}"
    for c in calls:
        assert c.get("skip_gateway") is True, f"skip_gateway not True: {c}"
        assert c.get("log_errors") is False, f"log_errors not False: {c}"
        assert c.get("chain") == chain, f"chain mismatch: {c}"


def test_swaps_when_first_symbol_has_higher_address(monkeypatch):
    # USDC 0x11… < WETH 0xcc… → chain order USDC, WETH
    calls = _patch_resolver(monkeypatch, {"USDC": _addr("11"), "WETH": _addr("cc")})
    assert realign_token_pair_by_address("WETH", "USDC", "ethereum") == ("USDC", "WETH")
    _assert_offline_contract(calls, chain="ethereum")


def test_keeps_order_when_already_address_sorted(monkeypatch):
    calls = _patch_resolver(monkeypatch, {"WETH": _addr("11"), "USDC": _addr("cc")})
    assert realign_token_pair_by_address("WETH", "USDC", "arbitrum") == ("WETH", "USDC")
    _assert_offline_contract(calls, chain="arbitrum")


def test_fail_open_when_unresolved(monkeypatch):
    class _Empty:
        def resolve(self, *_a, **_k):
            return None

    monkeypatch.setattr(
        "almanak.framework.data.tokens.resolver.get_token_resolver",
        lambda: _Empty(),
    )
    assert realign_token_pair_by_address("WETH", "USDC", "ethereum") == ("WETH", "USDC")


def test_fail_open_on_resolver_exception(monkeypatch):
    class _Boom:
        def resolve(self, *_a, **_k):
            raise RuntimeError("resolver down")

    monkeypatch.setattr(
        "almanak.framework.data.tokens.resolver.get_token_resolver",
        lambda: _Boom(),
    )
    assert realign_token_pair_by_address("WETH", "USDC", "ethereum") == ("WETH", "USDC")


def test_empty_inputs_pass_through():
    assert realign_token_pair_by_address("", "USDC", "ethereum") == ("", "USDC")
    assert realign_token_pair_by_address("WETH", "", "ethereum") == ("WETH", "")
    assert realign_token_pair_by_address("WETH", "USDC", "") == ("WETH", "USDC")
