"""Direct tests for ``MarketSnapshot.lending_position_balances`` (VIB-5468 / TD-10).

The guard (``lending_unwind_guard``) mocks this method, so it needs its own direct
coverage. Empty != Zero throughout: every unmeasured path returns ``None`` (never a
fabricated ``0``). Finding (CodeRabbit #3070): a present-but-DISCONNECTED gateway
client must be treated as unmeasured, not just a ``None`` client.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from almanak.framework.market.snapshot import MarketSnapshot


def _snap() -> MarketSnapshot:
    return MarketSnapshot(chain="arbitrum", wallet_address="0x" + "11" * 20)


class _Reader:
    def __init__(self, supply: int | None, debt: int | None, *, raises: bool = False) -> None:
        self._supply = supply
        self._debt = debt
        self._raises = raises

    def get_supply_balance(self, *_a: Any, **_k: Any) -> int | None:
        if self._raises:
            raise RuntimeError("rpc down")
        return self._supply

    def get_debt_balance(self, *_a: Any, **_k: Any) -> int | None:
        if self._raises:
            raise RuntimeError("rpc down")
        return self._debt


def _wire(monkeypatch, snap: MarketSnapshot, reader: _Reader | None) -> None:
    monkeypatch.setattr(snap, "_resolve_token_address", lambda token, chain: "0xtoken")
    monkeypatch.setattr(
        "almanak.framework.intents.balance_readers.get_reader_for_protocol",
        lambda protocol: reader,
    )


def _forbid_read_path(monkeypatch, snap: MarketSnapshot) -> None:
    """Make ANY downstream read-path access fail loudly.

    The no-client / disconnected-client contract is *no downstream read path at
    all* — not merely "returns (None, None)". Asserting only the tuple would still
    pass if the method reached ``_resolve_token_address`` / the reader registry /
    the reader and then swallowed the result. Wiring these to raise turns that
    silent reachability into a test failure (CodeRabbit #3070).
    """

    def _unexpected(*_a: Any, **_k: Any) -> None:
        raise AssertionError("read path must not be reached for an absent/disconnected client")

    monkeypatch.setattr(snap, "_resolve_token_address", _unexpected)
    monkeypatch.setattr(
        "almanak.framework.intents.balance_readers.get_reader_for_protocol",
        _unexpected,
    )


def test_no_client_is_unmeasured(monkeypatch) -> None:
    snap = _snap()
    snap._gateway_client = None
    _forbid_read_path(monkeypatch, snap)  # absent client must short-circuit before any read
    assert snap.lending_position_balances("aave_v3", "USDC") == (None, None)


def test_disconnected_client_is_unmeasured(monkeypatch) -> None:
    """A present-but-disconnected client must not reach the readers (Empty != Zero)."""
    snap = _snap()
    snap._gateway_client = SimpleNamespace(is_connected=False)
    _forbid_read_path(monkeypatch, snap)  # readers would answer, but must not be reached
    assert snap.lending_position_balances("aave_v3", "USDC") == (None, None)


def test_connected_client_reads_both_legs(monkeypatch) -> None:
    snap = _snap()
    snap._gateway_client = SimpleNamespace(is_connected=True)
    _wire(monkeypatch, snap, _Reader(100, 50))
    assert snap.lending_position_balances("aave_v3", "USDC") == (100, 50)


def test_reader_fault_is_unmeasured(monkeypatch) -> None:
    snap = _snap()
    snap._gateway_client = SimpleNamespace(is_connected=True)
    _wire(monkeypatch, snap, _Reader(None, None, raises=True))
    assert snap.lending_position_balances("aave_v3", "USDC") == (None, None)


def test_unsupported_protocol_is_unmeasured(monkeypatch) -> None:
    snap = _snap()
    snap._gateway_client = SimpleNamespace(is_connected=True)
    _wire(monkeypatch, snap, None)  # no reader for protocol
    assert snap.lending_position_balances("not_a_protocol", "USDC") == (None, None)
