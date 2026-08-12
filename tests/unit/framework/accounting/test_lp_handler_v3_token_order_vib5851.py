"""VIB-5851 — V3-family LP cost basis mis-scaled when config pool-label order
disagrees with on-chain ``token0()``/``token1()`` (address) order.

Root cause (see ``docs/internal/qa/g6-matrix-sweep-2026-07-15.md`` §6): the
receipt parser emits ``amount0`` / ``amount1`` in on-chain, address-sorted
pool order, but the accounting handler paired those amounts with token
**decimals** resolved from the *config pool-label* order (e.g. ``"WETH/USDC/500"``).
On Ethereum the real WETH/USDC pool is USDC-first (``0xA0b8… < 0xC02a…``), so the
label order (WETH, USDC) is the OPPOSITE of on-chain order — both legs get
mis-scaled and one is mis-priced, yielding a ~$1.03bn phantom cost basis on a ~$4
position.

The class was already fixed for Uniswap **V4** (``_v4_realign_token_pair``, which
re-pairs by the receipt-emitted ``currency0``/``currency1`` addresses). V3 never
populates those currency fields, so the V4 guard is a permanent no-op for V3.
This mirrors that fix for the V3 family via an offline address sort of the label
symbols.

These tests exercise BOTH orderings:

* ``label == chain`` order (Arbitrum-style WETH/USDC where WETH < USDC) — must be
  unchanged (no collateral movement).
* ``label != chain`` order (Ethereum-style WETH/USDC where USDC < WETH) — the
  regression: FAILS with the fix reverted (mutation-checked), PASSES with it.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from almanak.framework.accounting.category_handlers.lp_handler import handle_lp
from almanak.framework.execution.extracted_data import LPCloseData, LPOpenData
from almanak.framework.observability.ledger import serialize_extracted_data

WALLET = "0x1234567890abcdef1234567890abcdef12345678"

# Raw amounts as they appear on-chain, in pool token0()/token1() (address-sorted)
# order. These are the exact magnitudes reconstructed in the G6 sweep §6.
USDC_RAW = 2_185_779  # 6 decimals -> 2.185779 USDC
WETH_RAW = 1_032_114_889_479_681  # 18 decimals -> 0.001032114889479681 WETH

WETH_PRICE = "1917.0"
USDC_PRICE = "1.0"

# Human-scaled truth for the position: ~$2.19 USDC + ~$1.98 WETH ~= $4.17.
EXPECTED_BASIS_LOW = 3.0
EXPECTED_BASIS_HIGH = 6.0


def _addr(byte: str) -> str:
    return "0x" + byte * 20


# Two synthetic chains that only differ in the relative address ordering of the
# WETH/USDC pair, so the SAME "WETH/USDC/500" label lands in opposite on-chain
# orders — exactly the chain-dependence the ticket describes.
#
#   * "ethereum-like": USDC (0x11…) < WETH (0xcc…)  -> label(WETH,USDC) != chain
#   * "arbitrum-like": WETH (0x11…) < USDC (0xcc…)  -> label(WETH,USDC) == chain
_ADDR_BOOKS = {
    "eth_like": {"USDC": _addr("11"), "WETH": _addr("cc")},
    "arb_like": {"WETH": _addr("11"), "USDC": _addr("cc")},
}
_DECIMALS = {"USDC": 6, "WETH": 18}


def _patch_resolver(monkeypatch, book_key: str) -> None:
    """Patch the token resolver so symbol->(address, decimals) is deterministic.

    Both the decimals pairing (``_resolve_lp_amounts``) and the new V3 address
    realignment resolve through ``get_token_resolver`` — one fake serves both.
    Accepts arbitrary kwargs (``chain=``, ``skip_gateway=``, ``log_errors=``).
    """
    book = _ADDR_BOOKS[book_key]

    class _FakeResolver:
        def resolve(self, token, chain, *, log_errors=True, skip_gateway=False):  # noqa: ANN001, ARG002
            up = str(token).upper()
            # Symbol lookups (decimals + address realignment).
            if up in book:
                return SimpleNamespace(symbol=up, address=book[up], decimals=_DECIMALS[up])
            # Address lookups (defensive; not used by the V3 path).
            for sym, addr in book.items():
                if addr.lower() == str(token).lower():
                    return SimpleNamespace(symbol=sym, address=addr, decimals=_DECIMALS[sym])
            return None

    monkeypatch.setattr(
        "almanak.framework.data.tokens.resolver.get_token_resolver",
        lambda: _FakeResolver(),
    )


def _price_inputs() -> str:
    return json.dumps({"WETH": WETH_PRICE, "USDC": USDC_PRICE})


def _v3_open_ledger(chain: str, pool_addr: str) -> tuple[dict, dict]:
    """A Uniswap-V3 LP_OPEN with a "WETH/USDC/500" label and address-sorted raw
    amounts (amount0 = on-chain token0). token_in/token_out carry the label order.
    """
    lp_open = LPOpenData(
        position_id=42,
        tick_lower=-60000,
        tick_upper=60000,
        liquidity=500_000,
        amount0=USDC_RAW,  # on-chain token0 == USDC on ethereum
        amount1=WETH_RAW,  # on-chain token1 == WETH on ethereum
        pool_address=pool_addr,
    )
    ledger = {
        "id": "le-open",
        "deployment_id": "d1",
        "cycle_id": "c1",
        "intent_type": "LP_OPEN",
        "protocol": "uniswap_v3",
        "chain": chain,
        "execution_mode": "paper",
        "tx_hash": "0xopen",
        "token_in": "WETH",  # label order (unsorted user intent)
        "token_out": "USDC",
        "amount_in": "",
        "amount_out": "",
        "timestamp": "2026-07-15T00:00:00+00:00",
        "extracted_data_json": serialize_extracted_data({"lp_open_data": lp_open}),
        "price_inputs_json": _price_inputs(),
    }
    outbox = {
        "outbox_id": "ob-open",
        "deployment_id": "d1",
        "cycle_id": "c1",
        "position_key": f"lp:uniswap_v3:{chain}:{WALLET}:{pool_addr}",
        "wallet_address": WALLET,
    }
    return outbox, ledger


def _v3_close_ledger(chain: str, pool_addr: str) -> tuple[dict, dict]:
    """A Uniswap-V3 LP_CLOSE: token_in/token_out are empty (a close returns BOTH
    tokens), so the handler falls back to the ``weth/usdc/500`` position-key tail
    — the exact seam the ticket blames for the label-order read.
    """
    lp_close = LPCloseData(
        amount0_collected=USDC_RAW,  # on-chain token0 == USDC
        amount1_collected=WETH_RAW,  # on-chain token1 == WETH
        fees0=None,
        fees1=None,
        liquidity_removed=500_000,
        pool_address=pool_addr,
    )
    ledger = {
        "id": "le-close",
        "deployment_id": "d1",
        "cycle_id": "c1",
        "intent_type": "LP_CLOSE",
        "protocol": "uniswap_v3",
        "chain": chain,
        "execution_mode": "paper",
        "tx_hash": "0xclose",
        "token_in": "",
        "token_out": "",
        "amount_in": "",
        "amount_out": "",
        "timestamp": "2026-07-15T00:00:00+00:00",
        "extracted_data_json": serialize_extracted_data({"lp_close_data": lp_close}),
        "price_inputs_json": _price_inputs(),
    }
    # Position-key tail is the V3 fee-tier descriptor; the real pool address is
    # recovered from the receipt's ``lp_close_data.pool_address``.
    outbox = {
        "outbox_id": "ob-close",
        "deployment_id": "d1",
        "cycle_id": "c1",
        "position_key": f"lp:uniswap_v3:{chain}:{WALLET}:weth/usdc/500",
        "wallet_address": WALLET,
    }
    return outbox, ledger


class TestV3TokenOrderRealignment:
    """The core VIB-5851 regression: label order != chain order must not corrupt
    cost basis. Reverting the fix makes ``test_ethereum_*`` FAIL with a ~$1bn
    phantom (mutation-checked)."""

    def test_ethereum_open_label_order_differs_from_chain(self, monkeypatch):
        """Ethereum: label (WETH, USDC) but chain token0 = USDC. LP_OPEN basis
        must be ~$4, not ~$1.03bn."""
        _patch_resolver(monkeypatch, "eth_like")
        pool = _addr("ab")
        outbox, ledger = _v3_open_ledger("ethereum", pool)
        event = handle_lp(outbox, ledger)
        assert event is not None
        assert event.cost_basis_usd is not None, "basis must be measured, not None"
        basis = float(event.cost_basis_usd)
        assert EXPECTED_BASIS_LOW < basis < EXPECTED_BASIS_HIGH, (
            f"phantom basis {basis} (expected ~$4.17) — token decimals mis-paired"
        )

    def test_ethereum_close_label_order_differs_from_chain(self, monkeypatch):
        """Ethereum LP_CLOSE (position-key tail fallback) must also be ~$4."""
        _patch_resolver(monkeypatch, "eth_like")
        pool = _addr("ab")
        outbox, ledger = _v3_close_ledger("ethereum", pool)
        event = handle_lp(outbox, ledger)
        assert event is not None
        assert event.cost_basis_usd is not None
        basis = float(event.cost_basis_usd)
        assert EXPECTED_BASIS_LOW < basis < EXPECTED_BASIS_HIGH, (
            f"phantom basis {basis} (expected ~$4.17) — LP_CLOSE decimals mis-paired"
        )

    def test_arbitrum_open_label_order_matches_chain_unchanged(self, monkeypatch):
        """Arbitrum: label (WETH, USDC) and chain token0 = WETH already agree.
        Raw amounts are in on-chain order (amount0 = WETH, amount1 = USDC here),
        so the basis is the same ~$4 and no collateral movement occurs."""
        _patch_resolver(monkeypatch, "arb_like")
        pool = _addr("ab")
        outbox, ledger = _v3_open_ledger("arbitrum", pool)
        # On arbitrum the on-chain token0 is WETH, so swap the raw amounts to
        # keep amount0 == on-chain token0's (WETH) amount.
        lp_open = LPOpenData(
            position_id=42,
            tick_lower=-60000,
            tick_upper=60000,
            liquidity=500_000,
            amount0=WETH_RAW,  # on-chain token0 == WETH on arbitrum
            amount1=USDC_RAW,  # on-chain token1 == USDC on arbitrum
            pool_address=pool,
        )
        ledger["extracted_data_json"] = serialize_extracted_data({"lp_open_data": lp_open})
        event = handle_lp(outbox, ledger)
        assert event is not None
        assert event.cost_basis_usd is not None
        basis = float(event.cost_basis_usd)
        assert EXPECTED_BASIS_LOW < basis < EXPECTED_BASIS_HIGH, (
            f"arbitrum (already-aligned) basis {basis} moved — collateral damage"
        )
        # Token order stays label order (WETH, USDC) because it already matched.
        assert (event.token0 or "").upper() == "WETH"
        assert (event.token1 or "").upper() == "USDC"

    def test_ethereum_open_token0_is_onchain_lower_address(self, monkeypatch):
        """After realignment the emitted token0 is the LOWER-address symbol
        (USDC on ethereum), so decimals pair with the on-chain amount0."""
        _patch_resolver(monkeypatch, "eth_like")
        outbox, ledger = _v3_open_ledger("ethereum", _addr("ab"))
        event = handle_lp(outbox, ledger)
        assert event is not None
        assert (event.token0 or "").upper() == "USDC"
        assert (event.token1 or "").upper() == "WETH"


class TestV3RealignHelperGatesAndFailOpen:
    """Unit-level contracts for ``_v3_realign_token_pair``: fires only on the
    on-chain-ordered raw-int branch, and fails open (keeps label order) for every
    shape whose amounts are NOT address-sorted or whose order cannot be proven."""

    @staticmethod
    def _realign():
        from almanak.framework.accounting.category_handlers.lp_handler import (
            _v3_realign_token_pair,
        )

        return _v3_realign_token_pair

    def _open_data(self, **kw) -> LPOpenData:
        base = {"position_id": 1, "amount0": USDC_RAW, "amount1": WETH_RAW}
        base.update(kw)
        return LPOpenData(**base)

    def test_swaps_symbols_when_label_order_differs(self, monkeypatch):
        _patch_resolver(monkeypatch, "eth_like")  # USDC < WETH
        out = self._realign()(
            lp_data=self._open_data(),
            intent_type_str="LP_OPEN",
            extracted={"lp_open_data": self._open_data()},
            chain="ethereum",
            token0="WETH",
            token1="USDC",
        )
        assert out == ("USDC", "WETH")

    def test_keeps_symbols_when_label_order_matches(self, monkeypatch):
        _patch_resolver(monkeypatch, "arb_like")  # WETH < USDC
        out = self._realign()(
            lp_data=self._open_data(),
            intent_type_str="LP_OPEN",
            extracted={"lp_open_data": self._open_data()},
            chain="arbitrum",
            token0="WETH",
            token1="USDC",
        )
        assert out == ("WETH", "USDC")

    def test_empty_symbol_passes_through(self, monkeypatch):
        _patch_resolver(monkeypatch, "eth_like")
        out = self._realign()(
            lp_data=self._open_data(),
            intent_type_str="LP_OPEN",
            extracted={},
            chain="ethereum",
            token0="",
            token1="USDC",
        )
        assert out == ("", "USDC")

    def test_primitive_money_legs_open_not_reordered(self, monkeypatch):
        """Declared money legs (Curve etc.) align amounts to token_in/out — must
        keep label order regardless of address sort."""
        _patch_resolver(monkeypatch, "eth_like")
        out = self._realign()(
            lp_data=self._open_data(),
            intent_type_str="LP_OPEN",
            extracted={"lp_open_data": self._open_data(), "primitive_money_legs": object()},
            chain="ethereum",
            token0="WETH",
            token1="USDC",
        )
        assert out == ("WETH", "USDC")

    def test_string_fallback_open_not_reordered(self, monkeypatch):
        """No typed lp_open_data raw amounts → string fallback (token_in/out
        order) → not reordered."""
        _patch_resolver(monkeypatch, "eth_like")
        out = self._realign()(
            lp_data=None,
            intent_type_str="LP_OPEN",
            extracted={},
            chain="ethereum",
            token0="WETH",
            token1="USDC",
        )
        assert out == ("WETH", "USDC")

    def test_v4_currency_pair_skipped(self, monkeypatch):
        """A pair the V4 path ESTABLISHED is left alone — but only when it says so.

        VIB-6476 — this used to be gated on ``currency0 and currency1`` being present,
        which conflated "V4 realigned this" with "V4 saw currencies and gave up". The
        gate now reads the explicit ``v4_realigned`` flag instead.
        """
        _patch_resolver(monkeypatch, "eth_like")
        lp_data = self._open_data(currency0=_addr("11"), currency1=_addr("cc"))
        kwargs = {
            "lp_data": lp_data,
            "intent_type_str": "LP_OPEN",
            "extracted": {"lp_open_data": lp_data},
            "chain": "ethereum",
            "token0": "WETH",
            "token1": "USDC",
        }

        # V4 established the pair -> untouched, regardless of what the addresses say.
        assert self._realign()(**kwargs, v4_realigned=True) == ("WETH", "USDC")

        # V4 did NOT establish it, so the gate no longer short-circuits. In ``eth_like``
        # USDC IS _addr("11") and WETH IS _addr("cc"), so the observed currencies match
        # the labels and positional PLACEMENT succeeds: slot 0 -> USDC, slot 1 -> WETH.
        # The value coincides with what the address sort would give on this chain, so it
        # does NOT exercise the fallthrough — TestBothObservedButUnplaceable covers the
        # case where placement fails and the two mechanisms diverge.
        assert self._realign()(**kwargs, v4_realigned=False) == ("USDC", "WETH")

    def test_both_observed_but_unplaceable_keeps_label_order(self, monkeypatch):
        """VIB-6484 / delta review — both slots observed but the addresses match
        NEITHER label: keep label order, do NOT fall through to the address sort.

        This path is the one the ``and`` -> ``or`` widening opened. The old gate was
        ``if currency0 and currency1: return token0, token1``, which made the address
        sort **unreachable** once both currencies were stamped. Widening it to reach
        positional placement also exposed that fallthrough, and on a venue whose slots
        are not address-sorted — TraderJoe Liquidity Book stamps both currencies and
        emits no ``coin_symbols``, so the N-coin early return does not catch it — the
        sort transposes the row. That is the VIB-6383 $322bn class, on a path this
        function had previously closed.

        Trigger: a stale or bridged pool label, e.g. one written ``USDC`` for a pool
        that actually holds ``USDC.e``. The observed currency then matches no label.
        """
        _patch_resolver(monkeypatch, "eth_like")
        # Neither 0x99… nor 0x77… is in the eth_like book, so placement must refuse.
        lp_data = {
            "amount0_collected": USDC_RAW,
            "amount1_collected": WETH_RAW,
            "currency0": _addr("99"),
            "currency1": _addr("77"),
        }
        out = self._realign()(
            lp_data=lp_data,
            intent_type_str="LP_CLOSE",
            extracted={"lp_close_data": lp_data},
            chain="ethereum",
            token0="WETH",
            token1="USDC",
            v4_realigned=False,
        )
        assert out == ("WETH", "USDC"), "unplaceable observation must keep label order"

        # And it must genuinely DIVERGE from the address sort — otherwise this test
        # would pass even if the fallthrough were still live.
        from almanak.framework.data.tokens.pair_order import realign_token_pair_by_address

        assert realign_token_pair_by_address("WETH", "USDC", "ethereum") == ("USDC", "WETH")
        assert out != realign_token_pair_by_address("WETH", "USDC", "ethereum")

    def test_fungible_coin_symbols_skipped(self, monkeypatch):
        """N-coin fungible pools order coins by pool index, not address."""
        _patch_resolver(monkeypatch, "eth_like")
        lp_data = self._open_data(coin_symbols=["USDC", "WETH"])
        out = self._realign()(
            lp_data=lp_data,
            intent_type_str="LP_OPEN",
            extracted={"lp_open_data": lp_data},
            chain="ethereum",
            token0="WETH",
            token1="USDC",
        )
        assert out == ("WETH", "USDC")


    def test_resolver_exception_fails_open(self, monkeypatch):
        class _Boom:
            def resolve(self, token, chain, *, log_errors=True, skip_gateway=False):  # noqa: ANN002, ANN003, ARG002
                raise RuntimeError("resolver down")

        monkeypatch.setattr(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            lambda: _Boom(),
        )
        out = self._realign()(
            lp_data=self._open_data(),
            intent_type_str="LP_OPEN",
            extracted={"lp_open_data": self._open_data()},
            chain="ethereum",
            token0="WETH",
            token1="USDC",
        )
        assert out == ("WETH", "USDC")

    def test_missing_address_fails_open(self, monkeypatch):
        class _NoAddr:
            def resolve(self, token, chain, *, log_errors=True, skip_gateway=False):  # noqa: ANN001, ANN003, ARG002
                return SimpleNamespace(symbol=str(token).upper(), address=None, decimals=18)

        monkeypatch.setattr(
            "almanak.framework.data.tokens.resolver.get_token_resolver",
            lambda: _NoAddr(),
        )
        out = self._realign()(
            lp_data=self._open_data(),
            intent_type_str="LP_OPEN",
            extracted={"lp_open_data": self._open_data()},
            chain="ethereum",
            token0="WETH",
            token1="USDC",
        )
        assert out == ("WETH", "USDC")

    def test_close_dict_fallback_reads_currency_keys(self, monkeypatch):
        """dict-shaped lp_data (deserialize fallback) must still see currency
        keys and skip (V4-owned)."""
        _patch_resolver(monkeypatch, "eth_like")
        lp_data = {"amount0_collected": USDC_RAW, "amount1_collected": WETH_RAW,
                   "currency0": _addr("11"), "currency1": _addr("cc")}
        kwargs = {
            "lp_data": lp_data,
            "intent_type_str": "LP_CLOSE",
            "extracted": {"lp_close_data": lp_data},
            "chain": "ethereum",
            "token0": "WETH",
            "token1": "USDC",
        }
        # The dict shape must still be READ (the point of this test): both currency
        # keys are seen, so a V4-established pair is skipped...
        assert self._realign()(**kwargs, v4_realigned=True) == ("WETH", "USDC")
        # ...and an unestablished one is no longer waved through on presence alone
        # (VIB-6476): the dict-read currencies match the ``eth_like`` labels, so
        # positional placement binds slot 0 -> USDC, slot 1 -> WETH.
        assert self._realign()(**kwargs, v4_realigned=False) == ("USDC", "WETH")
