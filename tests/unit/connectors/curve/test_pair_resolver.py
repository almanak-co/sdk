"""Curve pair→pool dynamic resolution (VIB-5716).

``build_pair_candidates`` enumerates MetaRegistry ``find_pool_for_coins``,
shape-resolves each candidate, applies the USD liquidity floor and the
``get_pool_name`` provenance tell; ``classify_add_liquidity_probe`` fuses the
deployability probe with that tell. These tests drive a fake gateway for the
enumeration selectors and canned metadata for the shape step (the shape
resolver has its own suite in ``test_pool_resolver.py``), pinning:

- enumeration termination (zero address / revert-on-healthy / cap / dedupe),
- the indeterminate-vs-definitive transport contract,
- the ALM-2931 screening semantics (dust floor, unpriceable, provenance), and
- the full probe classification matrix — including the reasonless-revert
  policy (USDT-style tokens revert reasonlessly on missing allowance, so a
  reasonless revert must NOT disqualify on its own; provenance decides).
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.connectors._strategy_base.rpc import StaticCallProbe
from almanak.connectors.curve import pair_resolver, pool_resolver
from almanak.connectors.curve.pair_resolver import (
    _FIND_POOL_FOR_COINS_SEL,
    _GET_BALANCES_SEL,
    _GET_POOL_NAME_SEL,
    PairCandidateSet,
    build_pair_candidates,
    classify_add_liquidity_probe,
    format_pair_miss,
    pool_provenance_suspect,
)
from almanak.connectors.curve.pool_resolver import _GET_ADDRESS_SEL, CurvePoolMetadata

CRVUSD = "0xf939E0A03FB07F59A73314E73794Be0E57ac1b4E"
WBTC = "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"
META_REGISTRY = "0xF98B45FA17DE75FB1aD0e7aFD971b0ca00e379fC"
POOL_BIG = "0x1111111111111111111111111111111111111111"
POOL_MID = "0x2222222222222222222222222222222222222222"
POOL_DUST = "0x3333333333333333333333333333333333333333"
ZERO = "0x" + "0" * 40


def _addr_word(addr: str) -> str:
    return addr.lower().removeprefix("0x").zfill(64)


def _uint_word(value: int) -> str:
    return format(value, "064x")


def _uint_array(values: list[int]) -> str:
    padded = list(values) + [0] * (8 - len(values))
    return "".join(_uint_word(v) for v in padded[:8])


def _metadata(address: str, *, decimals: tuple[int, int] = (18, 8)) -> CurvePoolMetadata:
    return CurvePoolMetadata(
        address=address,
        lp_token=address,  # NG-style: pool IS the LP token
        coin_addresses=[CRVUSD, WBTC],
        coin_decimals=list(decimals),
        coin_symbols=["CRVUSD", "WBTC"],
        n_coins=2,
        pool_type="cryptoswap",
        is_metapool=False,
        base_pool=None,
        base_pool_coin_addresses=None,
        base_pool_coins=None,
    )


class FakePairGateway:
    """Connected gateway answering the pair-enumeration selectors.

    ``pools`` maps pool address → (crvUSD balance, WBTC balance) raw units;
    enumeration returns them in insertion order. ``name_reverts`` marks pools
    whose ``get_pool_name`` reverts (the Yield-Basis provenance tell);
    ``find_reverts_at`` forces the enumeration read at that index to revert;
    ``healthy=False`` makes the transport-health probe fail so nothing can be
    confirmed definitive.
    """

    is_connected = True

    def __init__(
        self,
        pools: dict[str, tuple[int, int]],
        *,
        name_reverts: frozenset[str] = frozenset(),
        find_reverts_at: int | None = None,
        find_blips: int = 0,
        healthy: bool = True,
    ) -> None:
        self._pools = pools
        self._order = list(pools)
        self._name_reverts = {a.lower() for a in name_reverts}
        self._find_reverts_at = find_reverts_at
        # One-shot transport blips: the first N find_pool_for_coins reads
        # raise, then recover — the confirm-and-re-read hardening must survive.
        self._find_blips_remaining = find_blips
        self._healthy = healthy

    def eth_call(self, *, chain: str, to: str, data: str) -> str:
        selector = data[:10]
        if selector == _GET_ADDRESS_SEL:
            arg = int(data[10:], 16)
            if arg == 0 and not self._healthy:
                raise ValueError("transport error: connection reset")
            return "0x" + _addr_word(META_REGISTRY)
        if selector == _FIND_POOL_FOR_COINS_SEL:
            if self._find_blips_remaining > 0:
                self._find_blips_remaining -= 1
                raise ValueError("transport error: read timeout")
            index = int(data[10 + 128 :], 16)
            if self._find_reverts_at is not None and index >= self._find_reverts_at:
                raise ValueError("execution reverted")
            if index < len(self._order):
                return "0x" + _addr_word(self._order[index])
            return "0x" + _addr_word(ZERO)
        pool = "0x" + data[10:][24:64]
        if selector == _GET_BALANCES_SEL:
            balances = self._pools[pool]
            return "0x" + _uint_array(list(balances))
        if selector == _GET_POOL_NAME_SEL:
            if pool in self._name_reverts:
                raise ValueError("execution reverted")
            return "0x" + _uint_word(32) + _uint_word(4) + b"Pool".hex().ljust(64, "0")
        raise ValueError(f"unexpected selector {selector}")


@pytest.fixture(autouse=True)
def _canned_metadata(monkeypatch: pytest.MonkeyPatch):
    """Answer the shape step with canned metadata (covered by its own suite)."""

    def fake_resolve(chain, pool_address, **_kwargs):
        return _metadata(pool_address)

    monkeypatch.setattr(pair_resolver, "resolve_pool_metadata", fake_resolve)
    pool_resolver._clear_cache()
    yield
    pool_resolver._clear_cache()


def _prices(symbol: str) -> Decimal | None:
    return {"CRVUSD": Decimal("1"), "WBTC": Decimal("100000")}.get(symbol)


def _build(gateway: FakePairGateway, **kwargs) -> PairCandidateSet:
    return build_pair_candidates(
        "ethereum", "CRVUSD/WBTC", CRVUSD, WBTC, gateway_client=gateway, usd_price=_prices, **kwargs
    )


# Raw reserve fixtures (18-decimals crvUSD, 8-decimals WBTC):
_BIG = (20_000_000 * 10**18, 300 * 10**8)  # ~$50M
_MID = (100_000 * 10**18, 1 * 10**8)  # ~$200k
_DUST = (100 * 10**18, 0)  # ~$100


class TestBuildPairCandidates:
    def test_ranks_by_usd_liquidity_and_floors_dust(self) -> None:
        gw = FakePairGateway({POOL_MID: _MID, POOL_BIG: _BIG, POOL_DUST: _DUST})
        result = _build(gw)
        assert not result.indeterminate
        assert [c.address.lower() for c in result.ranked] == [POOL_BIG.lower(), POOL_MID.lower()]
        assert result.ranked[0].liquidity_usd > result.ranked[1].liquidity_usd
        (dust,) = result.rejected
        assert dust.address.lower() == POOL_DUST.lower()
        assert "below" in dust.rejection and "floor" in dust.rejection

    def test_confirmed_non_pool_candidate_is_rejected_not_fatal(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def resolve_only_big(chain, pool_address, **_kwargs):
            return _metadata(pool_address) if pool_address.lower() == POOL_BIG.lower() else None

        monkeypatch.setattr(pair_resolver, "resolve_pool_metadata", resolve_only_big)
        # A DEFINITIVE miss is one the resolver cached (transport-confirmed
        # not-a-plain-pool) — seed the memo the way the real resolver would.
        pool_resolver._METADATA_CACHE[("ethereum", POOL_MID.lower())] = None
        gw = FakePairGateway({POOL_BIG: _BIG, POOL_MID: _MID})
        result = _build(gw)
        assert [c.address.lower() for c in result.ranked] == [POOL_BIG.lower()]
        (miss,) = result.rejected
        assert "not a plain Curve pool" in miss.rejection

    def test_transient_metadata_blip_is_indeterminate_not_a_rejection(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """resolve_pool_metadata returns None UNCACHED on a transient blip — the
        screen must not record that as a determinate rejection, or an
        all-candidates blip fabricates a determinate honest-miss (Codex +
        pr-auditor high-confidence audit finding)."""

        def resolve_blips(chain, pool_address, **_kwargs):
            return None  # and deliberately NOT cached → indistinguishable from a blip

        monkeypatch.setattr(pair_resolver, "resolve_pool_metadata", resolve_blips)
        gw = FakePairGateway({POOL_BIG: _BIG})
        result = _build(gw)
        assert result.indeterminate is True
        assert not result.rejected

    def test_superset_pool_is_rejected_by_the_exact_pair_screen(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """find_pool_for_coins matches CONTAINING pools (tricryptos included);
        a pair request must never resolve to one — LP amounts map positionally
        to pool coin indices (VIB-3946 discipline, the WETH/WBTC→tricrypto2
        near-miss caught in review)."""
        weth = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"

        def resolve_tricrypto(chain, pool_address, **_kwargs):
            base = _metadata(pool_address)
            return CurvePoolMetadata(
                address=base.address,
                lp_token=base.lp_token,
                coin_addresses=[CRVUSD, WBTC, weth],
                coin_decimals=[18, 8, 18],
                coin_symbols=["CRVUSD", "WBTC", "WETH"],
                n_coins=3,
                pool_type="tricrypto",
                is_metapool=False,
                base_pool=None,
                base_pool_coin_addresses=None,
                base_pool_coins=None,
            )

        monkeypatch.setattr(pair_resolver, "resolve_pool_metadata", resolve_tricrypto)
        gw = FakePairGateway({POOL_BIG: _BIG})
        result = _build(gw)
        assert not result.ranked
        (miss,) = result.rejected
        assert "not exactly the requested pair" in miss.rejection

    def test_unpriceable_reserves_are_rejected_with_reason(self) -> None:
        gw = FakePairGateway({POOL_BIG: _BIG})
        result = build_pair_candidates(
            "ethereum", "CRVUSD/WBTC", CRVUSD, WBTC, gateway_client=gw, usd_price=lambda _s: None
        )
        assert not result.ranked
        (miss,) = result.rejected
        assert "cannot price" in miss.rejection

    def test_provenance_tell_marks_suspect_without_rejecting(self) -> None:
        gw = FakePairGateway({POOL_BIG: _BIG}, name_reverts=frozenset({POOL_BIG}))
        result = _build(gw)
        (candidate,) = result.ranked
        assert candidate.provenance_suspect is True
        assert candidate.rejection is None

    def test_floor_zero_disables_liquidity_screen(self) -> None:
        gw = FakePairGateway({POOL_DUST: _DUST})
        result = _build(gw, liquidity_floor_usd=Decimal(0))
        assert [c.address.lower() for c in result.ranked] == [POOL_DUST.lower()]
        assert result.ranked[0].liquidity_usd is None

    def test_no_transport_is_indeterminate(self) -> None:
        result = build_pair_candidates("ethereum", "CRVUSD/WBTC", CRVUSD, WBTC, usd_price=_prices)
        assert result.indeterminate is True

    def test_persistent_enumeration_failure_is_never_a_fabricated_empty(self) -> None:
        """find_pool_for_coins terminates with the ZERO ADDRESS, never a revert —
        so even a health-confirmed, re-read-confirmed read failure must yield
        INDETERMINATE (legacy miss path), not "no pools exist" (the cold-fork
        false-empty caught during the VIB-5716 fork acceptance)."""
        gw = FakePairGateway({POOL_BIG: _BIG, POOL_MID: _MID}, find_reverts_at=1)
        result = _build(gw)
        assert result.indeterminate is True

    def test_single_enumeration_blip_recovers_via_re_read(self) -> None:
        gw = FakePairGateway({POOL_BIG: _BIG, POOL_MID: _MID}, find_blips=1)
        result = _build(gw)
        assert not result.indeterminate
        assert [c.address.lower() for c in result.ranked] == [POOL_BIG.lower(), POOL_MID.lower()]

    def test_enumeration_failure_on_unhealthy_transport_is_indeterminate(self) -> None:
        gw = FakePairGateway({POOL_BIG: _BIG}, find_reverts_at=0, healthy=False)
        result = _build(gw)
        assert result.indeterminate is True

    def test_zero_matches_is_definitive_empty(self) -> None:
        gw = FakePairGateway({})
        result = _build(gw)
        assert not result.indeterminate
        assert not result.ranked and not result.rejected

    def test_cap_reached_without_terminator_is_indeterminate(self) -> None:
        """32+ pools with no zero-address terminator = an incomplete universe;
        treating it as complete could hide a deployable pool past the cap."""
        many = {f"0x{i + 1:040x}": _BIG for i in range(pair_resolver._MAX_PAIR_POOLS + 1)}
        gw = FakePairGateway(many)
        result = _build(gw)
        assert result.indeterminate is True


class TestClassifyProbe:
    def _revert(self, reason: str | None) -> StaticCallProbe:
        return StaticCallProbe(outcome="revert", revert_reason=reason)

    def test_success_passes(self) -> None:
        ok, _ = classify_add_liquidity_probe(StaticCallProbe(outcome="success"), provenance_suspect=True)
        assert ok is True

    def test_explicit_gate_disqualifies_even_with_clean_provenance(self) -> None:
        ok, detail = classify_add_liquidity_probe(self._revert("!wl"), provenance_suspect=False)
        assert ok is False
        assert "!wl" in detail

    @pytest.mark.parametrize(
        "reason",
        [
            "ERC20: transfer amount exceeds allowance",
            "Insufficient balance",
            "Dai/insufficient-allowance",
            "insufficient funds for transfer",
            "STF",
            "SafeERC20: low-level call failed",
            f"custom error {pair_resolver._selector('ERC20InsufficientAllowance(address,uint256,uint256)')}",
        ],
    )
    def test_prefund_shaped_reverts_pass(self, reason: str) -> None:
        ok, _ = classify_add_liquidity_probe(self._revert(reason), provenance_suspect=False)
        assert ok is True

    def test_unknown_custom_error_disqualifies(self) -> None:
        ok, _ = classify_add_liquidity_probe(self._revert("custom error 0xdeadbeef"), provenance_suspect=False)
        assert ok is False

    def test_stf_matches_exactly_not_as_substring(self) -> None:
        ok, _ = classify_add_liquidity_probe(self._revert("stfoo gate"), provenance_suspect=False)
        assert ok is False

    def test_reasonless_revert_defers_to_provenance(self) -> None:
        ok_clean, _ = classify_add_liquidity_probe(self._revert(None), provenance_suspect=False)
        ok_suspect, detail = classify_add_liquidity_probe(self._revert(None), provenance_suspect=True)
        assert ok_clean is True  # USDT-style reasonless prefund revert must not false-reject
        assert ok_suspect is False
        assert "get_pool_name" in detail

    def test_transport_outcome_defers_to_provenance(self) -> None:
        probe = StaticCallProbe(outcome="transport", error="timeout")
        assert classify_add_liquidity_probe(probe, provenance_suspect=False)[0] is True
        assert classify_add_liquidity_probe(probe, provenance_suspect=True)[0] is False


class TestProvenanceHelper:
    def test_readable_name_is_not_suspect(self) -> None:
        gw = FakePairGateway({POOL_BIG: _BIG})
        assert pool_provenance_suspect("ethereum", POOL_BIG, gateway_client=gw) is False

    def test_confirmed_name_revert_is_suspect(self) -> None:
        gw = FakePairGateway({POOL_BIG: _BIG}, name_reverts=frozenset({POOL_BIG}))
        assert pool_provenance_suspect("ethereum", POOL_BIG, gateway_client=gw) is True

    def test_unconfirmable_revert_is_not_suspect(self) -> None:
        gw = FakePairGateway({POOL_BIG: _BIG}, name_reverts=frozenset({POOL_BIG}), healthy=False)
        assert pool_provenance_suspect("ethereum", POOL_BIG, gateway_client=gw) is False

    def test_no_transport_is_not_suspect(self) -> None:
        assert pool_provenance_suspect("ethereum", POOL_BIG) is False

    def test_tell_is_not_consulted_on_unverified_chains(self) -> None:
        """The get_pool_name tell is Ethereum-verified only; on other chains a
        name revert must NOT mark suspicion (it would false-reject legitimate
        USDT-style pools whose prefund reverts are reasonless — audit finding)."""
        gw = FakePairGateway({POOL_BIG: _BIG}, name_reverts=frozenset({POOL_BIG}))
        assert pool_provenance_suspect("arbitrum", POOL_BIG, gateway_client=gw) is False
        result = build_pair_candidates("arbitrum", "CRVUSD/WBTC", CRVUSD, WBTC, gateway_client=gw, usd_price=_prices)
        (candidate,) = result.ranked
        assert candidate.provenance_suspect is False


class TestFormatPairMiss:
    def test_zero_matches_message(self) -> None:
        empty = PairCandidateSet(chain="ethereum", pair_label="CRVUSD/WBTC", ranked=[], rejected=[])
        message = format_pair_miss(empty)
        assert "No Curve pool holds both sides" in message
        assert "CRVUSD/WBTC" in message

    def test_lists_every_rejection_reason(self) -> None:
        gw = FakePairGateway({POOL_BIG: _BIG, POOL_DUST: _DUST})
        result = _build(gw)
        message = format_pair_miss(result, probe_rejections=[(POOL_BIG, "add_liquidity reverted '!wl'")])
        assert "matched 2 pool(s)" in message
        assert "!wl" in message
        assert "floor" in message
        assert "explicit pool address" in message
