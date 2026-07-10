"""Curve compiler wiring for dynamic pair→pool resolution (VIB-5716).

Pins the LP_OPEN / LP_CLOSE resolution order around the new pair lane:

- LP_OPEN pair string: ranked candidates are probed best-liquidity-first; the
  first deployable one wins; a determinate all-rejected outcome is the
  honest-miss FAILED result (never the legacy static-pool-list error).
- LP_OPEN uncurated ADDRESS: the deployability gate turns a deposit-gated pool
  (Yield Basis ``!wl``) from a compile-SUCCESS-that-dies-on-chain into a clean
  FAILED naming the gate.
- LP_CLOSE pair string: selection is by the wallet's LP-token holdings — no
  floor, no probe (closing must never be screened) — with a loud ambiguity
  error and a fall-through (never a false "no position") on unreadable
  balances.
- Indeterminate pair resolution falls through to the legacy unknown-pool
  error, preserving pre-VIB-5716 behaviour when there is no transport.

The pair-resolver internals and the probe seam are covered by their own
suites; here they are stubbed at their module seams to drive the compiler's
decision logic deterministically.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import pytest

from almanak.connectors.curve import compiler as curve_compiler
from almanak.connectors.curve import pair_resolver
from almanak.connectors.curve.compiler import CurveCompiler
from almanak.connectors.curve.pair_resolver import PairCandidate, PairCandidateSet
from almanak.connectors.curve.pool_resolver import CurvePoolMetadata
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus
from almanak.framework.intents.vocabulary import LPCloseIntent, LPOpenIntent

WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
CRVUSD = "0xf939E0A03FB07F59A73314E73794Be0E57ac1b4E"
WBTC = "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"
POOL_A = "0x1111111111111111111111111111111111111111"
POOL_B = "0x2222222222222222222222222222222222222222"
LP_A = "0xaaaa111111111111111111111111111111111111"
LP_B = "0xaaaa222222222222222222222222222222222222"


@dataclass
class _Token:
    address: str
    symbol: str


class _Services:
    """resolve_token + query_erc20_balance stub for the pair lane."""

    def __init__(self, balances: dict[str, int | None] | None = None) -> None:
        self._tokens = {"CRVUSD": _Token(CRVUSD, "CRVUSD"), "WBTC": _Token(WBTC, "WBTC")}
        self._balances = balances or {}
        self.balance_queries: list[str] = []

    def resolve_token(self, token: str, chain: str | None = None) -> _Token | None:
        return self._tokens.get(token.upper())

    def require_token_price(self, symbol: str) -> Decimal:
        return Decimal("1")

    def query_erc20_balance(self, token_address: str, wallet_address: str) -> int | None:
        self.balance_queries.append(token_address.lower())
        return self._balances.get(token_address.lower(), 0)


@dataclass
class _Ctx:
    chain: str = "ethereum"
    wallet_address: str = WALLET
    rpc_url: str | None = "https://eth.example.invalid"
    rpc_timeout: float = 5.0
    gateway_client: Any = None
    services: Any = field(default_factory=_Services)


def _metadata(address: str, lp_token: str) -> CurvePoolMetadata:
    return CurvePoolMetadata(
        address=address,
        lp_token=lp_token,
        coin_addresses=[CRVUSD, WBTC],
        coin_decimals=[18, 8],
        coin_symbols=["CRVUSD", "WBTC"],
        n_coins=2,
        pool_type="cryptoswap",
        is_metapool=False,
        base_pool=None,
        base_pool_coin_addresses=None,
        base_pool_coins=None,
    )


def _candidate(address: str, lp_token: str, liquidity: str, *, suspect: bool = False) -> PairCandidate:
    return PairCandidate(
        address=address,
        metadata=_metadata(address, lp_token),
        liquidity_usd=Decimal(liquidity),
        provenance_suspect=suspect,
        rejection=None,
    )


def _pool_data(address: str, lp_token: str) -> dict[str, Any]:
    return {
        "address": address,
        "lp_token": lp_token,
        "coins": ["CRVUSD", "WBTC"],
        "coin_addresses": [CRVUSD, WBTC],
        "n_coins": 2,
        "pool_type": "cryptoswap",
        "is_metapool": False,
    }


@pytest.fixture()
def stubs(monkeypatch: pytest.MonkeyPatch):
    """Stub the pair-resolver + probe + dynamic-resolve seams around the compiler."""

    class _Stubs:
        candidate_set: PairCandidateSet | None = None
        probe_results: dict[str, tuple[bool, str]] = {}
        probed: list[str] = []
        provenance: bool = False
        flip_coin_order: bool = False

    stubs = _Stubs()

    def fake_build(chain, pair_label, a, b, **kwargs):
        assert stubs.candidate_set is not None, "test forgot to set candidate_set"
        return stubs.candidate_set

    def fake_probe(ctx, pool_address, *, provenance_suspect):
        stubs.probed.append(pool_address.lower())
        return stubs.probe_results.get(pool_address.lower(), (True, "stub pass"))

    def fake_dynamic(ctx, pool_address):
        lp = {POOL_A.lower(): LP_A, POOL_B.lower(): LP_B}[pool_address.lower()]
        data = _pool_data(pool_address, lp)
        if stubs.flip_coin_order:
            data["coin_addresses"] = list(reversed(data["coin_addresses"]))
            data["coins"] = list(reversed(data["coins"]))
        return f"dynamic:{pool_address[:10]}", data

    monkeypatch.setattr(pair_resolver, "build_pair_candidates", fake_build)
    monkeypatch.setattr(pair_resolver, "pool_provenance_suspect", lambda *a, **k: stubs.provenance)
    monkeypatch.setattr(curve_compiler, "_probe_lp_open_deployability", fake_probe)
    monkeypatch.setattr(curve_compiler, "_resolve_dynamic_pool", fake_dynamic)
    return stubs


def _open_intent(pool: str) -> LPOpenIntent:
    return LPOpenIntent(
        pool=pool,
        amount0=Decimal("10"),
        amount1=Decimal("0.0001"),
        range_lower=Decimal("1"),
        range_upper=Decimal("2"),
        protocol="curve",
    )


def _close_intent(pool: str) -> LPCloseIntent:
    return LPCloseIntent(pool=pool, position_id="all", protocol="curve")


class TestOpenPairLane:
    def test_first_deployable_candidate_wins_in_rank_order(self, stubs) -> None:
        stubs.candidate_set = PairCandidateSet(
            chain="ethereum",
            pair_label="CRVUSD/WBTC",
            ranked=[_candidate(POOL_A, LP_A, "60000000", suspect=True), _candidate(POOL_B, LP_B, "200000")],
            rejected=[],
        )
        stubs.probe_results = {POOL_A.lower(): (False, "add_liquidity reverted '!wl'")}
        resolved = CurveCompiler()._resolve_open_pool(_Ctx(), _open_intent("CRVUSD/WBTC"))
        assert not isinstance(resolved, CompilationResult)
        name, data, aligned = resolved
        assert data["address"] == POOL_B
        assert stubs.probed == [POOL_A.lower(), POOL_B.lower()]

    def test_amounts_realign_to_the_pool_coin_order(self, stubs) -> None:
        """The user's amounts follow the PAIR STRING; an uncurated pool's coin
        order is unknowable in advance — a flipped pool must swap amount0/1,
        never deposit them positionally into the wrong coins."""
        stubs.candidate_set = PairCandidateSet(
            chain="ethereum",
            pair_label="CRVUSD/WBTC",
            ranked=[_candidate(POOL_A, LP_A, "60000000")],
            rejected=[],
        )
        stubs.flip_coin_order = True
        intent = _open_intent("CRVUSD/WBTC")  # amount0=10 crvUSD, amount1=0.0001 WBTC
        resolved = CurveCompiler()._resolve_open_pool(_Ctx(), intent)
        assert not isinstance(resolved, CompilationResult)
        _name, data, aligned = resolved
        assert data["coin_addresses"] == [WBTC, CRVUSD]
        assert aligned.amount0 == Decimal("0.0001")  # WBTC now leads
        assert aligned.amount1 == Decimal("10")
        assert intent.amount0 == Decimal("10")  # original intent untouched

    def test_all_candidates_rejected_is_the_honest_miss(self, stubs) -> None:
        stubs.candidate_set = PairCandidateSet(
            chain="ethereum",
            pair_label="CRVUSD/WBTC",
            ranked=[_candidate(POOL_A, LP_A, "60000000")],
            rejected=[
                PairCandidate(
                    address=POOL_B,
                    metadata=None,
                    liquidity_usd=Decimal("100"),
                    provenance_suspect=False,
                    rejection="~$100 liquidity below the $10,000 floor",
                )
            ],
        )
        stubs.probe_results = {POOL_A.lower(): (False, "add_liquidity reverted '!wl'")}
        result = CurveCompiler()._resolve_open_pool(_Ctx(), _open_intent("CRVUSD/WBTC"))
        assert isinstance(result, CompilationResult)
        assert result.status == CompilationStatus.FAILED
        assert "!wl" in result.error
        assert "floor" in result.error
        assert "Available pools" not in result.error  # never the misleading static list

    def test_indeterminate_falls_through_to_legacy_error(self, stubs) -> None:
        stubs.candidate_set = PairCandidateSet(
            chain="ethereum", pair_label="CRVUSD/WBTC", ranked=[], rejected=[], indeterminate=True
        )
        result = CurveCompiler()._resolve_open_pool(_Ctx(), _open_intent("CRVUSD/WBTC"))
        assert isinstance(result, CompilationResult)
        assert "Unknown Curve pool" in result.error

    def test_unresolvable_symbol_falls_through_to_legacy_error(self, stubs) -> None:
        stubs.candidate_set = PairCandidateSet(
            chain="ethereum", pair_label="x", ranked=[], rejected=[], indeterminate=False
        )
        result = CurveCompiler()._resolve_open_pool(_Ctx(), _open_intent("NOSUCH/WBTC"))
        assert isinstance(result, CompilationResult)
        assert "Unknown Curve pool" in result.error

    def test_three_token_sets_stay_curated_only(self, stubs) -> None:
        result = CurveCompiler()._resolve_open_pool(_Ctx(), _open_intent("CRVUSD/WBTC/WETH"))
        assert isinstance(result, CompilationResult)
        assert "Unknown Curve pool" in result.error

    def test_coin_amounts_with_a_pair_string_is_refused(self, stubs) -> None:
        """coin_amounts is pool-coin-indexed; with a pair string the framework
        picks the pool, so the user cannot have indexed it — passing it through
        would deposit into the wrong coins on flipped pools (audit blocker)."""
        intent = _open_intent("CRVUSD/WBTC").model_copy(update={"coin_amounts": [Decimal("10"), Decimal("0.0001")]})
        result = CurveCompiler()._resolve_open_pool(_Ctx(), intent)
        assert isinstance(result, CompilationResult)
        assert "coin_amounts is pool-coin-indexed" in result.error
        assert "explicit pool address" in result.error


class TestOpenAddressGate:
    def test_gated_uncurated_address_fails_with_the_gate_reason(self, stubs) -> None:
        stubs.probe_results = {POOL_A.lower(): (False, "add_liquidity reverted '!wl' — deposit-gated")}
        stubs.provenance = True
        result = CurveCompiler()._resolve_open_pool(_Ctx(), _open_intent(POOL_A))
        assert isinstance(result, CompilationResult)
        assert result.status == CompilationStatus.FAILED
        assert "not LP-deployable" in result.error
        assert "!wl" in result.error

    def test_deployable_uncurated_address_resolves(self, stubs) -> None:
        resolved = CurveCompiler()._resolve_open_pool(_Ctx(), _open_intent(POOL_A))
        assert not isinstance(resolved, CompilationResult)
        assert resolved[1]["address"] == POOL_A


class TestClosePairLane:
    def _resolve(self, stubs, balances: dict[str, int | None]):
        stubs.candidate_set = PairCandidateSet(
            chain="ethereum",
            pair_label="CRVUSD/WBTC",
            ranked=[_candidate(POOL_A, LP_A, "60000000"), _candidate(POOL_B, LP_B, "200000")],
            rejected=[],
        )
        ctx = _Ctx(services=_Services(balances={k.lower(): v for k, v in balances.items()}))
        return CurveCompiler()._resolve_close_pool(ctx, _close_intent("CRVUSD/WBTC")), ctx

    def test_selects_the_pool_the_wallet_holds(self, stubs) -> None:
        resolved, ctx = self._resolve(stubs, {LP_B: 5})
        assert not isinstance(resolved, CompilationResult)
        assert resolved.address == POOL_B
        assert stubs.probed == []  # closing is NEVER probe-gated

    def test_holdings_in_two_pools_is_a_loud_ambiguity(self, stubs) -> None:
        resolved, _ctx = self._resolve(stubs, {LP_A: 5, LP_B: 7})
        assert isinstance(resolved, CompilationResult)
        assert "Ambiguous" in resolved.error

    def test_no_holdings_is_an_honest_no_position_miss(self, stubs) -> None:
        resolved, _ctx = self._resolve(stubs, {})
        assert isinstance(resolved, CompilationResult)
        assert "holds no LP tokens" in resolved.error

    def test_unreadable_balance_never_claims_no_position(self, stubs) -> None:
        resolved, _ctx = self._resolve(stubs, {LP_A: None, LP_B: 0})
        assert isinstance(resolved, CompilationResult)
        # Falls through to the legacy unknown-pool error, not a false honest-miss.
        assert "Unknown Curve pool" in resolved.error

    def test_shaped_exits_with_a_pair_string_are_refused(self, stubs) -> None:
        """imbalanced_amounts / coin_index are pool-coin-indexed — the same
        positional hazard as coin_amounts on the open lane (audit blocker)."""
        stubs.candidate_set = PairCandidateSet(
            chain="ethereum", pair_label="CRVUSD/WBTC", ranked=[_candidate(POOL_A, LP_A, "1000")], rejected=[]
        )
        ctx = _Ctx(services=_Services(balances={LP_A.lower(): 5}))
        for update in ({"coin_index": 1}, {"imbalanced_amounts": [Decimal("1"), Decimal("2")]}):
            intent = _close_intent("CRVUSD/WBTC").model_copy(update=update)
            resolved = CurveCompiler()._resolve_close_pool(ctx, intent)
            assert isinstance(resolved, CompilationResult)
            assert "pool-coin-indexed" in resolved.error, update


class TestProbeDeployability:
    """Direct branch coverage for _probe_lp_open_deployability (the other suites
    stub it at the compiler seam). Every degraded path must collapse into the
    classified INCONCLUSIVE outcome — never an exception out of the probe."""

    class _FakeAdapter:
        behavior = "ok"
        decimals: list[int] | None = [18, 8]

        def __init__(self, config) -> None:
            self.config = config

        def get_pool_info(self, pool_address):
            from types import SimpleNamespace

            if self.behavior == "raise":
                raise RuntimeError("malformed pool boom")
            if self.behavior == "no_info":
                return None
            return SimpleNamespace(coin_decimals=self.decimals, n_coins=2)

        def add_liquidity(self, pool_address, amounts):
            from types import SimpleNamespace

            self.__class__.seen_amounts = list(amounts)
            if self.behavior == "build_fail":
                return SimpleNamespace(success=False, error="quote read failed", transactions=[])
            txs = [SimpleNamespace(tx_type="approve", to=CRVUSD, data="0x00", value=0)]
            if self.behavior != "no_addliq_tx":
                txs.append(SimpleNamespace(tx_type="add_liquidity", to=pool_address, data="0x1234", value=7))
            return SimpleNamespace(success=True, transactions=txs)

    @pytest.fixture()
    def probe_env(self, monkeypatch: pytest.MonkeyPatch):
        from almanak.connectors._strategy_base.rpc import StaticCallProbe

        env = {"probe": StaticCallProbe(outcome="success"), "calls": []}

        def fake_probe(**kwargs):
            env["calls"].append(kwargs)
            return env["probe"]

        self._FakeAdapter.behavior = "ok"
        self._FakeAdapter.decimals = [18, 8]
        monkeypatch.setattr("almanak.connectors.curve.adapter.CurveAdapter", self._FakeAdapter)
        monkeypatch.setattr("almanak.connectors._strategy_base.rpc.eth_call_static_probe", fake_probe)
        return env

    def _probe(self, *, suspect: bool = False):
        return curve_compiler._probe_lp_open_deployability(_Ctx(), POOL_A, provenance_suspect=suspect)

    def test_happy_path_probes_the_real_bundle_tx(self, probe_env) -> None:
        deployable, detail = self._probe()
        assert deployable is True
        assert "succeeded" in detail
        (call,) = probe_env["calls"]
        assert call["from_address"] == WALLET
        assert call["to"] == POOL_A
        assert call["value"] == 7

    def test_gate_revert_disqualifies(self, probe_env) -> None:
        from almanak.connectors._strategy_base.rpc import StaticCallProbe

        probe_env["probe"] = StaticCallProbe(outcome="revert", revert_reason="!wl")
        deployable, detail = self._probe()
        assert deployable is False
        assert "!wl" in detail

    @pytest.mark.parametrize("behavior", ["raise", "no_info", "build_fail", "no_addliq_tx"])
    def test_degraded_bundle_paths_are_inconclusive_and_provenance_decides(self, probe_env, behavior) -> None:
        self._FakeAdapter.behavior = behavior
        assert self._probe(suspect=False)[0] is True  # clean provenance → fail-safe pass
        assert self._probe(suspect=True)[0] is False  # suspect provenance → fail closed
        assert not probe_env["calls"]  # the static probe never ran

    def test_missing_decimals_fall_back_to_18_for_dust_sizing_only(self, probe_env) -> None:
        self._FakeAdapter.decimals = None
        deployable, _ = self._probe()
        assert deployable is True
        assert self._FakeAdapter.seen_amounts == [Decimal("0.001"), Decimal("0.001")]
