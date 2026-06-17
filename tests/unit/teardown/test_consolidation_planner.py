"""VIB-5011 — unit tests for the token-consolidation planner (teardown Phase 2).

Pins the pure planner in ``almanak/framework/teardown/consolidation.py``:

* Incident regression: target-policy residual WETH (~$18) plans exactly one
  ``SWAP WETH → USDC amount='all'`` while a $0.02 dust residual is skipped.
* The chain's native gas symbol is never swapped; wrapped native (WETH) IS.
* keep_outputs policy and HARD (emergency) mode produce empty plans.
* entry_token policy resolves via the teardown profile, falls back to the
  earliest SWAP accounting event, and degrades loudly (empty plan + warning)
  when undiscoverable — never guesses a trade.
* The token universe is strategy-scoped: an out-of-universe wallet token is
  excluded (shared-wallet protection).
* A token with no price is skipped with a warning (Empty ≠ Zero); a zero
  balance plans no intent.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

from almanak.framework.teardown.config import TokenConsolidationConfig
from almanak.framework.teardown.consolidation import (
    ConsolidationOutcome,
    derive_strategy_token_universe,
    fold_consolidation_outcome,
    plan_consolidation,
    resolve_consolidation_targets,
)
from almanak.framework.teardown.models import (
    PositionInfo,
    PositionType,
    TeardownAssetPolicy,
    TeardownMode,
    TeardownPositionSummary,
    TeardownResult,
)

CHAIN = "ethereum"


class FakeMarket:
    """Minimal market double: per-token balances + prices, call-recording."""

    def __init__(self, balances: dict[str, Decimal], prices: dict[str, Decimal]):
        self._balances = balances
        self._prices = prices
        self.balance_calls: list[str] = []

    def balance(self, token: str, chain: str | None = None):  # noqa: ARG002
        self.balance_calls.append(token)
        if token not in self._balances:
            raise ValueError(f"token {token} not registered")
        return SimpleNamespace(balance=self._balances[token])

    def price(self, token: str, chain: str | None = None) -> Decimal:  # noqa: ARG002
        if token not in self._prices:
            raise ValueError(f"no price for {token}")
        return self._prices[token]

    def get_price_oracle_dict(self) -> dict:
        return dict(self._prices)


def _plan(
    *,
    market,
    universe,
    mode=TeardownMode.SOFT,
    asset_policy=TeardownAssetPolicy.TARGET_TOKEN,
    target_token="USDC",
    cfg=None,
    targets=None,
    wallet_tokens=None,
):
    return plan_consolidation(
        market=market,
        chain=CHAIN,
        asset_policy=asset_policy,
        target_token=target_token,
        token_consolidation_cfg=cfg or TokenConsolidationConfig(),
        token_universe=universe,
        mode=mode,
        targets=targets,
        wallet_tokens=wallet_tokens,
    )


def _swap_tokens(plan):
    return [(getattr(i, "from_token", None), getattr(i, "to_token", None)) for i in plan.intents]


def _decision(plan, token):
    matches = [d for d in plan.decisions if d.token == token]
    assert matches, f"no decision recorded for {token}: {plan.decisions}"
    return matches[0]


class TestTargetPolicyPlanning:
    def test_residual_weth_plans_single_swap_to_usdc(self):
        """Incident regression: 0.011 WETH (~$18) left after LP_CLOSE must
        plan exactly one consolidation swap to USDC."""
        market = FakeMarket(
            balances={"WETH": Decimal("0.011"), "USDC": Decimal("12")},
            prices={"WETH": Decimal("1650"), "USDC": Decimal("1")},
        )
        plan = _plan(market=market, universe={"WETH", "USDC"})
        assert _swap_tokens(plan) == [("WETH", "USDC")]
        weth = _decision(plan, "WETH")
        assert weth.action == "swap"
        assert weth.value_usd == Decimal("0.011") * Decimal("1650")
        # amount='all' so the live balance is swept at execution time.
        assert plan.intents[0].amount == "all"
        assert plan.intents[0].chain == CHAIN

    def test_dust_skipped_while_material_residual_swaps(self):
        """$0.02 dust is below the $5 floor and never swapped; $18 swaps."""
        market = FakeMarket(
            balances={"WETH": Decimal("0.011"), "DAI": Decimal("0.02")},
            prices={"WETH": Decimal("1650"), "DAI": Decimal("1")},
        )
        plan = _plan(market=market, universe={"WETH", "DAI"})
        assert _swap_tokens(plan) == [("WETH", "USDC")]
        dai = _decision(plan, "DAI")
        assert dai.action == "skip"
        assert dai.reason == "below_dust"
        assert dai.value_usd == Decimal("0.02")

    def test_default_dust_floor_is_five_usd(self):
        """VIB-5011 raised the dust default $1 → $5: a $3 residual stays."""
        assert TokenConsolidationConfig().min_swap_value_usd == Decimal("5")
        market = FakeMarket(
            balances={"DAI": Decimal("3")},
            prices={"DAI": Decimal("1")},
        )
        plan = _plan(market=market, universe={"DAI"})
        assert plan.intents == []
        assert _decision(plan, "DAI").reason == "below_dust"

    def test_native_gas_never_swapped_wrapped_native_is(self):
        market = FakeMarket(
            balances={"ETH": Decimal("1"), "WETH": Decimal("1")},
            prices={"ETH": Decimal("1650"), "WETH": Decimal("1650")},
        )
        plan = _plan(market=market, universe={"ETH", "WETH"})
        assert _swap_tokens(plan) == [("WETH", "USDC")]
        eth = _decision(plan, "ETH")
        assert eth.action == "skip"
        assert eth.reason == "native_gas"
        # The native token's balance was never even read.
        assert "ETH" not in market.balance_calls

    def test_target_token_itself_never_swapped(self):
        market = FakeMarket(
            balances={"USDC": Decimal("500")},
            prices={"USDC": Decimal("1")},
        )
        plan = _plan(market=market, universe={"USDC"})
        assert plan.intents == []
        assert _decision(plan, "USDC").reason == "target"

    def test_keep_tokens_excluded(self):
        market = FakeMarket(
            balances={"WETH": Decimal("1"), "ARB": Decimal("100")},
            prices={"WETH": Decimal("1650"), "ARB": Decimal("2")},
        )
        cfg = TokenConsolidationConfig(keep_tokens=["arb"])
        plan = _plan(market=market, universe={"WETH", "ARB"}, cfg=cfg)
        assert _swap_tokens(plan) == [("WETH", "USDC")]
        assert _decision(plan, "ARB").reason == "keep_token"

    def test_zero_balance_plans_no_intent(self):
        """A strategy that already swept leaves ~0 residual — structural
        double-swap safety: the planner emits nothing."""
        market = FakeMarket(
            balances={"WETH": Decimal("0")},
            prices={"WETH": Decimal("1650")},
        )
        plan = _plan(market=market, universe={"WETH"})
        assert plan.intents == []
        # VIB-5074 secondary defect: a measured zero is "zero_balance", not
        # "below_dust" — there is no residual at all, not a sub-floor one.
        weth = _decision(plan, "WETH")
        assert weth.reason == "zero_balance"
        assert weth.value_usd == Decimal("0")

    def test_no_price_token_skipped_with_warning(self):
        """Empty ≠ Zero: an unmeasured price means skip, never assume."""
        market = FakeMarket(
            balances={"WETH": Decimal("1"), "OBSCURE": Decimal("1000")},
            prices={"WETH": Decimal("1650")},  # no OBSCURE price
        )
        plan = _plan(market=market, universe={"WETH", "OBSCURE"})
        assert _swap_tokens(plan) == [("WETH", "USDC")]
        obscure = _decision(plan, "OBSCURE")
        assert obscure.action == "skip"
        assert obscure.reason == "no_price"
        assert any("OBSCURE" in w for w in plan.warnings)

    def test_balance_unavailable_skipped_with_warning(self):
        market = FakeMarket(balances={}, prices={})
        plan = _plan(market=market, universe={"WETH"})
        assert plan.intents == []
        assert _decision(plan, "WETH").reason == "balance_unavailable"
        assert any("WETH" in w for w in plan.warnings)

    def test_out_of_universe_wallet_token_excluded(self):
        """Shared-wallet protection: a wallet token outside the strategy's
        universe is never swapped (it may belong to a sibling deployment),
        and is surfaced in the decision trail as not_in_universe."""
        market = FakeMarket(
            balances={"WETH": Decimal("1"), "SIBLING": Decimal("9999")},
            prices={"WETH": Decimal("1650"), "SIBLING": Decimal("10")},
        )
        plan = _plan(market=market, universe={"WETH"}, wallet_tokens={"SIBLING", "WETH"})
        assert _swap_tokens(plan) == [("WETH", "USDC")]
        sibling = _decision(plan, "SIBLING")
        assert sibling.action == "skip"
        assert sibling.reason == "not_in_universe"
        # Never even reads the sibling balance.
        assert "SIBLING" not in market.balance_calls


class TestModesAndPolicies:
    def test_hard_mode_plans_nothing_with_emergency_warning(self):
        market = FakeMarket(
            balances={"WETH": Decimal("10")},
            prices={"WETH": Decimal("1650")},
        )
        plan = _plan(market=market, universe={"WETH"}, mode=TeardownMode.HARD)
        assert plan.intents == []
        assert any("emergency_mode" in w for w in plan.warnings)

    def test_keep_outputs_policy_plans_nothing(self):
        market = FakeMarket(
            balances={"WETH": Decimal("10")},
            prices={"WETH": Decimal("1650")},
        )
        plan = _plan(
            market=market,
            universe={"WETH"},
            asset_policy=TeardownAssetPolicy.KEEP_OUTPUTS,
        )
        assert plan.intents == []
        assert plan.warnings == []

    def test_disabled_consolidation_plans_nothing(self):
        market = FakeMarket(
            balances={"WETH": Decimal("10")},
            prices={"WETH": Decimal("1650")},
        )
        cfg = TokenConsolidationConfig(enabled=False)
        plan = _plan(market=market, universe={"WETH"}, cfg=cfg)
        assert plan.intents == []


class TestEntryPolicyTargets:
    def _strategy_with_profile(self, entry_assets):
        from almanak.framework.teardown.models import TeardownProfile

        return SimpleNamespace(get_teardown_profile=lambda: TeardownProfile(original_entry_assets=entry_assets))

    def test_entry_policy_via_profile(self):
        strategy = self._strategy_with_profile(["WBTC"])
        targets, warnings = resolve_consolidation_targets(TeardownAssetPolicy.ENTRY_TOKEN, "USDC", strategy)
        assert targets == {"WBTC"}
        assert warnings == []

    def test_entry_policy_via_earliest_swap_event_fallback(self):
        strategy = self._strategy_with_profile([])
        events = [
            # Non-SWAP event first — must be ignored.
            {"event_type": "OPEN", "position_key": "lp:eth:0xabc", "payload_json": '{"token0": "WETH"}'},
            # Earliest SWAP: entered with USDT.
            {
                "event_type": "SWAP",
                "position_key": "swap:ethereum:0xw",
                "payload_json": '{"token_in": "USDT", "token_out": "WETH"}',
            },
            # Later SWAP must NOT win.
            {
                "event_type": "SWAP",
                "position_key": "swap:ethereum:0xw",
                "payload_json": '{"token_in": "DAI", "token_out": "WETH"}',
            },
        ]
        targets, warnings = resolve_consolidation_targets(
            TeardownAssetPolicy.ENTRY_TOKEN, "USDC", strategy, accounting_events=events
        )
        assert targets == {"USDT"}
        assert warnings == []

    def test_entry_policy_undiscoverable_degrades_with_warning(self):
        """Never guess a trade: no profile assets + no SWAP events → no
        consolidation, loud warning."""
        strategy = self._strategy_with_profile([])
        targets, warnings = resolve_consolidation_targets(
            TeardownAssetPolicy.ENTRY_TOKEN, "USDC", strategy, accounting_events=[]
        )
        assert targets is None
        assert len(warnings) == 1
        assert "entry" in warnings[0].lower()

        # And the plan built from a None target set is empty.
        market = FakeMarket(
            balances={"WETH": Decimal("10")},
            prices={"WETH": Decimal("1650")},
        )
        plan = _plan(
            market=market,
            universe={"WETH"},
            asset_policy=TeardownAssetPolicy.ENTRY_TOKEN,
            targets=None,
        )
        assert plan.intents == []

    def test_target_policy_targets(self):
        targets, warnings = resolve_consolidation_targets(TeardownAssetPolicy.TARGET_TOKEN, "usdt", SimpleNamespace())
        # Original casing preserved — the symbol feeds Intent.swap(to_token=...).
        assert targets == {"usdt"}
        assert warnings == []

    def test_keep_policy_targets_none(self):
        targets, warnings = resolve_consolidation_targets(TeardownAssetPolicy.KEEP_OUTPUTS, "USDC", SimpleNamespace())
        assert targets is None
        assert warnings == []

    def test_entry_targets_flow_into_plan(self):
        market = FakeMarket(
            balances={"WETH": Decimal("0.011")},
            prices={"WETH": Decimal("1650")},
        )
        plan = _plan(
            market=market,
            universe={"WETH"},
            asset_policy=TeardownAssetPolicy.ENTRY_TOKEN,
            targets={"USDT"},
        )
        assert _swap_tokens(plan) == [("WETH", "USDT")]


class TestTokenUniverse:
    def _positions(self):
        return TeardownPositionSummary(
            deployment_id="dep-1",
            timestamp=datetime.now(UTC),
            positions=[
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id="123",
                    chain=CHAIN,
                    protocol="uniswap_v3",
                    value_usd=Decimal("18"),
                    details={"token0": "WETH", "token1": "USDC"},
                )
            ],
        )

    def test_union_of_intents_positions_events_and_profile(self):
        from almanak.framework.teardown.models import TeardownProfile

        sm = SimpleNamespace(
            get_accounting_events_sync=lambda deployment_id: [
                {"payload_json": '{"token_in": "USDT", "token_out": "WETH"}'},
            ]
        )
        strategy = SimpleNamespace(get_teardown_profile=lambda: TeardownProfile(natural_exit_assets=["WBTC"]))
        closing_intents = [SimpleNamespace(from_token="ARB", to_token=None, token=None, asset=None)]
        universe = derive_strategy_token_universe(sm, "dep-1", strategy, closing_intents, self._positions())
        assert universe == {"ARB", "WETH", "USDC", "USDT", "WBTC"}

    def test_no_state_manager_still_derives_from_intents_and_positions(self):
        strategy = SimpleNamespace(get_teardown_profile=lambda: SimpleNamespace(natural_exit_assets=[]))
        universe = derive_strategy_token_universe(None, "dep-1", strategy, [], self._positions())
        assert universe == {"WETH", "USDC"}

    def test_event_read_failure_shrinks_universe_never_raises(self):
        def _boom(deployment_id):
            raise RuntimeError("db locked")

        sm = SimpleNamespace(get_accounting_events_sync=_boom)
        strategy = SimpleNamespace(get_teardown_profile=lambda: SimpleNamespace(natural_exit_assets=[]))
        universe = derive_strategy_token_universe(sm, "dep-1", strategy, [], self._positions())
        assert universe == {"WETH", "USDC"}

    def test_dict_intents_supported(self):
        strategy = SimpleNamespace(get_teardown_profile=lambda: SimpleNamespace(natural_exit_assets=[]))
        universe = derive_strategy_token_universe(
            None, "dep-1", strategy, [{"from_token": "weth", "to_token": "usdc"}], None
        )
        # Original casing preserved (Codex audit) — folding happens only at
        # comparison time inside plan_consolidation.
        assert universe == {"weth", "usdc"}


class TestFoldOutcome:
    def _result(self):
        return TeardownResult(
            success=True,
            deployment_id="dep-1",
            mode="graceful",
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
            duration_seconds=1.0,
            intents_total=1,
            intents_succeeded=1,
            intents_failed=0,
            starting_value_usd=Decimal("100"),
            final_value_usd=Decimal("99"),
            total_costs_usd=Decimal("1"),
            final_balances={},
        )

    def test_consolidation_failure_keeps_success_true(self):
        outcome = ConsolidationOutcome(planned=1, succeeded=0, failed=1, warnings=["swap failed"])
        folded = fold_consolidation_outcome(self._result(), outcome)
        assert folded.success is True
        assert folded.consolidation_planned == 1
        assert folded.consolidation_failed == 1
        assert folded.consolidation_warnings == ["swap failed"]

    def test_degraded_accounting_propagates(self):
        outcome = ConsolidationOutcome(planned=1, succeeded=1, failed=0, accounting_degraded_count=2)
        folded = fold_consolidation_outcome(self._result(), outcome)
        assert folded.accounting_degraded is True
        assert folded.accounting_degraded_count == 2


class MemoMarket(FakeMarket):
    """Snapshot double with a memoized balance layer mirroring MarketSnapshot:
    ``balance()`` serves the stale memo until ``invalidate_balance`` evicts
    it, after which reads hit the live provider truth (the constructor's
    ``balances``)."""

    def __init__(
        self,
        memo: dict[str, Decimal],
        live: dict[str, Decimal],
        prices: dict[str, Decimal],
        *,
        invalidate_raises: bool = False,
    ):
        super().__init__(balances=live, prices=prices)
        self._memo = dict(memo)
        self._invalidate_raises = invalidate_raises
        self.invalidate_calls: list[str] = []

    def balance(self, token: str, chain: str | None = None):
        if token in self._memo:
            self.balance_calls.append(token)
            return SimpleNamespace(balance=self._memo[token])
        return super().balance(token, chain=chain)

    def invalidate_balance(self, token: str, protocol: str | None = None) -> None:  # noqa: ARG002
        self.invalidate_calls.append(token)
        if self._invalidate_raises:
            raise RuntimeError("eviction failed")
        self._memo.pop(token, None)


class TestStaleBalanceMemoVIB5074:
    """VIB-5074: the decision pass runs AFTER closing intents executed against
    the SAME market snapshot, whose ``balance()`` memoizes. Field incident
    (deployment:78fc633158d7, Base, td_7f3c209d7b6a): the closing swap sold
    0.002136918204775968 WETH at 23:56:13Z, then the decision pass logged
    ``skip WETH (reason=below_dust, value_usd=3.575...)`` from the memoized
    pre-swap balance. The planner must evict the per-token memo before
    reading — the same discipline as the execution lane's zero-balance skip."""

    FIELD_WETH = Decimal("0.002136918204775968")

    def test_field_repro_stale_weth_memo_decides_zero_balance_after_eviction(self):
        market = MemoMarket(
            memo={"WETH": self.FIELD_WETH},  # stale: pre-closure
            live={"WETH": Decimal("0")},  # truth: closing swap sold it all
            prices={"WETH": Decimal("1673.10")},
        )
        plan = _plan(market=market, universe={"WETH"})

        assert plan.intents == []
        weth = _decision(plan, "WETH")
        assert weth.action == "skip"
        # Without eviction this is the field log line: below_dust at the
        # stale ~$3.58 valuation of WETH that no longer exists.
        assert weth.reason == "zero_balance"
        assert weth.balance == Decimal("0")
        assert weth.value_usd == Decimal("0")
        # The memo was evicted before the read.
        assert market.invalidate_calls == ["WETH"]

    def test_stale_zero_memo_does_not_strand_live_residual(self):
        """The money-losing direction (the VIB-5011 $18-WETH mechanism): a
        stale zero memo while the wallet really holds a material residual
        must still plan the consolidation swap."""
        market = MemoMarket(
            memo={"WETH": Decimal("0")},  # stale: pre-closure
            live={"WETH": Decimal("0.011")},  # truth: LP_CLOSE returned WETH
            prices={"WETH": Decimal("1650")},
        )
        plan = _plan(market=market, universe={"WETH"})

        assert _swap_tokens(plan) == [("WETH", "USDC")]
        weth = _decision(plan, "WETH")
        assert weth.action == "swap"
        assert weth.value_usd == Decimal("0.011") * Decimal("1650")

    def test_invalidate_failure_fails_closed_never_swaps(self):
        """VIB-5196: when invalidate_balance() RAISES, the planner can no
        longer trust the cached balance, so it must fail CLOSED — skip the
        token (balance_unavailable) and NEVER decide a swap off the
        possibly-stale memo. The dangerous direction: the closure already sold
        the token (live 0), but the eviction that would reveal that failed, so
        the memo still serves a stale-positive balance worth well above the
        dust floor. Emitting a swap there is a real money-path action for a
        token the wallet no longer holds; skipping only strands recoverable
        dust. This is the residual VIB-5074 made loud but left reachable."""
        market = MemoMarket(
            memo={"WETH": Decimal("0.011")},  # stale-positive: pre-closure (~$18)
            live={"WETH": Decimal("0")},  # truth: the closing swap sold it all
            prices={"WETH": Decimal("1650")},
            invalidate_raises=True,  # eviction fails → live truth unreachable
        )
        plan = _plan(market=market, universe={"WETH"})

        # Eviction was attempted, but on failure we skip BEFORE reading the
        # untrusted balance — fail closed, no swap even though the stale value
        # (~$18.15) is far above the $5 dust floor.
        assert market.invalidate_calls == ["WETH"]
        assert market.balance_calls == []
        assert _swap_tokens(plan) == []
        weth = _decision(plan, "WETH")
        assert weth.action == "skip"
        assert weth.reason == "balance_unavailable"
        # Loud-but-non-blocking: the failure names the token in the audit trail.
        failclosed = [w for w in plan.warnings if "WETH" in w and "invalidate_balance failed" in w]
        assert failclosed, f"expected a loud fail-closed warning, got {plan.warnings}"

    def test_multi_token_universe_evicts_every_token_memo(self):
        """Eviction is per-token across the WHOLE universe — not special-cased
        to WETH/wrapped-native (Phase 1 spec critique round 2). Three
        non-WETH tokens, including a mixed-case canonical symbol, each with a
        stale memo in one of both directions: every memo is evicted and every
        decision is made from the LIVE balance."""
        market = MemoMarket(
            memo={
                "DAI": Decimal("100"),  # stale positive — closure sold it
                "USDC.e": Decimal("0"),  # stale zero — closure returned it
                "ARB": Decimal("3"),  # stale undervalue — live is larger
            },
            live={
                "DAI": Decimal("0"),
                "USDC.e": Decimal("25"),
                "ARB": Decimal("50"),
            },
            prices={"DAI": Decimal("1"), "USDC.e": Decimal("1"), "ARB": Decimal("0.40")},
        )
        plan = _plan(market=market, universe={"DAI", "USDC.e", "ARB"})

        # Every universe token's memo was evicted (original casing preserved).
        assert sorted(market.invalidate_calls) == ["ARB", "DAI", "USDC.e"]
        # Sold token: measured zero from live truth, never the stale $100.
        dai = _decision(plan, "DAI")
        assert (dai.action, dai.reason, dai.value_usd) == ("skip", "zero_balance", Decimal("0"))
        # Returned token: stale zero must not strand the live $25 residual.
        usdce = _decision(plan, "USDC.e")
        assert usdce.action == "swap"
        assert usdce.value_usd == Decimal("25")
        # Re-valued token: decision priced from live 50, not stale 3.
        arb = _decision(plan, "ARB")
        assert arb.action == "swap"
        assert arb.value_usd == Decimal("50") * Decimal("0.40")
        assert sorted(_swap_tokens(plan)) == [("ARB", "USDC"), ("USDC.e", "USDC")]

    def test_market_without_invalidate_still_plans(self):
        """Provider-less / legacy snapshots without invalidate_balance keep
        working — the eviction is capability-gated."""
        market = FakeMarket(
            balances={"WETH": Decimal("0.011")},
            prices={"WETH": Decimal("1650")},
        )
        plan = _plan(market=market, universe={"WETH"})
        assert _swap_tokens(plan) == [("WETH", "USDC")]

    def test_unparseable_balance_is_unmeasured_not_zero(self):
        """Empty ≠ Zero: a read that cannot be coerced to Decimal is
        UNMEASURED → balance_unavailable (value_usd None), never a zero
        balance and never below_dust."""
        market = FakeMarket(balances={"WETH": "not-a-number"}, prices={"WETH": Decimal("1650")})  # type: ignore[dict-item]
        plan = _plan(market=market, universe={"WETH"})

        assert plan.intents == []
        weth = _decision(plan, "WETH")
        assert weth.reason == "balance_unavailable"
        assert weth.value_usd is None
        assert any("WETH" in w for w in plan.warnings)


class TestCanonicalCasing:
    """Codex audit (VIB-5011): canonical registry symbols can be mixed-case
    (``USDC.e``, ``WETH.e``). Upper-casing them before ``market.balance()``
    or ``Intent.swap(from_token=...)`` breaks lookups and silently skips the
    consolidation it was supposed to perform. Comparisons fold to upper;
    market reads and intents keep the ORIGINAL symbol."""

    def test_mixed_case_symbol_consolidates_with_original_casing(self):
        market = FakeMarket(
            balances={"USDC.e": Decimal("42")},
            prices={"USDC.e": Decimal("1")},
        )
        plan = _plan(market=market, universe={"USDC.e"})

        assert len(plan.intents) == 1
        assert plan.intents[0].from_token == "USDC.e"  # NOT "USDC.E"
        assert plan.intents[0].to_token == "USDC"
        # The balance read used the canonical symbol exactly as given.
        assert market.balance_calls == ["USDC.e"]
        swap_decisions = [d for d in plan.decisions if d.action == "swap"]
        assert [d.token for d in swap_decisions] == ["USDC.e"]

    def test_target_membership_is_case_insensitive(self):
        market = FakeMarket(balances={}, prices={})
        plan = _plan(market=market, universe={"usdc"}, target_token="USDC")

        assert plan.intents == []
        assert [d.reason for d in plan.decisions] == ["target"]

    def test_not_in_universe_audit_is_case_insensitive(self):
        market = FakeMarket(
            balances={"WETH": Decimal("1")},
            prices={"WETH": Decimal("1650")},
        )
        plan = _plan(market=market, universe={"WETH"}, wallet_tokens={"weth"})

        # "weth" IS the universe token (case-insensitively) — it must not be
        # recorded as a shared-wallet out-of-universe exclusion.
        assert not any(d.reason == "not_in_universe" for d in plan.decisions)
