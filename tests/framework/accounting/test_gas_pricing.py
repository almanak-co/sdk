"""Unit tests for ``transaction_ledger.gas_usd`` population (April 30 audit #3).

The bug
-------
``transaction_ledger.gas_usd`` was always ``""`` for SWAP and LP intents.
The runner had ``total_gas_cost_wei`` and the per-cycle ``price_oracle``
in scope, but no writer multiplied them through to USD.  The lending lane
did this correctly via ``lending_accounting._amount_to_usd``; everything
else dropped the conversion on the floor.

What this test pins
-------------------
1. The shared helper ``accounting.gas_pricing.compute_gas_usd`` returns
   the correct Decimal when given a valid wei figure, chain, and price
   oracle — and ``None`` (not 0!) when the oracle is missing or cannot
   resolve the native-token price.
2. ``observability.ledger.build_ledger_entry`` populates
   ``LedgerEntry.gas_usd`` from ``total_gas_cost_wei × native_usd`` when
   ``price_oracle`` is supplied — closing the gap that the audit flagged.
3. ``accounting.sidecar.AccountingSidecarWriter`` populates the sidecar's
   ``gas_usd`` field via the same helper.
4. The chain → native-token map covers every chain currently supported by
   the gateway (the parallel map in ``gateway.data.balance.web3_provider``).

We deliberately use Decimal throughout — float would lose precision below
the 7-significant-digit floor of double, and accumulated gas figures on
long-running strategies would drift visibly.
"""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

from almanak.framework.accounting.gas_pricing import (
    _CHAIN_NATIVE_TOKEN,
    compute_gas_usd,
    native_token_for_chain,
)
from almanak.framework.accounting.sidecar import AccountingSidecarWriter
from almanak.framework.observability.ledger import build_ledger_entry

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _make_intent(intent_type: str = "SWAP", protocol: str = "uniswap_v3") -> Any:
    return SimpleNamespace(
        intent_type=SimpleNamespace(value=intent_type),
        protocol=protocol,
        from_token="USDC",
        to_token="WETH",
    )


def _make_swap_amounts(
    amount_in_decimal: Decimal = Decimal("100"),
    amount_out_decimal: Decimal = Decimal("0.04"),
) -> Any:
    return SimpleNamespace(
        token_in="USDC",
        token_out="WETH",
        amount_in_decimal=amount_in_decimal,
        amount_out_decimal=amount_out_decimal,
        amount_in_decimal_resolved=True,
        amount_out_decimal_resolved=True,
        effective_price=Decimal("2500"),
        slippage_bps=5,
    )


def _make_result(
    *,
    gas_used: int = 150_000,
    gas_price_wei: int = 50_000_000_000,  # 50 gwei
    tx_hash: str = "0xdeadbeef",
    gas_cost_usd: Any = None,  # use a sentinel: any non-None preempts the helper
) -> Any:
    """Build a minimal ExecutionResult-shaped namespace.

    The conversion is ``gas_used × gas_price`` for ``total_gas_cost_wei``
    so the test arithmetic mirrors the real orchestrator: 150k × 50 gwei =
    7.5e15 wei = 0.0075 ETH, and at $3000/ETH that's $22.50.
    """
    total_gas_cost_wei = gas_used * gas_price_wei
    ns_kwargs = {
        "transaction_results": [SimpleNamespace(tx_hash=tx_hash, gas_used=gas_used, success=True)],
        "total_gas_used": gas_used,
        "total_gas_cost_wei": total_gas_cost_wei,
        "swap_amounts": _make_swap_amounts(),
        "extracted_data": {},
    }
    # Only set gas_cost_usd when explicitly provided — that way the helper
    # path (the new code) is exercised by default, and the legacy
    # pre-computed path is tested with an explicit override.
    if gas_cost_usd is not None:
        ns_kwargs["gas_cost_usd"] = gas_cost_usd
    return SimpleNamespace(**ns_kwargs)


# ---------------------------------------------------------------------------
# compute_gas_usd — pure-arithmetic contract
# ---------------------------------------------------------------------------


class TestComputeGasUsd:
    def test_known_gas_and_eth_price_yields_exact_decimal(self):
        """The canonical case: 21k gas × 50 gwei × $3000/ETH = $3.15."""
        gas_used = 21_000
        gas_price_wei = 50_000_000_000  # 50 gwei
        gas_cost_wei = gas_used * gas_price_wei

        result = compute_gas_usd(
            gas_cost_wei=gas_cost_wei,
            chain="ethereum",
            price_oracle={"ETH": "3000"},
        )

        # Hand-calculated: 21000 * 50e9 / 1e18 * 3000 = 0.00105 * 3000 = 3.15
        expected = Decimal(gas_used) * Decimal(gas_price_wei) / Decimal(10**18) * Decimal("3000")
        assert result == expected
        assert result == Decimal("3.15000")

    def test_decimal_precision_preserved_at_subdollar_scale(self):
        """L2 gas costs are pennies — float would drop the trailing digits."""
        gas_used = 200_000
        gas_price_wei = 100_000_000  # 0.1 gwei (Arbitrum-ish)
        result = compute_gas_usd(
            gas_cost_wei=gas_used * gas_price_wei,
            chain="arbitrum",
            price_oracle={"ETH": "3000"},
        )
        # 200k * 1e8 / 1e18 * 3000 = 6e-5 ETH × $3000 = $0.06
        expected = Decimal("200000") * Decimal("100000000") / Decimal(10**18) * Decimal("3000")
        assert result == expected
        # Sanity: the value is non-trivially small but precisely representable.
        assert result == Decimal("0.06000")

    def test_missing_price_oracle_returns_none_not_zero(self):
        """Distinguishes 'unknown' (None) from 'measured zero' (Decimal(0))."""
        result = compute_gas_usd(
            gas_cost_wei=21_000 * 50_000_000_000,
            chain="ethereum",
            price_oracle=None,
        )
        assert result is None

    def test_oracle_missing_native_symbol_returns_none(self):
        """Oracle present but doesn't know ETH → None, not 0."""
        result = compute_gas_usd(
            gas_cost_wei=21_000 * 50_000_000_000,
            chain="ethereum",
            price_oracle={"USDC": "1"},
        )
        assert result is None

    def test_zero_gas_returns_zero_not_none(self):
        """Measured-zero gas (e.g. dry-run) is distinguishable from missing."""
        result = compute_gas_usd(
            gas_cost_wei=0,
            chain="ethereum",
            price_oracle={"ETH": "3000"},
        )
        assert result == Decimal("0")

    def test_none_gas_returns_none(self):
        """``None`` wei means the runner didn't measure it — stay None."""
        result = compute_gas_usd(
            gas_cost_wei=None,
            chain="ethereum",
            price_oracle={"ETH": "3000"},
        )
        assert result is None

    def test_negative_gas_returns_none(self):
        """Defensive: a negative wei figure is structurally impossible."""
        result = compute_gas_usd(
            gas_cost_wei=-1,
            chain="ethereum",
            price_oracle={"ETH": "3000"},
        )
        assert result is None

    def test_zero_or_negative_price_returns_none(self):
        """A ≤0 native-token price is never legitimate — treat as unavailable."""
        zero = compute_gas_usd(
            gas_cost_wei=21_000 * 50_000_000_000,
            chain="ethereum",
            price_oracle={"ETH": "0"},
        )
        negative = compute_gas_usd(
            gas_cost_wei=21_000 * 50_000_000_000,
            chain="ethereum",
            price_oracle={"ETH": "-100"},
        )
        assert zero is None
        assert negative is None

    def test_chain_lookup_is_case_insensitive(self):
        """Strategy configs vary in chain casing — both shapes must resolve."""
        upper = compute_gas_usd(
            gas_cost_wei=21_000 * 50_000_000_000,
            chain="ETHEREUM",
            price_oracle={"ETH": "3000"},
        )
        lower = compute_gas_usd(
            gas_cost_wei=21_000 * 50_000_000_000,
            chain="ethereum",
            price_oracle={"ETH": "3000"},
        )
        assert upper == lower
        assert upper is not None

    def test_unknown_chain_falls_back_to_eth(self):
        """An unsupported chain name uses ETH as default — matches gateway behaviour."""
        result = compute_gas_usd(
            gas_cost_wei=21_000 * 50_000_000_000,
            chain="hypothetical_chain",
            price_oracle={"ETH": "3000"},
        )
        assert result is not None
        assert result == Decimal("3.15000")

    def test_oracle_lookup_falls_back_through_case_variants(self):
        """Lookup tries upper / exact / lower in that order."""
        # Stored uppercase (canonical)
        upper = compute_gas_usd(
            gas_cost_wei=21_000 * 50_000_000_000,
            chain="ethereum",
            price_oracle={"ETH": "3000"},
        )
        # Stored lowercase
        lower = compute_gas_usd(
            gas_cost_wei=21_000 * 50_000_000_000,
            chain="ethereum",
            price_oracle={"eth": "3000"},
        )
        assert upper == lower


class TestNativeTokenForChain:
    def test_returns_eth_for_arbitrum(self):
        assert native_token_for_chain("arbitrum") == "ETH"

    def test_returns_mnt_for_mantle(self):
        assert native_token_for_chain("mantle") == "MNT"

    def test_returns_avax_for_avalanche(self):
        assert native_token_for_chain("avalanche") == "AVAX"

    def test_returns_sol_for_solana(self):
        assert native_token_for_chain("solana") == "SOL"

    def test_unknown_chain_defaults_to_eth(self):
        assert native_token_for_chain("does_not_exist") == "ETH"

    def test_empty_chain_defaults_to_eth(self):
        assert native_token_for_chain("") == "ETH"

    def test_bnb_alias_resolves_to_bnb_native_token(self):
        """Chain aliases (``bnb`` -> ``bsc``) must produce the right native token.

        BSC strategies and connectors commonly pass ``chain='bnb'``; without
        alias normalization, ``native_token_for_chain`` would silently fall
        back to ETH and ``compute_gas_usd`` would either miss the BNB price
        (returning None) or pick up an unrelated ETH price. Either way, BSC
        gas costs would be misreported.
        """
        assert native_token_for_chain("bnb") == "BNB"

    def test_bsc_canonical_resolves_to_bnb(self):
        """Sanity check: the canonical ``bsc`` form still works."""
        assert native_token_for_chain("bsc") == "BNB"

    def test_other_aliases_resolve(self):
        """A few other common aliases — all routed through resolve_chain_name."""
        assert native_token_for_chain("eth") == "ETH"
        assert native_token_for_chain("avax") == "AVAX"
        assert native_token_for_chain("matic") == "MATIC"
        assert native_token_for_chain("arb") == "ETH"
        assert native_token_for_chain("op") == "ETH"


class TestSolanaLamportsHandling:
    """Solana fees are in lamports (10**9) on a separate field, not wei.

    ``SolanaExecutionResult`` always leaves ``total_gas_cost_wei = 0`` and
    stores the real fee in ``fee_lamports``. Pre-fix, ``compute_gas_usd``
    would short-circuit ``gas_cost_wei == 0`` to ``Decimal("0")`` and the
    ledger would record ``gas_usd="0"`` for every Solana intent — silently
    erasing gas drag from PnL totals.
    """

    def test_zero_wei_on_solana_returns_none_not_zero(self):
        """Lamport-denominated chains return None for 0 wei (unknown, not measured zero)."""
        result = compute_gas_usd(
            gas_cost_wei=0,
            chain="solana",
            price_oracle={"SOL": "150"},
        )
        assert result is None, (
            "Solana fees are stored in fee_lamports, not total_gas_cost_wei. "
            "Returning Decimal('0') for a 0-wei input on Solana would silently "
            "record gas_usd='0' for every intent and corrupt PnL."
        )

    def test_zero_wei_on_solana_alias_returns_none(self):
        """The ``sol`` alias must also avoid the measured-zero trap."""
        result = compute_gas_usd(
            gas_cost_wei=0,
            chain="sol",
            price_oracle={"SOL": "150"},
        )
        assert result is None

    def test_zero_wei_on_evm_still_returns_decimal_zero(self):
        """EVM chains keep the measured-zero contract (e.g. dry-run)."""
        for chain in ("ethereum", "arbitrum", "polygon", "bsc", "bnb"):
            result = compute_gas_usd(
                gas_cost_wei=0,
                chain=chain,
                price_oracle={"ETH": "3000", "MATIC": "0.5", "BNB": "600"},
            )
            assert result == Decimal("0"), (
                f"chain={chain!r}: zero wei must remain a measured zero on EVM, not None"
            )

    def test_non_zero_wei_on_solana_returns_none_not_wrong_decimal(self):
        """Even if a future path pipes a non-zero value through, Solana
        returns None — dividing lamports by 10**18 would silently
        underreport gas drag by ~10**9. Pinned per Gemini Code Assist
        feedback on PR #1978.
        """
        # 5000 lamports = a typical Solana tx fee. If the helper divided by
        # 10**18 it would produce ~7.5e-13 USD — a silent corruption. It
        # should return None instead.
        result = compute_gas_usd(
            gas_cost_wei=5000,
            chain="solana",
            price_oracle={"SOL": "150"},
        )
        assert result is None


class TestChainNativeTokenCoverage:
    """The map must cover every chain the gateway currently supports.

    If a chain ships in the gateway's NATIVE_TOKEN_SYMBOLS without a matching
    entry here, gas_usd silently falls back to ETH — which is wrong for AVAX,
    MATIC, BNB, MNT, SOL etc. Pin the coverage so a divergence trips this
    test instead of producing wrong USD figures in production.
    """

    def test_all_evm_chains_in_runner_chain_map_have_native_token(self):
        """Sanity: every chain the strategy_runner sets up must resolve."""
        # Use the gateway's NATIVE_TOKEN_SYMBOLS as the canonical set —
        # every chain there is also supported in framework code paths.
        from almanak.gateway.data.balance.web3_provider import NATIVE_TOKEN_SYMBOLS

        for chain in NATIVE_TOKEN_SYMBOLS:
            assert chain in _CHAIN_NATIVE_TOKEN, (
                f"chain {chain!r} is in gateway NATIVE_TOKEN_SYMBOLS but missing from "
                "accounting.gas_pricing._CHAIN_NATIVE_TOKEN — gas_usd will silently "
                "default to ETH for that chain"
            )
            assert _CHAIN_NATIVE_TOKEN[chain] == NATIVE_TOKEN_SYMBOLS[chain], (
                f"chain {chain!r} has divergent native tokens: framework="
                f"{_CHAIN_NATIVE_TOKEN[chain]!r}, gateway={NATIVE_TOKEN_SYMBOLS[chain]!r}"
            )

    def test_solana_is_covered(self):
        """Solana is non-EVM but still produces a gas figure in SOL."""
        assert _CHAIN_NATIVE_TOKEN.get("solana") == "SOL"

    def test_plasma_resolves_to_xpl_through_lending_writer(self):
        """VIB-3805 regression pin: prior to the consolidation,
        ``lending_accounting`` carried its own map with plasma="ETH" while
        the gas_pricing helper said plasma="XPL". After dropping the local
        map, the lending writer must resolve plasma via
        ``native_token_for_chain`` — which returns "XPL". A future commit
        that reverts this consolidation (or typos plasma's symbol back to
        "ETH") trips this test instead of silently mispricing plasma gas.
        """
        from almanak.framework.accounting import lending_accounting
        from almanak.framework.accounting.gas_pricing import native_token_for_chain

        assert native_token_for_chain("plasma") == "XPL"
        # Hard-pin: lending writer MUST go through the framework SSOT, not
        # a local map. The presence of any module-level _CHAIN_NATIVE_TOKEN
        # in lending_accounting would mean somebody re-introduced the
        # divergent map.
        assert not hasattr(lending_accounting, "_CHAIN_NATIVE_TOKEN"), (
            "lending_accounting reintroduced a local _CHAIN_NATIVE_TOKEN map — "
            "this is the divergence VIB-3805 was filed to fix. The writer "
            "must call native_token_for_chain() instead."
        )


# ---------------------------------------------------------------------------
# build_ledger_entry — populates LedgerEntry.gas_usd via the helper
# ---------------------------------------------------------------------------


class TestBuildLedgerEntryPopulatesGasUsd:
    """The headline fix: SWAP / LP intents now ship gas_usd populated.

    Each test runs an actual ``build_ledger_entry`` call with a stubbed
    intent + result and asserts ``entry.gas_usd`` matches
    ``gas_used × gas_price × native_usd / 1e18`` to Decimal precision.
    """

    def test_swap_with_price_oracle_populates_gas_usd(self):
        """The canonical SWAP path that was always empty before the fix."""
        result = _make_result(gas_used=150_000, gas_price_wei=50_000_000_000)
        intent = _make_intent("SWAP", protocol="uniswap_v3")

        entry = build_ledger_entry(
            strategy_id="strat_test",
            cycle_id="cycle_test",
            intent=intent,
            result=result,
            chain="arbitrum",
            price_oracle={"ETH": "3000", "USDC": "1"},
        )

        # Reproduce the exact arithmetic the helper performs — the test is
        # the spec, and the implementation must match it byte-for-byte.
        gas_used = 150_000
        gas_price_wei = 50_000_000_000
        native_usd = Decimal("3000")
        expected_gas_usd = (
            Decimal(gas_used) * Decimal(gas_price_wei) / Decimal(10**18) * native_usd
        )
        assert entry.gas_usd != ""  # pre-fix this would be ""
        assert Decimal(entry.gas_usd) == expected_gas_usd
        assert Decimal(entry.gas_usd) == Decimal("22.500000")

    def test_lp_open_with_price_oracle_populates_gas_usd(self):
        """LP_OPEN was the second category called out in the audit."""
        result = _make_result(gas_used=400_000, gas_price_wei=10_000_000_000)
        intent = _make_intent("LP_OPEN", protocol="uniswap_v3")

        entry = build_ledger_entry(
            strategy_id="strat_test",
            cycle_id="cycle_test",
            intent=intent,
            result=result,
            chain="ethereum",
            price_oracle={"ETH": "3000"},
        )

        expected_gas_usd = (
            Decimal("400000") * Decimal("10000000000") / Decimal(10**18) * Decimal("3000")
        )
        assert entry.gas_usd != ""
        assert Decimal(entry.gas_usd) == expected_gas_usd

    def test_lp_close_with_price_oracle_populates_gas_usd(self):
        result = _make_result(gas_used=250_000, gas_price_wei=20_000_000_000)
        intent = _make_intent("LP_CLOSE", protocol="aerodrome")

        entry = build_ledger_entry(
            strategy_id="strat_test",
            cycle_id="cycle_test",
            intent=intent,
            result=result,
            chain="base",
            price_oracle={"ETH": "3000"},
        )

        expected = Decimal("250000") * Decimal("20000000000") / Decimal(10**18) * Decimal("3000")
        assert Decimal(entry.gas_usd) == expected

    def test_avalanche_uses_avax_price_not_eth(self):
        """A regression guard: a wrong chain map silently uses the wrong token."""
        result = _make_result(gas_used=100_000, gas_price_wei=25_000_000_000)
        intent = _make_intent("SWAP", protocol="traderjoe_v2")

        entry = build_ledger_entry(
            strategy_id="strat_test",
            cycle_id="cycle_test",
            intent=intent,
            result=result,
            chain="avalanche",
            # Both ETH and AVAX present — the writer must pick AVAX for avalanche.
            price_oracle={"ETH": "3000", "AVAX": "40"},
        )

        expected = Decimal("100000") * Decimal("25000000000") / Decimal(10**18) * Decimal("40")
        assert Decimal(entry.gas_usd) == expected
        # And NOT the ETH-priced figure — that would silently overstate gas
        # 75x on Avalanche.
        wrong = Decimal("100000") * Decimal("25000000000") / Decimal(10**18) * Decimal("3000")
        assert Decimal(entry.gas_usd) != wrong

    def test_no_price_oracle_yields_empty_gas_usd(self):
        """Backward-compat: callers that don't pass price_oracle still work."""
        result = _make_result()
        intent = _make_intent("SWAP")

        entry = build_ledger_entry(
            strategy_id="strat_test",
            cycle_id="cycle_test",
            intent=intent,
            result=result,
            chain="arbitrum",
            price_oracle=None,
        )

        # With no oracle we cannot convert — ledger column stays "".
        assert entry.gas_usd == ""

    def test_oracle_missing_native_symbol_logs_warning_and_yields_empty(self, caplog):
        """Operator must see ONE warning per missing-price ledger row."""
        result = _make_result()
        intent = _make_intent("SWAP")

        with caplog.at_level("WARNING", logger="almanak.framework.observability.ledger"):
            entry = build_ledger_entry(
                strategy_id="strat_oracle_gap",
                cycle_id="cycle_test",
                intent=intent,
                result=result,
                chain="arbitrum",
                price_oracle={"USDC": "1"},  # ETH absent
            )

        assert entry.gas_usd == ""
        # Exactly one warning, mentioning the chain and the missing symbol
        # — not silent, not spammy.
        warnings = [r for r in caplog.records if r.levelname == "WARNING"]
        assert len(warnings) == 1
        assert "arbitrum" in warnings[0].getMessage()
        assert "ETH" in warnings[0].getMessage()
        assert "strat_oracle_gap" in warnings[0].getMessage()

    def test_legacy_precomputed_gas_cost_usd_is_honoured(self):
        """ResultEnricher's prediction-handler path already did the conversion;
        the ledger writer must not double-compute or override it.
        """
        result = _make_result(gas_cost_usd=Decimal("0.42"))
        intent = _make_intent("PREDICTION_BUY", protocol="polymarket")

        entry = build_ledger_entry(
            strategy_id="strat_test",
            cycle_id="cycle_test",
            intent=intent,
            result=result,
            chain="polygon",
            # Even with a contradictory oracle the pre-computed value wins.
            price_oracle={"MATIC": "100"},
        )

        assert entry.gas_usd == "0.42"

    def test_zero_gas_cost_wei_records_zero_gas_usd(self):
        """A measured-zero gas (dry-run / skipped tx) records as "0", not ""."""
        result = _make_result(gas_used=0, gas_price_wei=0)
        intent = _make_intent("SWAP")

        entry = build_ledger_entry(
            strategy_id="strat_test",
            cycle_id="cycle_test",
            intent=intent,
            result=result,
            chain="ethereum",
            price_oracle={"ETH": "3000"},
        )

        # total_gas_cost_wei == 0 → helper short-circuits to Decimal("0")
        # → "0" — preserves the measured-zero contract.
        # NOTE: with 0 wei, the warning path doesn't fire because the
        # `total_gas_cost_wei` check in build_ledger_entry guards on truthiness.
        # But the returned gas_usd should still be a valid stringified Decimal.
        # When 0 wei: compute_gas_usd returns Decimal(0), str() = "0".
        # When the helper returns None (missing oracle), gas_usd is "".
        # Here oracle is present → "0".
        assert entry.gas_usd == "0"


# ---------------------------------------------------------------------------
# AccountingSidecarWriter — sidecar gas_usd matches the ledger
# ---------------------------------------------------------------------------


class TestSidecarPopulatesGasUsd:
    def test_sidecar_writes_gas_usd_when_oracle_supplied(self, tmp_path: Path):
        writer = AccountingSidecarWriter()
        result = _make_result(gas_used=150_000, gas_price_wei=50_000_000_000)

        with patch(
            "almanak.framework.accounting.sidecar._sidecar_dir", return_value=tmp_path
        ):
            writer.append(
                strategy_id="strat_sidecar",
                intent=_make_intent("SWAP"),
                result=result,
                chain="arbitrum",
                price_oracle={"ETH": "3000"},
            )

        line = json.loads((tmp_path / "strat_sidecar.jsonl").read_text().strip())
        expected = Decimal("150000") * Decimal("50000000000") / Decimal(10**18) * Decimal("3000")
        assert line["gas_usd"] is not None
        assert Decimal(line["gas_usd"]) == expected

    def test_sidecar_gas_usd_null_when_oracle_missing(self, tmp_path: Path):
        """Backward-compat: callers that don't pass price_oracle keep null."""
        writer = AccountingSidecarWriter()
        result = _make_result()

        with patch(
            "almanak.framework.accounting.sidecar._sidecar_dir", return_value=tmp_path
        ):
            writer.append(
                strategy_id="strat_sidecar_no_oracle",
                intent=_make_intent("SWAP"),
                result=result,
                chain="arbitrum",
                # No price_oracle — old call sites still work.
            )

        line = json.loads(
            (tmp_path / "strat_sidecar_no_oracle.jsonl").read_text().strip()
        )
        assert line["gas_usd"] is None

    def test_sidecar_honours_precomputed_gas_cost_usd(self, tmp_path: Path):
        """Symmetric with the ledger: pre-computed values pass through."""
        writer = AccountingSidecarWriter()
        result = _make_result(gas_cost_usd=Decimal("0.99"))

        with patch(
            "almanak.framework.accounting.sidecar._sidecar_dir", return_value=tmp_path
        ):
            writer.append(
                strategy_id="strat_sidecar_precomputed",
                intent=_make_intent("PREDICTION_BUY", protocol="polymarket"),
                result=result,
                chain="polygon",
                price_oracle={"MATIC": "100"},
            )

        line = json.loads(
            (tmp_path / "strat_sidecar_precomputed.jsonl").read_text().strip()
        )
        assert line["gas_usd"] == "0.99"

    def test_sidecar_gas_usd_zero_for_measured_zero_gas(self, tmp_path: Path):
        """Distinguish measured-zero gas from missing oracle in the sidecar too."""
        writer = AccountingSidecarWriter()
        result = _make_result(gas_used=0, gas_price_wei=0)

        with patch(
            "almanak.framework.accounting.sidecar._sidecar_dir", return_value=tmp_path
        ):
            writer.append(
                strategy_id="strat_sidecar_zero",
                intent=_make_intent("SWAP"),
                result=result,
                chain="ethereum",
                price_oracle={"ETH": "3000"},
            )

        line = json.loads(
            (tmp_path / "strat_sidecar_zero.jsonl").read_text().strip()
        )
        # "0" preserves the "we measured zero gas" signal vs None ("we don't know").
        assert line["gas_usd"] == "0"
