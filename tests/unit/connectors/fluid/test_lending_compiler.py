"""Unit tests for the Fluid fToken lending compiler (Phase 2, VIB-5030).

Fluid fTokens are ERC-4626 vaults: supply = approve + ``deposit(assets,
receiver)``, exact withdraw = ``withdraw(assets, receiver, owner)`` behind a
``maxWithdraw`` pre-flight, and full exits route through ``redeem(shares,
receiver, owner)`` over the exact share balance so no dust is stranded.
Fluid's withdrawal limits expand over time, so a withdraw beyond the
currently-withdrawable amount must be a DISTINCT retryable failure — never a
silent clamp, never a compiled-but-doomed transaction (UAT card VIB-5030
D3.F1). These tests mock the SDK boundary (``FluidCompiler._build_sdk``) —
the real on-chain behaviour is covered by
``tests/intents/{base,arbitrum}/test_fluid_lending.py``.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from almanak.connectors._strategy_base.base.compiler import SwapCompilerContext
from almanak.connectors._strategy_base.protocol_aliases import normalize_protocol
from almanak.connectors.fluid.compiler import FluidCompiler
from almanak.connectors.fluid.sdk import FluidSDKError
from almanak.framework.intents import SupplyIntent, WithdrawIntent
from almanak.framework.intents.compiler_models import CompilationStatus, TransactionData

FTOKEN = "0xf42f5795D9ac7e9D757dB633D693cD548Cfd9169"  # base fUSDC
WALLET = "0x2222222222222222222222222222222222222222"
USDC_ADDR = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"  # base USDC

# ERC-4626 selectors (verified Phase-0, VIB-5028): deposit(uint256,address),
# withdraw(uint256,address,address), redeem(uint256,address,address)
DEPOSIT_SELECTOR = "0x6e553f65"
WITHDRAW_SELECTOR = "0xb460af94"
REDEEM_SELECTOR = "0xba087652"


def _token(
    symbol: str = "USDC", address: str = USDC_ADDR, decimals: int = 6, is_native: bool = False
) -> SimpleNamespace:
    return SimpleNamespace(
        symbol=symbol,
        address=address,
        decimals=decimals,
        is_native=is_native,
        to_dict=lambda: {"symbol": symbol, "address": address, "decimals": decimals},
    )


def _services(token: SimpleNamespace) -> MagicMock:
    services = MagicMock()
    services.resolve_token.side_effect = lambda t: token if t == token.symbol else None
    services.build_approve_tx.return_value = [
        TransactionData(
            to=token.address,
            value=0,
            data="0x095ea7b3" + "00" * 64,
            gas_estimate=46_000,
            tx_type="approve",
            description="approve",
        )
    ]
    services.format_amount.side_effect = lambda amount, decimals: str(amount)
    return services


def _ctx(services: MagicMock, **overrides) -> SwapCompilerContext:
    defaults = {
        "chain": "base",
        "wallet_address": WALLET,
        "rpc_url": "http://localhost:8545",
        "rpc_timeout": 10.0,
        "permission_discovery": False,
        "allow_placeholder_prices": True,
        "token_resolver": None,
        "gateway_client": None,
        "price_oracle": {},
        "cache": {},
        "services": services,
    }
    defaults.update(overrides)
    return SwapCompilerContext(**defaults)


def _lending_sdk(
    ftoken: str | None = FTOKEN,
    max_withdraw: int = 10**12,
    max_redeem: int = 10**12,
    share_balance: int = 75_000_000,
    converted_assets: int | None = None,
) -> MagicMock:
    sdk = MagicMock()
    sdk.find_ftoken_for_underlying.return_value = ftoken
    sdk.get_max_withdraw.return_value = max_withdraw
    sdk.get_max_redeem.return_value = max_redeem
    sdk.get_ftoken_share_balance.return_value = share_balance
    sdk.convert_to_assets.side_effect = lambda ft, shares: converted_assets if converted_assets is not None else shares
    sdk.build_deposit_tx.side_effect = lambda ft, assets, receiver: {
        "to": ft,
        "data": DEPOSIT_SELECTOR + "00" * 64,
        "value": 0,
        "gas": 400_000,
    }
    sdk.build_withdraw_tx.side_effect = lambda ft, assets, receiver, owner: {
        "to": ft,
        "data": WITHDRAW_SELECTOR + "00" * 96,
        "value": 0,
        "gas": 500_000,
    }
    sdk.build_redeem_tx.side_effect = lambda ft, shares, receiver, owner: {
        "to": ft,
        "data": REDEEM_SELECTOR + "00" * 96,
        "value": 0,
        "gas": 500_000,
    }
    return sdk


def _supply_intent(**overrides) -> SupplyIntent:
    defaults = {"protocol": "fluid", "token": "USDC", "amount": Decimal("50"), "chain": "base"}
    defaults.update(overrides)
    return SupplyIntent(**defaults)


def _withdraw_intent(**overrides) -> WithdrawIntent:
    defaults = {"protocol": "fluid", "token": "USDC", "amount": Decimal("20"), "chain": "base"}
    defaults.update(overrides)
    return WithdrawIntent(**defaults)


def _compile_supply(sdk=None, intent=None, token=None, **ctx_overrides):
    token = token or _token()
    sdk = sdk or _lending_sdk()
    with patch.object(FluidCompiler, "_build_sdk", return_value=sdk):
        result = FluidCompiler().compile_supply(_ctx(_services(token), **ctx_overrides), intent or _supply_intent())
    return result, sdk


def _compile_withdraw(sdk=None, intent=None, token=None, **ctx_overrides):
    token = token or _token()
    sdk = sdk or _lending_sdk()
    with patch.object(FluidCompiler, "_build_sdk", return_value=sdk):
        result = FluidCompiler().compile_withdraw(_ctx(_services(token), **ctx_overrides), intent or _withdraw_intent())
    return result, sdk


class TestCompileSupplySuccess:
    def test_supply_success_approve_plus_deposit(self):
        result, sdk = _compile_supply()
        assert result.status is CompilationStatus.SUCCESS, result.error
        assert [tx.tx_type for tx in result.transactions] == ["approve", "lending_supply"]
        deposit = result.transactions[-1]
        assert deposit.to == FTOKEN
        assert deposit.data.startswith(DEPOSIT_SELECTOR)
        sdk.build_deposit_tx.assert_called_once_with(FTOKEN, 50_000_000, WALLET)
        md = result.action_bundle.metadata
        assert md["protocol"] == "fluid"
        assert md["ftoken"] == FTOKEN
        # ``supply_token`` + wei ``supply_amount`` is the shape the
        # orchestrator pre-flight balance check reads (aave_helpers shape).
        assert md["supply_token"] == {"symbol": "USDC", "address": USDC_ADDR, "decimals": 6}
        assert md["supply_amount"] == "50000000"

    def test_supply_any_market_id_rejected(self):
        # Even a CORRECT fToken address is rejected: the accounting key
        # deriver inserts any intent market_id as an extra key segment, so
        # accepting it would fork one real position into two key shapes
        # (lending:{chain}:fluid:{wallet}:{asset} is the whole contract).
        result, sdk = _compile_supply(intent=_supply_intent(market_id=FTOKEN.lower()))
        assert result.status is CompilationStatus.FAILED
        assert "must omit market_id" in result.error
        assert FTOKEN in result.error  # error still names the resolved market
        assert result.transactions == []
        sdk.build_deposit_tx.assert_not_called()

    def test_supply_mismatched_market_id_fails(self):
        result, sdk = _compile_supply(intent=_supply_intent(market_id="0x1111111111111111111111111111111111111111"))
        assert result.status is CompilationStatus.FAILED
        assert "must omit market_id" in result.error
        assert result.transactions == []
        sdk.build_deposit_tx.assert_not_called()

    def test_supply_zero_base_units_fails(self):
        # 1e-7 USDC truncates to 0 base units at 6 decimals.
        result, sdk = _compile_supply(intent=_supply_intent(amount=Decimal("0.0000001")))
        assert result.status is CompilationStatus.FAILED
        assert "0 base units" in result.error
        sdk.build_deposit_tx.assert_not_called()


class TestCompileWithdrawSuccess:
    def test_withdraw_exact_amount_single_withdraw_tx_no_approve(self):
        result, sdk = _compile_withdraw()
        assert result.status is CompilationStatus.SUCCESS, result.error
        assert [tx.tx_type for tx in result.transactions] == ["lending_withdraw"]
        tx = result.transactions[0]
        assert tx.to == FTOKEN
        assert tx.data.startswith(WITHDRAW_SELECTOR)
        sdk.build_withdraw_tx.assert_called_once_with(FTOKEN, 20_000_000, WALLET, WALLET)
        md = result.action_bundle.metadata
        assert md["mode"] == "withdraw_assets"
        assert md["withdraw_token"] == {"symbol": "USDC", "address": USDC_ADDR, "decimals": 6}
        assert md["withdraw_amount"] == "20000000"

    def test_withdraw_all_full_exit_redeems_exact_share_balance(self):
        sdk = _lending_sdk(share_balance=75_000_000, max_redeem=75_000_000)
        result, sdk = _compile_withdraw(sdk=sdk, intent=_withdraw_intent(amount=Decimal("0"), withdraw_all=True))
        assert result.status is CompilationStatus.SUCCESS, result.error
        assert [tx.tx_type for tx in result.transactions] == ["lending_withdraw"]
        assert result.transactions[0].data.startswith(REDEEM_SELECTOR)
        # Shares passed to redeem == the wallet's exact share balance (no
        # assets-based rounding that could strand dust shares).
        sdk.build_redeem_tx.assert_called_once_with(FTOKEN, 75_000_000, WALLET, WALLET)
        md = result.action_bundle.metadata
        assert md["mode"] == "redeem_all_shares"
        assert md["withdraw_amount"] == "75000000"

    def test_withdraw_zero_base_units_fails(self):
        result, sdk = _compile_withdraw(intent=_withdraw_intent(amount=Decimal("0.0000001")))
        assert result.status is CompilationStatus.FAILED
        assert "0 base units" in result.error
        sdk.build_withdraw_tx.assert_not_called()

    def test_full_exit_zero_shares_fails(self):
        result, sdk = _compile_withdraw(
            sdk=_lending_sdk(share_balance=0),
            intent=_withdraw_intent(amount=Decimal("0"), withdraw_all=True),
        )
        assert result.status is CompilationStatus.FAILED
        assert "0" in result.error and "shares" in result.error
        sdk.build_redeem_tx.assert_not_called()


class TestWithdrawLimitGated:
    """D3.F1 — Fluid's time-expanding withdrawal limit is a DISTINCT retryable failure."""

    def test_withdraw_beyond_max_withdraw_is_limit_gated_retryable(self):
        # Requested 20 USDC; only 15 currently withdrawable.
        result, sdk = _compile_withdraw(sdk=_lending_sdk(max_withdraw=15_000_000))
        assert result.status is CompilationStatus.FAILED
        # (a) distinct, branchable marker; (b) retryable, time-expanding.
        assert "limit-gated" in result.error
        assert "retry later" in result.error
        # (c) names the currently-withdrawable amount (mock format_amount = str).
        assert "15000000" in result.error
        # Never compiled, never silently clamped to the available limit.
        assert result.transactions == []
        sdk.build_withdraw_tx.assert_not_called()

    def test_full_exit_max_redeem_below_shares_is_limit_gated(self):
        sdk = _lending_sdk(share_balance=100_000_000, max_redeem=40_000_000, converted_assets=39_000_000)
        result, sdk = _compile_withdraw(sdk=sdk, intent=_withdraw_intent(amount=Decimal("0"), withdraw_all=True))
        assert result.status is CompilationStatus.FAILED
        assert "limit-gated" in result.error
        assert "retry later" in result.error
        assert "39000000" in result.error  # withdrawable assets, not shares
        assert result.transactions == []
        sdk.build_redeem_tx.assert_not_called()

    def test_max_withdraw_exactly_equal_compiles(self):
        # Boundary: requested == maxWithdraw is NOT limit-gated.
        result, _ = _compile_withdraw(sdk=_lending_sdk(max_withdraw=20_000_000))
        assert result.status is CompilationStatus.SUCCESS, result.error


class TestLendingReadFailures:
    """D3.F2 — external read failures fail CLOSED (never compile blind)."""

    def test_ftoken_lookup_read_failure_fails_closed(self):
        sdk = _lending_sdk()
        sdk.find_ftoken_for_underlying.side_effect = FluidSDKError("rpc unreachable")
        result, _ = _compile_supply(sdk=sdk)
        assert result.status is CompilationStatus.FAILED
        assert "failing closed" in result.error
        assert "rpc unreachable" in result.error

    def test_max_withdraw_read_failure_fails_closed(self):
        sdk = _lending_sdk()
        sdk.get_max_withdraw.side_effect = FluidSDKError("eth_call failed")
        result, sdk = _compile_withdraw(sdk=sdk)
        assert result.status is CompilationStatus.FAILED
        assert "fail closed" in result.error
        sdk.build_withdraw_tx.assert_not_called()

    def test_full_exit_balance_read_failure_fails_closed(self):
        sdk = _lending_sdk()
        sdk.get_ftoken_share_balance.side_effect = FluidSDKError("eth_call failed")
        result, sdk = _compile_withdraw(sdk=sdk, intent=_withdraw_intent(amount=Decimal("0"), withdraw_all=True))
        assert result.status is CompilationStatus.FAILED
        assert "fail closed" in result.error
        sdk.build_redeem_tx.assert_not_called()

    def test_no_transport_supply_fails_naming_gateway_and_rpc(self):
        # Real _build_sdk path: no gateway client AND no RPC URL.
        token = _token()
        result = FluidCompiler().compile_supply(
            _ctx(_services(token), rpc_url=None, gateway_client=None), _supply_intent()
        )
        assert result.status is CompilationStatus.FAILED
        assert "gateway" in result.error.lower()
        assert "rpc" in result.error.lower()


class TestUnresolvableInputs:
    """D3.F3 — unknown token, no fToken market, unresolved amount='all'."""

    def test_unknown_token_supply_fails(self):
        result, sdk = _compile_supply(intent=_supply_intent(token="NOPE"))
        assert result.status is CompilationStatus.FAILED
        assert "Unknown token" in result.error
        sdk.build_deposit_tx.assert_not_called()

    def test_unknown_token_withdraw_fails(self):
        result, _ = _compile_withdraw(intent=_withdraw_intent(token="NOPE"))
        assert result.status is CompilationStatus.FAILED
        assert "Unknown token" in result.error

    def test_no_market_no_ftoken_fails(self):
        result, sdk = _compile_supply(sdk=_lending_sdk(ftoken=None))
        assert result.status is CompilationStatus.FAILED
        assert "no fToken" in result.error
        sdk.build_deposit_tx.assert_not_called()

    def test_supply_amount_all_unresolved_fails(self):
        result, sdk = _compile_supply(intent=_supply_intent(amount="all"))
        assert result.status is CompilationStatus.FAILED
        assert "amount='all'" in result.error
        sdk.build_deposit_tx.assert_not_called()

    def test_unsupported_chain_fails_naming_lending_chains(self):
        # SWAP ships on ethereum/polygon, but lending is scoped to the
        # Phase-0-validated chains and must fail loudly elsewhere.
        result, _ = _compile_supply(intent=_supply_intent(chain="ethereum"), chain="ethereum")
        assert result.status is CompilationStatus.FAILED
        assert "arbitrum" in result.error
        assert "base" in result.error
        assert "ethereum" in result.error

    def test_native_token_supply_fails(self):
        eth = _token("ETH", "0x0000000000000000000000000000000000000000", decimals=18, is_native=True)
        result, _ = _compile_supply(token=eth, intent=_supply_intent(token="ETH"))
        assert result.status is CompilationStatus.FAILED
        assert "ERC-20" in result.error


class TestDiscoveryBypass:
    """Permission discovery compiles for calldata SHAPE; balance/limit
    pre-flights that depend on the synthetic wallet's empty position are skipped."""

    def test_discovery_withdraw_skips_max_withdraw_preflight(self):
        result, sdk = _compile_withdraw(permission_discovery=True)
        assert result.status is CompilationStatus.SUCCESS, result.error
        sdk.get_max_withdraw.assert_not_called()
        assert result.transactions[0].data.startswith(WITHDRAW_SELECTOR)

    def test_discovery_full_exit_skips_balance_reads(self):
        result, sdk = _compile_withdraw(
            intent=_withdraw_intent(amount=Decimal("0"), withdraw_all=True),
            permission_discovery=True,
        )
        assert result.status is CompilationStatus.SUCCESS, result.error
        sdk.get_ftoken_share_balance.assert_not_called()
        sdk.get_max_redeem.assert_not_called()
        assert result.transactions[0].data.startswith(REDEEM_SELECTOR)


class TestFluidLendingAlias:
    """D2.M3 — the platform-spec ``fluid_lending`` string reaches the same path."""

    def test_alias_fluid_lending_normalizes_to_fluid(self):
        assert normalize_protocol("base", "fluid_lending") == "fluid"

    def test_alias_fluid_lending_supply_compiles_identical_shape(self):
        token = _token()
        results = {}
        for protocol in ("fluid", "fluid_lending"):
            sdk = _lending_sdk()
            intent = _supply_intent(protocol=protocol)
            with patch.object(FluidCompiler, "_build_sdk", return_value=sdk):
                # Full compile() dispatch — the compiler does not gate on the
                # intent.protocol string, so the alias reaches the same path.
                results[protocol] = FluidCompiler().compile(_ctx(_services(token)), intent)
        for protocol, result in results.items():
            assert result.status is CompilationStatus.SUCCESS, f"{protocol}: {result.error}"
        canonical, aliased = results["fluid"], results["fluid_lending"]
        assert [tx.tx_type for tx in aliased.transactions] == [tx.tx_type for tx in canonical.transactions]
        assert [tx.to for tx in aliased.transactions] == [tx.to for tx in canonical.transactions]
        assert [tx.data for tx in aliased.transactions] == [tx.data for tx in canonical.transactions]
        assert aliased.transactions[-1].data.startswith(DEPOSIT_SELECTOR)
        assert aliased.action_bundle.metadata["ftoken"] == FTOKEN


class TestFluidPositionKeyParity:
    """D3.F5 — L3 writer and L5 valuer derive byte-identical keys (VIB-4981 class)."""

    def test_position_key_parity_writer_valuer(self):
        from almanak.framework.observability.position_events import lending_position_id
        from almanak.framework.teardown.models import PositionInfo, PositionType
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        # Mixed-case inputs on purpose — both sides must lowercase.
        wallet = "0xAbCdEf2222222222222222222222222222222222"
        l3_key = lending_position_id(chain="base", protocol="fluid", wallet=wallet, asset="USDC", market_id=None)
        position = PositionInfo(
            position_type=PositionType.SUPPLY,
            position_id=l3_key,
            chain="base",
            protocol="fluid",
            value_usd=Decimal("50"),
            # The fToken address travels in details (for valuation), NOT in the key.
            details={"wallet_address": wallet, "asset": "USDC", "ftoken": FTOKEN},
        )
        l5_key = PortfolioValuer._try_derive_lending_position_key(position, "base")
        assert l5_key is not None
        assert l3_key == l5_key, f"writer/valuer key mismatch: {l3_key!r} != {l5_key!r}"
        assert l3_key == f"lending:base:fluid:{wallet.lower()}:usdc"

    def test_position_key_no_market_id_segment(self):
        from almanak.framework.observability.position_events import lending_position_id
        from almanak.framework.teardown.models import PositionInfo, PositionType
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        l3_key = lending_position_id(chain="base", protocol="fluid", wallet=WALLET, asset="USDC", market_id=None)
        position = PositionInfo(
            position_type=PositionType.SUPPLY,
            position_id=l3_key,
            chain="base",
            protocol="fluid",
            value_usd=Decimal("50"),
            details={"wallet_address": WALLET, "asset": "USDC", "ftoken": FTOKEN},
        )
        l5_key = PortfolioValuer._try_derive_lending_position_key(position, "base")
        # Exactly 5 colon segments — NEITHER side inserts a market segment
        # when the intent carries no market_id (asymmetric insertion is the
        # VIB-4981 silent-join-miss class).
        assert l3_key.split(":") == ["lending", "base", "fluid", WALLET.lower(), "usdc"]
        assert l5_key.split(":") == ["lending", "base", "fluid", WALLET.lower(), "usdc"]
        assert FTOKEN.lower() not in l3_key


class TestFluidPreStateCaptureEnabled:
    def test_fluid_enabled_for_generic_pre_post_state_capture(self):
        # Sibling-connector convention (silo_v2 / euler_v2 / benqi): the
        # connector's lending_read powers confidence=HIGH ONLY if the protocol
        # is explicitly enabled on the live-money read path.
        from almanak.framework.accounting.lending_accounting import _GENERIC_PRE_STATE_PROTOCOLS

        assert "fluid" in _GENERIC_PRE_STATE_PROTOCOLS


class TestMarketUniverseSync:
    """pr-auditor 2026-06-11 — compile/valuation market-universe desync.

    Compilation resolves the fToken on-chain; valuation/accounting read the
    pinned ``FLUID_FTOKEN_MARKETS`` table. A resolvable-but-unpinned market
    or a pinned-but-migrated fToken must FAIL CLOSED at compile time, never
    compile a supply the accounting layer cannot value (or values against a
    stale vault).
    """

    def test_resolvable_but_unpinned_market_fails_closed(self):
        # DAI resolves on-chain (mocked) but has no FLUID_FTOKEN_MARKETS row.
        dai = _token("DAI", "0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb", decimals=18)
        result, sdk = _compile_supply(
            token=dai,
            intent=_supply_intent(token="DAI"),
            sdk=_lending_sdk(ftoken="0x9999999999999999999999999999999999999999"),
        )
        assert result.status is CompilationStatus.FAILED
        assert "not yet enabled" in result.error
        assert "FLUID_FTOKEN_MARKETS" in result.error
        sdk.build_deposit_tx.assert_not_called()

    def test_pinned_address_resolver_mismatch_fails_closed(self):
        # USDC IS pinned on base, but the resolver returns a DIFFERENT vault
        # — fToken migration; the stale table must fail loudly.
        migrated = "0x8888888888888888888888888888888888888888"
        result, sdk = _compile_supply(sdk=_lending_sdk(ftoken=migrated))
        assert result.status is CompilationStatus.FAILED
        assert "mismatch" in result.error
        assert "migration" in result.error
        assert migrated in result.error
        assert FTOKEN in result.error  # names the pinned address too
        sdk.build_deposit_tx.assert_not_called()

    def test_pinned_address_matches_resolver_case_insensitive(self):
        # Happy path: table pin == resolver (different casing) → compiles.
        result, _ = _compile_supply(sdk=_lending_sdk(ftoken=FTOKEN.lower()))
        assert result.status is CompilationStatus.SUCCESS, result.error

    def test_withdraw_also_gated_by_market_table(self):
        result, sdk = _compile_withdraw(sdk=_lending_sdk(ftoken="0x8888888888888888888888888888888888888888"))
        assert result.status is CompilationStatus.FAILED
        assert "mismatch" in result.error
        sdk.build_withdraw_tx.assert_not_called()


class TestPreflightSupplyBalanceCheck:
    """Codex P2 — SUPPLY metadata must reach the shared pre-flight balance check.

    The orchestrator reads ``metadata["supply_token"]`` + ``supply_amount``
    and classifies wei/human from the manifest's ``metadata_amount_encoding``.
    Fluid emits wei — without the declaration a 50 USDC supply would be
    treated as 50 raw units (5e-5 USDC) and the check would always pass.
    """

    def test_fluid_lending_metadata_classified_wei(self):
        from almanak.framework.execution.orchestrator import _lending_amount_is_wei

        assert _lending_amount_is_wei("fluid") is True

    def test_preflight_collects_supply_requirement_in_wei(self):
        from almanak.framework.execution.orchestrator import _preflight_supply_requirements

        result, _ = _compile_supply()
        assert result.status is CompilationStatus.SUCCESS, result.error
        reqs = _preflight_supply_requirements(result.action_bundle.metadata, "fluid")
        assert len(reqs) == 1
        symbol, address, amount_wei, decimals, is_native = reqs[0]
        assert symbol == "USDC"
        assert address == USDC_ADDR
        assert amount_wei == 50_000_000  # exact wei — NOT 50 * 10**6 * 10**6
        assert decimals == 6
        assert is_native is False


class TestFluidLendingAliasAccounting:
    """BLOCKER (2 auditors, 2026-06-11) — the platform-spec alias
    ``protocol="fluid_lending"`` must neither degrade accounting confidence
    to ESTIMATED (``_GENERIC_PRE_STATE_PROTOCOLS`` gate) nor diverge the
    position keys (``lending:{chain}:fluid_lending:...`` vs ``:fluid:``).
    Canonicalization happens once at the accounting boundary via
    ``LendingReadRegistry.normalize_protocol`` (manifest-declared
    ``LendingReadDecl.aliases``; aave precedent: ``"aave"`` → aave_v3).
    """

    CANONICAL_KEY = f"lending:base:fluid:{WALLET.lower()}:usdc"

    @staticmethod
    def _alias_supply_intent():
        intent = MagicMock()
        intent.intent_type.value = "SUPPLY"
        intent.protocol = "fluid_lending"
        intent.token = "USDC"
        intent.borrow_token = None
        intent.collateral_token = None
        intent.market_id = None
        return intent

    def test_lending_read_alias_declared_on_manifest(self):
        from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry
        from almanak.connectors.fluid.connector import CONNECTOR

        assert CONNECTOR.lending_read is not None
        assert "fluid_lending" in CONNECTOR.lending_read.aliases
        assert LendingReadRegistry.normalize_protocol("fluid_lending") == "fluid"

    def test_alias_passes_generic_pre_state_gate(self):
        """capture_lending_pre_state must NOT early-return for the alias.

        The read seam (``read_lending_account_state``) is mocked: reaching
        it with the CANONICAL protocol proves the alias passed the
        ``_GENERIC_PRE_STATE_PROTOCOLS`` gate and was normalized before the
        registry read (→ confidence=HIGH lane, not silent ESTIMATED).
        """
        from almanak.connectors._strategy_base.lending_read_base import LendingAccountState
        from almanak.framework.accounting import lending_accounting

        sentinel = LendingAccountState(
            collateral_usd=Decimal("50"),
            debt_usd=Decimal("0"),
            health_factor=None,
            liquidation_threshold_bps=None,
            e_mode_category=None,
            lltv=None,
        )
        with patch.object(lending_accounting, "read_lending_account_state", return_value=sentinel) as read:
            state = lending_accounting.capture_lending_pre_state(
                intent=self._alias_supply_intent(),
                chain="base",
                wallet_address=WALLET,
                gateway_client=MagicMock(),
                price_oracle={"USDC": Decimal("1")},
            )
        assert state is sentinel, "alias protocol must not early-return at the pre-state gate"
        read.assert_called_once()
        assert read.call_args.kwargs["protocol"] == "fluid"  # canonicalized, not raw alias

    def test_alias_position_key_canonical_in_accounting_event(self):
        """L5 — ``_derive_position_key`` path via build_lending_accounting_event."""
        from almanak.framework.accounting.basis import FIFOBasisStore
        from almanak.framework.accounting.lending_accounting import build_lending_accounting_event

        result = MagicMock()
        result.tx_hash = "0xdeadbeef"
        result.extracted_data = {"supply_amount": 50_000_000}
        result.total_gas_cost_wei = None

        event = build_lending_accounting_event(
            intent=self._alias_supply_intent(),
            result=result,
            deployment_id="deploy-1",
            cycle_id="cycle-1",
            execution_mode="paper",
            chain="base",
            wallet_address=WALLET,
            gateway_client=None,  # post-state read skipped; key derivation is the subject
            basis_store=FIFOBasisStore(),
            price_oracle={"USDC": Decimal("1")},
        )
        assert event is not None
        assert event.position_key == self.CANONICAL_KEY  # NOT lending:base:fluid_lending:...
        assert event.identity.protocol == "fluid"

    def test_alias_position_key_canonical_in_position_events_writer(self):
        """L3 — ``lending_position_id`` writer path (does NOT flow through
        build_lending_accounting_event; needs its own normalization)."""
        from almanak.framework.observability.position_events import build_position_event_from_intent

        event = build_position_event_from_intent(
            deployment_id="deploy-1",
            intent=_supply_intent(protocol="fluid_lending"),
            result=None,
            chain="base",
            wallet_address=WALLET,
        )
        assert event is not None
        assert event.position_id == self.CANONICAL_KEY  # joins L5 position_key byte-for-byte
