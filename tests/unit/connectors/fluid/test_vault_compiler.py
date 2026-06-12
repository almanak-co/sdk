"""Unit tests for the Fluid vault (NFT-CDP) compiler (VIB-5031).

UAT card coverage (D2.M3 / D2.M5 / D3.F1 / D3.F3 / D3.F4 / D3.F6): protocol-key
routing, deleverage dispatch, the over-repay guard trio, fail-closed external
reads, scope guards, and the L3==L5 position-key byte-identity pin. The SDK
boundary (``FluidVaultCompiler._build_sdk``) is mocked — calldata still flows
through the REAL offline ``operate()`` encoder so the int-min sentinel and
signed deltas are byte-real. On-chain behaviour is covered by
``tests/intents/{arbitrum,base}/test_fluid_vault_lending.py``.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors._strategy_base.base.compiler import BaseCompilerContext
from almanak.connectors.fluid.compiler import FluidCompiler
from almanak.connectors.fluid.sdk import FluidSDKError
from almanak.connectors.fluid.vault_compiler import FluidVaultCompiler
from almanak.connectors.fluid.vault_sdk import (
    INT256_MIN,
    OPERATE_SELECTOR,
    FluidVaultData,
    FluidVaultPosition,
    FluidVaultSDK,
)
from almanak.framework.intents import (
    BorrowIntent,
    DeleverageIntent,
    RepayIntent,
    SupplyIntent,
    WithdrawIntent,
)
from almanak.framework.intents.compiler_models import CompilationStatus, TransactionData

ARB_VAULT = "0xeabbfca72f8a8bf14c4ac59e69ecb2eb69f0811c"  # vault id 1 (ETH -> USDC)
BASE_VAULT = "0x01f0d07fde184614216e76782c6b7df663f5375e"  # vault id 47 (sUSDai -> USDC)
ARB_USDC = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
BASE_USDC = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
SUSDAI = "0x0B2b2B2076d95dda7817e785989fE353fe955ef9"
NATIVE = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"
ARB_FUSDC = "0x1A996cb54bb95462040408C06122D45D6Cdb6096"  # arbitrum fToken (fUSDC)
WALLET = "0x2222222222222222222222222222222222222222"
NFT = 12542

# Vault-oracle exchange rate for ETH(18dp) -> USDC(6dp) at $2500:
# debt_units = col_units * price / 1e27  =>  price = 2500e6 * 1e27 / 1e18.
ORACLE_PRICE = 2500 * 10**6 * 10**27 // 10**18  # == 2.5e18
DEBT_NOW = 500_000_000  # 500 USDC
SUPPLY_NOW = 10**18  # 1 ETH


def _eth_token() -> SimpleNamespace:
    return SimpleNamespace(
        symbol="ETH",
        address="0x0000000000000000000000000000000000000000",
        decimals=18,
        is_native=True,
        to_dict=lambda: {"symbol": "ETH", "address": NATIVE, "decimals": 18},
    )


def _usdc_token(address: str = ARB_USDC) -> SimpleNamespace:
    return SimpleNamespace(
        symbol="USDC",
        address=address,
        decimals=6,
        is_native=False,
        to_dict=lambda: {"symbol": "USDC", "address": address, "decimals": 6},
    )


def _susdai_token() -> SimpleNamespace:
    return SimpleNamespace(
        symbol="sUSDai",
        address=SUSDAI,
        decimals=18,
        is_native=False,
        to_dict=lambda: {"symbol": "sUSDai", "address": SUSDAI, "decimals": 18},
    )


def _services(tokens: dict[str, SimpleNamespace], balance: int | None = 10**15) -> MagicMock:
    services = MagicMock()
    services.resolve_token.side_effect = lambda symbol: tokens.get(symbol)
    services.build_approve_tx.side_effect = lambda token_address, spender, amount: [
        TransactionData(
            to=token_address,
            value=0,
            data="0x095ea7b3" + spender[2:].lower().zfill(64) + f"{amount:064x}",
            gas_estimate=46_000,
            tx_type="approve",
            description=f"approve {amount} to {spender}",
        )
    ]
    services.format_amount.side_effect = lambda amount, decimals: str(amount)
    services.query_erc20_balance.return_value = balance
    return services


def _ctx(services: MagicMock, chain: str = "arbitrum", **overrides) -> BaseCompilerContext:
    defaults = {
        "chain": chain,
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
    return BaseCompilerContext(**defaults)


def _vault_data(
    vault: str = ARB_VAULT,
    oracle_price: int = ORACLE_PRICE,
    cf: int = 8700,
    supply_token: str = NATIVE,
    borrow_token: str = ARB_USDC,
    vault_id: int = 1,
    withdrawable: int = 10**24,
    borrowable: int = 10**12,
) -> FluidVaultData:
    return FluidVaultData(
        vault=vault,
        is_smart_col=False,
        is_smart_debt=False,
        supply_token=supply_token,
        borrow_token=borrow_token,
        vault_id=vault_id,
        vault_type=10000,
        collateral_factor=cf,
        liquidation_threshold=9200,
        liquidation_max_limit=9500,
        liquidation_penalty=100,
        oracle="0x" + "0" * 40,
        oracle_price_operate=oracle_price,
        oracle_price_liquidate=oracle_price,
        withdrawable=withdrawable,
        borrowable=borrowable,
        total_supply_vault=0,
        total_borrow_vault=0,
    )


def _position(
    supply: int = SUPPLY_NOW, borrow: int = DEBT_NOW, nft: int = NFT, vault: str = ARB_VAULT
) -> FluidVaultPosition:
    return FluidVaultPosition(
        nft_id=nft,
        owner=WALLET,
        is_liquidated=False,
        is_supply_position=borrow == 0,
        tick=0,
        tick_id=0,
        supply=supply,
        borrow=borrow,
        dust_borrow=0,
        vault=vault,
    )


def _vault_sdk(
    nft: int | None = NFT,
    supply: int = SUPPLY_NOW,
    borrow: int = DEBT_NOW,
    resolve_exc: Exception | None = None,
    position_exc: Exception | None = None,
    vault_exc: Exception | None = None,
    vault: str = ARB_VAULT,
    chain: str = "arbitrum",
    supply_token: str = NATIVE,
    borrow_token: str = ARB_USDC,
    vault_id: int = 1,
    withdrawable: int = 10**24,
    borrowable: int = 10**12,
) -> MagicMock:
    sdk = MagicMock()
    encoder = FluidVaultSDK(chain=chain, rpc_url="http://localhost:9")  # offline calldata only
    sdk.build_operate_tx.side_effect = encoder.build_operate_tx
    vault_data = _vault_data(
        vault=vault,
        supply_token=supply_token,
        borrow_token=borrow_token,
        vault_id=vault_id,
        withdrawable=withdrawable,
        borrowable=borrowable,
    )
    if resolve_exc is not None:
        sdk.resolve_user_nft_for_vault.side_effect = resolve_exc
    else:
        sdk.resolve_user_nft_for_vault.return_value = nft
    if position_exc is not None:
        sdk.position_by_nft_id.side_effect = position_exc
    else:
        sdk.position_by_nft_id.return_value = (
            _position(supply=supply, borrow=borrow, nft=nft or 0, vault=vault),
            vault_data,
        )
    if vault_exc is not None:
        sdk.get_vault_entire_data.side_effect = vault_exc
    else:
        sdk.get_vault_entire_data.return_value = vault_data
    return sdk


def _arb_tokens() -> dict[str, SimpleNamespace]:
    return {"ETH": _eth_token(), "USDC": _usdc_token()}


def _compile(intent, sdk=None, tokens=None, chain="arbitrum", balance: int | None = 10**15, **ctx_overrides):
    sdk = sdk if sdk is not None else _vault_sdk()
    tokens = tokens if tokens is not None else _arb_tokens()
    ctx = _ctx(_services(tokens, balance=balance), chain=chain, **ctx_overrides)
    with patch.object(FluidVaultCompiler, "_build_sdk", return_value=sdk):
        result = FluidVaultCompiler().compile(ctx, intent)
    return result, sdk


def _supply_intent(**overrides) -> SupplyIntent:
    defaults = {
        "protocol": "fluid_vault",
        "token": "ETH",
        "amount": Decimal("1"),
        "market_id": ARB_VAULT,
        "chain": "arbitrum",
    }
    defaults.update(overrides)
    return SupplyIntent(**defaults)


def _borrow_intent(**overrides) -> BorrowIntent:
    defaults = {
        "protocol": "fluid_vault",
        "collateral_token": "ETH",
        "collateral_amount": Decimal("1"),
        "borrow_token": "USDC",
        "borrow_amount": Decimal("500"),
        "market_id": ARB_VAULT,
        "chain": "arbitrum",
    }
    defaults.update(overrides)
    return BorrowIntent(**defaults)


def _repay_intent(**overrides) -> RepayIntent:
    defaults = {
        "protocol": "fluid_vault",
        "token": "USDC",
        "amount": Decimal("200"),
        "market_id": ARB_VAULT,
        "chain": "arbitrum",
    }
    defaults.update(overrides)
    return RepayIntent(**defaults)


def _withdraw_intent(**overrides) -> WithdrawIntent:
    defaults = {
        "protocol": "fluid_vault",
        "token": "ETH",
        "amount": Decimal("0.5"),
        "market_id": ARB_VAULT,
        "chain": "arbitrum",
    }
    defaults.update(overrides)
    return WithdrawIntent(**defaults)


def _operate_words(tx: TransactionData) -> tuple[int, int, int]:
    """Decode (nftId, colDelta, debtDelta) from real operate() calldata."""
    assert tx.data.startswith(OPERATE_SELECTOR)
    body = tx.data[10:]

    def _signed(word_hex: str) -> int:
        value = int(word_hex, 16)
        return value - (1 << 256) if value >= (1 << 255) else value

    return int(body[0:64], 16), _signed(body[64:128]), _signed(body[128:192])


def _operate_tx(result) -> TransactionData:
    operate_txs = [tx for tx in result.transactions if tx.data.startswith(OPERATE_SELECTOR)]
    assert len(operate_txs) == 1, "exactly one operate() per compiled lifecycle action"
    return operate_txs[0]


# =============================================================================
# D2.M3 — protocol-key routing (fluid_vault vs the shipped fToken surface)
# =============================================================================


class TestProtocolKeyRouting:
    def test_protocol_key_requires_market_id_at_intent_construction(self):
        # Capability requires_market_id=True: rejection happens at INTENT
        # CONSTRUCTION, before any compiler runs.
        with pytest.raises(ValueError, match="market_id"):
            SupplyIntent(protocol="fluid_vault", token="ETH", amount=Decimal("1"), chain="arbitrum")

    def test_protocol_key_ftoken_reject_market_id_names_both_universes(self):
        # An fToken (ERC-4626) address on the VAULT key fails with a message
        # naming both universes.
        result, sdk = _compile(_supply_intent(market_id=ARB_FUSDC))
        assert result.status is CompilationStatus.FAILED
        assert "fToken" in result.error
        assert "fluid_vault" in result.error
        assert "protocol='fluid'" in result.error
        sdk.build_operate_tx.assert_not_called()

    def test_protocol_key_routing_fluid_still_rejects_any_market_id(self):
        # Phase-2 regression guard: the fToken compiler STILL rejects any
        # truthy market_id exactly as shipped (byte-stable error class).
        ftoken_sdk = MagicMock()
        ftoken_sdk.find_ftoken_for_underlying.return_value = ARB_FUSDC
        intent = SupplyIntent(protocol="fluid", token="USDC", amount=Decimal("50"), market_id=ARB_FUSDC.lower())
        from almanak.connectors._strategy_base.base.compiler import SwapCompilerContext

        services = _services({"USDC": _usdc_token()})
        ctx = SwapCompilerContext(
            chain="arbitrum",
            wallet_address=WALLET,
            rpc_url="http://localhost:8545",
            rpc_timeout=10.0,
            permission_discovery=False,
            allow_placeholder_prices=True,
            token_resolver=None,
            gateway_client=None,
            price_oracle={},
            cache={},
            services=services,
        )
        with patch.object(FluidCompiler, "_build_sdk", return_value=ftoken_sdk):
            result = FluidCompiler().compile_supply(ctx, intent)
        assert result.status is CompilationStatus.FAILED
        assert "must omit market_id" in result.error

    def test_protocol_key_routing_dispatches_full_lifecycle(self):
        # compile() routes SUPPLY/BORROW/REPAY/WITHDRAW onto the operate paths.
        for intent in (_supply_intent(), _borrow_intent(), _repay_intent(), _withdraw_intent()):
            result, _ = _compile(intent)
            assert result.status is CompilationStatus.SUCCESS, f"{intent.intent_type}: {result.error}"
            assert result.action_bundle.metadata["protocol"] == "fluid_vault"
            assert result.action_bundle.metadata["market_id"] == ARB_VAULT  # canonical lowercase

    def test_protocol_key_market_id_lowercased_canonical_form(self):
        result, _ = _compile(_supply_intent(market_id=ARB_VAULT.upper().replace("0X", "0x")))
        assert result.status is CompilationStatus.SUCCESS, result.error
        assert result.action_bundle.metadata["market_id"] == ARB_VAULT


# =============================================================================
# D2.M5 — DELEVERAGE routes to the repay compile path
# =============================================================================


class TestDeleverage:
    def test_deleverage_routes_to_repay_compile_path(self):
        repay_result, _ = _compile(_repay_intent(amount=Decimal("200")))
        deleverage_result, _ = _compile(
            DeleverageIntent(
                protocol="fluid_vault",
                token="USDC",
                amount=Decimal("200"),
                market_id=ARB_VAULT,
                chain="arbitrum",
                trigger_reason="HF guard",
            )
        )
        assert deleverage_result.status is CompilationStatus.SUCCESS, deleverage_result.error
        assert _operate_words(_operate_tx(deleverage_result)) == _operate_words(_operate_tx(repay_result))
        assert _operate_words(_operate_tx(deleverage_result)) == (NFT, 0, -200_000_000)

    def test_deleverage_repay_full_uses_int_min_and_guards(self):
        result, _ = _compile(
            DeleverageIntent(
                protocol="fluid_vault",
                token="USDC",
                amount=Decimal("0"),
                repay_full=True,
                market_id=ARB_VAULT,
                chain="arbitrum",
            )
        )
        assert result.status is CompilationStatus.SUCCESS, result.error
        assert _operate_words(_operate_tx(result)) == (NFT, 0, INT256_MIN)

    def test_deleverage_over_repay_guard_applies(self):
        result, _ = _compile(
            DeleverageIntent(
                protocol="fluid_vault",
                token="USDC",
                amount=Decimal("600"),  # > 500 USDC debt
                market_id=ARB_VAULT,
                chain="arbitrum",
            )
        )
        assert result.status is CompilationStatus.FAILED
        assert "repay_full=True" in result.error


# =============================================================================
# D3.F1 — over-repay can never reach the chain
# =============================================================================


class TestRepayGuards:
    def test_over_repay_exceeds_debt_fails_naming_debt_and_repay_full(self):
        result, sdk = _compile(_repay_intent(amount=Decimal("600")))
        assert result.status is CompilationStatus.FAILED
        assert str(DEBT_NOW) in result.error  # names the current debt
        assert "repay_full=True" in result.error  # points at the safe path
        assert "31015" in result.error  # names the protocol revert it prevents
        assert result.transactions == []
        sdk.build_operate_tx.assert_not_called()

    def test_repay_full_compiles_int_min_sentinel_never_explicit_amount(self):
        result, _ = _compile(_repay_intent(amount=Decimal("0"), repay_full=True))
        assert result.status is CompilationStatus.SUCCESS, result.error
        nft_id, col, debt = _operate_words(_operate_tx(result))
        assert (nft_id, col) == (NFT, 0)
        assert debt == INT256_MIN, "full repay must be the protocol sentinel, never an explicit amount"
        assert result.action_bundle.metadata["mode"] == "repay_full_int_min"

    def test_repay_full_approval_bounded_at_debt_plus_headroom_not_max_uint(self):
        services = _services(_arb_tokens(), balance=10**15)
        ctx = _ctx(services)
        with patch.object(FluidVaultCompiler, "_build_sdk", return_value=_vault_sdk()):
            result = FluidVaultCompiler().compile(ctx, _repay_intent(amount=Decimal("0"), repay_full=True))
        assert result.status is CompilationStatus.SUCCESS, result.error
        # Decode the EMITTED approve calldata — asserting on a helper mock's
        # input let a 10% buffered over-approval ship undetected (CodeRabbit
        # audit): ctx.services.build_approve_tx must not be involved at all.
        services.build_approve_tx.assert_not_called()
        approve_tx = result.transactions[0]
        assert approve_tx.tx_type == "approve"
        data = approve_tx.data.removeprefix("0x095ea7b3")
        assert data != approve_tx.data, "approve selector missing"
        expected_bound = 500_500_000  # ceil(500e6 * 1.001)
        assert "0x" + data[:64][-40:] == ARB_VAULT.lower()
        assert int(data[64:128], 16) == expected_bound, "approval must be EXACTLY debt*(1+headroom), no extra buffer"
        assert result.action_bundle.metadata["approval_bound"] == str(expected_bound)

    def test_repay_full_insufficient_balance_fails_fund_or_partial(self):
        # Wallet holds exactly debt_now — NOT enough to cover accrual headroom.
        result, sdk = _compile(_repay_intent(amount=Decimal("0"), repay_full=True), balance=DEBT_NOW)
        assert result.status is CompilationStatus.FAILED
        assert "Fund the wallet or repay partially" in result.error
        sdk.build_operate_tx.assert_not_called()

    def test_repay_guard_partial_at_exact_debt_compiles(self):
        # Boundary: amount == debt_now is monotonically safe (debt only grows).
        result, _ = _compile(_repay_intent(amount=Decimal("500")))
        assert result.status is CompilationStatus.SUCCESS, result.error
        assert _operate_words(_operate_tx(result)) == (NFT, 0, -DEBT_NOW)

    def test_repay_full_zero_debt_fails(self):
        result, _ = _compile(_repay_intent(amount=Decimal("0"), repay_full=True), sdk=_vault_sdk(borrow=0))
        assert result.status is CompilationStatus.FAILED
        assert "no outstanding" in result.error

    def test_repay_guard_partial_insufficient_wallet_balance_fails(self):
        result, _ = _compile(_repay_intent(amount=Decimal("200")), balance=100_000_000)
        assert result.status is CompilationStatus.FAILED
        assert "Fund the wallet" in result.error

    def test_repay_guard_amount_all_unresolved_fails(self):
        result, _ = _compile(_repay_intent(amount="all"))
        assert result.status is CompilationStatus.FAILED
        assert "resolved" in result.error
        assert "repay_full" in result.error  # int-min is reachable ONLY via repay_full


# =============================================================================
# D3.F3 — external read failures fail CLOSED
# =============================================================================


class TestReadFailuresFailClosed:
    def test_nft_resolution_read_failure_fails_closed_never_mints(self):
        # The duplicate-position disaster: a failed resolution must NEVER
        # fall back to a mint (nftId=0).
        result, sdk = _compile(_supply_intent(), sdk=_vault_sdk(resolve_exc=FluidSDKError("rpc unreachable")))
        assert result.status is CompilationStatus.FAILED
        assert "fail closed" in result.error
        assert "duplicate" in result.error
        sdk.build_operate_tx.assert_not_called()

    def test_repay_position_read_failure_fails_closed_keeps_debt_preflight(self):
        # The debt pre-flight must never be SKIPPED on a read failure.
        result, sdk = _compile(_repay_intent(), sdk=_vault_sdk(position_exc=FluidSDKError("eth_call failed")))
        assert result.status is CompilationStatus.FAILED
        assert "fail closed" in result.error
        sdk.build_operate_tx.assert_not_called()

    def test_borrow_vault_data_read_failure_fails_closed(self):
        result, sdk = _compile(_borrow_intent(), sdk=_vault_sdk(vault_exc=FluidSDKError("eth_call failed")))
        assert result.status is CompilationStatus.FAILED
        assert "fail closed" in result.error
        sdk.build_operate_tx.assert_not_called()

    def test_no_transport_fails_closed_naming_gateway_and_rpc(self):
        # Real _build_sdk path: no gateway client AND no RPC URL.
        ctx = _ctx(_services(_arb_tokens()), rpc_url=None, gateway_client=None)
        result = FluidVaultCompiler().compile(ctx, _supply_intent())
        assert result.status is CompilationStatus.FAILED
        assert "gateway" in result.error.lower()
        assert "rpc" in result.error.lower()

    def test_disconnected_gateway_fails_closed_never_falls_back_to_rpc(self):
        # A PRESENT-but-disconnected gateway client must FAIL the compile:
        # silently nulling it would turn a gateway outage into a direct-RPC
        # bypass via ctx.rpc_url.
        gateway = MagicMock()
        gateway.is_connected = False
        ctx = _ctx(_services(_arb_tokens()), gateway_client=gateway)
        with patch("almanak.connectors.fluid.vault_compiler.FluidVaultSDK") as sdk_cls:
            result = FluidVaultCompiler().compile(ctx, _supply_intent())
        assert result.status is CompilationStatus.FAILED
        assert "DISCONNECTED" in result.error
        assert "gateway" in result.error.lower()
        sdk_cls.assert_not_called()  # the rpc_url fallback was NOT taken

    def test_no_gateway_with_rpc_url_still_builds_sdk(self):
        # The intent-test harness path: NO gateway client configured at all —
        # the rpc_url fallback remains valid there and only there.
        ctx = _ctx(_services(_arb_tokens()), gateway_client=None)
        built = FluidVaultCompiler._build_sdk(ctx, _supply_intent())
        assert isinstance(built, FluidVaultSDK)
        assert built.rpc_url == "http://localhost:8545"

    def test_connected_gateway_prefers_gateway_transport_over_rpc(self):
        gateway = MagicMock()
        gateway.is_connected = True
        ctx = _ctx(_services(_arb_tokens()), gateway_client=gateway)
        built = FluidVaultCompiler._build_sdk(ctx, _supply_intent())
        assert isinstance(built, FluidVaultSDK)
        assert built.rpc_url is None  # gateway transport, not the ctx rpc_url

    def test_repay_full_balance_read_failure_fails_closed(self):
        result, sdk = _compile(_repay_intent(amount=Decimal("0"), repay_full=True), balance=None)
        assert result.status is CompilationStatus.FAILED
        assert "fail closed" in result.error
        sdk.build_operate_tx.assert_not_called()

    def test_withdraw_position_read_failure_fails_closed(self):
        result, sdk = _compile(_withdraw_intent(), sdk=_vault_sdk(position_exc=FluidSDKError("timeout")))
        assert result.status is CompilationStatus.FAILED
        assert "fail closed" in result.error
        sdk.build_operate_tx.assert_not_called()


# =============================================================================
# D3.F4 — scope guards: non-type-1, off-chain, native-debt, unknown vault
# =============================================================================


class TestScopeGuards:
    def test_off_chain_ethereum_scope_fails_naming_supported_universe(self):
        result, _ = _compile(_supply_intent(chain="ethereum"), chain="ethereum")
        assert result.status is CompilationStatus.FAILED
        assert "arbitrum" in result.error
        assert "base" in result.error
        assert "ethereum" in result.error

    def test_off_chain_polygon_scope_fails(self):
        result, _ = _compile(_supply_intent(chain="polygon"), chain="polygon")
        assert result.status is CompilationStatus.FAILED
        assert "polygon" in result.error

    def test_unknown_vault_market_id_fails_naming_both_universes(self):
        unknown = "0x9999999999999999999999999999999999999999"
        result, sdk = _compile(_supply_intent(market_id=unknown))
        assert result.status is CompilationStatus.FAILED
        assert "Unknown Fluid vault" in result.error
        assert unknown in result.error
        assert ARB_VAULT in result.error  # names the pinned universe
        assert "fToken" in result.error  # points fToken callers at protocol='fluid'
        sdk.build_operate_tx.assert_not_called()

    def test_non_type1_vault_fails_pointing_at_phase4(self):
        from almanak.connectors.fluid.addresses import FLUID_VAULT_MARKETS

        smart_vault = "0x7777777777777777777777777777777777777777"
        entry = dict(FLUID_VAULT_MARKETS["arbitrum"][ARB_VAULT], vault_type=20000)
        with patch.dict(FLUID_VAULT_MARKETS["arbitrum"], {smart_vault: entry}):
            result, _ = _compile(_supply_intent(market_id=smart_vault))
        assert result.status is CompilationStatus.FAILED
        assert "type-1" in result.error
        assert "Phase 4" in result.error

    def test_native_debt_vault_fails_with_scoped_message(self):
        from almanak.connectors.fluid.addresses import FLUID_VAULT_MARKETS

        native_debt_vault = "0x8888888888888888888888888888888888888888"
        entry = dict(FLUID_VAULT_MARKETS["arbitrum"][ARB_VAULT], native_debt=True)
        with patch.dict(FLUID_VAULT_MARKETS["arbitrum"], {native_debt_vault: entry}):
            result, _ = _compile(_supply_intent(market_id=native_debt_vault))
        assert result.status is CompilationStatus.FAILED
        assert "NATIVE debt" in result.error
        assert "out of" in result.error

    def test_scope_wrong_collateral_token_fails_naming_vault_pair(self):
        result, _ = _compile(_supply_intent(token="USDC"))
        assert result.status is CompilationStatus.FAILED
        assert "not the collateral token" in result.error
        assert "ETH" in result.error and "USDC" in result.error


# =============================================================================
# SUPPLY — operate(nft_or_0, +col, 0)
# =============================================================================


class TestSupplyCompile:
    def test_supply_native_collateral_sets_msg_value_no_approve(self):
        result, _ = _compile(_supply_intent())
        assert result.status is CompilationStatus.SUCCESS, result.error
        assert [tx.tx_type for tx in result.transactions] == ["lending_supply"], "native leg compiles NO approve"
        tx = _operate_tx(result)
        assert tx.value == 10**18, "collateral rides msg.value on native vaults"
        assert _operate_words(tx) == (NFT, 10**18, 0)
        assert any("msg.value" in w for w in result.warnings)

    def test_supply_mint_path_when_no_nft(self):
        result, _ = _compile(_supply_intent(), sdk=_vault_sdk(nft=None))
        assert result.status is CompilationStatus.SUCCESS, result.error
        assert _operate_words(_operate_tx(result))[0] == 0, "first action mints with nftId=0"
        assert any("mints" in w for w in result.warnings)

    def test_supply_reuses_existing_nft(self):
        result, _ = _compile(_supply_intent())
        assert _operate_words(_operate_tx(result))[0] == NFT

    def test_supply_erc20_collateral_base_vault_exact_approve_spender_is_vault(self):
        # Base vault 47: sUSDai is an ERC-20 leg — approve(spender==vault,
        # exact amount) + operate with msg.value == 0 (D2.M1 contract).
        tokens = {"sUSDai": _susdai_token(), "USDC": _usdc_token(BASE_USDC)}
        services = _services(tokens)
        ctx = _ctx(services, chain="base")
        intent = SupplyIntent(
            protocol="fluid_vault",
            token="sUSDai",
            amount=Decimal("100"),
            market_id=BASE_VAULT,
            chain="base",
        )
        sdk = _vault_sdk(
            nft=None,
            vault=BASE_VAULT,
            chain="base",
            supply_token=SUSDAI,
            borrow_token=BASE_USDC,
            vault_id=47,
        )
        with patch.object(FluidVaultCompiler, "_build_sdk", return_value=sdk):
            result = FluidVaultCompiler().compile(ctx, intent)
        assert result.status is CompilationStatus.SUCCESS, result.error
        assert [tx.tx_type for tx in result.transactions] == ["approve", "lending_supply"]
        # The compiler builds the approve calldata itself — the framework's
        # ctx.services.build_approve_tx adds a 10% swap-style buffer and must
        # NOT be used for vault collateral. Decode the emitted tx instead.
        services.build_approve_tx.assert_not_called()
        approve_tx = result.transactions[0]
        assert approve_tx.to == SUSDAI
        data = approve_tx.data.removeprefix("0x095ea7b3")
        assert data != approve_tx.data, "approve selector missing"
        assert "0x" + data[:64][-40:] == BASE_VAULT.lower()
        assert int(data[64:128], 16) == 100 * 10**18  # exact, never MAX_UINT
        assert _operate_tx(result).value == 0

    def test_supply_metadata_wei_encoded(self):
        result, _ = _compile(_supply_intent())
        md = result.action_bundle.metadata
        assert md["supply_amount"] == str(10**18)
        assert md["supply_token"]["symbol"] == "ETH"
        assert md["nft_id"] == str(NFT)


# =============================================================================
# BORROW — operate(nft_or_0, +col, +debt) single atomic call
# =============================================================================


class TestBorrowCompile:
    def test_borrow_atomic_open_single_operate_moves_both_legs(self):
        result, _ = _compile(_borrow_intent(), sdk=_vault_sdk(nft=None, supply=0, borrow=0))
        assert result.status is CompilationStatus.SUCCESS, result.error
        nft_id, col, debt = _operate_words(_operate_tx(result))
        assert (nft_id, col, debt) == (0, 10**18, 500_000_000), "ONE atomic operate moves both legs (mint path)"
        assert _operate_tx(result).value == 10**18  # native collateral leg

    def test_borrow_against_existing_collateral_zero_col_delta(self):
        result, _ = _compile(_borrow_intent(collateral_amount=Decimal("0"), borrow_amount=Decimal("100")))
        assert result.status is CompilationStatus.SUCCESS, result.error
        assert _operate_words(_operate_tx(result)) == (NFT, 0, 100_000_000)
        assert _operate_tx(result).value == 0

    def test_borrow_zero_collateral_without_position_fails(self):
        result, _ = _compile(
            _borrow_intent(collateral_amount=Decimal("0")),
            sdk=_vault_sdk(nft=None),
        )
        assert result.status is CompilationStatus.FAILED
        assert "holds no position" in result.error
        assert "supply collateral" in result.error

    def test_borrow_negative_collateral_rejected_at_intent_construction(self):
        with pytest.raises(ValueError, match="non-negative"):
            _borrow_intent(collateral_amount=Decimal("-1"))

    def test_borrow_negative_collateral_compiler_guard_never_encodes_withdraw(self):
        # Defense in depth: even if a negative collateral_amount slips past
        # intent validation (model_copy skips validators), the compiler must
        # fail closed — operate(nft, -col, +debt) is a withdraw smuggled
        # into a BORROW.
        intent = _borrow_intent().model_copy(update={"collateral_amount": Decimal("-1")})
        result, sdk = _compile(intent)
        assert result.status is CompilationStatus.FAILED
        assert "non-negative" in result.error
        assert "WithdrawIntent" in result.error
        sdk.build_operate_tx.assert_not_called()

    def test_borrow_above_collateral_factor_fails_preflight(self):
        # 1 ETH @ $2500, CF 87% => max debt 2175 USDC; 2200 must fail.
        result, sdk = _compile(
            _borrow_intent(borrow_amount=Decimal("2200")),
            sdk=_vault_sdk(nft=None, supply=0, borrow=0),
        )
        assert result.status is CompilationStatus.FAILED
        assert "collateral factor" in result.error
        assert "Vault__PositionAboveCF" in result.error
        sdk.build_operate_tx.assert_not_called()

    def test_borrow_includes_existing_position_in_cf_preflight(self):
        # Existing 1 ETH col + 500 debt; borrowing 1700 more (total 2200 > 2175 cap).
        result, _ = _compile(
            _borrow_intent(collateral_amount=Decimal("0"), borrow_amount=Decimal("1700")),
        )
        assert result.status is CompilationStatus.FAILED
        assert "collateral factor" in result.error

    def test_borrow_at_liquidity_cap_compiles(self):
        # borrowable == requested borrow: AT the live availability cap is fine.
        result, _ = _compile(
            _borrow_intent(collateral_amount=Decimal("0"), borrow_amount=Decimal("100")),
            sdk=_vault_sdk(borrowable=100_000_000),
        )
        assert result.status is CompilationStatus.SUCCESS, result.error
        assert _operate_words(_operate_tx(result)) == (NFT, 0, 100_000_000)

    def test_borrow_above_liquidity_cap_fails_limit_gated_retryable(self):
        # CF allows it (well under the 2175 USDC cap) but the Liquidity layer
        # only has 99.999999 USDC available right now — the compile-time guard
        # for UserModule__MaxUtilizationReached (errorId 11011).
        result, sdk = _compile(
            _borrow_intent(collateral_amount=Decimal("0"), borrow_amount=Decimal("100")),
            sdk=_vault_sdk(borrowable=99_999_999),
        )
        assert result.status is CompilationStatus.FAILED
        assert "limit-gated (retryable)" in result.error, "must be distinguishable from hard failures"
        assert "99999999" in result.error, "must name the currently available amount"
        assert "time/utilization-gated" in result.error
        assert "11011" in result.error
        # DISTINCT from the CF hard failure — strategies branch on this.
        assert "Vault__PositionAboveCF" not in result.error
        assert "collateral factor" not in result.error
        sdk.build_operate_tx.assert_not_called()


# =============================================================================
# WITHDRAW — operate(nftId, −amount | INT256_MIN, 0)
# =============================================================================


class TestWithdrawCompile:
    def test_withdraw_all_with_outstanding_debt_fails(self):
        result, _ = _compile(_withdraw_intent(amount=Decimal("0"), withdraw_all=True))
        assert result.status is CompilationStatus.FAILED
        assert "repay_full=True" in result.error

    def test_withdraw_all_zero_debt_int_min_sentinel(self):
        result, _ = _compile(
            _withdraw_intent(amount=Decimal("0"), withdraw_all=True),
            sdk=_vault_sdk(borrow=0),
        )
        assert result.status is CompilationStatus.SUCCESS, result.error
        assert _operate_words(_operate_tx(result)) == (NFT, INT256_MIN, 0)
        assert result.action_bundle.metadata["mode"] == "withdraw_all_int_min"

    def test_withdraw_exact_negative_delta_no_approve(self):
        result, _ = _compile(_withdraw_intent(amount=Decimal("0.5")))
        assert result.status is CompilationStatus.SUCCESS, result.error
        assert [tx.tx_type for tx in result.transactions] == ["lending_withdraw"]
        assert _operate_words(_operate_tx(result)) == (NFT, -(10**18 // 2), 0)

    def test_withdraw_exact_above_withdrawable_fails(self):
        # 1 ETH col, 500 USDC debt @ CF 87%: min col ~0.2299 ETH, so a 0.9 ETH
        # withdraw must fail the CF-aware cap.
        result, sdk = _compile(_withdraw_intent(amount=Decimal("0.9")))
        assert result.status is CompilationStatus.FAILED
        assert "withdrawable" in result.error
        sdk.build_operate_tx.assert_not_called()

    def test_withdraw_amount_all_unresolved_fails(self):
        result, _ = _compile(_withdraw_intent(amount="all"))
        assert result.status is CompilationStatus.FAILED
        assert "resolved" in result.error

    def test_withdraw_at_liquidity_cap_compiles(self):
        # Zero debt (CF cap = full supply); withdrawable exactly the request.
        result, _ = _compile(
            _withdraw_intent(amount=Decimal("0.5")),
            sdk=_vault_sdk(borrow=0, withdrawable=10**18 // 2),
        )
        assert result.status is CompilationStatus.SUCCESS, result.error
        assert _operate_words(_operate_tx(result)) == (NFT, -(10**18 // 2), 0)

    def test_withdraw_above_liquidity_cap_fails_limit_gated_retryable(self):
        # CF allows the 0.5 ETH withdraw (zero debt) but the Liquidity layer
        # only has 0.1 ETH available right now — time-gated, NOT a CF failure.
        result, sdk = _compile(
            _withdraw_intent(amount=Decimal("0.5")),
            sdk=_vault_sdk(borrow=0, withdrawable=10**17),
        )
        assert result.status is CompilationStatus.FAILED
        assert "limit-gated (retryable)" in result.error, "must be distinguishable from hard failures"
        assert str(10**17) in result.error, "must name the currently available amount"
        assert "time/utilization-gated" in result.error
        # DISTINCT from the CF-aware hard failure — strategies branch on this.
        assert "Repay debt" not in result.error
        assert "collateral factor" not in result.error
        sdk.build_operate_tx.assert_not_called()


# =============================================================================
# D3.F6 — L3 == L5 position-key byte identity (VIB-4981 regression class)
# =============================================================================


class TestFluidVaultPositionKeyParity:
    def test_fluid_vault_position_key_byte_identity_l3_l5(self):
        # Mirrors tests/framework/accounting/test_lending_close_net_pnl_vib4977.py:
        # the L3 writer (lending_position_id) and the L5 valuer
        # (_derive_position_key) must produce byte-identical strings, with the
        # vault market segment inserted AND lowercased by BOTH sides.
        from almanak.framework.accounting.lending_accounting import _derive_position_key
        from almanak.framework.observability.position_events import lending_position_id

        chain, protocol, asset = "arbitrum", "fluid_vault", "USDC"
        wallet = "0xAbCdEf2222222222222222222222222222222222"
        mixed_case_vault = "0xeAbBfca72F8a8bf14C4ac59e69ECB2eB69F0811C"

        l3_position_id = lending_position_id(
            chain=chain, protocol=protocol, wallet=wallet, asset=asset, market_id=mixed_case_vault
        )
        l5_position_key = _derive_position_key(protocol, chain, wallet, mixed_case_vault, asset)
        assert l3_position_id == l5_position_key, (
            f"writer/valuer key mismatch: {l3_position_id!r} != {l5_position_key!r}"
        )
        assert l3_position_id == f"lending:arbitrum:fluid_vault:{wallet.lower()}:{ARB_VAULT}:usdc"

    def test_fluid_vault_position_key_market_segment_present_both_sides(self):
        # A side that DROPS the market segment is the silent-join-miss class.
        from almanak.framework.accounting.lending_accounting import _derive_position_key
        from almanak.framework.observability.position_events import lending_position_id

        l3_key = lending_position_id(
            chain="arbitrum", protocol="fluid_vault", wallet=WALLET, asset="ETH", market_id=ARB_VAULT
        )
        l5_key = _derive_position_key("fluid_vault", "arbitrum", WALLET, ARB_VAULT, "ETH")
        assert l3_key.split(":") == ["lending", "arbitrum", "fluid_vault", WALLET, ARB_VAULT, "eth"]
        assert l5_key.split(":") == ["lending", "arbitrum", "fluid_vault", WALLET, ARB_VAULT, "eth"]

    def test_fluid_vault_collateral_and_debt_legs_key_to_distinct_position_keys(self):
        # LENDING_COLLATERAL vs LENDING_DEBT split: the asset leg differs.
        from almanak.framework.observability.position_events import lending_position_id

        collateral_key = lending_position_id(
            chain="arbitrum", protocol="fluid_vault", wallet=WALLET, asset="ETH", market_id=ARB_VAULT
        )
        debt_key = lending_position_id(
            chain="arbitrum", protocol="fluid_vault", wallet=WALLET, asset="USDC", market_id=ARB_VAULT
        )
        assert collateral_key != debt_key
        assert collateral_key.endswith(":eth")
        assert debt_key.endswith(":usdc")


# =============================================================================
# Permission-discovery mode — calldata shape only, no on-chain reads
# =============================================================================


class TestDiscoveryMode:
    @pytest.mark.parametrize(
        "intent_factory",
        [_supply_intent, _borrow_intent, _repay_intent, _withdraw_intent],
    )
    def test_discovery_compiles_shape_without_reads(self, intent_factory):
        sdk = _vault_sdk(resolve_exc=FluidSDKError("must not be called in discovery"))
        sdk.position_by_nft_id.side_effect = FluidSDKError("must not be called in discovery")
        sdk.get_vault_entire_data.side_effect = FluidSDKError("must not be called in discovery")
        result, _ = _compile(intent_factory(), sdk=sdk, permission_discovery=True)
        assert result.status is CompilationStatus.SUCCESS, result.error
        assert _operate_tx(result).data.startswith(OPERATE_SELECTOR)
