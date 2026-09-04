from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors.uniswap_v4.compiler import UniswapV4Compiler
from almanak.framework.intents.compiler_models import CompilationStatus, TransactionData
from almanak.framework.intents.vocabulary import CollectFeesIntent, LPCloseIntent, LPOpenIntent
from almanak.framework.models.reproduction_bundle import ActionBundle

WALLET = "0x" + "11" * 20
CURRENCY0 = "0x" + "22" * 20
CURRENCY1 = "0x" + "ee" * 20


def _ctx() -> SimpleNamespace:
    return SimpleNamespace(price_oracle={"ETH": Decimal("2000")}, rpc_url="http://localhost:8545")


def _tx(description: str = "LP action") -> dict[str, object]:
    return {
        "to": WALLET,
        "value": "0x2a",
        "data": "0x1234",
        "gas_estimate": 123_456,
        "description": description,
    }


def _bundle(*transactions: dict[str, object], **metadata: object) -> ActionBundle:
    return ActionBundle(intent_type="test", transactions=list(transactions), metadata=metadata)


def _open_intent() -> LPOpenIntent:
    return LPOpenIntent(
        pool="WETH/USDC/3000",
        amount0=Decimal("1"),
        amount1=Decimal("2000"),
        range_lower=Decimal("1500"),
        range_upper=Decimal("2500"),
        protocol="uniswap_v4",
        intent_id="open-1",
    )


def _close_intent(protocol_params: dict[str, object] | None = None) -> LPCloseIntent:
    return LPCloseIntent(
        position_id="17",
        protocol="uniswap_v4",
        protocol_params=protocol_params,
        intent_id="close-1",
    )


def _collect_intent(protocol_params: dict[str, object] | None = None) -> CollectFeesIntent:
    return CollectFeesIntent(
        pool="WETH/USDC/3000",
        protocol="uniswap_v4",
        protocol_params=protocol_params,
        intent_id="collect-1",
    )


@pytest.mark.parametrize("warnings", [[], ["hook warning"]], ids=["without-warnings", "with-warnings"])
def test_lp_open_preserves_transaction_types_gas_and_warnings(warnings: list[str]) -> None:
    compiler = UniswapV4Compiler()
    ctx = _ctx()
    intent = _open_intent()
    adapter = MagicMock()
    bundle = _bundle(
        _tx("Approve token"),
        _tx("Mint position"),
        gas_estimate=246_912,
        warnings=warnings,
    )
    adapter.compile_lp_open_intent.return_value = bundle

    with patch.object(compiler, "_adapter", return_value=adapter):
        result = compiler.compile_lp_open(ctx, intent)

    assert result.status is CompilationStatus.SUCCESS
    assert result.action_bundle is bundle
    assert result.transactions == [
        TransactionData(WALLET, 42, "0x1234", 123_456, "Approve token", "approve"),
        TransactionData(WALLET, 42, "0x1234", 123_456, "Mint position", "lp_mint"),
    ]
    assert result.total_gas_estimate == 246_912
    assert result.warnings == warnings
    adapter.compile_lp_open_intent.assert_called_once_with(intent, ctx.price_oracle)


@pytest.mark.parametrize(
    ("metadata", "expected_error"),
    [
        ({"error": "mint rejected"}, "mint rejected"),
        ({}, "Unknown error during V4 LP_OPEN compilation"),
    ],
)
def test_lp_open_preserves_empty_bundle_error(metadata: dict[str, object], expected_error: str) -> None:
    compiler = UniswapV4Compiler()
    adapter = MagicMock()
    adapter.compile_lp_open_intent.return_value = ActionBundle("LP_OPEN", [], metadata)

    with patch.object(compiler, "_adapter", return_value=adapter):
        result = compiler.compile_lp_open(_ctx(), _open_intent())

    assert result.status is CompilationStatus.FAILED
    assert result.error == expected_error
    assert result.action_bundle is None


def test_lp_open_contains_adapter_exception() -> None:
    compiler = UniswapV4Compiler()
    with patch.object(compiler, "_adapter", side_effect=RuntimeError("adapter exploded")):
        result = compiler.compile_lp_open(_ctx(), _open_intent())

    assert result.status is CompilationStatus.FAILED
    assert result.error == "adapter exploded"


def test_lp_close_rejects_chained_nft_amount_before_adapter_creation() -> None:
    compiler = UniswapV4Compiler()
    intent = LPCloseIntent(position_id="pending", amount="all", protocol="uniswap_v4", intent_id="close-1")

    with patch.object(compiler, "_adapter") as adapter:
        result = compiler.compile_lp_close(_ctx(), intent)

    assert result.status is CompilationStatus.FAILED
    assert result.error == (
        "LP_CLOSE amount='all' chaining is not supported for uniswap_v4: "
        "position_id is a position identity (NFT token-id), not a fungible amount"
    )
    adapter.assert_not_called()


@pytest.mark.parametrize("warnings", [[], ["unprotected withdrawal"]], ids=["without-warnings", "with-warnings"])
def test_lp_close_preserves_canonical_order_transaction_gas_and_warnings(warnings: list[str]) -> None:
    compiler = UniswapV4Compiler()
    adapter = MagicMock()
    bundle = _bundle(_tx("Close position"), gas_estimate=123_456, warnings=warnings)
    adapter.compile_lp_close_intent.return_value = bundle
    intent = _close_intent({"liquidity": "99", "currency0": CURRENCY1, "currency1": CURRENCY0})

    with patch.object(compiler, "_adapter", return_value=adapter):
        result = compiler.compile_lp_close(_ctx(), intent)

    assert result.status is CompilationStatus.SUCCESS
    assert result.action_bundle is bundle
    assert result.transactions == [TransactionData(WALLET, 42, "0x1234", 123_456, "Close position", "lp_close")]
    assert result.total_gas_estimate == 123_456
    assert result.warnings == warnings
    adapter.get_position_liquidity.assert_not_called()
    adapter.compile_lp_close_intent.assert_called_once_with(
        intent,
        liquidity=99,
        currency0=CURRENCY0,
        currency1=CURRENCY1,
    )


@pytest.mark.parametrize("position_id", ["not-numeric", None], ids=["value-error", "type-error"])
def test_lp_close_rejects_invalid_position_id_when_liquidity_must_be_queried(position_id: object) -> None:
    compiler = UniswapV4Compiler()
    intent = SimpleNamespace(
        intent_id="close-1",
        position_id=position_id,
        protocol="uniswap_v4",
        protocol_params={"currency0": CURRENCY0, "currency1": CURRENCY1},
        pool=None,
        is_chained_amount=False,
    )

    with patch.object(compiler, "_adapter", return_value=MagicMock()):
        result = compiler.compile_lp_close(_ctx(), intent)

    assert result.status is CompilationStatus.FAILED
    assert result.error == f"V4 LP_CLOSE: invalid position_id '{position_id}' (must be numeric)"


def test_lp_close_preserves_liquidity_lookup_failure() -> None:
    compiler = UniswapV4Compiler()
    adapter = MagicMock()
    adapter.get_position_liquidity.side_effect = RuntimeError("rpc down")
    intent = _close_intent({"currency0": CURRENCY0, "currency1": CURRENCY1})

    with patch.object(compiler, "_adapter", return_value=adapter):
        result = compiler.compile_lp_close(_ctx(), intent)

    assert result.status is CompilationStatus.FAILED
    assert result.error == (
        "V4 LP_CLOSE: could not determine position liquidity. "
        "Either provide 'liquidity' in protocol_params or ensure RPC is available. Error: rpc down"
    )


def test_lp_close_rejects_zero_onchain_liquidity() -> None:
    compiler = UniswapV4Compiler()
    adapter = MagicMock()
    adapter.get_position_liquidity.return_value = 0
    intent = _close_intent({"currency0": CURRENCY0, "currency1": CURRENCY1})

    with patch.object(compiler, "_adapter", return_value=adapter):
        result = compiler.compile_lp_close(_ctx(), intent)

    assert result.status is CompilationStatus.FAILED
    assert result.error == (
        "V4 LP_CLOSE: position 17 has zero liquidity on-chain. "
        "Provide 'liquidity' in protocol_params or ensure the position exists with liquidity > 0."
    )


def test_lp_close_rejects_unresolved_currencies() -> None:
    compiler = UniswapV4Compiler()
    adapter = MagicMock()
    adapter.get_position_currencies.return_value = ("", "")

    with patch.object(compiler, "_adapter", return_value=adapter):
        result = compiler.compile_lp_close(_ctx(), _close_intent({"liquidity": 1}))

    assert result.status is CompilationStatus.FAILED
    assert result.error == (
        "V4 LP_CLOSE requires 'currency0' and 'currency1' in protocol_params "
        "or a resolvable 'pool' string (e.g. 'WETH/USDC/3000')."
    )


@pytest.mark.parametrize(
    ("metadata", "expected_error"),
    [
        ({"error": "close rejected"}, "close rejected"),
        ({}, "Unknown error during V4 LP_CLOSE compilation"),
    ],
)
def test_lp_close_preserves_empty_bundle_error(metadata: dict[str, object], expected_error: str) -> None:
    compiler = UniswapV4Compiler()
    adapter = MagicMock()
    adapter.compile_lp_close_intent.return_value = ActionBundle("LP_CLOSE", [], metadata)
    intent = _close_intent({"liquidity": 1, "currency0": CURRENCY0, "currency1": CURRENCY1})

    with patch.object(compiler, "_adapter", return_value=adapter):
        result = compiler.compile_lp_close(_ctx(), intent)

    assert result.status is CompilationStatus.FAILED
    assert result.error == expected_error
    assert result.action_bundle is None


def test_lp_close_contains_adapter_exception() -> None:
    compiler = UniswapV4Compiler()
    with patch.object(compiler, "_adapter", side_effect=RuntimeError("adapter exploded")):
        result = compiler.compile_lp_close(_ctx(), _close_intent())

    assert result.status is CompilationStatus.FAILED
    assert result.error == "adapter exploded"


def test_collect_fees_requires_position_id() -> None:
    compiler = UniswapV4Compiler()
    with patch.object(compiler, "_adapter", return_value=MagicMock()):
        result = compiler.compile_collect_fees(_ctx(), _collect_intent())

    assert result.status is CompilationStatus.FAILED
    assert result.error == "V4 LP_COLLECT_FEES requires 'position_id' in protocol_params."


def test_collect_fees_requires_resolvable_currencies() -> None:
    compiler = UniswapV4Compiler()
    with (
        patch.object(compiler, "_adapter", return_value=MagicMock()),
        patch.object(compiler, "_resolve_pool_currencies", return_value=("", "")),
    ):
        result = compiler.compile_collect_fees(_ctx(), _collect_intent({"position_id": 17}))

    assert result.status is CompilationStatus.FAILED
    assert result.error == (
        "V4 LP_COLLECT_FEES requires 'currency0' and 'currency1' in protocol_params "
        "or a resolvable 'pool' string (e.g. 'WETH/USDC/3000')."
    )


@pytest.mark.parametrize("warnings", [[], ["hook warning"]], ids=["without-warnings", "with-warnings"])
def test_collect_fees_preserves_identity_hook_data_transaction_and_warnings(warnings: list[str]) -> None:
    compiler = UniswapV4Compiler()
    adapter = MagicMock()
    bundle = _bundle(_tx("Collect fees"), gas_estimate=123_456, warnings=warnings)
    adapter.compile_collect_fees_intent.return_value = bundle
    intent = _collect_intent(
        {
            "position_id": "17",
            "currency0": CURRENCY1,
            "currency1": CURRENCY0,
            "hook_data": "0x1234",
        }
    )

    with patch.object(compiler, "_adapter", return_value=adapter):
        result = compiler.compile_collect_fees(_ctx(), intent)

    assert result.status is CompilationStatus.SUCCESS
    assert result.action_bundle is bundle
    assert result.transactions == [TransactionData(WALLET, 42, "0x1234", 123_456, "Collect fees", "lp_collect_fees")]
    assert result.total_gas_estimate == 123_456
    assert result.warnings == warnings
    adapter.compile_collect_fees_intent.assert_called_once_with(
        position_id=17,
        currency0=CURRENCY0,
        currency1=CURRENCY1,
        hook_data=b"\x12\x34",
    )


def test_collect_fees_falls_back_to_intent_position_and_pool_currencies() -> None:
    compiler = UniswapV4Compiler()
    adapter = MagicMock()
    adapter.compile_collect_fees_intent.return_value = _bundle(_tx())
    intent = SimpleNamespace(
        intent_id="collect-1",
        position_id="23",
        protocol_params={},
        pool="WETH/USDC/3000",
    )

    with (
        patch.object(compiler, "_adapter", return_value=adapter),
        patch.object(compiler, "_resolve_pool_currencies", return_value=(CURRENCY0, CURRENCY1)) as resolve,
    ):
        result = compiler.compile_collect_fees(_ctx(), intent)

    assert result.status is CompilationStatus.SUCCESS
    resolve.assert_called_once_with(adapter, intent.pool, "", "")
    adapter.compile_collect_fees_intent.assert_called_once_with(
        position_id=23,
        currency0=CURRENCY0,
        currency1=CURRENCY1,
        hook_data=b"",
    )


@pytest.mark.parametrize(
    ("metadata", "expected_error"),
    [
        ({"error": "collection rejected"}, "collection rejected"),
        ({}, "Unknown error during V4 LP_COLLECT_FEES compilation"),
    ],
)
def test_collect_fees_preserves_empty_bundle_error(metadata: dict[str, object], expected_error: str) -> None:
    compiler = UniswapV4Compiler()
    adapter = MagicMock()
    adapter.compile_collect_fees_intent.return_value = ActionBundle("LP_COLLECT_FEES", [], metadata)
    intent = _collect_intent({"position_id": 17, "currency0": CURRENCY0, "currency1": CURRENCY1})

    with patch.object(compiler, "_adapter", return_value=adapter):
        result = compiler.compile_collect_fees(_ctx(), intent)

    assert result.status is CompilationStatus.FAILED
    assert result.error == expected_error
    assert result.action_bundle is None


def test_collect_fees_contains_invalid_hook_data() -> None:
    compiler = UniswapV4Compiler()
    intent = _collect_intent(
        {"position_id": 17, "currency0": CURRENCY0, "currency1": CURRENCY1, "hook_data": "not-hex"}
    )

    with patch.object(compiler, "_adapter", return_value=MagicMock()):
        result = compiler.compile_collect_fees(_ctx(), intent)

    assert result.status is CompilationStatus.FAILED
    assert "non-hexadecimal number" in (result.error or "")


@pytest.mark.parametrize(
    ("currency0", "currency1", "expected"),
    [
        ("", "", (CURRENCY0, CURRENCY1)),
        (CURRENCY0, "", (CURRENCY0, CURRENCY1)),
        ("", CURRENCY1, (CURRENCY0, CURRENCY1)),
    ],
)
def test_resolve_pool_currencies_fills_missing_values_in_canonical_order(
    currency0: str,
    currency1: str,
    expected: tuple[str, str],
) -> None:
    adapter = MagicMock()
    adapter._resolve_token.side_effect = [(CURRENCY1, 6), (CURRENCY0, 18)]

    result = UniswapV4Compiler._resolve_pool_currencies(
        adapter,
        "USDC/WETH/3000",
        currency0,
        currency1,
    )

    assert result == expected
    assert adapter._resolve_token.call_args_list[0].args == ("USDC",)
    assert adapter._resolve_token.call_args_list[0].kwargs == {"for_v4_pool": True}
    assert adapter._resolve_token.call_args_list[1].args == ("WETH",)


def test_resolve_pool_currencies_ignores_pool_without_pair() -> None:
    adapter = MagicMock()

    result = UniswapV4Compiler._resolve_pool_currencies(adapter, "WETH", CURRENCY0, "")

    assert result == (CURRENCY0, "")
    adapter._resolve_token.assert_not_called()


@pytest.mark.parametrize("error", [ValueError("unknown token"), KeyError("missing token")])
def test_resolve_pool_currencies_preserves_values_on_expected_resolution_error(error: Exception) -> None:
    adapter = MagicMock()
    adapter._resolve_token.side_effect = error

    result = UniswapV4Compiler._resolve_pool_currencies(adapter, "WETH/USDC/3000", CURRENCY0, "")

    assert result == (CURRENCY0, "")


def test_resolve_pool_currencies_preserves_values_on_unexpected_resolution_error() -> None:
    adapter = MagicMock()
    adapter._resolve_token.side_effect = RuntimeError("resolver exploded")

    result = UniswapV4Compiler._resolve_pool_currencies(adapter, "WETH/USDC/3000", "", CURRENCY1)

    assert result == ("", CURRENCY1)
