"""Compiler tests for Intent.collect_fees on Aerodrome (issue #2071).

Covers two protocol surfaces:
- ``aerodrome`` (Classic V1, fungible LP tokens): standalone fee collection is
  not representable on-chain (Solidly-fork pools auto-compound trading fees
  into reserves), so the compiler must surface a clear, actionable error
  pointing at ``LP_CLOSE(collect_fees=True)``.
- ``aerodrome_slipstream`` (CL, NFT positions): supports
  ``NonfungiblePositionManager.collect()`` for in-position fee harvest.
"""

from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.intents.compiler import (
    CompilationStatus,
    IntentCompiler,
    IntentCompilerConfig,
)
from almanak.framework.intents.vocabulary import Intent


def _make_compiler(chain: str = "base") -> IntentCompiler:
    compiler = IntentCompiler.__new__(IntentCompiler)
    compiler.chain = chain
    compiler.wallet_address = "0x" + "11" * 20
    compiler.price_oracle = {}
    compiler._gateway_client = None
    return compiler


# ---------------------------------------------------------------------------
# Aerodrome Classic: explicit-unsupported path (Option B in issue #2071)
# ---------------------------------------------------------------------------


def test_aerodrome_classic_collect_fees_returns_clear_unsupported_error() -> None:
    """Classic Aerodrome must fail compilation with a clear, actionable message
    pointing the caller at ``LP_CLOSE(collect_fees=True)``."""
    compiler = _make_compiler()
    intent = Intent.collect_fees(
        pool="USDC/DAI/stable",
        protocol="aerodrome",
        chain="base",
    )

    result = compiler._compile_collect_fees(intent)

    assert result.status == CompilationStatus.FAILED
    assert result.action_bundle is None
    assert result.error is not None
    msg = result.error.lower()
    assert "aerodrome classic" in msg
    assert "lp_close" in msg and "collect_fees=true" in msg
    assert "aerodrome_slipstream" in msg


def test_aerodrome_classic_collect_fees_error_resolves_velodrome_alias() -> None:
    """Velodrome on Optimism normalizes to ``aerodrome``; same Classic limitation
    must surface the same error rather than the generic catch-all."""
    compiler = _make_compiler(chain="optimism")
    intent = Intent.collect_fees(
        pool="USDC/DAI/stable",
        protocol="velodrome",
        chain="optimism",
    )

    result = compiler._compile_collect_fees(intent)

    assert result.status == CompilationStatus.FAILED
    assert result.error is not None
    assert "aerodrome classic" in result.error.lower()


# ---------------------------------------------------------------------------
# Aerodrome Slipstream: supported path (Option A in issue #2071)
# ---------------------------------------------------------------------------


def _make_collect_tx_mock(*, gas_estimate: int = 120_000):
    tx = MagicMock()
    tx.gas_estimate = gas_estimate
    tx.tx_type = "lp_collect_fees"
    tx.to = "0x" + "dd" * 20
    tx.to_dict.return_value = {
        "tx_type": "lp_collect_fees",
        "to": tx.to,
        "gas_estimate": gas_estimate,
    }
    return tx


def test_aerodrome_slipstream_collect_fees_compiles_success() -> None:
    """Slipstream collect_fees should produce a single ``collect`` transaction
    bundle when given a valid NFT tokenId via protocol_params."""
    compiler = _make_compiler()
    intent = Intent.collect_fees(
        pool="WETH/USDC/200",
        protocol="aerodrome_slipstream",
        chain="base",
        protocol_params={"position_id": "12345"},
    )
    collect_tx = _make_collect_tx_mock(gas_estimate=120_000)
    collect_result = MagicMock(success=True, transactions=[collect_tx], error=None)

    with (
        patch.object(compiler, "_get_chain_rpc_url", return_value="http://localhost:8545"),
        patch("almanak.framework.connectors.aerodrome.AerodromeAdapter") as mock_adapter_cls,
    ):
        mock_adapter = mock_adapter_cls.return_value
        mock_adapter.collect_cl_fees.return_value = collect_result

        result = compiler._compile_collect_fees(intent)

    assert result.status == CompilationStatus.SUCCESS, result.error
    assert result.error is None
    assert result.action_bundle is not None
    assert result.action_bundle.intent_type == "LP_COLLECT_FEES"
    assert result.action_bundle.metadata["protocol"] == "aerodrome_slipstream"
    assert result.action_bundle.metadata["position_id"] == "12345"
    assert result.action_bundle.metadata["token_id"] == 12345
    assert len(result.transactions) == 1
    assert result.total_gas_estimate == 120_000

    # Confirm the adapter was called with the parsed tokenId, not the raw string,
    # and the recipient defaults to the wallet.
    mock_adapter.collect_cl_fees.assert_called_once_with(
        token_id=12345,
        recipient=compiler.wallet_address,
    )


@pytest.mark.parametrize(
    "raw_protocol",
    ["aerodrome_slipstream", "Aerodrome_Slipstream", "AERODROME_SLIPSTREAM", "aerodrome-slipstream"],
)
def test_aerodrome_slipstream_collect_fees_dispatch_is_case_and_separator_insensitive(
    raw_protocol: str,
) -> None:
    """Mirror ``normalize_protocol``'s case + hyphen normalization on the
    pre-resolution dispatch so callers passing mixed-case or hyphenated
    protocol strings aren't silently routed into the unsupported branch."""
    compiler = _make_compiler()
    intent = Intent.collect_fees(
        pool="WETH/USDC/200",
        protocol=raw_protocol,
        chain="base",
        protocol_params={"position_id": "12345"},
    )
    collect_tx = _make_collect_tx_mock()
    collect_result = MagicMock(success=True, transactions=[collect_tx], error=None)

    with (
        patch.object(compiler, "_get_chain_rpc_url", return_value="http://localhost:8545"),
        patch("almanak.framework.connectors.aerodrome.AerodromeAdapter") as mock_adapter_cls,
    ):
        mock_adapter = mock_adapter_cls.return_value
        mock_adapter.collect_cl_fees.return_value = collect_result
        result = compiler._compile_collect_fees(intent)

    assert result.status == CompilationStatus.SUCCESS, result.error
    assert result.action_bundle is not None
    assert result.action_bundle.metadata["protocol"] == "aerodrome_slipstream"


def test_aerodrome_slipstream_collect_fees_missing_position_id_fails() -> None:
    compiler = _make_compiler()
    intent = Intent.collect_fees(
        pool="WETH/USDC/200",
        protocol="aerodrome_slipstream",
        chain="base",
    )

    result = compiler._compile_collect_fees(intent)

    assert result.status == CompilationStatus.FAILED
    assert result.error is not None
    assert "position_id" in result.error
    assert "protocol_params" in result.error


@pytest.mark.parametrize(
    "bad_position_id",
    [
        # String inputs that aren't integer literals.
        "WETH/USDC/200",
        "0xabc",
        "not-a-number",
        # Numeric coercion footguns that ``int(...)`` would silently swallow:
        # ``int(1.9) == 1`` and ``int(True) == 1`` would build a tx for the
        # wrong NFT. Coercing through ``str(...)`` rejects both.
        "1.9",
        "1e5",
        1.9,
        -2.3,
        True,
        False,
    ],
)
def test_aerodrome_slipstream_collect_fees_non_numeric_position_id_fails(bad_position_id: object) -> None:
    compiler = _make_compiler()
    intent = Intent.collect_fees(
        pool="WETH/USDC/200",
        protocol="aerodrome_slipstream",
        chain="base",
        protocol_params={"position_id": bad_position_id},
    )

    result = compiler._compile_collect_fees(intent)

    assert result.status == CompilationStatus.FAILED
    assert result.error is not None
    assert str(bad_position_id) in result.error
    assert "numeric NFT tokenId" in result.error


@pytest.mark.parametrize(
    "good_position_id, expected_token_id",
    [
        ("12345", 12345),
        (" 12345 ", 12345),  # whitespace tolerated
        (12345, 12345),  # plain int still accepted
    ],
)
def test_aerodrome_slipstream_collect_fees_accepts_clean_integer_inputs(
    good_position_id: object, expected_token_id: int
) -> None:
    """Confirm the strict-string parsing still accepts the legitimate input
    shapes (decimal-literal strings, possibly whitespace-padded, and plain
    ``int`` values) and threads the parsed numeric tokenId to the adapter."""
    compiler = _make_compiler()
    intent = Intent.collect_fees(
        pool="WETH/USDC/200",
        protocol="aerodrome_slipstream",
        chain="base",
        protocol_params={"position_id": good_position_id},
    )
    collect_tx = _make_collect_tx_mock()
    collect_result = MagicMock(success=True, transactions=[collect_tx], error=None)

    with (
        patch.object(compiler, "_get_chain_rpc_url", return_value="http://localhost:8545"),
        patch("almanak.framework.connectors.aerodrome.AerodromeAdapter") as mock_adapter_cls,
    ):
        mock_adapter = mock_adapter_cls.return_value
        mock_adapter.collect_cl_fees.return_value = collect_result
        result = compiler._compile_collect_fees(intent)

    assert result.status == CompilationStatus.SUCCESS, result.error
    mock_adapter.collect_cl_fees.assert_called_once()
    assert mock_adapter.collect_cl_fees.call_args.kwargs["token_id"] == expected_token_id


def test_aerodrome_slipstream_collect_fees_unsupported_chain_fails() -> None:
    """Slipstream is currently base-only. Other chains must fail loudly so
    callers don't silently end up with an empty bundle. The error reads the
    supported set from LP_POSITION_MANAGERS rather than hardcoding it so it
    stays accurate as new chains light up."""
    compiler = _make_compiler(chain="optimism")
    intent = Intent.collect_fees(
        pool="WETH/USDC/200",
        protocol="aerodrome_slipstream",
        chain="optimism",
        protocol_params={"position_id": "12345"},
    )

    result = compiler._compile_collect_fees(intent)

    assert result.status == CompilationStatus.FAILED
    assert result.error is not None
    assert "Aerodrome Slipstream CL not supported" in result.error
    assert "optimism" in result.error
    # Supported list reflects what LP_POSITION_MANAGERS actually contains, so
    # base must be present (Slipstream's only deployment as of this PR) and
    # optimism (the unsupported chain we passed) must NOT appear.
    assert "base" in result.error
    assert "['optimism']" not in result.error


@pytest.mark.parametrize("bad_position_id", ["-1", "-12345", "0"])
def test_aerodrome_slipstream_collect_fees_non_positive_position_id_fails(
    bad_position_id: str,
) -> None:
    """Non-positive tokenIds revert in NonfungiblePositionManager.collect();
    fail at compile time so the strategy doesn't pay gas for a doomed tx."""
    compiler = _make_compiler()
    intent = Intent.collect_fees(
        pool="WETH/USDC/200",
        protocol="aerodrome_slipstream",
        chain="base",
        protocol_params={"position_id": bad_position_id},
    )

    result = compiler._compile_collect_fees(intent)

    assert result.status == CompilationStatus.FAILED
    assert result.error is not None
    assert bad_position_id in result.error
    assert "positive NFT tokenId" in result.error


def test_aerodrome_slipstream_collect_fees_propagates_adapter_error() -> None:
    compiler = _make_compiler()
    intent = Intent.collect_fees(
        pool="WETH/USDC/200",
        protocol="aerodrome_slipstream",
        chain="base",
        protocol_params={"position_id": "12345"},
    )

    with (
        patch.object(compiler, "_get_chain_rpc_url", return_value="http://localhost:8545"),
        patch("almanak.framework.connectors.aerodrome.AerodromeAdapter") as mock_adapter_cls,
    ):
        mock_adapter = mock_adapter_cls.return_value
        mock_adapter.collect_cl_fees.return_value = MagicMock(
            success=False,
            transactions=[],
            error="rpc unavailable for collect tx build",
        )

        result = compiler._compile_collect_fees(intent)

    assert result.status == CompilationStatus.FAILED
    assert result.error is not None
    assert "Failed to build CL collect TX" in result.error
    assert "rpc unavailable for collect tx build" in result.error


def test_aerodrome_slipstream_collect_fees_permission_discovery_substitutes_token_id() -> None:
    """Under ``permission_discovery=True`` a tokenId of 0 is invalid for
    ``positions(tokenId)`` calls, so the compiler substitutes a synthetic 1
    to keep manifest discovery working without an on-chain NFT.

    The user's original position_id input is preserved verbatim in the
    bundle metadata under ``position_id`` while the synthetic substitute
    flows through the on-chain ``token_id`` field — matching the LP_CLOSE
    Slipstream metadata convention."""
    compiler = _make_compiler()
    compiler._config = IntentCompilerConfig(
        allow_placeholder_prices=True,
        permission_discovery=True,
    )
    intent = Intent.collect_fees(
        pool="WETH/USDC/200",
        protocol="aerodrome_slipstream",
        chain="base",
        protocol_params={"position_id": "0"},
    )
    collect_tx = _make_collect_tx_mock()
    collect_result = MagicMock(success=True, transactions=[collect_tx], error=None)

    with (
        patch.object(compiler, "_get_chain_rpc_url", return_value="http://localhost:8545"),
        patch("almanak.framework.connectors.aerodrome.AerodromeAdapter") as mock_adapter_cls,
    ):
        mock_adapter = mock_adapter_cls.return_value
        mock_adapter.collect_cl_fees.return_value = collect_result

        result = compiler._compile_collect_fees(intent)

    assert result.status == CompilationStatus.SUCCESS
    mock_adapter.collect_cl_fees.assert_called_once()
    kwargs = mock_adapter.collect_cl_fees.call_args.kwargs
    assert kwargs["token_id"] == 1
    # Original user input is preserved verbatim; synthetic substitute lives
    # in the numeric on-chain token_id slot only.
    assert result.action_bundle is not None
    assert result.action_bundle.metadata["position_id"] == "0"
    assert result.action_bundle.metadata["token_id"] == 1


# ---------------------------------------------------------------------------
# Generic catch-all error: list now includes aerodrome_slipstream as supported
# ---------------------------------------------------------------------------


def test_collect_fees_unsupported_protocol_lists_slipstream_as_supported() -> None:
    compiler = _make_compiler()
    intent = Intent.collect_fees(
        pool="WETH/USDC/3000",
        protocol="some_unknown_protocol",
        chain="base",
    )

    result = compiler._compile_collect_fees(intent)

    assert result.status == CompilationStatus.FAILED
    assert result.error is not None
    assert "aerodrome_slipstream" in result.error
    assert "traderjoe_v2" in result.error
    assert "uniswap_v4" in result.error


# ---------------------------------------------------------------------------
# Receipt parser: extract_fees0 / extract_fees1 read the Slipstream Collect event
# ---------------------------------------------------------------------------


def _make_collect_log(amount0: int, amount1: int) -> dict[str, object]:
    """Build a Slipstream CL Collect log matching the parser's expected layout.

    Topic 0: Collect event signature
    Topic 1: tokenId (indexed)
    Data:   recipient(32b) + amount0(32b) + amount1(32b)
    """
    from almanak.framework.connectors.aerodrome.receipt_parser import EVENT_TOPICS

    recipient = "0x" + "11" * 20
    data = (
        "0x"
        + recipient[2:].rjust(64, "0")
        + format(amount0, "064x")
        + format(amount1, "064x")
    )
    return {
        "topics": [EVENT_TOPICS["CollectCL"], "0x" + format(12345, "064x")],
        "data": data,
        "address": "0x" + "ee" * 20,
    }


def test_slipstream_parser_extract_fees0_reads_collect_event() -> None:
    from almanak.framework.connectors.aerodrome.receipt_parser import (
        AerodromeSlipstreamReceiptParser,
    )

    parser = AerodromeSlipstreamReceiptParser()
    receipt = {"logs": [_make_collect_log(1_000_000, 2_500_000_000_000_000)]}

    assert parser.extract_fees0(receipt) == 1_000_000
    assert parser.extract_fees1(receipt) == 2_500_000_000_000_000


def test_slipstream_parser_extract_fees_returns_none_when_no_collect_event() -> None:
    from almanak.framework.connectors.aerodrome.receipt_parser import (
        AerodromeSlipstreamReceiptParser,
    )

    parser = AerodromeSlipstreamReceiptParser()
    # An IncreaseLiquidity-only receipt — no Collect event present.
    receipt = {
        "logs": [
            {
                "topics": [
                    "0x0000000000000000000000000000000000000000000000000000000000000000",
                ],
                "data": "0x",
            }
        ]
    }

    assert parser.extract_fees0(receipt) is None
    assert parser.extract_fees1(receipt) is None


def test_slipstream_parser_extract_fees_sums_multiple_collect_events() -> None:
    from almanak.framework.connectors.aerodrome.receipt_parser import (
        AerodromeSlipstreamReceiptParser,
    )

    parser = AerodromeSlipstreamReceiptParser()
    receipt = {
        "logs": [
            _make_collect_log(100, 200),
            _make_collect_log(300, 400),
        ],
    }

    assert parser.extract_fees0(receipt) == 400
    assert parser.extract_fees1(receipt) == 600


def _make_decrease_liquidity_log(token_id: int = 12345) -> dict[str, object]:
    """Build a Slipstream DecreaseLiquidity log so we can simulate an
    LP_CLOSE bundle's first receipt where the parser must NOT report
    fees0/fees1 (the paired Collect amounts include unlocked principal)."""
    from almanak.framework.connectors.aerodrome.receipt_parser import EVENT_TOPICS

    # data layout: liquidity(uint128) + amount0(uint256) + amount1(uint256)
    data = "0x" + format(1_000_000, "064x") + format(50_000, "064x") + format(75_000, "064x")
    return {
        "topics": [EVENT_TOPICS["DecreaseLiquidity"], "0x" + format(token_id, "064x")],
        "data": data,
        "address": "0x" + "ee" * 20,
    }


def test_slipstream_parser_fees_extraction_returns_none_on_lp_close_bundle() -> None:
    """An LP_CLOSE bundle's second receipt carries both DecreaseLiquidity AND
    Collect events; the Collect amounts include unlocked principal, so
    ``extract_fees0``/``extract_fees1`` must NOT report them as fees.
    Returning None preserves the LP_CLOSE typed data path (which reports the
    same numbers as principal via ``lp_close_data.amount0_collected``) without
    double-counting them as standalone fees."""
    from almanak.framework.connectors.aerodrome.receipt_parser import (
        AerodromeSlipstreamReceiptParser,
    )

    parser = AerodromeSlipstreamReceiptParser()
    receipt = {
        "logs": [
            _make_decrease_liquidity_log(token_id=12345),
            _make_collect_log(50_000, 75_000),
        ],
    }

    assert parser.extract_fees0(receipt) is None
    assert parser.extract_fees1(receipt) is None


def test_slipstream_parser_fees_extraction_ignores_decrease_liquidity_only_receipt() -> None:
    """A receipt with DecreaseLiquidity but no Collect (e.g. the first
    Slipstream LP_CLOSE tx receipt before the collect leg) must report no
    fees rather than zeroing them out."""
    from almanak.framework.connectors.aerodrome.receipt_parser import (
        AerodromeSlipstreamReceiptParser,
    )

    parser = AerodromeSlipstreamReceiptParser()
    receipt = {"logs": [_make_decrease_liquidity_log(token_id=12345)]}

    assert parser.extract_fees0(receipt) is None
    assert parser.extract_fees1(receipt) is None
