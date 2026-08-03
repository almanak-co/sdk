"""VIB-6301: an empty token extraction must never yield a fabricated oracle.

The VIB-523 mainnet price gate (``_enforce_mainnet_price_gate``) fails closed
for 7 of the 9 ``PRICE_SENSITIVE_INTENT_TYPES`` when no token symbols can be
extracted from the intent, but lets ``LP_CLOSE`` / ``PERP_CLOSE`` through —
correctly, because ``LPCloseIntent`` carries no token fields at all, so an empty
extraction is the *normal* case for an LP close and failing closed there would
strand capital (teardown must never be blocked from reducing on-chain risk).

Before VIB-6301 the bypassed compile ran in **placeholder mode**: the compiler
kept ``IntentCompiler._get_placeholder_prices()``, a hardcoded table its own
docstring calls 40-60% wrong (ETH=$2000, WBTC=$45000, unknown symbol=$1). These
tests pin the fix: the close still compiles, but with a **real-but-empty**
oracle — ``update_prices({})`` — so an unpriceable symbol raises instead of
silently pricing at a made-up number.

Two properties are load-bearing and asserted directly against a live
``IntentCompiler`` driven through the real gate, not against mocks:

* ``require_token_price("ETH")`` now raises where it used to return $2000;
* ``assert_prices_available(["USDC"])`` now **passes** where placeholder mode
  refused everything. That is a deliberate loosening confined to the bypass
  path: $1 for USDC is a real peg via the known-stablecoin fallback, not a
  fabrication, and ``assert_prices_available``'s own docstring notes a false
  positive there would strand a safe unwind.
"""

import json
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.intents.compiler import (
    CompilationStatus,
    IntentCompiler,
    IntentCompilerConfig,
)
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.execution_service import (
    PRICE_OPTIONAL_CLOSE_INTENT_TYPES,
    PRICE_SENSITIVE_INTENT_TYPES,
    ExecutionServiceServicer,
)

TEST_WALLET = "0x1234567890123456789012345678901234567890"

# Wire-form spellings of every member of PRICE_SENSITIVE_INTENT_TYPES, paired
# with whether the gate is expected to let an EMPTY extraction through.
# 2 bypass, 7 fail closed. Widening the bypass to a third type turns this red.
PRICE_SENSITIVE_MATRIX = [
    ("borrow", False),
    ("lp_close", True),
    ("lp_open", False),
    ("perp_close", True),
    ("perp_open", False),
    ("repay", False),
    ("supply", False),
    ("swap", False),
    ("withdraw", False),
]


def _request(intent_type: str = "lp_close", price_map: dict[str, str] | None = None):
    intent_data = json.dumps({"position_id": "12345"}).encode("utf-8")
    return gateway_pb2.CompileIntentRequest(
        intent_type=intent_type,
        intent_data=intent_data,
        chain="arbitrum",
        wallet_address=TEST_WALLET,
        price_map=price_map or {},
    )


def _success_result():
    bundle = MagicMock()
    bundle.to_dict.return_value = {"intent_type": "lp_close", "transactions": []}
    bundle.sensitive_data = None
    result = MagicMock()
    result.status = CompilationStatus.SUCCESS
    result.action_bundle = bundle
    result.error = None
    return result


def _service(network: str = "mainnet") -> ExecutionServiceServicer:
    service = ExecutionServiceServicer(GatewaySettings(network=network))
    service._ensure_initialized = AsyncMock()
    service._create_intent = MagicMock(return_value=MagicMock())
    return service


def _mock_compiler():
    """A compiler mock in the pre-gate state: placeholder mode, fake oracle."""
    compiler = MagicMock()
    compiler.price_oracle = {"ETH": Decimal("2000")}
    compiler._using_placeholders = True
    compiler.compile.return_value = _success_result()
    return compiler


def test_price_optional_closes_are_a_subset_of_price_sensitive():
    """The bypass list only matters for types the gate actually reaches.

    A member outside PRICE_SENSITIVE_INTENT_TYPES would be dead config: the gate
    is never entered for it (see ``_apply_compile_prices``).
    """
    assert PRICE_OPTIONAL_CLOSE_INTENT_TYPES <= PRICE_SENSITIVE_INTENT_TYPES
    assert PRICE_OPTIONAL_CLOSE_INTENT_TYPES == frozenset({"LPCLOSE", "PERPCLOSE"})


def test_matrix_covers_every_price_sensitive_type():
    """The matrix below is exhaustive — a new price-sensitive type turns it red."""
    service = _service()
    covered = {service._normalize_intent_type(name).upper() for name, _ in PRICE_SENSITIVE_MATRIX}
    assert covered == set(PRICE_SENSITIVE_INTENT_TYPES)


@pytest.mark.asyncio
@pytest.mark.parametrize(("intent_type", "bypasses_gate"), PRICE_SENSITIVE_MATRIX)
async def test_empty_extraction_matrix(intent_type: str, bypasses_gate: bool):
    """Exactly 2 of the 9 price-sensitive types bypass the gate on empty extraction.

    The bypassing pair must compile with an explicitly empty oracle; the other
    seven must refuse with NO_PRICES_AVAILABLE and never reach compile().
    """
    service = _service()
    compiler = _mock_compiler()
    service._get_compiler = MagicMock(return_value=compiler)
    service._extract_token_symbols_from_intent = MagicMock(return_value=[])

    result = await service.CompileIntent(_request(intent_type=intent_type), MagicMock())

    if bypasses_gate:
        assert result.success is True
        compiler.compile.assert_called_once()
        # The whole point of VIB-6301: real-but-empty, never placeholders.
        compiler.update_prices.assert_called_once_with({})
    else:
        assert result.success is False
        assert result.error_code == "NO_PRICES_AVAILABLE"
        compiler.compile.assert_not_called()
        compiler.update_prices.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("intent_type", ["lp_close", "perp_close"])
async def test_bypass_leaves_a_real_but_empty_oracle_not_placeholders(intent_type: str):
    """At compile() time the live compiler is out of placeholder mode.

    Uses a real IntentCompiler so the assertions are about the object the
    connector compilers actually read, not about a mock's recorded calls. State
    is sampled inside compile() because the ``finally`` block restores the
    original oracle before CompileIntent returns.
    """
    compiler = IntentCompiler(
        chain="arbitrum",
        wallet_address=TEST_WALLET,
        config=IntentCompilerConfig(allow_placeholder_prices=True),
    )
    # Pre-condition: this is the fabricated table VIB-6301 removes from the path.
    assert compiler._using_placeholders is True
    assert compiler._get_placeholder_prices()["ETH"] == Decimal("2000")

    seen: dict = {}

    def _spy_compile(intent=None, **kwargs):
        seen["using_placeholders"] = compiler._using_placeholders
        seen["price_oracle"] = compiler.price_oracle
        try:
            seen["eth_price"] = compiler._require_token_price("ETH")
        except ValueError as exc:
            seen["eth_price"] = exc
        try:
            seen["usdc_price"] = compiler._require_token_price("USDC")
        except ValueError as exc:
            seen["usdc_price"] = exc
        return _success_result()

    compiler.compile = _spy_compile  # type: ignore[method-assign]

    service = _service()
    service._get_compiler = MagicMock(return_value=compiler)
    service._extract_token_symbols_from_intent = MagicMock(return_value=[])

    result = await service.CompileIntent(_request(intent_type=intent_type), MagicMock())

    assert result.success is True
    assert seen["using_placeholders"] is False
    assert seen["price_oracle"] == {}
    # ETH used to compile at the fabricated $2000; it must now be unpriceable.
    assert isinstance(seen["eth_price"], ValueError)
    # USDC still prices — a real peg through the known-stablecoin fallback.
    assert seen["usdc_price"] == Decimal("1")

    # The finally-block restore still returns the compiler to its prior state.
    assert compiler._using_placeholders is True


@pytest.mark.asyncio
async def test_bypass_lets_assert_prices_available_pass_for_a_stablecoin():
    """Documented, deliberate loosening: the peg resolves, the fabrication does not.

    Placeholder mode makes ``assert_prices_available`` report EVERY token as
    missing. On the empty-extraction bypass path it now admits real pegs while
    still refusing ETH. Pinned so the loosening cannot widen silently.
    """
    compiler = IntentCompiler(
        chain="arbitrum",
        wallet_address=TEST_WALLET,
        config=IntentCompilerConfig(allow_placeholder_prices=True),
    )
    with pytest.raises(ValueError):
        compiler.assert_prices_available(["USDC"])

    seen: dict = {}

    def _spy_compile(intent=None, **kwargs):
        try:
            compiler.assert_prices_available(["USDC"])
            seen["usdc"] = "passed"
        except ValueError as exc:
            seen["usdc"] = exc
        try:
            compiler.assert_prices_available(["ETH"])
            seen["eth"] = "passed"
        except ValueError as exc:
            seen["eth"] = exc
        return _success_result()

    compiler.compile = _spy_compile  # type: ignore[method-assign]

    service = _service()
    service._get_compiler = MagicMock(return_value=compiler)
    service._extract_token_symbols_from_intent = MagicMock(return_value=[])

    result = await service.CompileIntent(_request(intent_type="lp_close"), MagicMock())

    assert result.success is True
    assert seen["usdc"] == "passed"
    assert isinstance(seen["eth"], ValueError)


@pytest.mark.asyncio
async def test_client_supplied_price_map_path_is_unchanged():
    """A client price_map short-circuits before the gate; VIB-6301 must not touch it."""
    service = _service()
    compiler = _mock_compiler()
    service._get_compiler = MagicMock(return_value=compiler)
    gate = AsyncMock()
    service._enforce_mainnet_price_gate = gate

    result = await service.CompileIntent(_request(intent_type="lp_close", price_map={"ETH": "3000"}), MagicMock())

    assert result.success is True
    gate.assert_not_awaited()
    compiler.update_prices.assert_called_once_with({"ETH": Decimal("3000")})


@pytest.mark.asyncio
async def test_self_served_price_path_is_unchanged():
    """Non-empty extraction with full coverage still self-serves the real prices."""
    service = _service()
    compiler = _mock_compiler()
    service._get_compiler = MagicMock(return_value=compiler)
    service._extract_token_symbols_from_intent = MagicMock(return_value=["WETH"])
    service._fetch_prices_for_tokens = AsyncMock(return_value={"WETH": Decimal("3000")})

    result = await service.CompileIntent(_request(intent_type="lp_close"), MagicMock())

    assert result.success is True
    compiler.update_prices.assert_called_once_with({"WETH": Decimal("3000")})


@pytest.mark.asyncio
async def test_close_with_extractable_tokens_but_no_prices_still_fails_closed():
    """The bypass is keyed on an EMPTY extraction, not on the intent being a close.

    A close whose payload DOES name tokens keeps the fail-closed behaviour when
    the price sources return nothing — VIB-6301 must not widen the hole.
    """
    service = _service()
    compiler = _mock_compiler()
    service._get_compiler = MagicMock(return_value=compiler)
    service._extract_token_symbols_from_intent = MagicMock(return_value=["WETH"])
    service._fetch_prices_for_tokens = AsyncMock(return_value={})

    result = await service.CompileIntent(_request(intent_type="lp_close"), MagicMock())

    assert result.success is False
    assert result.error_code == "NO_PRICES_AVAILABLE"
    compiler.compile.assert_not_called()
    compiler.update_prices.assert_not_called()
