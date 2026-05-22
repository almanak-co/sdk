"""Characterization tests for ``ExecutionOrchestrator._preflight_balance_check``.

The preflight runs before every intent submission to block transactions that
would revert on-chain due to insufficient token balance. This is safety-
critical: a regression that lets bad-balance intents through wastes gas on
approvals and dirties nonces. These tests pin the exact behaviour of the
function (return values, message strings, short-circuits) BEFORE the Phase
8.1a phase-extraction refactor so the refactor can be validated byte-for-byte
against this suite.

Scope (12 characterization cases):

1.  RPC URL missing -> returns ``None`` (preflight disabled).
2.  HOLD / no-requirement intent -> returns ``None``.
3.  SWAP native ETH, sufficient -> returns ``None``.
4.  SWAP native ETH, insufficient -> returns shortfall string.
5.  SWAP ERC20, sufficient -> returns ``None``.
6.  SWAP ERC20, insufficient -> returns shortfall string with decimals.
7.  LP_OPEN mixed basket (native + ERC20), sufficient -> returns ``None``.
8.  LP_OPEN mixed basket, insufficient native -> returns shortfall string.
9.  LP_OPEN mixed basket, insufficient ERC20 -> returns shortfall string.
10. SUPPLY wei-coded protocol (aave_v3) vs human-coded (morpho_blue).
11. SWAP uses ``original_from_token`` for multi-step bundles (VIB-2533).
12. RPC call failure on one requirement is a soft-fail (non-blocking).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.execution.orchestrator import ExecutionContext, ExecutionOrchestrator
from almanak.framework.models.reproduction_bundle import ActionBundle

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
USDC = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
WETH = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
AWETH = "0xe50fA9b3c56FfB159cB0FCA61F5c9D750e8128c8"


def _make_orchestrator(rpc_url: str | None = "http://localhost:8545") -> ExecutionOrchestrator:
    """Construct an orchestrator without running ``__init__`` side effects.

    We bypass ``__init__`` because that path instantiates gas-buffer / risk-
    config objects that are irrelevant to the balance-check code path under
    test. Attribute-for-attribute, the bits actually used by
    ``_preflight_balance_check`` are ``rpc_url``, ``chain``, ``signer.address``
    and ``_web3`` (which is patched via ``_get_web3``).
    """
    orch = ExecutionOrchestrator.__new__(ExecutionOrchestrator)
    orch.rpc_url = rpc_url
    orch.chain = "arbitrum"
    orch._web3 = None
    signer = MagicMock()
    signer.address = WALLET
    orch.signer = signer
    return orch


def _make_context(
    *,
    wallet: str = WALLET,
    chain: str = "arbitrum",
) -> ExecutionContext:
    return ExecutionContext(
        deployment_id="test",
        intent_id="intent-1",
        chain=chain,
        wallet_address=wallet,
    )


def _bundle(intent_type: str, metadata: dict) -> ActionBundle:
    return ActionBundle(intent_type=intent_type, transactions=[], metadata=metadata)


def _balance_bytes(amount_wei: int) -> bytes:
    """Encode a ``balanceOf`` return value (uint256 big-endian)."""
    return amount_wei.to_bytes(32, "big")


def _mock_web3(*, eth_balance: int = 0, erc20_returns: list[bytes] | bytes | None = None):
    """Build an ``AsyncWeb3``-shaped mock with predictable responses.

    Args:
        eth_balance: Native balance returned by ``get_balance`` in wei.
        erc20_returns: Either a single ``bytes`` payload (used for every
            ``eth.call``) or a list of payloads returned in order, or ``None``
            to leave ``eth.call`` returning zero-wei encoded bytes.
    """
    web3 = MagicMock()
    web3.to_checksum_address = lambda addr: addr
    web3.eth = MagicMock()
    web3.eth.get_balance = AsyncMock(return_value=eth_balance)

    if isinstance(erc20_returns, list):
        web3.eth.call = AsyncMock(side_effect=erc20_returns)
    else:
        web3.eth.call = AsyncMock(return_value=erc20_returns or _balance_bytes(0))
    return web3


# ---------------------------------------------------------------------------
# 1. Preflight disabled -- no rpc_url configured
# ---------------------------------------------------------------------------


class TestRpcUrlMissing:
    @pytest.mark.asyncio
    async def test_rpc_url_none_short_circuits_to_none(self):
        """With no RPC URL the check is a no-op and returns ``None``."""
        orch = _make_orchestrator(rpc_url=None)
        bundle = _bundle(
            "SWAP",
            {
                "from_token": {"address": USDC, "symbol": "USDC", "decimals": 6},
                "amount_in": "1000000",
            },
        )

        result = await orch._preflight_balance_check(bundle, _make_context())

        assert result is None


# ---------------------------------------------------------------------------
# 2. No-requirement intents (HOLD, LP_CLOSE, unknown) -- nothing to check
# ---------------------------------------------------------------------------


class TestNoRequirements:
    @pytest.mark.asyncio
    async def test_hold_intent_returns_none_without_rpc(self):
        """HOLD carries no balance requirements, so the check short-circuits."""
        orch = _make_orchestrator()
        # No token metadata at all
        bundle = _bundle("HOLD", {})

        # Fail loudly if any web3 call is made
        with patch.object(orch, "_get_web3", side_effect=AssertionError("no RPC expected")):
            result = await orch._preflight_balance_check(bundle, _make_context())

        assert result is None

    @pytest.mark.asyncio
    async def test_zero_amount_requirement_is_skipped(self):
        """Amounts of 0 wei are not appended to the requirements list."""
        orch = _make_orchestrator()
        bundle = _bundle(
            "SWAP",
            {
                "from_token": {"address": USDC, "symbol": "USDC", "decimals": 6},
                "amount_in": "0",
            },
        )

        with patch.object(orch, "_get_web3", side_effect=AssertionError("no RPC expected")):
            result = await orch._preflight_balance_check(bundle, _make_context())

        assert result is None


# ---------------------------------------------------------------------------
# 3 + 4. Native ETH only
# ---------------------------------------------------------------------------


class TestNativeETH:
    @pytest.mark.asyncio
    async def test_native_eth_sufficient_returns_none(self):
        """``is_native=True`` routes through ``eth.get_balance``; sufficient -> None."""
        orch = _make_orchestrator()
        bundle = _bundle(
            "SWAP",
            {
                "from_token": {"symbol": "ETH", "decimals": 18, "is_native": True, "address": ""},
                "amount_in": str(10**18),  # 1 ETH
            },
        )
        web3 = _mock_web3(eth_balance=2 * 10**18)  # 2 ETH
        with patch.object(orch, "_get_web3", return_value=web3):
            result = await orch._preflight_balance_check(bundle, _make_context())

        assert result is None
        web3.eth.get_balance.assert_awaited_once()
        web3.eth.call.assert_not_called()

    @pytest.mark.asyncio
    async def test_native_eth_insufficient_returns_shortfall(self):
        """Insufficient native balance -> shortfall message with decimals."""
        orch = _make_orchestrator()
        bundle = _bundle(
            "SWAP",
            {
                "from_token": {"symbol": "ETH", "decimals": 18, "is_native": True, "address": ""},
                "amount_in": str(2 * 10**18),  # need 2 ETH
            },
        )
        web3 = _mock_web3(eth_balance=5 * 10**17)  # have 0.5 ETH
        with patch.object(orch, "_get_web3", return_value=web3):
            result = await orch._preflight_balance_check(bundle, _make_context())

        assert result is not None
        assert "Pre-flight balance check failed" in result
        assert "Insufficient ETH" in result
        assert "have 0.500000" in result
        assert "need 2.000000" in result
        assert "No transactions submitted" in result


# ---------------------------------------------------------------------------
# 5 + 6. ERC20 only
# ---------------------------------------------------------------------------


class TestERC20Only:
    @pytest.mark.asyncio
    async def test_erc20_sufficient_returns_none(self):
        orch = _make_orchestrator()
        bundle = _bundle(
            "SWAP",
            {
                "from_token": {"address": USDC, "symbol": "USDC", "decimals": 6},
                "amount_in": "1000000",  # 1 USDC in wei
            },
        )
        web3 = _mock_web3(erc20_returns=_balance_bytes(5_000_000))  # 5 USDC
        with patch.object(orch, "_get_web3", return_value=web3):
            result = await orch._preflight_balance_check(bundle, _make_context())

        assert result is None
        web3.eth.call.assert_awaited_once()
        web3.eth.get_balance.assert_not_called()

    @pytest.mark.asyncio
    async def test_erc20_insufficient_returns_shortfall_with_decimals(self):
        orch = _make_orchestrator()
        bundle = _bundle(
            "SWAP",
            {
                "from_token": {"address": USDC, "symbol": "USDC", "decimals": 6},
                "amount_in": "10000000",  # need 10 USDC
            },
        )
        web3 = _mock_web3(erc20_returns=_balance_bytes(2_500_000))  # have 2.5 USDC
        with patch.object(orch, "_get_web3", return_value=web3):
            result = await orch._preflight_balance_check(bundle, _make_context())

        assert result is not None
        assert "Insufficient USDC" in result
        assert "have 2.500000" in result
        assert "need 10.000000" in result

    @pytest.mark.asyncio
    async def test_erc20_insufficient_without_decimals_reports_wei(self):
        """When ``decimals`` is missing the shortfall is reported in raw wei."""
        orch = _make_orchestrator()
        bundle = _bundle(
            "REPAY",
            {
                "repay_token": {"address": USDC, "symbol": "MYSTERY"},
                # No decimals -> human-readable path fails, wei fallback used
                "repay_amount": "1000000",
            },
        )
        # aave_v3 is wei-coded so the int parses cleanly
        bundle.metadata["protocol"] = "aave_v3"
        web3 = _mock_web3(erc20_returns=_balance_bytes(100))
        with patch.object(orch, "_get_web3", return_value=web3):
            result = await orch._preflight_balance_check(bundle, _make_context())

        assert result is not None
        assert "Insufficient MYSTERY" in result
        assert "have 100 wei" in result
        assert "need 1000000 wei" in result


# ---------------------------------------------------------------------------
# 7 + 8 + 9. Mixed basket (LP_OPEN native + ERC20)
# ---------------------------------------------------------------------------


class TestLPOpenMixedBasket:
    @pytest.mark.asyncio
    async def test_mixed_basket_sufficient_returns_none(self):
        """Both legs sufficient -> no error."""
        orch = _make_orchestrator()
        bundle = _bundle(
            "LP_OPEN",
            {
                "token0": {"symbol": "ETH", "decimals": 18, "is_native": True, "address": ""},
                "token1": {"address": USDC, "symbol": "USDC", "decimals": 6},
                "amount0_desired": str(10**18),  # 1 ETH
                "amount1_desired": "1000000",  # 1 USDC
            },
        )
        web3 = _mock_web3(
            eth_balance=5 * 10**18,
            erc20_returns=_balance_bytes(10_000_000),
        )
        with patch.object(orch, "_get_web3", return_value=web3):
            result = await orch._preflight_balance_check(bundle, _make_context())

        assert result is None
        web3.eth.get_balance.assert_awaited_once()
        web3.eth.call.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_mixed_basket_insufficient_native_returns_shortfall(self):
        orch = _make_orchestrator()
        bundle = _bundle(
            "LP_OPEN",
            {
                "token0": {"symbol": "ETH", "decimals": 18, "is_native": True, "address": ""},
                "token1": {"address": USDC, "symbol": "USDC", "decimals": 6},
                "amount0_desired": str(10 * 10**18),  # need 10 ETH
                "amount1_desired": "1000000",
            },
        )
        web3 = _mock_web3(
            eth_balance=10**17,  # 0.1 ETH
            erc20_returns=_balance_bytes(10_000_000),
        )
        with patch.object(orch, "_get_web3", return_value=web3):
            result = await orch._preflight_balance_check(bundle, _make_context())

        assert result is not None
        assert "Insufficient ETH" in result
        # USDC leg is fine -- must not appear as a shortfall
        assert "Insufficient USDC" not in result

    @pytest.mark.asyncio
    async def test_mixed_basket_insufficient_erc20_returns_shortfall(self):
        orch = _make_orchestrator()
        bundle = _bundle(
            "LP_OPEN",
            {
                "token0": {"symbol": "ETH", "decimals": 18, "is_native": True, "address": ""},
                "token1": {"address": USDC, "symbol": "USDC", "decimals": 6},
                "amount0_desired": str(10**18),
                "amount1_desired": "100000000",  # need 100 USDC
            },
        )
        web3 = _mock_web3(
            eth_balance=5 * 10**18,
            erc20_returns=_balance_bytes(1_000_000),  # 1 USDC
        )
        with patch.object(orch, "_get_web3", return_value=web3):
            result = await orch._preflight_balance_check(bundle, _make_context())

        assert result is not None
        assert "Insufficient USDC" in result
        assert "Insufficient ETH" not in result


# ---------------------------------------------------------------------------
# 10. Lending protocol wei vs human encoding
# ---------------------------------------------------------------------------


class TestSupplyLending:
    @pytest.mark.asyncio
    async def test_supply_aave_v3_uses_wei_encoding(self):
        """aave_v3 ``supply_amount`` is interpreted as wei (no decimals multiply)."""
        orch = _make_orchestrator()
        bundle = _bundle(
            "SUPPLY",
            {
                "protocol": "aave_v3",
                "supply_token": {"address": USDC, "symbol": "USDC", "decimals": 6},
                "supply_amount": "1000000",  # 1 USDC in wei
            },
        )
        web3 = _mock_web3(erc20_returns=_balance_bytes(500_000))  # 0.5 USDC
        with patch.object(orch, "_get_web3", return_value=web3):
            result = await orch._preflight_balance_check(bundle, _make_context())

        assert result is not None
        assert "Insufficient USDC" in result
        assert "have 0.500000" in result
        assert "need 1.000000" in result

    @pytest.mark.asyncio
    async def test_supply_morpho_uses_human_readable_encoding(self):
        """morpho_blue ``supply_amount`` is human-readable and multiplied by 10^decimals."""
        orch = _make_orchestrator()
        bundle = _bundle(
            "SUPPLY",
            {
                "protocol": "morpho_blue",
                "supply_token": {"address": USDC, "symbol": "USDC", "decimals": 6},
                "supply_amount": "1",  # 1 USDC human-readable -> 1_000_000 wei
            },
        )
        web3 = _mock_web3(erc20_returns=_balance_bytes(500_000))  # 0.5 USDC
        with patch.object(orch, "_get_web3", return_value=web3):
            result = await orch._preflight_balance_check(bundle, _make_context())

        assert result is not None
        assert "Insufficient USDC" in result
        assert "have 0.500000" in result
        assert "need 1.000000" in result

    @pytest.mark.asyncio
    async def test_repay_full_sentinel_is_skipped(self):
        """``repay_full=True`` means MAX_UINT256 -- must NOT be appended to requirements."""
        orch = _make_orchestrator()
        bundle = _bundle(
            "REPAY",
            {
                "protocol": "aave_v3",
                "repay_token": {"address": USDC, "symbol": "USDC", "decimals": 6},
                "repay_amount": str(2**256 - 1),
                "repay_full": True,
            },
        )
        # If the check somehow ran, web3 would be called; assert it isn't.
        with patch.object(orch, "_get_web3", side_effect=AssertionError("no RPC expected")):
            result = await orch._preflight_balance_check(bundle, _make_context())

        assert result is None


# ---------------------------------------------------------------------------
# 11. SWAP multi-step bundles (VIB-2533) -- original_from_token wins
# ---------------------------------------------------------------------------


class TestSwapMultiStepOriginalToken:
    @pytest.mark.asyncio
    async def test_original_from_token_overrides_intermediate(self):
        """For Pendle-style pre-swap bundles, the check must read the ORIGINAL
        input token, not the intermediate ``from_token`` written by the
        compiler."""
        orch = _make_orchestrator()
        bundle = _bundle(
            "SWAP",
            {
                # Intermediate (not yet in wallet)
                "from_token": {"address": WETH, "symbol": "sUSDe", "decimals": 18},
                "amount_in": str(10**18),
                # Original input the wallet actually holds
                "original_from_token": {"address": USDC, "symbol": "USDC", "decimals": 6},
                "original_amount_in": "1000000",
            },
        )
        # Insufficient USDC -- must trigger shortfall on USDC (not sUSDe)
        web3 = _mock_web3(erc20_returns=_balance_bytes(100))
        with patch.object(orch, "_get_web3", return_value=web3):
            result = await orch._preflight_balance_check(bundle, _make_context())

        assert result is not None
        assert "Insufficient USDC" in result
        assert "sUSDe" not in result


# ---------------------------------------------------------------------------
# 12. RPC failures are soft-fail (never block submission)
# ---------------------------------------------------------------------------


class TestRpcFailureSoftFail:
    @pytest.mark.asyncio
    async def test_individual_check_timeout_is_logged_and_does_not_block(self):
        """Per-token RPC timeouts must NOT be treated as shortfalls -- the
        function logs a warning and returns ``None`` (fail-open)."""
        orch = _make_orchestrator()
        bundle = _bundle(
            "SWAP",
            {
                "from_token": {"address": USDC, "symbol": "USDC", "decimals": 6},
                "amount_in": "1000000",
            },
        )
        web3 = MagicMock()
        web3.to_checksum_address = lambda addr: addr
        web3.eth = MagicMock()
        web3.eth.call = AsyncMock(side_effect=TimeoutError())
        web3.eth.get_balance = AsyncMock(return_value=0)

        with patch.object(orch, "_get_web3", return_value=web3):
            result = await orch._preflight_balance_check(bundle, _make_context())

        # Soft-fail: no shortfall raised even though RPC failed
        assert result is None

    @pytest.mark.asyncio
    async def test_web3_setup_exception_is_swallowed(self):
        """A failure to construct the web3 client is also soft-fail."""
        orch = _make_orchestrator()
        bundle = _bundle(
            "SWAP",
            {
                "from_token": {"address": USDC, "symbol": "USDC", "decimals": 6},
                "amount_in": "1000000",
            },
        )
        with patch.object(orch, "_get_web3", side_effect=ConnectionError("RPC down")):
            result = await orch._preflight_balance_check(bundle, _make_context())

        assert result is None


# ---------------------------------------------------------------------------
# Bonus: multi-chain context does not change preflight behaviour
# ---------------------------------------------------------------------------


class TestMultiChainContext:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("chain", ["arbitrum", "base", "optimism", "polygon"])
    async def test_chain_field_does_not_affect_balance_logic(self, chain: str):
        """The preflight is chain-agnostic -- the same metadata produces the
        same answer regardless of the ``context.chain`` field."""
        orch = _make_orchestrator()
        bundle = _bundle(
            "SWAP",
            {
                "from_token": {"address": USDC, "symbol": "USDC", "decimals": 6},
                "amount_in": "1000000",
            },
        )
        web3 = _mock_web3(erc20_returns=_balance_bytes(100))  # insufficient
        with patch.object(orch, "_get_web3", return_value=web3):
            result = await orch._preflight_balance_check(bundle, _make_context(chain=chain))

        assert result is not None
        assert "Insufficient USDC" in result
