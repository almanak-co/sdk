"""Unit tests for the Morpho Blue yield-poke calldata.

Morpho Blue's ``accrueInterest(MarketParams)`` derives the market id by
hashing ALL MarketParams fields (keccak256 of the abi-encoded struct) and
only checks that the derived market exists. A poke encoded with placeholder
oracle/irm/lltv therefore hashes to a nonexistent market and reverts
(PR #2755 review, comment 3401797700). These tests pin:

  (a) the encoded calldata uses the real market params from MORPHO_MARKETS;
  (b) the abi-encoded MarketParams tuple hashes to POKE_MARKET_ID, proving
      the calldata identifies a real on-chain market;
  (c) the poke targets the Ethereum Morpho Blue singleton with the
      accrueInterest selector.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from eth_abi import encode
from eth_utils import keccak

from almanak.connectors.morpho_blue.addresses import MORPHO_BLUE, MORPHO_MARKETS
from almanak.connectors.morpho_blue.backtest_poke import (
    MORPHO_ACCRUE_SIG,
    MORPHO_BLUE_ETHEREUM,
    POKE_MARKET_ID,
    _accrue_interest_calldata,
    poke_morpho_blue,
)

POKE_MARKET = MORPHO_MARKETS["ethereum"][POKE_MARKET_ID]

# accrueInterest(MarketParams) calldata for the catalogued wstETH/USDC market
# (86% LLTV): selector + abi.encode(loanToken, collateralToken, oracle, irm, lltv).
EXPECTED_CALLDATA = (
    "0x151c1ade"
    "000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"  # loanToken USDC
    "0000000000000000000000007f39c581f595b53c5cb19bd0b3f8da6c935e2ca0"  # collateralToken wstETH
    "00000000000000000000000048f7e36eb6b826b2df4b2e630b62cd25e89e40e2"  # oracle
    "000000000000000000000000870ac11d48b15db9a138cf899d20f13f79ba00bc"  # irm
    "0000000000000000000000000000000000000000000000000bef55718ad60000"  # lltv 0.86e18
)


class TestAccrueInterestCalldata:
    def test_calldata_matches_pinned_encoding(self) -> None:
        """Calldata must encode the real market params, not placeholders."""
        assert _accrue_interest_calldata(POKE_MARKET) == EXPECTED_CALLDATA

    def test_market_params_hash_to_poke_market_id(self) -> None:
        """keccak256(abi.encode(MarketParams)) must equal POKE_MARKET_ID.

        This is the on-chain market-id derivation (MarketParamsLib.id in
        morpho-org/morpho-blue). If this holds, accrueInterest resolves the
        calldata to the real catalogued market.
        """
        encoded = encode(
            ["address", "address", "address", "address", "uint256"],
            [
                POKE_MARKET["loan_token_address"],
                POKE_MARKET["collateral_token_address"],
                POKE_MARKET["oracle"],
                POKE_MARKET["irm"],
                POKE_MARKET["lltv"],
            ],
        )
        assert "0x" + keccak(encoded).hex() == POKE_MARKET_ID

    def test_placeholder_params_would_hash_to_different_market(self) -> None:
        """The pre-fix zero-placeholder params hash to a nonexistent market id."""
        encoded = encode(
            ["address", "address", "address", "address", "uint256"],
            [
                POKE_MARKET["loan_token_address"],
                POKE_MARKET["collateral_token_address"],
                "0x0000000000000000000000000000000000000000",
                "0x0000000000000000000000000000000000000000",
                0,
            ],
        )
        assert "0x" + keccak(encoded).hex() != POKE_MARKET_ID


class TestPokeMorphoBlue:
    @pytest.mark.asyncio
    async def test_poke_sends_pinned_calldata_to_morpho_singleton(self) -> None:
        with patch(
            "almanak.connectors.morpho_blue.backtest_poke._send_tx",
            new=AsyncMock(return_value="0xtxhash"),
        ) as send_tx:
            result = await poke_morpho_blue("http://localhost:8545", "0xwallet")

        assert result.success is True
        assert result.protocol == "morpho_blue"
        assert result.tx_hash == "0xtxhash"
        send_tx.assert_awaited_once_with(
            "http://localhost:8545", "0xwallet", MORPHO_BLUE_ETHEREUM, EXPECTED_CALLDATA
        )

    @pytest.mark.asyncio
    async def test_poke_failure_returns_unsuccessful_result(self) -> None:
        with patch(
            "almanak.connectors.morpho_blue.backtest_poke._send_tx",
            new=AsyncMock(side_effect=RuntimeError("execution reverted")),
        ):
            result = await poke_morpho_blue("http://localhost:8545", "0xwallet")

        assert result.success is False
        assert result.error is not None and "execution reverted" in result.error

    def test_target_is_ethereum_morpho_singleton(self) -> None:
        assert MORPHO_BLUE_ETHEREUM == MORPHO_BLUE["ethereum"]["morpho"]

    def test_selector_is_accrue_interest(self) -> None:
        assert MORPHO_ACCRUE_SIG == "0x151c1ade"
        assert EXPECTED_CALLDATA.startswith(MORPHO_ACCRUE_SIG)
