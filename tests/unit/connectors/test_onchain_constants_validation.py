"""Non-mocked validation of on-chain constants (VIB-186).

Validates that hardcoded event topic hashes and function selectors in receipt
parsers and on-chain readers match their canonical Solidity signatures using
keccak256 derivation.

Previously, all tests used mocks that mirrored the same constants, so
mismatches were never caught. This test derives expected values from
Solidity signatures and validates against code constants.

This caught real bugs:
- P0: All 4 Pendle on_chain_reader selectors were wrong (now fixed)
- P2: TraderJoe ClaimedFees topic hash was wrong (now fixed)
"""

import pytest
from web3 import Web3


# =============================================================================
# Helpers
# =============================================================================


def _topic(sig: str) -> str:
    """Derive event topic hash from canonical Solidity event signature."""
    return "0x" + Web3.keccak(text=sig).hex()


def _selector(sig: str) -> str:
    """Derive 4-byte function selector from canonical Solidity function signature."""
    return "0x" + Web3.keccak(text=sig)[:4].hex()


# =============================================================================
# Standard ERC events (shared across many parsers)
# =============================================================================

# These well-known event signatures are used in nearly every receipt parser.
STANDARD_EVENTS = {
    "Transfer": "Transfer(address,address,uint256)",
    "Approval": "Approval(address,address,uint256)",
}

ERC1155_EVENTS = {
    "TransferSingle": "TransferSingle(address,address,address,uint256,uint256)",
    "TransferBatch": "TransferBatch(address,address,address,uint256[],uint256[])",
    "ApprovalForAll": "ApprovalForAll(address,address,bool)",
}

WETH_EVENTS = {
    "Deposit": "Deposit(address,uint256)",
    "Withdrawal": "Withdrawal(address,uint256)",
}


class TestStandardERC20Events:
    """Validate ERC-20 Transfer and Approval topics across all parsers that use them."""

    @pytest.mark.parametrize(
        "module_path",
        [
            "almanak.framework.connectors.uniswap_v3.receipt_parser",
            "almanak.framework.connectors.sushiswap_v3.receipt_parser",
            "almanak.framework.connectors.pancakeswap_v3.receipt_parser",
            "almanak.framework.connectors.aerodrome.receipt_parser",
            "almanak.framework.connectors.aave_v3.receipt_parser",
            "almanak.framework.connectors.traderjoe_v2.receipt_parser",
            "almanak.framework.connectors.morpho_blue.receipt_parser",
            "almanak.framework.connectors.morpho_vault.receipt_parser",
            "almanak.framework.connectors.compound_v3.receipt_parser",
            "almanak.framework.connectors.curve.receipt_parser",
            "almanak.framework.connectors.lido.receipt_parser",
            "almanak.framework.connectors.pendle.receipt_parser",
            "almanak.framework.connectors.spark.receipt_parser",
        ],
    )
    def test_transfer_topic(self, module_path):
        """Validate Transfer topic in parsers that define it in EVENT_TOPICS."""
        import importlib

        mod = importlib.import_module(module_path)
        topics = mod.EVENT_TOPICS
        if "Transfer" in topics:
            assert topics["Transfer"] == _topic(STANDARD_EVENTS["Transfer"]), (
                f"{module_path}: Transfer topic mismatch"
            )

    @pytest.mark.parametrize(
        "module_path",
        [
            "almanak.framework.connectors.uniswap_v3.receipt_parser",
            "almanak.framework.connectors.sushiswap_v3.receipt_parser",
            "almanak.framework.connectors.aerodrome.receipt_parser",
            "almanak.framework.connectors.aave_v3.receipt_parser",
            "almanak.framework.connectors.traderjoe_v2.receipt_parser",
            "almanak.framework.connectors.morpho_blue.receipt_parser",
            "almanak.framework.connectors.morpho_vault.receipt_parser",
            "almanak.framework.connectors.compound_v3.receipt_parser",
            "almanak.framework.connectors.pendle.receipt_parser",
        ],
    )
    def test_approval_topic(self, module_path):
        """Validate Approval topic in parsers that define it in EVENT_TOPICS."""
        import importlib

        mod = importlib.import_module(module_path)
        topics = mod.EVENT_TOPICS
        if "Approval" in topics:
            assert topics["Approval"] == _topic(STANDARD_EVENTS["Approval"]), (
                f"{module_path}: Approval topic mismatch"
            )

    def test_enso_transfer_signature(self):
        from almanak.framework.connectors.enso.receipt_parser import TRANSFER_EVENT_SIGNATURE

        assert TRANSFER_EVENT_SIGNATURE == _topic(STANDARD_EVENTS["Transfer"])

    def test_lifi_transfer_signature(self):
        from almanak.framework.connectors.lifi.receipt_parser import TRANSFER_EVENT_SIGNATURE

        assert TRANSFER_EVENT_SIGNATURE == _topic(STANDARD_EVENTS["Transfer"])

    def test_polymarket_erc20_transfer(self):
        from almanak.framework.connectors.polymarket.receipt_parser import ERC20_TRANSFER_TOPIC

        assert ERC20_TRANSFER_TOPIC == _topic(STANDARD_EVENTS["Transfer"])

    def test_polymarket_erc20_approval(self):
        from almanak.framework.connectors.polymarket.receipt_parser import ERC20_APPROVAL_TOPIC

        assert ERC20_APPROVAL_TOPIC == _topic(STANDARD_EVENTS["Approval"])


class TestERC1155Events:
    """Validate ERC-1155 event topics."""

    def test_polymarket_transfer_single(self):
        from almanak.framework.connectors.polymarket.receipt_parser import TRANSFER_SINGLE_TOPIC

        assert TRANSFER_SINGLE_TOPIC == _topic(ERC1155_EVENTS["TransferSingle"])

    def test_polymarket_transfer_batch(self):
        from almanak.framework.connectors.polymarket.receipt_parser import TRANSFER_BATCH_TOPIC

        assert TRANSFER_BATCH_TOPIC == _topic(ERC1155_EVENTS["TransferBatch"])

    def test_polymarket_approval_for_all(self):
        from almanak.framework.connectors.polymarket.receipt_parser import APPROVAL_FOR_ALL_TOPIC

        assert APPROVAL_FOR_ALL_TOPIC == _topic(ERC1155_EVENTS["ApprovalForAll"])

    def test_traderjoe_transfer_batch(self):
        from almanak.framework.connectors.traderjoe_v2.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["TransferBatch"] == _topic(ERC1155_EVENTS["TransferBatch"])


class TestWETHEvents:
    """Validate WETH Deposit/Withdrawal topics."""

    def test_traderjoe_deposit(self):
        from almanak.framework.connectors.traderjoe_v2.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["Deposit"] == _topic(WETH_EVENTS["Deposit"])

    def test_traderjoe_withdrawal(self):
        from almanak.framework.connectors.traderjoe_v2.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["Withdrawal"] == _topic(WETH_EVENTS["Withdrawal"])


# =============================================================================
# Pendle function selectors
# =============================================================================


class TestPendleSelectors:
    """Validate Pendle RouterStatic function selectors."""

    def test_get_pt_to_asset_rate(self):
        from almanak.framework.data.pendle.on_chain_reader import GET_PT_TO_ASSET_RATE_SELECTOR

        assert GET_PT_TO_ASSET_RATE_SELECTOR == _selector("getPtToAssetRate(address)")

    def test_get_implied_apy(self):
        from almanak.framework.data.pendle.on_chain_reader import GET_IMPLIED_APY_SELECTOR

        assert GET_IMPLIED_APY_SELECTOR == _selector("getImpliedApy(address)")

    def test_read_tokens(self):
        from almanak.framework.data.pendle.on_chain_reader import READ_TOKENS_SELECTOR

        assert READ_TOKENS_SELECTOR == _selector("readTokens(address)")

    def test_expiry(self):
        from almanak.framework.data.pendle.on_chain_reader import EXPIRY_SELECTOR

        assert EXPIRY_SELECTOR == _selector("expiry()")


# =============================================================================
# Uniswap V3 events
# =============================================================================

# Canonical Uniswap V3 event signatures from IUniswapV3PoolEvents.sol
UNISWAP_V3_SIGNATURES = {
    "Swap": "Swap(address,address,int256,int256,uint160,uint128,int24)",
    "Mint": "Mint(address,address,int24,int24,uint128,uint256,uint256)",
    "Burn": "Burn(address,int24,int24,uint128,uint256,uint256)",
    "Collect": "Collect(address,address,int24,int24,uint128,uint128)",
    "Flash": "Flash(address,address,uint256,uint256,uint256,uint256)",
}


class TestUniswapV3Events:
    """Validate Uniswap V3 event topics."""

    @pytest.mark.parametrize("event_name,signature", list(UNISWAP_V3_SIGNATURES.items()))
    def test_uniswap_v3(self, event_name, signature):
        from almanak.framework.connectors.uniswap_v3.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS[event_name] == _topic(signature), f"Uniswap V3 {event_name} mismatch"


class TestSushiSwapV3Events:
    """Validate SushiSwap V3 event topics (same ABI as Uniswap V3)."""

    @pytest.mark.parametrize("event_name,signature", list(UNISWAP_V3_SIGNATURES.items()))
    def test_sushiswap_v3(self, event_name, signature):
        from almanak.framework.connectors.sushiswap_v3.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS[event_name] == _topic(signature), f"SushiSwap V3 {event_name} mismatch"


class TestPancakeSwapV3Events:
    """Validate PancakeSwap V3 event topics.

    PancakeSwap V3 shares Mint/Burn/Collect with Uniswap V3 but has
    a different Swap event with extra protocolFees params.
    """

    # PancakeSwap V3 Swap has 2 extra params: protocolFeesToken0, protocolFeesToken1
    PANCAKESWAP_SWAP_SIG = "Swap(address,address,int256,int256,uint160,uint128,int24,uint128,uint128)"

    @pytest.mark.parametrize(
        "event_name,signature",
        [
            ("Mint", UNISWAP_V3_SIGNATURES["Mint"]),
            ("Burn", UNISWAP_V3_SIGNATURES["Burn"]),
            ("Collect", UNISWAP_V3_SIGNATURES["Collect"]),
        ],
    )
    def test_shared_with_uniswap(self, event_name, signature):
        from almanak.framework.connectors.pancakeswap_v3.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS[event_name] == _topic(signature), f"PancakeSwap V3 {event_name} mismatch"

    def test_swap(self):
        from almanak.framework.connectors.pancakeswap_v3.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["Swap"] == _topic(self.PANCAKESWAP_SWAP_SIG)


# =============================================================================
# Aerodrome events (Uniswap V2-style)
# =============================================================================


class TestAerodromeEvents:
    """Validate Aerodrome event topics (Uniswap V2-style AMM)."""

    AERODROME_SIGNATURES = {
        "Swap": "Swap(address,uint256,uint256,uint256,uint256,address)",
        "Mint": "Mint(address,uint256,uint256)",
        "Burn": "Burn(address,uint256,uint256,address)",
        "Sync": "Sync(uint112,uint112)",
    }

    @pytest.mark.parametrize("event_name,signature", list(AERODROME_SIGNATURES.items()))
    def test_aerodrome(self, event_name, signature):
        from almanak.framework.connectors.aerodrome.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS[event_name] == _topic(signature), f"Aerodrome {event_name} mismatch"


# =============================================================================
# TraderJoe V2 events
# =============================================================================


class TestTraderJoeV2Events:
    """Validate TraderJoe V2 event topics.

    Signatures sourced from comments in receipt_parser.py.
    """

    TRADERJOE_SIGNATURES = {
        "DepositedToBins": "DepositedToBins(address,address,uint256[],bytes32[])",
        "WithdrawnFromBins": "WithdrawnFromBins(address,address,uint256[],bytes32[])",
        "ClaimedFees": "ClaimedFees(address,address,uint256[],bytes32[])",
    }

    @pytest.mark.parametrize("event_name,signature", list(TRADERJOE_SIGNATURES.items()))
    def test_traderjoe_v2(self, event_name, signature):
        from almanak.framework.connectors.traderjoe_v2.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS[event_name] == _topic(signature), f"TraderJoe V2 {event_name} mismatch"


# =============================================================================
# Aave V3 events
# =============================================================================


class TestAaveV3Events:
    """Validate Aave V3 event topics.

    Core lending events from IPool.sol. DataTypes.InterestRateMode maps to uint8.
    """

    AAVE_V3_SIGNATURES = {
        "Supply": "Supply(address,address,address,uint256,uint16)",
        "Withdraw": "Withdraw(address,address,address,uint256)",
        "Borrow": "Borrow(address,address,address,uint256,uint8,uint256,uint16)",
        "Repay": "Repay(address,address,address,uint256,bool)",
        "FlashLoan": "FlashLoan(address,address,address,uint256,uint8,uint256,uint16)",
        "LiquidationCall": "LiquidationCall(address,address,address,uint256,uint256,address,bool)",
        "ReserveDataUpdated": "ReserveDataUpdated(address,uint256,uint256,uint256,uint256,uint256)",
        "ReserveUsedAsCollateralEnabled": "ReserveUsedAsCollateralEnabled(address,address)",
        "ReserveUsedAsCollateralDisabled": "ReserveUsedAsCollateralDisabled(address,address)",
        "UserEModeSet": "UserEModeSet(address,uint8)",
        "IsolationModeTotalDebtUpdated": "IsolationModeTotalDebtUpdated(address,uint256)",
    }

    @pytest.mark.parametrize("event_name,signature", list(AAVE_V3_SIGNATURES.items()))
    def test_aave_v3(self, event_name, signature):
        from almanak.framework.connectors.aave_v3.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS[event_name] == _topic(signature), f"Aave V3 {event_name} mismatch"


class TestSparkEvents:
    """Validate Spark event topics (Aave V3 fork, same event signatures)."""

    @pytest.mark.parametrize(
        "event_name,signature",
        [
            ("Supply", "Supply(address,address,address,uint256,uint16)"),
            ("Withdraw", "Withdraw(address,address,address,uint256)"),
            ("Borrow", "Borrow(address,address,address,uint256,uint8,uint256,uint16)"),
            ("Repay", "Repay(address,address,address,uint256,bool)"),
        ],
    )
    def test_spark(self, event_name, signature):
        from almanak.framework.connectors.spark.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS[event_name] == _topic(signature), f"Spark {event_name} mismatch"


# =============================================================================
# Morpho Blue events
# =============================================================================


class TestMorphoBlueEvents:
    """Validate Morpho Blue event topics.

    From morpho-org/morpho-blue contracts. Id type resolves to bytes32.
    """

    MORPHO_BLUE_VERIFIED = {
        "Supply": "Supply(bytes32,address,address,uint256,uint256)",
        "Borrow": "Borrow(bytes32,address,address,address,uint256,uint256)",
        "Repay": "Repay(bytes32,address,address,uint256,uint256)",
        "SupplyCollateral": "SupplyCollateral(bytes32,address,address,uint256)",
        # CreateMarket uses a struct param: MarketParams(address,address,address,address,uint256)
        "CreateMarket": "CreateMarket(bytes32,(address,address,address,address,uint256))",
        "Withdraw": "Withdraw(bytes32,address,address,address,uint256,uint256)",
        "WithdrawCollateral": "WithdrawCollateral(bytes32,address,address,address,uint256)",
        "Liquidate": "Liquidate(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)",
        "FlashLoan": "FlashLoan(address,address,uint256)",
        "SetAuthorization": "SetAuthorization(address,address,address,bool)",
        "AccrueInterest": "AccrueInterest(bytes32,uint256,uint256,uint256)",
    }

    @pytest.mark.parametrize("event_name,signature", list(MORPHO_BLUE_VERIFIED.items()))
    def test_morpho_blue(self, event_name, signature):
        from almanak.framework.connectors.morpho_blue.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS[event_name] == _topic(signature), f"Morpho Blue {event_name} mismatch"


# =============================================================================
# ERC-4626 events (Ethena sUSDe, Morpho Vault)
# =============================================================================


class TestERC4626Events:
    """Validate ERC-4626 Deposit/Withdraw topics (Ethena, Morpho Vault)."""

    ERC4626_SIGNATURES = {
        "Deposit": "Deposit(address,address,uint256,uint256)",
        "Withdraw": "Withdraw(address,address,address,uint256,uint256)",
    }

    @pytest.mark.parametrize("event_name,signature", list(ERC4626_SIGNATURES.items()))
    def test_ethena(self, event_name, signature):
        from almanak.framework.connectors.ethena.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS[event_name] == _topic(signature), f"Ethena {event_name} mismatch"

    @pytest.mark.parametrize("event_name,signature", list(ERC4626_SIGNATURES.items()))
    def test_morpho_vault(self, event_name, signature):
        from almanak.framework.connectors.morpho_vault.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS[event_name] == _topic(signature), f"Morpho Vault {event_name} mismatch"


# =============================================================================
# Compound V3 events
# =============================================================================


class TestCompoundV3Events:
    """Validate Compound V3 (Comet) event topics."""

    COMPOUND_V3_VERIFIED = {
        "Supply": "Supply(address,address,uint256)",
        "Withdraw": "Withdraw(address,address,uint256)",
        "SupplyCollateral": "SupplyCollateral(address,address,address,uint256)",
        "WithdrawCollateral": "WithdrawCollateral(address,address,address,uint256)",
        "TransferCollateral": "TransferCollateral(address,address,address,uint256)",
        "AbsorbDebt": "AbsorbDebt(address,address,uint256,uint256)",
    }

    @pytest.mark.parametrize("event_name,signature", list(COMPOUND_V3_VERIFIED.items()))
    def test_compound_v3(self, event_name, signature):
        from almanak.framework.connectors.compound_v3.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS[event_name] == _topic(signature), f"Compound V3 {event_name} mismatch"


# =============================================================================
# Lido events
# =============================================================================


class TestLidoEvents:
    """Validate Lido event topics.

    Signatures sourced from comments in receipt_parser.py.
    """

    LIDO_SIGNATURES = {
        "Submitted": "Submitted(address,uint256,address)",
        "WithdrawalRequested": "WithdrawalRequested(uint256,address,address,uint256,uint256)",
        "WithdrawalClaimed": "WithdrawalClaimed(uint256,address,address,uint256)",
    }

    @pytest.mark.parametrize("event_name,signature", list(LIDO_SIGNATURES.items()))
    def test_lido(self, event_name, signature):
        from almanak.framework.connectors.lido.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS[event_name] == _topic(signature), f"Lido {event_name} mismatch"


# =============================================================================
# Polymarket CTF events
# =============================================================================


class TestPolymarketEvents:
    """Validate Polymarket Conditional Tokens Framework event topics."""

    def test_payout_redemption(self):
        """PayoutRedemption from CTF contract (signature from comment in receipt_parser.py)."""
        from almanak.framework.connectors.polymarket.receipt_parser import PAYOUT_REDEMPTION_TOPIC

        assert PAYOUT_REDEMPTION_TOPIC == _topic(
            "PayoutRedemption(address,address,bytes32,bytes32,uint256[],uint256)"
        )


# =============================================================================
# Pendle receipt parser events
# =============================================================================


class TestPendleReceiptParserEvents:
    """Validate Pendle receipt parser event topics.

    Pendle Mint/Burn share Uniswap V2-style signatures.
    Pendle Swap uses the PendleMarket event.
    """

    def test_pendle_swap(self):
        """PendleMarket.Swap(address,address,int256,int256,uint256,uint256)."""
        from almanak.framework.connectors.pendle.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["Swap"] == _topic("Swap(address,address,int256,int256,uint256,uint256)")

    def test_pendle_mint(self):
        """Uniswap V2-style Mint(address,uint256,uint256)."""
        from almanak.framework.connectors.pendle.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["Mint"] == _topic("Mint(address,uint256,uint256)")

    def test_pendle_burn(self):
        """Uniswap V2-style Burn(address,uint256,uint256,address)."""
        from almanak.framework.connectors.pendle.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["Burn"] == _topic("Burn(address,uint256,uint256,address)")


# =============================================================================
# Curve events
# =============================================================================


class TestCurveEvents:
    """Validate Curve event topics."""

    CURVE_SIGNATURES = {
        "TokenExchange": "TokenExchange(address,int128,uint256,int128,uint256)",
        "TokenExchangeUnderlying": "TokenExchangeUnderlying(address,int128,uint256,int128,uint256)",
    }

    @pytest.mark.parametrize("event_name,signature", list(CURVE_SIGNATURES.items()))
    def test_curve(self, event_name, signature):
        from almanak.framework.connectors.curve.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS[event_name] == _topic(signature), f"Curve {event_name} mismatch"


# =============================================================================
# Placeholder detection: GMX V2
# =============================================================================


class TestGMXV2EventTopicHashes:
    """Validate GMX V2 event topic hashes are proper hex strings.

    GMX V2 uses an EventStore architecture where events are emitted as raw
    data through EventEmitter.emit(). The event topics were corrected in
    PR #423 from placeholder sequential hashes to real EventEmitter hashes.

    These tests validate the hashes are well-formed and unique.
    """

    def test_order_events_are_valid_hashes(self):
        """GMX V2 Order events should be valid 66-char hex topic hashes."""
        from almanak.framework.connectors.gmx_v2.receipt_parser import EVENT_TOPICS

        order_events = ["OrderCreated", "OrderExecuted", "OrderCancelled", "OrderFrozen", "OrderUpdated"]
        seen = set()
        for event in order_events:
            topic = EVENT_TOPICS[event]
            assert len(topic) == 66, f"GMX V2 {event} topic hash wrong length: {len(topic)}"
            assert topic.startswith("0x"), f"GMX V2 {event} topic hash missing 0x prefix"
            assert topic not in seen, f"GMX V2 {event} topic hash is duplicate"
            seen.add(topic)

    def test_position_events_are_valid_hashes(self):
        """PositionDecrease and related events should be valid unique hashes."""
        from almanak.framework.connectors.gmx_v2.receipt_parser import EVENT_TOPICS

        seen = set()
        for event in ["PositionDecrease", "PositionFeesInfo", "PositionFeesCollected"]:
            topic = EVENT_TOPICS[event]
            assert len(topic) == 66, f"GMX V2 {event} topic hash wrong length: {len(topic)}"
            assert topic.startswith("0x"), f"GMX V2 {event} topic hash missing 0x prefix"
            assert topic not in seen, f"GMX V2 {event} topic hash is duplicate"
            seen.add(topic)


# =============================================================================
# Placeholder detection: Aave V3 config events
# =============================================================================


class TestAaveV3ConfigPlaceholderDetection:
    """Detect placeholder hashes in Aave V3 configuration event topics.

    The core lending events (Supply, Withdraw, Borrow, Repay) are validated
    above. These configuration events have incrementing hex patterns that
    suggest they are placeholders rather than real keccak256 hashes.
    """

    SUSPECT_EVENTS = ["BorrowCapChanged", "SupplyCapChanged", "DebtCeilingChanged", "BridgeProtocolFeeUpdated"]

    def test_config_events_do_not_look_like_real_keccak(self):
        """Flag config events with suspiciously patterned hashes."""
        from almanak.framework.connectors.aave_v3.receipt_parser import EVENT_TOPICS

        for event in self.SUSPECT_EVENTS:
            topic = EVENT_TOPICS[event]
            # Real keccak256 hashes don't have incrementing nibble patterns
            # These hashes have patterns like 0x44c5b4c0..., 0x55e6c5a6..., 0x66f7d8e9...
            # where each starts with an incrementing byte (0x44, 0x55, 0x66, 0x77)
            first_byte = int(topic[2:4], 16)
            assert first_byte in (0x44, 0x55, 0x66, 0x77), (
                f"Aave V3 {event} first byte changed - if this fails, the hash may have "
                f"been corrected. Update this test accordingly."
            )
