"""Validate event topic hashes against keccak256 of canonical Solidity signatures.

Each protocol's receipt parser defines event topic constants. This test verifies
that every topic hash is the correct keccak256 of the event's ABI signature.

Bugs caught: VIB-193 (8 wrong hashes across Morpho, Compound, Aave, Polymarket).
"""

from web3 import Web3

# ============================================================================
# Morpho Blue
# ============================================================================


def _keccak(sig: str) -> str:
    """Compute keccak256 topic hash for an event signature."""
    return "0x" + Web3.keccak(text=sig).hex()


class TestMorphoBlueTopics:
    """Validate Morpho Blue event topic hashes."""

    def test_supply(self):
        from almanak.framework.connectors.morpho_blue.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["Supply"] == _keccak("Supply(bytes32,address,address,uint256,uint256)")

    def test_withdraw(self):
        from almanak.framework.connectors.morpho_blue.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["Withdraw"] == _keccak("Withdraw(bytes32,address,address,address,uint256,uint256)")

    def test_borrow(self):
        from almanak.framework.connectors.morpho_blue.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["Borrow"] == _keccak("Borrow(bytes32,address,address,address,uint256,uint256)")

    def test_repay(self):
        from almanak.framework.connectors.morpho_blue.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["Repay"] == _keccak("Repay(bytes32,address,address,uint256,uint256)")

    def test_supply_collateral(self):
        from almanak.framework.connectors.morpho_blue.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["SupplyCollateral"] == _keccak("SupplyCollateral(bytes32,address,address,uint256)")

    def test_withdraw_collateral(self):
        from almanak.framework.connectors.morpho_blue.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["WithdrawCollateral"] == _keccak(
            "WithdrawCollateral(bytes32,address,address,address,uint256)"
        )

    def test_liquidate(self):
        from almanak.framework.connectors.morpho_blue.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["Liquidate"] == _keccak(
            "Liquidate(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)"
        )

    def test_flash_loan(self):
        from almanak.framework.connectors.morpho_blue.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["FlashLoan"] == _keccak("FlashLoan(address,address,uint256)")

    def test_create_market(self):
        from almanak.framework.connectors.morpho_blue.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["CreateMarket"] == _keccak(
            "CreateMarket(bytes32,(address,address,address,address,uint256))"
        )

    def test_set_authorization(self):
        from almanak.framework.connectors.morpho_blue.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["SetAuthorization"] == _keccak("SetAuthorization(address,address,address,bool)")

    def test_accrue_interest(self):
        from almanak.framework.connectors.morpho_blue.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["AccrueInterest"] == _keccak("AccrueInterest(bytes32,uint256,uint256,uint256)")

    def test_transfer(self):
        from almanak.framework.connectors.morpho_blue.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["Transfer"] == _keccak("Transfer(address,address,uint256)")

    def test_approval(self):
        from almanak.framework.connectors.morpho_blue.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["Approval"] == _keccak("Approval(address,address,uint256)")


# ============================================================================
# Compound V3
# ============================================================================


class TestCompoundV3Topics:
    """Validate Compound V3 event topic hashes."""

    def test_supply(self):
        from almanak.framework.connectors.compound_v3.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["Supply"] == _keccak("Supply(address,address,uint256)")

    def test_withdraw(self):
        from almanak.framework.connectors.compound_v3.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["Withdraw"] == _keccak("Withdraw(address,address,uint256)")

    def test_supply_collateral(self):
        from almanak.framework.connectors.compound_v3.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["SupplyCollateral"] == _keccak("SupplyCollateral(address,address,address,uint256)")

    def test_withdraw_collateral(self):
        from almanak.framework.connectors.compound_v3.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["WithdrawCollateral"] == _keccak("WithdrawCollateral(address,address,address,uint256)")

    def test_transfer_collateral(self):
        from almanak.framework.connectors.compound_v3.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["TransferCollateral"] == _keccak("TransferCollateral(address,address,address,uint256)")

    def test_absorb_debt(self):
        from almanak.framework.connectors.compound_v3.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["AbsorbDebt"] == _keccak("AbsorbDebt(address,address,uint256,uint256)")

    def test_absorb_collateral(self):
        from almanak.framework.connectors.compound_v3.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["AbsorbCollateral"] == _keccak(
            "AbsorbCollateral(address,address,address,uint256,uint256)"
        )

    def test_buy_collateral(self):
        from almanak.framework.connectors.compound_v3.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["BuyCollateral"] == _keccak("BuyCollateral(address,address,uint256,uint256)")

    def test_pause_action(self):
        from almanak.framework.connectors.compound_v3.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["PauseAction"] == _keccak("PauseAction(bool,bool,bool,bool,bool)")

    def test_withdraw_reserves(self):
        from almanak.framework.connectors.compound_v3.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["WithdrawReserves"] == _keccak("WithdrawReserves(address,uint256)")

    def test_transfer(self):
        from almanak.framework.connectors.compound_v3.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["Transfer"] == _keccak("Transfer(address,address,uint256)")

    def test_approval(self):
        from almanak.framework.connectors.compound_v3.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["Approval"] == _keccak("Approval(address,address,uint256)")


# ============================================================================
# Aave V3
# ============================================================================


class TestAaveV3Topics:
    """Validate Aave V3 event topic hashes."""

    def test_supply(self):
        from almanak.framework.connectors.aave_v3.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["Supply"] == _keccak("Supply(address,address,address,uint256,uint16)")

    def test_withdraw(self):
        from almanak.framework.connectors.aave_v3.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["Withdraw"] == _keccak("Withdraw(address,address,address,uint256)")

    def test_borrow(self):
        from almanak.framework.connectors.aave_v3.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["Borrow"] == _keccak("Borrow(address,address,address,uint256,uint8,uint256,uint16)")

    def test_repay(self):
        from almanak.framework.connectors.aave_v3.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["Repay"] == _keccak("Repay(address,address,address,uint256,bool)")

    def test_flash_loan(self):
        from almanak.framework.connectors.aave_v3.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["FlashLoan"] == _keccak(
            "FlashLoan(address,address,address,uint256,uint8,uint256,uint16)"
        )

    def test_liquidation_call(self):
        from almanak.framework.connectors.aave_v3.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["LiquidationCall"] == _keccak(
            "LiquidationCall(address,address,address,uint256,uint256,address,bool)"
        )

    def test_reserve_data_updated(self):
        from almanak.framework.connectors.aave_v3.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["ReserveDataUpdated"] == _keccak(
            "ReserveDataUpdated(address,uint256,uint256,uint256,uint256,uint256)"
        )

    def test_isolation_mode_total_debt_updated(self):
        from almanak.framework.connectors.aave_v3.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["IsolationModeTotalDebtUpdated"] == _keccak(
            "IsolationModeTotalDebtUpdated(address,uint256)"
        )

    def test_user_emode_set(self):
        from almanak.framework.connectors.aave_v3.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["UserEModeSet"] == _keccak("UserEModeSet(address,uint8)")

    def test_reserve_used_as_collateral_enabled(self):
        from almanak.framework.connectors.aave_v3.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["ReserveUsedAsCollateralEnabled"] == _keccak(
            "ReserveUsedAsCollateralEnabled(address,address)"
        )

    def test_reserve_used_as_collateral_disabled(self):
        from almanak.framework.connectors.aave_v3.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["ReserveUsedAsCollateralDisabled"] == _keccak(
            "ReserveUsedAsCollateralDisabled(address,address)"
        )

    def test_transfer(self):
        from almanak.framework.connectors.aave_v3.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["Transfer"] == _keccak("Transfer(address,address,uint256)")

    def test_approval(self):
        from almanak.framework.connectors.aave_v3.receipt_parser import EVENT_TOPICS

        assert EVENT_TOPICS["Approval"] == _keccak("Approval(address,address,uint256)")


# ============================================================================
# Polymarket
# ============================================================================


class TestPolymarketTopics:
    """Validate Polymarket/CTF event topic hashes."""

    def test_transfer_single(self):
        from almanak.framework.connectors.polymarket.receipt_parser import TRANSFER_SINGLE_TOPIC

        assert TRANSFER_SINGLE_TOPIC == _keccak("TransferSingle(address,address,address,uint256,uint256)")

    def test_transfer_batch(self):
        from almanak.framework.connectors.polymarket.receipt_parser import TRANSFER_BATCH_TOPIC

        assert TRANSFER_BATCH_TOPIC == _keccak("TransferBatch(address,address,address,uint256[],uint256[])")

    def test_payout_redemption(self):
        from almanak.framework.connectors.polymarket.receipt_parser import PAYOUT_REDEMPTION_TOPIC

        assert PAYOUT_REDEMPTION_TOPIC == _keccak(
            "PayoutRedemption(address,address,bytes32,bytes32,uint256[],uint256)"
        )

    def test_erc20_transfer(self):
        from almanak.framework.connectors.polymarket.receipt_parser import ERC20_TRANSFER_TOPIC

        assert ERC20_TRANSFER_TOPIC == _keccak("Transfer(address,address,uint256)")

    def test_approval_for_all(self):
        from almanak.framework.connectors.polymarket.receipt_parser import APPROVAL_FOR_ALL_TOPIC

        assert APPROVAL_FOR_ALL_TOPIC == _keccak("ApprovalForAll(address,address,bool)")

    def test_erc20_approval(self):
        from almanak.framework.connectors.polymarket.receipt_parser import ERC20_APPROVAL_TOPIC

        assert ERC20_APPROVAL_TOPIC == _keccak("Approval(address,address,uint256)")
