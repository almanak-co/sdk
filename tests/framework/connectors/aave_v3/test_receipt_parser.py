"""Tests for Aave V3 Receipt Parser (Refactored)."""

from almanak.framework.connectors.aave_v3.receipt_parser import (
    AaveV3EventType,
    AaveV3ReceiptParser,
)

# Test data
AAVE_POOL_ADDRESS = "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2"
USER_ADDRESS = "0x742d35cc6634c0532925a3b844bc454e4438f44e"
USDC_ADDRESS = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
WETH_ADDRESS = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"


def create_supply_log(reserve, user, on_behalf_of, amount, referral_code=0):
    """Create Supply log.

    Supply(address indexed reserve, address user, address indexed onBehalfOf,
           uint256 amount, uint16 referralCode)
    """
    data = f"0x{'00' * 12}{user[2:].lower()}{amount:064x}{referral_code:064x}"
    return {
        "address": AAVE_POOL_ADDRESS,
        "topics": [
            "0x2b627736bca15cd5381dcf80b0bf11fd197d01a037c52b927a881a10fb73ba61",
            f"0x000000000000000000000000{reserve[2:].lower()}",
            f"0x000000000000000000000000{on_behalf_of[2:].lower()}",
        ],
        "data": data,
        "logIndex": 0,
    }


def create_withdraw_log(reserve, user, to, amount):
    """Create Withdraw log.

    Withdraw(address indexed reserve, address indexed user, address indexed to,
             uint256 amount)
    """
    data = f"0x{amount:064x}"
    return {
        "address": AAVE_POOL_ADDRESS,
        "topics": [
            "0x3115d1449a7b732c986cba18244e897a450f61e1bb8d589cd2e69e6c8924f9f7",
            f"0x000000000000000000000000{reserve[2:].lower()}",
            f"0x000000000000000000000000{user[2:].lower()}",
            f"0x000000000000000000000000{to[2:].lower()}",
        ],
        "data": data,
        "logIndex": 1,
    }


def create_borrow_log(reserve, user, on_behalf_of, amount, rate_mode=2, borrow_rate=0, referral_code=0):
    """Create Borrow log.

    Borrow(address indexed reserve, address user, address indexed onBehalfOf,
           uint256 amount, uint256 interestRateMode, uint256 borrowRate,
           uint16 referralCode)
    """
    data = f"0x{'00' * 12}{user[2:].lower()}{amount:064x}{rate_mode:064x}{borrow_rate:064x}{referral_code:064x}"
    return {
        "address": AAVE_POOL_ADDRESS,
        "topics": [
            "0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0",
            f"0x000000000000000000000000{reserve[2:].lower()}",
            f"0x000000000000000000000000{on_behalf_of[2:].lower()}",
        ],
        "data": data,
        "logIndex": 2,
    }


def create_repay_log(reserve, user, repayer, amount, use_atokens=False):
    """Create Repay log.

    Repay(address indexed reserve, address indexed user, address indexed repayer,
          uint256 amount, bool useATokens)
    """
    use_atokens_val = 1 if use_atokens else 0
    data = f"0x{amount:064x}{use_atokens_val:064x}"
    return {
        "address": AAVE_POOL_ADDRESS,
        "topics": [
            "0xa534c8dbe71f871f9f3530e97a74601fea17b426cae02e1c5aee42c96c784051",
            f"0x000000000000000000000000{reserve[2:].lower()}",
            f"0x000000000000000000000000{user[2:].lower()}",
            f"0x000000000000000000000000{repayer[2:].lower()}",
        ],
        "data": data,
        "logIndex": 3,
    }


def create_flash_loan_log(target, asset, initiator, amount, rate_mode=0, premium=0, referral_code=0):
    """Create FlashLoan log.

    FlashLoan(address indexed target, address initiator, address indexed asset,
              uint256 amount, uint256 interestRateMode, uint256 premium,
              uint16 referralCode)
    """
    data = f"0x{'00' * 12}{initiator[2:].lower()}{amount:064x}{rate_mode:064x}{premium:064x}{referral_code:064x}"
    return {
        "address": AAVE_POOL_ADDRESS,
        "topics": [
            "0xefefaba5e921573100900a3ad9cf29f222d995fb3b6045797eaea7521bd8d6f0",
            f"0x000000000000000000000000{target[2:].lower()}",
            f"0x000000000000000000000000{asset[2:].lower()}",
        ],
        "data": data,
        "logIndex": 4,
    }


def create_liquidation_log(
    collateral_asset, debt_asset, user, debt_to_cover, liquidated_collateral, liquidator, receive_atoken=False
):
    """Create LiquidationCall log.

    LiquidationCall(address indexed collateralAsset, address indexed debtAsset,
                    address indexed user, uint256 debtToCover,
                    uint256 liquidatedCollateralAmount, address liquidator,
                    bool receiveAToken)
    """
    receive_val = 1 if receive_atoken else 0
    data = f"0x{debt_to_cover:064x}{liquidated_collateral:064x}{'00' * 12}{liquidator[2:].lower()}{receive_val:064x}"
    return {
        "address": AAVE_POOL_ADDRESS,
        "topics": [
            "0xe413a321e8681d831f4dbccbca790d2952b56f977908e45be37335533e005286",
            f"0x000000000000000000000000{collateral_asset[2:].lower()}",
            f"0x000000000000000000000000{debt_asset[2:].lower()}",
            f"0x000000000000000000000000{user[2:].lower()}",
        ],
        "data": data,
        "logIndex": 5,
    }


def create_receipt(logs, status=1):
    """Create receipt with logs."""
    return {
        "transactionHash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        "blockNumber": 12345678,
        "status": status,
        "logs": logs,
        "gasUsed": 100000,
    }


class TestAaveV3ReceiptParser:
    """Test Aave V3 receipt parser."""

    def test_parse_supply_event(self):
        """Test parsing Supply event."""
        parser = AaveV3ReceiptParser()

        amount = 1_000_000_000  # 1000 USDC (6 decimals)
        log = create_supply_log(
            reserve=USDC_ADDRESS,
            user=USER_ADDRESS,
            on_behalf_of=USER_ADDRESS,
            amount=amount,
        )
        receipt = create_receipt([log])

        result = parser.parse_receipt(receipt)

        assert result.success
        assert len(result.events) == 1
        assert result.events[0].event_type == AaveV3EventType.SUPPLY
        assert result.events[0].event_name == "Supply"

        # Check parsed data
        assert len(result.supplies) == 1
        supply = result.supplies[0]
        assert supply.reserve.lower() == USDC_ADDRESS.lower()
        assert supply.user.lower() == USER_ADDRESS.lower()
        assert supply.on_behalf_of.lower() == USER_ADDRESS.lower()
        assert supply.amount == amount

    def test_parse_withdraw_event(self):
        """Test parsing Withdraw event."""
        parser = AaveV3ReceiptParser()

        amount = 500_000_000  # 500 USDC
        log = create_withdraw_log(
            reserve=USDC_ADDRESS,
            user=USER_ADDRESS,
            to=USER_ADDRESS,
            amount=amount,
        )
        receipt = create_receipt([log])

        result = parser.parse_receipt(receipt)

        assert result.success
        assert len(result.events) == 1
        assert result.events[0].event_type == AaveV3EventType.WITHDRAW

        # Check parsed data
        assert len(result.withdraws) == 1
        withdraw = result.withdraws[0]
        assert withdraw.reserve.lower() == USDC_ADDRESS.lower()
        assert withdraw.user.lower() == USER_ADDRESS.lower()
        assert withdraw.to.lower() == USER_ADDRESS.lower()
        assert withdraw.amount == amount

    def test_parse_borrow_event(self):
        """Test parsing Borrow event."""
        parser = AaveV3ReceiptParser()

        amount = 1_000_000_000_000_000_000  # 1 ETH
        borrow_rate = 50_000_000_000_000_000_000_000_000  # 5% in ray (1e27)
        log = create_borrow_log(
            reserve=WETH_ADDRESS,
            user=USER_ADDRESS,
            on_behalf_of=USER_ADDRESS,
            amount=amount,
            rate_mode=2,  # variable rate
            borrow_rate=borrow_rate,
        )
        receipt = create_receipt([log])

        result = parser.parse_receipt(receipt)

        assert result.success
        assert len(result.events) == 1
        assert result.events[0].event_type == AaveV3EventType.BORROW

        # Check parsed data
        assert len(result.borrows) == 1
        borrow = result.borrows[0]
        assert borrow.reserve.lower() == WETH_ADDRESS.lower()
        assert borrow.user.lower() == USER_ADDRESS.lower()
        assert borrow.on_behalf_of.lower() == USER_ADDRESS.lower()
        assert borrow.amount == amount
        assert borrow.interest_rate_mode == 2
        assert borrow.is_variable_rate

    def test_parse_repay_event(self):
        """Test parsing Repay event."""
        parser = AaveV3ReceiptParser()

        amount = 500_000_000_000_000_000  # 0.5 ETH
        log = create_repay_log(
            reserve=WETH_ADDRESS,
            user=USER_ADDRESS,
            repayer=USER_ADDRESS,
            amount=amount,
            use_atokens=False,
        )
        receipt = create_receipt([log])

        result = parser.parse_receipt(receipt)

        assert result.success
        assert len(result.events) == 1
        assert result.events[0].event_type == AaveV3EventType.REPAY

        # Check parsed data
        assert len(result.repays) == 1
        repay = result.repays[0]
        assert repay.reserve.lower() == WETH_ADDRESS.lower()
        assert repay.user.lower() == USER_ADDRESS.lower()
        assert repay.repayer.lower() == USER_ADDRESS.lower()
        assert repay.amount == amount
        assert not repay.use_atokens

    def test_parse_flash_loan_event(self):
        """Test parsing FlashLoan event."""
        parser = AaveV3ReceiptParser()

        amount = 10_000_000_000  # 10,000 USDC
        premium = 9_000_000  # 9 USDC premium
        log = create_flash_loan_log(
            target=USER_ADDRESS,
            asset=USDC_ADDRESS,
            initiator=USER_ADDRESS,
            amount=amount,
            rate_mode=0,
            premium=premium,
        )
        receipt = create_receipt([log])

        result = parser.parse_receipt(receipt)

        assert result.success
        assert len(result.events) == 1
        assert result.events[0].event_type == AaveV3EventType.FLASH_LOAN

        # Check parsed data
        assert len(result.flash_loans) == 1
        flash_loan = result.flash_loans[0]
        assert flash_loan.target.lower() == USER_ADDRESS.lower()
        assert flash_loan.asset.lower() == USDC_ADDRESS.lower()
        assert flash_loan.initiator.lower() == USER_ADDRESS.lower()
        assert flash_loan.amount == amount
        assert flash_loan.premium == premium
        assert not flash_loan.opened_debt

    def test_parse_liquidation_event(self):
        """Test parsing LiquidationCall event."""
        parser = AaveV3ReceiptParser()

        debt_to_cover = 1_000_000_000  # 1000 USDC
        liquidated_collateral = 500_000_000_000_000_000  # 0.5 ETH
        liquidator = "0x9876543210987654321098765432109876543210"

        log = create_liquidation_log(
            collateral_asset=WETH_ADDRESS,
            debt_asset=USDC_ADDRESS,
            user=USER_ADDRESS,
            debt_to_cover=debt_to_cover,
            liquidated_collateral=liquidated_collateral,
            liquidator=liquidator,
            receive_atoken=False,
        )
        receipt = create_receipt([log])

        result = parser.parse_receipt(receipt)

        assert result.success
        assert len(result.events) == 1
        assert result.events[0].event_type == AaveV3EventType.LIQUIDATION_CALL

        # Check parsed data
        assert len(result.liquidations) == 1
        liquidation = result.liquidations[0]
        assert liquidation.collateral_asset.lower() == WETH_ADDRESS.lower()
        assert liquidation.debt_asset.lower() == USDC_ADDRESS.lower()
        assert liquidation.user.lower() == USER_ADDRESS.lower()
        assert liquidation.debt_to_cover == debt_to_cover
        assert liquidation.liquidated_collateral_amount == liquidated_collateral
        assert liquidation.liquidator.lower() == liquidator.lower()
        assert not liquidation.receive_atoken

    def test_parse_multiple_events(self):
        """Test parsing multiple events in one receipt."""
        parser = AaveV3ReceiptParser()

        supply_amount = 1_000_000_000
        borrow_amount = 500_000_000_000_000_000

        logs = [
            create_supply_log(
                reserve=USDC_ADDRESS,
                user=USER_ADDRESS,
                on_behalf_of=USER_ADDRESS,
                amount=supply_amount,
            ),
            create_borrow_log(
                reserve=WETH_ADDRESS,
                user=USER_ADDRESS,
                on_behalf_of=USER_ADDRESS,
                amount=borrow_amount,
            ),
        ]
        receipt = create_receipt(logs)

        result = parser.parse_receipt(receipt)

        assert result.success
        assert len(result.events) == 2
        assert result.events[0].event_type == AaveV3EventType.SUPPLY
        assert result.events[1].event_type == AaveV3EventType.BORROW

        # Check typed data
        assert len(result.supplies) == 1
        assert len(result.borrows) == 1
        assert result.supplies[0].amount == supply_amount
        assert result.borrows[0].amount == borrow_amount

    def test_parse_empty_receipt(self):
        """Test parsing receipt with no logs."""
        parser = AaveV3ReceiptParser()

        receipt = create_receipt([])
        result = parser.parse_receipt(receipt)

        assert result.success
        assert len(result.events) == 0
        assert len(result.supplies) == 0
        assert len(result.borrows) == 0

    def test_parse_reverted_transaction(self):
        """Test parsing reverted transaction."""
        parser = AaveV3ReceiptParser()

        log = create_supply_log(
            reserve=USDC_ADDRESS,
            user=USER_ADDRESS,
            on_behalf_of=USER_ADDRESS,
            amount=1_000_000_000,
        )
        receipt = create_receipt([log], status=0)

        result = parser.parse_receipt(receipt)

        # Should still succeed parsing but indicate transaction failed
        assert result.success
        assert result.error == "Transaction reverted"

    def test_parse_unknown_event(self):
        """Test parsing receipt with unknown event."""
        parser = AaveV3ReceiptParser()

        unknown_log = {
            "address": AAVE_POOL_ADDRESS,
            "topics": [
                "0x0000000000000000000000000000000000000000000000000000000000000000",
            ],
            "data": "0x",
            "logIndex": 0,
        }
        receipt = create_receipt([unknown_log])

        result = parser.parse_receipt(receipt)

        assert result.success
        assert len(result.events) == 0  # Unknown events are skipped

    def test_is_aave_event(self):
        """Test is_aave_event method."""
        parser = AaveV3ReceiptParser()

        # Known events
        assert parser.is_aave_event("0x2b627736bca15cd5381dcf80b0bf11fd197d01a037c52b927a881a10fb73ba61")  # Supply
        assert parser.is_aave_event("0x3115d1449a7b732c986cba18244e897a450f61e1bb8d589cd2e69e6c8924f9f7")  # Withdraw

        # Unknown event
        assert not parser.is_aave_event("0x0000000000000000000000000000000000000000000000000000000000000000")

    def test_get_event_type(self):
        """Test get_event_type method."""
        parser = AaveV3ReceiptParser()

        # Known events
        assert (
            parser.get_event_type("0x2b627736bca15cd5381dcf80b0bf11fd197d01a037c52b927a881a10fb73ba61")
            == AaveV3EventType.SUPPLY
        )
        assert (
            parser.get_event_type("0x3115d1449a7b732c986cba18244e897a450f61e1bb8d589cd2e69e6c8924f9f7")
            == AaveV3EventType.WITHDRAW
        )

        # Unknown event
        assert (
            parser.get_event_type("0x0000000000000000000000000000000000000000000000000000000000000000")
            == AaveV3EventType.UNKNOWN
        )
