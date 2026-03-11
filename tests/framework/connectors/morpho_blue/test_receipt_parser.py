"""Tests for Morpho Blue Receipt Parser (Refactored)."""

from almanak.framework.connectors.morpho_blue.receipt_parser import (
    MorphoBlueEventType,
    MorphoBlueReceiptParser,
)

# Test data
MORPHO_ADDRESS = "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb"
USER_ADDRESS = "0x742d35cc6634c0532925a3b844bc454e4438f44e"
MARKET_ID = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
USDC_ADDRESS = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
WETH_ADDRESS = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"


def create_supply_log(market_id, on_behalf_of, caller, assets, shares):
    """Create Supply log. caller is indexed (topic[2]), onBehalfOf is indexed (topic[3])."""
    data = f"0x{assets:064x}{shares:064x}"
    return {
        "address": MORPHO_ADDRESS,
        "topics": [
            "0xedf8870433c83823eb071d3df1caa8d008f12f6440918c20d75a3602cda30fe0",
            market_id,
            f"0x000000000000000000000000{caller[2:].lower()}",
            f"0x000000000000000000000000{on_behalf_of[2:].lower()}",
        ],
        "data": data,
        "logIndex": 0,
    }


def create_withdraw_log(market_id, on_behalf_of, receiver, caller, assets, shares):
    """Create Withdraw log."""
    data = f"0x{'00' * 12}{caller[2:].lower()}{assets:064x}{shares:064x}"
    return {
        "address": MORPHO_ADDRESS,
        "topics": [
            "0xa56fc0ad5702ec05ce63666221f796fb62437c32db1aa1aa075fc6484cf58fbf",
            market_id,
            f"0x000000000000000000000000{on_behalf_of[2:].lower()}",
            f"0x000000000000000000000000{receiver[2:].lower()}",
        ],
        "data": data,
        "logIndex": 1,
    }


def create_borrow_log(market_id, on_behalf_of, receiver, caller, assets, shares):
    """Create Borrow log."""
    data = f"0x{'00' * 12}{caller[2:].lower()}{assets:064x}{shares:064x}"
    return {
        "address": MORPHO_ADDRESS,
        "topics": [
            "0x570954540bed6b1304a87dfe815a5eda4a648f7097a16240dcd85c9b5fd42a43",
            market_id,
            f"0x000000000000000000000000{on_behalf_of[2:].lower()}",
            f"0x000000000000000000000000{receiver[2:].lower()}",
        ],
        "data": data,
        "logIndex": 2,
    }


def create_repay_log(market_id, on_behalf_of, caller, assets, shares):
    """Create Repay log. caller is indexed (topic[2]), onBehalfOf is indexed (topic[3])."""
    data = f"0x{assets:064x}{shares:064x}"
    return {
        "address": MORPHO_ADDRESS,
        "topics": [
            "0x52acb05cebbd3cd39715469f22afbf5a17496295ef3bc9bb5944056c63ccaa09",
            market_id,
            f"0x000000000000000000000000{caller[2:].lower()}",
            f"0x000000000000000000000000{on_behalf_of[2:].lower()}",
        ],
        "data": data,
        "logIndex": 3,
    }


def create_supply_collateral_log(market_id, on_behalf_of, caller, assets):
    """Create SupplyCollateral log. caller is indexed (topic[2]), onBehalfOf is indexed (topic[3])."""
    data = f"0x{assets:064x}"
    return {
        "address": MORPHO_ADDRESS,
        "topics": [
            "0xa3b9472a1399e17e123f3c2e6586c23e504184d504de59cdaa2b375e880c6184",
            market_id,
            f"0x000000000000000000000000{caller[2:].lower()}",
            f"0x000000000000000000000000{on_behalf_of[2:].lower()}",
        ],
        "data": data,
        "logIndex": 4,
    }


def create_withdraw_collateral_log(market_id, on_behalf_of, receiver, caller, assets):
    """Create WithdrawCollateral log."""
    data = f"0x{'00' * 12}{caller[2:].lower()}{assets:064x}"
    return {
        "address": MORPHO_ADDRESS,
        "topics": [
            "0xe80ebd7cc9223d7382aab2e0d1d6155c65651f83d53c8b9b06901d167e321142",
            market_id,
            f"0x000000000000000000000000{on_behalf_of[2:].lower()}",
            f"0x000000000000000000000000{receiver[2:].lower()}",
        ],
        "data": data,
        "logIndex": 5,
    }


def create_liquidate_log(
    market_id, borrower, caller, repaid_assets, repaid_shares, seized_assets, bad_debt_assets, bad_debt_shares
):
    """Create Liquidate log. caller is indexed (topic[2]), borrower is indexed (topic[3])."""
    data = f"0x{repaid_assets:064x}{repaid_shares:064x}{seized_assets:064x}{bad_debt_assets:064x}{bad_debt_shares:064x}"
    return {
        "address": MORPHO_ADDRESS,
        "topics": [
            "0xa4946ede45d0c6f06a0f5ce92c9ad3b4751452d2fe0e25010783bcab57a67e41",
            market_id,
            f"0x000000000000000000000000{caller[2:].lower()}",
            f"0x000000000000000000000000{borrower[2:].lower()}",
        ],
        "data": data,
        "logIndex": 6,
    }


def create_flash_loan_log(caller, token, assets):
    """Create FlashLoan log."""
    data = f"0x{assets:064x}"
    return {
        "address": MORPHO_ADDRESS,
        "topics": [
            "0xc76f1b4fe4396ac07a9fa55a415d4ca430e72651d37d3401f3bed7cb13fc4f12",
            f"0x000000000000000000000000{caller[2:].lower()}",
            f"0x000000000000000000000000{token[2:].lower()}",
        ],
        "data": data,
        "logIndex": 7,
    }


def create_create_market_log(market_id, loan_token, collateral_token, oracle, irm, lltv):
    """Create CreateMarket log."""
    data = (
        f"0x{'00' * 12}{loan_token[2:].lower()}"
        f"{'00' * 12}{collateral_token[2:].lower()}"
        f"{'00' * 12}{oracle[2:].lower()}"
        f"{'00' * 12}{irm[2:].lower()}"
        f"{lltv:064x}"
    )
    return {
        "address": MORPHO_ADDRESS,
        "topics": [
            "0xac4b2400f169220b0c0afdde7a0b32e775ba727ea1cb30b35f935cdaab8683ac",
            market_id,
        ],
        "data": data,
        "logIndex": 8,
    }


def create_set_authorization_log(caller, authorized, is_authorized):
    """Create SetAuthorization log.

    Morpho Blue signature: SetAuthorization(address indexed caller, address indexed authorized, bool isAuthorized)
    """
    data = f"0x{1 if is_authorized else 0:064x}"
    return {
        "address": MORPHO_ADDRESS,
        "topics": [
            "0x536fbf298df21953aa2d4dcf413275ef35de20bc0e6716be5e3b15be977b2a6e",
            f"0x000000000000000000000000{caller[2:].lower()}",
            f"0x000000000000000000000000{authorized[2:].lower()}",
        ],
        "data": data,
        "logIndex": 9,
    }


def create_accrue_interest_log(market_id, prev_borrow_rate, interest, fee_shares):
    """Create AccrueInterest log."""
    data = f"0x{prev_borrow_rate:064x}{interest:064x}{fee_shares:064x}"
    return {
        "address": MORPHO_ADDRESS,
        "topics": [
            "0x9d9bd501d0657d7dfe415f779a620a62b78bc508ddc0891fbbd8b7ac0f8fce87",
            market_id,
        ],
        "data": data,
        "logIndex": 10,
    }


def create_transfer_log(from_address, to_address, amount):
    """Create Transfer log."""
    data = f"0x{amount:064x}"
    return {
        "address": MORPHO_ADDRESS,
        "topics": [
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
            f"0x000000000000000000000000{from_address[2:].lower()}",
            f"0x000000000000000000000000{to_address[2:].lower()}",
        ],
        "data": data,
        "logIndex": 11,
    }


def create_approval_log(owner, spender, amount):
    """Create Approval log."""
    data = f"0x{amount:064x}"
    return {
        "address": MORPHO_ADDRESS,
        "topics": [
            "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",
            f"0x000000000000000000000000{owner[2:].lower()}",
            f"0x000000000000000000000000{spender[2:].lower()}",
        ],
        "data": data,
        "logIndex": 12,
    }


class TestMorphoBlueReceiptParser:
    """Tests for MorphoBlueReceiptParser."""

    def test_parse_supply_event(self):
        """Test parsing Supply event."""
        parser = MorphoBlueReceiptParser()

        assets = 1_000_000_000  # 1000 USDC
        shares = 999_500_000  # 999.5 shares

        receipt = {
            "transactionHash": "0x123",
            "blockNumber": 12345,
            "status": 1,
            "logs": [create_supply_log(MARKET_ID, USER_ADDRESS, USER_ADDRESS, assets, shares)],
            "gasUsed": 150000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1
        assert result.events[0].event_type == MorphoBlueEventType.SUPPLY
        assert result.events[0].data["market_id"] == MARKET_ID
        assert result.events[0].data["caller"] == USER_ADDRESS.lower()
        assert result.events[0].data["on_behalf_of"] == USER_ADDRESS.lower()
        assert result.events[0].data["assets"] == str(assets)
        assert result.events[0].data["shares"] == str(shares)

    def test_parse_withdraw_event(self):
        """Test parsing Withdraw event."""
        parser = MorphoBlueReceiptParser()

        assets = 500_000_000
        shares = 500_000_000
        receiver = "0x1234567890123456789012345678901234567890"

        receipt = {
            "transactionHash": "0x456",
            "blockNumber": 12346,
            "status": 1,
            "logs": [create_withdraw_log(MARKET_ID, USER_ADDRESS, receiver, USER_ADDRESS, assets, shares)],
            "gasUsed": 150000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 1
        assert result.events[0].event_type == MorphoBlueEventType.WITHDRAW
        assert result.events[0].data["market_id"] == MARKET_ID
        assert result.events[0].data["receiver"] == receiver.lower()
        assert result.events[0].data["assets"] == str(assets)
        assert result.events[0].data["shares"] == str(shares)

    def test_parse_borrow_event(self):
        """Test parsing Borrow event."""
        parser = MorphoBlueReceiptParser()

        assets = 1_000_000_000
        shares = 1_000_000_000

        receipt = {
            "transactionHash": "0x789",
            "blockNumber": 12347,
            "status": 1,
            "logs": [create_borrow_log(MARKET_ID, USER_ADDRESS, USER_ADDRESS, USER_ADDRESS, assets, shares)],
            "gasUsed": 150000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.events[0].event_type == MorphoBlueEventType.BORROW
        assert result.events[0].data["assets"] == str(assets)
        assert result.events[0].data["shares"] == str(shares)

    def test_parse_repay_event(self):
        """Test parsing Repay event."""
        parser = MorphoBlueReceiptParser()

        assets = 500_000_000
        shares = 500_000_000

        receipt = {
            "transactionHash": "0xabc",
            "blockNumber": 12348,
            "status": 1,
            "logs": [create_repay_log(MARKET_ID, USER_ADDRESS, USER_ADDRESS, assets, shares)],
            "gasUsed": 150000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.events[0].event_type == MorphoBlueEventType.REPAY
        assert result.events[0].data["market_id"] == MARKET_ID
        assert result.events[0].data["assets"] == str(assets)
        assert result.events[0].data["shares"] == str(shares)

    def test_parse_supply_collateral_event(self):
        """Test parsing SupplyCollateral event."""
        parser = MorphoBlueReceiptParser()

        assets = 1_000_000_000_000_000_000  # 1 WETH

        receipt = {
            "transactionHash": "0xdef",
            "blockNumber": 12349,
            "status": 1,
            "logs": [create_supply_collateral_log(MARKET_ID, USER_ADDRESS, USER_ADDRESS, assets)],
            "gasUsed": 150000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.events[0].event_type == MorphoBlueEventType.SUPPLY_COLLATERAL
        assert result.events[0].data["assets"] == str(assets)

    def test_parse_withdraw_collateral_event(self):
        """Test parsing WithdrawCollateral event."""
        parser = MorphoBlueReceiptParser()

        assets = 500_000_000_000_000_000  # 0.5 WETH
        receiver = "0x1234567890123456789012345678901234567890"

        receipt = {
            "transactionHash": "0x111",
            "blockNumber": 12350,
            "status": 1,
            "logs": [create_withdraw_collateral_log(MARKET_ID, USER_ADDRESS, receiver, USER_ADDRESS, assets)],
            "gasUsed": 150000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.events[0].event_type == MorphoBlueEventType.WITHDRAW_COLLATERAL
        assert result.events[0].data["receiver"] == receiver.lower()
        assert result.events[0].data["assets"] == str(assets)

    def test_parse_liquidate_event(self):
        """Test parsing Liquidate event."""
        parser = MorphoBlueReceiptParser()

        borrower = "0x9876543210987654321098765432109876543210"
        repaid_assets = 1_000_000_000
        repaid_shares = 1_000_000_000
        seized_assets = 1_100_000_000_000_000_000  # 1.1 WETH seized
        bad_debt_assets = 0
        bad_debt_shares = 0

        receipt = {
            "transactionHash": "0x222",
            "blockNumber": 12351,
            "status": 1,
            "logs": [
                create_liquidate_log(
                    MARKET_ID,
                    borrower,
                    USER_ADDRESS,
                    repaid_assets,
                    repaid_shares,
                    seized_assets,
                    bad_debt_assets,
                    bad_debt_shares,
                )
            ],
            "gasUsed": 200000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.events[0].event_type == MorphoBlueEventType.LIQUIDATE
        assert result.events[0].data["borrower"] == borrower.lower()
        assert result.events[0].data["repaid_assets"] == str(repaid_assets)
        assert result.events[0].data["seized_assets"] == str(seized_assets)
        assert result.events[0].data["bad_debt_assets"] == str(bad_debt_assets)

    def test_parse_flash_loan_event(self):
        """Test parsing FlashLoan event."""
        parser = MorphoBlueReceiptParser()

        assets = 10_000_000_000  # 10000 USDC

        receipt = {
            "transactionHash": "0x333",
            "blockNumber": 12352,
            "status": 1,
            "logs": [create_flash_loan_log(USER_ADDRESS, USDC_ADDRESS, assets)],
            "gasUsed": 100000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.events[0].event_type == MorphoBlueEventType.FLASH_LOAN
        assert result.events[0].data["caller"] == USER_ADDRESS.lower()
        assert result.events[0].data["token"] == USDC_ADDRESS.lower()
        assert result.events[0].data["assets"] == str(assets)

    def test_parse_create_market_event(self):
        """Test parsing CreateMarket event."""
        parser = MorphoBlueReceiptParser()

        oracle = "0x5f4ec3df9cbd43714fe2740f5e3616155c5b8419"
        irm = "0x870ac11d48b15db9a138cf899d20f13f79ba00bc"
        lltv = 860000000000000000  # 86% LTV

        receipt = {
            "transactionHash": "0x444",
            "blockNumber": 12353,
            "status": 1,
            "logs": [create_create_market_log(MARKET_ID, USDC_ADDRESS, WETH_ADDRESS, oracle, irm, lltv)],
            "gasUsed": 300000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.events[0].event_type == MorphoBlueEventType.CREATE_MARKET
        assert result.events[0].data["market_id"] == MARKET_ID
        assert result.events[0].data["loan_token"] == USDC_ADDRESS.lower()
        assert result.events[0].data["collateral_token"] == WETH_ADDRESS.lower()
        assert result.events[0].data["lltv"] == lltv

    def test_parse_set_authorization_event(self):
        """Test parsing SetAuthorization event."""
        parser = MorphoBlueReceiptParser()

        authorized = "0x1111111111111111111111111111111111111111"

        receipt = {
            "transactionHash": "0x555",
            "blockNumber": 12354,
            "status": 1,
            "logs": [create_set_authorization_log(USER_ADDRESS, authorized, True)],
            "gasUsed": 50000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.events[0].event_type == MorphoBlueEventType.SET_AUTHORIZATION
        assert result.events[0].data["authorized"] == authorized.lower()
        assert result.events[0].data["is_authorized"] is True

    def test_parse_accrue_interest_event(self):
        """Test parsing AccrueInterest event."""
        parser = MorphoBlueReceiptParser()

        prev_borrow_rate = 31709791983  # ~10% APY
        interest = 1_000_000  # 1 USDC interest
        fee_shares = 100_000  # 0.1 shares as fee

        receipt = {
            "transactionHash": "0x666",
            "blockNumber": 12355,
            "status": 1,
            "logs": [create_accrue_interest_log(MARKET_ID, prev_borrow_rate, interest, fee_shares)],
            "gasUsed": 50000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.events[0].event_type == MorphoBlueEventType.ACCRUE_INTEREST
        assert result.events[0].data["market_id"] == MARKET_ID
        assert result.events[0].data["prev_borrow_rate"] == str(prev_borrow_rate)
        assert result.events[0].data["interest"] == str(interest)
        assert result.events[0].data["fee_shares"] == str(fee_shares)

    def test_parse_transfer_event(self):
        """Test parsing Transfer event."""
        parser = MorphoBlueReceiptParser()

        to_address = "0x2222222222222222222222222222222222222222"
        amount = 1_000_000_000

        receipt = {
            "transactionHash": "0x777",
            "blockNumber": 12356,
            "status": 1,
            "logs": [create_transfer_log(USER_ADDRESS, to_address, amount)],
            "gasUsed": 50000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.events[0].event_type == MorphoBlueEventType.TRANSFER
        assert result.events[0].data["from"] == USER_ADDRESS.lower()
        assert result.events[0].data["to"] == to_address.lower()
        assert result.events[0].data["amount"] == str(amount)

    def test_parse_approval_event(self):
        """Test parsing Approval event."""
        parser = MorphoBlueReceiptParser()

        spender = "0x3333333333333333333333333333333333333333"
        amount = 1_000_000_000_000

        receipt = {
            "transactionHash": "0x888",
            "blockNumber": 12357,
            "status": 1,
            "logs": [create_approval_log(USER_ADDRESS, spender, amount)],
            "gasUsed": 50000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.events[0].event_type == MorphoBlueEventType.APPROVAL
        assert result.events[0].data["owner"] == USER_ADDRESS.lower()
        assert result.events[0].data["spender"] == spender.lower()
        assert result.events[0].data["amount"] == str(amount)

    def test_empty_logs(self):
        """Test parsing receipt with no logs."""
        parser = MorphoBlueReceiptParser()

        receipt = {
            "transactionHash": "0x999",
            "blockNumber": 12358,
            "status": 1,
            "logs": [],
            "gasUsed": 21000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert len(result.events) == 0

    def test_backward_compatibility(self):
        """Test backward compatibility methods."""
        parser = MorphoBlueReceiptParser()

        supply_topic = "0xedf8870433c83823eb071d3df1caa8d008f12f6440918c20d75a3602cda30fe0"
        assert parser.is_morpho_event(supply_topic) is True
        assert parser.get_event_type(supply_topic) == MorphoBlueEventType.SUPPLY

        # Test with event name
        assert parser.get_event_type("Supply") == MorphoBlueEventType.SUPPLY

        unknown_topic = "0x9999999999999999999999999999999999999999999999999999999999999999"
        assert parser.is_morpho_event(unknown_topic) is False
        assert parser.get_event_type(unknown_topic) == MorphoBlueEventType.UNKNOWN

    def test_bytes_transaction_hash(self):
        """Test handling bytes transaction hash."""
        parser = MorphoBlueReceiptParser()

        receipt = {
            "transactionHash": b"\x12\x34\x56\x78",
            "blockNumber": 12359,
            "status": 1,
            "logs": [create_supply_log(MARKET_ID, USER_ADDRESS, USER_ADDRESS, 1000000, 1000000)],
            "gasUsed": 150000,
        }

        result = parser.parse_receipt(receipt)

        assert result.success is True
        assert result.transaction_hash == "0x12345678"
