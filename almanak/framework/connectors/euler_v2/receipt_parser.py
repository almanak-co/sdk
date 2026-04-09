"""Receipt parser for Euler V2 lending protocol.

Parses transaction receipts for Euler V2 operations (deposit, withdraw, borrow, repay).

Euler V2 uses ERC-4626 standard events for deposit/withdraw, plus custom events for borrow/repay:
- Deposit(sender, owner, assets, shares) -- ERC-4626 standard
- Withdraw(sender, receiver, owner, assets, shares) -- ERC-4626 standard
- Borrow(account, assets) -- Euler V2 specific (simpler than Silo V2)
- Repay(account, assets) -- Euler V2 specific (simpler than Silo V2)
"""

import logging
from dataclasses import dataclass, field

from almanak.framework.connectors.base import HexDecoder

logger = logging.getLogger(__name__)

# =============================================================================
# Event Topics (keccak256 of event signatures)
# =============================================================================

# ERC-4626 standard events (same as Silo V2)
# Deposit(address indexed sender, address indexed owner, uint256 assets, uint256 shares)
DEPOSIT_TOPIC = "0xdcbc1c05240f31ff3ad067ef1ee35ce4997762752e3a095284754544f4c709d7"

# Withdraw(address indexed sender, address indexed receiver, address indexed owner, uint256 assets, uint256 shares)
WITHDRAW_TOPIC = "0xfbde797d201c681b91056529119e0b02407c7bb96a4a2c75c01fc9667232c8db"

# Euler V2 specific events
# Borrow(address indexed account, uint256 assets)
BORROW_TOPIC = "0xcbc04eca7e9da35cb1393a6135a199ca52e450d5e9251cbd99f7847d33a36750"

# Repay(address indexed account, uint256 assets)
REPAY_TOPIC = "0x5c16de4f8b59bd9caf0f49a545f25819a895ed223294290b408242e72a594231"

# ERC-20 Transfer for balance verification
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# Approval event
APPROVAL_TOPIC = "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"


@dataclass
class EulerV2ParseResult:
    """Result of parsing an Euler V2 transaction receipt."""

    success: bool = False
    error: str | None = None

    # Deposit data
    deposit_amount: int = 0
    deposit_shares: int = 0

    # Withdraw data
    withdraw_amount: int = 0
    withdraw_shares: int = 0

    # Borrow data
    borrow_amount: int = 0

    # Repay data
    repay_amount: int = 0

    # Raw events
    events: list[dict] = field(default_factory=list)


class EulerV2ReceiptParser:
    """Parser for Euler V2 transaction receipts.

    Extracts deposit, withdraw, borrow, and repay data from on-chain events.
    """

    def __init__(self, underlying_decimals: int = 6) -> None:
        self.underlying_decimals = underlying_decimals

    def parse_receipt(
        self,
        receipt: dict,
        vault_address: str | None = None,
    ) -> EulerV2ParseResult:
        """Parse a transaction receipt for Euler V2 events.

        Args:
            receipt: Transaction receipt dict with 'logs' list
            vault_address: Optional vault address to filter events

        Returns:
            EulerV2ParseResult with extracted data
        """
        result = EulerV2ParseResult()

        try:
            logs = receipt.get("logs", [])
            if not logs:
                result.error = "No logs in receipt"
                return result

            vault_lower = vault_address.lower() if vault_address else None

            for log in logs:
                topics = log.get("topics", [])
                if not topics:
                    continue

                # Filter by vault address if provided
                log_address = log.get("address", "")
                if isinstance(log_address, bytes):
                    log_address = "0x" + log_address.hex()
                log_address = str(log_address).lower()

                if vault_lower and log_address != vault_lower:
                    continue

                topic0 = topics[0]
                if isinstance(topic0, bytes):
                    topic0 = "0x" + topic0.hex()
                topic0 = str(topic0).lower()

                data = log.get("data", "0x")
                if isinstance(data, bytes):
                    data = "0x" + data.hex()
                data = str(data)

                if topic0 == DEPOSIT_TOPIC.lower():
                    self._parse_deposit_event(result, data, topics)
                elif topic0 == WITHDRAW_TOPIC.lower():
                    self._parse_withdraw_event(result, data, topics)
                elif topic0 == BORROW_TOPIC.lower():
                    self._parse_borrow_event(result, data, topics)
                elif topic0 == REPAY_TOPIC.lower():
                    self._parse_repay_event(result, data, topics)

            # Mark success if we found any events
            if (
                result.deposit_amount > 0
                or result.withdraw_amount > 0
                or result.borrow_amount > 0
                or result.repay_amount > 0
            ):
                result.success = True

        except Exception as e:
            logger.exception(f"Error parsing Euler V2 receipt: {e}")
            result.error = str(e)

        return result

    def _parse_deposit_event(self, result: EulerV2ParseResult, data: str, topics: list) -> None:
        """Parse Deposit(sender, owner, assets, shares) event.

        Data layout: assets (uint256) + shares (uint256)
        """
        try:
            data_hex = data[2:] if data.startswith("0x") else data
            if len(data_hex) < 128:
                return

            assets = HexDecoder.decode_uint256(data_hex[:64])
            shares = HexDecoder.decode_uint256(data_hex[64:128])

            result.deposit_amount += assets
            result.deposit_shares += shares
            result.events.append(
                {
                    "event": "Deposit",
                    "assets": assets,
                    "shares": shares,
                }
            )

            logger.info(f"Euler V2 Deposit: assets={assets}, shares={shares}")
        except Exception as e:
            logger.warning(f"Failed to parse Deposit event: {e}")

    def _parse_withdraw_event(self, result: EulerV2ParseResult, data: str, topics: list) -> None:
        """Parse Withdraw(sender, receiver, owner, assets, shares) event.

        Data layout: assets (uint256) + shares (uint256)
        """
        try:
            data_hex = data[2:] if data.startswith("0x") else data
            if len(data_hex) < 128:
                return

            assets = HexDecoder.decode_uint256(data_hex[:64])
            shares = HexDecoder.decode_uint256(data_hex[64:128])

            result.withdraw_amount += assets
            result.withdraw_shares += shares
            result.events.append(
                {
                    "event": "Withdraw",
                    "assets": assets,
                    "shares": shares,
                }
            )

            logger.info(f"Euler V2 Withdraw: assets={assets}, shares={shares}")
        except Exception as e:
            logger.warning(f"Failed to parse Withdraw event: {e}")

    def _parse_borrow_event(self, result: EulerV2ParseResult, data: str, topics: list) -> None:
        """Parse Borrow(account, assets) event.

        Euler V2 Borrow is simpler than Silo V2 — only has assets in data (no shares).
        Data layout: assets (uint256)
        """
        try:
            data_hex = data[2:] if data.startswith("0x") else data
            if len(data_hex) < 64:
                return

            assets = HexDecoder.decode_uint256(data_hex[:64])

            result.borrow_amount += assets
            result.events.append(
                {
                    "event": "Borrow",
                    "assets": assets,
                }
            )

            logger.info(f"Euler V2 Borrow: assets={assets}")
        except Exception as e:
            logger.warning(f"Failed to parse Borrow event: {e}")

    def _parse_repay_event(self, result: EulerV2ParseResult, data: str, topics: list) -> None:
        """Parse Repay(account, assets) event.

        Euler V2 Repay is simpler than Silo V2 — only has assets in data (no shares).
        Data layout: assets (uint256)
        """
        try:
            data_hex = data[2:] if data.startswith("0x") else data
            if len(data_hex) < 64:
                return

            assets = HexDecoder.decode_uint256(data_hex[:64])

            result.repay_amount += assets
            result.events.append(
                {
                    "event": "Repay",
                    "assets": assets,
                }
            )

            logger.info(f"Euler V2 Repay: assets={assets}")
        except Exception as e:
            logger.warning(f"Failed to parse Repay event: {e}")

    # =========================================================================
    # Extraction methods for ResultEnricher
    # =========================================================================

    def extract_supply_data(self, receipt: dict, vault_address: str | None = None) -> dict | None:
        """Extract supply data from receipt for ResultEnricher."""
        result = self.parse_receipt(receipt, vault_address=vault_address)
        if result.deposit_amount > 0:
            return {
                "supply_amount": result.deposit_amount,
                "shares_minted": result.deposit_shares,
            }
        return None

    def extract_borrow_data(self, receipt: dict, vault_address: str | None = None) -> dict | None:
        """Extract borrow data from receipt for ResultEnricher."""
        result = self.parse_receipt(receipt, vault_address=vault_address)
        if result.borrow_amount > 0:
            return {
                "borrow_amount": result.borrow_amount,
            }
        return None

    def extract_withdraw_data(self, receipt: dict, vault_address: str | None = None) -> dict | None:
        """Extract withdraw data from receipt for ResultEnricher."""
        result = self.parse_receipt(receipt, vault_address=vault_address)
        if result.withdraw_amount > 0:
            return {
                "withdraw_amount": result.withdraw_amount,
                "shares_redeemed": result.withdraw_shares,
            }
        return None

    def extract_repay_data(self, receipt: dict, vault_address: str | None = None) -> dict | None:
        """Extract repay data from receipt for ResultEnricher."""
        result = self.parse_receipt(receipt, vault_address=vault_address)
        if result.repay_amount > 0:
            return {
                "repay_amount": result.repay_amount,
            }
        return None
