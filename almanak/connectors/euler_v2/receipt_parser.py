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
from typing import TYPE_CHECKING, Any

from almanak.connectors._strategy_base.base import HexDecoder

if TYPE_CHECKING:
    from almanak.connectors._strategy_base.primitive_money_leg import PrimitiveMoneyLegs

logger = logging.getLogger(__name__)

# VIB-5218 — vault_address (lower) -> (underlying_symbol, underlying_decimals),
# derived once from the connector's static vault registry. Euler V2's ERC-4626
# Deposit / Withdraw events carry only ``assets`` / ``shares`` — NOT the
# underlying token — so the principal token identity is recovered from the
# emitting *vault* address (the event's emitter) via this map. Lazily built
# (the adapter import is heavy and must not run at parser-module import time)
# and cached.
_VAULT_UNDERLYING_CACHE: dict[str, tuple[str, int]] | None = None


def _vault_underlying_map() -> dict[str, tuple[str, int]]:
    """Return the cached ``vault_address(lower) -> (underlying_symbol, decimals)`` map."""
    global _VAULT_UNDERLYING_CACHE
    if _VAULT_UNDERLYING_CACHE is None:
        built: dict[str, tuple[str, int]] = {}
        try:
            from almanak.connectors.euler_v2.adapter import EULER_V2_VAULTS_BY_CHAIN

            for chain_vaults in EULER_V2_VAULTS_BY_CHAIN.values():
                for info in chain_vaults.values():
                    addr = str(info.get("vault_address", "")).lower()
                    symbol = info.get("underlying_symbol") or ""
                    decimals = info.get("decimals")
                    if addr and symbol and isinstance(decimals, int):
                        built[addr] = (symbol, decimals)
        except Exception as exc:  # noqa: BLE001 — degrade to legacy (no legs) on the accounting path
            logger.debug("euler_v2: could not build vault->underlying map: %s", exc)
        _VAULT_UNDERLYING_CACHE = built
    return _VAULT_UNDERLYING_CACHE


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

    # VIB-5218 — declare the lending money legs (one PRINCIPAL leg) as a typed
    # ``PrimitiveMoneyLegs`` via ``extract_primitive_money_legs``, surfaced under
    # ``extracted_data["primitive_money_legs"]`` by the enricher's connector-owned
    # ``EXTRA_EXTRACTIONS_BY_INTENT`` merge. The US-009 ledger dispatcher prefers
    # that typed fact over its legacy guesser, so the Euler V2
    # SUPPLY/WITHDRAW/BORROW/REPAY ledger row carries a real ``token_in`` (the
    # vault's underlying symbol) instead of the empty string that made the lending
    # handler resolve ``UNKNOWN`` → ``deployed_capital_usd = 0``.
    EXTRA_EXTRACTIONS_BY_INTENT: dict[str, tuple[str, ...]] = {
        "SUPPLY": ("primitive_money_legs",),
        "WITHDRAW": ("primitive_money_legs",),
        "BORROW": ("primitive_money_legs",),
        "REPAY": ("primitive_money_legs",),
    }

    def __init__(self, underlying_decimals: int = 6, chain: str | None = None, **kwargs: Any) -> None:
        self.underlying_decimals = underlying_decimals
        # Threaded by the ResultEnricher; retained for parity with the other
        # lending parsers (the vault->underlying map already carries decimals, so
        # Euler does not need a token-resolver round-trip).
        self.chain = (chain or "").lower()

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
            logger.exception("Error parsing Euler V2 receipt: %s", e)
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

            logger.info("Euler V2 Deposit: assets=%s, shares=%s", assets, shares)
        except Exception as e:
            logger.warning("Failed to parse Deposit event: %s", e)

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

            logger.info("Euler V2 Withdraw: assets=%s, shares=%s", assets, shares)
        except Exception as e:
            logger.warning("Failed to parse Withdraw event: %s", e)

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

            logger.info("Euler V2 Borrow: assets=%s", assets)
        except Exception as e:
            logger.warning("Failed to parse Borrow event: %s", e)

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

            logger.info("Euler V2 Repay: assets=%s", assets)
        except Exception as e:
            logger.warning("Failed to parse Repay event: %s", e)

    # =========================================================================
    # Extraction methods for ResultEnricher
    #
    # Method names MUST match the enricher's EXTRACTION_SPECS field names:
    #   extract_{field}(receipt) -> value | None
    # E.g., SUPPLY spec has "supply_amount" -> enricher calls extract_supply_amount()
    # =========================================================================

    def extract_supply_amount(self, receipt: dict) -> int | None:
        """Extract supply amount from receipt for ResultEnricher.

        Called by ResultEnricher for SUPPLY intents (field: supply_amount).
        """
        result = self.parse_receipt(receipt)
        if result.deposit_amount > 0:
            return result.deposit_amount
        return None

    def extract_borrow_amount(self, receipt: dict) -> int | None:
        """Extract borrow amount from receipt for ResultEnricher.

        Called by ResultEnricher for BORROW intents (field: borrow_amount).
        """
        result = self.parse_receipt(receipt)
        if result.borrow_amount > 0:
            return result.borrow_amount
        return None

    def extract_withdraw_amount(self, receipt: dict) -> int | None:
        """Extract withdraw amount from receipt for ResultEnricher.

        Called by ResultEnricher for WITHDRAW intents (field: withdraw_amount).
        """
        result = self.parse_receipt(receipt)
        if result.withdraw_amount > 0:
            return result.withdraw_amount
        return None

    def extract_repay_amount(self, receipt: dict) -> int | None:
        """Extract repay amount from receipt for ResultEnricher.

        Called by ResultEnricher for REPAY intents (field: repay_amount).
        """
        result = self.parse_receipt(receipt)
        if result.repay_amount > 0:
            return result.repay_amount
        return None

    def extract_primitive_money_legs(self, receipt: dict) -> "PrimitiveMoneyLegs | None":
        """VIB-5218 — declare the lending money legs as a typed ``PrimitiveMoneyLegs``
        the ledger dispatcher consumes directly (the Lido / Curve / Pendle pattern).

        Inverts the legacy control flow (blueprint 27 §6.6): instead of the ledger
        reverse-engineering a SUPPLY / WITHDRAW / BORROW / REPAY's ``token_in`` from
        the intent (which it never matched for lending — landing an empty
        ``token_in`` → ``asset = "UNKNOWN"`` → no FIFO lot → ``deployed_capital_usd =
        0``), the connector DECLARES the principal asset it actually moved on-chain.

        Euler V2's ERC-4626 ``Deposit`` / ``Withdraw`` events (and the Euler-specific
        ``Borrow`` / ``Repay``) carry only ``assets`` / ``shares`` — NOT the
        underlying token. The underlying is recovered from the *emitting vault*
        address (the event's emitter) via the connector's static vault registry,
        which also carries the underlying's decimals — so no token-resolver round-trip
        is needed. Emits ONE PRINCIPAL leg (token = the underlying symbol, amount = a
        human-unit ``MeasuredMoney``), which the dispatcher projects onto ``token_in``
        / ``amount_in``.

        Returns ``None`` (→ legacy fallback, byte-identical rows) when the receipt
        carries no Euler V2 lending event or the emitting vault is not in the static
        registry — so unknown-vault receipts degrade unchanged (Empty ≠ Zero). Never
        raises: any failure degrades to ``None`` rather than halting the live
        accounting writer.
        """
        from almanak.connectors._strategy_base.lending_money_legs import lending_principal_legs

        # A BORROW / REPAY action compiles to a SINGLE EVC-batch tx that emits BOTH
        # a collateral event (ERC-4626 ``Deposit`` / ``Withdraw`` on the collateral
        # vault) AND a loan event (Euler ``Borrow`` / ``Repay`` on the borrow vault).
        # The principal of a BORROW / REPAY is the LOAN token, never the collateral,
        # so the loan event wins when both are present. A pure SUPPLY / WITHDRAW has
        # no loan event and books the deposit / withdraw vault as before (VIB-5218).
        _LOAN_TOPICS = {BORROW_TOPIC.lower(), REPAY_TOPIC.lower()}
        _COLLATERAL_TOPICS = {DEPOSIT_TOPIC.lower(), WITHDRAW_TOPIC.lower()}
        try:
            logs = receipt.get("logs", [])
            loan_log: dict | None = None
            collateral_log: dict | None = None
            for log in logs:
                topics = log.get("topics", [])
                if not topics:
                    continue
                topic0 = topics[0]
                if isinstance(topic0, bytes):
                    topic0 = "0x" + topic0.hex()
                topic0 = str(topic0).lower()
                if topic0 in _LOAN_TOPICS and loan_log is None:
                    loan_log = log
                elif topic0 in _COLLATERAL_TOPICS and collateral_log is None:
                    collateral_log = log

            # Loan event (BORROW / REPAY) is the principal; collateral is the
            # fallback for a pure SUPPLY / WITHDRAW with no loan event.
            chosen = loan_log if loan_log is not None else collateral_log
            if chosen is None:
                return None

            # Emitter of the chosen lending event IS the vault — recover its underlying.
            vault_addr = chosen.get("address", "")
            if isinstance(vault_addr, bytes):
                vault_addr = "0x" + vault_addr.hex()
            vault_addr = str(vault_addr).lower()
            underlying = _vault_underlying_map().get(vault_addr)
            if underlying is None:
                # Vault not in the static registry — fall back to legacy rather
                # than declare a token-less leg.
                return None
            symbol, decimals = underlying

            # ``assets`` is the first 32-byte word of data for all four events.
            data = chosen.get("data", "0x")
            if isinstance(data, bytes):
                data = "0x" + data.hex()
            data_hex = data[2:] if str(data).startswith("0x") else str(data)
            if len(data_hex) < 64:
                return None
            assets = HexDecoder.decode_uint256(data_hex[:64])
            return lending_principal_legs(token_symbol=symbol, raw_amount=assets, decimals=decimals)
        except Exception as exc:  # noqa: BLE001 — never halt the accounting writer
            logger.warning("Failed to extract primitive_money_legs: %s", exc)
            return None

    # Legacy methods kept for backward compatibility
    def extract_supply_data(self, receipt: dict, vault_address: str | None = None) -> dict | None:
        """Extract supply data from receipt (legacy API)."""
        result = self.parse_receipt(receipt, vault_address=vault_address)
        if result.deposit_amount > 0:
            return {
                "supply_amount": result.deposit_amount,
                "shares_minted": result.deposit_shares,
            }
        return None

    def extract_borrow_data(self, receipt: dict, vault_address: str | None = None) -> dict | None:
        """Extract borrow data from receipt (legacy API)."""
        result = self.parse_receipt(receipt, vault_address=vault_address)
        if result.borrow_amount > 0:
            return {
                "borrow_amount": result.borrow_amount,
            }
        return None

    def extract_withdraw_data(self, receipt: dict, vault_address: str | None = None) -> dict | None:
        """Extract withdraw data from receipt (legacy API)."""
        result = self.parse_receipt(receipt, vault_address=vault_address)
        if result.withdraw_amount > 0:
            return {
                "withdraw_amount": result.withdraw_amount,
                "shares_redeemed": result.withdraw_shares,
            }
        return None

    def extract_repay_data(self, receipt: dict, vault_address: str | None = None) -> dict | None:
        """Extract repay data from receipt (legacy API)."""
        result = self.parse_receipt(receipt, vault_address=vault_address)
        if result.repay_amount > 0:
            return {
                "repay_amount": result.repay_amount,
            }
        return None
