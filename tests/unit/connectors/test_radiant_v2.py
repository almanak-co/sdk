"""Tests for Radiant V2 lending connector integration.

Pins the protocol's capabilities, selector mapping, pool-address registry,
and absence on arbitrum (regression guard for #1842 / #1847 / #1889).
"""

from almanak.framework.intents.compiler import (
    AAVE_COMPATIBLE_PROTOCOLS,
    AAVE_SUPPLY_SELECTOR,
    AAVE_V2_DEPOSIT_SELECTOR,
    AAVE_WITHDRAW_SELECTOR,
    LENDING_POOL_ADDRESSES,
    AaveV3Adapter,
)
from almanak.framework.intents.compiler_constants import LENDING_POOL_DATA_PROVIDERS
from almanak.framework.intents.vocabulary import PROTOCOL_CAPABILITIES


class TestRadiantV2ProtocolCapabilities:
    """Radiant V2 is registered with correct capabilities."""

    def test_radiant_v2_in_capabilities(self):
        """radiant_v2 must be in PROTOCOL_CAPABILITIES."""
        assert "radiant_v2" in PROTOCOL_CAPABILITIES

    def test_radiant_v2_supports_supply(self):
        """radiant_v2 must support supply operation."""
        assert "supply" in PROTOCOL_CAPABILITIES["radiant_v2"]["operations"]

    def test_radiant_v2_supports_withdraw(self):
        """radiant_v2 must support withdraw operation."""
        assert "withdraw" in PROTOCOL_CAPABILITIES["radiant_v2"]["operations"]

    def test_radiant_v2_supports_borrow(self):
        """radiant_v2 must support borrow operation."""
        assert "borrow" in PROTOCOL_CAPABILITIES["radiant_v2"]["operations"]

    def test_radiant_v2_supports_repay(self):
        """radiant_v2 must support repay operation."""
        assert "repay" in PROTOCOL_CAPABILITIES["radiant_v2"]["operations"]

    def test_radiant_v2_supports_interest_rate_mode(self):
        """radiant_v2 (Aave V2 fork) supports interest rate mode selection."""
        assert PROTOCOL_CAPABILITIES["radiant_v2"]["supports_interest_rate_mode"] is True

    def test_radiant_v2_supports_collateral_toggle(self):
        """radiant_v2 supports setUserUseReserveAsCollateral."""
        assert PROTOCOL_CAPABILITIES["radiant_v2"]["supports_collateral_toggle"] is True


class TestRadiantV2PoolAddresses:
    """Radiant V2 pool addresses are configured only for supported chains."""

    def test_ethereum_pool_address(self):
        """Radiant V2 LendingPool on Ethereum."""
        assert "radiant_v2" in LENDING_POOL_ADDRESSES["ethereum"]
        assert LENDING_POOL_ADDRESSES["ethereum"]["radiant_v2"] == "0xA950974f64aA33f27F6C5e017eEE93BF7588ED07"

    def test_arbitrum_radiant_v2_not_registered(self):
        """Regression guard for issues #1842 / #1847 / #1889.

        The Radiant V2 LendingPool proxy on Arbitrum
        (``0xF4B1486DD74D07706052A33d31d7c0AAFD0659E1``) was reduced to a stub
        implementation after the October 2024 attack. Every non-admin call
        reverts on-chain, so routing user funds through it is unsafe. This
        test fails fast if a future contributor silently re-introduces the
        entry without acknowledging that the pool is permanently dead.
        """
        chain_pools = LENDING_POOL_ADDRESSES.get("arbitrum", {})
        assert "radiant_v2" not in chain_pools, (
            "radiant_v2 must not be registered on arbitrum — the LendingPool "
            "implementation is a stub post-Oct-2024 attack. See #1842."
        )

    def test_arbitrum_radiant_v2_data_provider_not_registered(self):
        """Mirror of the pool-address regression guard for the data provider.

        Removing only the pool address but leaving the data-provider entry
        would let the strategy-side ``assert_lending_reserve_active``
        pre-flight succeed against a stale provider — masking the upstream
        compile-time "not available on chain" error. Both must be absent.
        """
        chain_providers = LENDING_POOL_DATA_PROVIDERS.get("arbitrum", {})
        assert "radiant_v2" not in chain_providers, (
            "radiant_v2 must not be registered in LENDING_POOL_DATA_PROVIDERS "
            "on arbitrum — the pool implementation is a stub. See #1842."
        )

    def test_unsupported_chain_returns_zero_address(self):
        """Chains without Radiant V2 should not have entries."""
        for chain in ["arbitrum", "base", "optimism", "avalanche", "bsc"]:
            chain_pools = LENDING_POOL_ADDRESSES.get(chain, {})
            assert "radiant_v2" not in chain_pools, f"Radiant V2 should not be on {chain}"


class TestAaveCompatibleProtocols:
    """AAVE_COMPATIBLE_PROTOCOLS set includes all Aave-style protocols."""

    def test_aave_v3_in_set(self):
        assert "aave_v3" in AAVE_COMPATIBLE_PROTOCOLS

    def test_radiant_v2_in_set(self):
        assert "radiant_v2" in AAVE_COMPATIBLE_PROTOCOLS


class TestAaveV3AdapterV2ForkDetection:
    """AaveV3Adapter correctly detects V2 forks and uses deposit() selector."""

    def test_aave_v3_not_v2_fork(self):
        """Aave V3 adapter should NOT be flagged as V2 fork."""
        adapter = AaveV3Adapter("ethereum", "aave_v3")
        assert not adapter._is_v2_fork

    def test_radiant_v2_is_v2_fork(self):
        """Radiant V2 adapter should be flagged as V2 fork."""
        adapter = AaveV3Adapter("ethereum", "radiant_v2")
        assert adapter._is_v2_fork

    def test_radiant_v2_pool_address_resolved(self):
        """Radiant V2 adapter should resolve the correct pool address."""
        adapter = AaveV3Adapter("ethereum", "radiant_v2")
        assert adapter.get_pool_address() == "0xA950974f64aA33f27F6C5e017eEE93BF7588ED07"


class TestRadiantV2SupplyCalldata:
    """Radiant V2 uses deposit() selector (Aave V2), not supply() (Aave V3)."""

    ASSET = "0xdAC17F958D2ee523a2206206994597C13D831ec7"  # USDT
    WALLET = "0x1234567890123456789012345678901234567890"
    AMOUNT = 1000000  # 1 USDT (6 decimals)

    def test_radiant_v2_uses_deposit_selector(self):
        """Radiant V2 supply calldata must use deposit() selector 0xe8eda9df."""
        adapter = AaveV3Adapter("ethereum", "radiant_v2")
        calldata = adapter.get_supply_calldata(
            asset=self.ASSET,
            amount=self.AMOUNT,
            on_behalf_of=self.WALLET,
        )
        selector = "0x" + calldata[:4].hex()
        assert selector == AAVE_V2_DEPOSIT_SELECTOR, (
            f"Radiant V2 must use deposit() selector {AAVE_V2_DEPOSIT_SELECTOR}, got {selector}"
        )

    def test_aave_v3_uses_supply_selector(self):
        """Aave V3 supply calldata must still use supply() selector 0x617ba037."""
        adapter = AaveV3Adapter("ethereum", "aave_v3")
        calldata = adapter.get_supply_calldata(
            asset=self.ASSET,
            amount=self.AMOUNT,
            on_behalf_of=self.WALLET,
        )
        selector = "0x" + calldata[:4].hex()
        assert selector == AAVE_SUPPLY_SELECTOR, (
            f"Aave V3 must use supply() selector {AAVE_SUPPLY_SELECTOR}, got {selector}"
        )

    def test_radiant_v2_supply_params_match_aave_v3(self):
        """Parameter encoding is identical between V2 deposit() and V3 supply()."""
        v2_adapter = AaveV3Adapter("ethereum", "radiant_v2")
        v3_adapter = AaveV3Adapter("ethereum", "aave_v3")

        v2_calldata = v2_adapter.get_supply_calldata(
            asset=self.ASSET, amount=self.AMOUNT, on_behalf_of=self.WALLET,
        )
        v3_calldata = v3_adapter.get_supply_calldata(
            asset=self.ASSET, amount=self.AMOUNT, on_behalf_of=self.WALLET,
        )

        # Selectors differ (4 bytes), but parameter encoding (rest) is identical
        assert v2_calldata[4:] == v3_calldata[4:], (
            "deposit() and supply() parameter encoding must be identical"
        )


class TestRadiantV2WithdrawCalldata:
    """Radiant V2 withdraw uses the same selector as Aave V3 (same ABI)."""

    ASSET = "0xdAC17F958D2ee523a2206206994597C13D831ec7"
    WALLET = "0x1234567890123456789012345678901234567890"
    AMOUNT = 1000000

    def test_radiant_v2_withdraw_same_as_aave_v3(self):
        """withdraw() selector is identical for V2 and V3 (0x69328dec)."""
        v2_adapter = AaveV3Adapter("ethereum", "radiant_v2")
        v3_adapter = AaveV3Adapter("ethereum", "aave_v3")

        v2_calldata = v2_adapter.get_withdraw_calldata(
            asset=self.ASSET, amount=self.AMOUNT, to=self.WALLET,
        )
        v3_calldata = v3_adapter.get_withdraw_calldata(
            asset=self.ASSET, amount=self.AMOUNT, to=self.WALLET,
        )

        # withdraw() calldata is identical for both protocols
        assert v2_calldata == v3_calldata
        selector = "0x" + v2_calldata[:4].hex()
        assert selector == AAVE_WITHDRAW_SELECTOR


class TestRadiantV2ReceiptParser:
    """Verify _decode_deposit/_decode_borrow handle the actual Aave V2 ABI
    (referral indexed). Regression guard for the silent-no-events bug
    surfaced by tests/intents/ethereum/test_radiant_v2_lending.py."""

    def _make_log(self, address: str, topics: list[str], data: str) -> dict:
        return {
            "address": address,
            "topics": [
                bytes.fromhex(t[2:]) if t.startswith("0x") else bytes.fromhex(t)
                for t in topics
            ],
            "data": data,
            "logIndex": 0,
            "transactionIndex": 0,
            "transactionHash": b"\x00" * 32,
            "blockHash": b"\x00" * 32,
            "blockNumber": 0,
            "removed": False,
        }

    def _make_receipt(self, logs: list[dict]) -> dict:
        return {
            "transactionHash": b"\x00" * 32,
            "blockNumber": 0,
            "status": 1,
            "logs": logs,
        }

    def test_decode_deposit_with_indexed_referral(self):
        from almanak.framework.connectors.radiant_v2.receipt_parser import (
            EVENT_TOPICS,
            RadiantV2ReceiptParser,
        )

        # Ethereum Radiant pool address (lowercased for the parser's filter).
        pool = "0xA950974f64aA33f27F6C5e017eEE93BF7588ED07"
        reserve = "0x" + "11" * 20
        on_behalf_of = "0x" + "22" * 20
        user = "0x" + "33" * 20
        amount = 1234567890
        referral = 0
        topics = [
            EVENT_TOPICS["Deposit"],
            "0x" + "00" * 12 + reserve[2:],
            "0x" + "00" * 12 + on_behalf_of[2:],
            "0x" + "00" * 31 + f"{referral:02x}",  # indexed uint16
        ]
        data = (
            "0x"
            + "00" * 12 + user[2:]              # user (address, 32 bytes)
            + f"{amount:064x}"                  # amount (uint256, 32 bytes)
        )
        receipt = self._make_receipt([self._make_log(pool, topics, data)])

        parser = RadiantV2ReceiptParser()
        result = parser.parse_receipt(receipt)

        assert result.success, "Parser should succeed with valid Deposit log"
        assert len(result.supplies) == 1, "Must surface the Deposit event"
        assert result.supplies[0].reserve.lower() == reserve.lower()
        assert int(result.supplies[0].amount) == amount

    def test_decode_borrow_with_indexed_referral(self):
        from almanak.framework.connectors.radiant_v2.receipt_parser import (
            EVENT_TOPICS,
            RadiantV2ReceiptParser,
        )

        pool = "0xA950974f64aA33f27F6C5e017eEE93BF7588ED07"
        reserve = "0x" + "44" * 20
        on_behalf_of = "0x" + "55" * 20
        user = "0x" + "66" * 20
        amount = 987654321
        borrow_rate_mode = 2  # variable
        borrow_rate_ray = 10**27  # 1.0 in ray
        referral = 0
        topics = [
            EVENT_TOPICS["Borrow"],
            "0x" + "00" * 12 + reserve[2:],
            "0x" + "00" * 12 + on_behalf_of[2:],
            "0x" + "00" * 31 + f"{referral:02x}",
        ]
        data = (
            "0x"
            + "00" * 12 + user[2:]
            + f"{amount:064x}"
            + f"{borrow_rate_mode:064x}"
            + f"{borrow_rate_ray:064x}"
        )
        receipt = self._make_receipt([self._make_log(pool, topics, data)])

        parser = RadiantV2ReceiptParser()
        result = parser.parse_receipt(receipt)

        assert result.success
        assert len(result.borrows) == 1
        assert result.borrows[0].reserve.lower() == reserve.lower()
        assert int(result.borrows[0].amount) == amount
        assert result.borrows[0].interest_rate_mode == 2
