"""Unit tests for Euler V2 Supply Lifecycle on Ethereum demo strategy.

Validates the strategy's decide() logic and state transitions without
requiring Anvil or gateway. Tests the supply/withdraw lifecycle:
idle -> supplying -> supplied -> withdrawing -> complete
"""

from decimal import Decimal
from unittest.mock import MagicMock, PropertyMock

import pytest

from almanak.demo_strategies.euler_v2_supply_ethereum.strategy import (
    COMPLETE,
    IDLE,
    SUPPLIED,
    SUPPLYING,
    WITHDRAWING,
    EulerV2SupplyEthereumStrategy,
)


@pytest.fixture
def mock_market():
    """Create a mock MarketSnapshot with Ethereum prices."""
    market = MagicMock()
    market.price.side_effect = lambda token: {
        "USDC": Decimal("1.00"),
        "ETH": Decimal("3000.00"),
        "WETH": Decimal("3000.00"),
    }.get(token, Decimal("0"))
    return market


@pytest.fixture
def strategy():
    """Create an EulerV2SupplyEthereumStrategy with mock config."""
    s = EulerV2SupplyEthereumStrategy.__new__(EulerV2SupplyEthereumStrategy)
    s._chain = "ethereum"
    s.supply_token = "USDC"
    s.supply_amount = Decimal("1000")
    s._loop_state = IDLE
    s._previous_stable_state = IDLE
    s._supplied_amount = Decimal("0")
    type(s).chain = PropertyMock(return_value="ethereum")
    type(s).STRATEGY_NAME = PropertyMock(return_value="euler_v2_supply_ethereum")
    return s


class TestEulerV2SupplyLifecycle:
    """Test the supply/withdraw lifecycle state machine."""

    def test_idle_emits_supply_intent(self, strategy, mock_market):
        """First iteration: idle -> supplying, returns SupplyIntent."""
        intent = strategy.decide(mock_market)
        assert intent is not None
        assert intent.intent_type.value == "SUPPLY"
        assert strategy._loop_state == SUPPLYING

    def test_supply_success_transitions_to_supplied(self, strategy, mock_market):
        """Successful supply moves state to 'supplied'."""
        intent = strategy.decide(mock_market)
        strategy.on_intent_executed(intent, success=True, result=None)
        assert strategy._loop_state == SUPPLIED
        assert strategy._supplied_amount == Decimal("1000")

    def test_supplied_emits_withdraw_intent(self, strategy, mock_market):
        """After supplying, next decide() returns WithdrawIntent."""
        intent = strategy.decide(mock_market)
        strategy.on_intent_executed(intent, success=True, result=None)
        intent = strategy.decide(mock_market)
        assert intent is not None
        assert intent.intent_type.value == "WITHDRAW"
        assert strategy._loop_state == WITHDRAWING

    def test_full_lifecycle_completes(self, strategy, mock_market):
        """Full lifecycle: idle -> supply -> withdraw -> complete."""
        # Step 1: Supply
        intent = strategy.decide(mock_market)
        assert intent.intent_type.value == "SUPPLY"
        strategy.on_intent_executed(intent, success=True, result=None)

        # Step 2: Withdraw
        intent = strategy.decide(mock_market)
        assert intent.intent_type.value == "WITHDRAW"
        strategy.on_intent_executed(intent, success=True, result=None)

        assert strategy._loop_state == COMPLETE
        assert strategy._supplied_amount == Decimal("0")

        # Step 3: Hold
        intent = strategy.decide(mock_market)
        assert intent.intent_type.value == "HOLD"
        assert "complete" in intent.reason.lower()

    def test_supply_failure_reverts_to_idle(self, strategy, mock_market):
        """Failed supply reverts state back to idle."""
        intent = strategy.decide(mock_market)
        strategy.on_intent_executed(intent, success=False, result=None)
        assert strategy._loop_state == IDLE

    def test_withdraw_failure_reverts_to_supplied(self, strategy, mock_market):
        """Failed withdraw reverts state back to supplied."""
        intent = strategy.decide(mock_market)
        strategy.on_intent_executed(intent, success=True, result=None)
        intent = strategy.decide(mock_market)
        strategy.on_intent_executed(intent, success=False, result=None)
        assert strategy._loop_state == SUPPLIED

    def test_stuck_transitional_state_recovery(self, strategy, mock_market):
        """Strategy recovers from stuck transitional state."""
        strategy._loop_state = SUPPLYING
        strategy._previous_stable_state = IDLE
        intent = strategy.decide(mock_market)
        assert intent.intent_type.value == "HOLD"
        assert strategy._loop_state == IDLE

    def test_persistent_state_round_trip(self, strategy, mock_market):
        """Persistent state survives save/load cycle."""
        intent = strategy.decide(mock_market)
        strategy.on_intent_executed(intent, success=True, result=None)

        state = strategy.get_persistent_state()
        assert state["state"] == SUPPLIED
        assert state["supplied_amount"] == "1000"

        strategy._loop_state = IDLE
        strategy._supplied_amount = Decimal("0")
        strategy.load_persistent_state(state)

        assert strategy._loop_state == SUPPLIED
        assert strategy._supplied_amount == Decimal("1000")

    def test_status_report(self, strategy):
        """get_status() returns current state info."""
        status = strategy.get_status()
        assert status["state"] == IDLE
        assert status["chain"] == "ethereum"
        assert status["strategy"] == "euler_v2_supply_ethereum"

    def test_supply_intent_uses_correct_protocol(self, strategy, mock_market):
        """Supply intent targets euler_v2 protocol."""
        intent = strategy.decide(mock_market)
        assert intent.protocol == "euler_v2"

    def test_supply_intent_uses_ethereum_chain(self, strategy, mock_market):
        """Supply intent targets ethereum chain."""
        intent = strategy.decide(mock_market)
        assert intent.chain == "ethereum"

    def test_withdraw_all_clears_position(self, strategy, mock_market):
        """Withdraw intent uses withdraw_all=True to fully clear the position."""
        intent = strategy.decide(mock_market)
        strategy.on_intent_executed(intent, success=True, result=None)
        intent = strategy.decide(mock_market)
        assert intent.withdraw_all is True
        assert intent.amount == Decimal("1000")


class TestEulerV2AdapterMultiChain:
    """Test that the Euler V2 adapter supports multiple chains."""

    def test_ethereum_config_accepted(self):
        """EulerV2Config accepts 'ethereum' chain."""
        from almanak.connectors.euler_v2.adapter import EulerV2Config

        config = EulerV2Config(chain="ethereum", wallet_address="0x" + "1" * 40)
        assert config.chain == "ethereum"

    def test_avalanche_config_accepted(self):
        """EulerV2Config still accepts 'avalanche' chain."""
        from almanak.connectors.euler_v2.adapter import EulerV2Config

        config = EulerV2Config(chain="avalanche", wallet_address="0x" + "1" * 40)
        assert config.chain == "avalanche"

    def test_unsupported_chain_rejected(self):
        """EulerV2Config rejects unsupported chains."""
        from almanak.connectors.euler_v2.adapter import EulerV2Config

        with pytest.raises(ValueError, match="supports"):
            EulerV2Config(chain="polygon", wallet_address="0x" + "1" * 40)

    def test_ethereum_adapter_finds_usdc_vault(self):
        """Ethereum adapter resolves USDC to the eUSDC-2 vault."""
        from almanak.connectors.euler_v2.adapter import EulerV2Adapter, EulerV2Config

        config = EulerV2Config(chain="ethereum", wallet_address="0x" + "1" * 40)
        adapter = EulerV2Adapter(config)
        vault = adapter.find_vault_for_asset("USDC")
        assert vault is not None
        assert vault.vault_address == "0x797DD80692c3b2dAdabCe8e30C07fDE5307D48a9"
        assert vault.underlying_symbol == "USDC"
        assert vault.decimals == 6

    def test_ethereum_adapter_uses_correct_evc(self):
        """Ethereum adapter uses the Ethereum EVC address."""
        from almanak.connectors.euler_v2.adapter import EulerV2Adapter, EulerV2Config

        config = EulerV2Config(chain="ethereum", wallet_address="0x" + "1" * 40)
        adapter = EulerV2Adapter(config)
        assert adapter.evc_address == "0x0C9a3dd6b8F28529d72d7f9cE918D493519EE383"

    def test_avalanche_adapter_uses_correct_evc(self):
        """Avalanche adapter still uses the Avalanche EVC address."""
        from almanak.connectors.euler_v2.adapter import EulerV2Adapter, EulerV2Config

        config = EulerV2Config(chain="avalanche", wallet_address="0x" + "1" * 40)
        adapter = EulerV2Adapter(config)
        assert adapter.evc_address == "0xddcbe30A761Edd2e19bba930A977475265F36Fa1"

    def test_ethereum_supply_builds_correct_tx(self):
        """Supply on Ethereum targets the correct vault address."""
        from almanak.connectors.euler_v2.adapter import EulerV2Adapter, EulerV2Config

        config = EulerV2Config(chain="ethereum", wallet_address="0x" + "1" * 40)
        adapter = EulerV2Adapter(config)
        result = adapter.supply("USDC", Decimal("1000"))
        assert result.success
        assert result.tx_data["to"] == "0x797DD80692c3b2dAdabCe8e30C07fDE5307D48a9"
