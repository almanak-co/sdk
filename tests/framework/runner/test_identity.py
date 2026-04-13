"""Tests for the deployment_id resolver (VIB-2764).

Validates:
- Deterministic deployment_id from (wallet, chain, strategy_name)
- CLI override (--id) takes precedence
- Bare name fallback when wallet/chain unavailable
- run_id is always unique
"""

from almanak.framework.runner.identity import generate_run_id, resolve_deployment_id


class TestResolveDeploymentId:
    """Test deployment_id resolution with three-tier identity model."""

    def test_deterministic_hash(self):
        """Same (wallet, chain, name) always produces the same deployment_id."""
        id1 = resolve_deployment_id(
            strategy_name="AaveYield",
            wallet_address="0xAbC123",
            chain="arbitrum",
        )
        id2 = resolve_deployment_id(
            strategy_name="AaveYield",
            wallet_address="0xAbC123",
            chain="arbitrum",
        )
        assert id1 == id2
        assert id1.startswith("AaveYield:")
        assert len(id1.split(":")[1]) == 12

    def test_case_insensitive_wallet_and_chain(self):
        """Wallet address and chain are lowercased before hashing."""
        id1 = resolve_deployment_id(
            strategy_name="MyStrat",
            wallet_address="0xABCDEF",
            chain="Arbitrum",
        )
        id2 = resolve_deployment_id(
            strategy_name="MyStrat",
            wallet_address="0xabcdef",
            chain="arbitrum",
        )
        assert id1 == id2

    def test_different_wallets_different_ids(self):
        id1 = resolve_deployment_id(
            strategy_name="Strat",
            wallet_address="0xAAA",
            chain="arbitrum",
        )
        id2 = resolve_deployment_id(
            strategy_name="Strat",
            wallet_address="0xBBB",
            chain="arbitrum",
        )
        assert id1 != id2

    def test_different_chains_different_ids(self):
        id1 = resolve_deployment_id(
            strategy_name="Strat",
            wallet_address="0xAAA",
            chain="arbitrum",
        )
        id2 = resolve_deployment_id(
            strategy_name="Strat",
            wallet_address="0xAAA",
            chain="base",
        )
        assert id1 != id2

    def test_cli_override_with_colon(self):
        """User-supplied --id with colon is used as-is."""
        result = resolve_deployment_id(
            strategy_name="Strat",
            wallet_address="0xAAA",
            chain="arbitrum",
            cli_id="MyStrat:custom123",
        )
        assert result == "MyStrat:custom123"

    def test_cli_override_without_colon(self):
        """User-supplied --id without colon gets strategy_name prefix."""
        result = resolve_deployment_id(
            strategy_name="Strat",
            wallet_address="0xAAA",
            chain="arbitrum",
            cli_id="custom123",
        )
        assert result == "Strat:custom123"

    def test_bare_name_fallback(self):
        """No wallet/chain falls back to bare strategy_name."""
        result = resolve_deployment_id(strategy_name="MyStrat")
        assert result == "MyStrat"

    def test_empty_wallet_fallback(self):
        """Empty wallet address falls back to bare name."""
        result = resolve_deployment_id(
            strategy_name="MyStrat",
            wallet_address="",
            chain="arbitrum",
        )
        assert result == "MyStrat"


class TestGenerateRunId:
    """Test per-process run_id generation."""

    def test_run_id_is_12_hex_chars(self):
        rid = generate_run_id()
        assert len(rid) == 12
        int(rid, 16)  # Must be valid hex

    def test_run_ids_are_unique(self):
        ids = {generate_run_id() for _ in range(100)}
        assert len(ids) == 100
