"""Tests for the contract address registry."""

from almanak.framework.connectors.contract_registry import (
    ContractInfo,
    ContractRegistry,
    get_default_registry,
)


class TestContractRegistry:
    """Tests for ContractRegistry class."""

    def _make_registry(self) -> ContractRegistry:
        registry = ContractRegistry()
        registry.register(
            "arbitrum",
            "0xABCD1234",
            ContractInfo(
                protocol="test_protocol",
                contract_type="swap_router",
                parser_module="test.module",
                parser_class_name="TestParser",
            ),
        )
        return registry

    def test_lookup_returns_correct_info(self) -> None:
        registry = self._make_registry()
        info = registry.lookup("arbitrum", "0xabcd1234")
        assert info is not None
        assert info.protocol == "test_protocol"
        assert info.contract_type == "swap_router"
        assert info.parser_module == "test.module"
        assert info.parser_class_name == "TestParser"

    def test_lookup_wrong_chain_returns_none(self) -> None:
        registry = self._make_registry()
        assert registry.lookup("ethereum", "0xabcd1234") is None

    def test_lookup_wrong_address_returns_none(self) -> None:
        registry = self._make_registry()
        assert registry.lookup("arbitrum", "0xDEAD") is None

    def test_lookup_case_insensitive_address(self) -> None:
        registry = self._make_registry()
        # Registered with mixed case "0xABCD1234", lookup with upper case
        info = registry.lookup("arbitrum", "0xABCD1234")
        assert info is not None
        assert info.protocol == "test_protocol"

    def test_lookup_case_insensitive_chain(self) -> None:
        registry = self._make_registry()
        info = registry.lookup("Arbitrum", "0xabcd1234")
        assert info is not None
        assert info.protocol == "test_protocol"

    def test_get_monitored_addresses_returns_addresses_for_chain(self) -> None:
        registry = ContractRegistry()
        registry.register(
            "arbitrum",
            "0xAAA",
            ContractInfo("proto_a", "router", "mod_a", "ClassA"),
        )
        registry.register(
            "arbitrum",
            "0xBBB",
            ContractInfo("proto_b", "router", "mod_b", "ClassB"),
        )
        registry.register(
            "base",
            "0xCCC",
            ContractInfo("proto_c", "router", "mod_c", "ClassC"),
        )
        addresses = registry.get_monitored_addresses("arbitrum")
        assert sorted(addresses) == ["0xaaa", "0xbbb"]

    def test_get_monitored_addresses_empty_chain(self) -> None:
        registry = self._make_registry()
        assert registry.get_monitored_addresses("unknown_chain") == []

    def test_get_supported_protocols(self) -> None:
        registry = ContractRegistry()
        registry.register("a", "0x1", ContractInfo("proto_x", "r", "m", "C"))
        registry.register("b", "0x2", ContractInfo("proto_y", "r", "m", "C"))
        registry.register("c", "0x3", ContractInfo("proto_x", "r", "m", "C"))
        assert registry.get_supported_protocols() == {"proto_x", "proto_y"}


class TestContractInfoSupportedActions:
    """Tests for ContractInfo supported_actions field."""

    def test_supported_actions_default_empty(self) -> None:
        info = ContractInfo("proto", "router", "mod", "Cls")
        assert info.supported_actions == []

    def test_supported_actions_set_on_creation(self) -> None:
        info = ContractInfo("proto", "router", "mod", "Cls", supported_actions=["SWAP"])
        assert info.supported_actions == ["SWAP"]

    def test_supported_actions_multiple(self) -> None:
        info = ContractInfo("proto", "pool", "mod", "Cls", supported_actions=["SUPPLY", "WITHDRAW", "BORROW", "REPAY"])
        assert "SUPPLY" in info.supported_actions
        assert len(info.supported_actions) == 4


class TestDefaultRegistry:
    """Tests for get_default_registry factory."""

    def test_all_expected_protocols_registered(self) -> None:
        registry = get_default_registry()
        protocols = registry.get_supported_protocols()
        assert "uniswap_v3" in protocols
        assert "pancakeswap_v3" in protocols
        assert "aerodrome" in protocols
        assert "traderjoe_v2" in protocols
        assert "aave_v3" in protocols
        assert "morpho_blue" in protocols
        assert "gmx_v2" in protocols

    def test_uniswap_v3_arbitrum_lookup(self) -> None:
        registry = get_default_registry()
        info = registry.lookup("arbitrum", "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45")
        assert info is not None
        assert info.protocol == "uniswap_v3"
        assert info.parser_class_name == "UniswapV3ReceiptParser"

    def test_uniswap_v3_base_lookup(self) -> None:
        registry = get_default_registry()
        info = registry.lookup("base", "0x2626664c2603336E57B271c5C0b26F421741e481")
        assert info is not None
        assert info.protocol == "uniswap_v3"

    def test_pancakeswap_v3_bnb_lookup(self) -> None:
        registry = get_default_registry()
        info = registry.lookup("bnb", "0x13f4EA83D0bd40E75C8222255bc855a974568Dd4")
        assert info is not None
        assert info.protocol == "pancakeswap_v3"

    def test_aerodrome_base_lookup(self) -> None:
        registry = get_default_registry()
        info = registry.lookup("base", "0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43")
        assert info is not None
        assert info.protocol == "aerodrome"

    def test_traderjoe_v2_avalanche_lookup(self) -> None:
        registry = get_default_registry()
        info = registry.lookup("avalanche", "0xb4315e873dBcf96Ffd0acd8EA43f689D8c20fB30")
        assert info is not None
        assert info.protocol == "traderjoe_v2"

    def test_lookup_unknown_address_returns_none(self) -> None:
        registry = get_default_registry()
        assert registry.lookup("arbitrum", "0xDEADDEADDEADDEAD") is None

    def test_case_insensitive_lookup(self) -> None:
        registry = get_default_registry()
        # Lookup with all-lowercase
        info = registry.lookup("arbitrum", "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45")
        assert info is not None
        assert info.protocol == "uniswap_v3"

    def test_uniswap_v3_has_multiple_chains(self) -> None:
        registry = get_default_registry()
        # Uniswap V3 should be registered on multiple chains
        for chain in ["ethereum", "arbitrum", "optimism", "polygon", "base"]:
            addresses = registry.get_monitored_addresses(chain)
            assert len(addresses) > 0, f"No addresses registered for {chain}"

    def test_dex_swap_router_has_swap_action(self) -> None:
        registry = get_default_registry()
        info = registry.lookup("arbitrum", "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45")
        assert info is not None
        assert "SWAP" in info.supported_actions

    def test_aave_v3_pool_has_lending_actions(self) -> None:
        registry = get_default_registry()
        info = registry.lookup("arbitrum", "0x794a61358D6845594F94dc1DB02A252b5b4814aD")
        assert info is not None
        assert info.protocol == "aave_v3"
        assert set(info.supported_actions) == {"SUPPLY", "WITHDRAW", "BORROW", "REPAY"}

    def test_morpho_blue_has_lending_actions(self) -> None:
        registry = get_default_registry()
        info = registry.lookup("ethereum", "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb")
        assert info is not None
        assert info.protocol == "morpho_blue"
        assert "SUPPLY" in info.supported_actions

    def test_gmx_v2_has_perp_actions(self) -> None:
        registry = get_default_registry()
        info = registry.lookup("arbitrum", "0x1C3fa76e6E1088bCE750f23a5BFcffa1efEF6A41")
        assert info is not None
        assert info.protocol == "gmx_v2"
        assert set(info.supported_actions) == {"PERP_OPEN", "PERP_CLOSE"}

    def test_uniswap_v3_position_manager_has_lp_actions(self) -> None:
        registry = get_default_registry()
        # Uniswap V3 position manager on arbitrum
        info = registry.lookup("arbitrum", "0xC36442b4a4522E871399CD717aBDD847Ab11FE88")
        assert info is not None
        assert info.protocol == "uniswap_v3"
        assert set(info.supported_actions) == {"LP_OPEN", "LP_CLOSE"}
