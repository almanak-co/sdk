"""Tests for ``almanak.framework.demos.spec``."""

from __future__ import annotations

import json
import textwrap
from decimal import Decimal
from pathlib import Path

import pytest

from almanak.framework.demos import DemoCatalog, DemoSpec, QaConfig
from almanak.framework.demos.sidecar import SidecarEntry, SidecarRegistry


STRATEGY_TEMPLATE = textwrap.dedent(
    '''
    """Test strategy fixture."""
    from almanak.framework.strategies import IntentStrategy, almanak_strategy
    from almanak.framework.intents import Intent


    @almanak_strategy(
        name="{name}",
        description="{description}",
        supported_chains={supported_chains!r},
        supported_protocols={supported_protocols!r},
        intent_types={intent_types!r},
        default_chain="{default_chain}",
    )
    class TestStrategy(IntentStrategy):
        def decide(self, market):
            return Intent.hold()
    '''
).strip()


@pytest.fixture
def demos_root(tmp_path: Path) -> Path:
    root = tmp_path / "demos"
    root.mkdir()
    return root


def _make_demo(
    root: Path,
    name: str,
    *,
    description: str = "",
    supported_chains: list[str] | None = None,
    supported_protocols: list[str] | None = None,
    intent_types: list[str] | None = None,
    default_chain: str = "",
    config: dict | None = None,
) -> Path:
    """Create a synthetic demo directory at ``root / name``."""
    supported_chains = supported_chains if supported_chains is not None else ["arbitrum"]
    supported_protocols = supported_protocols if supported_protocols is not None else []
    intent_types = intent_types if intent_types is not None else ["SWAP"]
    default_chain = default_chain or supported_chains[0]
    demo_dir = root / name
    demo_dir.mkdir()
    (demo_dir / "strategy.py").write_text(
        STRATEGY_TEMPLATE.format(
            name=name,
            description=description,
            supported_chains=supported_chains,
            supported_protocols=supported_protocols,
            intent_types=intent_types,
            default_chain=default_chain,
        )
    )
    if config is not None:
        (demo_dir / "config.json").write_text(json.dumps(config, indent=2))
    return demo_dir


class TestQaConfig:
    def test_empty_input_yields_default(self):
        assert QaConfig.from_mapping(None) == QaConfig()
        assert QaConfig.from_mapping({}) == QaConfig()
        assert QaConfig.from_mapping(42) == QaConfig()

    def test_regress_string_promoted_to_tuple(self):
        qa = QaConfig.from_mapping({"regress": "smoke"})
        assert qa.regress == ("smoke",)

    def test_regress_list(self):
        qa = QaConfig.from_mapping({"regress": ["smoke", "nightly"]})
        assert qa.regress == ("smoke", "nightly")

    def test_expected_actions_coerced_int(self):
        assert QaConfig.from_mapping({"expected_actions": "3"}).expected_actions == 3
        assert QaConfig.from_mapping({"expected_actions": "abc"}).expected_actions is None

    def test_full_block(self):
        qa = QaConfig.from_mapping(
            {
                "regress": ["smoke"],
                "force_action": "open",
                "expected_actions": 1,
                "sidecar_skip_ticket": "VIB-9999",
            }
        )
        assert qa.regress == ("smoke",)
        assert qa.force_action == "open"
        assert qa.expected_actions == 1
        assert qa.sidecar_skip_ticket == "VIB-9999"


class TestDemoSpecLoad:
    def test_load_basic_demo(self, demos_root):
        _make_demo(
            demos_root,
            "demo_basic",
            description="basic test demo",
            supported_chains=["arbitrum", "base"],
            supported_protocols=["uniswap_v3"],
            intent_types=["SWAP"],
            default_chain="arbitrum",
            config={"chain": "arbitrum", "anvil_funding": {"WETH": 1, "USDC": 1000}},
        )

        spec = DemoSpec.load(demos_root / "demo_basic")
        assert spec.name == "demo_basic"
        assert spec.supported_chains == ["arbitrum", "base"]
        assert spec.default_chain == "arbitrum"
        assert spec.supported_protocols == ["uniswap_v3"]
        assert "SWAP" in spec.intent_types
        assert spec.description == "basic test demo"
        assert spec.required_funding("arbitrum") == {
            "WETH": Decimal("1"),
            "USDC": Decimal("1000"),
        }

    def test_load_rejects_missing_strategy(self, demos_root):
        bad = demos_root / "no_strategy"
        bad.mkdir()
        (bad / "config.json").write_text("{}")
        with pytest.raises(FileNotFoundError, match="missing strategy.py"):
            DemoSpec.load(bad)

    def test_load_rejects_missing_dir(self, demos_root):
        with pytest.raises(FileNotFoundError):
            DemoSpec.load(demos_root / "ghost")

    def test_load_rejects_malformed_json(self, demos_root):
        demo = _make_demo(demos_root, "broken_cfg", config={"chain": "arbitrum"})
        (demo / "config.json").write_text("{ not valid json")
        with pytest.raises(ValueError, match="Malformed JSON"):
            DemoSpec.load(demo)

    def test_load_rejects_non_object_json(self, demos_root):
        demo = _make_demo(demos_root, "list_cfg", config={"chain": "arbitrum"})
        (demo / "config.json").write_text("[1, 2, 3]")
        with pytest.raises(ValueError, match="must be a JSON object"):
            DemoSpec.load(demo)

    def test_load_without_config_json(self, demos_root):
        # _make_demo only writes config.json when explicitly passed; omitting
        # the keyword leaves the directory with strategy.py only.
        demo_dir = _make_demo(demos_root, "no_config")
        assert not (demo_dir / "config.json").exists()
        spec = DemoSpec.load(demo_dir)
        assert spec.config == {}
        assert spec.required_funding() == {}

    def test_default_chain_validation_at_decorator(self, demos_root):
        # ``@almanak_strategy`` raises if default_chain not in supported_chains.
        # The error must surface during DemoSpec.load() because import executes
        # the decorator.
        demo_dir = demos_root / "bad_default"
        demo_dir.mkdir()
        (demo_dir / "strategy.py").write_text(
            STRATEGY_TEMPLATE.format(
                name="bad_default",
                description="",
                supported_chains=["arbitrum"],
                supported_protocols=[],
                intent_types=["SWAP"],
                default_chain="ethereum",  # not in supported_chains
            )
        )
        with pytest.raises(ValueError, match="default_chain"):
            DemoSpec.load(demo_dir)

    def test_qa_block_parsed(self, demos_root):
        demo = _make_demo(
            demos_root,
            "qa_demo",
            config={
                "chain": "arbitrum",
                "qa": {"regress": ["smoke"], "expected_actions": 2},
            },
        )
        spec = DemoSpec.load(demo)
        assert spec.qa.regress == ("smoke",)
        assert spec.qa.expected_actions == 2

    def test_chains_in_config_preferred_string(self, demos_root):
        demo = _make_demo(
            demos_root,
            "chain_str",
            supported_chains=["arbitrum", "ethereum"],
            config={"chain": "ethereum"},
        )
        spec = DemoSpec.load(demo)
        assert spec.chains_in_config() == ["ethereum"]

    def test_chains_in_config_list(self, demos_root):
        demo = _make_demo(
            demos_root,
            "chain_list",
            supported_chains=["arbitrum", "ethereum"],
            config={"chains": ["arbitrum", "ethereum"]},
        )
        spec = DemoSpec.load(demo)
        assert spec.chains_in_config() == ["arbitrum", "ethereum"]


class TestRequiredFunding:
    def test_flat_layout(self, demos_root):
        demo = _make_demo(
            demos_root,
            "flat",
            config={"anvil_funding": {"WETH": 1, "USDC": "1000"}},
        )
        spec = DemoSpec.load(demo)
        funds = spec.required_funding()
        assert funds["WETH"] == Decimal("1")
        assert funds["USDC"] == Decimal("1000")

    def test_per_chain_layout_picks_default(self, demos_root):
        demo = _make_demo(
            demos_root,
            "per_chain",
            supported_chains=["arbitrum", "base"],
            default_chain="base",
            config={
                "anvil_funding": {
                    "arbitrum": {"WETH": 1},
                    "base": {"WETH": 2, "USDC": 500},
                }
            },
        )
        spec = DemoSpec.load(demo)
        assert spec.required_funding() == {
            "WETH": Decimal("2"),
            "USDC": Decimal("500"),
        }
        assert spec.required_funding("arbitrum") == {"WETH": Decimal("1")}

    def test_per_chain_missing_chain_returns_empty(self, demos_root):
        demo = _make_demo(
            demos_root,
            "per_chain_miss",
            supported_chains=["arbitrum"],
            config={"anvil_funding": {"arbitrum": {"WETH": 1}}},
        )
        spec = DemoSpec.load(demo)
        assert spec.required_funding("ethereum") == {}

    def test_zero_amounts_dropped(self, demos_root):
        demo = _make_demo(
            demos_root,
            "zero",
            config={"anvil_funding": {"WETH": 0, "USDC": "1000"}},
        )
        spec = DemoSpec.load(demo)
        assert spec.required_funding() == {"USDC": Decimal("1000")}

    def test_unparseable_amounts_dropped(self, demos_root):
        demo = _make_demo(
            demos_root,
            "junk",
            config={"anvil_funding": {"WETH": "abc", "USDC": 1000}},
        )
        spec = DemoSpec.load(demo)
        assert spec.required_funding() == {"USDC": Decimal("1000")}


class TestDiscover:
    def test_discover_loads_multiple(self, demos_root):
        _make_demo(demos_root, "alpha", supported_chains=["arbitrum"])
        _make_demo(demos_root, "beta", supported_chains=["base"])

        catalog = DemoCatalog.discover(demos_root)
        names = [s.name for s in catalog.specs]
        assert names == ["alpha", "beta"]
        assert catalog.errors == []

    def test_discover_records_dirs_with_config_but_no_strategy(self, demos_root):
        _make_demo(demos_root, "good", supported_chains=["arbitrum"])
        # Stub demo: config.json present but no strategy.py
        stub = demos_root / "stub"
        stub.mkdir()
        (stub / "config.json").write_text("{}")

        catalog = DemoCatalog.discover(demos_root)
        assert [s.name for s in catalog.specs] == ["good"]
        assert len(catalog.errors) == 1
        assert "strategy.py missing" in catalog.errors[0].reason

    def test_discover_ignores_dunder_dirs(self, demos_root):
        _make_demo(demos_root, "good", supported_chains=["arbitrum"])
        (demos_root / "__pycache__").mkdir()
        (demos_root / ".cache").mkdir()
        catalog = DemoCatalog.discover(demos_root)
        assert [s.name for s in catalog.specs] == ["good"]
        assert catalog.errors == []

    def test_discover_records_load_errors(self, demos_root):
        _make_demo(demos_root, "good", supported_chains=["arbitrum"])
        bad = _make_demo(demos_root, "bad", supported_chains=["arbitrum"])
        (bad / "config.json").write_text("{ broken")
        catalog = DemoCatalog.discover(demos_root)
        assert [s.name for s in catalog.specs] == ["good"]
        assert len(catalog.errors) == 1
        assert catalog.errors[0].directory.name == "bad"

    def test_by_name_and_for_chain(self, demos_root):
        _make_demo(demos_root, "alpha", supported_chains=["arbitrum"])
        _make_demo(demos_root, "beta", supported_chains=["base", "arbitrum"])
        _make_demo(demos_root, "gamma", supported_chains=["ethereum"])

        catalog = DemoCatalog.discover(demos_root)
        assert catalog.by_name("alpha").name == "alpha"
        assert catalog.by_name("missing") is None
        assert sorted(s.name for s in catalog.for_chain("arbitrum")) == ["alpha", "beta"]
        assert [s.name for s in catalog.for_chain("ethereum")] == ["gamma"]

    def test_discover_with_custom_sidecar(self, demos_root):
        demo_dir = _make_demo(demos_root, "alpha", supported_chains=["arbitrum"])
        sidecar = SidecarRegistry(
            connectors={
                "uniswap_v3": SidecarEntry(
                    connector="uniswap_v3",
                    demo_dir=demo_dir,
                    chain="arbitrum",
                    force_action="open",
                    max_iterations=1,
                )
            }
        )
        catalog = DemoCatalog.discover(demos_root, sidecar_registry=sidecar)
        spec = catalog.by_name("alpha")
        assert spec is not None
        assert spec.sidecar is not None
        assert spec.sidecar.connector == "uniswap_v3"
        # by_connector returns matching spec
        assert catalog.by_connector("uniswap_v3").name == "alpha"
        assert catalog.by_connector("missing") is None
