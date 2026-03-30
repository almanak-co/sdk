"""Manifest coverage tests - verify generated manifests are a superset of compiled transactions.

The key invariant: every (target, selector) that the IntentCompiler produces
for a given strategy MUST appear in the generated PermissionManifest. If any
compiled transaction targets a contract or selector not in the manifest, the
strategy would fail at runtime when Zodiac Roles enforcement is active.

These tests parametrize over demo strategies in strategies/demo/ and
independently compile synthetic intents to verify coverage.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from almanak.framework.execution.signer.safe.constants import (
    MULTISEND_ADDRESSES,
    MULTISEND_SELECTOR,
)
from almanak.framework.intents.compiler import (
    DEFAULT_SWAP_FEE_TIER,
    SWAP_FEE_TIERS,
    ERC20_APPROVE_SELECTOR,
    IntentCompiler,
    IntentCompilerConfig,
)
from almanak.framework.permissions.generator import generate_manifest, load_strategy_config
from almanak.framework.permissions.hints import get_permission_hints
from almanak.framework.permissions.synthetic_intents import build_synthetic_intents

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DEMO_DIR = Path(__file__).parents[3] / "almanak" / "demo_strategies"


def _load_strategy_class(strategy_dir: Path):
    """Load strategy class from strategy.py and return it (or None on failure)."""
    from almanak.framework.cli.intent_debug import load_strategy_from_file

    strategy_file = strategy_dir / "strategy.py"
    if not strategy_file.exists():
        return None
    cls, error = load_strategy_from_file(strategy_file)
    if error or cls is None:
        return None
    return cls


def _collect_demo_strategies() -> list[tuple[str, Path]]:
    """Yield (strategy_name, strategy_path) for demo strategies with metadata.

    Filters to strategies that have:
    - A strategy.py file
    - A valid @almanak_strategy decorator with supported_protocols and intent_types
    """
    if not _DEMO_DIR.exists():
        return []

    results = []
    for strategy_dir in sorted(_DEMO_DIR.iterdir()):
        if not strategy_dir.is_dir():
            continue
        if not (strategy_dir / "strategy.py").exists():
            continue

        cls = _load_strategy_class(strategy_dir)
        if cls is None:
            continue

        metadata = getattr(cls, "STRATEGY_METADATA", None)
        if metadata is None:
            continue
        if not metadata.supported_protocols or not metadata.intent_types:
            continue

        results.append((strategy_dir.name, strategy_dir))

    return results


def _get_compiler_for_protocol(protocol: str, chain: str) -> IntentCompiler:
    """Create an IntentCompiler matching the config used by discover_permissions."""
    hints = get_permission_hints(protocol)
    chain_fee_override = hints.synthetic_fee_tier.get(chain)

    fee_tiers = SWAP_FEE_TIERS.get(protocol)
    if fee_tiers:
        mode = "fixed"
        fee_tier = chain_fee_override or DEFAULT_SWAP_FEE_TIER.get(protocol, fee_tiers[0])
    else:
        mode = "auto"
        fee_tier = chain_fee_override or 3000

    return IntentCompiler(
        chain=chain,
        config=IntentCompilerConfig(
            allow_placeholder_prices=True,
            swap_pool_selection_mode=mode,
            fixed_swap_fee_tier=fee_tier,
        ),
    )


# Collect once at module level for parametrize
_DEMO_STRATEGIES = _collect_demo_strategies()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestManifestCoverage:
    """Verify generated manifests cover all compiled transactions."""

    @pytest.mark.parametrize(
        "name,path",
        _DEMO_STRATEGIES,
        ids=[s[0] for s in _DEMO_STRATEGIES],
    )
    def test_manifest_covers_compiled_intents(self, name: str, path: Path) -> None:
        """Every (target, selector) from compilation must appear in the manifest."""
        cls = _load_strategy_class(path)
        assert cls is not None, f"Failed to load strategy class from {path}"

        metadata = cls.STRATEGY_METADATA
        protocols = list(metadata.supported_protocols)
        intent_types = list(metadata.intent_types)

        # Determine chains
        if metadata.supported_chains:
            chains = list(metadata.supported_chains)
        elif metadata.default_chain:
            chains = [metadata.default_chain]
        else:
            chains = ["arbitrum"]

        # Load config
        config_path = path / "config.json"
        config = load_strategy_config(config_path)

        for chain in chains:
            # Generate manifest
            manifest = generate_manifest(
                strategy_name=metadata.name or cls.__name__,
                chain=chain,
                supported_protocols=protocols,
                intent_types=intent_types,
                config=config,
            )

            # Build lookup set from manifest: {(target, selector)}
            manifest_pairs: set[tuple[str, str]] = set()
            for perm in manifest.permissions:
                for sel in perm.function_selectors:
                    manifest_pairs.add((perm.target.lower(), sel.selector.lower()))

            # Also track manifest targets without selectors (e.g. Enso delegates
            # which have empty selector lists = wildcard)
            wildcard_targets: set[str] = set()
            for perm in manifest.permissions:
                if not perm.function_selectors:
                    wildcard_targets.add(perm.target.lower())

            # Independently compile synthetic intents and verify coverage
            missing: list[tuple[str, str, str, str]] = []  # (protocol, intent_type, target, selector)

            for protocol in protocols:
                compiler = _get_compiler_for_protocol(protocol, chain)

                for intent_type in intent_types:
                    synthetic_intents = build_synthetic_intents(protocol, intent_type, chain)
                    if not synthetic_intents:
                        continue

                    for intent in synthetic_intents:
                        try:
                            result = compiler.compile(intent)
                        except Exception:  # noqa: BLE001
                            # Same as discover_permissions: skip compilation errors
                            logger.debug("Compilation error for %s/%s: %s", protocol, intent_type, intent)
                            continue

                        if result.status.value != "SUCCESS":
                            continue

                        for tx in result.transactions:
                            target = tx.to.lower()
                            selector = tx.data[:10].lower() if tx.data and len(tx.data) >= 10 else None

                            if selector is None:
                                continue

                            # Check: pair must be in manifest OR target is a wildcard
                            if (target, selector) not in manifest_pairs and target not in wildcard_targets:
                                missing.append((protocol, intent_type, target, selector))

            assert not missing, (
                f"Manifest for {name} on {chain} is missing {len(missing)} (target, selector) pair(s):\n"
                + "\n".join(
                    f"  - {protocol}/{intent_type}: target={target} selector={selector}"
                    for protocol, intent_type, target, selector in missing
                )
            )

    @pytest.mark.parametrize(
        "name,path",
        _DEMO_STRATEGIES,
        ids=[s[0] for s in _DEMO_STRATEGIES],
    )
    def test_manifest_includes_multisend(self, name: str, path: Path) -> None:
        """MultiSend address should be in manifest for strategies on supported chains."""
        cls = _load_strategy_class(path)
        assert cls is not None

        metadata = cls.STRATEGY_METADATA
        protocols = list(metadata.supported_protocols)
        intent_types = list(metadata.intent_types)

        if metadata.supported_chains:
            chains = list(metadata.supported_chains)
        elif metadata.default_chain:
            chains = [metadata.default_chain]
        else:
            chains = ["arbitrum"]

        config = load_strategy_config(path / "config.json")

        for chain in chains:
            multisend_addr = MULTISEND_ADDRESSES.get(chain.lower())
            if not multisend_addr:
                # Chain doesn't have a known MultiSend - skip
                continue

            manifest = generate_manifest(
                strategy_name=metadata.name or cls.__name__,
                chain=chain,
                supported_protocols=protocols,
                intent_types=intent_types,
                config=config,
            )

            manifest_targets = {p.target.lower() for p in manifest.permissions}
            assert multisend_addr.lower() in manifest_targets, (
                f"MultiSend {multisend_addr} not in manifest for {name} on {chain}"
            )

            # Also verify the multiSend(bytes) selector is present
            multisend_perm = next(
                p for p in manifest.permissions if p.target.lower() == multisend_addr.lower()
            )
            selectors = {s.selector for s in multisend_perm.function_selectors}
            assert MULTISEND_SELECTOR in selectors, (
                f"MultiSend permission missing multiSend(bytes) selector for {name} on {chain}"
            )

    @pytest.mark.parametrize(
        "name,path",
        _DEMO_STRATEGIES,
        ids=[s[0] for s in _DEMO_STRATEGIES],
    )
    def test_manifest_includes_token_approvals(self, name: str, path: Path) -> None:
        """Config tokens that resolve to addresses should have ERC-20 approve permissions."""
        cls = _load_strategy_class(path)
        assert cls is not None

        metadata = cls.STRATEGY_METADATA
        protocols = list(metadata.supported_protocols)
        intent_types = list(metadata.intent_types)

        if metadata.supported_chains:
            chains = list(metadata.supported_chains)
        elif metadata.default_chain:
            chains = [metadata.default_chain]
        else:
            chains = ["arbitrum"]

        config_path = path / "config.json"
        config = load_strategy_config(config_path)
        if not config:
            pytest.skip(f"No config.json for {name}")

        # Collect token symbols from config using the same fields as the generator
        from almanak.framework.permissions.generator import _TOKEN_CONFIG_FIELDS

        token_symbols: set[str] = set()
        for key, value in config.items():
            if key in _TOKEN_CONFIG_FIELDS and isinstance(value, str) and value:
                token_symbols.add(value)

        anvil_funding = config.get("anvil_funding", {})
        if isinstance(anvil_funding, dict):
            for token_key in anvil_funding:
                if isinstance(token_key, str):
                    token_symbols.add(token_key)

        if not token_symbols:
            pytest.skip(f"No token symbols in config for {name}")

        # Native ETH sentinel - not an ERC-20
        native_sentinel = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

        from almanak.framework.data.tokens import get_token_resolver

        resolver = get_token_resolver()

        for chain in chains:
            manifest = generate_manifest(
                strategy_name=metadata.name or cls.__name__,
                chain=chain,
                supported_protocols=protocols,
                intent_types=intent_types,
                config=config,
            )

            # Build lookup: target -> set of selectors
            manifest_selectors: dict[str, set[str]] = {}
            for perm in manifest.permissions:
                manifest_selectors[perm.target.lower()] = {
                    s.selector for s in perm.function_selectors
                }

            # For each resolvable token, verify approve selector exists
            for symbol in sorted(token_symbols):
                try:
                    resolved = resolver.resolve(symbol, chain)
                except Exception:  # noqa: BLE001
                    # Token not resolvable on this chain - skip
                    logger.debug("Cannot resolve token '%s' on %s", symbol, chain)
                    continue

                if not resolved or not resolved.address:
                    continue
                if resolved.address.lower() == native_sentinel:
                    continue

                target = resolved.address.lower()
                target_selectors = manifest_selectors.get(target, set())
                assert ERC20_APPROVE_SELECTOR in target_selectors, (
                    f"Token {symbol} ({target}) missing approve selector in manifest for {name} on {chain}"
                )
