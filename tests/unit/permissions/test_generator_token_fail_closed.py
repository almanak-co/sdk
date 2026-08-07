"""Fail-closed token-approval extraction in the manifest generator (ALM-3175).

The previous behaviour silently dropped any config token the resolver could
not name (``except Exception: logger.debug``), producing a Zodiac manifest
whose missing approve target reverts unauthorized at
``execTransactionWithRole`` at runtime (ALM-3173: SPCXB on BSC). The
contract pinned here: every referenced token yields an approve permission,
is skipped with a manifest warning (native asset), or fails generation with
``PermissionGenerationError`` — never a silent drop. Address-form
references emit without any registry dependency; symbol-form references
resolve through the static registry only (``skip_gateway=True``).

Negative control: reverting the generator change makes
``test_address_form_registry_unknown_token_emits_approve`` and
``test_symbol_form_unknown_raises`` fail.
"""

from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.intents.compiler import ERC20_APPROVE_SELECTOR
from almanak.framework.permissions.generator import (
    PermissionGenerationError,
    _extract_token_permissions,
    generate_manifest,
)

# ALM-3173: real registry-unknown token (SPCXB on BSC) — absent from
# data/tokens.json, which is the point of these tests.
SPCXB_ADDR = "0xbe9d156892e55e7154bcd3cb0fea677f9d3103e1"
USDT_BSC_ADDR = "0x55d398326f99059ff775485246999027b3197955"
NATIVE_SENTINEL = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"


def _approve_targets(permissions) -> set[str]:
    return {
        p.target
        for p in permissions
        if any(s.selector == ERC20_APPROVE_SELECTOR for s in p.function_selectors)
    }


class TestAddressFormEmitsWithoutRegistry:
    def test_address_form_registry_unknown_token_emits_approve(self):
        """The ALM-3173 shape: a token only the gateway can name still gets
        its approve target — the registry is never a gate for address-form."""
        permissions, warnings = _extract_token_permissions("bsc", {"base_token": SPCXB_ADDR})
        assert _approve_targets(permissions) == {SPCXB_ADDR}
        assert warnings == []

    def test_address_form_unknown_token_label_falls_back_to_short_address(self):
        permissions, _ = _extract_token_permissions("bsc", {"base_token": SPCXB_ADDR})
        (perm,) = permissions
        assert perm.label == f"ERC-20 ({SPCXB_ADDR[:6]}...{SPCXB_ADDR[-4:]})"

    def test_address_form_registry_known_token_gets_symbol_label(self):
        permissions, _ = _extract_token_permissions("bsc", {"quote_token": USDT_BSC_ADDR})
        (perm,) = permissions
        assert perm.target == USDT_BSC_ADDR
        assert perm.label == "ERC-20: USDT"

    def test_checksummed_address_normalizes_to_lowercase_target(self):
        checksummed = "0xbe9D156892E55e7154BcD3cB0FEA677F9D3103E1"
        permissions, _ = _extract_token_permissions("bsc", {"base_token": checksummed})
        assert _approve_targets(permissions) == {SPCXB_ADDR}

    def test_anvil_funding_address_keys_emit(self):
        """The exact ALM-3173 config shape: address-keyed anvil_funding plus
        the native symbol."""
        config = {"anvil_funding": {USDT_BSC_ADDR: 10000, SPCXB_ADDR: 10000, "BNB": 10}}
        permissions, warnings = _extract_token_permissions("bsc", config)
        assert _approve_targets(permissions) == {USDT_BSC_ADDR, SPCXB_ADDR}
        assert any("native asset" in w for w in warnings)


class TestFailClosed:
    def test_symbol_form_unknown_raises(self):
        with pytest.raises(PermissionGenerationError, match="SPCXB"):
            _extract_token_permissions("bsc", {"base_token": "SPCXB"})

    def test_symbol_form_known_still_emits(self):
        permissions, _ = _extract_token_permissions("bsc", {"quote_token": "USDT"})
        assert _approve_targets(permissions) == {USDT_BSC_ADDR}
        (perm,) = permissions
        assert perm.label == "ERC-20: USDT"

    def test_all_unresolved_symbols_named_in_error(self):
        with pytest.raises(PermissionGenerationError) as excinfo:
            _extract_token_permissions("bsc", {"base_token": "SPCXB", "reward_token": "NOTATOKEN"})
        assert "SPCXB" in str(excinfo.value)
        assert "NOTATOKEN" in str(excinfo.value)

    def test_resolver_unavailable_propagates(self):
        """Resolver construction failure must not silently drop all approvals."""
        with patch(
            "almanak.framework.data.tokens.get_token_resolver",
            side_effect=RuntimeError("resolver down"),
        ):
            with pytest.raises(RuntimeError, match="resolver down"):
                _extract_token_permissions("bsc", {"base_token": SPCXB_ADDR})


class TestNativeHandling:
    def test_native_sentinel_address_skipped_with_warning(self):
        permissions, warnings = _extract_token_permissions("bsc", {"base_token": NATIVE_SENTINEL})
        assert permissions == []
        assert any("native asset" in w for w in warnings)

    def test_native_symbol_skipped_with_warning(self):
        permissions, warnings = _extract_token_permissions("arbitrum", {"base_token": "ETH"})
        assert permissions == []
        assert any("native asset" in w for w in warnings)


class TestDeterminism:
    def test_resolution_never_touches_the_gateway(self):
        """A security manifest must not depend on live gateway state, and a
        market-search-resolved address must never enter a Safe grant."""
        resolver = MagicMock()
        resolver.resolve.side_effect = RuntimeError("unresolved")
        with patch("almanak.framework.data.tokens.get_token_resolver", return_value=resolver):
            with pytest.raises(PermissionGenerationError):
                _extract_token_permissions("bsc", {"base_token": "SPCXB", "quote_token": SPCXB_ADDR})
        assert resolver.resolve.call_count >= 1
        for call in resolver.resolve.call_args_list:
            assert call.kwargs.get("skip_gateway") is True


class TestNonStrictMode:
    def test_non_strict_records_omission_as_manifest_warning(self):
        """strict=False (multichain sweeps): the omission is loud, never silent."""
        permissions, warnings = _extract_token_permissions("bsc", {"base_token": "SPCXB"}, strict=False)
        assert permissions == []
        assert any("OMITTED" in w and "SPCXB" in w for w in warnings)

    def test_non_strict_still_emits_resolvable_tokens(self):
        permissions, warnings = _extract_token_permissions(
            "bsc", {"base_token": "SPCXB", "quote_token": USDT_BSC_ADDR}, strict=False
        )
        assert _approve_targets(permissions) == {USDT_BSC_ADDR}
        assert any("SPCXB" in w for w in warnings)


class TestGenerateManifestIntegration:
    def test_manifest_carries_unknown_token_approve_and_warnings(self):
        manifest = generate_manifest(
            strategy_name="spcxb-test",
            chain="bsc",
            supported_protocols=[],
            intent_types=["SWAP"],
            config={"base_token": SPCXB_ADDR, "anvil_funding": {"BNB": 10}},
        )
        assert SPCXB_ADDR in {p.target for p in manifest.permissions}
        assert any("native asset" in w for w in manifest.warnings)

    def test_manifest_generation_fails_on_unresolvable_symbol(self):
        with pytest.raises(PermissionGenerationError):
            generate_manifest(
                strategy_name="spcxb-test",
                chain="bsc",
                supported_protocols=[],
                intent_types=["SWAP"],
                config={"base_token": "SPCXB"},
            )
