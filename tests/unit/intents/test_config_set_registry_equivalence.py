"""VIB-4928 PR-3b: pin the swap-classification + protocol-family registry views.

PR-3b inverted the seven config-set symbols in
``framework/intents/compiler_constants.py`` (``SWAP_FEE_TIERS``,
``DEFAULT_SWAP_FEE_TIER``, ``SWAP_ROUTER_V1_PROTOCOLS``,
``SWAP_ROUTER_V1_CHAIN_OVERRIDES``, ``SWAP_ROUTER_ALGEBRA_PROTOCOLS``,
``AAVE_COMPATIBLE_PROTOCOLS``, ``UNIV3_LP_GROUPING_PROTOCOLS``) off hand-imported
connector ``swap_constants`` / ``lp_constants`` / ``lending_constants`` modules
and onto two connector-self-registering registries
(``SWAP_CLASSIFICATION_REGISTRY`` / ``PROTOCOL_FAMILY_REGISTRY``).

These pin (a) the registry-derived views against the historical literal
snapshot, and (b) that ``compiler_constants`` surfaces exactly the
registry-derived values (the wiring). Sibling of
``test_contract_role_registry_equivalence.py`` for the PR-3a address tables.

Do NOT loosen to ``assert <subset>`` — silent classification drift on the swap
hot path is exactly what this file guards.
"""

from __future__ import annotations

import subprocess
import sys

import pytest

from almanak.connectors._strategy_base.protocol_family_registry import (
    PROTOCOL_FAMILY_REGISTRY,
    ProtocolFamily,
)
from almanak.connectors._strategy_base.swap_classification_registry import (
    SWAP_CLASSIFICATION_REGISTRY,
)


@pytest.fixture
def _bootstrapped_registries() -> None:
    """Reset both registries to a fresh, canonically-populated state.

    The registry-view snapshot classes assert *exact* equality against the
    historical literal snapshot, but ``SWAP_CLASSIFICATION_REGISTRY`` /
    ``PROTOCOL_FAMILY_REGISTRY`` are process-global mutable singletons. An
    earlier test that registers a synthetic slug (e.g. the ``__test_dex__``
    collision fixtures in ``test_compiler_constants_byte_equivalence.py``) and
    failed to pop it would otherwise leak into these exact-equality assertions
    and make the file order-dependent. So drop both singletons and repopulate
    them from the connector boot files' canonical ``_register_all`` — leaving
    exactly the real connector specs, nothing leaked.

    Applied (via ``usefixtures``) only to the snapshot classes. It is
    deliberately NOT applied to ``TestCompilerConstantsWiredToRegistries``:
    those tests must prove that importing ``compiler_constants`` *itself*
    bootstraps the registries (also pinned cold by
    ``test_cold_import_bootstraps_registries``), so force-populating the
    singletons there would mask a self-bootstrap regression.
    """
    import almanak.connectors._strategy_protocol_family_registry as _pf_boot
    import almanak.connectors._strategy_swap_classification_registry as _sc_boot

    SWAP_CLASSIFICATION_REGISTRY.reset()
    PROTOCOL_FAMILY_REGISTRY.reset()
    _sc_boot._register_all()
    _pf_boot._register_all()

EXPECTED_FEE_TIERS = {
    "uniswap_v3": (100, 500, 3000, 10000),
    "sushiswap_v3": (100, 500, 3000, 10000),
    "pancakeswap_v3": (100, 500, 2500, 10000),
    "agni_finance": (100, 500, 2500, 3000, 10000),
}
EXPECTED_DEFAULT_FEE_TIER = {
    "uniswap_v3": 3000,
    "sushiswap_v3": 3000,
    "pancakeswap_v3": 2500,
    "agni_finance": 3000,
}
EXPECTED_V1 = frozenset({"sushiswap_v3"})
EXPECTED_V1_CHAIN = {
    "mantle": frozenset({"agni_finance"}),
    "zerog": frozenset({"uniswap_v3"}),
}
EXPECTED_ALGEBRA = frozenset({"camelot"})
EXPECTED_AAVE = frozenset({"aave_v3"})
EXPECTED_UNIV3_LP = frozenset(
    {
        "uniswap_v3",
        "sushiswap_v3",
        "pancakeswap_v3",
        "aerodrome_slipstream",
        "velodrome_slipstream",
    }
)


@pytest.mark.usefixtures("_bootstrapped_registries")
class TestSwapClassificationRegistryViews:
    """The registry-derived views match the historical literal snapshot."""

    def test_fee_tiers(self) -> None:
        assert SWAP_CLASSIFICATION_REGISTRY.fee_tiers() == EXPECTED_FEE_TIERS

    def test_default_fee_tiers(self) -> None:
        assert SWAP_CLASSIFICATION_REGISTRY.default_fee_tiers() == EXPECTED_DEFAULT_FEE_TIER

    def test_router_v1_protocols(self) -> None:
        assert SWAP_CLASSIFICATION_REGISTRY.router_v1_protocols() == EXPECTED_V1

    def test_router_v1_chain_overrides(self) -> None:
        assert SWAP_CLASSIFICATION_REGISTRY.router_v1_chain_overrides() == EXPECTED_V1_CHAIN

    def test_router_algebra_protocols(self) -> None:
        assert SWAP_CLASSIFICATION_REGISTRY.router_algebra_protocols() == EXPECTED_ALGEBRA


@pytest.mark.usefixtures("_bootstrapped_registries")
class TestProtocolFamilyRegistryViews:
    def test_aave_v3_family(self) -> None:
        assert PROTOCOL_FAMILY_REGISTRY.members(ProtocolFamily.AAVE_V3) == EXPECTED_AAVE

    def test_univ3_lp_grouping_family(self) -> None:
        assert PROTOCOL_FAMILY_REGISTRY.members(ProtocolFamily.UNIV3_LP_GROUPING) == EXPECTED_UNIV3_LP


class TestCompilerConstantsWiredToRegistries:
    """``compiler_constants`` must surface exactly the registry-derived views.

    Before the PR-3b flip this proves registry == legacy-built; after the flip
    it proves ``compiler_constants`` is actually wired to the registries (not
    still reading the retired raw dicts).

    These tests deliberately do NOT use ``_bootstrapped_registries``: each one
    imports ``compiler_constants``, whose eager builders import the connector
    boot files and thereby self-bootstrap the registries. Pre-populating the
    singletons here would mask a regression where that self-bootstrap stopped
    happening (see ``test_cold_import_bootstraps_registries`` for the airtight
    cold-process proof).
    """

    def test_swap_fee_tiers_wired(self) -> None:
        from almanak.framework.intents.compiler_constants import SWAP_FEE_TIERS

        assert SWAP_FEE_TIERS == SWAP_CLASSIFICATION_REGISTRY.fee_tiers()

    def test_default_swap_fee_tier_wired(self) -> None:
        from almanak.framework.intents.compiler_constants import DEFAULT_SWAP_FEE_TIER

        assert DEFAULT_SWAP_FEE_TIER == SWAP_CLASSIFICATION_REGISTRY.default_fee_tiers()

    def test_swap_router_v1_protocols_wired(self) -> None:
        from almanak.framework.intents.compiler_constants import SWAP_ROUTER_V1_PROTOCOLS

        assert SWAP_ROUTER_V1_PROTOCOLS == SWAP_CLASSIFICATION_REGISTRY.router_v1_protocols()

    def test_swap_router_v1_chain_overrides_wired(self) -> None:
        from almanak.framework.intents.compiler_constants import (
            SWAP_ROUTER_V1_CHAIN_OVERRIDES,
        )

        assert SWAP_ROUTER_V1_CHAIN_OVERRIDES == SWAP_CLASSIFICATION_REGISTRY.router_v1_chain_overrides()

    def test_swap_router_algebra_protocols_wired(self) -> None:
        from almanak.framework.intents.compiler_constants import (
            SWAP_ROUTER_ALGEBRA_PROTOCOLS,
        )

        assert SWAP_ROUTER_ALGEBRA_PROTOCOLS == SWAP_CLASSIFICATION_REGISTRY.router_algebra_protocols()

    def test_aave_compatible_protocols_wired(self) -> None:
        from almanak.framework.intents.compiler_constants import AAVE_COMPATIBLE_PROTOCOLS

        assert AAVE_COMPATIBLE_PROTOCOLS == PROTOCOL_FAMILY_REGISTRY.members(ProtocolFamily.AAVE_V3)

    def test_univ3_lp_grouping_protocols_wired(self) -> None:
        from almanak.framework.intents.compiler_constants import (
            UNIV3_LP_GROUPING_PROTOCOLS,
        )

        assert UNIV3_LP_GROUPING_PROTOCOLS == PROTOCOL_FAMILY_REGISTRY.members(ProtocolFamily.UNIV3_LP_GROUPING)


# Run in a fresh interpreter so the process-global registry singletons start
# empty: importing ``compiler_constants`` ALONE (no connector boot-file import)
# must populate both registries via its eager builders. In-process this can't be
# proven — some other test/conftest import has already populated the singleton.
_COLD_IMPORT_PROBE = """
import almanak.framework.intents.compiler_constants as cc
from almanak.connectors._strategy_base.swap_classification_registry import (
    SWAP_CLASSIFICATION_REGISTRY,
)
from almanak.connectors._strategy_base.protocol_family_registry import (
    PROTOCOL_FAMILY_REGISTRY,
    ProtocolFamily,
)

# Importing compiler_constants alone must have self-bootstrapped both registries.
assert SWAP_CLASSIFICATION_REGISTRY.registered_protocols(), "swap registry empty after cold import"
assert PROTOCOL_FAMILY_REGISTRY.members(ProtocolFamily.AAVE_V3), "aave family empty after cold import"
assert PROTOCOL_FAMILY_REGISTRY.members(ProtocolFamily.UNIV3_LP_GROUPING), "univ3-lp family empty after cold import"

# ...and the surfaced symbols equal the registry-derived views (wiring holds from
# cold, not merely because a sibling test pre-populated the singleton).
assert cc.SWAP_FEE_TIERS == SWAP_CLASSIFICATION_REGISTRY.fee_tiers()
assert cc.SWAP_ROUTER_V1_CHAIN_OVERRIDES == SWAP_CLASSIFICATION_REGISTRY.router_v1_chain_overrides()
assert cc.AAVE_COMPATIBLE_PROTOCOLS == PROTOCOL_FAMILY_REGISTRY.members(ProtocolFamily.AAVE_V3)
assert cc.UNIV3_LP_GROUPING_PROTOCOLS == PROTOCOL_FAMILY_REGISTRY.members(ProtocolFamily.UNIV3_LP_GROUPING)

print("COLD_IMPORT_OK")
"""


def test_cold_import_bootstraps_registries() -> None:
    """A cold ``import compiler_constants`` self-populates both registries.

    Guards the self-bootstrap contract the in-process wiring tests cannot: in a
    full session the registry singletons are already populated, so only a fresh
    interpreter proves ``compiler_constants`` does the bootstrapping itself
    rather than depending on a connector boot file being imported elsewhere.
    """
    result = subprocess.run(
        [sys.executable, "-c", _COLD_IMPORT_PROBE],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, (
        f"cold-import probe failed (exit {result.returncode}):\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    assert "COLD_IMPORT_OK" in result.stdout
