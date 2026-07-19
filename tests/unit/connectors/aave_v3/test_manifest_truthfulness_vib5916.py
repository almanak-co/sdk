"""Manifest-truthfulness regression for Aave V3 (VIB-5916).

Phase 1 of the Aave-V3-on-Linea enablement made the canonical connector
manifest *truthful*:

* ``linea`` is declared strategy support (added after Phase-0 live-reserve
  verification);
* ``FLASH_LOAN`` is **removed** from ``strategy_intents`` — the flash-loan lane
  compiles but has no executable receiver/accounting support, so it must not be
  advertised as strategy support;
* the manual ``strategy_matrix_entries`` override is deleted, so the displayed
  support matrix DERIVES from ``(intents, chains)`` and cannot outrun the
  declaration (it previously optimistically claimed ``plasma`` / ``sonic``).

The load-bearing invariant these tests protect is the **decoupling** the
roadmap demands: dropping ``FLASH_LOAN`` from ``strategy_intents`` must NOT
deregister the Aave flash-loan *provider*, because provider discovery is keyed
on the dedicated ``flash_loan_*`` descriptor fields
(``almanak/connectors/_strategy_flash_loan_registry.py`` — driven purely by
``flash_loan_provider_name`` / ``flash_loan_provider`` / ``flash_loan_builder``,
never by ``strategy_intents``).

Scope guard: this file asserts *only* about ``aave_v3``. ``balancer_v2`` and
``morpho_blue`` still legitimately declare ``FLASH_LOAN`` as strategy support
and are out of scope.
"""

from __future__ import annotations

from almanak.connectors._connector import CONNECTOR_REGISTRY
from almanak.connectors._strategy_base.registry import ConnectorRegistry, _import_all_connectors
from almanak.connectors._strategy_flash_loan_registry import FLASH_LOAN_PROVIDER_REGISTRY
from almanak.framework.cli.support_matrix import _build_matrix
from almanak.framework.execution.config import SUPPORTED_PROTOCOLS
from almanak.framework.intents.vocabulary import IntentType

# The four executable lending intents Aave V3 genuinely compiles + executes.
_LENDING_INTENTS = {IntentType.SUPPLY, IntentType.BORROW, IntentType.REPAY, IntentType.WITHDRAW}


def _aave_manifest():
    """Return the strategy-side ``aave_v3`` manifest, forcing a full sweep.

    Other tests in the session may clear/repopulate ``ConnectorRegistry``; the
    sweep is the same one ``support_matrix._build_matrix`` and the coverage
    gate use, so it deterministically hydrates the registry here.
    """
    _import_all_connectors()
    manifest = ConnectorRegistry.get("aave_v3")
    assert manifest is not None, "aave_v3 must be registered in the strategy-side ConnectorRegistry"
    return manifest


# ---------------------------------------------------------------------------
# (a) linea is present in the connector-derived runtime manifest AND the
#     runtime protocol→chains gate; FLASH_LOAN is absent from strategy support.
# ---------------------------------------------------------------------------


def test_linea_present_in_connector_manifest_chains() -> None:
    manifest = _aave_manifest()
    assert "linea" in manifest.chains, (
        "linea must be declared in aave_v3 strategy_chains after VIB-5916 Phase-0 verification"
    )


def test_linea_present_in_supported_protocols_runtime_gate() -> None:
    # SUPPORTED_PROTOCOLS is derived from each connector's supported_chains.py;
    # it must stay exactly consistent with the canonical manifest chains for
    # the shipped lending surface (runtime gate == tested-support claim).
    assert "aave_v3" in SUPPORTED_PROTOCOLS
    assert "linea" in SUPPORTED_PROTOCOLS["aave_v3"]


def test_runtime_gate_matches_manifest_chains_exactly() -> None:
    manifest = _aave_manifest()
    assert SUPPORTED_PROTOCOLS["aave_v3"] == set(manifest.chains), (
        "supported_chains.py (runtime gate) must be exactly consistent with the canonical "
        "manifest strategy_chains for aave_v3 — no chain admitted at runtime that the "
        "manifest does not advertise as tested support, and none advertised but ungated."
    )


def test_plasma_and_sonic_not_advertised() -> None:
    manifest = _aave_manifest()
    # plasma (incomplete token catalogue) and sonic (untested) were only ever
    # claimed by the deleted optimistic matrix override — they must not leak
    # into either the manifest or the runtime gate.
    assert "plasma" not in manifest.chains
    assert "sonic" not in manifest.chains
    assert "plasma" not in SUPPORTED_PROTOCOLS["aave_v3"]
    assert "sonic" not in SUPPORTED_PROTOCOLS["aave_v3"]


def test_flash_loan_not_declared_as_strategy_support() -> None:
    manifest = _aave_manifest()
    assert IntentType.FLASH_LOAN not in manifest.intents, (
        "aave_v3 must NOT advertise FLASH_LOAN as strategy support — the lane compiles but "
        "has no executable receiver/accounting support (VIB-5916)."
    )
    assert set(manifest.intents) == _LENDING_INTENTS


# ---------------------------------------------------------------------------
# (b) The displayed support matrix for aave_v3 lending equals the manifest
#     chains — the matrix cannot outrun the declaration, and no flash_loan row.
# ---------------------------------------------------------------------------


def test_matrix_lending_row_equals_manifest_chains() -> None:
    manifest = _aave_manifest()
    matrix = _build_matrix()
    lending_rows = [p for p in matrix["protocols"] if p["name"] == "aave_v3" and p["category"] == "lending"]
    assert len(lending_rows) == 1, "aave_v3 must render exactly one lending row in the support matrix"
    assert set(lending_rows[0]["chains"]) == set(manifest.chains), (
        "The displayed matrix lending row must equal the manifest strategy_chains exactly — "
        "deleting the manual matrix override means the row derives from (intents, chains)."
    )


def test_matrix_has_no_aave_flash_loan_row() -> None:
    matrix = _build_matrix()
    flash_rows = [p for p in matrix["protocols"] if p["name"] == "aave_v3" and p["category"] == "flash_loan"]
    assert flash_rows == [], "aave_v3 must not render a flash_loan support-matrix row"


# ---------------------------------------------------------------------------
# (c) DECOUPLING: the aave_v3 flash-loan PROVIDER stays registered/discoverable
#     despite FLASH_LOAN being absent from strategy_intents. This is the
#     regression the roadmap explicitly requires.
# ---------------------------------------------------------------------------


def test_flash_loan_provider_still_discoverable_via_descriptor_fields() -> None:
    # Provider discovery is keyed on the descriptor's flash_loan_* fields, not
    # on strategy_intents — so removing FLASH_LOAN from strategy support must
    # leave the provider registered.
    descriptor = CONNECTOR_REGISTRY.get("aave_v3")
    assert descriptor is not None
    assert descriptor.flash_loan_provider_name == "aave"
    assert descriptor.flash_loan_provider is not None
    assert descriptor.flash_loan_builder is not None

    connectors_with_flash_loan = {c.name for c in CONNECTOR_REGISTRY.with_flash_loan()}
    assert "aave_v3" in connectors_with_flash_loan, (
        "aave_v3 must remain in the flash-loan connector set — provider discovery is "
        "decoupled from strategy_intents (VIB-5916)."
    )


def test_aave_provider_resolves_for_flash_loan_compilation() -> None:
    # The flash-loan compiler resolves a provider via
    # ``FLASH_LOAN_PROVIDER_REGISTRY.has(<name>)`` (compiler_flash_loan.py) and
    # names it in its "Supported providers: ..." error string. Provider "aave"
    # must still resolve even though FLASH_LOAN is no longer strategy support.
    assert FLASH_LOAN_PROVIDER_REGISTRY.has("aave"), (
        "FlashLoanIntent compilation for provider 'aave' must still resolve — the provider "
        "is registered independently of strategy_intents."
    )
    assert "aave" in FLASH_LOAN_PROVIDER_REGISTRY.names()
