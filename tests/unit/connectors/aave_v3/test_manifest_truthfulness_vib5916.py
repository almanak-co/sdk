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

VIB-6111 extends the same truthfulness contract to a *per-chain* claim: BORROW
is advertised on every declared chain EXCEPT ``mantle``, where Aave governance
zeroed ``ltv`` on all 10 reserves so no asset can be enabled as collateral.
That exact coverage is expressed through ``SupportedChainsSpec.intent_overrides``;
SUPPLY / WITHDRAW / REPAY still work on Mantle.

Scope guard: this file asserts *only* about ``aave_v3``. ``balancer_v2`` and
``morpho_blue`` still legitimately declare ``FLASH_LOAN`` as strategy support
and are out of scope.
"""

from __future__ import annotations

from almanak.connectors._connector import CONNECTOR_REGISTRY
from almanak.connectors._strategy_flash_loan_registry import FLASH_LOAN_PROVIDER_REGISTRY
from almanak.framework.cli.support_matrix import _build_matrix
from almanak.framework.execution.config import SUPPORTED_PROTOCOLS

# The four executable lending intents Aave V3 genuinely compiles + executes.
_LENDING_INTENTS = {"SUPPLY", "BORROW", "REPAY", "WITHDRAW"}


def _aave_manifest():
    """Return the canonical Aave V3 connector descriptor."""
    manifest = CONNECTOR_REGISTRY.get("aave_v3")
    assert manifest is not None, "aave_v3 must be registered"
    return manifest


# ---------------------------------------------------------------------------
# (a) linea is present in the connector-derived runtime manifest AND the
#     runtime protocol→chains gate; FLASH_LOAN is absent from strategy support.
# ---------------------------------------------------------------------------


def test_linea_present_in_connector_manifest_chains() -> None:
    manifest = _aave_manifest()
    assert "linea" in manifest.all_supported_chains, (
        "linea must be declared in aave_v3 supported_chains after VIB-5916 Phase-0 verification"
    )


def test_linea_present_in_supported_protocols_runtime_gate() -> None:
    # SUPPORTED_PROTOCOLS is derived from each connector's inline support spec;
    # it must stay exactly consistent with the canonical manifest chains for
    # the shipped lending surface (runtime gate == tested-support claim).
    assert "aave_v3" in SUPPORTED_PROTOCOLS
    assert "linea" in SUPPORTED_PROTOCOLS["aave_v3"]


def test_runtime_gate_matches_manifest_chains_exactly() -> None:
    manifest = _aave_manifest()
    assert SUPPORTED_PROTOCOLS["aave_v3"] == set(manifest.all_supported_chains), (
        "the runtime gate must be exactly consistent with the canonical "
        "supported_chains for aave_v3 — no chain admitted at runtime that the "
        "manifest does not advertise as tested support, and none advertised but ungated."
    )


def test_plasma_and_sonic_not_advertised() -> None:
    manifest = _aave_manifest()
    # plasma (incomplete token catalogue) and sonic (untested) were only ever
    # claimed by the deleted optimistic matrix override — they must not leak
    # into either the manifest or the runtime gate.
    assert "plasma" not in manifest.all_supported_chains
    assert "sonic" not in manifest.all_supported_chains
    assert "plasma" not in SUPPORTED_PROTOCOLS["aave_v3"]
    assert "sonic" not in SUPPORTED_PROTOCOLS["aave_v3"]


def test_flash_loan_not_declared_as_strategy_support() -> None:
    manifest = _aave_manifest()
    assert "FLASH_LOAN" not in (manifest.strategy_intent_names or ()), (
        "aave_v3 must NOT advertise FLASH_LOAN as strategy support — the lane compiles but "
        "has no executable receiver/accounting support (VIB-5916)."
    )
    assert set(manifest.strategy_intent_names or ()) == _LENDING_INTENTS


# ---------------------------------------------------------------------------
# (b) The displayed support matrix for aave_v3 lending equals the manifest
#     chains — the matrix cannot outrun the declaration, and no flash_loan row.
# ---------------------------------------------------------------------------


def test_matrix_lending_row_equals_manifest_chains() -> None:
    manifest = _aave_manifest()
    matrix = _build_matrix()
    lending_rows = [p for p in matrix["protocols"] if p["name"] == "aave_v3" and p["category"] == "lending"]
    assert len(lending_rows) == 1, "aave_v3 must render exactly one lending row in the support matrix"
    assert set(lending_rows[0]["chains"]) == set(manifest.all_supported_chains), (
        "The displayed matrix lending row must equal supported_chains exactly — "
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


# ---------------------------------------------------------------------------
# (d) VIB-6111 — BORROW is NOT supported on mantle.
#
# Aave governance set ``ltv = 0`` on all 10 reserves of the Aave V3 Mantle
# market at block ~98303344 (2026-07-22). ``liquidationThreshold`` was left
# intact, nothing was frozen, ``isActive`` stayed true. Consequence: no asset on
# that market can be enabled as collateral, so BORROW is impossible for EVERY
# asset on the chain — while SUPPLY / WITHDRAW / REPAY keep working.
#
# The manifest declares this exact per-intent coverage with an override rather
# than dropping Mantle from the connector-wide union.
# ---------------------------------------------------------------------------


def test_borrow_is_not_advertised_on_mantle() -> None:
    manifest = _aave_manifest()
    borrow_chains = manifest.supported_chains_for_intent("BORROW") or ()
    assert "mantle" not in borrow_chains, (
        "aave_v3 must NOT advertise BORROW on mantle — governance zeroed ltv on every "
        "reserve, so no asset can be enabled as collateral (VIB-6111)."
    )
    # The override narrows exactly one cell: every other declared chain keeps BORROW.
    assert set(borrow_chains) == set(manifest.all_supported_chains) - {"mantle"}
    assert len(borrow_chains) == 9


def test_supply_withdraw_and_repay_still_advertised_on_mantle() -> None:
    manifest = _aave_manifest()
    for intent in ("SUPPLY", "WITHDRAW"):
        assert "mantle" in (manifest.supported_chains_for_intent(intent) or ()), (
            f"{intent} still works on Aave V3 Mantle (only ltv was zeroed) and must stay advertised (VIB-6111)."
        )
    # REPAY explicitly and separately: pre-existing borrowers on Aave V3 Mantle
    # must be able to repay their debt. Narrowing REPAY away — or dropping the
    # chain wholesale — would strand exactly the users who most need the verb.
    assert "mantle" in (manifest.supported_chains_for_intent("REPAY") or ()), (
        "REPAY must remain available on mantle so pre-existing borrowers can unwind."
    )


def test_mantle_intents_for_chain_is_the_three_working_verbs() -> None:
    manifest = _aave_manifest()
    assert {
        intent for intent in manifest.strategy_intent_names or () if manifest.supports(chain="mantle", intent=intent)
    } == {"SUPPLY", "REPAY", "WITHDRAW"}
    # Any other declared chain keeps the full four-verb lending surface.
    assert {
        intent for intent in manifest.strategy_intent_names or () if manifest.supports(chain="base", intent=intent)
    } == _LENDING_INTENTS


def test_mantle_stays_a_declared_chain_and_a_gated_chain() -> None:
    """The intent override narrows BORROW, not the connector-wide union."""
    manifest = _aave_manifest()
    assert "mantle" in manifest.all_supported_chains
    assert "mantle" in SUPPORTED_PROTOCOLS["aave_v3"]
    # And the exact-equality invariant from VIB-5916 still holds.
    assert SUPPORTED_PROTOCOLS["aave_v3"] == set(manifest.all_supported_chains)


def test_mantle_borrow_is_encoded_in_the_unified_intent_override() -> None:
    manifest = _aave_manifest()
    assert manifest.supported_chains is not None
    from almanak.core.intent_types import IntentType

    assert manifest.supported_chains.intent_overrides[IntentType.BORROW] == manifest.supported_chains_for_intent(
        "BORROW"
    )


def test_matrix_lending_row_still_contains_mantle() -> None:
    """The derived lending row keeps mantle — SUPPLY/WITHDRAW/REPAY still work there.

    The support matrix unions a category's chains over that category's intents,
    so a category only disappears from a chain when EVERY verb in it is
    excluded. Aave V3 keeps three of four lending verbs on mantle.
    """
    matrix = _build_matrix()
    lending_rows = [p for p in matrix["protocols"] if p["name"] == "aave_v3" and p["category"] == "lending"]
    assert len(lending_rows) == 1
    assert "mantle" in lending_rows[0]["chains"]
