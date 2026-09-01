"""Protocol-neutral permission closure and resource discrimination controls."""

from __future__ import annotations

import copy

import pytest
from web3 import Web3

from almanak.framework.execution.signer.safe.constants import get_multisend_address
from almanak.framework.execution.signer.safe.multisend import MultiSendEncoder
from scripts.qa.permission_attestation import derive_permission_attestation, validate_permission_attestation

POOL = "0x1111111111111111111111111111111111111111"
CATALOG_POOL = "0x2222222222222222222222222222222222222222"
SELECTOR = "0xabcdef01"


def _manifest(*targets: tuple[str, str, int, bool]) -> dict[str, object]:
    return {
        "permissions": [
            {
                "target": target,
                "function_selectors": [] if not selector else [{"selector": selector}],
                "operation": operation,
                "send_allowed": send_allowed,
            }
            for target, selector, operation, send_allowed in targets
        ]
    }


def _resource(*, address: str = POOL) -> dict[str, object]:
    return {
        "kind": "exact_pool",
        "address": address,
        "source": "live_registry",
        "authoritative": True,
        "catalog_scope": "curve.ethereum.curated_pools",
        "catalog_targets": [CATALOG_POOL],
    }


def test_exact_external_resource_is_bound_to_compiled_call_and_permission() -> None:
    attestation = derive_permission_attestation(
        transactions=[{"to": POOL, "data": SELECTOR + "00" * 32, "operation": 0, "value": 0}],
        manifest=_manifest((POOL, SELECTOR, 0, False)),
        chain="ethereum",
        exact_resources=[_resource()],
    )

    assert attestation["status"] == "PASS"
    assert attestation["resource_discrimination"] == "PASS"
    assert validate_permission_attestation(attestation) == attestation


def test_catalogue_sample_cannot_substitute_for_compiled_exact_resource() -> None:
    """Regression shape from #3724: runtime pool and Safe pool disagree."""
    attestation = derive_permission_attestation(
        transactions=[{"to": POOL, "data": SELECTOR, "operation": 0, "value": 0}],
        manifest=_manifest((CATALOG_POOL, SELECTOR, 0, False)),
        chain="ethereum",
        exact_resources=[_resource()],
    )

    assert attestation["status"] == "FAIL"
    assert attestation["missing_authorizations"][0]["target"] == POOL.lower()
    assert any("absent from permission grants" in failure for failure in attestation["resource_failures"])
    with pytest.raises(ValueError, match="permission closure failed"):
        validate_permission_attestation(attestation)


@pytest.mark.parametrize(
    ("permission", "transaction"),
    [
        ((POOL, "0xdeadbeef", 0, False), {"to": POOL, "data": SELECTOR, "operation": 0, "value": 0}),
        ((POOL, SELECTOR, 1, False), {"to": POOL, "data": SELECTOR, "operation": 0, "value": 0}),
        ((POOL, SELECTOR, 0, False), {"to": POOL, "data": SELECTOR, "operation": 0, "value": 1}),
    ],
    ids=("selector", "operation", "native-value"),
)
def test_every_load_bearing_permission_dimension_is_checked(
    permission: tuple[str, str, int, bool], transaction: dict[str, object]
) -> None:
    attestation = derive_permission_attestation(
        transactions=[transaction], manifest=_manifest(permission), chain="ethereum"
    )
    assert attestation["status"] == "FAIL"
    assert attestation["missing_authorizations"]


def test_multisend_wrapper_permission_cannot_hide_missing_inner_permission() -> None:
    chain = "arbitrum"
    multisend = get_multisend_address(chain)
    calldata = MultiSendEncoder.encode_from_dicts([{"to": POOL, "value": 0, "data": SELECTOR}], Web3())
    attestation = derive_permission_attestation(
        transactions=[{"to": multisend, "data": calldata, "operation": 1, "value": 0}],
        manifest=_manifest((multisend, "0x8d80ff0a", 1, False)),
        chain=chain,
        exact_resources=[_resource()],
    )

    assert attestation["status"] == "FAIL"
    assert [call["path"] for call in attestation["effective_calls"]] == [
        "transactions[0]",
        "transactions[0].multisend[0]",
    ]
    assert attestation["missing_authorizations"][0]["target"] == POOL.lower()


def test_sealer_rederivation_rejects_a_tampered_pass() -> None:
    attestation = derive_permission_attestation(
        transactions=[{"to": POOL, "data": SELECTOR, "operation": 0, "value": 0}],
        manifest=_manifest((POOL, SELECTOR, 0, False)),
        chain="ethereum",
    )
    tampered = copy.deepcopy(attestation)
    tampered["manifest_grants"][0]["target"] = CATALOG_POOL

    with pytest.raises(ValueError, match="not entailed"):
        validate_permission_attestation(tampered)


def test_manifest_boolean_is_typed_not_truthy() -> None:
    manifest = _manifest((POOL, SELECTOR, 0, False))
    manifest["permissions"][0]["send_allowed"] = "false"  # type: ignore[index]

    with pytest.raises(ValueError, match="send_allowed must be boolean"):
        derive_permission_attestation(
            transactions=[{"to": POOL, "data": SELECTOR}], manifest=manifest, chain="ethereum"
        )


def test_external_to_catalog_is_measured_not_a_producer_label() -> None:
    attestation = derive_permission_attestation(
        transactions=[{"to": POOL, "data": SELECTOR}],
        manifest=_manifest((POOL, SELECTOR, 0, False)),
        chain="ethereum",
        exact_resources=[_resource()],
    )
    attestation["exact_resources"][0]["external_to_catalog"] = False

    with pytest.raises(ValueError, match="not derived"):
        validate_permission_attestation(attestation)


# --- Live-money gate: an unknown chain label cannot produce an attestation ---
#
# Reproduced on main: ``effective_calls`` resolved the canonical MultiSend with
# ``MULTISEND_ADDRESSES.get(chain.lower()) or ""``, so a chain label the registry
# does not know produced a permission closure attestation for calls that carried
# no MultiSend selector. A registered chain that has no Safe MultiSend is a
# different case and stays attestable for plain calls.


@pytest.mark.parametrize("chain", ["not-a-chain", "arbitrum ", "Arbitrum", ""])
def test_an_unknown_chain_label_cannot_be_attested(chain: str) -> None:
    with pytest.raises(ValueError, match="Unknown chain"):
        derive_permission_attestation(
            transactions=[{"to": POOL, "data": SELECTOR}],
            manifest=_manifest((POOL, SELECTOR, 0, False)),
            chain=chain,
        )


def _registered_chain_without_multisend() -> str:
    from almanak.core.chains import ChainRegistry
    from almanak.framework.execution.signer.safe.constants import MULTISEND_ADDRESSES

    candidates = sorted(set(ChainRegistry.names()) - set(MULTISEND_ADDRESSES))
    if not candidates:
        pytest.skip("every registered chain declares a Safe MultiSend")
    return candidates[0]


def test_a_registered_chain_without_a_multisend_attests_plain_calls_and_refuses_batches() -> None:
    chain = _registered_chain_without_multisend()

    attestation = derive_permission_attestation(
        transactions=[{"to": POOL, "data": SELECTOR}],
        manifest=_manifest((POOL, SELECTOR, 0, False)),
        chain=chain,
    )
    assert attestation["missing_authorizations"] == []

    batch = MultiSendEncoder.encode_from_dicts([{"to": POOL, "value": 0, "data": SELECTOR}], Web3())
    with pytest.raises(ValueError, match="not the canonical MultiSend"):
        derive_permission_attestation(
            transactions=[{"to": POOL, "data": batch, "operation": 1}],
            manifest=_manifest((POOL, SELECTOR, 0, False)),
            chain=chain,
        )
