"""ReproductionBundle legacy-chain deserialization contract (VIB-4851).

Mirror of tests/unit/data/tokens/test_token_chain_compat.py for the failure
forensics artifact: legacy records may carry UPPERCASE Chain-enum values;
``from_dict`` canonicalizes resolvable chains and passes unknown values
through verbatim so an artifact from an unrecognized chain still loads.
"""

from datetime import UTC, datetime

from almanak.framework.models.reproduction_bundle import ReproductionBundle


def _minimal_record(chain: str) -> dict:
    return {
        "bundle_id": "bundle-1",
        "deployment_id": "deployment:chaintest00",
        "failure_timestamp": datetime(2026, 1, 1, tzinfo=UTC).isoformat(),
        "block_number": 123,
        "chain": chain,
        "persistent_state": {},
        "config": {},
    }


def test_legacy_uppercase_chain_record_canonicalizes() -> None:
    bundle = ReproductionBundle.from_dict(_minimal_record("ETHEREUM"))
    assert bundle.chain == "ethereum"


def test_alias_chain_record_canonicalizes() -> None:
    bundle = ReproductionBundle.from_dict(_minimal_record("bnb"))
    assert bundle.chain == "bsc"


def test_unknown_chain_passes_through_verbatim() -> None:
    # Fail-open: a forensics artifact from an unrecognized chain must load.
    bundle = ReproductionBundle.from_dict(_minimal_record("notachain"))
    assert bundle.chain == "notachain"
