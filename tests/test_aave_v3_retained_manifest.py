"""A retained Safe/Roles manifest names the Pool implementation it was created against.

The recorded checkpoint is a deployment created on Ethereum while the Pool ran revision
11. The revision 12 identity is read from the captured revision 12 code through the
authority reader. Only the connector's identity reads are replaced; the comparison is
the gateway's own.
"""

import json
from types import SimpleNamespace

import pytest

from almanak.caip import ChainId
from almanak.connectors.aave_v3.permissions import AavePermissions
from almanak.connectors.aave_v3.reader import POOL_REVISIONS
from almanak.gateway.permissions import configuration_hash, verify
from almanak.gateway.roles_storage import AuthorityReader
from almanak.models import Block
from tests.aave_v3_helpers import recorded_v3 as recorded_v3
from tests.test_aave_v3_reader import captured, replay

POOL = "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2"


def revision_12_pool():
    """The Ethereum Pool's identity at a block after its upgrade, from recorded code."""
    raw = captured("ethereum", 12)
    with replay(raw) as reader:
        block = reader.pin_block(Block.model_validate_json(json.dumps(raw["block"])).number)
        return AuthorityReader(reader, block).identity(POOL)


def connector_reading(contracts):
    """The Aave V3 permission profile, with its identity reads answered by `contracts`."""
    return SimpleNamespace(
        codec=AavePermissions.codec,
        derive=AavePermissions.derive,
        identities=lambda config, account, reader: contracts,
    )


def verified(checkpoint, contracts):
    (partition,) = checkpoint.permissions.partitions
    head = partition.observation
    backend = SimpleNamespace(
        reader=SimpleNamespace(_pinned=head, rpc_count=0, chain=partition.profile.fund.chain)
    )
    # The capture predates the current configuration hash and grant derivation. Both are
    # produced again from the recorded configuration and capabilities, so the call
    # reaches the identity comparison. The contract identities are as recorded.
    retained = checkpoint.permissions
    grants = AavePermissions.derive(partition.profile, retained.capabilities)
    manifest = retained.model_copy(
        update={
            "configuration_hash": configuration_hash(checkpoint.config),
            "partitions": (partition.model_copy(update={"grants": grants}),),
        }
    )
    return verify(
        checkpoint.config,
        manifest,
        backend,
        connector=connector_reading(contracts),
        account=partition.account,
        head=head,
    )


def test_manifest_created_at_revision_11_is_refused_once_the_pool_runs_revision_12(recorded_v3):
    (partition,) = recorded_v3.permissions.partitions
    retained = next(c for c in partition.contracts if c.address == POOL)
    current = revision_12_pool()
    pins = POOL_REVISIONS[ChainId("eip155:1")]
    assert pins[(retained.code_hash, retained.implementation_hash)] == 11
    assert pins[(current.code_hash, current.implementation_hash)] == 12
    assert (retained.address, retained.code_hash) == (current.address, current.code_hash)
    observed = tuple(current if c.address == POOL else c for c in partition.contracts)
    with pytest.raises(ValueError, match="manifest contract identities differ"):
        verified(recorded_v3, observed)


def test_the_refusal_is_the_identity_comparison_and_nothing_before_it(recorded_v3):
    """With the retained identities the same call passes the comparison and goes on to
    read installed authority, which this reader does not serve."""
    (partition,) = recorded_v3.permissions.partitions
    with pytest.raises(AttributeError):
        verified(recorded_v3, partition.contracts)
