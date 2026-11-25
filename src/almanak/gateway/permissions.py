"""Public permission generation and installed-authority verification implementation."""

import hashlib
import json
from pathlib import Path

from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import chain_number
from almanak.gateway.permission_models import (
    OwnerTransaction,
    PermissionManifest,
    PermissionPartition,
)
from almanak.gateway.roles_storage import (
    AuthorityReader,
    mapping_slot,
    verify_grant_storage,
    verify_wiring,
)
from almanak.gateway.routing import backend_for
from almanak.gateway.safe_fixture import calldata
from almanak.models import Eoa, SafeRoles


def configuration_hash(config) -> str:
    return hashlib.sha256(config.model_dump_json().encode()).hexdigest()


def owner_call(profile, signature, types, args):
    return OwnerTransaction(to=profile.roles_module, data=calldata(signature, types, args))


def installation_bundle(partition, *, assign_operator=True):
    profile = partition.profile
    role = bytes.fromhex(profile.role_key[2:])
    calls = [
        owner_call(profile, "scopeTarget(bytes32,address)", ("bytes32", "address"), (role, target))
        for target in sorted({g.target for g in partition.grants})
    ]
    for grant in partition.grants:
        calls.append(function_installation(profile, grant))
    if assign_operator:
        calls.append(
            owner_call(
                profile,
                "assignRoles(address,bytes32[],bool[])",
                ("address", "bytes32[]", "bool[]"),
                (profile.operator.address, [role], [True]),
            )
        )
    return tuple(calls)


def function_installation(profile, grant):
    if grant.condition is None:
        return owner_call(
            profile,
            "allowFunction(bytes32,address,bytes4,uint8)",
            ("bytes32", "address", "bytes4", "uint8"),
            (bytes.fromhex(profile.role_key[2:]), grant.target, grant.selector, 0),
        )
    return owner_call(
        profile,
        "scopeFunction(bytes32,address,bytes4,(uint8,uint8,uint8,bytes)[],uint8)",
        ("bytes32", "address", "bytes4", "(uint8,uint8,uint8,bytes)[]", "uint8"),
        (
            bytes.fromhex(profile.role_key[2:]),
            grant.target,
            grant.selector,
            [c.abi() for c in grant.conditions],
            0,
        ),
    )


def generate(config, strategy, backend, *, connector, prepared=None) -> PermissionManifest:
    """Derive each Safe partition's grants and its owner bundle or installed verification.

    Installed authority is read from Roles storage at the pinned block. Only the grants the
    strategy needs are checked; dedicated Safes accept that grants beyond them,
    other members of the role or other roles of the operator go undetected.
    """
    if prepared is None:
        capabilities = connector.codec.evaluate(strategy, config)
    else:
        prepared.require_configuration(config, strategy)
        capabilities = prepared.capabilities
    bindings = safe_bindings(config)
    if not bindings:
        # Nothing takes generated authority: a caller that asks anyway has mistaken
        # the deployment's accounts, and an empty manifest would hide that.
        raise ValueError("permission generation requires a bound Safe account; none is bound")
    partitions = []
    for binding in bindings:
        selected = backend_for(backend, binding.profile.fund.chain)
        selected.guard()
        block = selected.reader.pin_block("latest")
        reader = AuthorityReader(selected.reader, block)
        partition = PermissionPartition(
            account=binding.name,
            profile=binding.profile,
            grants=connector.derive(binding.profile, capabilities),
            contracts=connector.identities(config, binding.name, reader),
            observation=block,
            transactions=(),
        )
        if installation_started(reader, partition):
            # This verifies the wiring, with the operator enabled, before the grants.
            verify_partition(reader, partition)
        else:
            assigned = operator_assigned(reader, binding.profile)
            verify_wiring(reader, binding.profile, operator_enabled=assigned)
            bundle = installation_bundle(partition, assign_operator=not assigned)
            partition = partition.model_copy(update={"transactions": bundle})
        partitions.append(partition)
    return connector.manifest_type(
        runtime_checks=connector.runtime_checks,
        source_hash=strategy.sha256,
        configuration_hash=configuration_hash(config),
        capabilities=capabilities,
        partitions=tuple(partitions),
    )


def safe_bindings(config) -> tuple:
    """The bound accounts that take generated authority: the Safes.

    An EOA account signs with its own key and takes no grant, so a deployment may bind
    one beside a Safe, as a bridge between a Safe and an EOA does. It has no partition.
    """
    for binding in config.accounts.bindings:
        if not isinstance(binding.profile, (Eoa, SafeRoles)):
            raise ValueError("permission generation requires an explicit supported account adapter")
    return tuple(b for b in config.accounts.bindings if isinstance(b.profile, SafeRoles))


def installation_started(reader, partition) -> bool:
    """Whether any of the partition's scoped targets is present in Roles storage.

    One present means the owner has installed, or started to: the partition is then verified
    in full, which refuses a partial installation. None present means it is pending.
    """
    profile = partition.profile
    role_slot = mapping_slot("bytes32", bytes.fromhex(profile.role_key[2:]), 4)
    for target in sorted({grant.target for grant in partition.grants}):
        if reader.storage(profile.roles_module, mapping_slot("address", target, role_slot + 1)):
            return True
    return False


def operator_assigned(reader, profile) -> bool:
    """Whether the operator already holds the role and is the Roles module's only module.

    A Safe may assign it in its creation transaction, and its bundle then omits assignRoles.
    """
    if reader.storage(profile.roles_module, membership_slot(profile)) != 1:
        return False
    return reader.modules(profile.roles_module) == (profile.operator.address,)


def membership_slot(profile) -> int:
    role_slot = mapping_slot("bytes32", bytes.fromhex(profile.role_key[2:]), 4)
    return mapping_slot("address", profile.operator.address, role_slot)


def verify_partition(reader, partition):
    """Every generated grant is installed exactly, with the operator in the role."""
    profile = partition.profile
    verify_wiring(reader, profile, operator_enabled=True)
    for grant in partition.grants:
        verify_grant_storage(reader, profile, grant)
    if reader.storage(profile.roles_module, membership_slot(profile)) != 1:
        raise DataError("authority", "operator role membership is absent from storage")
    default = mapping_slot("address", profile.operator.address, 7)
    if reader.storage(profile.roles_module, default):
        raise DataError("authority", "operator has an unexpected default role")
    for identity in partition.contracts:
        if reader.identity(identity.address) != identity:
            raise DataError("authority", "permission target code changed after generation")
    return {"account": partition.account}


def validate_manifest(config, manifest, *, connector):
    if manifest.configuration_hash != configuration_hash(config):
        raise ValueError("permission manifest belongs to another authoritative configuration")
    connector.codec.validate(config, manifest.capabilities)
    if tuple(p.account for p in manifest.partitions) != tuple(
        b.name for b in safe_bindings(config)
    ):
        raise ValueError("manifest must retain each Safe authorization partition exactly once")
    for partition in manifest.partitions:
        binding = config.accounts.binding(partition.account)
        if binding.profile != partition.profile:
            raise ValueError("manifest account adapter differs from its retained binding")
        if partition.grants != connector.derive(partition.profile, manifest.capabilities):
            raise ValueError("manifest grants differ from trusted connector derivation")


def verify(config, manifest, backend, *, connector, account=None, head=None, reads=None):
    """Verify installed authority at the latest head, or at `head`, which one account's
    caller has just pinned after guarding its provider. `reads` replays that account's
    previous verification reads in batches and retains this one's."""
    if (head is not None or reads is not None) and account is None:
        raise ValueError("a caller-pinned head or read plan verifies exactly one account")
    validate_manifest(config, manifest, connector=connector)
    results, observations, calls = [], [], 0
    for partition in manifest.partitions:
        if account is not None and partition.account != account:
            continue
        selected = backend_for(backend, partition.profile.fund.chain)
        initial_calls = selected.reader.rpc_count
        if head is None:
            selected.guard()
            block = selected.reader.pin_block("latest")
        else:
            block = head
        reader = AuthorityReader(selected.reader, block, reads)
        expected = connector.identities(config, partition.account, reader)
        if expected != partition.contracts:
            raise ValueError("manifest contract identities differ from the supported profile")
        results.append(verify_partition(reader, partition))
        reader.retain_plan()
        observations.append(block.model_dump(mode="json"))
        calls += selected.reader.rpc_count - initial_calls
    return {
        "manifest": manifest.digest,
        "observation": observations[0] if len(observations) == 1 else None,
        "observations": observations,
        "partitions": results,
        "verification_rpc_calls": calls,
    }


STATEMENT_SCHEMA = "almanak-permissions-v1"


def safe_bundle(partition: PermissionPartition) -> dict:
    """The partition's owner transactions as one Safe Transaction Builder batch."""
    return {
        "version": "1.0",
        "chainId": str(chain_number(partition.profile.fund.chain.chain_id)),
        "createdAt": partition.observation.timestamp * 1000,
        "meta": {
            "name": f"Almanak {partition.account} generated permissions",
            "txBuilderVersion": "1.18.0",
            "createdFromSafeAddress": partition.profile.fund.address,
        },
        "transactions": [t.model_dump() for t in partition.transactions],
    }


def grant_explanations(partition: PermissionPartition) -> list[str]:
    return [e for grant in partition.grants for e in grant.explanation]


def write_artifacts(manifest: PermissionManifest, output: Path):
    """Write the manifest, one owner batch per partition still lacking authority, and notes.

    `safe-<index>.json` keeps the partition's manifest index. A partition whose authority
    is already installed has nothing to sign and gets no batch, as `statement` reports it.
    """
    output.mkdir(parents=True, exist_ok=False)
    (output / "manifest.json").write_text(manifest.model_dump_json(indent=2) + "\n")
    explanations = []
    for index, partition in enumerate(manifest.partitions):
        if partition.transactions:
            bundle = safe_bundle(partition)
            (output / f"safe-{index}.json").write_text(json.dumps(bundle, indent=2) + "\n")
        explanations.append(partition.account + " (" + partition.profile.fund.address + ")")
        explanations.extend(grant_explanations(partition))
    explanations.extend(("Runtime checks, not enforced by Roles:", *manifest.runtime_checks))
    (output / "permissions.txt").write_text("\n".join(explanations) + "\n")


def statement(manifest: PermissionManifest) -> dict:
    """What each Safe owner must install before admission, as `check --json` reports it.

    Only partitions without installed authority carry a bundle, the same batch admission
    exports. The installed state is observed at `observed_block`; admission verifies it
    again, so a statement never makes a deployment ready.
    """
    return {
        "schema": STATEMENT_SCHEMA,
        "manifest_digest": manifest.digest,
        "pending": any(partition.transactions for partition in manifest.partitions),
        "partitions": [partition_statement(partition) for partition in manifest.partitions],
        "runtime_checks": list(manifest.runtime_checks),
    }


def partition_statement(partition: PermissionPartition) -> dict:
    profile = partition.profile
    return {
        "account": partition.account,
        "chain": str(profile.fund.chain.chain_id),
        "safe": str(profile.fund.address),
        "roles_module": str(profile.roles_module),
        "operator": str(profile.operator.address),
        "observed_block": partition.observation.number,
        "status": "pending" if partition.transactions else "installed",
        "explanation": grant_explanations(partition),
        "bundle": safe_bundle(partition) if partition.transactions else None,
    }
