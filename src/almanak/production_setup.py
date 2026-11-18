"""Read-only production admission and reviewable owner transaction export."""

from almanak.connectors.composition import connector_for
from almanak.gateway.production_cli import preflight_config, production_routes
from almanak.models import SafeRoles
from almanak.strategy import StrategyDefinition

FUNDING_REASON = "funding or market checks failed"
PERMISSIONS_REASON = "owner must install exported permissions"


def admit(config, env_file, source, output, *, prepared=None):
    """Check funding and Safe authority; report every gap an owner must close at once.

    Permissions are generated and exported even when funding fails, so one
    action_required round names both the missing funds and the owner transactions.
    """
    definition = checked_definition(config, source, prepared)
    result = preflight_config(config, env_file)
    if not requires_permissions(config):
        if not result["funded"]:
            return {"ready": False, "reason": FUNDING_REASON, "preflight": result}
        return {"ready": True, "preflight": result}
    from almanak.gateway.permissions import verify

    backend = production_routes(config, env_file)
    try:
        connector, manifest = exported(config, source, output, prepared, definition, backend)
        pending = any(partition.transactions for partition in manifest.partitions)
        reasons = [FUNDING_REASON] * (not result["funded"]) + [PERMISSIONS_REASON] * pending
        if reasons:
            return {
                "ready": False,
                "reason": "; ".join(reasons),
                "permissions": str(output),
                "preflight": result,
            }
        verified = verify(config, manifest, backend, connector=connector)
        return {"ready": True, "permissions": verified, "preflight": result}
    finally:
        backend.close()


def checked_definition(config, source, prepared):
    if prepared is None:
        return None
    definition = StrategyDefinition.read(source)
    prepared.require_configuration(config, definition)
    return definition


def requires_permissions(config):
    return any(isinstance(b.profile, SafeRoles) for b in config.accounts.bindings)


def exported(config, source, output, prepared, definition, backend):
    """Generate the manifest and export the owner bundles."""
    from almanak.gateway.permissions import generate

    connector = connector_for(config).permissions
    if definition is None:
        definition = StrategyDefinition.read(source)
    manifest = generate(config, definition, backend, connector=connector, prepared=prepared)
    export_permissions(manifest, output)
    return connector, manifest


def export_permissions(manifest, output):
    from almanak.gateway.permissions import write_artifacts

    if output is not None and not (output / manifest.digest).exists():
        write_artifacts(manifest, output / manifest.digest)
