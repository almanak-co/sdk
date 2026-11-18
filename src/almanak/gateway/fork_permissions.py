"""Generated authority installed only by explicitly identified fork fixture owners."""

import json

from almanak.connectors.composition import connector_for
from almanak.gateway.fork_workspace import fork_routes
from almanak.gateway.permissions import generate, verify, write_artifacts
from almanak.gateway.safe_fixture import install_manifest
from almanak.strategy import StrategyDefinition


def prepare_permissions(nodes, deployment, source, output, *, prepared=None):
    backend = fork_routes(deployment, output / "fork-routes.json", output / "fork-keys.json")
    try:
        connector = connector_for(deployment).permissions
        manifest = generate(
            deployment,
            StrategyDefinition.read(source),
            backend,
            connector=connector,
            prepared=prepared,
        )
        write_artifacts(manifest, output / "permissions")
        receipts = install_manifest(nodes, manifest)
        result = verify(deployment, manifest, backend, connector=connector)
        (output / "permission-installation.json").write_text(json.dumps(receipts, indent=2) + "\n")
        (output / "permission-verification.json").write_text(json.dumps(result, indent=2) + "\n")
        return manifest
    finally:
        backend.close()
