"""Camelot connector."""

from almanak.framework.connectors.camelot.compiler import CamelotCompiler

__all__ = ["CamelotCompiler"]

# Connector registration (VIB-4298). The registry powers the (connector,
# intent, chain) coverage gate in scripts/ci/check_connector_registry.py.
from almanak.framework.connectors.registry import register_connector  # noqa: E402
from almanak.framework.intents.vocabulary import IntentType  # noqa: E402

register_connector(
    name="camelot",
    intents=(IntentType.SWAP,),
    chains=("arbitrum",),
)
