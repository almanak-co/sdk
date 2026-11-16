"""Public connector choices come from the trusted composition root."""

from difflib import get_close_matches

from almanak.configuration.yaml import ConfigurationError
from almanak.connectors.composition import CONNECTOR_IDS
from almanak.connectors.identifiers import ConnectorId


def require_connector(value, *, field="strategy.connector", file="<configuration>") -> ConnectorId:
    candidates = tuple(str(identity) for identity in CONNECTOR_IDS)
    if isinstance(value, str) and value in candidates:
        return ConnectorId(value)
    suggestions = get_close_matches(value, candidates, n=1) if isinstance(value, str) else []
    hint = f" Did you mean {suggestions[0]!r}?" if suggestions else ""
    raise ConfigurationError(
        f"Unknown connector {value!r}.{hint} Supported connectors: {', '.join(candidates)}.",
        file=file,
        field=field,
        code="unknown_connector",
        remedy="Choose a canonical connector ID from almanak support --json.",
    )
