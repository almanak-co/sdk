"""Unresolved position economics that name a market and a fund account.

Each connector's position inputs extend this base and carry an excluded
`connector` tag that the parser derives from the named market, so the author
never writes it and the authored digest never includes it.
"""

from almanak.configuration.input_values import resolve_quantity
from almanak.configuration.yaml import ConfigurationError
from almanak.gateway.operator_config import Name
from almanak.models import Model


class PositionInput(Model):
    market: Name
    account: Name


def converted(value, decimals, field):
    try:
        return resolve_quantity(value, decimals)
    except ConfigurationError as error:
        raise ConfigurationError(str(error), field=field) from error
