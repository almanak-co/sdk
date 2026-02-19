class SimulationError(Exception):
    """Base class for simulation errors."""

    pass


class GasPriceTooHigh(Exception):
    """Raised when gas price exceeds threshold, but within retry window"""

    pass


class GasPriceExceededFinalLimit(Exception):
    """Raised when gas price validation fails permanently"""

    pass
