"""Strategy framework exceptions.

This module defines custom exceptions raised by strategy lifecycle hooks.

Key Exceptions:
    - ConfigValidationError: Raised by ``IntentStrategy.validate_config()`` when
      a strategy's configuration fails validation.

Example:
    from decimal import Decimal
    from almanak.framework.strategies.exceptions import ConfigValidationError

    class MyStrategy(IntentStrategy):
        def validate_config(self) -> None:
            # NOTE: configs loaded from JSON / env come back as strings.
            # Coerce numerics through Decimal(str(...)) to avoid lexicographic
            # comparisons (e.g. "9" >= "10" is True as strings but False as numbers).
            size = Decimal(str(self.get_config("trade_size_usd", "0")))
            if size <= 0:
                raise ConfigValidationError(
                    "trade_size_usd must be > 0",
                    field="trade_size_usd",
                )
"""


class ConfigValidationError(Exception):
    """Raised when a strategy's configuration fails ``validate_config()``.

    Strategies override :py:meth:`IntentStrategy.validate_config` to enforce
    preconditions on their configuration (e.g. required fields, value ranges,
    cross-field invariants). When validation fails, raise this exception so
    the framework — and tooling like the Portfolio Manager's ``strat check``
    preflight — can surface an actionable error to the operator.

    Attributes:
        message: Human-readable explanation of the validation failure.
        field: Optional name of the configuration field that failed. ``None``
            when the error is cross-field or not attributable to a single field.

    Example:
        raise ConfigValidationError(
            "rsi_overbought must be greater than rsi_oversold",
            field="rsi_overbought",
        )
    """

    def __init__(self, message: str, field: str | None = None) -> None:
        """Initialize the exception.

        Args:
            message: Human-readable description of the validation failure.
            field: Optional name of the offending config field. Stored as an
                attribute for programmatic access (e.g. CLI error rendering).
        """
        self.message = message
        self.field = field

        if field is not None:
            formatted = f"Config validation failed for '{field}': {message}"
        else:
            formatted = f"Config validation failed: {message}"
        super().__init__(formatted)

    def __repr__(self) -> str:
        """Return a detailed representation of the exception."""
        return f"{self.__class__.__name__}(message={self.message!r}, field={self.field!r})"


__all__ = [
    "ConfigValidationError",
]
