"""Intent error and exception classes.

Custom exceptions raised during intent validation, chain resolution,
and protocol parameter checking.
"""

from collections.abc import Sequence
from typing import Any


class InvalidChainError(ValueError):
    """Raised when an intent specifies a chain not configured for the strategy.

    Attributes:
        chain: The invalid chain that was specified
        configured_chains: The list of chains configured for the strategy
    """

    def __init__(self, chain: str, configured_chains: Sequence[str]) -> None:
        self.chain = chain
        self.configured_chains = list(configured_chains)
        chains_str = ", ".join(sorted(self.configured_chains)) if self.configured_chains else "(none)"
        super().__init__(f"Chain '{chain}' is not configured for this strategy. Configured chains: {chains_str}")


class InvalidSequenceError(ValueError):
    """Raised when an intent sequence is invalid.

    Attributes:
        message: Description of the error
    """

    def __init__(self, message: str) -> None:
        super().__init__(message)


class InvalidAmountError(ValueError):
    """Raised when amount='all' is used incorrectly.

    The 'all' amount is only valid when chaining outputs from a previous step.
    Using amount='all' on the first step of a sequence or on a standalone intent
    is invalid because there is no previous step output to reference.

    Attributes:
        intent_type: Type of intent with invalid amount
        reason: Explanation of why the amount is invalid
    """

    def __init__(self, intent_type: str, reason: str) -> None:
        self.intent_type = intent_type
        self.reason = reason
        super().__init__(f"Invalid amount='all' for {intent_type}: {reason}")


class InvalidProtocolParameterError(ValueError):
    """Raised when a protocol-specific parameter is invalid or not supported.

    Protocol-specific parameters are validated against the protocol's capabilities.
    For example, Aave supports 'variable' interest rate mode, while
    other protocols may not support interest rate mode selection at all.

    Attributes:
        protocol: The protocol that doesn't support the parameter
        parameter: The parameter name that is invalid
        value: The value that was provided
        reason: Explanation of why the parameter is invalid
    """

    def __init__(self, protocol: str, parameter: str, value: Any, reason: str) -> None:
        self.protocol = protocol
        self.parameter = parameter
        self.value = value
        self.reason = reason
        super().__init__(f"Invalid protocol parameter for '{protocol}': {parameter}={value!r}. {reason}")


class ProtocolRequiredError(ValueError):
    """Raised when protocol parameter is required but not provided.

    When a chain has multiple protocols configured that support the same operation,
    the protocol parameter must be explicitly specified to avoid ambiguity.

    Attributes:
        operation: The operation being performed (e.g., "borrow", "supply")
        available_protocols: List of protocols that support this operation on the chain
    """

    def __init__(self, operation: str, available_protocols: list[str]) -> None:
        self.operation = operation
        self.available_protocols = available_protocols
        protocols_str = ", ".join(sorted(available_protocols))
        super().__init__(
            f"Protocol must be specified for '{operation}' operation. Available protocols: {protocols_str}"
        )
