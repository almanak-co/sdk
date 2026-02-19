"""Token resolution exceptions.

This module defines custom exceptions for token resolution operations,
providing clear error messages with actionable suggestions.

Key Exceptions:
    - TokenResolutionError: Base exception for all token resolution errors
    - TokenNotFoundError: Token not found in any registry
    - TokenResolutionTimeoutError: On-chain lookup timed out
    - InvalidTokenAddressError: Malformed or invalid token address
    - AmbiguousTokenError: Multiple tokens match the given identifier

Example:
    from almanak.framework.data.tokens.exceptions import (
        TokenResolutionError,
        TokenNotFoundError,
        InvalidTokenAddressError,
    )

    # Raise when token not found
    raise TokenNotFoundError(
        token="UNKNOWN",
        chain="arbitrum",
        reason="Token not in static registry or cache",
        suggestions=["Check spelling: did you mean 'UNI'?", "Try using the full address"],
    )

    # Raise when address is malformed
    raise InvalidTokenAddressError(
        token="0xinvalid",
        chain="ethereum",
        reason="Address failed checksum validation",
    )
"""


class TokenResolutionError(Exception):
    """Base exception for token resolution errors.

    This is the base class for all token resolution-related exceptions.
    It provides structured error information including the token identifier,
    chain, reason for failure, and actionable suggestions.

    Attributes:
        token: The token identifier that failed to resolve (symbol or address)
        chain: The chain where resolution was attempted
        reason: Explanation of why resolution failed
        suggestions: List of actionable suggestions to fix the issue

    Example:
        raise TokenResolutionError(
            token="USDC",
            chain="unknown_chain",
            reason="Chain 'unknown_chain' is not supported",
            suggestions=["Use a supported chain: ethereum, arbitrum, base, optimism"],
        )
    """

    def __init__(
        self,
        token: str,
        chain: str,
        reason: str,
        suggestions: list[str] | None = None,
    ) -> None:
        """Initialize the exception.

        Args:
            token: The token identifier that failed to resolve
            chain: The chain where resolution was attempted
            reason: Explanation of why resolution failed
            suggestions: List of actionable suggestions to fix the issue
        """
        self.token = token
        self.chain = chain
        self.reason = reason
        self.suggestions = suggestions or []

        # Build formatted message
        message = f"Cannot resolve token '{token}' on {chain}: {reason}"
        if self.suggestions:
            suggestions_str = "; ".join(self.suggestions)
            message += f". Suggestions: {suggestions_str}"

        super().__init__(message)

    def __repr__(self) -> str:
        """Return a detailed representation of the exception."""
        return (
            f"{self.__class__.__name__}("
            f"token={self.token!r}, "
            f"chain={self.chain!r}, "
            f"reason={self.reason!r}, "
            f"suggestions={self.suggestions!r})"
        )


class TokenNotFoundError(TokenResolutionError):
    """Raised when a token is not found in any registry.

    This exception is raised when:
    - Token symbol is not in the static registry
    - Token symbol is not in the cache
    - Token address (if provided) doesn't match any known token
    - Gateway on-chain lookup (if enabled) also fails

    Example:
        raise TokenNotFoundError(
            token="UNKNOWNTOKEN",
            chain="arbitrum",
            reason="Token not in static registry or cache",
            suggestions=[
                "Check spelling - did you mean 'UNI' or 'LINK'?",
                "If using an address, ensure it's a valid ERC20 contract",
                "Use register() to add custom tokens to the resolver",
            ],
        )
    """

    def __init__(
        self,
        token: str,
        chain: str,
        reason: str = "Token not found in any registry",
        suggestions: list[str] | None = None,
    ) -> None:
        """Initialize the exception.

        Args:
            token: The token identifier that was not found
            chain: The chain where the token was searched
            reason: Explanation of why the token wasn't found
            suggestions: List of actionable suggestions
        """
        default_suggestions = [
            "Check the token symbol spelling",
            "Use the full contract address if available",
            "Verify the token exists on this chain",
        ]
        all_suggestions = (suggestions or []) + [s for s in default_suggestions if s not in (suggestions or [])]
        super().__init__(token=token, chain=chain, reason=reason, suggestions=all_suggestions)


class TokenResolutionTimeoutError(TokenResolutionError):
    """Raised when an on-chain token lookup times out.

    This exception is raised when:
    - Gateway call to fetch on-chain ERC20 metadata exceeds timeout
    - RPC endpoint is slow or unresponsive
    - Network issues prevent completion

    Attributes:
        timeout_seconds: The timeout duration that was exceeded

    Example:
        raise TokenResolutionTimeoutError(
            token="0x<token_address>",
            chain="ethereum",
            timeout_seconds=10.0,
            reason="On-chain lookup timed out after 10.0 seconds",
        )
    """

    def __init__(
        self,
        token: str,
        chain: str,
        timeout_seconds: float,
        reason: str | None = None,
        suggestions: list[str] | None = None,
    ) -> None:
        """Initialize the exception.

        Args:
            token: The token address that timed out during lookup
            chain: The chain where the lookup was attempted
            timeout_seconds: The timeout duration that was exceeded
            reason: Explanation of the timeout (auto-generated if not provided)
            suggestions: List of actionable suggestions
        """
        self.timeout_seconds = timeout_seconds

        if reason is None:
            reason = f"On-chain lookup timed out after {timeout_seconds:.1f} seconds"

        default_suggestions = [
            "Check if the RPC endpoint is responsive",
            "Increase the timeout if the network is slow",
            "Try again later if the network is congested",
            "Verify the address is a valid ERC20 contract",
        ]
        all_suggestions = (suggestions or []) + [s for s in default_suggestions if s not in (suggestions or [])]
        super().__init__(token=token, chain=chain, reason=reason, suggestions=all_suggestions)


class InvalidTokenAddressError(TokenResolutionError):
    """Raised when a token address is malformed or invalid.

    This exception is raised when:
    - Address doesn't start with '0x'
    - Address is not 42 characters (0x + 40 hex chars)
    - Address contains invalid hex characters
    - Address fails EIP-55 checksum validation (if checksummed)

    Example:
        raise InvalidTokenAddressError(
            token="0xinvalid",
            chain="ethereum",
            reason="Address must be 42 characters (0x + 40 hex chars)",
            suggestions=["Example valid address: 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"],
        )
    """

    def __init__(
        self,
        token: str,
        chain: str,
        reason: str = "Invalid token address format",
        suggestions: list[str] | None = None,
    ) -> None:
        """Initialize the exception.

        Args:
            token: The invalid token address
            chain: The chain context
            reason: Explanation of why the address is invalid
            suggestions: List of actionable suggestions
        """
        default_suggestions = [
            "Address must start with '0x'",
            "Address must be 42 characters (0x + 40 hex digits)",
            "Use only valid hexadecimal characters (0-9, a-f, A-F)",
        ]
        all_suggestions = (suggestions or []) + [s for s in default_suggestions if s not in (suggestions or [])]
        super().__init__(token=token, chain=chain, reason=reason, suggestions=all_suggestions)


class AmbiguousTokenError(TokenResolutionError):
    """Raised when multiple tokens match the given identifier.

    This exception is raised when:
    - A symbol matches multiple tokens on the same chain (e.g., multiple USDC variants)
    - Bridged tokens create ambiguity (USDC vs USDC.e)
    - Multiple protocols have deployed tokens with the same symbol

    Attributes:
        matching_addresses: List of addresses that match the token identifier

    Example:
        raise AmbiguousTokenError(
            token="USDC",
            chain="arbitrum",
            reason="Multiple USDC variants found on Arbitrum",
            matching_addresses=[
                "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",  # Native USDC
                "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",  # USDC.e (bridged)
            ],
            suggestions=[
                "Use 'USDC' for native USDC: 0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                "Use 'USDC.e' for bridged USDC: 0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",
                "Or specify the full contract address",
            ],
        )
    """

    def __init__(
        self,
        token: str,
        chain: str,
        reason: str = "Multiple tokens match the identifier",
        matching_addresses: list[str] | None = None,
        suggestions: list[str] | None = None,
    ) -> None:
        """Initialize the exception.

        Args:
            token: The ambiguous token identifier
            chain: The chain where ambiguity occurred
            reason: Explanation of the ambiguity
            matching_addresses: List of addresses that match
            suggestions: List of actionable suggestions
        """
        self.matching_addresses = matching_addresses or []

        default_suggestions = [
            "Specify the full contract address to avoid ambiguity",
            "Use the specific variant symbol (e.g., 'USDC.e' for bridged)",
        ]
        all_suggestions = (suggestions or []) + [s for s in default_suggestions if s not in (suggestions or [])]
        super().__init__(token=token, chain=chain, reason=reason, suggestions=all_suggestions)


__all__ = [
    "TokenResolutionError",
    "TokenNotFoundError",
    "TokenResolutionTimeoutError",
    "InvalidTokenAddressError",
    "AmbiguousTokenError",
]
