"""Token and chain resolution for Kraken.

This module provides mapping between stack-v2 token/chain names
and Kraken's internal symbols and method names.

Key responsibilities:
- Convert stack-v2 token symbols to Kraken symbols (e.g., "ETH" -> "XETH")
- Convert Kraken symbols back to standard tokens
- Map chains to Kraken deposit/withdrawal method strings
- Handle special cases like USDC vs USDC.e
"""

from .exceptions import KrakenChainNotSupportedError


class KrakenTokenResolver:
    """Maps stack-v2 tokens to Kraken symbols.

    Kraken uses non-standard symbols for some assets:
    - ETH -> XETH (internal)
    - BTC -> XXBT (internal)
    - USD -> ZUSD (for some pairs)

    This resolver handles the mapping in both directions.

    Example:
        resolver = KrakenTokenResolver()

        # Convert to Kraken format
        kraken_sym = resolver.to_kraken_symbol("arbitrum", "ETH")  # "ETH"
        kraken_sym = resolver.to_kraken_symbol("arbitrum", "USDC.e")  # "USDC"

        # Convert from Kraken format
        standard = resolver.from_kraken_symbol("XETH")  # "ETH"
    """

    # Special mappings for tokens that differ between chains
    # (chain, token) -> kraken_symbol
    # Note: token keys are uppercase to match the lookup logic
    TOKEN_MAPPING: dict[tuple[str, str], str] = {
        # USDC.e on Arbitrum/Optimism maps to USDC on Kraken
        ("arbitrum", "USDC.E"): "USDC",
        ("optimism", "USDC.E"): "USDC",
        # Native USDC also maps to USDC
        ("arbitrum", "USDC"): "USDC",
        ("optimism", "USDC"): "USDC",
        ("ethereum", "USDC"): "USDC",
        ("base", "USDC"): "USDC",
        # ETH variants all map to ETH (Kraken uses XETH internally but accepts ETH)
        ("ethereum", "ETH"): "ETH",
        ("ethereum", "WETH"): "ETH",
        ("arbitrum", "ETH"): "ETH",
        ("arbitrum", "WETH"): "ETH",
        ("optimism", "ETH"): "ETH",
        ("optimism", "WETH"): "ETH",
        ("base", "ETH"): "ETH",
        ("base", "WETH"): "ETH",
        # BTC
        ("ethereum", "WBTC"): "XBT",
        ("arbitrum", "WBTC"): "XBT",
        # USDT
        ("ethereum", "USDT"): "USDT",
        ("arbitrum", "USDT"): "USDT",
        # DAI
        ("ethereum", "DAI"): "DAI",
        ("arbitrum", "DAI"): "DAI",
        # AAVE
        ("ethereum", "AAVE"): "AAVE",
        ("arbitrum", "AAVE"): "AAVE",
        # LINK
        ("ethereum", "LINK"): "LINK",
        ("arbitrum", "LINK"): "LINK",
    }

    # Kraken internal symbols -> standard symbols
    KRAKEN_TO_STANDARD: dict[str, str] = {
        "XETH": "ETH",
        "XXBT": "BTC",
        "XBT": "BTC",
        "ZUSD": "USD",
        "ZEUR": "EUR",
    }

    # Standard symbols -> Kraken display symbols
    STANDARD_TO_KRAKEN: dict[str, str] = {
        "BTC": "XBT",  # Kraken uses XBT for Bitcoin in trading
    }

    def to_kraken_symbol(self, chain: str, token: str) -> str:
        """Convert stack-v2 token to Kraken symbol.

        Args:
            chain: Blockchain name (e.g., "arbitrum")
            token: Token symbol (e.g., "USDC.e", "ETH")

        Returns:
            Kraken symbol for the token
        """
        key = (chain.lower(), token.upper())

        # Check specific chain+token mapping
        if key in self.TOKEN_MAPPING:
            return self.TOKEN_MAPPING[key]

        # Check standard -> Kraken mapping
        if token.upper() in self.STANDARD_TO_KRAKEN:
            return self.STANDARD_TO_KRAKEN[token.upper()]

        # Default: use the token as-is (uppercase)
        return token.upper()

    def from_kraken_symbol(self, symbol: str) -> str:
        """Convert Kraken symbol to standard token symbol.

        Args:
            symbol: Kraken symbol (e.g., "XETH", "XXBT")

        Returns:
            Standard token symbol
        """
        # Check Kraken internal -> standard mapping
        if symbol.upper() in self.KRAKEN_TO_STANDARD:
            return self.KRAKEN_TO_STANDARD[symbol.upper()]

        return symbol.upper()

    def get_trading_pair(
        self,
        base_token: str,
        quote_token: str,
        chain: str = "ethereum",
    ) -> str:
        """Get Kraken trading pair symbol.

        Args:
            base_token: Base asset symbol
            quote_token: Quote asset symbol
            chain: Chain for token resolution

        Returns:
            Trading pair (e.g., "ETHUSD")
        """
        base = self.to_kraken_symbol(chain, base_token)
        quote = self.to_kraken_symbol(chain, quote_token)
        return f"{base}{quote}"


class KrakenChainMapper:
    """Maps stack-v2 chains to Kraken deposit/withdrawal method names.

    Kraken uses specific method strings for deposits and withdrawals
    that include the chain name and sometimes the asset.

    Example:
        mapper = KrakenChainMapper()

        # Get deposit method
        method = mapper.get_deposit_method("arbitrum", "ETH")
        # Returns: "ETH - Arbitrum One (Unified)"

        # Get withdrawal method
        method = mapper.get_withdraw_method("arbitrum")
        # Returns: "Arbitrum One"
    """

    # Chain -> Kraken network name for deposits
    # Format is typically: "{ASSET} - {NETWORK} (Unified)"
    DEPOSIT_NETWORKS: dict[str, str] = {
        "arbitrum": "Arbitrum One (Unified)",
        "optimism": "Optimism (Unified)",
        "ethereum": "Ether (Hex)",  # Special case for ETH mainnet
        # "base": "Base (Unified)",  # TODO: Verify when Kraken adds support
        # "polygon": "Polygon (Unified)",
    }

    # Chain -> Kraken method name for withdrawals
    WITHDRAW_METHODS: dict[str, str] = {
        "arbitrum": "Arbitrum One",
        "optimism": "Optimism",
        "ethereum": "Ether",  # or "Ethereum" depending on asset
        # "base": "Base",
        # "polygon": "Polygon",
    }

    # Chain -> Kraken network string (for parsing responses)
    NETWORK_STRINGS: dict[str, str] = {
        "arbitrum": "Arbitrum One",
        "optimism": "Optimism",
        "ethereum": "Ethereum",
    }

    # Inverse mapping: Kraken network string -> chain
    NETWORK_TO_CHAIN: dict[str, str] = {
        "Arbitrum One": "arbitrum",
        "Arbitrum One (Unified)": "arbitrum",
        "Optimism": "optimism",
        "Optimism (Unified)": "optimism",
        "Ethereum": "ethereum",
        "Ether (Hex)": "ethereum",
        "Ether": "ethereum",
    }

    def get_deposit_method(self, chain: str, asset: str) -> str:
        """Get Kraken deposit method name for a chain and asset.

        Args:
            chain: Chain name (e.g., "arbitrum")
            asset: Asset symbol (e.g., "ETH", "USDC")

        Returns:
            Kraken deposit method string

        Raises:
            KrakenChainNotSupportedError: If chain is not supported
        """
        chain_lower = chain.lower()
        if chain_lower not in self.DEPOSIT_NETWORKS:
            raise KrakenChainNotSupportedError(chain, "deposit")

        network = self.DEPOSIT_NETWORKS[chain_lower]

        # Ethereum mainnet uses special format without asset prefix
        if chain_lower == "ethereum":
            return network

        # Other chains: "{ASSET} - {NETWORK}"
        return f"{asset.upper()} - {network}"

    def get_withdraw_method(self, chain: str) -> str:
        """Get Kraken withdrawal method name for a chain.

        Args:
            chain: Chain name (e.g., "arbitrum")

        Returns:
            Kraken withdrawal method string

        Raises:
            KrakenChainNotSupportedError: If chain is not supported
        """
        chain_lower = chain.lower()
        if chain_lower not in self.WITHDRAW_METHODS:
            raise KrakenChainNotSupportedError(chain, "withdrawal")

        return self.WITHDRAW_METHODS[chain_lower]

    def get_supported_chains(self) -> list[str]:
        """Get list of supported chains for deposits/withdrawals."""
        return list(self.WITHDRAW_METHODS.keys())

    def chain_from_network(self, network_string: str) -> str | None:
        """Parse Kraken network string to chain name.

        Used when parsing deposit/withdrawal status responses.

        Args:
            network_string: Kraken network string from API response

        Returns:
            Chain name or None if not recognized
        """
        # Direct match
        if network_string in self.NETWORK_TO_CHAIN:
            return self.NETWORK_TO_CHAIN[network_string]

        # Try partial matching (handles variations)
        network_lower = network_string.lower()
        for chain in self.NETWORK_STRINGS:
            if self.NETWORK_STRINGS[chain].lower() in network_lower:
                return chain

        return None

    def parse_deposit_method(
        self,
        method_string: str,
        expected_asset: str | None = None,
    ) -> str | None:
        """Parse deposit method string to extract chain.

        Args:
            method_string: Kraken deposit method (e.g., "ETH - Arbitrum One (Unified)")
            expected_asset: If provided, verify the asset matches

        Returns:
            Chain name or None if parsing fails
        """
        method = method_string.strip()

        # Handle special cases first
        if method == "Ether (Hex)":
            return "ethereum"

        # Parse "ASSET - NETWORK" format
        if " - " in method:
            asset_part, network_part = method.split(" - ", 1)

            # Verify asset if provided
            if expected_asset and asset_part.upper() != expected_asset.upper():
                return None

            return self.chain_from_network(network_part)

        # Try direct network matching
        return self.chain_from_network(method)


# Singleton instances for convenience
token_resolver = KrakenTokenResolver()
chain_mapper = KrakenChainMapper()


__all__ = [
    "KrakenTokenResolver",
    "KrakenChainMapper",
    "token_resolver",
    "chain_mapper",
]
