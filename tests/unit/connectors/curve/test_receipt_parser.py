"""Tests for CurveReceiptParser extraction methods.

VIB-441: Verifies that extract_swap_amounts() uses actual token decimals
from TokenResolver instead of hardcoding 18.

VIB-1502: Tests for LP enrichment methods (extract_position_id, extract_liquidity,
extract_lp_close_data).
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

from almanak.connectors.curve.receipt_parser import (
    EVENT_TOPICS,
    CurveEventType,
    CurveReceiptParser,
)


def _make_topic(hex_str: str) -> str:
    """Ensure topic is lowercase 0x-prefixed."""
    return hex_str.lower()


def _pad_hex(value: int, signed: bool = False) -> str:
    """Encode an integer as a 32-byte hex word (no 0x prefix)."""
    if signed and value < 0:
        value = (1 << 256) + value
    return f"{value:064x}"


def _build_swap_receipt(
    wallet: str = "0xaabbccddee1122334455667788990011aabbccdd",
    pool: str = "0xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7",
    token_in: str = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",  # USDC
    token_out: str = "0x6b175474e89094c44da98b954eedeac495271d0f",  # DAI
    sold_id: int = 1,
    bought_id: int = 0,
    tokens_sold: int = 100_000_000,  # 100 USDC (6 decimals)
    tokens_bought: int = 99_984_871_483_550_784_213,  # ~99.98 DAI (18 decimals)
) -> dict:
    """Build a synthetic Curve swap receipt with TokenExchange + Transfer events."""
    # TokenExchange event: buyer (indexed), sold_id, tokens_sold, bought_id, tokens_bought
    buyer_topic = "0x" + "00" * 12 + wallet[2:]
    exchange_data = (
        "0x"
        + _pad_hex(sold_id, signed=True)
        + _pad_hex(tokens_sold)
        + _pad_hex(bought_id, signed=True)
        + _pad_hex(tokens_bought)
    )

    # ERC-20 Transfer: from wallet to pool (token_in)
    transfer_topic = _make_topic(EVENT_TOPICS["Transfer"])
    wallet_topic = "0x" + "00" * 12 + wallet[2:]
    pool_topic = "0x" + "00" * 12 + pool[2:]
    transfer_in_data = "0x" + _pad_hex(tokens_sold)
    transfer_out_data = "0x" + _pad_hex(tokens_bought)

    return {
        "status": 1,
        "from": wallet,
        "transactionHash": "0x" + "ab" * 32,
        "blockNumber": 19_000_000,
        "gasUsed": 150_000,
        "logs": [
            # Transfer: wallet -> pool (token_in = USDC)
            {
                "address": token_in,
                "topics": [transfer_topic, wallet_topic, pool_topic],
                "data": transfer_in_data,
                "logIndex": 0,
            },
            # TokenExchange event from pool
            {
                "address": pool,
                "topics": [_make_topic(EVENT_TOPICS["TokenExchange"]), buyer_topic],
                "data": exchange_data,
                "logIndex": 1,
            },
            # Transfer: pool -> wallet (token_out = DAI)
            {
                "address": token_out,
                "topics": [transfer_topic, pool_topic, wallet_topic],
                "data": transfer_out_data,
                "logIndex": 2,
            },
        ],
    }


def _mock_resolver(decimals_map: dict[str, int]):
    """Create a mock token resolver that returns decimals from a map."""
    mock_resolver = MagicMock()

    def resolve(address, chain):
        addr = address.lower()
        if addr in decimals_map:
            token = MagicMock()
            token.decimals = decimals_map[addr]
            return token
        raise ValueError(f"Unknown token: {addr}")

    mock_resolver.resolve = resolve
    return mock_resolver


class TestExtractSwapAmountsDecimals:
    """Test that extract_swap_amounts uses actual token decimals."""

    USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    DAI = "0x6b175474e89094c44da98b954eedeac495271d0f"
    USDT = "0xdac17f958d2ee523a2206206994597c13d831ec7"
    WBTC = "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"

    def test_usdc_to_dai_correct_decimals(self):
        """USDC (6 dec) -> DAI (18 dec) should give effective_price ~1.0."""
        receipt = _build_swap_receipt(
            token_in=self.USDC,
            token_out=self.DAI,
            tokens_sold=100_000_000,  # 100 USDC
            tokens_bought=99_984_871_483_550_784_213,  # ~99.98 DAI
        )
        resolver = _mock_resolver(
            {
                self.USDC: 6,
                self.DAI: 18,
            }
        )

        parser = CurveReceiptParser(chain="ethereum")
        with patch("almanak.framework.data.tokens.get_token_resolver", return_value=resolver):
            result = parser.extract_swap_amounts(receipt)

        assert result is not None
        assert result.amount_in == 100_000_000
        assert result.amount_out == 99_984_871_483_550_784_213
        assert result.amount_in_decimal == Decimal("100")
        # ~99.98 DAI
        assert Decimal("99") < result.amount_out_decimal < Decimal("100")
        # effective_price should be ~1.0 for stablecoin pair
        assert Decimal("0.9") < result.effective_price < Decimal("1.1")
        assert result.token_in == self.USDC
        assert result.token_out == self.DAI

    def test_dai_to_usdt_correct_decimals(self):
        """DAI (18 dec) -> USDT (6 dec) should also give ~1.0."""
        receipt = _build_swap_receipt(
            token_in=self.DAI,
            token_out=self.USDT,
            sold_id=0,
            bought_id=2,
            tokens_sold=500_000_000_000_000_000_000,  # 500 DAI
            tokens_bought=499_750_000,  # 499.75 USDT
        )
        resolver = _mock_resolver(
            {
                self.DAI: 18,
                self.USDT: 6,
            }
        )

        parser = CurveReceiptParser(chain="ethereum")
        with patch("almanak.framework.data.tokens.get_token_resolver", return_value=resolver):
            result = parser.extract_swap_amounts(receipt)

        assert result is not None
        assert result.amount_in_decimal == Decimal("500")
        assert result.amount_out_decimal == Decimal("499.75")
        assert Decimal("0.9") < result.effective_price < Decimal("1.1")

    def test_wbtc_8_decimals(self):
        """WBTC (8 dec) -> DAI (18 dec) should handle 8-decimal token."""
        receipt = _build_swap_receipt(
            token_in=self.WBTC,
            token_out=self.DAI,
            sold_id=0,
            bought_id=1,
            tokens_sold=100_000_000,  # 1 WBTC (8 decimals)
            tokens_bought=60_000_000_000_000_000_000_000,  # 60000 DAI
        )
        resolver = _mock_resolver(
            {
                self.WBTC: 8,
                self.DAI: 18,
            }
        )

        parser = CurveReceiptParser(chain="ethereum")
        with patch("almanak.framework.data.tokens.get_token_resolver", return_value=resolver):
            result = parser.extract_swap_amounts(receipt)

        assert result is not None
        assert result.amount_in_decimal == Decimal("1")
        assert result.amount_out_decimal == Decimal("60000")
        assert result.effective_price == Decimal("60000")

    def test_returns_none_when_resolver_unavailable(self):
        """Should return None (not wrong data) when resolver fails."""
        receipt = _build_swap_receipt()

        parser = CurveReceiptParser(chain="ethereum")
        with patch(
            "almanak.framework.data.tokens.get_token_resolver",
            side_effect=Exception("No resolver"),
        ):
            result = parser.extract_swap_amounts(receipt)

        assert result is None

    def test_returns_none_for_empty_receipt(self):
        """Should return None for receipt with no swap events."""
        parser = CurveReceiptParser(chain="ethereum")
        result = parser.extract_swap_amounts({"status": 1, "logs": [], "from": "0x1234"})
        assert result is None

    def test_18_to_18_still_works(self):
        """Both 18-decimal tokens should still produce correct results."""
        receipt = _build_swap_receipt(
            token_in=self.DAI,
            token_out="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",  # WETH
            sold_id=0,
            bought_id=1,
            tokens_sold=2000_000_000_000_000_000_000,  # 2000 DAI
            tokens_bought=1_000_000_000_000_000_000,  # 1 WETH
        )
        weth = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
        resolver = _mock_resolver(
            {
                self.DAI: 18,
                weth: 18,
            }
        )

        parser = CurveReceiptParser(chain="ethereum")
        with patch("almanak.framework.data.tokens.get_token_resolver", return_value=resolver):
            result = parser.extract_swap_amounts(receipt)

        assert result is not None
        assert result.amount_in_decimal == Decimal("2000")
        assert result.amount_out_decimal == Decimal("1")
        assert result.effective_price == Decimal("0.0005")


# =============================================================================
# LP Enrichment Tests (VIB-1502)
# =============================================================================

POOL_3POOL = "0xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7"
LP_TOKEN_3CRV = "0x6c3f90f043a72fa612cbac8115ee7e52bde6e490"
WALLET = "0xaabbccddee1122334455667788990011aabbccdd"
ZERO_ADDR = "0x0000000000000000000000000000000000000000"


def _build_add_liquidity_receipt(
    pool: str = POOL_3POOL,
    lp_token: str = LP_TOKEN_3CRV,
    wallet: str = WALLET,
    token_amounts: list[int] | None = None,
    lp_minted: int = 99_000_000_000_000_000_000,  # ~99 LP tokens
) -> dict:
    """Build a synthetic Curve AddLiquidity receipt for 3pool."""
    if token_amounts is None:
        token_amounts = [50_000_000_000_000_000_000, 50_000_000, 0]  # 50 DAI, 50 USDC, 0 USDT

    # AddLiquidity3 event data: 3 amounts + 3 fees + invariant + token_supply
    add_liq_topic = _make_topic(EVENT_TOPICS["AddLiquidity3"])
    provider_topic = "0x" + "00" * 12 + wallet[2:]
    fees = [0, 0, 0]
    invariant = 100_000_000_000_000_000_000
    token_supply = 1_000_000_000_000_000_000_000

    data_parts = [_pad_hex(a) for a in token_amounts]
    data_parts += [_pad_hex(f) for f in fees]
    data_parts += [_pad_hex(invariant), _pad_hex(token_supply)]
    add_liq_data = "0x" + "".join(data_parts)

    # ERC-20 Transfer: mint LP tokens (from zero address to wallet)
    transfer_topic = _make_topic(EVENT_TOPICS["Transfer"])
    zero_topic = "0x" + "00" * 12 + ZERO_ADDR[2:]
    wallet_topic = "0x" + "00" * 12 + wallet[2:]
    mint_data = "0x" + _pad_hex(lp_minted)

    return {
        "status": 1,
        "from": wallet,
        "transactionHash": "0x" + "cc" * 32,
        "blockNumber": 19_000_000,
        "gasUsed": 300_000,
        "logs": [
            # AddLiquidity3 event from pool
            {
                "address": pool,
                "topics": [add_liq_topic, provider_topic],
                "data": add_liq_data,
                "logIndex": 0,
            },
            # Transfer: mint LP tokens (zero -> wallet)
            {
                "address": lp_token,
                "topics": [transfer_topic, zero_topic, wallet_topic],
                "data": mint_data,
                "logIndex": 1,
            },
        ],
    }


def _build_remove_liquidity_receipt(
    pool: str = POOL_3POOL,
    wallet: str = WALLET,
    token_amounts: list[int] | None = None,
) -> dict:
    """Build a synthetic Curve RemoveLiquidity receipt for 3pool."""
    if token_amounts is None:
        token_amounts = [33_000_000_000_000_000_000, 33_000_000, 33_000_000]

    # RemoveLiquidity3 event data: 3 amounts + 3 fees + token_supply
    remove_liq_topic = _make_topic(EVENT_TOPICS["RemoveLiquidity3"])
    provider_topic = "0x" + "00" * 12 + wallet[2:]
    fees = [0, 0, 0]
    token_supply = 900_000_000_000_000_000_000

    data_parts = [_pad_hex(a) for a in token_amounts]
    data_parts += [_pad_hex(f) for f in fees]
    data_parts += [_pad_hex(token_supply)]
    remove_liq_data = "0x" + "".join(data_parts)

    return {
        "status": 1,
        "from": wallet,
        "transactionHash": "0x" + "dd" * 32,
        "blockNumber": 19_000_001,
        "gasUsed": 200_000,
        "logs": [
            {
                "address": pool,
                "topics": [remove_liq_topic, provider_topic],
                "data": remove_liq_data,
                "logIndex": 0,
            },
        ],
    }


def _build_cryptoswap_receipt(
    wallet: str = "0xaabbccddee1122334455667788990011aabbccdd",
    pool: str = "0xd51a44d3fae010294c616388b506acda1bfaae46",  # tricrypto2
    token_in: str = "0xdac17f958d2ee523a2206206994597c13d831ec7",  # USDT
    token_out: str = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",  # WETH
    sold_id: int = 0,
    bought_id: int = 2,
    tokens_sold: int = 500_000_000,  # 500 USDT (6 decimals)
    tokens_bought: int = 215_700_000_000_000_000,  # ~0.2157 WETH (18 decimals)
) -> dict:
    """Build a synthetic Curve CryptoSwap receipt with TokenExchangeCrypto + Transfer events.

    CryptoSwap uses uint256 indices and a different keccak256 topic than StableSwap.
    """
    buyer_topic = "0x" + "00" * 12 + wallet[2:]
    # CryptoSwap encodes indices as uint256 (same byte layout as int128 for small values)
    exchange_data = (
        "0x"
        + _pad_hex(sold_id)  # uint256 sold_id
        + _pad_hex(tokens_sold)  # uint256 tokens_sold
        + _pad_hex(bought_id)  # uint256 bought_id
        + _pad_hex(tokens_bought)  # uint256 tokens_bought
    )

    transfer_topic = _make_topic(EVENT_TOPICS["Transfer"])
    wallet_topic = "0x" + "00" * 12 + wallet[2:]
    pool_topic = "0x" + "00" * 12 + pool[2:]

    return {
        "status": 1,
        "from": wallet,
        "transactionHash": "0x" + "cd" * 32,
        "blockNumber": 19_500_000,
        "gasUsed": 320_000,
        "logs": [
            # Transfer: wallet -> pool (token_in = USDT)
            {
                "address": token_in,
                "topics": [transfer_topic, wallet_topic, pool_topic],
                "data": "0x" + _pad_hex(tokens_sold),
                "logIndex": 0,
            },
            # TokenExchange event from CryptoSwap pool (different topic than StableSwap)
            {
                "address": pool,
                "topics": [_make_topic(EVENT_TOPICS["TokenExchangeCrypto"]), buyer_topic],
                "data": exchange_data,
                "logIndex": 1,
            },
            # Transfer: pool -> wallet (token_out = WETH)
            {
                "address": token_out,
                "topics": [transfer_topic, pool_topic, wallet_topic],
                "data": "0x" + _pad_hex(tokens_bought),
                "logIndex": 2,
            },
        ],
    }


class TestExtractPositionId:
    """Test extract_position_id returns LP token address for Curve LP.

    Curve uses pool-based LP (no NFT tokenId). The position_id is the LP token
    contract address — a stable identifier for the position. The minted LP token
    *amount* is available separately via extract_liquidity().
    """

    def test_returns_lp_token_address_for_add_liquidity(self):
        """AddLiquidity receipt should return LP token address (not amount)."""
        lp_minted = 99_000_000_000_000_000_000  # 99 LP tokens (18 decimals)
        receipt = _build_add_liquidity_receipt(lp_minted=lp_minted)
        parser = CurveReceiptParser(chain="ethereum")
        position_id = parser.extract_position_id(receipt)
        # Should return the LP token address, not the minted amount
        assert position_id == LP_TOKEN_3CRV

    def test_returns_none_for_swap_receipt(self):
        """Swap receipt (no AddLiquidity) should return None."""
        receipt = _build_swap_receipt()
        parser = CurveReceiptParser(chain="ethereum")
        position_id = parser.extract_position_id(receipt)
        assert position_id is None

    def test_returns_none_for_empty_receipt(self):
        """Empty receipt should return None."""
        parser = CurveReceiptParser(chain="ethereum")
        position_id = parser.extract_position_id({"status": 1, "logs": []})
        assert position_id is None

    def test_position_id_is_address_not_amount(self):
        """Verify position_id looks like an Ethereum address, not a decimal number."""
        receipt = _build_add_liquidity_receipt(lp_minted=96_167_061_043_518_866_468)
        parser = CurveReceiptParser(chain="ethereum")
        position_id = parser.extract_position_id(receipt)
        assert position_id is not None
        assert position_id.startswith("0x")
        assert len(position_id) == 42


class TestExtractLiquidity:
    """Test extract_liquidity returns human-readable LP token amount.

    VIB-1753: extract_liquidity must return human-readable Decimal (e.g., 99.0)
    NOT raw wei (e.g., 99000000000000000000). The LP_CLOSE compiler expects
    human-readable amounts and converts to wei internally.
    """

    def test_returns_human_readable_decimal(self):
        """Should extract LP tokens and convert from wei to human-readable Decimal."""
        lp_amount_wei = 99_000_000_000_000_000_000  # 99 LP tokens in wei (18 decimals)
        receipt = _build_add_liquidity_receipt(lp_minted=lp_amount_wei)
        parser = CurveReceiptParser(chain="ethereum")
        result = parser.extract_liquidity(receipt)
        assert isinstance(result, Decimal)
        assert result == Decimal(99)

    def test_fractional_amount(self):
        """Should preserve fractional LP token amounts."""
        # 98.133240027002648655 LP tokens
        lp_amount_wei = 98_133_240_027_002_648_655
        receipt = _build_add_liquidity_receipt(lp_minted=lp_amount_wei)
        parser = CurveReceiptParser(chain="ethereum")
        result = parser.extract_liquidity(receipt)
        assert isinstance(result, Decimal)
        expected = Decimal(lp_amount_wei) / Decimal(10**18)
        assert result == expected

    def test_returns_none_for_no_mint(self):
        """Receipt without mint Transfer should return None."""
        receipt = _build_swap_receipt()
        parser = CurveReceiptParser(chain="ethereum")
        result = parser.extract_liquidity(receipt)
        assert result is None


class TestExtractLPCloseData:
    """Test extract_lp_close_data for RemoveLiquidity events."""

    def test_returns_token_amounts_from_remove_liquidity(self):
        """Should extract amounts from RemoveLiquidity event."""
        amounts = [33_000_000_000_000_000_000, 33_000_000, 33_000_000]
        receipt = _build_remove_liquidity_receipt(token_amounts=amounts)
        parser = CurveReceiptParser(chain="ethereum")
        result = parser.extract_lp_close_data(receipt)
        assert result is not None
        assert result.amount0_collected == amounts[0]
        assert result.amount1_collected == amounts[1]

    def test_stamps_canonical_pool_address(self):
        """VIB-4968: pool_address must be the canonical 0x pool contract.

        The RemoveLiquidity event emitter IS the Curve pool contract. Without
        this the LP accounting handler could not resolve a pool address and
        dropped the LP_CLOSE event entirely.
        """
        receipt = _build_remove_liquidity_receipt(pool=POOL_3POOL)
        parser = CurveReceiptParser(chain="ethereum")
        result = parser.extract_lp_close_data(receipt)
        assert result is not None
        assert result.pool_address == POOL_3POOL

    def test_3coin_pool_includes_all_amounts(self):
        """3-coin pool should include amount2 in additional_amounts."""
        amounts = [33_000_000_000_000_000_000, 33_000_000, 33_000_000]
        receipt = _build_remove_liquidity_receipt(token_amounts=amounts)
        parser = CurveReceiptParser(chain="ethereum")
        result = parser.extract_lp_close_data(receipt)
        assert result is not None
        assert result.additional_amounts == {2: 33_000_000}
        assert result.all_amounts == amounts

    def test_4coin_pool_includes_all_amounts(self):
        """4-coin NG pool should include amount2 and amount3."""
        # Simulate Base 4pool: USDC/USDbC/axlUSDC/crvUSD
        pool = "0xf6c5f01c7f3148891ad0e19df78743d31e390d1f"
        amounts = [
            50_000_000,  # 50 USDC (6 dec)
            50_000_000,  # 50 USDbC (6 dec)
            50_000_000,  # 50 axlUSDC (6 dec)
            91_000_000_000_000_000_000,  # 91 crvUSD (18 dec)
        ]
        receipt = _build_remove_liquidity_4coin_receipt(
            pool=pool,
            token_amounts=amounts,
        )
        parser = CurveReceiptParser(chain="base")
        result = parser.extract_lp_close_data(receipt)
        assert result is not None
        assert result.amount0_collected == amounts[0]
        assert result.amount1_collected == amounts[1]
        assert result.additional_amounts == {2: amounts[2], 3: amounts[3]}
        assert result.all_amounts == amounts
        assert len(result.all_fees) == 4

    def test_returns_none_for_swap_receipt(self):
        """Swap receipt should return None."""
        receipt = _build_swap_receipt()
        parser = CurveReceiptParser(chain="ethereum")
        result = parser.extract_lp_close_data(receipt)
        assert result is None


class TestExtractLPOpenData:
    """Test extract_lp_open_data for AddLiquidity events (VIB-4968)."""

    def test_stamps_canonical_pool_address(self):
        """pool_address must be the canonical 0x pool contract (event emitter).

        This is the chain-data identity the LP accounting handler needs to
        book an LP_OPEN event; without it the event was dropped entirely.
        """
        receipt = _build_add_liquidity_receipt(pool=POOL_3POOL)
        parser = CurveReceiptParser(chain="ethereum")
        result = parser.extract_lp_open_data(receipt)
        assert result is not None
        assert result.pool_address == POOL_3POOL

    def test_carries_measured_amounts(self):
        """amount0 / amount1 carry the raw measured token_amounts."""
        amounts = [50_000_000_000_000_000_000, 50_000_000, 0]
        receipt = _build_add_liquidity_receipt(token_amounts=amounts)
        parser = CurveReceiptParser(chain="ethereum")
        result = parser.extract_lp_open_data(receipt)
        assert result is not None
        assert result.amount0 == amounts[0]
        assert result.amount1 == amounts[1]

    def test_fungible_lp_null_contract(self):
        """Curve is fungible LP: no NFT id, no tick bracket, no V4 hash.

        ``position_id == 0`` is the canonical "no per-position discriminator"
        sentinel the accounting handler collapses to ``position_id=None``.
        """
        receipt = _build_add_liquidity_receipt()
        parser = CurveReceiptParser(chain="ethereum")
        result = parser.extract_lp_open_data(receipt)
        assert result is not None
        assert result.position_id == 0
        assert result.tick_lower is None
        assert result.tick_upper is None
        assert result.liquidity is None
        assert result.current_tick is None
        assert result.position_hash is None

    def test_returns_none_for_swap_receipt(self):
        """Swap receipt (no AddLiquidity) should return None."""
        receipt = _build_swap_receipt()
        parser = CurveReceiptParser(chain="ethereum")
        result = parser.extract_lp_open_data(receipt)
        assert result is None


def _build_add_liquidity_4coin_receipt(
    pool: str = "0xf6c5f01c7f3148891ad0e19df78743d31e390d1f",
    wallet: str = WALLET,
    token_amounts: list[int] | None = None,
    lp_minted: int = 98_133_240_027_002_648_655,
) -> dict:
    """Build a synthetic Curve AddLiquidity4 receipt for a 4-coin NG pool.

    NG pools: LP token address = pool address.
    """
    if token_amounts is None:
        token_amounts = [50_000_000, 50_000_000, 50_000_000, 91_000_000_000_000_000_000]

    # AddLiquidity4 event data: 4 amounts + 4 fees + invariant + token_supply
    add_liq_topic = _make_topic(EVENT_TOPICS["AddLiquidity4"])
    provider_topic = "0x" + "00" * 12 + wallet[2:]
    fees = [0, 0, 0, 0]
    invariant = 241_000_000_000_000_000_000
    token_supply = 1_000_000_000_000_000_000_000

    data_parts = [_pad_hex(a) for a in token_amounts]
    data_parts += [_pad_hex(f) for f in fees]
    data_parts += [_pad_hex(invariant), _pad_hex(token_supply)]
    add_liq_data = "0x" + "".join(data_parts)

    # ERC-20 Transfer: mint LP tokens (from zero address to wallet)
    # NG pool: LP token address IS the pool address
    transfer_topic = _make_topic(EVENT_TOPICS["Transfer"])
    zero_topic = "0x" + "00" * 12 + ZERO_ADDR[2:]
    wallet_topic = "0x" + "00" * 12 + wallet[2:]
    mint_data = "0x" + _pad_hex(lp_minted)

    return {
        "status": 1,
        "from": wallet,
        "transactionHash": "0x" + "ee" * 32,
        "blockNumber": 25_000_000,
        "gasUsed": 450_000,
        "logs": [
            {
                "address": pool,
                "topics": [add_liq_topic, provider_topic],
                "data": add_liq_data,
                "logIndex": 0,
            },
            # NG pool: LP token = pool address
            {
                "address": pool,
                "topics": [transfer_topic, zero_topic, wallet_topic],
                "data": mint_data,
                "logIndex": 1,
            },
        ],
    }


def _build_remove_liquidity_4coin_receipt(
    pool: str = "0xf6c5f01c7f3148891ad0e19df78743d31e390d1f",
    wallet: str = WALLET,
    token_amounts: list[int] | None = None,
) -> dict:
    """Build a synthetic Curve RemoveLiquidity4 receipt for a 4-coin NG pool."""
    if token_amounts is None:
        token_amounts = [50_000_000, 50_000_000, 50_000_000, 91_000_000_000_000_000_000]

    # RemoveLiquidity4 event data: 4 amounts + 4 fees + token_supply
    remove_liq_topic = _make_topic(EVENT_TOPICS["RemoveLiquidity4"])
    provider_topic = "0x" + "00" * 12 + wallet[2:]
    fees = [100_000, 200_000, 50_000, 1_000_000_000_000_000]
    token_supply = 900_000_000_000_000_000_000

    data_parts = [_pad_hex(a) for a in token_amounts]
    data_parts += [_pad_hex(f) for f in fees]
    data_parts += [_pad_hex(token_supply)]
    remove_liq_data = "0x" + "".join(data_parts)

    return {
        "status": 1,
        "from": wallet,
        "transactionHash": "0x" + "ff" * 32,
        "blockNumber": 25_000_001,
        "gasUsed": 350_000,
        "logs": [
            {
                "address": pool,
                "topics": [remove_liq_topic, provider_topic],
                "data": remove_liq_data,
                "logIndex": 0,
            },
        ],
    }


class TestNG4CoinPool:
    """Tests for Curve StableSwap NG 4-coin pool receipt parsing.

    Validates that extract_position_id(), extract_liquidity(), and
    extract_lp_close_data() work correctly for NG pools where
    LP token address == pool address and n_coins > 2.
    """

    POOL_4POOL = "0xf6c5f01c7f3148891ad0e19df78743d31e390d1f"

    def test_extract_position_id_ng_pool(self):
        """NG 4-coin pool should return LP token address (pool address for NG)."""
        lp_minted = 98_133_240_027_002_648_655
        receipt = _build_add_liquidity_4coin_receipt(lp_minted=lp_minted)
        parser = CurveReceiptParser(chain="base")
        position_id = parser.extract_position_id(receipt)
        # NG pool: LP token address IS the pool address
        assert position_id == self.POOL_4POOL

    def test_extract_liquidity_ng_pool(self):
        """NG 4-coin pool should return human-readable LP amount."""
        lp_minted = 98_133_240_027_002_648_655
        receipt = _build_add_liquidity_4coin_receipt(lp_minted=lp_minted)
        resolver = _mock_resolver({self.POOL_4POOL: 18})
        parser = CurveReceiptParser(chain="base")
        with patch("almanak.framework.data.tokens.get_token_resolver", return_value=resolver):
            result = parser.extract_liquidity(receipt)
        assert isinstance(result, Decimal)
        expected = Decimal(lp_minted) / Decimal(10**18)
        assert result == expected

    def test_extract_liquidity_ng_pool_resolver_fallback(self):
        """When resolver can't resolve NG LP token, should fall back to 18 decimals."""
        lp_minted = 98_133_240_027_002_648_655
        receipt = _build_add_liquidity_4coin_receipt(lp_minted=lp_minted)
        parser = CurveReceiptParser(chain="base")
        # No resolver mock — will fail to resolve, should fall back to 18
        with patch(
            "almanak.framework.data.tokens.get_token_resolver",
            side_effect=Exception("No resolver"),
        ):
            result = parser.extract_liquidity(receipt)
        assert isinstance(result, Decimal)
        expected = Decimal(lp_minted) / Decimal(10**18)
        assert result == expected

    def test_add_liquidity_4coin_event_parsing(self):
        """AddLiquidity4 event should decode all 4 token amounts."""
        amounts = [50_000_000, 50_000_000, 50_000_000, 91_000_000_000_000_000_000]
        receipt = _build_add_liquidity_4coin_receipt(token_amounts=amounts)
        parser = CurveReceiptParser(chain="base")
        result = parser.parse_receipt(receipt)
        assert result.success
        add_events = [e for e in result.events if e.event_type == CurveEventType.ADD_LIQUIDITY]
        assert len(add_events) == 1
        assert add_events[0].data["token_amounts"] == amounts


# =============================================================================
# StableSwap NG dynamic-array event decoding (VIB-4836)
# =============================================================================


def _build_add_liquidity_dyn_data(amounts: list[int], fees: list[int], invariant: int, token_supply: int) -> str:
    """Encode `AddLiquidity(address,uint256[],uint256[],uint256,uint256)` data.

    Layout (with `provider` indexed):
        head (4 × 32B):  offset_to_amounts, offset_to_fees, invariant, token_supply
        tail:            amounts_length, *amounts, fees_length, *fees

    The offsets are byte counts from the *start of `data`*.
    """
    n = len(amounts)
    assert len(fees) == n
    head_len = 4 * 32  # 0x80
    amounts_tail_len = 32 + n * 32  # length word + elements
    offset_to_amounts = head_len  # = 0x80
    offset_to_fees = head_len + amounts_tail_len

    head = _pad_hex(offset_to_amounts) + _pad_hex(offset_to_fees) + _pad_hex(invariant) + _pad_hex(token_supply)
    amounts_tail = _pad_hex(n) + "".join(_pad_hex(a) for a in amounts)
    fees_tail = _pad_hex(n) + "".join(_pad_hex(f) for f in fees)
    return "0x" + head + amounts_tail + fees_tail


def _build_remove_liquidity_dyn_data(amounts: list[int], fees: list[int], token_supply: int) -> str:
    """Encode `RemoveLiquidity(address,uint256[],uint256[],uint256)` data.

    Layout (with `provider` indexed):
        head (3 × 32B):  offset_to_amounts, offset_to_fees, token_supply
        tail:            amounts_length, *amounts, fees_length, *fees
    """
    n = len(amounts)
    assert len(fees) == n
    head_len = 3 * 32  # 0x60
    amounts_tail_len = 32 + n * 32
    offset_to_amounts = head_len
    offset_to_fees = head_len + amounts_tail_len

    head = _pad_hex(offset_to_amounts) + _pad_hex(offset_to_fees) + _pad_hex(token_supply)
    amounts_tail = _pad_hex(n) + "".join(_pad_hex(a) for a in amounts)
    fees_tail = _pad_hex(n) + "".join(_pad_hex(f) for f in fees)
    return "0x" + head + amounts_tail + fees_tail


def _build_add_liquidity_dyn_receipt(
    pool: str = "0x03771e24b7c9172d163bf447490b142a15be3485",  # OPT crvUSD/USDC
    wallet: str = WALLET,
    token_amounts: list[int] | None = None,
    fees: list[int] | None = None,
    invariant: int = 200_000_000_000_000_000_000,
    token_supply: int = 1_000_000_000_000_000_000_000,
    lp_minted: int = 19_525_895_236_381_251_599,
) -> dict:
    """Build an AddLiquidityDyn receipt for an NG pool (e.g. OPT crvUSD/USDC)."""
    if token_amounts is None:
        token_amounts = [10 * 10**18, 10 * 10**6]
    if fees is None:
        fees = [0] * len(token_amounts)

    add_liq_topic = _make_topic(EVENT_TOPICS["AddLiquidityDyn"])
    provider_topic = "0x" + "00" * 12 + wallet[2:]
    data = _build_add_liquidity_dyn_data(token_amounts, fees, invariant, token_supply)

    transfer_topic = _make_topic(EVENT_TOPICS["Transfer"])
    zero_topic = "0x" + "00" * 12 + ZERO_ADDR[2:]
    wallet_topic = "0x" + "00" * 12 + wallet[2:]
    mint_data = "0x" + _pad_hex(lp_minted)

    return {
        "status": 1,
        "from": wallet,
        "transactionHash": "0x" + "1a" * 32,
        "blockNumber": 25_000_100,
        "gasUsed": 250_000,
        "logs": [
            {
                "address": pool,
                "topics": [add_liq_topic, provider_topic],
                "data": data,
                "logIndex": 0,
            },
            # NG pool: LP token address IS the pool address.
            {
                "address": pool,
                "topics": [transfer_topic, zero_topic, wallet_topic],
                "data": mint_data,
                "logIndex": 1,
            },
        ],
    }


def _build_remove_liquidity_dyn_receipt(
    pool: str = "0x03771e24b7c9172d163bf447490b142a15be3485",
    wallet: str = WALLET,
    token_amounts: list[int] | None = None,
    fees: list[int] | None = None,
    token_supply: int = 900_000_000_000_000_000_000,
) -> dict:
    """Build a RemoveLiquidityDyn receipt for an NG pool."""
    if token_amounts is None:
        token_amounts = [9 * 10**18, 11 * 10**6]
    if fees is None:
        fees = [0] * len(token_amounts)

    remove_liq_topic = _make_topic(EVENT_TOPICS["RemoveLiquidityDyn"])
    provider_topic = "0x" + "00" * 12 + wallet[2:]
    data = _build_remove_liquidity_dyn_data(token_amounts, fees, token_supply)

    return {
        "status": 1,
        "from": wallet,
        "transactionHash": "0x" + "1b" * 32,
        "blockNumber": 25_000_101,
        "gasUsed": 180_000,
        "logs": [
            {
                "address": pool,
                "topics": [remove_liq_topic, provider_topic],
                "data": data,
                "logIndex": 0,
            },
        ],
    }


class TestNGDynamicArrayDecoding:
    """Tests for AddLiquidityDyn / RemoveLiquidityDyn parsing (VIB-4836).

    StableSwap NG pools (e.g. Optimism crvUSD/USDC at 0x03771e24…) emit
    AddLiquidity / RemoveLiquidity with **dynamic** uint256[] arrays for both
    amounts and fees, instead of the fixed-size variants. The decoder must
    follow offset pointers in the ABI head section.
    """

    OPT_CRVUSD_USDC = "0x03771e24b7c9172d163bf447490b142a15be3485"

    def test_add_liquidity_dyn_event_recognised(self):
        """AddLiquidityDyn topic resolves to ADD_LIQUIDITY event type."""
        receipt = _build_add_liquidity_dyn_receipt()
        parser = CurveReceiptParser(chain="optimism")
        result = parser.parse_receipt(receipt)
        assert result.success
        add_events = [e for e in result.events if e.event_type == CurveEventType.ADD_LIQUIDITY]
        assert len(add_events) == 1
        assert add_events[0].event_name == "AddLiquidityDyn"

    def test_add_liquidity_dyn_amounts_decoded(self):
        """Dynamic-array decoder follows the offset pointer and reads correct amounts."""
        amounts = [10 * 10**18, 10 * 10**6]
        fees = [123, 456]
        invariant = 200_000_000_000_000_000_000
        token_supply = 1_000_000_000_000_000_000_000
        receipt = _build_add_liquidity_dyn_receipt(
            token_amounts=amounts,
            fees=fees,
            invariant=invariant,
            token_supply=token_supply,
        )
        parser = CurveReceiptParser(chain="optimism")
        result = parser.parse_receipt(receipt)
        add = next(e for e in result.events if e.event_type == CurveEventType.ADD_LIQUIDITY)
        assert add.data["token_amounts"] == amounts
        assert add.data["fees"] == fees
        assert add.data["invariant"] == invariant
        assert add.data["token_supply"] == token_supply
        assert add.data["provider"].lower() == WALLET.lower()

    def test_add_liquidity_dyn_3coin_pool(self):
        """Decoder handles n_coins=3 (different offset to fees)."""
        amounts = [1 * 10**18, 2 * 10**18, 3 * 10**18]
        fees = [10, 20, 30]
        receipt = _build_add_liquidity_dyn_receipt(token_amounts=amounts, fees=fees)
        parser = CurveReceiptParser(chain="optimism")
        result = parser.parse_receipt(receipt)
        add = next(e for e in result.events if e.event_type == CurveEventType.ADD_LIQUIDITY)
        assert add.data["token_amounts"] == amounts
        assert add.data["fees"] == fees

    def test_extract_position_id_dyn_returns_pool_address(self):
        """NG dyn pools: LP token = pool, so position_id is the pool address."""
        receipt = _build_add_liquidity_dyn_receipt()
        parser = CurveReceiptParser(chain="optimism")
        position_id = parser.extract_position_id(receipt)
        assert position_id == self.OPT_CRVUSD_USDC

    def test_extract_liquidity_dyn_returns_lp_minted(self):
        """extract_liquidity reads the mint Transfer (zero -> wallet) for NG dyn pools."""
        lp_minted = 19_525_895_236_381_251_599
        receipt = _build_add_liquidity_dyn_receipt(lp_minted=lp_minted)
        resolver = _mock_resolver({self.OPT_CRVUSD_USDC: 18})
        parser = CurveReceiptParser(chain="optimism")
        with patch("almanak.framework.data.tokens.get_token_resolver", return_value=resolver):
            result = parser.extract_liquidity(receipt)
        assert isinstance(result, Decimal)
        assert result == Decimal(lp_minted) / Decimal(10**18)

    def test_remove_liquidity_dyn_event_recognised(self):
        """RemoveLiquidityDyn topic resolves to REMOVE_LIQUIDITY event type."""
        receipt = _build_remove_liquidity_dyn_receipt()
        parser = CurveReceiptParser(chain="optimism")
        result = parser.parse_receipt(receipt)
        assert result.success
        rm_events = [e for e in result.events if e.event_type == CurveEventType.REMOVE_LIQUIDITY]
        assert len(rm_events) == 1
        assert rm_events[0].event_name == "RemoveLiquidityDyn"

    def test_remove_liquidity_dyn_amounts_decoded(self):
        """Dynamic-array decoder for RemoveLiquidity reads amounts + fees + supply."""
        amounts = [9 * 10**18, 11 * 10**6]
        fees = [7, 8]
        supply = 900_000_000_000_000_000_000
        receipt = _build_remove_liquidity_dyn_receipt(token_amounts=amounts, fees=fees, token_supply=supply)
        parser = CurveReceiptParser(chain="optimism")
        result = parser.parse_receipt(receipt)
        rm = next(e for e in result.events if e.event_type == CurveEventType.REMOVE_LIQUIDITY)
        assert rm.data["token_amounts"] == amounts
        assert rm.data["fees"] == fees
        assert rm.data["token_supply"] == supply
        # invariant is not part of RemoveLiquidity (only AddLiquidity carries it)
        assert "invariant" not in rm.data

    def test_remove_liquidity_dyn_3coin_pool(self):
        """RemoveLiquidity decoder handles n_coins=3."""
        amounts = [1 * 10**6, 2 * 10**6, 3 * 10**18]
        fees = [100, 200, 300]
        receipt = _build_remove_liquidity_dyn_receipt(token_amounts=amounts, fees=fees)
        parser = CurveReceiptParser(chain="optimism")
        result = parser.parse_receipt(receipt)
        rm = next(e for e in result.events if e.event_type == CurveEventType.REMOVE_LIQUIDITY)
        assert rm.data["token_amounts"] == amounts
        assert rm.data["fees"] == fees

    def test_extract_lp_close_data_dyn_2coin(self):
        """extract_lp_close_data reads amounts from a RemoveLiquidityDyn receipt."""
        amounts = [9 * 10**18, 11 * 10**6]
        receipt = _build_remove_liquidity_dyn_receipt(token_amounts=amounts)
        parser = CurveReceiptParser(chain="optimism")
        close_data = parser.extract_lp_close_data(receipt)
        assert close_data is not None
        assert close_data.amount0_collected == amounts[0]
        assert close_data.amount1_collected == amounts[1]


class TestLPCloseDataModel:
    """Test LPCloseData model's all_amounts/all_fees properties."""

    def test_all_amounts_2coin(self):
        """2-coin pool should return [amount0, amount1]."""
        from almanak.framework.execution.extracted_data import LPCloseData

        data = LPCloseData(amount0_collected=100, amount1_collected=200)
        assert data.all_amounts == [100, 200]
        # VIB-4470 — fees default flipped from int=0 to int|None=None
        # (Empty ≠ Zero). Unmeasured fees surface as None, not zero.
        assert data.all_fees == [None, None]

    def test_all_amounts_4coin(self):
        """4-coin pool should return all 4 amounts in order."""
        from almanak.framework.execution.extracted_data import LPCloseData

        data = LPCloseData(
            amount0_collected=100,
            amount1_collected=200,
            fees0=10,
            fees1=20,
            additional_amounts={2: 300, 3: 400},
            additional_fees={2: 30, 3: 40},
        )
        assert data.all_amounts == [100, 200, 300, 400]
        assert data.all_fees == [10, 20, 30, 40]

    def test_to_dict_includes_additional(self):
        """to_dict should include additional amounts when present."""
        from almanak.framework.execution.extracted_data import LPCloseData

        data = LPCloseData(
            amount0_collected=100,
            amount1_collected=200,
            additional_amounts={2: 300},
        )
        d = data.to_dict()
        assert "additional_amounts" in d
        assert d["additional_amounts"] == {"2": "300"}

    def test_to_dict_omits_additional_when_none(self):
        """to_dict should not include additional_amounts when None."""
        from almanak.framework.execution.extracted_data import LPCloseData

        data = LPCloseData(amount0_collected=100, amount1_collected=200)
        d = data.to_dict()
        assert "additional_amounts" not in d


class TestCryptoSwapReceiptParsing:
    """Test that CryptoSwap (TokenExchangeCrypto) receipts are parsed correctly.

    CryptoSwap pools use a different keccak256 topic than StableSwap pools.
    This was added in iter-95 to fix missing swap_amounts enrichment for tricrypto pools.
    """

    USDT = "0xdac17f958d2ee523a2206206994597c13d831ec7"
    WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    WBTC = "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"

    def test_cryptoswap_usdt_to_weth(self):
        """USDT (6 dec) -> WETH (18 dec) via CryptoSwap should produce correct swap_amounts."""
        receipt = _build_cryptoswap_receipt(
            token_in=self.USDT,
            token_out=self.WETH,
            tokens_sold=500_000_000,  # 500 USDT
            tokens_bought=215_700_000_000_000_000,  # ~0.2157 WETH
        )
        resolver = _mock_resolver(
            {
                self.USDT: 6,
                self.WETH: 18,
            }
        )

        parser = CurveReceiptParser(chain="ethereum")
        with patch("almanak.framework.data.tokens.get_token_resolver", return_value=resolver):
            result = parser.extract_swap_amounts(receipt)

        assert result is not None, "CryptoSwap receipt must be parsed (TokenExchangeCrypto topic)"
        assert result.amount_in == 500_000_000
        assert result.amount_out == 215_700_000_000_000_000
        assert result.amount_in_decimal == Decimal("500")
        assert Decimal("0.21") < result.amount_out_decimal < Decimal("0.22")
        # effective_price = amount_out / amount_in ~= 0.000431 WETH per USDT
        assert result.effective_price > Decimal("0")
        assert result.token_in == self.USDT
        assert result.token_out == self.WETH

    def test_cryptoswap_topic_distinct_from_stableswap(self):
        """Verify the two event topics are different (regression guard for the fix)."""
        assert EVENT_TOPICS["TokenExchange"] != EVENT_TOPICS["TokenExchangeCrypto"], (
            "StableSwap and CryptoSwap TokenExchange events have different keccak256 topics"
        )
        assert EVENT_TOPICS["TokenExchange"] == "0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140"
        assert (
            EVENT_TOPICS["TokenExchangeCrypto"] == "0xb2e76ae99761dc136e598d4a629bb347eccb9532a5f8bbd72e18467c3c34cc98"
        )

    def test_stableswap_receipt_still_works_after_cryptoswap_addition(self):
        """Adding TokenExchangeCrypto must not break StableSwap parsing."""
        receipt = _build_swap_receipt(
            token_in="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",  # USDC
            token_out="0x6b175474e89094c44da98b954eedeac495271d0f",  # DAI
            tokens_sold=100_000_000,
            tokens_bought=99_984_871_483_550_784_213,
        )
        usdc = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
        dai = "0x6b175474e89094c44da98b954eedeac495271d0f"
        resolver = _mock_resolver({usdc: 6, dai: 18})

        parser = CurveReceiptParser(chain="ethereum")
        with patch("almanak.framework.data.tokens.get_token_resolver", return_value=resolver):
            result = parser.extract_swap_amounts(receipt)

        assert result is not None
        assert result.amount_in_decimal == Decimal("100")
        assert Decimal("99") < result.amount_out_decimal < Decimal("100")


# =============================================================================
# Old-style 3-coin CryptoSwap RemoveLiquidity (Tricrypto2) — VIB-5491
# =============================================================================

_TRICRYPTO2 = "0xd51a44d3fae010294c616388b506acda1bfaae46"
_TRICRYPTO2_LP = "0xc4ad29ba4b3c580e6d59105fff484999997675ff"


def _build_remove_liquidity_v2crypto3_receipt(
    token_amounts: list[int],
    token_supply: int = 6_274_344_285_069_545_554_166,
    lp_burned: int = 1_000_000_000_000_000,
    wallet: str = WALLET,
) -> dict:
    """Tricrypto2 RemoveLiquidity receipt: provider(indexed) + amounts[3] + supply."""
    topic = _make_topic(EVENT_TOPICS["RemoveLiquidityV2Crypto3"])
    provider_topic = "0x" + "00" * 12 + wallet[2:]
    data = "0x" + "".join(_pad_hex(v) for v in (*token_amounts, token_supply))
    transfer_topic = _make_topic(EVENT_TOPICS["Transfer"])
    wallet_topic = "0x" + "00" * 12 + wallet[2:]
    zero_topic = "0x" + "00" * 12 + ZERO_ADDR[2:]
    return {
        "status": 1,
        "from": wallet,
        "transactionHash": "0x" + "d3" * 32,
        "blockNumber": 25_000_300,
        "gasUsed": 280_000,
        "logs": [
            # LP burn (Transfer to zero) then the RemoveLiquidity event.
            {
                "address": _TRICRYPTO2_LP,
                "topics": [transfer_topic, wallet_topic, zero_topic],
                "data": "0x" + _pad_hex(lp_burned),
                "logIndex": 0,
            },
            {"address": _TRICRYPTO2, "topics": [topic, provider_topic], "data": data, "logIndex": 1},
        ],
    }


class TestRemoveLiquidityV2Crypto3:
    """Tricrypto2 (old-style 3-coin CryptoSwap) RemoveLiquidity must decode (was a ghost)."""

    def test_event_recognised_as_remove_liquidity(self):
        receipt = _build_remove_liquidity_v2crypto3_receipt([147_735_751, 228_642, 84_520_182_346_515_188])
        result = CurveReceiptParser(chain="ethereum").parse_receipt(receipt)
        assert result.success
        removes = [e for e in result.events if e.event_type == CurveEventType.REMOVE_LIQUIDITY]
        assert len(removes) == 1
        assert removes[0].event_name == "RemoveLiquidityV2Crypto3"

    def test_amounts_and_supply_decoded(self):
        amounts = [147_735_751, 228_642, 84_520_182_346_515_188]  # USDT/WBTC/WETH (real on-chain)
        receipt = _build_remove_liquidity_v2crypto3_receipt(amounts)
        result = CurveReceiptParser(chain="ethereum").parse_receipt(receipt)
        rm = next(e for e in result.events if e.event_type == CurveEventType.REMOVE_LIQUIDITY)
        assert rm.data["token_amounts"] == amounts
        assert rm.data["token_supply"] == 6_274_344_285_069_545_554_166
        assert rm.data["fees"] == []
        assert rm.data["provider"].lower() == WALLET.lower()

    def test_extract_lp_close_data_carries_all_three_legs(self):
        """Accounting contract: the 3rd coin reaches LPCloseData.additional_amounts."""
        amounts = [147_735_751, 228_642, 84_520_182_346_515_188]  # USDT/WBTC/WETH
        receipt = _build_remove_liquidity_v2crypto3_receipt(amounts)
        close = CurveReceiptParser(chain="ethereum").extract_lp_close_data(receipt)
        assert close is not None
        assert close.amount0_collected == amounts[0]  # USDT
        assert close.amount1_collected == amounts[1]  # WBTC
        assert close.additional_amounts == {2: amounts[2]}  # WETH — 3rd leg, not dropped

    def test_truncated_payload_fails_closed(self):
        """A short (3-word) payload fails closed to raw_data, not a ghost LP_CLOSE
        with a fabricated zero token_supply (decode_uint256 returns 0 for a missing
        word)."""
        topic = _make_topic(EVENT_TOPICS["RemoveLiquidityV2Crypto3"])
        provider_topic = "0x" + "00" * 12 + WALLET[2:]
        short_data = "0x" + "".join(_pad_hex(v) for v in (147_735_751, 228_642, 84_520_182_346_515_188))
        receipt = {
            "status": 1,
            "from": WALLET,
            "transactionHash": "0x" + "d4" * 32,
            "logs": [{"address": _TRICRYPTO2, "topics": [topic, provider_topic], "data": short_data, "logIndex": 0}],
        }
        result = CurveReceiptParser(chain="ethereum").parse_receipt(receipt)
        rm = next(e for e in result.events if e.event_type == CurveEventType.REMOVE_LIQUIDITY)
        assert "raw_data" in rm.data
        assert "token_amounts" not in rm.data  # no fabricated amounts at the event level
        # Empty ≠ Zero: the extractor must not fabricate measured-zero proceeds —
        # the legs are None (unmeasured), never 0.
        close = CurveReceiptParser(chain="ethereum").extract_lp_close_data(receipt)
        assert close is None or (
            close.amount0_collected is None and close.amount1_collected is None and close.additional_amounts is None
        )


# =============================================================================
# Old-style 3-coin CryptoSwap AddLiquidity (Tricrypto2) — VIB-5441
# =============================================================================

TRICRYPTO2_POOL = "0xd51a44d3fae010294c616388b506acda1bfaae46"


def _build_add_liquidity_v2crypto3_receipt(
    token_amounts: list[int],
    fee: int = 232_489_209_237,
    token_supply: int = 6_282_725_994_016_461_367_439,
    lp_minted: int = 1_084_907_444_686_091,
    wallet: str = WALLET,
) -> dict:
    """Build a Tricrypto2 AddLiquidity receipt: amounts[3] + fee + supply (no fees array)."""
    add_topic = _make_topic(EVENT_TOPICS["AddLiquidityV2Crypto3"])
    provider_topic = "0x" + "00" * 12 + wallet[2:]
    data = "0x" + "".join(_pad_hex(v) for v in (*token_amounts, fee, token_supply))
    transfer_topic = _make_topic(EVENT_TOPICS["Transfer"])
    zero_topic = "0x" + "00" * 12 + ZERO_ADDR[2:]
    wallet_topic = "0x" + "00" * 12 + wallet[2:]
    return {
        "status": 1,
        "from": wallet,
        "transactionHash": "0x" + "c3" * 32,
        "blockNumber": 25_000_200,
        "gasUsed": 300_000,
        "logs": [
            {"address": TRICRYPTO2_POOL, "topics": [add_topic, provider_topic], "data": data, "logIndex": 0},
            {
                "address": "0xc4ad29ba4b3c580e6d59105fff484999997675ff",  # crv3crypto LP token
                "topics": [transfer_topic, zero_topic, wallet_topic],
                "data": "0x" + _pad_hex(lp_minted),
                "logIndex": 1,
            },
        ],
    }


class TestAddLiquidityV2Crypto3:
    """Tricrypto2 (old-style 3-coin CryptoSwap) AddLiquidity must decode (was a ghost)."""

    def test_event_recognised_as_add_liquidity(self):
        receipt = _build_add_liquidity_v2crypto3_receipt([10_122_800, 16_238, 6_004_489_832_657_005])
        result = CurveReceiptParser(chain="ethereum").parse_receipt(receipt)
        assert result.success
        adds = [e for e in result.events if e.event_type == CurveEventType.ADD_LIQUIDITY]
        assert len(adds) == 1
        assert adds[0].event_name == "AddLiquidityV2Crypto3"

    def test_amounts_and_supply_decoded(self):
        amounts = [10_122_800, 16_238, 6_004_489_832_657_005]  # USDT/WBTC/WETH (real on-chain)
        receipt = _build_add_liquidity_v2crypto3_receipt(amounts)
        result = CurveReceiptParser(chain="ethereum").parse_receipt(receipt)
        add = next(e for e in result.events if e.event_type == CurveEventType.ADD_LIQUIDITY)
        assert add.data["token_amounts"] == amounts
        assert add.data["token_supply"] == 6_282_725_994_016_461_367_439
        # Mirrors the 2-coin V2Crypto2 shape: pool-level scalar under invariant,
        # fees empty (no per-coin fees array for a consumer to misread).
        assert add.data["fees"] == []
        assert add.data["invariant"] == 232_489_209_237
        assert add.data["provider"].lower() == WALLET.lower()

    def test_truncated_payload_fails_closed(self):
        """A short (3-word) payload must fail closed to raw_data, not decode as a
        ghost LP_OPEN with fabricated zero invariant/supply (decode_uint256 returns 0
        for a missing word)."""
        add_topic = _make_topic(EVENT_TOPICS["AddLiquidityV2Crypto3"])
        provider_topic = "0x" + "00" * 12 + WALLET[2:]
        short_data = "0x" + "".join(_pad_hex(v) for v in (10_122_800, 16_238, 6_004_489_832_657_005))  # 3 words
        receipt = {
            "status": 1,
            "from": WALLET,
            "transactionHash": "0x" + "c4" * 32,
            "logs": [
                {"address": TRICRYPTO2_POOL, "topics": [add_topic, provider_topic], "data": short_data, "logIndex": 0}
            ],
        }
        result = CurveReceiptParser(chain="ethereum").parse_receipt(receipt)
        add = next(e for e in result.events if e.event_type == CurveEventType.ADD_LIQUIDITY)
        assert "raw_data" in add.data
        assert "token_amounts" not in add.data  # no fabricated amounts
