"""Demo Strategies - Tutorial strategies for learning the Almanak stack.

This package contains educational strategies with extensive comments
to help developers understand how to build trading strategies.

Available Strategies:
- uniswap_rsi: RSI-based trading on Uniswap V3
- uniswap_lp: Dynamic LP position management
- aave_borrow: Supply collateral and borrow on Aave V3
- almanak_rsi: RSI-based trading on Almanak token
- enso_rsi: RSI trading via Enso DEX aggregator
- traderjoe_lp: Liquidity Book position management on Avalanche
- aerodrome_lp: Solidly-based LP management on Base
- ethena_yield: Stake USDe for yield-bearing sUSDe
- spark_lender: Supply DAI for lending yield
- morpho_looping: Leveraged yield farming via recursive borrowing on Morpho Blue
- pancakeswap_simple: Simple swap on PancakeSwap V3
- pendle_basics: Pendle PT trading basics
- sushiswap_lp: SushiSwap V3 LP position management
"""

# Import strategies so they get registered
from .aave_borrow import AaveBorrowStrategy
from .aerodrome_lp import AerodromeLPStrategy
from .almanak_rsi import AlmanakRSIStrategy
from .enso_rsi import EnsoRSIStrategy
from .ethena_yield import EthenaYieldStrategy
from .morpho_looping import MorphoLoopingStrategy
from .pancakeswap_simple import PancakeSwapSimpleStrategy
from .pendle_basics import PendleBasicsStrategy
from .spark_lender import SparkLenderStrategy
from .sushiswap_lp import SushiSwapLPStrategy
from .traderjoe_lp import TraderJoeLPStrategy
from .uniswap_lp import UniswapLPStrategy
from .uniswap_rsi import UniswapRSIStrategy

__all__ = [
    "AaveBorrowStrategy",
    "AerodromeLPStrategy",
    "AlmanakRSIStrategy",
    "EnsoRSIStrategy",
    "EthenaYieldStrategy",
    "MorphoLoopingStrategy",
    "PancakeSwapSimpleStrategy",
    "PendleBasicsStrategy",
    "SparkLenderStrategy",
    "SushiSwapLPStrategy",
    "TraderJoeLPStrategy",
    "UniswapLPStrategy",
    "UniswapRSIStrategy",
]
