from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

from almanak.connectors.pendle import compiler as cp
from almanak.framework.intents.compiler_models import TokenInfo
from almanak.framework.intents.vocabulary import SwapIntent

ARBITRUM_PT_WSTETH_ADDRESS = "0x71fBF40651E9D4278a74586AfC99F307f369Ce9A"
ARBITRUM_YT_WSTETH_ADDRESS = "0x25bda1edd6af17c61399aa0eb84b93daa3069764"


def _mock_compiler(chain: str = "arbitrum", resolve_token_returns: TokenInfo | None = None) -> MagicMock:
    compiler = MagicMock(name="MockCompiler")
    compiler.chain = chain
    compiler._resolve_token.return_value = resolve_token_returns
    return compiler


def _swap_intent(from_token: str, to_token: str = "USDC") -> SwapIntent:
    return SwapIntent(from_token=from_token, to_token=to_token, amount_usd=Decimal("100"))


class TestResolvePendleFromToken:
    def test_compiler_resolves_token_directly(self) -> None:
        weth = TokenInfo(symbol="WETH", address="0x" + "ab" * 20, decimals=18, is_native=False)
        compiler = _mock_compiler(resolve_token_returns=weth)
        intent = _swap_intent(from_token="WETH")

        out = cp._resolve_pendle_from_token(compiler, intent)

        assert out is weth
        compiler._resolve_token.assert_called_once_with("WETH")

    def test_pt_branch_static_lookup_hit(self) -> None:
        compiler = _mock_compiler(chain="arbitrum", resolve_token_returns=None)
        intent = _swap_intent(from_token="PT-wstETH")

        out = cp._resolve_pendle_from_token(compiler, intent)

        assert out is not None
        assert out.symbol == "PT-wstETH"
        assert out.address == ARBITRUM_PT_WSTETH_ADDRESS
        assert out.decimals == 18
        assert out.is_native is False

    def test_pt_branch_unknown_token_returns_none(self) -> None:
        compiler = _mock_compiler(chain="arbitrum", resolve_token_returns=None)
        intent = _swap_intent(from_token="PT-NOTAREALTOKEN")

        assert cp._resolve_pendle_from_token(compiler, intent) is None

    def test_yt_branch_static_lookup_hit(self) -> None:
        compiler = _mock_compiler(chain="arbitrum", resolve_token_returns=None)
        intent = _swap_intent(from_token="YT-wstETH")

        out = cp._resolve_pendle_from_token(compiler, intent)

        assert out is not None
        assert out.symbol == "YT-wstETH"
        assert out.address == ARBITRUM_YT_WSTETH_ADDRESS
        assert out.decimals == 18
        assert out.is_native is False

    def test_yt_branch_unknown_token_returns_none(self) -> None:
        compiler = _mock_compiler(chain="arbitrum", resolve_token_returns=None)
        intent = _swap_intent(from_token="YT-NOTAREALTOKEN")

        assert cp._resolve_pendle_from_token(compiler, intent) is None

    def test_non_pt_or_yt_unresolved_returns_none(self) -> None:
        compiler = _mock_compiler(chain="arbitrum", resolve_token_returns=None)
        intent = _swap_intent(from_token="FOOBAR")

        assert cp._resolve_pendle_from_token(compiler, intent) is None
