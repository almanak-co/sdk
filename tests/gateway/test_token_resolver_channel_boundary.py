"""Gateway-owned resolution must never enter the strategy's synchronous channel."""

import ast
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.data.tokens.resolver import TokenResolver
from almanak.gateway.data.balance.solana_provider import SolanaBalanceProvider


def test_gateway_token_resolver_calls_are_local():
    root = Path(__file__).resolve().parents[2] / "almanak"
    gateway_roots = [
        root / "gateway",
        *(root / "integrations").glob("*/gateway"),
        *(root / "connectors").glob("*/gateway"),
    ]
    violations = []
    for gateway_root in gateway_roots:
        for path in gateway_root.rglob("*.py"):
            for node in ast.walk(ast.parse(path.read_text())):
                if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                    continue
                if (
                    node.func.attr not in {"resolve", "resolve_caip19", "get_decimals", "get_address", "get_symbol"}
                    or "resolver" not in ast.unparse(node.func.value).lower()
                ):
                    continue
                if not any(
                    keyword.arg == "skip_gateway"
                    and (
                        (isinstance(keyword.value, ast.Constant) and keyword.value.value is True)
                        or (
                            # Price-source tests pin the local default and the paper-only opt-in.
                            path.relative_to(root).as_posix() == "integrations/dexscreener/gateway/price_source.py"
                            and ast.unparse(keyword.value) == "self._skip_gateway_resolution"
                        )
                    )
                    for keyword in node.keywords
                ):
                    violations.append(f"{path.relative_to(root)}:{node.lineno}")
    assert not violations, f"Gateway token lookups can recurse through gRPC: {violations}"


@pytest.mark.asyncio
async def test_unknown_solana_mint_uses_owned_balance_lookup(tmp_path):
    mint = "9" * 44
    channel = MagicMock()
    resolver = TokenResolver(cache_file=str(tmp_path / "tokens.json"), gateway_channel=channel)
    provider = SolanaBalanceProvider(rpc_url="https://solana.invalid", wallet_address="1" * 32)
    with (
        patch.object(provider, "_get_token_resolver", return_value=resolver),
        patch.object(provider, "_get_spl_token_balance", new_callable=AsyncMock, return_value=(1230000, 6)) as lookup,
    ):
        result = await provider.get_balance(mint)
    assert result.balance == Decimal("1.23")
    assert result.decimals == 6
    lookup.assert_awaited_once_with(mint, None)
    channel.unary_unary.assert_not_called()


def test_metamorpho_unknown_asset_fails_closed_without_self_rpc(tmp_path):
    from almanak.connectors.morpho_vault.gateway.provider import _metamorpho_resolve_vault
    from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

    channel = MagicMock()
    resolver = TokenResolver(cache_file=str(tmp_path / "tokens.json"), gateway_channel=channel)
    with patch("almanak.framework.data.tokens.get_token_resolver", return_value=resolver):
        with pytest.raises(RateHistoryUnavailable, match="Cannot resolve MetaMorpho asset"):
            _metamorpho_resolve_vault("ethereum", "UNKNOWNTEST")
    channel.unary_unary.assert_not_called()


@pytest.mark.asyncio
async def test_registered_solana_symbol_keeps_owned_balance_lookup(tmp_path):
    channel = MagicMock()
    resolver = TokenResolver(cache_file=str(tmp_path / "tokens.json"), gateway_channel=channel)
    token = resolver.resolve("JUP", "solana", skip_gateway=True)
    provider = SolanaBalanceProvider(rpc_url="https://solana.invalid", wallet_address="1" * 32)
    with (
        patch.object(provider, "_get_token_resolver", return_value=resolver),
        patch.object(provider, "_get_spl_token_balance", new_callable=AsyncMock, return_value=(1000000, 6)) as lookup,
    ):
        result = await provider.get_balance("JUP")
    assert result.balance == Decimal("1")
    lookup.assert_awaited_once_with(token.address, token.decimals)
    channel.unary_unary.assert_not_called()
