"""Factory for creating a ToolExecutor wired for CLI (one-shot) use.

Shared bootstrap logic used by ``almanak ax`` commands and potentially
the MCP server. Connects to a running gateway (fail-fast, no auto-start),
builds PolicyEngine with persistent state, and returns a ready-to-use
ToolExecutor + GatewayClient pair.
"""

from __future__ import annotations

import logging
import os
from decimal import Decimal

import click

logger = logging.getLogger(__name__)

from almanak.framework.agent_tools.executor import ToolExecutor
from almanak.framework.agent_tools.policy import AgentPolicy
from almanak.framework.gateway_client import GatewayClient, GatewayClientConfig


class GatewayConnectionError(click.ClickException):
    """Raised when the CLI cannot connect to the gateway."""

    def __init__(self, host: str, port: int) -> None:
        super().__init__(
            f"Cannot connect to gateway at {host}:{port}.\n"
            "Start a gateway first with: almanak gateway\n"
            "Or start one for Anvil testing: almanak gateway --network anvil"
        )


def create_cli_executor(
    *,
    gateway_host: str = "localhost",
    gateway_port: int = 50051,
    chain: str = "arbitrum",
    wallet_address: str = "",
    max_single_trade_usd: float = 10000,
    max_daily_spend_usd: float = 50000,
    allowed_chains: tuple[str, ...] | None = None,
    allowed_tokens: tuple[str, ...] | None = None,
    allowed_protocols: tuple[str, ...] | None = None,
    connect_timeout: float = 5.0,
    auth_token: str | None = None,
) -> tuple[ToolExecutor, GatewayClient]:
    """Create a ToolExecutor connected to a running gateway.

    This is the shared bootstrap for all ``almanak ax`` commands.
    It connects to the gateway (fail-fast -- never auto-starts one),
    builds a PolicyEngine with persistent daily spend state, and
    returns a ready-to-use (executor, client) pair.

    The caller is responsible for calling ``client.disconnect()``
    when done (or using the returned client as a context manager).

    Args:
        gateway_host: Gateway hostname.
        gateway_port: Gateway gRPC port.
        chain: Default chain for commands.
        wallet_address: Wallet address (auto-derived from ALMANAK_PRIVATE_KEY if empty).
        max_single_trade_usd: Max single trade size in USD.
        max_daily_spend_usd: Max daily spend in USD.
        allowed_chains: Restrict to specific chains. Defaults to None (all chains)
            so cross-chain operations like bridge work without extra config.
        allowed_tokens: Restrict to specific tokens. None = all.
        allowed_protocols: Restrict to specific protocols. None = all.
        connect_timeout: Seconds to wait for gateway connection.

    Returns:
        Tuple of (ToolExecutor, GatewayClient). Client is connected and ready.

    Raises:
        GatewayConnectionError: If the gateway is not reachable.
    """
    # Resolve wallet address from env if not provided
    if not wallet_address:
        wallet_address = _resolve_wallet_address()

    # Connect to gateway (fail-fast)
    config = GatewayClientConfig(host=gateway_host, port=gateway_port, auth_token=auth_token)
    client = GatewayClient(config)
    try:
        client.connect()
        if not client.wait_for_ready(timeout=connect_timeout):
            client.disconnect()
            raise GatewayConnectionError(gateway_host, gateway_port)
    except GatewayConnectionError:
        raise
    except Exception as exc:
        try:
            client.disconnect()
        except Exception:
            pass
        raise GatewayConnectionError(gateway_host, gateway_port) from exc

    policy = AgentPolicy(
        max_single_trade_usd=Decimal(str(max_single_trade_usd)),
        max_daily_spend_usd=Decimal(str(max_daily_spend_usd)),
        # CLI users explicitly choose actions and confirm via the safety gate.
        # Default to None (all chains allowed) so cross-chain operations like
        # bridge work without extra config. Users can still restrict via allowed_chains.
        allowed_chains=set(allowed_chains) if allowed_chains else None,
        allowed_tokens=set(allowed_tokens) if allowed_tokens else None,
        allowed_protocols=set(allowed_protocols) if allowed_protocols else None,
        # CLI users explicitly request actions — no autonomous agent loop
        # that needs pre-validation gates like rebalance checks or cooldowns.
        require_rebalance_check=False,
        cooldown_seconds=0,
    )

    executor = ToolExecutor(
        gateway_client=client,
        policy=policy,
        wallet_address=wallet_address,
        default_chain=chain,
    )

    return executor, client


def _resolve_wallet_address() -> str:
    """Derive wallet address from ALMANAK_PRIVATE_KEY env var.

    Returns empty string if no key is set (some read-only commands
    don't need a wallet).
    """
    pk = os.environ.get("ALMANAK_PRIVATE_KEY", "")
    if not pk:
        return ""
    try:
        from eth_account import Account

        pk_hex = pk if pk.startswith("0x") else f"0x{pk}"
        return Account.from_key(pk_hex).address
    except Exception:
        logger.warning("ALMANAK_PRIVATE_KEY is set but could not be parsed as a valid private key")
        return ""
