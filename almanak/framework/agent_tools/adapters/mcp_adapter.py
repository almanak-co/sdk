"""MCP (Model Context Protocol) adapter for Almanak agent tools.

Exposes the Almanak tool catalog as an MCP server that any MCP-compatible
client (Claude Desktop, Cursor, custom agents) can connect to.

Transport options:
    - stdio  (default, for local agent processes)
    - HTTP/SSE (for remote orchestrators)

Usage::

    from almanak.framework.agent_tools.adapters.mcp_adapter import AlmanakMCPServer

    server = AlmanakMCPServer(executor)
    server.run_stdio()  # Blocks, serving over stdin/stdout
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from almanak.framework.agent_tools.catalog import ToolCatalog, get_default_catalog

if TYPE_CHECKING:
    from almanak.framework.agent_tools.executor import ToolExecutor

logger = logging.getLogger(__name__)

# MCP resource URIs
RESOURCE_CHAINS = "almanak://chains"
RESOURCE_PROTOCOLS = "almanak://protocols"
RESOURCE_RISK_POLICY = "almanak://risk-policy/current"
RESOURCE_WALLET = "almanak://wallet/capabilities"


class AlmanakMCPServer:
    """MCP server wrapping the Almanak tool catalog and executor.

    Implements the MCP protocol's ``tools/list`` and ``tools/call``
    endpoints, plus resource endpoints for static context.
    """

    def __init__(
        self,
        executor: ToolExecutor,
        catalog: ToolCatalog | None = None,
    ) -> None:
        self._executor = executor
        self._catalog = catalog or get_default_catalog()

    # -- MCP tools endpoints ------------------------------------------------

    def tools_list(self) -> list[dict]:
        """Return MCP-formatted tool definitions for ``tools/list``."""
        return self._catalog.to_mcp_tools()

    async def tools_call(self, name: str, arguments: dict) -> dict:
        """Execute a tool call for ``tools/call``.

        Returns an MCP-formatted result with ``content`` array.
        """
        response = await self._executor.execute(name, arguments)
        # MCP expects a content array with text/image blocks
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(response.model_dump(exclude_none=True), indent=2),
                }
            ],
        }

    # -- MCP resources endpoints --------------------------------------------

    def resources_list(self) -> list[dict]:
        """Return available MCP resources."""
        return [
            {
                "uri": RESOURCE_CHAINS,
                "name": "Supported Chains",
                "description": "Available blockchain networks and their status.",
                "mimeType": "application/json",
            },
            {
                "uri": RESOURCE_PROTOCOLS,
                "name": "Supported Protocols",
                "description": "DeFi protocols available per chain.",
                "mimeType": "application/json",
            },
            {
                "uri": RESOURCE_RISK_POLICY,
                "name": "Risk Policy",
                "description": "Active agent risk policy and constraints.",
                "mimeType": "application/json",
            },
            {
                "uri": RESOURCE_WALLET,
                "name": "Wallet Capabilities",
                "description": "Strategy wallet type and supported actions.",
                "mimeType": "application/json",
            },
        ]

    def resources_read(self, uri: str) -> dict:
        """Read an MCP resource by URI."""
        from almanak.core.enums import Chain, Protocol

        if uri == RESOURCE_CHAINS:
            return {
                "contents": [
                    {
                        "uri": uri,
                        "mimeType": "application/json",
                        "text": json.dumps({"chains": [c.value for c in Chain]}),
                    }
                ]
            }

        if uri == RESOURCE_PROTOCOLS:
            return {
                "contents": [
                    {
                        "uri": uri,
                        "mimeType": "application/json",
                        "text": json.dumps({"protocols": [p.value for p in Protocol]}),
                    }
                ]
            }

        if uri == RESOURCE_RISK_POLICY:
            from dataclasses import asdict

            policy = self._executor._policy_engine.policy
            # Convert Decimal fields to strings for JSON serialization
            policy_dict: dict[str, object] = {}
            for k, v in asdict(policy).items():
                if hasattr(v, "__str__") and not isinstance(
                    v, str | int | float | bool | list | dict | set | type(None)
                ):
                    policy_dict[k] = str(v)
                elif isinstance(v, set):
                    policy_dict[k] = sorted(v) if v else []
                else:
                    policy_dict[k] = v
            return {
                "contents": [
                    {
                        "uri": uri,
                        "mimeType": "application/json",
                        "text": json.dumps(policy_dict, default=str),
                    }
                ]
            }

        if uri == RESOURCE_WALLET:
            return {
                "contents": [
                    {
                        "uri": uri,
                        "mimeType": "application/json",
                        "text": json.dumps(
                            {
                                "wallet_address": self._executor._wallet_address,
                                "strategy_id": self._executor._strategy_id,
                                "tools_available": self._catalog.list_names(),
                            }
                        ),
                    }
                ]
            }

        return {"contents": []}
