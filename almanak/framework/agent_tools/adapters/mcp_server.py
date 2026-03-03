"""MCP stdio transport server for Almanak agent tools.

Implements the Model Context Protocol (MCP) over stdio transport, enabling
MCP-compatible clients (Claude Desktop, Cursor, custom agents) to connect
and use all Almanak agent tools.

The protocol uses content-length framed JSON-RPC messages over stdin/stdout,
following the same framing as the Language Server Protocol (LSP).

Usage::

    from almanak.framework.agent_tools.adapters.mcp_server import AlmanakMCPStdioServer

    server = AlmanakMCPStdioServer(executor=executor)
    asyncio.run(server.run())

Or via CLI::

    almanak mcp serve
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from typing import TYPE_CHECKING, Any

from almanak.framework.agent_tools.adapters.mcp_adapter import AlmanakMCPServer
from almanak.framework.agent_tools.catalog import get_default_catalog

if TYPE_CHECKING:
    from almanak.framework.agent_tools.executor import ToolExecutor

logger = logging.getLogger(__name__)

# MCP protocol version we implement
MCP_PROTOCOL_VERSION = "2024-11-05"
SERVER_NAME = "almanak-agent-tools"
SERVER_VERSION = "1.0.0"

# JSON-RPC error codes
PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INTERNAL_ERROR = -32603
MAX_CONTENT_LENGTH = 10 * 1024 * 1024  # 10 MiB safety cap


class MCPReadError(Exception):
    """Raised when a message frame is malformed (not EOF)."""

    def __init__(self, code: int, message: str) -> None:
        super().__init__(message)
        self.code = code


class AlmanakMCPStdioServer:
    """MCP server that runs over stdio transport.

    Reads content-length framed JSON-RPC messages from stdin and writes
    responses to stdout. Delegates tool listing to AlmanakMCPServer (schema
    adapter) and tool execution to ToolExecutor with full PolicyEngine checks.

    Args:
        executor: ToolExecutor instance for dispatching tool calls.
            If None, tool calls will return an error indicating no executor
            is configured (useful for testing tools/list only).
        adapter: Optional AlmanakMCPServer for schema generation. If not
            provided, a new one is created from the executor.
    """

    def __init__(
        self,
        executor: ToolExecutor | None = None,
        adapter: AlmanakMCPServer | None = None,
    ) -> None:
        self._executor = executor
        self._adapter: AlmanakMCPServer | None
        if adapter is not None:
            self._adapter = adapter
        elif executor is not None:
            self._adapter = AlmanakMCPServer(executor=executor)
        else:
            # Schema-only mode: no adapter
            self._adapter = None
        self._initialized = False

    async def run(
        self,
        reader: asyncio.StreamReader | None = None,
        writer: asyncio.StreamWriter | None = None,
    ) -> None:
        """Run the stdio transport loop.

        Reads messages from stdin (or the provided reader) and writes
        responses to stdout (or the provided writer). Runs until EOF
        on stdin or a KeyboardInterrupt.

        Args:
            reader: Optional async reader (for testing). Defaults to stdin.
            writer: Optional async writer (for testing). Defaults to stdout.
        """
        if reader is None:
            reader = asyncio.StreamReader()
            protocol = asyncio.StreamReaderProtocol(reader)
            loop = asyncio.get_running_loop()
            await loop.connect_read_pipe(lambda: protocol, sys.stdin.buffer)

        self._writer = writer
        self._stdout = None
        if writer is None:
            # Use raw stdout for writing
            self._stdout = sys.stdout.buffer

        logger.info("Almanak MCP stdio server started")

        try:
            while True:
                try:
                    message = await self._read_message(reader)
                except MCPReadError as exc:
                    await self._write_message(_error_response(None, exc.code, str(exc)))
                    continue
                if message is None:
                    # EOF on stdin
                    logger.info("EOF on stdin, shutting down")
                    break

                response = await self.handle_message(message)
                if response is not None:
                    await self._write_message(response)
        except asyncio.CancelledError:
            logger.info("MCP server cancelled")
        except Exception:
            logger.exception("MCP server error")
            raise

    async def handle_message(self, message: dict) -> dict | None:
        """Handle a single JSON-RPC message.

        Returns a JSON-RPC response dict, or None for notifications
        (messages without an ``id`` field).
        """
        if not isinstance(message, dict):
            return _error_response(None, INVALID_REQUEST, "JSON-RPC message must be an object")

        msg_id = message.get("id")
        method = message.get("method")

        if method is None:
            if msg_id is not None:
                return _error_response(msg_id, INVALID_REQUEST, "Missing 'method' field")
            return None  # Notification without method, ignore

        # Dispatch to handler
        handler = self._get_handler(method)
        if handler is None:
            if msg_id is not None:
                return _error_response(msg_id, METHOD_NOT_FOUND, f"Unknown method: {method}")
            return None  # Notification for unknown method

        try:
            result = await handler(message)
            if msg_id is not None:
                return _success_response(msg_id, result)
            return None  # Notification response is dropped
        except ValueError as exc:
            if msg_id is not None:
                return _error_response(msg_id, INVALID_REQUEST, str(exc))
            return None
        except Exception as exc:
            logger.exception("Error handling method %s", method)
            if msg_id is not None:
                return _error_response(msg_id, INTERNAL_ERROR, str(exc))
            return None

    def _get_handler(self, method: str):
        """Return the handler coroutine for a method, or None."""
        handlers = {
            "initialize": self._handle_initialize,
            "initialized": self._handle_initialized,
            "ping": self._handle_ping,
            "tools/list": self._handle_tools_list,
            "tools/call": self._handle_tools_call,
            "resources/list": self._handle_resources_list,
            "resources/read": self._handle_resources_read,
            "notifications/cancelled": self._handle_notification,
        }
        return handlers.get(method)

    # -- Protocol handlers ---------------------------------------------------

    async def _handle_initialize(self, message: dict) -> dict:
        """Handle initialize request. Returns server capabilities."""
        params = message.get("params", {})
        if not isinstance(params, dict):
            raise ValueError("'params' must be an object")
        self._initialized = True
        return {
            "protocolVersion": MCP_PROTOCOL_VERSION,
            "capabilities": {
                "tools": {"listChanged": False},
                "resources": {"subscribe": False, "listChanged": False},
            },
            "serverInfo": {
                "name": SERVER_NAME,
                "version": SERVER_VERSION,
            },
        }

    async def _handle_initialized(self, _message: dict) -> dict:
        """Handle initialized notification. No-op acknowledgement."""
        return {}

    async def _handle_ping(self, _message: dict) -> dict:
        """Handle ping request."""
        return {}

    async def _handle_tools_list(self, _message: dict) -> dict:
        """Handle tools/list request. Returns all tools in MCP schema."""
        if self._adapter is not None:
            tools = self._adapter.tools_list()
        else:
            # Fallback: use default catalog directly
            catalog = get_default_catalog()
            tools = catalog.to_mcp_tools()
        return {"tools": tools}

    async def _handle_tools_call(self, message: dict) -> dict:
        """Handle tools/call request. Executes a tool via ToolExecutor."""
        params = message.get("params", {})
        if not isinstance(params, dict):
            raise ValueError("'params' must be an object")
        tool_name = params.get("name")
        arguments = params.get("arguments", {})
        if not isinstance(arguments, dict):
            raise ValueError("'arguments' must be an object")

        if not isinstance(tool_name, str) or not tool_name.strip():
            raise ValueError("'name' in tools/call params must be a non-empty string")

        if self._adapter is None:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(
                            {
                                "status": "error",
                                "error": {
                                    "error_code": "no_executor",
                                    "message": "No executor configured. Start with gateway connection for tool execution.",
                                    "recoverable": False,
                                },
                            }
                        ),
                    }
                ],
                "isError": True,
            }

        result = await self._adapter.tools_call(tool_name, arguments)

        # Check if the result indicates an error
        is_error = False
        if result.get("content"):
            for content_item in result["content"]:
                if content_item.get("type") == "text":
                    try:
                        parsed = json.loads(content_item["text"])
                        if parsed.get("status") in ("error", "blocked"):
                            is_error = True
                    except (json.JSONDecodeError, KeyError):
                        pass

        return {
            "content": result.get("content", []),
            "isError": is_error,
        }

    async def _handle_resources_list(self, _message: dict) -> dict:
        """Handle resources/list request."""
        if self._adapter is not None:
            resources = self._adapter.resources_list()
        else:
            resources = []
        return {"resources": resources}

    async def _handle_resources_read(self, message: dict) -> dict:
        """Handle resources/read request."""
        params = message.get("params", {})
        if not isinstance(params, dict):
            raise ValueError("'params' must be an object")
        uri = params.get("uri", "")
        if not isinstance(uri, str) or not uri:
            raise ValueError("'uri' must be a non-empty string")

        if self._adapter is not None:
            return self._adapter.resources_read(uri)

        return {"contents": []}

    async def _handle_notification(self, _message: dict) -> dict:
        """Handle notification messages (no response needed)."""
        return {}

    # -- Message framing (content-length delimited) --------------------------

    async def _read_message(self, reader: asyncio.StreamReader) -> dict | None:
        """Read a content-length framed JSON-RPC message from the reader.

        Returns the parsed message dict, or None on EOF.
        Raises MCPReadError for malformed frames (missing headers, invalid JSON).
        """
        # Read headers until empty line
        content_length = None
        while True:
            try:
                line = await reader.readline()
            except asyncio.IncompleteReadError:
                return None

            if not line:
                return None  # EOF

            try:
                line_str = line.decode("ascii").rstrip("\r\n")
            except UnicodeDecodeError as exc:
                raise MCPReadError(PARSE_ERROR, f"Invalid header encoding: {exc}") from exc
            if not line_str:
                break  # End of headers

            if line_str.lower().startswith("content-length:"):
                try:
                    content_length = int(line_str.split(":", 1)[1].strip())
                except (ValueError, IndexError):
                    logger.warning("Invalid Content-Length header: %s", line_str)
                    continue

        if content_length is None:
            raise MCPReadError(INVALID_REQUEST, "Missing Content-Length header")
        if content_length < 0:
            raise MCPReadError(INVALID_REQUEST, "Content-Length must be >= 0")
        if content_length > MAX_CONTENT_LENGTH:
            raise MCPReadError(
                INVALID_REQUEST, f"Content-Length {content_length} exceeds {MAX_CONTENT_LENGTH} byte limit"
            )

        # Read exactly content_length bytes
        try:
            data = await reader.readexactly(content_length)
        except asyncio.IncompleteReadError:
            return None

        try:
            return json.loads(data.decode("utf-8"))
        except UnicodeDecodeError as exc:
            raise MCPReadError(PARSE_ERROR, f"Invalid UTF-8 in message body: {exc}") from exc
        except json.JSONDecodeError as exc:
            raise MCPReadError(PARSE_ERROR, f"Invalid JSON in message body: {exc}") from exc

    async def _write_message(self, message: dict) -> None:
        """Write a content-length framed JSON-RPC message."""
        body = json.dumps(message).encode("utf-8")
        header = f"Content-Length: {len(body)}\r\n\r\n".encode()
        payload = header + body

        if self._writer is not None:
            self._writer.write(payload)
            await self._writer.drain()
        elif self._stdout is not None:
            self._stdout.write(payload)
            self._stdout.flush()


# -- JSON-RPC helpers --------------------------------------------------------


def _success_response(msg_id: Any, result: Any) -> dict:
    """Build a JSON-RPC success response."""
    return {
        "jsonrpc": "2.0",
        "id": msg_id,
        "result": result,
    }


def _error_response(msg_id: Any, code: int, message: str, data: Any = None) -> dict:
    """Build a JSON-RPC error response."""
    error: dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        error["data"] = data
    return {
        "jsonrpc": "2.0",
        "id": msg_id,
        "error": error,
    }


def parse_message_from_bytes(data: bytes) -> tuple[dict | None, bytes]:
    """Parse a single content-length framed message from a byte buffer.

    Returns (parsed_message, remaining_bytes). If the buffer does not
    contain a complete message, returns (None, original_data).

    This is a utility function for synchronous/testing use.
    """
    separator = b"\r\n\r\n"
    separator_idx = data.find(separator)
    if separator_idx == -1:
        return None, data

    header_part = data[:separator_idx].decode("ascii", errors="replace")
    content_length = None
    for line in header_part.split("\r\n"):
        if line.lower().startswith("content-length:"):
            try:
                content_length = int(line.split(":", 1)[1].strip())
            except (ValueError, IndexError):
                pass

    if content_length is None or content_length < 0:
        return None, data

    body_start = separator_idx + len(separator)
    body_end = body_start + content_length

    if len(data) < body_end:
        return None, data

    body = data[body_start:body_end]
    remaining = data[body_end:]

    try:
        message = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        # Frame is complete but malformed; consume it so callers advance past it.
        return None, remaining
    return message, remaining
