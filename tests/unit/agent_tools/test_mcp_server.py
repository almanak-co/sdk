"""Tests for the MCP stdio transport server."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.agent_tools.adapters.mcp_server import (
    INTERNAL_ERROR,
    INVALID_REQUEST,
    MAX_CONTENT_LENGTH,
    MCP_PROTOCOL_VERSION,
    MCPReadError,
    METHOD_NOT_FOUND,
    PARSE_ERROR,
    SERVER_NAME,
    SERVER_VERSION,
    AlmanakMCPStdioServer,
    _error_response,
    _success_response,
    parse_message_from_bytes,
)
from almanak.framework.agent_tools.catalog import get_default_catalog
from almanak.framework.agent_tools.schemas import ToolResponse


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_request(method: str, params: dict | None = None, msg_id: int = 1) -> dict:
    """Build a JSON-RPC request."""
    msg = {"jsonrpc": "2.0", "method": method, "id": msg_id}
    if params is not None:
        msg["params"] = params
    return msg


def _frame_message(message: dict) -> bytes:
    """Encode a message with content-length framing."""
    body = json.dumps(message).encode("utf-8")
    header = f"Content-Length: {len(body)}\r\n\r\n".encode("utf-8")
    return header + body


def _make_server(executor=None, adapter=None) -> AlmanakMCPStdioServer:
    """Create a server instance for testing."""
    return AlmanakMCPStdioServer(executor=executor, adapter=adapter)


# ---------------------------------------------------------------------------
# JSON-RPC helper tests
# ---------------------------------------------------------------------------


class TestJsonRpcHelpers:
    def test_success_response(self):
        resp = _success_response(1, {"key": "value"})
        assert resp["jsonrpc"] == "2.0"
        assert resp["id"] == 1
        assert resp["result"] == {"key": "value"}
        assert "error" not in resp

    def test_error_response(self):
        resp = _error_response(2, -32600, "Bad request")
        assert resp["jsonrpc"] == "2.0"
        assert resp["id"] == 2
        assert resp["error"]["code"] == -32600
        assert resp["error"]["message"] == "Bad request"
        assert "data" not in resp["error"]

    def test_error_response_with_data(self):
        resp = _error_response(3, -32603, "Internal", data={"detail": "oops"})
        assert resp["error"]["data"] == {"detail": "oops"}


# ---------------------------------------------------------------------------
# Message framing tests
# ---------------------------------------------------------------------------


class TestMessageFraming:
    def test_parse_complete_message(self):
        msg = {"jsonrpc": "2.0", "method": "ping", "id": 1}
        data = _frame_message(msg)
        parsed, remaining = parse_message_from_bytes(data)
        assert parsed == msg
        assert remaining == b""

    def test_parse_incomplete_message(self):
        msg = {"jsonrpc": "2.0", "method": "ping", "id": 1}
        data = _frame_message(msg)
        # Truncate the data
        partial = data[:10]
        parsed, remaining = parse_message_from_bytes(partial)
        assert parsed is None
        assert remaining == partial

    def test_parse_multiple_messages(self):
        msg1 = {"jsonrpc": "2.0", "method": "ping", "id": 1}
        msg2 = {"jsonrpc": "2.0", "method": "tools/list", "id": 2}
        data = _frame_message(msg1) + _frame_message(msg2)
        parsed1, remaining = parse_message_from_bytes(data)
        assert parsed1 == msg1
        assert len(remaining) > 0
        parsed2, remaining2 = parse_message_from_bytes(remaining)
        assert parsed2 == msg2
        assert remaining2 == b""

    def test_parse_no_content_length(self):
        data = b"Invalid-Header: foo\r\n\r\n{}"
        parsed, remaining = parse_message_from_bytes(data)
        assert parsed is None

    @pytest.mark.asyncio
    async def test_read_message_from_stream(self):
        server = _make_server()
        msg = {"jsonrpc": "2.0", "method": "ping", "id": 1}
        framed = _frame_message(msg)

        reader = asyncio.StreamReader()
        reader.feed_data(framed)
        reader.feed_eof()

        result = await server._read_message(reader)
        assert result == msg

    @pytest.mark.asyncio
    async def test_read_message_eof(self):
        server = _make_server()
        reader = asyncio.StreamReader()
        reader.feed_eof()

        result = await server._read_message(reader)
        assert result is None

    @pytest.mark.asyncio
    async def test_write_message(self):
        server = _make_server()
        msg = {"jsonrpc": "2.0", "id": 1, "result": {}}

        writer = MagicMock()
        writer.write = MagicMock()
        writer.drain = AsyncMock()
        server._writer = writer
        server._stdout = None

        await server._write_message(msg)

        writer.write.assert_called_once()
        written_data = writer.write.call_args[0][0]
        assert b"Content-Length:" in written_data
        assert b'"jsonrpc"' in written_data


# ---------------------------------------------------------------------------
# Initialize handler tests
# ---------------------------------------------------------------------------


class TestInitialize:
    @pytest.mark.asyncio
    async def test_initialize_response(self):
        server = _make_server()
        request = _make_request("initialize", {"protocolVersion": MCP_PROTOCOL_VERSION})
        response = await server.handle_message(request)

        assert response["jsonrpc"] == "2.0"
        assert response["id"] == 1
        result = response["result"]
        assert result["protocolVersion"] == MCP_PROTOCOL_VERSION
        assert result["serverInfo"]["name"] == SERVER_NAME
        assert result["serverInfo"]["version"] == SERVER_VERSION
        assert "tools" in result["capabilities"]
        assert result["capabilities"]["tools"]["listChanged"] is False

    @pytest.mark.asyncio
    async def test_initialize_sets_flag(self):
        server = _make_server()
        assert not server._initialized
        request = _make_request("initialize")
        await server.handle_message(request)
        assert server._initialized

    @pytest.mark.asyncio
    async def test_initialize_includes_resources_capability(self):
        server = _make_server()
        request = _make_request("initialize")
        response = await server.handle_message(request)
        result = response["result"]
        assert "resources" in result["capabilities"]


# ---------------------------------------------------------------------------
# Ping handler tests
# ---------------------------------------------------------------------------


class TestPing:
    @pytest.mark.asyncio
    async def test_ping_response(self):
        server = _make_server()
        request = _make_request("ping")
        response = await server.handle_message(request)

        assert response["jsonrpc"] == "2.0"
        assert response["id"] == 1
        assert response["result"] == {}


# ---------------------------------------------------------------------------
# Tools/list handler tests
# ---------------------------------------------------------------------------


class TestToolsList:
    @pytest.mark.asyncio
    async def test_tools_list_returns_all_tools(self):
        server = _make_server()
        request = _make_request("tools/list")
        response = await server.handle_message(request)

        assert response["jsonrpc"] == "2.0"
        result = response["result"]
        assert "tools" in result
        tools = result["tools"]
        # Sanity check: catalog should have a reasonable number of tools
        assert len(tools) >= 20

    @pytest.mark.asyncio
    async def test_tools_list_tool_format(self):
        server = _make_server()
        request = _make_request("tools/list")
        response = await server.handle_message(request)

        tools = response["result"]["tools"]
        # Verify each tool has required MCP fields
        for tool in tools:
            assert "name" in tool, f"Tool missing 'name': {tool}"
            assert "description" in tool, f"Tool {tool.get('name')} missing 'description'"
            assert "inputSchema" in tool, f"Tool {tool.get('name')} missing 'inputSchema'"

    @pytest.mark.asyncio
    async def test_tools_list_contains_known_tools(self):
        server = _make_server()
        request = _make_request("tools/list")
        response = await server.handle_message(request)

        tool_names = {t["name"] for t in response["result"]["tools"]}
        # Verify a few well-known tools
        assert "get_price" in tool_names
        assert "get_balance" in tool_names
        assert "swap_tokens" in tool_names
        assert "compile_intent" in tool_names

    @pytest.mark.asyncio
    async def test_tools_list_with_adapter(self):
        mock_adapter = MagicMock()
        mock_adapter.tools_list.return_value = [
            {"name": "test_tool", "description": "Test", "inputSchema": {}}
        ]
        server = _make_server(adapter=mock_adapter)
        request = _make_request("tools/list")
        response = await server.handle_message(request)

        assert len(response["result"]["tools"]) == 1
        assert response["result"]["tools"][0]["name"] == "test_tool"
        mock_adapter.tools_list.assert_called_once()


# ---------------------------------------------------------------------------
# Tools/call handler tests
# ---------------------------------------------------------------------------


class TestToolsCall:
    @pytest.mark.asyncio
    async def test_tools_call_no_executor(self):
        """Without an executor, tools/call returns a structured error."""
        server = _make_server(executor=None)
        request = _make_request("tools/call", {"name": "get_price", "arguments": {"token": "ETH"}})
        response = await server.handle_message(request)

        result = response["result"]
        assert result["isError"] is True
        content = result["content"][0]
        assert content["type"] == "text"
        parsed = json.loads(content["text"])
        assert parsed["status"] == "error"
        assert parsed["error"]["error_code"] == "no_executor"

    @pytest.mark.asyncio
    async def test_tools_call_dispatches_to_executor(self):
        """tools/call dispatches to the executor and returns the result."""
        mock_executor = AsyncMock()
        mock_executor.execute.return_value = ToolResponse(
            status="success",
            data={"price_usd": 3000.0},
        )

        mock_adapter = MagicMock()
        mock_adapter.tools_call = AsyncMock(return_value={
            "content": [{"type": "text", "text": json.dumps({"status": "success", "data": {"price_usd": 3000.0}})}]
        })

        server = _make_server(executor=mock_executor, adapter=mock_adapter)
        request = _make_request("tools/call", {"name": "get_price", "arguments": {"token": "ETH"}})
        response = await server.handle_message(request)

        result = response["result"]
        assert result["isError"] is False
        mock_adapter.tools_call.assert_called_once_with("get_price", {"token": "ETH"})

    @pytest.mark.asyncio
    async def test_tools_call_missing_name(self):
        """tools/call without a tool name returns an INVALID_REQUEST error."""
        server = _make_server()
        request = _make_request("tools/call", {"arguments": {}})
        response = await server.handle_message(request)

        assert "error" in response
        assert response["error"]["code"] == INVALID_REQUEST

    @pytest.mark.asyncio
    async def test_tools_call_non_string_name(self):
        """tools/call with a non-string name returns INVALID_REQUEST."""
        server = _make_server()
        request = _make_request("tools/call", {"name": 123, "arguments": {}})
        response = await server.handle_message(request)

        assert "error" in response
        assert response["error"]["code"] == INVALID_REQUEST

    @pytest.mark.asyncio
    async def test_tools_call_policy_violation(self):
        """tools/call with a blocked result sets isError."""
        mock_adapter = MagicMock()
        mock_adapter.tools_call = AsyncMock(return_value={
            "content": [{
                "type": "text",
                "text": json.dumps({
                    "status": "blocked",
                    "error": {"error_code": "risk_blocked", "message": "Trade exceeds limit"},
                }),
            }]
        })

        mock_executor = AsyncMock()
        server = _make_server(executor=mock_executor, adapter=mock_adapter)
        request = _make_request("tools/call", {"name": "swap_tokens", "arguments": {}})
        response = await server.handle_message(request)

        result = response["result"]
        assert result["isError"] is True

    @pytest.mark.asyncio
    async def test_tools_call_executor_error_status(self):
        """tools/call with error status from executor sets isError."""
        mock_adapter = MagicMock()
        mock_adapter.tools_call = AsyncMock(return_value={
            "content": [{
                "type": "text",
                "text": json.dumps({
                    "status": "error",
                    "error": {"error_code": "execution_failed", "message": "Reverted"},
                }),
            }]
        })

        mock_executor = AsyncMock()
        server = _make_server(executor=mock_executor, adapter=mock_adapter)
        request = _make_request("tools/call", {"name": "swap_tokens", "arguments": {}})
        response = await server.handle_message(request)

        result = response["result"]
        assert result["isError"] is True


# ---------------------------------------------------------------------------
# Error handling tests
# ---------------------------------------------------------------------------


class TestErrorHandling:
    @pytest.mark.asyncio
    async def test_unknown_method_returns_error(self):
        server = _make_server()
        request = _make_request("nonexistent/method")
        response = await server.handle_message(request)

        assert response["error"]["code"] == METHOD_NOT_FOUND
        assert "nonexistent/method" in response["error"]["message"]

    @pytest.mark.asyncio
    async def test_missing_method_returns_error(self):
        server = _make_server()
        message = {"jsonrpc": "2.0", "id": 1}  # No method
        response = await server.handle_message(message)

        assert response["error"]["code"] == INVALID_REQUEST

    @pytest.mark.asyncio
    async def test_notification_no_response(self):
        """Messages without id (notifications) should return None."""
        server = _make_server()
        notification = {"jsonrpc": "2.0", "method": "notifications/cancelled"}
        response = await server.handle_message(notification)

        assert response is None

    @pytest.mark.asyncio
    async def test_unknown_notification_ignored(self):
        """Unknown methods without id are silently ignored."""
        server = _make_server()
        notification = {"jsonrpc": "2.0", "method": "unknown/notification"}
        response = await server.handle_message(notification)

        assert response is None

    @pytest.mark.asyncio
    async def test_non_dict_json_returns_invalid_request(self):
        """Non-object JSON payloads should return INVALID_REQUEST."""
        server = _make_server()
        response = await server.handle_message([1, 2, 3])
        assert response["error"]["code"] == INVALID_REQUEST

    @pytest.mark.asyncio
    async def test_malformed_frame_continues_serving(self):
        """A malformed frame sends an error response instead of crashing."""
        server = _make_server()

        # Malformed frame (missing content-length) followed by EOF
        malformed = b"Bad-Header: foo\r\n\r\n"

        reader = asyncio.StreamReader()
        reader.feed_data(malformed)
        reader.feed_eof()

        written_chunks = []
        writer = MagicMock()
        writer.write = MagicMock(side_effect=lambda data: written_chunks.append(data))
        writer.drain = AsyncMock()

        # Should not raise - server sends error and continues to EOF
        await server.run(reader=reader, writer=writer)

        # Should have written an error response for the malformed frame
        assert len(written_chunks) >= 1
        err_msg, _ = parse_message_from_bytes(written_chunks[0])
        assert err_msg is not None
        assert "error" in err_msg
        assert err_msg["error"]["code"] == INVALID_REQUEST

    @pytest.mark.asyncio
    async def test_content_length_exceeds_cap(self):
        """Content-Length exceeding MAX_CONTENT_LENGTH raises MCPReadError."""
        server = _make_server()
        huge_length = MAX_CONTENT_LENGTH + 1
        frame = f"Content-Length: {huge_length}\r\n\r\n".encode()

        reader = asyncio.StreamReader()
        reader.feed_data(frame)
        reader.feed_eof()

        with pytest.raises(MCPReadError) as exc_info:
            await server._read_message(reader)
        assert "exceeds" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_resources_read_non_string_uri(self):
        """resources/read with non-string URI returns INVALID_REQUEST."""
        server = _make_server()
        request = _make_request("resources/read", {"uri": 123})
        response = await server.handle_message(request)

        assert "error" in response
        assert response["error"]["code"] == INVALID_REQUEST

    def test_parse_message_malformed_body_consumes_frame(self):
        """Malformed JSON body should consume the frame, not leave it in the buffer."""
        # Build a frame with valid headers but invalid JSON body
        bad_body = b"\xff\xfe"  # invalid UTF-8
        header = f"Content-Length: {len(bad_body)}\r\n\r\n".encode()
        trailing_valid = _frame_message({"jsonrpc": "2.0", "method": "ping", "id": 1})
        data = header + bad_body + trailing_valid

        # First parse should return None but consume the bad frame
        parsed, remaining = parse_message_from_bytes(data)
        assert parsed is None
        assert remaining == trailing_valid

        # Second parse should succeed with the valid trailing message
        parsed2, remaining2 = parse_message_from_bytes(remaining)
        assert parsed2 is not None
        assert parsed2["method"] == "ping"
        assert remaining2 == b""

    @pytest.mark.asyncio
    async def test_handler_exception_returns_internal_error(self):
        """If a handler raises, we get an internal error response."""
        server = _make_server()

        # Monkey-patch a handler to raise
        async def _broken_handler(_msg):
            raise RuntimeError("Something went wrong")

        server._get_handler = lambda method: _broken_handler if method == "ping" else None

        request = _make_request("ping")
        response = await server.handle_message(request)

        assert response["error"]["code"] == INTERNAL_ERROR
        assert "Something went wrong" in response["error"]["message"]


# ---------------------------------------------------------------------------
# Resources handler tests
# ---------------------------------------------------------------------------


class TestResources:
    @pytest.mark.asyncio
    async def test_resources_list_no_adapter(self):
        server = _make_server()
        request = _make_request("resources/list")
        response = await server.handle_message(request)

        result = response["result"]
        assert "resources" in result
        # No adapter = empty resources
        assert result["resources"] == []

    @pytest.mark.asyncio
    async def test_resources_list_with_adapter(self):
        mock_adapter = MagicMock()
        mock_adapter.resources_list.return_value = [
            {"uri": "almanak://chains", "name": "Supported Chains"}
        ]
        server = _make_server(adapter=mock_adapter)
        request = _make_request("resources/list")
        response = await server.handle_message(request)

        resources = response["result"]["resources"]
        assert len(resources) == 1
        assert resources[0]["uri"] == "almanak://chains"

    @pytest.mark.asyncio
    async def test_resources_read_no_adapter(self):
        server = _make_server()
        request = _make_request("resources/read", {"uri": "almanak://chains"})
        response = await server.handle_message(request)

        assert response["result"]["contents"] == []

    @pytest.mark.asyncio
    async def test_resources_read_with_adapter(self):
        mock_adapter = MagicMock()
        mock_adapter.resources_read.return_value = {
            "contents": [{"uri": "almanak://chains", "text": "{}"}]
        }
        server = _make_server(adapter=mock_adapter)
        request = _make_request("resources/read", {"uri": "almanak://chains"})
        response = await server.handle_message(request)

        contents = response["result"]["contents"]
        assert len(contents) == 1


# ---------------------------------------------------------------------------
# End-to-end stdio loop test
# ---------------------------------------------------------------------------


class TestStdioLoop:
    @pytest.mark.asyncio
    async def test_full_round_trip(self):
        """Test reading a framed message, handling it, and writing back."""
        server = _make_server()

        # Prepare input: initialize + ping + EOF
        init_req = _make_request("initialize", {"protocolVersion": MCP_PROTOCOL_VERSION}, msg_id=1)
        ping_req = _make_request("ping", msg_id=2)

        input_data = _frame_message(init_req) + _frame_message(ping_req)

        reader = asyncio.StreamReader()
        reader.feed_data(input_data)
        reader.feed_eof()

        # Capture output
        written_chunks = []

        writer = MagicMock()
        writer.write = MagicMock(side_effect=lambda data: written_chunks.append(data))
        writer.drain = AsyncMock()

        await server.run(reader=reader, writer=writer)

        # Should have written 2 responses
        assert len(written_chunks) == 2

        # Parse first response (initialize)
        first_msg, _ = parse_message_from_bytes(written_chunks[0])
        assert first_msg is not None
        assert first_msg["id"] == 1
        assert first_msg["result"]["protocolVersion"] == MCP_PROTOCOL_VERSION

        # Parse second response (ping)
        second_msg, _ = parse_message_from_bytes(written_chunks[1])
        assert second_msg is not None
        assert second_msg["id"] == 2
        assert second_msg["result"] == {}

    @pytest.mark.asyncio
    async def test_graceful_eof(self):
        """Server exits cleanly on EOF."""
        server = _make_server()

        reader = asyncio.StreamReader()
        reader.feed_eof()

        writer = MagicMock()
        writer.write = MagicMock()
        writer.drain = AsyncMock()

        # Should not raise
        await server.run(reader=reader, writer=writer)
        writer.write.assert_not_called()
