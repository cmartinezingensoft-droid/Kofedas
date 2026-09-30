"""Adaptador MCP SDK oficial para Kofedas ERP.

La logica de negocio y el contrato publico viven en ``kofedas_mcp``. Este
modulo solo traduce entre el SDK MCP y esa frontera.
"""
from __future__ import annotations

import json
import os
from typing import Any

import anyio
from mcp.server import Server, ServerRequestContext
from mcp.server.stdio import stdio_server
from mcp.types import (
    CallToolRequestParams,
    CallToolResult,
    ListToolsResult,
    PaginatedRequestParams,
    TextContent,
    Tool,
)

from kofedas_mcp import KofedasToolRuntime, SERVER_VERSION, tool_definitions

SERVER_NAME = "kofedas-mcp"
SERVER_TITLE = "Kofedas ERP MCP"
SERVER_DESCRIPTION = "Servidor MCP nativo para Kofedas ERP sobre ODBC/Firebird."


class KofedasSdkAdapter:
    """Une el SDK MCP oficial con el runtime de negocio de Kofedas."""

    def __init__(self, runtime: KofedasToolRuntime | None = None):
        self.runtime = runtime or KofedasToolRuntime()

    async def list_tools(
        self,
        ctx: ServerRequestContext,
        params: PaginatedRequestParams | None,
    ) -> ListToolsResult:
        del ctx, params
        tools = [
            Tool(
                name=definition["name"],
                description=definition.get("description", ""),
                input_schema=definition["inputSchema"],
            )
            for definition in tool_definitions(self.runtime.tool_profile)
        ]
        return ListToolsResult(tools=tools)

    async def call_tool(
        self,
        ctx: ServerRequestContext,
        params: CallToolRequestParams,
    ) -> CallToolResult:
        request_id = getattr(ctx, "request_id", None)
        result, is_error = self.runtime.invoke_tool(
            params.name,
            params.arguments or {},
            request_id=str(request_id) if request_id is not None else None,
        )
        return CallToolResult(
            content=[
                TextContent(
                    type="text",
                    text=json.dumps(result, ensure_ascii=False, indent=2),
                )
            ],
            is_error=is_error,
        )


def build_server(runtime: KofedasToolRuntime | None = None) -> Server:
    adapter = KofedasSdkAdapter(runtime)
    return Server(
        SERVER_NAME,
        version=SERVER_VERSION,
        title=SERVER_TITLE,
        description=SERVER_DESCRIPTION,
        on_list_tools=adapter.list_tools,
        on_call_tool=adapter.call_tool,
    )


async def run_stdio_async(server: Server | None = None) -> None:
    sdk_server = server or build_server()
    async with stdio_server() as (read_stream, write_stream):
        await sdk_server.run(
            read_stream,
            write_stream,
            sdk_server.create_initialization_options(),
        )


def build_streamable_http_app(server: Server | None = None):
    sdk_server = server or build_server()
    host = os.getenv("KOFEDAS_MCP_HTTP_HOST", "127.0.0.1")
    path = os.getenv("KOFEDAS_MCP_HTTP_PATH", "/mcp")
    stateless = os.getenv("KOFEDAS_MCP_HTTP_STATELESS", "true").lower() in {"1", "true", "yes", "si"}
    json_response = os.getenv("KOFEDAS_MCP_HTTP_JSON_RESPONSE", "true").lower() in {"1", "true", "yes", "si"}
    return sdk_server.streamable_http_app(
        streamable_http_path=path,
        stateless_http=stateless,
        json_response=json_response,
        host=host,
    )


def run_streamable_http(server: Server | None = None) -> None:
    try:
        import uvicorn
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("Instala 'uvicorn' para usar KOFEDAS_MCP_TRANSPORT=streamable-http") from exc

    host = os.getenv("KOFEDAS_MCP_HTTP_HOST", "127.0.0.1")
    port = int(os.getenv("KOFEDAS_MCP_HTTP_PORT", "8010"))
    uvicorn.run(
        build_streamable_http_app(server),
        host=host,
        port=port,
        log_level=os.getenv("KOFEDAS_MCP_HTTP_LOG_LEVEL", "info"),
    )


def main() -> None:
    transport = os.getenv("KOFEDAS_MCP_TRANSPORT", "stdio").strip().lower()
    server = build_server()
    if transport == "stdio":
        anyio.run(run_stdio_async, server)
        return
    if transport in {"streamable-http", "http"}:
        run_streamable_http(server)
        return
    raise RuntimeError("KOFEDAS_MCP_TRANSPORT no valido. Usa 'stdio' o 'streamable-http'.")


if __name__ == "__main__":
    main()
