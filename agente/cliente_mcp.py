"""Cliente MCP cru (JSON-RPC sobre Streamable HTTP) usado pelo agente como host.

Deliberadamente nao usa o `Client` do SDK com callback de elicitation: o agente
precisa enxergar o `resultType: input_required` sem tratamento para pausar a Task A2A
em vez de responder a pergunta sozinho.

Cada request e independente (spec 2026-07-28, sem sessao): leva no `_meta` a
versao do protocolo, as capabilities do cliente e o traceparent, e espelha nos
headers MCP-Protocol-Version, Mcp-Method e Mcp-Name o que vai no corpo.
"""

from __future__ import annotations

import itertools
import json
import logging
from typing import Any

import httpx

PROTOCOLO = "2026-07-28"
CAPABILITIES = {"elicitation": {"form": {}}}
CLIENT_INFO = {"name": "agente-central-de-salas", "version": "1.0.0"}

log = logging.getLogger("agente.mcp")


class ErroMCP(Exception):
    """Erro de protocolo (error de JSON-RPC) devolvido pelo servidor MCP."""

    def __init__(self, code: int, message: str) -> None:
        super().__init__(f"Erro MCP {code}: {message}")
        self.code = code


class ClienteMCP:
    def __init__(self, url: str) -> None:
        self.url = url
        self._http = httpx.AsyncClient(timeout=30)
        # Ids unicos no processo inteiro: o retry de um tools/call nunca reaproveita o id do request inicial.
        self._ids = itertools.count(1)

    async def fechar(self) -> None:
        await self._http.aclose()

    async def request(self, metodo: str, params: dict[str, Any] | None, traceparent: str) -> dict[str, Any]:
        params = dict(params or {})
        params["_meta"] = {
            "io.modelcontextprotocol/protocolVersion": PROTOCOLO,
            "io.modelcontextprotocol/clientInfo": CLIENT_INFO,
            "io.modelcontextprotocol/clientCapabilities": CAPABILITIES,
            "traceparent": traceparent,
        }
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "MCP-Protocol-Version": PROTOCOLO,
            "Mcp-Method": metodo,
        }
        if metodo == "tools/call":
            headers["Mcp-Name"] = params["name"]
        elif metodo == "resources/read":
            headers["Mcp-Name"] = params["uri"]
        corpo = {"jsonrpc": "2.0", "id": next(self._ids), "method": metodo, "params": params}
        log.info("-> %s id=%s traceparent=%s", metodo, corpo["id"], traceparent)
        resposta = await self._http.post(self.url, json=corpo, headers=headers)
        mensagem = self._decodificar(resposta)
        if "error" in mensagem:
            erro = mensagem["error"]
            raise ErroMCP(erro.get("code", 0), erro.get("message", ""))
        return mensagem.get("result") or {}

    @staticmethod
    def _decodificar(resposta: httpx.Response) -> dict[str, Any]:
        """Aceita as duas formas do Streamable HTTP: JSON direto ou um evento SSE."""
        if resposta.headers.get("content-type", "").startswith("text/event-stream"):
            for linha in resposta.text.splitlines():
                if linha.startswith("data:"):
                    return json.loads(linha[5:])
            raise ErroMCP(-32603, "resposta SSE sem evento de dados")
        return resposta.json()

    async def listar_tools(self, traceparent: str) -> list[dict[str, Any]]:
        return (await self.request("tools/list", {}, traceparent)).get("tools", [])

    async def ler_resource(self, uri: str, traceparent: str) -> str:
        contents = (await self.request("resources/read", {"uri": uri}, traceparent)).get("contents", [])
        return contents[0].get("text", "") if contents else ""

    async def chamar_tool(
        self,
        nome: str,
        argumentos: dict[str, Any],
        traceparent: str,
        input_responses: dict[str, Any] | None = None,
        request_state: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"name": nome, "arguments": argumentos}
        if input_responses is not None:
            params["inputResponses"] = input_responses
        if request_state is not None:
            params["requestState"] = request_state
        return await self.request("tools/call", params, traceparent)
