"""Agente Central de Salas: servidor A2A v1.0 (JSON-RPC) por fora, host MCP por dentro.

Sem LLM: o pedido chega em formato fixo e o agente decide por regra. Ele traduz
protocolo, nao domina o negocio: conflito, politica e alternativas sao decisao
do servidor MCP.

A ponte esta em `Agente._executar_reserva` e `Agente._continuar`:
- um `input_required` do MCP vira TASK_STATE_INPUT_REQUIRED, e o `requestState`
  fica guardado em `Task.pendente`, nunca serializado para o cliente A2A;
- a continuacao `escolha=<valor>` vira o retry do tools/call original, com id
  JSON-RPC novo, `inputResponses` com a mesma chave e o `requestState` ecoado sem
  modificacao.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import secrets
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from cliente_mcp import ClienteMCP, ErroMCP

logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger("agente")

HOST = os.environ.get("AGENTE_HOST", "127.0.0.1")
PORTA = int(os.environ.get("AGENTE_PORT", "7300"))
URL_PUBLICA = os.environ.get("AGENTE_URL", f"http://127.0.0.1:{PORTA}").rstrip("/")
URL_MCP = os.environ.get("MCP_URL", "http://127.0.0.1:7301/mcp")

TOOL_RESERVA = "reservar_sala"
RESOURCE_POLITICA = "politica://uso"
TERMINAIS = {"TASK_STATE_COMPLETED", "TASK_STATE_CANCELED", "TASK_STATE_FAILED"}
FORMATO_PEDIDO = "reservar sala=<id> inicio=<iso8601> fim=<iso8601> responsavel=<nome>"
TRACEPARENT = re.compile(r"^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$")

# Codigos de erro do A2A v1.0 (binding JSON-RPC)
TASK_NOT_FOUND = -32001
UNSUPPORTED_OPERATION = -32004

AGENT_CARD = {
    "name": "Central de Salas",
    "description": "Reserva salas de reuniao da Hill Valley Tech.",
    "provider": {"organization": "Hill Valley Tech", "url": "https://hillvalley.example"},
    "version": "1.0.0",
    "supportedInterfaces": [
        {"url": f"{URL_PUBLICA}/a2a", "protocolBinding": "JSONRPC", "protocolVersion": "1.0"},
    ],
    "capabilities": {"streaming": False, "pushNotifications": False, "extendedAgentCard": False},
    "defaultInputModes": ["text/plain"],
    "defaultOutputModes": ["text/plain"],
    "skills": [
        {
            "id": "reservar-sala",
            "name": "Reservar sala",
            "description": "Reserva uma sala em um intervalo. Se houver conflito, pergunta qual alternativa usar.",
            "tags": ["salas", "agenda"],
            "inputModes": ["text/plain"],
            "outputModes": ["text/plain"],
            "examples": [
                "reservar sala=sala-garagem inicio=2026-11-03T14:00:00-03:00 fim=2026-11-03T15:00:00-03:00 responsavel=Marty"
            ],
        }
    ],
}


def novo_id(prefixo: str) -> str:
    return f"{prefixo}-{secrets.token_hex(6)}"


def traceparent_da_task(cabecalho: str | None) -> str:
    """Mantem o trace-id do cliente A2A e gera um span-id novo para o lado MCP."""
    casado = TRACEPARENT.match((cabecalho or "").strip().lower())
    trace_id = casado.group(2) if casado else secrets.token_hex(16)
    flags = casado.group(4) if casado else "01"
    return f"00-{trace_id}-{secrets.token_hex(8)}-{flags}"


def texto_da_mensagem(mensagem: dict[str, Any]) -> str:
    return " ".join(p.get("text", "") for p in mensagem.get("parts", []) if isinstance(p, dict)).strip()


def campos_do_pedido(texto: str) -> dict[str, str] | None:
    """`reservar chave=valor ...` -> {chave: valor}; None se nao for um pedido de reserva."""
    partes = texto.split()
    if not partes or partes[0] != "reservar":
        return None
    campos: dict[str, str] = {}
    for parte in partes[1:]:
        chave, igual, valor = parte.partition("=")
        if not igual or not valor:
            return None
        campos[chave] = valor
    return campos


@dataclass
class Pendente:
    """Estado da pausa: o que e preciso para repetir o tools/call. Nunca sai do agente."""

    tool: str
    argumentos: dict[str, Any]
    chave: str
    campo: str
    opcoes: list[str]
    request_state: str


@dataclass
class Task:
    id: str
    context_id: str
    traceparent: str
    estado: str = "TASK_STATE_SUBMITTED"
    mensagem_status: dict[str, Any] | None = None
    historico: list[dict[str, Any]] = field(default_factory=list)
    artifacts: list[dict[str, Any]] = field(default_factory=list)
    politica: str | None = None
    pendente: Pendente | None = None
    trava: asyncio.Lock = field(default_factory=asyncio.Lock)

    def transitar(self, estado: str, texto: str | None = None) -> None:
        if self.estado in TERMINAIS:
            raise RuntimeError(f"Task {self.id} ja esta em estado terminal {self.estado}")
        self.estado = estado
        if texto is not None:
            mensagem = {
                "messageId": novo_id("msg"),
                "role": "ROLE_AGENT",
                "parts": [{"text": texto}],
                "taskId": self.id,
                "contextId": self.context_id,
            }
            self.mensagem_status = mensagem
            self.historico.append(mensagem)
        log.info("task=%s estado=%s%s", self.id, estado, f" mensagem={texto!r}" if texto else "")

    def para_a2a(self) -> dict[str, Any]:
        """Forma publica da Task. `pendente` (e o requestState) fica de fora por construcao."""
        status: dict[str, Any] = {"state": self.estado}
        if self.mensagem_status is not None:
            status["message"] = self.mensagem_status
        return {
            "id": self.id,
            "contextId": self.context_id,
            "status": status,
            "history": self.historico,
            "artifacts": self.artifacts,
        }


class ErroA2A(Exception):
    def __init__(self, code: int, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class Agente:
    def __init__(self, mcp: ClienteMCP) -> None:
        self.mcp = mcp
        self.tasks: dict[str, Task] = {}

    # ----------------------------------------------------------------- A2A

    async def send_message(self, params: dict[str, Any], traceparent: str | None) -> dict[str, Any]:
        mensagem = params.get("message")
        if not isinstance(mensagem, dict) or not isinstance(mensagem.get("parts"), list):
            raise ErroA2A(-32602, "params.message com parts e obrigatorio")
        task_id = mensagem.get("taskId")
        if task_id:
            task = self._task(task_id)
            async with task.trava:
                if task.estado in TERMINAIS:
                    raise ErroA2A(UNSUPPORTED_OPERATION, f"Task {task_id} esta em estado terminal {task.estado}")
                if task.estado != "TASK_STATE_INPUT_REQUIRED" or task.pendente is None:
                    raise ErroA2A(UNSUPPORTED_OPERATION, f"Task {task_id} nao esta aguardando resposta")
                if traceparent:
                    task.traceparent = traceparent_da_task(traceparent)
                task.historico.append(mensagem)
                await self._continuar(task, texto_da_mensagem(mensagem))
        else:
            task = Task(
                id=novo_id("task"),
                context_id=mensagem.get("contextId") or novo_id("ctx"),
                traceparent=traceparent_da_task(traceparent),
            )
            self.tasks[task.id] = task
            log.info("task=%s estado=%s", task.id, task.estado)
            async with task.trava:
                task.historico.append(mensagem)
                await self._iniciar(task, texto_da_mensagem(mensagem))
        return {"task": task.para_a2a()}

    async def get_task(self, params: dict[str, Any]) -> dict[str, Any]:
        task_id = params.get("id")
        if not task_id:
            raise ErroA2A(-32602, "params.id e obrigatorio")
        return {"task": self._task(task_id).para_a2a()}

    def _task(self, task_id: str) -> Task:
        task = self.tasks.get(task_id)
        if task is None:
            raise ErroA2A(TASK_NOT_FOUND, f"Task {task_id} nao encontrada")
        return task

    # ----------------------------------------------------------- a ponte

    async def _iniciar(self, task: Task, texto: str) -> None:
        task.transitar("TASK_STATE_WORKING")
        campos = campos_do_pedido(texto)
        if campos is None:
            task.transitar("TASK_STATE_FAILED", f"Pedido fora do formato. Use: {FORMATO_PEDIDO}")
            return
        try:
            # Descoberta em runtime: a tool e o schema dela vem do tools/list, nao do codigo.
            tools = {t["name"]: t for t in await self.mcp.listar_tools(task.traceparent)}
            tool = tools.get(TOOL_RESERVA)
            if tool is None:
                task.transitar("TASK_STATE_FAILED", f"O servidor MCP nao oferece a tool {TOOL_RESERVA}")
                return
            schema = tool.get("inputSchema") or {}
            faltando = [c for c in schema.get("required", []) if c not in campos]
            if faltando:
                task.transitar("TASK_STATE_FAILED", f"Pedido incompleto, faltam: {', '.join(faltando)}. Use: {FORMATO_PEDIDO}")
                return
            argumentos = {c: campos[c] for c in schema.get("properties", {}) if c in campos}

            politica = await self.mcp.ler_resource(RESOURCE_POLITICA, task.traceparent)
            primeira = politica.splitlines()[0] if politica else ""
            task.politica = primeira.split(":", 1)[1].strip() if ":" in primeira else None

            resultado = await self.mcp.chamar_tool(TOOL_RESERVA, argumentos, task.traceparent)
        except (ErroMCP, OSError, ValueError) as exc:
            task.transitar("TASK_STATE_FAILED", str(exc))
            return
        self._aplicar_resultado(task, TOOL_RESERVA, argumentos, resultado)

    async def _continuar(self, task: Task, texto: str) -> None:
        pendente = task.pendente
        assert pendente is not None
        chave, igual, valor = texto.partition("=")
        if chave.strip() != "escolha" or not igual or (valor.strip() != "recusar" and valor.strip() not in pendente.opcoes):
            # Escolha invalida: continua pausada e repete a mesma linha.
            task.transitar("TASK_STATE_INPUT_REQUIRED", self._linha_alternativas(pendente))
            return
        valor = valor.strip()
        resposta = (
            {"action": "decline"}
            if valor == "recusar"
            else {"action": "accept", "content": {pendente.campo: valor}}
        )
        task.transitar("TASK_STATE_WORKING")
        task.pendente = None
        try:
            # O retry: mesmo tools/call, id JSON-RPC novo (gerado pelo cliente), a mesma
            # chave do inputRequests e o requestState ecoado exatamente como veio.
            resultado = await self.mcp.chamar_tool(
                pendente.tool,
                pendente.argumentos,
                task.traceparent,
                input_responses={pendente.chave: resposta},
                request_state=pendente.request_state,
            )
        except (ErroMCP, OSError, ValueError) as exc:
            task.transitar("TASK_STATE_FAILED", str(exc))
            return
        self._aplicar_resultado(task, pendente.tool, pendente.argumentos, resultado)

    def _aplicar_resultado(self, task: Task, tool: str, argumentos: dict[str, Any], resultado: dict[str, Any]) -> None:
        """Traduz o resultado MCP em estado da Task A2A."""
        if resultado.get("resultType") == "input_required":
            pendente = self._pendente_de(tool, argumentos, resultado)
            if pendente is None:
                task.transitar("TASK_STATE_FAILED", "O servidor MCP pediu uma informacao que este agente nao sabe repassar")
                return
            task.pendente = pendente
            task.transitar("TASK_STATE_INPUT_REQUIRED", self._linha_alternativas(pendente))
            return

        texto = " ".join(c.get("text", "") for c in resultado.get("content", []) if c.get("type") == "text")
        if resultado.get("isError"):
            task.transitar("TASK_STATE_FAILED", texto)
            return
        dados = resultado.get("structuredContent") or {}
        if dados.get("reservado") is False:
            task.transitar("TASK_STATE_CANCELED", f"Reserva nao realizada: {dados.get('motivo') or 'recusada'}")
            return
        reserva = {
            "reserva": dados.get("reserva"),
            "sala": dados.get("sala"),
            "inicio": dados.get("inicio"),
            "fim": dados.get("fim"),
            "responsavel": dados.get("responsavel"),
            "politica": task.politica,
        }
        task.artifacts.append(
            {
                "artifactId": novo_id("art"),
                "name": "reserva",
                "parts": [{"text": json.dumps(reserva, ensure_ascii=False, separators=(",", ":"))}],
            }
        )
        task.transitar("TASK_STATE_COMPLETED", f"Reserva {reserva['reserva']} confirmada na {reserva['sala']}.")

    @staticmethod
    def _pendente_de(tool: str, argumentos: dict[str, Any], resultado: dict[str, Any]) -> Pendente | None:
        """Extrai a pergunta (elicitation em form mode, um campo) e guarda o requestState opaco."""
        pedidos = resultado.get("inputRequests") or {}
        request_state = resultado.get("requestState")
        if len(pedidos) != 1 or not isinstance(request_state, str):
            return None
        chave, pedido = next(iter(pedidos.items()))
        params = pedido.get("params") or {}
        propriedades = (params.get("requestedSchema") or {}).get("properties") or {}
        if pedido.get("method") != "elicitation/create" or params.get("mode", "form") != "form" or len(propriedades) != 1:
            return None
        campo, schema_campo = next(iter(propriedades.items()))
        opcoes = schema_campo.get("enum") or ([schema_campo["const"]] if "const" in schema_campo else [])
        if not opcoes:
            return None
        return Pendente(tool, argumentos, chave, campo, [str(o) for o in opcoes], request_state)

    @staticmethod
    def _linha_alternativas(pendente: Pendente) -> str:
        return "alternativas: " + ", ".join(pendente.opcoes)


# --------------------------------------------------------------------- HTTP


def resposta_jsonrpc(id_: Any, *, result: Any = None, error: dict[str, Any] | None = None) -> JSONResponse:
    corpo: dict[str, Any] = {"jsonrpc": "2.0", "id": id_}
    if error is not None:
        corpo["error"] = error
    else:
        corpo["result"] = result
    return JSONResponse(corpo)


def criar_app() -> Starlette:
    mcp = ClienteMCP(URL_MCP)
    agente = Agente(mcp)

    async def agent_card(_: Request) -> JSONResponse:
        return JSONResponse(AGENT_CARD)

    async def a2a(request: Request) -> JSONResponse:
        try:
            corpo = await request.json()
        except ValueError:
            return resposta_jsonrpc(None, error={"code": -32700, "message": "JSON invalido"})
        if not isinstance(corpo, dict) or corpo.get("jsonrpc") != "2.0" or not isinstance(corpo.get("method"), str):
            return resposta_jsonrpc(None, error={"code": -32600, "message": "Request JSON-RPC invalido"})
        id_, metodo, params = corpo.get("id"), corpo["method"], corpo.get("params") or {}
        traceparent = request.headers.get("traceparent")
        log.info("a2a method=%s id=%s traceparent=%s", metodo, id_, traceparent or "-")
        try:
            if metodo == "SendMessage":
                return resposta_jsonrpc(id_, result=await agente.send_message(params, traceparent))
            if metodo == "GetTask":
                return resposta_jsonrpc(id_, result=await agente.get_task(params))
            return resposta_jsonrpc(id_, error={"code": -32601, "message": f"Metodo nao suportado: {metodo}"})
        except ErroA2A as exc:
            return resposta_jsonrpc(id_, error={"code": exc.code, "message": exc.message})

    @asynccontextmanager
    async def ciclo(_: Starlette):
        yield
        await mcp.fechar()

    return Starlette(
        routes=[
            Route("/.well-known/agent-card.json", agent_card, methods=["GET"]),
            Route("/a2a", a2a, methods=["POST"]),
        ],
        lifespan=ciclo,
    )


def main() -> None:
    log.info("agente em %s, servidor MCP em %s", URL_PUBLICA, URL_MCP)
    uvicorn.run(criar_app(), host=HOST, port=PORTA, log_level="warning")


if __name__ == "__main__":
    main()
