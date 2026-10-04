"""Servidor MCP da Central de Salas (Streamable HTTP, stateless, spec 2026-07-28).

Expoe tres tools (listar_salas, consultar_disponibilidade, reservar_sala) e o
resource politica://uso. A reserva com conflito usa MRTR: o resolver
`escolha_de_sala` devolve um `Elicit`, o SDK transforma isso em
`resultType: input_required` e sela o `requestState` com a chave de
REQUEST_STATE_SECRET. Nada fica em memoria entre o input_required e o retry.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Annotated, Any, Literal

import uvicorn
from pydantic import BaseModel, Field, create_model

from mcp.server.context import CallNext, HandlerResult, ServerRequestContext
from mcp.server.elicitation import AcceptedElicitation, ElicitationResult
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.server.mcpserver.resolve import Elicit, Resolve
from mcp.server.request_state import RequestStateSecurity

DADOS = Path(__file__).resolve().parent.parent / "dados"
FUSO = timezone(timedelta(hours=-3))
ABERTURA, FECHAMENTO = time(8, 0), time(20, 0)
DURACAO_MAXIMA = timedelta(hours=2)
VALIDADE_REQUEST_STATE = 10 * 60  # segundos; o enunciado pede entre 5 e 30 minutos

logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger("servidor-mcp")


# ---------------------------------------------------------------------------
# Dominio: dados do starter, reservas em memoria
# ---------------------------------------------------------------------------

SALAS: list[dict[str, Any]] = json.loads((DADOS / "salas.json").read_text(encoding="utf-8"))
RESERVAS: list[dict[str, Any]] = json.loads((DADOS / "reservas.json").read_text(encoding="utf-8"))
POLITICA_TEXTO = (DADOS / "politica-de-uso.md").read_text(encoding="utf-8")
POLITICA_VERSAO = POLITICA_TEXTO.splitlines()[0].split(":", 1)[1].strip()


def sala_por_id(sala: str) -> dict[str, Any]:
    for s in SALAS:
        if s["id"] == sala:
            return s
    raise ToolError(f"Sala inexistente: {sala}")


def instante(valor: str) -> datetime:
    try:
        dt = datetime.fromisoformat(valor)
    except ValueError:
        raise ToolError("Intervalo invalido: fim deve ser posterior a inicio") from None
    return dt if dt.tzinfo else dt.replace(tzinfo=FUSO)


def validar(sala: str, inicio: str, fim: str) -> tuple[dict[str, Any], datetime, datetime]:
    """Regras da politica, compartilhadas por consulta e reserva."""
    dados_sala = sala_por_id(sala)
    ini, fi = instante(inicio), instante(fim)
    if fi <= ini:
        raise ToolError("Intervalo invalido: fim deve ser posterior a inicio")
    ini_local, fim_local = ini.astimezone(FUSO), fi.astimezone(FUSO)
    if (
        ini_local.timetz().replace(tzinfo=None) < ABERTURA
        or ini_local.timetz().replace(tzinfo=None) > FECHAMENTO
        or fim_local.date() != ini_local.date()
        or fim_local.timetz().replace(tzinfo=None) > FECHAMENTO
    ):
        raise ToolError("Fora da janela de uso: a politica permite reservas entre 08:00 e 20:00")
    if fi - ini > DURACAO_MAXIMA:
        raise ToolError("Duracao acima do limite: a politica permite no maximo 2 horas")
    return dados_sala, ini, fi


def conflitos(sala: str, ini: datetime, fi: datetime) -> list[dict[str, Any]]:
    return [
        r
        for r in RESERVAS
        if r["sala"] == sala and instante(r["inicio"]) < fi and ini < instante(r["fim"])
    ]


def alternativas(pedida: dict[str, Any], ini: datetime, fi: datetime) -> list[str]:
    """Salas livres com capacidade >= a pedida, por (capacidade, id), no maximo tres."""
    candidatas = [
        s
        for s in SALAS
        if s["id"] != pedida["id"] and s["capacidade"] >= pedida["capacidade"] and not conflitos(s["id"], ini, fi)
    ]
    candidatas.sort(key=lambda s: (s["capacidade"], s["id"]))
    return [s["id"] for s in candidatas[:3]]


def proximo_id() -> str:
    maior = max((int(r["id"].split("-")[1]) for r in RESERVAS), default=0)
    return f"res-{maior + 1:04d}"


# ---------------------------------------------------------------------------
# Schemas de saida
# ---------------------------------------------------------------------------


class SalaOut(BaseModel):
    id: str
    nome: str
    capacidade: int
    recursos: list[str]


class ListaDeSalas(BaseModel):
    salas: list[SalaOut]


class ConflitoOut(BaseModel):
    id: str
    inicio: str
    fim: str
    responsavel: str


class Disponibilidade(BaseModel):
    sala: str
    livre: bool
    conflitos: list[ConflitoOut]


class ReservaOut(BaseModel):
    reserva: str | None = None
    reservado: bool = True
    sala: str | None = None
    inicio: str | None = None
    fim: str | None = None
    responsavel: str | None = None
    politica: str | None = None
    motivo: str | None = None


# ---------------------------------------------------------------------------
# Servidor
# ---------------------------------------------------------------------------


def seguranca_do_request_state() -> RequestStateSecurity:
    segredo = os.environ.get("REQUEST_STATE_SECRET", "")
    try:
        material = bytes.fromhex(segredo)
    except ValueError:
        material = segredo.encode()
    if len(material) < 32:
        sys.exit(
            "REQUEST_STATE_SECRET ausente ou com menos de 32 bytes. Gere com: "
            'python3 -c "import secrets; print(secrets.token_hex(32))"'
        )
    return RequestStateSecurity(keys=[material], ttl=VALIDADE_REQUEST_STATE)


async def registrar_request(ctx: ServerRequestContext[Any, Any], call_next: CallNext) -> HandlerResult:
    """Registra no stderr metodo, id e traceparent de cada request."""
    meta = (ctx.params or {}).get("_meta") or {}
    alvo = (ctx.params or {}).get("name") or (ctx.params or {}).get("uri") or ""
    retry = " retry" if (ctx.params or {}).get("requestState") else ""
    log.info(
        "request method=%s id=%s name=%s traceparent=%s%s",
        ctx.method,
        ctx.request_id,
        alvo or "-",
        meta.get("traceparent", "-"),
        retry,
    )
    return await call_next(ctx)


mcp = MCPServer(
    name="central-de-salas",
    version="1.0.0",
    request_state_security=seguranca_do_request_state(),
)
# Primeiro da cadeia: registra o request antes da verificacao do requestState,
# para que retries rejeitados tambem aparecam no stderr.
mcp.middleware.insert(0, registrar_request)


@mcp.tool(description="Lista todas as salas com capacidade e recursos.")
def listar_salas() -> ListaDeSalas:
    return ListaDeSalas(salas=[SalaOut(**s) for s in SALAS])


@mcp.tool(description="Diz se uma sala esta livre no intervalo, e quais reservas conflitam.")
def consultar_disponibilidade(sala: str, inicio: str, fim: str) -> Disponibilidade:
    _, ini, fi = validar(sala, inicio, fim)
    em_conflito = conflitos(sala, ini, fi)
    return Disponibilidade(
        sala=sala,
        livre=not em_conflito,
        conflitos=[ConflitoOut(id=r["id"], inicio=r["inicio"], fim=r["fim"], responsavel=r["responsavel"]) for r in em_conflito],
    )


def escolha_de_sala(sala: str, inicio: str, fim: str) -> Elicit[Any] | None:
    """Resolver do MRTR: None quando a sala esta livre, Elicit quando ha conflito.

    Roda de novo a cada rodada; a resposta do cliente chega via inputResponses e e
    validada contra o mesmo schema (mesmas alternativas) que foi perguntado.
    """
    pedida, ini, fi = validar(sala, inicio, fim)
    if not conflitos(sala, ini, fi):
        return None
    opcoes = alternativas(pedida, ini, fi)
    if not opcoes:
        raise ToolError("Sem alternativas disponiveis no intervalo")
    escolha = create_model(
        "EscolhaDeSala",
        sala=(Literal[tuple(opcoes)], Field(title="Sala", description="Sala alternativa escolhida")),
    )
    return Elicit("A sala pedida esta ocupada nesse intervalo. Escolha uma alternativa.", escolha)


@mcp.tool(description="Reserva uma sala. Se o intervalo estiver ocupado, pergunta qual alternativa usar.")
def reservar_sala(
    sala: str,
    inicio: str,
    fim: str,
    responsavel: str,
    escolha: Annotated[ElicitationResult[Any], Resolve(escolha_de_sala)],
) -> ReservaOut:
    if not isinstance(escolha, AcceptedElicitation):
        return ReservaOut(reservado=False, motivo=f"Usuario recusou as alternativas ({escolha.action})")
    destino = escolha.data.sala if escolha.data is not None else sala
    reserva = {"id": proximo_id(), "sala": destino, "inicio": inicio, "fim": fim, "responsavel": responsavel}
    RESERVAS.append(reserva)
    log.info("reserva criada %s", json.dumps(reserva, ensure_ascii=False))
    return ReservaOut(
        reserva=reserva["id"],
        reservado=True,
        sala=destino,
        inicio=inicio,
        fim=fim,
        responsavel=responsavel,
        politica=POLITICA_VERSAO,
    )


@mcp.resource("politica://uso", name="politica-de-uso", description="Politica de uso das salas.", mime_type="text/markdown")
def politica_de_uso() -> str:
    return POLITICA_TEXTO


def main() -> None:
    host = os.environ.get("MCP_HOST", "127.0.0.1")
    porta = int(os.environ.get("MCP_PORT", "7301"))
    app = mcp.streamable_http_app(streamable_http_path="/mcp", stateless_http=True, json_response=True, host=host)
    uvicorn.run(app, host=host, port=porta, log_level="warning")


if __name__ == "__main__":
    main()
