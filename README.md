# A Ponte: agente A2A com MCP por dentro

Dois processos independentes:

- **`servidor-mcp/servidor.py`**: servidor MCP (SDK oficial `mcp` 2.3.0, spec 2026-07-28) em Streamable HTTP stateless, na porta 7301, em `/mcp`. Expõe as tools `listar_salas`, `consultar_disponibilidade` e `reservar_sala` e o resource `politica://uso`. A reserva com conflito faz o ciclo completo de MRTR.
- **`agente/agente.py`**: agente A2A v1.0 (binding JSON-RPC 2.0) na porta 7300, com o card em `/.well-known/agent-card.json` e o endpoint em `/a2a`. Por dentro ele é host MCP e fala por HTTP com o servidor acima, usando o cliente cru de `agente/cliente_mcp.py`. Não usa LLM: o pedido tem formato fixo e a decisão é por regra.

## Como rodar

Requisitos: [uv](https://docs.astral.sh/uv/) e Python 3.12 ou superior. As versões estão travadas em `pyproject.toml` e `uv.lock`.

```bash
git clone <url-do-fork> desafio-a2a-com-mcp && cd desafio-a2a-com-mcp
uv sync --locked
```

Gere a chave de integridade do `requestState`. Ela é obrigatória e precisa ter no mínimo 32 bytes; o servidor se recusa a subir sem ela. Nunca a versione:

```bash
export REQUEST_STATE_SECRET=$(python3 -c "import secrets; print(secrets.token_hex(32))")
```

Terminal 1, servidor MCP (o stderr mostra método, id e traceparent de cada request):

```bash
export REQUEST_STATE_SECRET=<o mesmo valor gerado acima>
uv run python servidor-mcp/servidor.py
```

Terminal 2, agente:

```bash
uv run python agente/agente.py
```

Terminal 3, validador (com os dois processos recém-iniciados):

```bash
python3 validador/validar.py --agente http://localhost:7300 --mcp http://localhost:7301
```

Variáveis opcionais, com os valores padrão entre parênteses: `MCP_HOST` (`127.0.0.1`) e `MCP_PORT` (`7301`) para o servidor; `AGENTE_HOST` (`127.0.0.1`), `AGENTE_PORT` (`7300`), `AGENTE_URL` (URL publicada no card, `http://127.0.0.1:7300`) e `MCP_URL` (`http://127.0.0.1:7301/mcp`) para o agente.

Para o teste de restart do roteiro do avaliador, reinicie o servidor MCP com o **mesmo** `REQUEST_STATE_SECRET`. Um `requestState` emitido antes continua válido, porque o estado viaja dentro dele.

## Onde a ponte acontece

A ponte fica em `agente/agente.py`. Para cada Task nova, `Agente._iniciar` (linha 223) faz `tools/list` (descoberta em runtime: o schema da tool vem do servidor), lê `politica://uso` para extrair a versão e chama `reservar_sala`. O resultado cru vai para `Agente._aplicar_resultado` (linha 284). Ali, um `resultType: input_required` do MCP **vira `TASK_STATE_INPUT_REQUIRED`** (linhas 291 e 292): a chave do `inputRequests`, as opções do `enum` e o `requestState` opaco são guardados em `Task.pendente` (dataclass `Pendente`, linha 112), e a Task devolve exatamente `alternativas: <ids>`. `Task.para_a2a` (linha 152) serializa a Task sem o campo `pendente`, então o `requestState` nunca sai do agente. Quando chega o `SendMessage` com `taskId` e `escolha=<valor>`, `Agente._continuar` (linha 253) **devolve o `requestState` ao servidor** (linhas 272 a 278): ele repete o `tools/call` com os mesmos argumentos, `inputResponses` com a mesma chave e o `requestState` ecoado sem modificação. O id JSON-RPC é novo porque cada request sai de um contador único do processo (`agente/cliente_mcp.py`, linha 64). `escolha=recusar` vira `action: decline` e termina em `TASK_STATE_CANCELED`. Uma escolha fora do enum mantém a Task pausada e repete a lista.

Do lado do servidor, o MRTR é o resolver `escolha_de_sala` (`servidor-mcp/servidor.py`, linha 209). Ele devolve `None` quando a sala está livre e `Elicit(...)` com um schema plano (`sala` restrita às alternativas) quando há conflito. O SDK transforma isso em `input_required`. No retry, o resolver roda de novo e o SDK injeta a resposta do `inputResponses`. Não existe canal de volta nem chamada aberta esperando.

## Decisões técnicas

- **Proteção do `requestState`:** é o `RequestStateSecurity` do SDK (`servidor-mcp/servidor.py`, linha 164), com AES-256-GCM e chave derivada por HKDF-SHA256 de `REQUEST_STATE_SECRET`. O conteúdo é cifrado e autenticado, então trocar um caractere falha na verificação da tag e o servidor responde `-32602` (`Invalid or expired requestState`). Por cima do selo, o envelope do SDK amarra o token ao método, ao nome da tool, a um digest dos argumentos e à audiência (o nome do servidor). Por isso um retry com argumentos adulterados é **rejeitado** com `-32602`, em vez de reservar com os valores alterados. A chave vem só do ambiente: sem ela, ou com menos de 32 bytes, o processo não sobe.
- **Validade:** 10 minutos (`VALIDADE_REQUEST_STATE`), dentro da faixa de 5 a 30 pedida. A expiração fica no envelope selado (`exp`).
- **Sem estado entre rodadas no servidor:** nada é guardado entre o `input_required` e o retry. O que precisa sobreviver (as perguntas feitas e o digest dos argumentos) viaja no `requestState`, e por isso um retry funciona depois de reiniciar o processo, desde que a chave seja a mesma.
- **Capability e _meta:** o SDK rejeita com `-32602` e HTTP 400 um request sem `protocolVersion` ou sem `clientCapabilities` no `_meta`. Também responde `-32021` com `data.requiredCapabilities` e HTTP 400 quando o resolver precisa perguntar e o cliente não declarou `elicitation.form`. O agente envia `{"elicitation": {"form": {}}}` em todo request, junto com os headers `MCP-Protocol-Version`, `Mcp-Method` e `Mcp-Name`.
- **Estado das Tasks:** fica em memória no processo do agente, num `dict` `task_id → Task`. Cada Task tem um `asyncio.Lock` e o seu próprio `Pendente`, então duas Tasks pausadas ao mesmo tempo nunca trocam de `requestState`. Estado terminal é definitivo: um `SendMessage` para uma Task `COMPLETED`, `CANCELED` ou `FAILED` recebe o erro JSON-RPC `-32004`.
- **traceparent:** o agente lê o header `traceparent` da chamada A2A, mantém o trace-id, gera um span-id novo e envia no `_meta` de todos os requests MCP daquela Task. O servidor registra no stderr o método, o id, a tool ou URI e o traceparent de cada request, inclusive dos retries rejeitados, porque o log roda antes da verificação do `requestState`.
- **Cliente MCP cru no agente:** o cliente do SDK com callback de elicitation responderia a pergunta sozinho. Um cliente JSON-RPC mínimo sobre `httpx` deixa o `input_required` visível para virar a pausa A2A.
- **Erros da tool:** um `isError: true` termina a Task em `TASK_STATE_FAILED` com o texto da tool, sem alteração, na mensagem de status e no histórico. O SDK acrescenta o prefixo `Error executing tool reservar_sala:`, aceito pelo enunciado.
- **Reservas:** ficam em memória e são carregadas de `dados/reservas.json` quando o processo sobe. Não persistem depois de um restart, como prevê o escopo.

## Saída do validador

Última execução, com os dois processos recém-iniciados:

```
trace-id desta execucao: 2ffe8a2eb07e067b3aa88742f42b7cb7
procure esse valor no stderr do servidor MCP para conferir a propagacao do traceparent.

PASS 01 tools/list traz as tres tools
PASS 02 toda tool tem inputSchema de objeto
PASS 03 listar_salas devolve structuredContent e o mesmo JSON em texto
PASS 04 _meta sem protocolVersion devolve -32602 e HTTP 400
PASS 05 _meta sem clientCapabilities devolve -32602 e HTTP 400
PASS 06 tool inexistente e recusada, por -32602 ou por isError
PASS 07 resources/read de politica://uso devolve a politica
PASS 08 resources/read de URI inexistente devolve -32602
PASS 09 sala inexistente devolve isError com a mensagem exata
PASS 10 fora da janela devolve isError com a mensagem exata
PASS 11 duracao acima de 2h devolve isError com a mensagem exata
PASS 12 intervalo invertido devolve isError com a mensagem exata
PASS 13 conflito devolve input_required com inputRequests e requestState
PASS 14 a elicitation e form mode e oferece as alternativas na ordem certa
PASS 15 conflito sem a capability elicitation devolve -32021 e HTTP 400
PASS 16 retry com inputResponses e requestState conclui a reserva
PASS 17 requestState adulterado e rejeitado com -32602
PASS 18 argumentos adulterados no retry nao tomam efeito
PASS 19 recusa conclui sem reservar e sem isError
PASS 20 conflito sem alternativa possivel devolve isError com a mensagem exata

PASS 21 agent card responde 200 no well-known com JSON
PASS 22 o card declara a interface JSON-RPC com url e versao 1.0
PASS 23 o card declara a skill reservar-sala
PASS 24 SendMessage com sala livre conclui a Task
PASS 25 o artifact chama reserva e traz a versao da politica
PASS 26 GetTask devolve id, contextId e estado corrente
PASS 27 SendMessage com sala ocupada pausa a Task
PASS 28 a Task pausada lista as alternativas na ordem certa
PASS 29 escolha fora do enum mantem a Task pausada
PASS 30 a continuacao conclui a Task na sala escolhida
PASS 31 SendMessage em Task terminal e recusado
PASS 32 a recusa termina a Task em CANCELED
PASS 33 duas Tasks pausadas ao mesmo tempo concluem cada uma com a sua reserva
PASS 34 nenhuma resposta A2A carrega o requestState
PASS 35 sala inexistente termina a Task em FAILED com a mensagem da tool
PASS 36 o agente e deterministico: o mesmo pedido produz a mesma pausa

resumo: 36 passaram, 0 falharam, de 36 verificacoes
```
