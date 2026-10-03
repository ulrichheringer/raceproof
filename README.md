# raceproof

`raceproof` é uma biblioteca Python para testar concorrência em backends.
Executa operações assíncronas em paralelo, verifica invariantes de domínio e
registra uma timeline da execução. Suporta HTTP, ASGI e pytest, com checkpoints
para sincronização e redução do número de workers em cenários que falham.

Python 3.11+. Licença MIT.

## Instalação

Na raiz do repositório:

```sh
python -m pip install -e '.[http,pytest]' pytest-asyncio
```

Os extras `http` e `pytest` instalam, respectivamente, httpx e pytest.

## Uso

`run()` recebe uma função assíncrona sem argumentos e executa uma cópia por worker:

```python
import raceproof

result = await raceproof.run(
    lambda: client.post("/coupons/123/redeem"),
    concurrency=20,
    seed=42,
    timeout=5,
)
result.assert_successes(max=1)
```

`client` pode ser um `httpx.AsyncClient`. Para ASGI, use
`async with raceproof.asgi_client(app) as client`. O helper não gerencia o
lifespan da aplicação.

Respostas HTTP 2xx e retornos sem exceção contam como sucesso. Exceções das
operações ficam registradas no resultado. `assert_successes()` verifica a
contagem; `assert_valid()` verifica erros operacionais e violações de invariantes.
Ambos lançam `RaceFailure` com o relatório quando a verificação falha.

Uma sequência de funções executa cada entrada uma vez. Nesse formato,
`concurrency` é ignorado.

### Invariantes e redução

Passe as regras do teste em `invariants`. Cada função recebe um `Result` e pode
ser síncrona ou assíncrona. Retornar `False` ou lançar `AssertionError` registra
uma violação; outras exceções propagam.

Para reduzir um cenário, use `shrink=True` e forneça uma função assíncrona
`reset` que restaure o estado antes de cada execução:

```python
def at_most_once(result):
    result.assert_successes(max=1)


result = await raceproof.run(
    operation,
    concurrency=20,
    invariants=[at_most_once],
    shrink=True,
    reset=reset,
)
result.assert_valid()
```

`operation` é a operação testada; `reset` restaura seus dados. O reset também roda
antes da execução inicial. Não há rollback ao final.

A redução testa contagens a partir de um worker e para na primeira execução que
viola as mesmas invariantes sem erros operacionais. A comparação usa os nomes
das invariantes, que devem ser distintos. Para sequências de operações, são
testados apenas prefixos da sequência. O resultado é o menor caso reproduzido
nessas tentativas, não uma busca por todas as combinações e ordens possíveis.

### Sincronização e seed

`await raceproof.checkpoint("after-read")` suspende cada worker até todos chegarem
ao mesmo checkpoint. O parâmetro `parties` permite definir um grupo menor.
Checkpoints são reutilizáveis e devem ser chamados dentro de uma operação ou
de um hook ASGI que preserve seu contexto assíncrono.

A seed controla a ordem de lançamento dos workers. Reprodução determinística
exige estado inicial controlado e checkpoints nos pontos relevantes; a seed
não controla rede, banco ou scheduler. Um checkpoint antes de uma requisição
sincroniza o envio, não as operações internas do servidor.

O timeout limita cada operação e cada invariante, não a redução inteira.
As operações devem respeitar o cancelamento do asyncio e não bloquear o event loop.

### Pytest

O plugin é registrado na instalação. A fixture `raceproof` aceita os argumentos
de `run()` e usa a seed e o timeout configurados no pytest. Testes assíncronos
precisam de `pytest-asyncio` e da marca `@pytest.mark.asyncio`, ou de
`asyncio_mode = "auto"` na configuração.

```sh
python -m pytest --raceproof-seed=42 --raceproof-timeout=5
```

A fixture `raceproof_seed` fornece a seed configurada.

### Resultados

| Acesso | Conteúdo |
| --- | --- |
| `result.format()` | Relatório com seed, timeline e redução |
| `result.outcomes` | Retorno ou exceção de cada worker |
| `result.timeline` | Eventos com sequência, worker e tempo relativo |
| `result.violations` | Violações das invariantes |
| `result.reduced` | Menor cenário encontrado, ou `None` |
| `result.shrink_attempts` | Número de tentativas de redução |

O relatório lista os workers bem-sucedidos como candidatos ao conflito.
A identificação da causa depende das invariantes e da análise da execução.

## Contribuição

Instale as dependências de desenvolvimento e execute as verificações:

```sh
python -m pip install -e '.[dev]'
python -m pytest
python -m ruff check src tests
python -m mypy src/raceproof
```

Issues de bugs devem incluir uma reprodução, o resultado esperado e as versões
de Python e raceproof. Pull requests devem descrever a alteração e incluir testes
quando houver mudança de comportamento.
