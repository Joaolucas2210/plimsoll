# Política de segurança

[English](SECURITY.md) · **Português (Brasil)**

O Plimsoll promete que uma query falha e o nó não, e que a governança (grants, colunas negadas, filtros de
linha) vale em todo scan. Um bug que quebre qualquer uma dessas promessas é uma vulnerabilidade. Reporte de forma
privada e nos dê a chance de corrigir antes de se tornar público.

## Versões suportadas

| Versão | Suportada |
| --- | --- |
| `main` e o release `0.x` mais recente | Sim |
| Releases `0.x` anteriores | Não; atualize primeiro |

O projeto é experimental (`0.x`): a API HTTP, o comportamento do Flight SQL e as variáveis de ambiente podem
mudar entre versões minor, e o [CHANGELOG](CHANGELOG.md) avisa quando isso acontece.

## Como reportar

- Use o reporte privado do GitHub: **Security → Report a vulnerability** neste repositório
  (<https://github.com/Joaolucas2210/plimsoll/security/advisories/new>).
- Não abra issue, pull request ou discussion pública para uma suspeita de vulnerabilidade.
- Informe o SHA do commit ou a tag do release, o papel (nó único, coordenador, worker), as configurações
  `PLIMSOLL_*` relevantes sem os segredos, o menor SQL ou requisição que reproduz o problema, e o que você esperava
  versus o que aconteceu.
- Nunca envie tokens reais, credenciais de catálogo, chaves de storage ou dados privados. Tabelas sintéticas
  bastam.

## O que esperar

| Etapa | Meta |
| --- | --- |
| Confirmação de recebimento | em até 3 dias úteis |
| Primeira avaliação (válido, gravidade, versões afetadas) | em até 10 dias úteis |
| Correção ou mitigação de um problema alto/crítico confirmado | o mais rápido possível, coordenado com você |
| Advisory público | depois de um release corrigido, com crédito a você, salvo se preferir o contrário |

São metas de um projeto com um único mantenedor, não um SLA contratual.

## No escopo

- **O nó morre por causa de uma query:** um panic, um abort ou o OOM killer do kernel disparado por SQL ou por
  uma requisição de um usuário autenticado, sob o teto de memória documentado (ADR 0003).
- **Contorno da governança:** ler um schema sem grant de leitura, uma coluna negada ou linhas fora de um filtro de
  linha; escrever ou apagar sem o grant correspondente (ADR 0025, 0044, 0045, 0077, 0089).
- **Contorno da autenticação** em `/v1/sql`, Flight SQL, `/v1/queries`, `/v1/maintenance/*`, no endpoint de
  worker `/v1/fragment` (`PLIMSOLL_WORKER_TOKEN`) ou nos endpoints de peer `/v1/peer/*`.
- **Injeção:** um parâmetro ligado (`params`, prepared statements do Flight SQL) que altere o SQL, ou um filtro
  federado de Postgres que chegue à fonte de outra forma que não como parâmetro (ADR 0005, 0006, 0043).
- **Resultado errado informado como sucesso** porque um filtro empurrado para a fonte se perdeu (ADR 0005).
- **Segredos ou texto SQL em logs, traces ou métricas** (tokens, `PLIMSOLL_PG_URL`, credenciais do catálogo,
  chaves AWS, headers OTLP).
- **Perda de dados na manutenção de tabelas:** compactação, expiração de snapshots ou remoção de órfãos apagando
  arquivos que um snapshot, branch ou tag vivo ainda referencia (ADR 0078–0080).

## Fora do escopo

Estes são limites documentados, não vulnerabilidades:

- Qualquer coisa com `PLIMSOLL_AUTH=disabled`, que confia no `user` do corpo por definição (só desenvolvimento).
- HTTP sem TLS entre nós ou para clientes quando o TLS (`PLIMSOLL_TLS_*`) não está configurado; um nó exposto à
  internet sem TLS.
- Caminhos de memória listados como contabilizados mas não recusados no [ADR 0003](docs/adr/0003-memory-ceiling.md),
  a menos que matem o processo na configuração documentada.
- Esgotamento de recursos que a admissão está configurada para permitir: `PLIMSOLL_MAX_RUNNING` alto, fila sem
  limite, ou teto de memória acima da memória física do host.
- Quem tem um token válido fazendo o que os grants permitem; quem tem o `PLIMSOLL_WORKER_TOKEN` agindo como
  coordenador (é um segredo compartilhado interno do cluster).
- Bugs no próprio Apache DataFusion, Arrow ou nas bibliotecas Iceberg: reporte upstream, e nos avise se
  derrubarem um nó do Plimsoll.
