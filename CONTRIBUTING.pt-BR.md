# Contribuindo com o Plimsoll

[English](CONTRIBUTING.md) · **Português (Brasil)**

Obrigado por ajudar. O Plimsoll tem uma regra que dá forma a toda mudança — **a query falha, o nó não** — e um
hábito: toda decisão é registrada como ADR, com o débito aceito.

## Antes de começar

- Leia a [arquitetura](docs/architecture.md) e o [índice de ADRs](docs/adr/README.md), que traz uma ordem de
  leitura por área. O [ROADMAP.md](ROADMAP.md) lista o que foi feito, o próximo PR óbvio e o que falta.
- Abra uma issue antes de qualquer coisa maior que uma correção pequena. Uma mudança de comportamento, um endpoint
  novo, uma variável de ambiente nova ou uma dependência nova precisam de um ADR (copie o
  [docs/adr/0000-template.md](docs/adr/0000-template.md)).
- Reporte vulnerabilidades de forma privada, como descrito no [SECURITY.pt-BR.md](SECURITY.pt-BR.md). Nunca numa
  issue pública.

## Ambiente de desenvolvimento

Rust ≥ 1.94 (edition 2024), `curl` e `python3`. Docker só para o início rápido com Compose e para os testes reais.

```bash
git clone https://github.com/Joaolucas2210/plimsoll.git && cd plimsoll
make help        # os pontos de entrada
make demo        # compila o servidor e sobrecarrega um nó local de propósito
make gates       # fmt, clippy -D warnings, cargo test --workspace, invariantes de plano
```

## Organização do repositório

| Caminho | O que é |
| --- | --- |
| `crates/plimsoll-core/` | Plano de computação: fachada do motor, `CeilingPool`, guarda de federação. Nunca depende dos conectores |
| `crates/plimsoll-connectors/` | Iceberg, Parquet, Postgres, distribuição (fragmentos, shuffles, broadcasts) |
| `crates/plimsoll-control/` | Plano de controle: admissão, registro, cliente Iceberg REST. Nunca depende do DataFusion nem do core |
| `crates/plimsoll-server/` | O binário: HTTP, Flight SQL, autenticação, TLS, configuração, observabilidade, o console web |
| `docs/adr/` | Uma decisão por arquivo; ADRs aceitos são substituídos, nunca reescritos |
| `scripts/` | `gates.sh`, `demo.sh`, fixtures de Iceberg e Spark, o benchmark TPC-H |

## Fluxo

1. Crie uma branch a partir da `main` e abra um pull request. A `main` só recebe mudanças por pull request, com
   merge por squash, então cada mudança vira um commit com um resumo em forma de frase, como no histórico
   (`Batched Flight SQL inserts as one statement and one commit`).
2. Os checks obrigatórios são `gates + live integration` e `secret scan`. Uma mudança só de documentação pula o
   job pesado, o que conta como aprovado (ADR 0136).
3. Testes vêm junto com a mudança. A invariante tem testes próprios: uma mudança que mexa em memória, admissão ou
   cancelamento acrescenta um caso em que a query falha e o nó continua atendendo.
4. Rode `make gates` e, para comportamento visível ao usuário, `make demo` antes do push. Um gate que você não
   conseguiu rodar é informado no PR como pulado, nunca como aprovado.
5. Atualize o `CHANGELOG.md` em `[Unreleased]` para tudo que um usuário enxerga: API, Flight SQL, variáveis de
   ambiente, SQL aceito.

## Convenções

- **Sem `unwrap`/`expect` fora de testes** (lints do workspace). Erros são tipados e classificados na origem
  (ADR 0013); um código de erro novo é uma mudança de API.
- **Memória passa pelo pool.** Um buffer, cache ou conector novo reserva do `CeilingPool`; uma alocação que ele
  não consiga contabilizar é registrada como débito no ADR correspondente.
- **Segredos vêm só do ambiente** e nunca vão para log; os logs levam um fingerprint do SQL, nunca o texto.
- **Atalhos são marcados** com um comentário `ponytail:` que diz o limite e o caminho de evolução; o ADR
  correspondente os lista em *Consequences → Negative*.
- **Toda afirmação é verificável:** ADRs e docs citam nomes de tipo, variável, teste ou job de CI, não paráfrases.
- **Documentação:** inglês primeiro. O README tem um irmão em português (`README.pt-BR.md`) com as mesmas seções;
  altere os dois no mesmo PR.

## Licença

Ao contribuir, você concorda que sua contribuição é licenciada sob a [Apache License 2.0](LICENSE). Siga o
[Código de Conduta](CODE_OF_CONDUCT.pt-BR.md).
