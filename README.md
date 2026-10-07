# CineData Analytics

Pipeline de Engenharia de Dados em PySpark para Databricks, organizado pela Arquitetura Medalhão. A solução inclui notebooks `.ipynb`, comentários no código e explicações em células Markdown.

## Arquivos e ordem de execução

| Ordem | Notebook | Responsabilidade |
|---|---|---|
| 1 | `01_Bronze.ipynb` | Leitura dos cinco CSVs, ingestão da API PTAX e gravação Delta em append. |
| 2 | `02_Silver.ipynb` | Limpeza, tipagem, deduplicação, forward fill da cotação e conversão USD/BRL. |
| 3 | `03_Gold.ipynb` | Dimensões, fato por filme, tabelas-ponte, avaliações resumidas e contexto para IA. |
| 4 | `04_Analytics.ipynb` | Seis consultas de negócio com `display()`. |

### Parâmetros (widgets)

| Widget | Padrão | Finalidade |
|---|---|---|
| `catalogo` | `workspace` | Catálogo onde ficam as camadas. Ajuste em todos os notebooks se usar outro. |
| `caminho_inputs` | `/Volumes/workspace/default/inputs` | Volume com os CSVs; utilizado na Bronze. |
| `data_inicio` | Hoje menos 6 dias | Início do período da PTAX, em `MM-DD-AAAA`. |
| `data_fim` | Hoje | Fim do período da PTAX, em `MM-DD-AAAA`. |

### Bronze — dados originais

- `spark.read.csv` com cabeçalho, todas as colunas STRING, leitura multiline e configuração de aspas.
- `current_timestamp()` para registrar `ingestion_datetime` em cada lote.
- `write.format("delta").mode("append")` para preservar os lotes anteriores.
- `requests`, parâmetros OData, timeout e retries para consultar a API pública do Banco Central.

Nenhuma regra de limpeza é aplicada aos valores da Bronze. A interpretação do CSV respeita o formato de origem; registros com campos deslocados permanecem sujos. Os CSVs originais continuam no Volume e permitem investigar problemas de parsing.

### Silver — dados tratados

- `trim`, `lower`, `regexp_replace` e dicionário de tradução para os status.
- `try_to_timestamp` + `coalesce` para datas em formatos diferentes, e `year` para extrair o ano.
- `try_cast` para conversões seguras sem desabilitar o modo ANSI.
- `DECIMAL(18,2)` para dinheiro, DOUBLE para notas/popularidade e INT para contagens.
- Tratamento de sufixos K/M/B, moedas e separadores numéricos antes dos casts.
- `row_number` para manter uma versão por filme e `groupBy` para deduplicar avaliações.
- `split` + `explode` para gêneros e entidades; aceita vírgula, ponto e vírgula e barra vertical.
- `unionByName` para reunir atores, diretores, roteiristas e produtoras.
- Calendário com `sequence` + `explode`, left join e janela acumulada com `last(..., ignorenulls=True)` para forward fill.

A Silver não modifica as tabelas Bronze. Suas tabelas são reconstruídas em overwrite, evitando acumular cópias na reexecução.

### Gold — modelo dimensional

- Chaves substitutas BIGINT geradas por `row_number`.
- Fato com um registro por filme lançado, sem joins de relações muitos-para-muitos.
- Dimensões de filmes, gêneros, pessoas, produtoras e avaliações resumidas.
- Três tabelas-ponte deduplicadas para ligar filmes a gêneros, pessoas e produtoras.
- `count` e `avg` para resumir as avaliações; médias com duas casas decimais.
- Tabela complementar `gold.tb_contexto_ia`, com metadados e listas agregadas por filme.
- Validações de unicidade, chaves estrangeiras e manutenção do grão do fato.

### Analytics — perguntas de negócio

1. Soma da receita em reais diretamente no fato, com informação sobre valores ausentes.
2. Cinco filmes mais populares.
3. Contagem de filmes distintos por gênero, do maior para o menor volume.
4. Dez maiores receitas com `rank()` preservando empates.
5. Ator com mais participações nos últimos 24 meses da base.
6. Produtora com maior lucro nos últimos 60 meses da base.

As consultas 5 e 6 exibem todos os líderes se houver empate.

## 3. Regras de negócio e decisões explícitas

- **Ausência não é zero:** dinheiro zerado ou negativo vira NULL. Lucro exige orçamento e receita conhecidos; margem exige receita positiva. Lucro negativo é válido.
- **Margem:** `(receita - orçamento) / receita * 100`; não é retorno sobre investimento (ROI).
- **Conversão para reais:** todos os filmes usam a última cotação de compra disponível no período selecionado. Não é uma conversão histórica na data da estreia. A Silver registra a taxa e a data de origem utilizadas.
- **Dias sem PTAX:** a Bronze consulta também os 15 dias anteriores ao início para obter uma taxa de apoio. A Silver usa forward fill e apresenta apenas o intervalo solicitado. Se não houver apoio, o notebook falha explicitamente, sem inventar uma taxa.
- **Datas ambíguas:** `dd/MM/yyyy` é interpretado como dia/mês; datas com hífen e ano ao final priorizam `MM-dd-yyyy`, conforme o arquivo. O padrão `dd-MM-yyyy` só é tentado quando a primeira interpretação falha.
- **Versões empatadas:** o timestamp distingue lotes, mas não a ordem das linhas no mesmo lote. O desempate é feito por preenchimento e hash determinístico, sem alegar uma ordem histórica inexistente.
- **Notas:** fora de 0–10 viram NULL, inclusive erros de escala. Não dividimos automaticamente notas inválidas por dez.
- **Votos:** somente representações inteiras não negativas são aceitas; números com parte decimal não são truncados.
- **Gêneros e nomes:** gêneros usam domínio explícito; entidades usam filtros conservadores para remover resíduos. Não é possível reconstruir perfeitamente o deslocamento de colunas sem uma fonte confiável. Nomes incomuns e alguns fragmentos plausíveis podem exigir revisão humana.
- **Filmes lançados:** o fato usa status `Lançado` e data até o dia da execução. Estreias futuras ou ausentes não são consideradas realizadas.
- **Contagem por gênero:** considera o catálogo inteiro da dimensão. Consultas financeiras e temporais usam os filmes com lançamento realizado.
- **Lucro por produtora:** sem percentuais de participação, o lucro integral do filme é atribuído a cada produtora participante. Não se deve somar os totais de produtoras para obter o lucro global, pois coproduções apareceriam mais de uma vez.
- **Avaliações:** a contagem inclui avaliações com nota inválida; a média ignora essas notas NULL.
- **Pessoas:** a dimensão tem grão nome + tipo de atuação, conforme as colunas solicitadas. Sem identificador de pessoa na origem, homônimos podem ser consolidados.
- **Chaves:** as chaves substitutas são reconstruídas junto com fato e pontes; podem mudar em outro snapshot. Um pipeline incremental exigiria um cadastro persistente de chaves.
- **Contexto de IA:** o objetivo menciona essa tabela, mas não especifica colunas; o notebook entrega um complemento com uma linha por filme e relações previamente agregadas.

## Referências técnicas

- [Leitura CSV e opções do Spark no Databricks](https://docs.databricks.com/aws/en/spark/api-options)
- [Conversão segura de timestamp](https://docs.databricks.com/aws/en/sql/language-manual/functions/try_to_timestamp)
- [Funções de janela do Spark](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html)
- [Cotações do dólar — Banco Central](https://dadosabertos.bcb.gov.br/dataset/dolar-americano-usd-todos-os-boletins-diarios)

## Autor

Samuel Alves Navarro

## Validação realizada

As transformações Silver, a modelagem Gold e as seis consultas foram executadas localmente com PySpark 3.5.3 e os cinco CSVs fornecidos. Passaram as verificações de grão, unicidade, integridade das pontes, faixa de notas, forward fill, formatos de datas e valores monetários (incluindo K/M e separadores brasileiros/americanos).

No teste local, as cotações foram fixtures sintéticas e as tabelas foram materializadas em Parquet para testar a lógica. Nenhuma cotação de teste foi incluída nos notebooks entregues. A gravação Delta no Unity Catalog, as permissões do Volume e o acesso real à API devem ser confirmados ao executar a sequência no Databricks. Os valores finais das análises dependem da PTAX real dessa execução.
