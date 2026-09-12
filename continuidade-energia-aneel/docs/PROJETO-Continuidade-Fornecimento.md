# Projeto: Continuidade de Fornecimento de Energia

## Visão Geral
Projeto de análise e monitoramento da continuidade do fornecimento de energia elétrica no Brasil, utilizando dados públicos da ANEEL (Agência Nacional de Energia Elétrica).

## Objetivo
Analisar indicadores de continuidade do fornecimento de energia (DEC, FEC, DIC, FIC, DMIC) para:
- Monitorar qualidade do serviço das distribuidoras
- Identificar padrões de interrupções
- Suportar decisões estratégicas sobre continuidade de fornecimento
- Criar métricas e dashboards de acompanhamento

---

## Arquitetura de Dados

### Modelo Medalhão (Bronze → Silver → Gold)

#### **Camada Bronze** (Dados Brutos)
- **Catálogo/Schema**: `workspace.proj_aneel_cont_01_bronze`
- **Tabelas**:
  - `indicadores_continuidade`: Indicadores coletivos de continuidade (DEC, FEC e variantes) — 5.108.332 registros
  - `interrupcoes_energia`: Dados brutos de interrupções da ANEEL (em `main.bronze_aneel`)

#### **Camada Silver** (Em planejamento)
- Limpeza e transformações
- Padronização de dados
- Enriquecimento com informações auxiliares

#### **Camada Gold** (Em planejamento)
- Agregações por distribuidora, região, período
- Métricas de negócio
- Datasets prontos para consumo em dashboards

---

## Fontes de Dados

### ANEEL - Indicadores Coletivos de Continuidade
- **Portal**: [Dados Abertos ANEEL](https://dadosabertos.aneel.gov.br/dataset/indicadores-coletivos-de-continuidade)
- **Formato**: Parquet (arquivo único, republicado a cada atualização)
- **URL do arquivo**: `https://dadosabertos.aneel.gov.br/dataset/d5f0712e-62f6-4736-8dff-9991f10758a7/resource/d7f70fb1-725c-4748-afeb-65c6a78df550/download/indicadores-continuidade-coletivos-2020-2029.parquet`
- **Frequência de atualização**: Mensal/Trimestral (conforme publicação da ANEEL)
- **Cobertura temporal**: jan/2020 a ago/2026 (80 períodos mensais)
- **Indicadores incluídos** (23 tipos):
  - **DEC**: Duração Equivalente de Interrupção por Unidade Consumidora
  - **FEC**: Frequência Equivalente de Interrupção por Unidade Consumidora
  - **DECIP/DECIPC/DECIND/DECINC/DECINE/DECINO/DECXP/DECXPC/DECXN/DECXNC**: Variantes de DEC
  - **FECIP/FECIPC/FECIND/FECINC/FECINE/FECINO/FECXP/FECXPC/FECXN/FECXNC**: Variantes de FEC
  - **NumCon**: Número de Consumidores
- **Distribuidoras**: 105 agentes
- **Total de registros**: 5.108.332

---

## Implementações

### ✅ Sessão: 08/09/2026 22:41

#### 1. Ingestão de Dados - Camada Bronze

**Notebook criado**: [Ingestão ANEEL - Interrupções de Energia](#notebook-1581954047410310)

**Descrição**: Pipeline de ingestão completo para carregar dados de interrupções da ANEEL na camada bronze.

**Funcionalidades implementadas**:
- ✅ Leitura de arquivos Parquet do portal de dados abertos da ANEEL
- ✅ Estratégia de fallback (leitura HTTP direta + download local caso falhe)
- ✅ Adição de metadados de rastreabilidade:
  - `_data_ingestao`: Timestamp da ingestão
  - `_arquivo_fonte`: URL do arquivo fonte
  - `_ano_referencia`: Ano dos dados ingeridos
- ✅ Criação automática do schema `bronze_aneel`
- ✅ Escrita em formato Delta com suporte a evolução de schema
- ✅ Validações de qualidade pós-ingestão
- ✅ Consultas de exemplo para exploração inicial

**Destino**: `main.bronze_aneel.interrupcoes_energia`

**Modo de execução**: Full load (overwrite) - dados completos substituídos a cada execução

**Observações técnicas**:
- URL do Parquet parametrizada por ano de referência
- Schema flexível para acomodar mudanças na estrutura dos dados da ANEEL
- Tratamento de erro robusto para diferentes cenários de acesso

### ✅ Sessão: 11/09/2026 00:23

#### 2. Ingestão de Indicadores Coletivos de Continuidade - Camada Bronze

**Notebook criado**: [Ingestão ANEEL - Indicadores Coletivos de Continuidade](#notebook-2458819770635339)

**Descrição**: Pipeline de ingestão completo para carregar os Indicadores Coletivos de Continuidade da ANEEL na camada bronze.

**Funcionalidades implementadas**:
- ✅ Download do arquivo Parquet (28.8 MB) do portal de dados abertos da ANEEL via `urllib.request`
- ✅ Leitura com pandas e conversão para Spark DataFrame (necessário em compute serverless, que não suporta leitura direta de URLs HTTPS nem arquivos locais fora de /Workspace)
- ✅ Normalização de nomes de colunas para lowercase (DatGeracaoConjuntoDados → datgeracaoconjuntodados, etc.)
- ✅ Adição de metadados de rastreabilidade:
  - `_fonte_url`: URL do arquivo fonte baixado
  - `_ingest_ts`: Timestamp UTC da ingestão
  - `_ingest_date`: Data da ingestão
  - `_run_id`: Identificador único da execução (YYYYMMDDHHMMSS)
- ✅ Criação automática do schema `workspace.proj_aneel_cont_01_bronze`
- ✅ Escrita em formato Delta com `overwriteSchema` e `mergeSchema` habilitados
- ✅ Validações de qualidade pós-ingestão (total, cobertura temporal, distribuição por indicador, nulos em colunas-chave)
- ✅ Consulta de exemplo para exploração inicial

**Destino**: `workspace.proj_aneel_cont_01_bronze.indicadores_continuidade`

**Modo de execução**: Full load (overwrite) - dados completos substituídos a cada execução

**Resultado da execução**:
- 5.108.332 registros escritos com sucesso
- 80 períodos mensais (jan/2020 a ago/2026)
- 23 indicadores distintos (DEC, FEC e variantes + NumCon)
- 105 distribuidoras (agentes)
- Zero nulos em colunas-chave (sigagente, sigindicador, anoindice, numperiodoindice, vlrindiceenviado)
- Run ID: 20260911032756

**Observações técnicas**:
- O Spark no compute serverless não suporta leitura direta de URLs HTTPS (`HttpFileSystem` sem `listStatus`) nem arquivos locais fora de `/Workspace` (`LocalFilesystemAccessDeniedException`). Solução: baixar com Python, ler com pandas e converter para Spark DataFrame.
- Colunas originais da fonte em PascalCase (ex: `DatGeracaoConjuntoDados`), normalizadas para lowercase no notebook.
- Arquivo temporário limpo automaticamente após a escrita.

---

## Próximos Passos

### Camada Silver
- [ ] Criar transformações de limpeza e padronização
- [ ] Implementar validações de qualidade de dados
- [ ] Enriquecer com dados auxiliares (geografia, classificações)
- [ ] Criar tabelas dimensionais (distribuidoras, municípios, períodos)

### Camada Gold
- [ ] Criar agregações por distribuidora e região
- [ ] Calcular métricas de tendência temporal
- [ ] Preparar datasets para dashboards
- [ ] Implementar KPIs de continuidade

### Automação
- [ ] Agendar execução periódica da ingestão (mensal/trimestral)
- [ ] Configurar alertas para falhas de ingestão
- [ ] Implementar monitoramento de qualidade de dados

### Análise e Visualização
- [ ] Criar dashboard de indicadores de continuidade
- [ ] Análises por distribuidora e região
- [ ] Comparativo temporal de indicadores
- [ ] Alertas para degradação de qualidade

### Governança
- [ ] Documentar schema e metadados no Unity Catalog
- [ ] Definir políticas de acesso aos dados
- [ ] Catalogar métricas e definições de negócio

---

## Estrutura de Pastas

```
continuidade-energia-aneel/
├── README.md
├── config.yaml
├── docs/
│   ├── PROJETO-Continuidade-Fornecimento.md    # Este arquivo
│   └── dicionario_dados.md                     # Dicionário de dados
├── notebooks/
│   ├── Ingestão ANEEL - Interrupções de Energia          # Notebook de ingestão de interrupções
│   ├── Ingestão ANEEL - Indicadores Coletivos de Continuidade  # Notebook de ingestão de indicadores
│   ├── 01_extracao_dados.py
│   ├── 02_processamento.py
│   └── 03_analise_dec.py
└── dados/
    ├── raw/
    └── processed/
```

---

## Contatos e Referências

- **Portal ANEEL**: https://dadosabertos.aneel.gov.br
- **Dataset**: Indicadores Coletivos de Continuidade
- **Documentação**: https://www.aneel.gov.br/indicadores-coletivos-de-continuidade

---

## Histórico de Atualizações

| Data | Descrição |
|------|----------|
| 08/09/2026 22:41 | Criação do projeto e implementação da ingestão bronze (interrupções) |
| 11/09/2026 00:23 | Ingestão de Indicadores Coletivos de Continuidade na bronze (5.108.332 registros) |

---

*Última atualização: 11/09/2026*