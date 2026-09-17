# Documentação Técnica — Continuidade de Fornecimento de Energia

## 1. Visão Geral da Arquitetura

O projeto implementa uma arquitetura de dados em modelo medalhão (Bronze → Silver → Gold) sobre a plataforma Databricks, com Unity Catalog para governança e Delta Lake como formato de armazenamento. Todo o processamento é feito em PySpark sobre compute serverless.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         FONTE EXTERNA — ANEEL                           │
│  Portal de Dados Abertos (https://dadosabertos.aneel.gov.br)            │
│  • Interrupções na Rede de Distribuição (Parquet)                       │
│  • Indicadores Coletivos de Continuidade (Parquet)                      │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                    Ingestão (Notebooks Bronze)
                               │
┌──────────────────────────────▼──────────────────────────────────────────┐
│                         CAMADA BRONZE                                   │
│  Catalog: workspace                                                     │
│  Schema: proj_aneel_cont_01_bronze                                      │
│  • 101_interrupcoes              (9.211.251 registros)                 │
│  • indicadores_continuidade      (5.108.332 registros)                 │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                    Transformação (Notebooks Silver)
                               │
┌──────────────────────────────▼──────────────────────────────────────────┐
│                         CAMADA SILVER                                   │
│  Catalog: workspace                                                     │
│  Schema: proj_aneel_cont_01_silver                                      │
│  • interrupcoes                  (9.086.036 registros)                 │
│  Schema: proj_aneel_cont_02_silver                                      │
│  • indicadores_continuidade      (5.108.332 registros)                 │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                    Agregação (Notebook Gold)
                               │
┌──────────────────────────────▼──────────────────────────────────────────┐
│                         CAMADA GOLD                                     │
│  Catalog: workspace                                                     │
│  Schema: proj_aneel_cont_03_gold                                        │
│  • tempo_interrupcao_mensal     (8.082 registros)                      │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Plataforma e Tecnologias

| Componente | Tecnologia | Observações |
|---|---|---|
| Plataforma | Databricks | Lakehouse |
| Compute | Serverless | Sem cluster dedicado; auto-selecionado |
| Processamento | PySpark (Spark) | DataFrame API + Window Functions |
| Armazenamento | Delta Lake | ACID, schema evolution, time travel |
| Governança | Unity Catalog | Catálogos, schemas, tabelas |
| Orquestração | Databricks Jobs | Multi-task, dependências entre tasks |
| Fonte externa | ANEEL Dados Abertos | Parquet via HTTP/HTTPS |
| Configuração | config.yaml (YAML) | Parâmetros centralizados |
| Linguagem | Python (notebooks) | Com células SQL para validação |

---

## 3. Estrutura de Diretórios

```
continuidade-energia-aneel/
├── README.md                    # Visão geral e histórico do projeto
├── config.yaml                  # Configurações centralizadas
├── .gitignore
├── docs/                         # Documentação
│   ├── PROJETO-Continuidade-Fornecimento.md    # Histórico de desenvolvimento
│   ├── DOCUMENTACAO-NEGOCIO.md                  # Documentação de negócio
│   ├── DOCUMENTACAO-TECNICA.md                  # Este arquivo
│   ├── dicionario_dados.md                      # Dicionário de dados detalhado
│   ├── Gold - Tempo Interrupção - Documentação.md  # Doc específica do notebook Gold
│   └── Análise de Qualidade - Interrupções Bronze  # Notebook de análise de qualidade
├── notebooks/                    # Notebooks do pipeline
│   ├── Ingestão ANEEL - Interrupções de Energia          # Bronze
│   ├── Ingestão ANEEL - Indicadores Coletivos de Continuidade  # Bronze
│   ├── Silver - Interrupções de Energia                   # Silver
│   ├── Silver - Indicadores de Continuidade               # Silver
│   ├── Gold - Tempo Interrupção por Distribuidora e Mês   # Gold
│   ├── Silver - Interrupções v2                            # Versão alternativa Silver
│   ├── EDA - Pipeline Continuidade Fornecimento COMPLETO   # Análise exploratória
│   ├── EDA - Pipeline Continuidade Fornecimento (5 tabelas) # EDA parcial
│   ├── 01_extracao_dados.py                               # Script extração
│   ├── 02_processamento.py                                # Script processamento
│   └── 03_analise_dec.py                                   # Script análise DEC
└── dados/                        # Diretório de dados
    ├── raw/                     # Dados brutos
    └── processed/               # Dados processados
```

---

## 4. Tabelas do Unity Catalog

### 4.1 Camada Bronze — `workspace.proj_aneel_cont_01_bronze`

#### Tabela: `101_interrupcoes`
* **Origem**: ANEEL — Interrupções na Rede de Distribuição (Parquet)
* **Registros**: 9.211.251
* **Modo**: Full load (overwrite)
* **Schema**: PascalCase original da fonte ANEEL

| Campo | Tipo | Descrição |
|---|---|---|
| `DatGeracaoConjuntoDados` | DATE | Data de geração do conjunto pela ANEEL |
| `IdeConjuntoUnidadeConsumidora` | LONG | ID do conjunto de unidades consumidoras |
| `DscConjuntoUnidadeConsumidora` | STRING | Nome do conjunto |
| `DscAlimentadorSubestacao` | STRING | Código do alimentador |
| `DscSubestacaoDistribuicao` | STRING | Código da subestação |
| `NumOrdemInterrupcao` | STRING | Número de ordem único da interrupção |
| `DscTipoInterrupcao` | STRING | Tipo: "Não Programada" / "Programada" |
| `IdeMotivoInterrupcao` | LONG | ID do motivo |
| `DatInicioInterrupcao` | TIMESTAMP | Início da interrupção |
| `DatFimInterrupcao` | TIMESTAMP | Fim da interrupção |
| `DscFatoGeradorInterrupcao` | STRING | Causa hierárquica (4 níveis separados por `;` ou `-`) |
| `NumNivelTensao` | LONG | Nível de tensão (volts) |
| `NumUnidadeConsumidora` | LONG | Unidades consumidoras afetadas |
| `NumConsumidorConjunto` | LONG | Total de consumidores do conjunto |
| `NumAno` | LONG | Ano de referência |
| `NomAgenteRegulado` | STRING | Nome da distribuidora |
| `SigAgente` | STRING | Sigla da distribuidora |
| `NumCPFCNPJ` | LONG | CNPJ da distribuidora |
| `_data_ingestao` | TIMESTAMP | Metadado: timestamp da ingestão |
| `_arquivo_fonte` | STRING | Metadado: URL do arquivo fonte |
| `_ano_referencia` | INT | Metadado: ano dos dados ingeridos |

#### Tabela: `indicadores_continuidade`
* **Origem**: ANEEL — Indicadores Coletivos de Continuidade (Parquet, 28,8 MB)
* **Registros**: 5.108.332
* **Período**: jan/2020 a ago/2026 (80 meses)
* **Distribuidoras**: 105 agentes
* **Indicadores**: 23 tipos
* **Modo**: Full load (overwrite)
* **Schema**: lowercase (normalizado na ingestão)

| Campo | Tipo | Descrição |
|---|---|---|
| `datgeracaoconjuntodados` | DATE | Data de geração do conjunto |
| `ideconjundconsumidoras` | BIGINT | ID do conjunto |
| `dscconjundconsumidoras` | STRING | Descrição do conjunto |
| `sigagente` | STRING | Sigla da distribuidora |
| `numcnpj` | BIGINT | CNPJ da distribuidora |
| `sigindicador` | STRING | Sigla do indicador (DEC, FEC, etc.) |
| `anoindice` | BIGINT | Ano de referência |
| `numperiodoindice` | BIGINT | Mês (1-12) |
| `vlrindiceenviado` | DOUBLE | Valor do indicador |
| `_fonte_url` | STRING | Metadado: URL do arquivo fonte |
| `_ingest_ts` | TIMESTAMP | Metadado: timestamp UTC da ingestão |
| `_ingest_date` | DATE | Metadado: data da ingestão |
| `_run_id` | STRING | Metadado: ID único da execução |

---

### 4.2 Camada Silver — `workspace.proj_aneel_cont_01_silver` e `workspace.proj_aneel_cont_02_silver`

#### Tabela: `proj_aneel_cont_01_silver.interrupcoes`
* **Origem**: `workspace.proj_aneel_cont_01_bronze.101_interrupcoes`
* **Registros**: 9.086.036 (após filtros de qualidade)
* **Colunas**: 34
* **Modo**: Full load (overwrite)
* **Schema**: snake_case (normalizado)

**Transformações aplicadas:**
1. Normalização PascalCase → snake_case (18 colunas)
2. Trim de espaços em 8 colunas texto
3. Cálculo de duração (segundos, minutos, horas)
4. Decomposição do fato gerador em 4 níveis (origem, natureza, categoria, detalhe) com tratamento de separadores mistos (`;` e `-`)
5. Formatação de CNPJ com zeros à esquerda (14 dígitos)
6. Conversão de tensão (V → kV)
7. Derivação temporal (data, mês, dia da semana, hora)
8. Filtros de qualidade: duração ≤ 0, unidades = 0, tensão = 0
9. Metadados silver: `_silver_ts`, `_silver_run_id`, `_origem_tabela`

**Campos principais:**

| Campo | Tipo | Descrição |
|---|---|---|
| `num_ordem_interrupcao` | STRING | Número de ordem único |
| `ide_conjunto_unidade_consumidora` | LONG | ID do conjunto |
| `dsc_conjunto_unidade_consumidora` | STRING | Nome do conjunto |
| `sig_agente` | STRING | Sigla da distribuidora (trim aplicado) |
| `nom_agente_regulado` | STRING | Nome da distribuidora |
| `num_cnpj` | LONG | CNPJ (valor numérico) |
| `cnpj_formatado` | STRING | CNPJ com zeros à esquerda (14 dígitos) |
| `dsc_tipo_interrupcao` | STRING | "Não Programada" / "Programada" |
| `fato_gerador_origem` | STRING | Nível 1 da causa |
| `fato_gerador_natureza` | STRING | Nível 2 da causa |
| `fato_gerador_categoria` | STRING | Nível 3 da causa |
| `fato_gerador_detalhe` | STRING | Nível 4 da causa |
| `dat_inicio_interrupcao` | TIMESTAMP | Início da interrupção |
| `dat_fim_interrupcao` | TIMESTAMP | Fim da interrupção |
| `data_interrupcao` | DATE | Data extraída |
| `mes_interrupcao` | INT | Mês (1-12) |
| `dia_semana_interrupcao` | INT | Dia da semana (1=Dom, 7=Sáb) |
| `hora_inicio_interrupcao` | INT | Hora (0-23) |
| `duracao_segundos` | LONG | Duração em segundos |
| `duracao_minutos` | DOUBLE | Duração em minutos |
| `duracao_horas` | DOUBLE | Duração em horas |
| `nivel_tensao_kv` | DOUBLE | Tensão em kV |
| `num_unidade_consumidora` | LONG | UCs afetadas |
| `num_consumidor_conjunto` | LONG | Total de consumidores do conjunto |
| `_silver_ts` | TIMESTAMP | Timestamp do processamento |
| `_silver_run_id` | STRING | ID da execução silver |
| `_origem_tabela` | STRING | Tabela bronze de origem |

**Chave natural**: `(ide_conjunto_unidade_consumidora, dat_inicio_interrupcao)`

#### Tabela: `proj_aneel_cont_02_silver.indicadores_continuidade`
* **Origem**: `workspace.proj_aneel_cont_01_bronze.indicadores_continuidade`
* **Registros**: 5.108.332
* **Transformações**: Padronização de colunas (snake_case), criação de `data_apuracao` (DATE YYYY-MM-01), limpeza de espaços, validações, metadados

---

### 4.3 Camada Gold — `workspace.proj_aneel_cont_03_gold`

#### Tabela: `tempo_interrupcao_mensal`
* **Origem**: `workspace.proj_aneel_cont_02_silver.indicadores_continuidade`
* **Registros**: 8.082 (agregações mensais)
* **Período**: 80 meses (jan/2020 a ago/2026)
* **Modo**: Full load (overwrite)
* **Granularidade**: Uma linha por distribuidora × mês

| Campo | Tipo | Descrição |
|---|---|---|
| `distribuidora` | STRING | Nome da distribuidora |
| `sigagente` | STRING | Sigla da distribuidora |
| `codigo_distribuidora` | STRING | Código único |
| `ano` | INT | Ano de referência |
| `mes` | INT | Mês (1-12) |
| `ano_mes` | DATE | Data (primeiro dia do mês) |
| `dec_medio` | DOUBLE | DEC médio do mês |
| `dec_minimo` | DOUBLE | Menor DEC registrado |
| `dec_maximo` | DOUBLE | Maior DEC registrado |
| `dec_total` | DOUBLE | Soma total do DEC |
| `decind_medio` | DOUBLE | DEC médio sem programadas |
| `decind_total` | DOUBLE | Total DEC sem programadas |
| `decinc_medio` | DOUBLE | DEC médio em conformidade |
| `decinc_total` | DOUBLE | Total DEC em conformidade |
| `fec_medio` | DOUBLE | FEC médio do mês |
| `fec_total` | DOUBLE | Total de interrupções |
| `numcon_medio` | DOUBLE | Número médio de consumidores |
| `numcon_total` | DOUBLE | Total de consumidores |
| `qtd_conjuntos` | LONG | Quantidade de conjuntos agregados |
| `dec_medio_mes_anterior` | DOUBLE | DEC médio do mês anterior |
| `variacao_perc_mes_anterior` | DOUBLE | Variação % mês a mês |
| `ranking_dec_mes` | INT | Posição no ranking mensal |
| `timestamp_processamento` | TIMESTAMP | Data/hora do processamento |

**Tecnologias utilizadas**: PySpark com Window Functions (lag, rank), Delta Lake com overwriteSchema

---

## 5. Notebooks do Pipeline

### 5.1 Bronze — Ingestão de Interrupções

| Atributo | Valor |
|---|---|
| **Nome** | Ingestão ANEEL - Interrupções de Energia |
| **ID** | 1581954047410310 |
| **Caminho** | `notebooks/Ingestão ANEEL - Interrupções de Energia` |
| **Origem** | ANEEL — Interrupções na Rede de Distribuição (Parquet via HTTPS) |
| **Destino** | `workspace.proj_aneel_cont_01_bronze.101_interrupcoes` |
| **Modo** | Full load (overwrite) |
| **Estratégia** | Leitura HTTP direta + fallback para download local |

**Funcionalidades:**
* Leitura de Parquet do portal ANEEL
* Fallback: leitura HTTP → download local (caso falha)
* Metadados de rastreabilidade: `_data_ingestao`, `_arquivo_fonte`, `_ano_referencia`
* Criação automática de schema
* Escrita Delta com `mergeSchema`
* Validações pós-ingestão
* URL parametrizada por ano de referência

### 5.2 Bronze — Ingestão de Indicadores Coletivos

| Atributo | Valor |
|---|---|
| **Nome** | Ingestão ANEEL - Indicadores Coletivos de Continuidade |
| **ID** | 2458819770635339 |
| **Caminho** | `notebooks/Ingestão ANEEL - Indicadores Coletivos de Continuidade` |
| **Origem** | ANEEL — Indicadores Coletivos de Continuidade (Parquet, 28,8 MB) |
| **Destino** | `workspace.proj_aneel_cont_01_bronze.indicadores_continuidade` |
| **Modo** | Full load (overwrite) |

**Funcionalidades:**
* Download via `urllib.request`
* Leitura com pandas → conversão para Spark DataFrame (necessário em serverless)
* Normalização de colunas para lowercase
* Metadados: `_fonte_url`, `_ingest_ts`, `_ingest_date`, `_run_id`
* Escrita Delta com `overwriteSchema` e `mergeSchema`
* Validações: total, cobertura temporal, distribuição por indicador, nulos

**Restrição técnica**: O Spark em serverless não suporta leitura direta de URLs HTTPS nem arquivos locais fora de `/Workspace`. Solução: baixar com Python, ler com pandas e converter.

### 5.3 Silver — Interrupções de Energia

| Atributo | Valor |
|---|---|
| **Nome** | Silver - Interrupções de Energia |
| **ID** | 3059658603716946 |
| **Origem** | `workspace.proj_aneel_cont_01_bronze.101_interrupcoes` |
| **Destino** | `workspace.proj_aneel_cont_01_silver.interrupcoes` |
| **Modo** | Full load (overwrite) |
| **Redução** | 125.215 registros removidos (1,36% por filtros de qualidade) |

**Transformações:**
* Normalização PascalCase → snake_case
* Trim de strings
* Cálculo de duração (segundos, minutos, horas)
* Decomposição do fato gerador (4 níveis com `F.when(F.size(partes) >= N, ...)` para acesso seguro)
* Formatação de CNPJ (14 dígitos com zeros à esquerda)
* Conversão de tensão (V → kV)
* Derivação temporal (data, mês, dia da semana, hora)
* Filtros de qualidade (duração ≤ 0, unidades = 0, tensão = 0)
* Metadados: `_silver_ts`, `_silver_run_id`, `_origem_tabela`

### 5.4 Silver — Indicadores de Continuidade

| Atributo | Valor |
|---|---|
| **Nome** | Silver - Indicadores de Continuidade |
| **ID** | 4356906633627185 |
| **Origem** | `workspace.proj_aneel_cont_01_bronze.indicadores_continuidade` |
| **Destino** | `workspace.proj_aneel_cont_02_silver.indicadores_continuidade` |
| **Modo** | Full load (overwrite) |

**Transformações:**
* Padronização de colunas (snake_case, nomes descritivos)
* Criação de `data_apuracao` (formato DATE YYYY-MM-01)
* Limpeza de espaços
* Metadados de processamento e rastreabilidade

### 5.5 Gold — Tempo Interrupção por Distribuidora e Mês

| Atributo | Valor |
|---|---|
| **Nome** | Gold - Tempo Interrupção por Distribuidora e Mês |
| **ID** | 218687058620801 |
| **Origem** | `workspace.proj_aneel_cont_02_silver.indicadores_continuidade` |
| **Destino** | `workspace.proj_aneel_cont_03_gold.tempo_interrupcao_mensal` |
| **Modo** | Full load (overwrite) |
| **Granularidade** | Distribuidora × Mês |

**Processamento:**
1. Extração temporal (ano, mês, ano_mes)
2. Agregação mensal por distribuidora (GroupBy com múltiplas métricas)
3. Enriquecimento com Window Functions:
   * `dec_medio_mes_anterior` (LAG)
   * `variacao_perc_mes_anterior` (variação %)
   * `ranking_dec_mes` (RANK)
4. Escrita Delta com `overwriteSchema`
5. Validações SQL: contagem por ano, top 10 distribuidoras, evolução temporal

### 5.6 Notebooks de Apoio

| Notebook | ID | Função |
|---|---|---|
| EDA - Pipeline Continuidade Fornecimento COMPLETO | 1419368866069979 | Análise exploratória de todas as 5 tabelas |
| EDA - Pipeline Continuidade Fornecimento (5 tabelas) | 1419368866069978 | EDA parcial (versão anterior) |
| Análise de Qualidade - Interrupções Bronze | 4356906633627129 | Análise de qualidade dos dados bronze |
| Silver - Interrupções v2 | 3082991586421712 | Versão alternativa da transformação Silver |

---

## 6. Orquestração — Databricks Jobs

### Job Principal: Pipeline Continuidade de Fornecimento - Bronze Silver Gold

| Atributo | Valor |
|---|---|
| **Job ID** | 940211691414181 |
| **Formato** | Multi-task |
| **max_concurrent_runs** | 1 |
| **max_retries** | 0 (job e todas as tasks) |
| **retry_on_timeout** | false |
| **Queue** | Habilitado |

**DAG de Tasks:**

```
bronze_interrupcoes ──┐
                       ├──→ silver_interrupcoes ──┐
bronze_indicadores ────┘                          ├──→ gold_tempo_interrupcao
                       └──→ silver_indicadores ──┘
```

| Task Key | Notebook | Depende de | Descrição |
|---|---|---|---|
| `bronze_interrupcoes` | Ingestão ANEEL - Interrupções de Energia | — | Ingestão bronze de interrupções |
| `bronze_indicadores` | Ingestão ANEEL - Indicadores Coletivos de Continuidade | — | Ingestão bronze de indicadores |
| `silver_interrupcoes` | Silver - Interrupções de Energia | `bronze_interrupcoes` | Transformação silver de interrupções |
| `silver_indicadores` | Silver - Indicadores de Continuidade | `bronze_indicadores` | Transformação silver de indicadores |
| `gold_tempo_interrupcao` | Gold - Tempo Interrupção por Distribuidora e Mês | `silver_interrupcoes`, `silver_indicadores` | Agregação gold |

**Execução de referência (Run 271484094733438):**
* Status: SUCCESS
* Duração total: ~511s (~8,5 min)
* Trigger: ONE_TIME

| Task | Tempo de Execução |
|---|---|
| bronze_interrupcoes | 11s |
| bronze_indicadores | 352s |
| silver_interrupcoes | 86s |
| silver_indicadores | 29s |
| gold_tempo_interrupcao | 33s |

### Job Secundário: Pipeline Continuidade Fornecimento - Completo

| Atributo | Valor |
|---|---|
| **Job ID** | 444579357162355 |
| **Formato** | Multi-task |
| **max_concurrent_runs** | 1 |
| **Status** | Configurado, sem tasks definidas (versão inicial/vazia) |

---

## 7. Estratégia de Carga e Atualização

### Modo de Carga
Todas as tabelas utilizam **Full Load (overwrite)** — os dados completos são substituídos a cada execução. Esta estratégia foi escolhida porque:
* A fonte ANEEL republica arquivos completos (não há incremental)
* Simplifica a lógica de processamento
* Evita problemas de consistência entre carga parcial e dados existentes

### Frequência
* **Execução atual**: Manual (trigger ONE_TIME)
* **Frequência sugerida**: Mensal, após publicação de atualizações pela ANEEL
* **Ordem de execução**: Bronze → Silver → Gold (garantida pelas dependências do job)

### Evolução de Schema
* `mergeSchema` e `overwriteSchema` habilitados para acomodar mudanças na estrutura dos dados da ANEEL

---

## 8. Qualidade de Dados

### Análise de Qualidade Bronze — Interrupções

| Problema | Quantidade | % do Total | Ação |
|---|---|---|---|
| Campos nulos críticos | 0 | 0,00% | Nenhuma |
| Durações zero (início = fim) | 2.702 | 0,54% | Excluir |
| Durações > 30 dias | 5 | 0,00% | Marcar com flag |
| Durações 7-30 dias | 298 | 0,06% | Marcar com flag |
| Duplicatas (chave natural) | 3.608 | 0,72% | Deduplicar |
| UCs inválidas (= 0) | 1.946 | 0,39% | Excluir |
| **Total com problemas** | **~8.559** | **1,71%** | — |

**Resultado**: 98,36% dos registros bronze são válidos.

### Regras de Tratamento (aplicadas na Silver)

1. **Exclusão de inválidos**: `WHERE NumUnidadeConsumidora > 0 AND DatFimInterrupcao > DatInicioInterrupcao`
2. **Deduplicação**: `ROW_NUMBER() OVER (PARTITION BY NumOrdemInterrupcao, NumUnidadeConsumidora ORDER BY DatGeracaoConjuntoDados DESC, DatFimInterrupcao DESC) = 1`
3. **Marcação de durações extremas**: Flag para durações > 7 dias (sem exclusão)
4. **Filtros adicionais**: Tensão = 0 também excluída

### Validações Pós-Processamento
* Verificação de cobertura temporal
* Distribuição por distribuidora
* Contagem de nulos em colunas-chave
* Zero duplicatas após deduplicação (validado com queries SQL)
* Zero registros inválidos pós-filtro

### Metadados de Rastreabilidade

| Metadado | Camada | Descrição |
|---|---|---|
| `_data_ingestao` | Bronze | Timestamp da ingestão |
| `_arquivo_fonte` | Bronze | URL do arquivo fonte |
| `_ano_referencia` | Bronze | Ano dos dados |
| `_fonte_url` | Bronze | URL do arquivo baixado |
| `_ingest_ts` | Bronze | Timestamp UTC da ingestão |
| `_ingest_date` | Bronze | Data da ingestão |
| `_run_id` | Bronze | ID único da execução (YYYYMMDDHHMMSS) |
| `_silver_ts` | Silver | Timestamp do processamento |
| `_silver_run_id` | Silver | ID da execução silver |
| `_origem_tabela` | Silver | Tabela bronze de origem |
| `timestamp_processamento` | Gold | Data/hora do processamento |

---

## 9. Configuração — `config.yaml`

Arquivo centralizado de configuração do projeto, contendo:

* **project**: Nome, versão (0.1.0), descrição, autor, data de criação
* **analysis**: Anos (2024, 2025), escopo (Brasil), pergunta de pesquisa
* **data_sources.aneel**: URLs dos datasets, catálogo/schema/tabela de destino, modo de carga
* **processing**: Tipos de interrupção (include/exclude), filtros de qualidade, limites de discrepância (warning 10%, critical 20%)
* **indicators**: Definições de DEC e FEC (nome, unidade, descrição)
* **paths**: Estrutura de diretórios
* **visualization**: Configurações de gráficos (bar, viridis, 12×6)
* **metadata**: Licença (Público - Dados ANEEL), tags, status

---

## 10. Dependências e Restrições Técnicas

### Dependências Externas
* **ANEEL Dados Abertos**: Disponibilidade e atualização dos datasets no portal
* **Conectividade HTTPS**: Acesso ao portal da ANEEL a partir do compute Databricks

### Restrições do Compute Serverless
* Não suporta leitura direta de URLs HTTPS (`HttpFileSystem` sem `listStatus`)
* Não suporta arquivos locais fora de `/Workspace` (`LocalFilesystemAccessDeniedException`)
* Não suporta comandos `SET` em células SQL
* **Solução aplicada**: Download com Python (`urllib.request`), leitura com pandas, conversão para Spark DataFrame

### Dependências entre Componentes
* Silver depende de Bronze (garantido pelo DAG do job)
* Gold depende de ambas as tabelas Silver (garantido pelo DAG do job)
* EDA depende de todas as tabelas (execução manual)

---

## 11. Operação e Manutenção

### Como Executar o Pipeline

1. Acessar o Job **Pipeline Continuidade de Fornecimento - Bronze Silver Gold** (ID: 940211691414181)
2. Executar com trigger manual (Run Now)
3. Monitorar progresso das 5 tasks
4. Verificar status de cada task e logs em caso de falha

### Pontos de Atenção
* **bronze_indicadores** é a task mais lenta (~6 min) devido ao download e processamento de 28,8 MB
* O modo **overwrite** substitui todos os dados a cada execução — garantir que o pipeline complete antes de consultar
* **max_retries = 0**: falhas não têm retry automático; verificar e re-executar manualmente
* Evolução de schema: se a ANEEL adicionar colunas, `mergeSchema` deve acomodar automaticamente

### Monitoramento Sugerido
* Configurar alertas de falha no job (email/webhook)
* Monitorar volume de registros após cada execução
* Verificar cobertura temporal após atualização
* Validar nulos em colunas-chave após cada carga

### Falhas Conhecidas e Correções Aplicadas

| Problema | Correção |
|---|---|
| `INVALID_ARRAY_INDEX` na decomposição do fato gerador | `F.when(F.size(partes) >= N, ...)` para acesso seguro |
| Comandos `SET` em células SQL incompatíveis com serverless | Removidos do notebook Silver |
| Leitura direta de URL HTTPS em serverless | Download com Python + pandas → Spark |
| Location DBFS com mount point | Substituído por Unity Catalog |

---

## 12. Mapeamento de Assets do Workspace

### Notebooks

| Notebook | ID | Camada |
|---|---|---|
| Ingestão ANEEL - Interrupções de Energia | 1581954047410310 | Bronze |
| Ingestão ANEEL - Indicadores Coletivos de Continuidade | 2458819770635339 | Bronze |
| Silver - Interrupções de Energia | 3059658603716946 | Silver |
| Silver - Indicadores de Continuidade | 4356906633627185 | Silver |
| Gold - Tempo Interrupção por Distribuidora e Mês | 218687058620801 | Gold |
| EDA - Pipeline Continuidade Fornecimento COMPLETO | 1419368866069979 | Apoio |
| EDA - Pipeline Continuidade Fornecimento (5 tabelas) | 1419368866069978 | Apoio |
| Análise de Qualidade - Interrupções Bronze | 4356906633627129 | Apoio |
| Silver - Interrupções v2 | 3082991586421712 | Silver (alt) |

### Jobs

| Job | ID | Status |
|---|---|---|
| Pipeline Continuidade de Fornecimento - Bronze Silver Gold | 940211691414181 | ✅ Operacional |
| Pipeline Continuidade Fornecimento - Completo | 444579357162355 | Configurado (vazio) |

### Arquivos de Documentação

| Arquivo | ID | Conteúdo |
|---|---|---|
| README.md | 3732400835204232 | Visão geral e histórico |
| PROJETO-Continuidade-Fornecimento.md | 1581954047410311 | Histórico de desenvolvimento detalhado |
| dicionario_dados.md | 3732400835204237 | Dicionário de dados completo |
| Gold - Tempo Interrupção - Documentação.md | 2841039778143464 | Documentação do notebook Gold |
| DOCUMENTACAO-NEGOCIO.md | 4302381070055321 | Documentação de negócio |
| DOCUMENTACAO-TECNICA.md | 4302381070055322 | Este arquivo |
| config.yaml | 3732400835204243 | Configuração do projeto |

---

## 13. Próximos Passos Técnicos (Backlog)

* [ ] Agendar execução periódica do job (mensal/trimestral)
* [ ] Configurar alertas de falha (email/webhook)
* [ ] Implementar monitoramento de qualidade de dados automatizado
* [ ] Criar dashboard de indicadores de continuidade
* [ ] Enriquecer com dados auxiliares (geografia, classificações)
* [ ] Criar tabelas dimensionais (distribuidoras, municípios, períodos)
* [ ] Implementar KPIs de continuidade calculados
* [ ] Comparar DEC observado vs. DEC oficial por distribuidora
* [ ] Documentar schema e metadados no Unity Catalog
* [ ] Definir políticas de acesso aos dados

---

*Documento criado em: 16/09/2026*
*Autor: Pedro Silva*
*Status: 🟢 Projeto em operação*