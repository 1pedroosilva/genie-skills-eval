# Arquitetura do Pipeline de Continuidade de Fornecimento

## Visão Geral

Pipeline medalhão (bronze → silver → gold) que processa dados de interrupções de fornecimento de energia elétrica da ANEEL, calculando tempo de interrupção agregado por distribuidora e período.

**Catálogo Unity Catalog**: `workspace`  
**Schemas**: 
- Bronze: `continuidade_aneel_bronze`, `proj_aneel_cont_01_bronze`
- Silver: `continuidade_aneel_silver`, `proj_aneel_cont_02_silver`
- Gold: `continuidade_aneel_gold`, `proj_aneel_cont_03_gold`

**Observação**: Existem dois padrões de nomenclatura de schemas em uso (`continuidade_aneel_*` e `proj_aneel_cont_*`). O padrão vigente é `continuidade_aneel_*`.

---

## Camada Bronze

### Notebook 101: Ingestão ANEEL Interrupções
**Path**: `01_bronze/101_ingestao_aneel_interrupcoes.py`  
**Responsabilidade**: Ingestão incremental de arquivos Parquet da ANEEL contendo dados de interrupções.

**Tabela de Destino**: `workspace.continuidade_aneel_bronze.aneel_interrupcoes`

#### Schema da Tabela

| Campo | Tipo | Descrição |
|-------|------|----------|
| `DatGeracaoConjuntoDados` | DATE | Data de geração do conjunto de dados |
| `IdeConjuntoUnidadeConsumidora` | BIGINT | ID do conjunto de unidades consumidoras |
| `DscConjuntoUnidadeConsumidora` | STRING | Descrição do conjunto de UCs |
| `DscAlimentadorSubestacao` | STRING | Alimentador da subestação |
| `DscSubestacaoDistribuicao` | STRING | Subestação de distribuição |
| `NumOrdemInterrupcao` | STRING | Número da ordem de interrupção |
| `DscTipoInterrupcao` | STRING | Tipo de interrupção (programada, emergencial) |
| `IdeMotivoInterrupcao` | BIGINT | ID do motivo da interrupção |
| `DatInicioInterrupcao` | TIMESTAMP_NTZ | Timestamp de início da interrupção |
| `DatFimInterrupcao` | TIMESTAMP_NTZ | Timestamp de fim da interrupção |
| `DscFatoGeradorInterrupcao` | STRING | Descrição do fato gerador |
| `NumNivelTensao` | BIGINT | Nível de tensão |
| `NumUnidadeConsumidora` | BIGINT | Número de unidades consumidoras afetadas |
| `NumConsumidorConjunto` | BIGINT | Número de consumidores no conjunto |
| `NumAno` | BIGINT | Ano de referência |
| `NomAgenteRegulado` | STRING | Nome da distribuidora |
| `SigAgente` | STRING | Sigla da distribuidora |
| `NumCPFCNPJ` | BIGINT | CNPJ da distribuidora |
| `_ingest_timestamp` | TIMESTAMP | Timestamp da ingestão |
| `_source_url` | STRING | URL do arquivo fonte |
| `_source_last_modified` | STRING | Data de modificação do arquivo fonte |
| `_ano_fonte` | INT | Ano extraído do arquivo fonte |

#### Estratégia de Gravação
- **Modo**: APPEND com verificação prévia de idempotência
- **Controle**: Tabela auxiliar `continuidade_aneel_bronze.controle_ingestao` registra arquivos já processados
- **Validações**:
  - Existência do schema e tabela (criação automática se ausente)
  - Verificação de arquivo já processado antes de ingestão
  - Registro de metadados completos da fonte

#### Fonte de Dados
- **Localização**: Unity Catalog Volume (path não especificado no código)
- **Formato**: Parquet
- **Padrão de arquivo**: `interrupcoes_[ano].parquet`

---

## Camada Silver

### Notebook 201: Transformação de Interrupções para Distribuição
**Path**: `02_silver/201_interrupcoes_distribuicao.py`  
**Responsabilidade**: Padronização, limpeza e cálculo de tempo de interrupção.

**Tabela de Origem**: `workspace.continuidade_aneel_bronze.aneel_interrupcoes`  
**Tabela de Destino**: `workspace.continuidade_aneel_silver.interrupcoes_distribuicao`

#### Schema da Tabela

| Campo | Tipo | Descrição |
|-------|------|----------|
| `cod_distribuidora` | STRING | Código da distribuidora (SigAgente) |
| `data_inicio` | TIMESTAMP | Data e hora de início da interrupção |
| `conjunto` | STRING | Conjunto de unidades consumidoras |
| `tipo_interrupcao` | STRING | Tipo de interrupção |
| `data_fim` | TIMESTAMP | Data e hora de fim da interrupção |
| `origem_interrupcao` | STRING | Origem/fato gerador |
| `tempo_interrupcao_minutos` | DOUBLE | Duração calculada em minutos |
| `_ingest_timestamp` | TIMESTAMP | Timestamp da ingestão bronze |
| `_source_url` | STRING | URL do arquivo fonte |
| `_source_last_modified` | STRING | Data de modificação do arquivo fonte |
| `_ano_fonte` | INT | Ano extraído do arquivo fonte |
| `_silver_timestamp` | TIMESTAMP | Timestamp da transformação silver |

#### Transformações Aplicadas
1. **Renomeação e padronização de campos**:
   - `SigAgente` → `cod_distribuidora`
   - `DatInicioInterrupcao` → `data_inicio`
   - `DatFimInterrupcao` → `data_fim`
   - `DscTipoInterrupcao` → `tipo_interrupcao`
   - `DscFatoGeradorInterrupcao` → `origem_interrupcao`
   - `DscConjuntoUnidadeConsumidora` → `conjunto`

2. **Cálculo de tempo de interrupção**:
   ```python
   tempo_interrupcao_minutos = (data_fim - data_inicio) / 60  # em minutos
   ```

3. **Metadados de linhagem**:
   - Preservação de campos `_ingest_timestamp`, `_source_url`, `_source_last_modified`, `_ano_fonte`
   - Adição de `_silver_timestamp` para rastreabilidade

#### Estratégia de Gravação
- **Modo**: DELETE + APPEND por ano
- **Particionamento lógico**: Por `_ano_fonte`
- **Validações**:
  - Nulos em campos críticos: `cod_distribuidora`, `data_inicio`, `data_fim`
  - Schema esperado (12 campos)
  - (Validação de unicidade de chave não explicitada no código lido)

---

### Notebook 202: Indicadores de Continuidade
**Path**: `02_silver/202_indicadores_continuidade.py`  
**Responsabilidade**: Transformação de indicadores DEC/FEC de formato longo para largo.

**Tabela de Origem**: `workspace.proj_aneel_cont_01_bronze.indicadores_continuidade`  
**Tabela de Destino**: `workspace.proj_aneel_cont_02_silver.indicadores_continuidade`

#### Schema da Tabela

| Campo | Tipo | Descrição |
|-------|------|----------|
| `dscconjundconsumidoras` | STRING | Descrição do conjunto de consumidoras |
| `sigagente` | STRING | Sigla da distribuidora |
| `anoindice` | BIGINT | Ano do índice |
| `_fonte_url` | STRING | URL da fonte |
| `_ingest_ts` | TIMESTAMP | Timestamp de ingestão |
| `dec` | DOUBLE | Duração Equivalente de Interrupção por UC |
| `decinc` | DOUBLE | DEC Índice de Controle |
| `decind` | DOUBLE | DEC Índice de Desempenho |
| `decine` | DOUBLE | DEC Índice de Nível de Excelência |
| `decino` | DOUBLE | DEC Índice Normal |
| `decip` | DOUBLE | DEC Índice Padrão |
| `decipc` | DOUBLE | DEC Índice de Penalidade Contínua |
| `decxn` | DOUBLE | DEC Excluindo Normal |
| `decxnc` | DOUBLE | DEC Excluindo Normal e Controle |
| `decxp` | DOUBLE | DEC Excluindo Padrão |
| `decxpc` | DOUBLE | DEC Excluindo Padrão e Controle |
| `fec` | DOUBLE | Frequência Equivalente de Interrupção por UC |
| `fecinc` | DOUBLE | FEC Índice de Controle |
| `fecind` | DOUBLE | FEC Índice de Desempenho |
| `fecine` | DOUBLE | FEC Índice de Nível de Excelência |
| `fecino` | DOUBLE | FEC Índice Normal |
| `fecip` | DOUBLE | FEC Índice Padrão |
| `fecipc` | DOUBLE | FEC Índice de Penalidade Contínua |
| `fecxn` | DOUBLE | FEC Excluindo Normal |
| `fecxnc` | DOUBLE | FEC Excluindo Normal e Controle |
| `fecxp` | DOUBLE | FEC Excluindo Padrão |
| `fecxpc` | DOUBLE | FEC Excluindo Padrão e Controle |
| `numcon` | DOUBLE | Número de consumidores |
| `codigo_distribuidora` | STRING | Código da distribuidora |
| `distribuidora` | STRING | Nome da distribuidora |
| `ano_referencia` | BIGINT | Ano de referência |
| `periodo_apuracao` | DATE | Período de apuração |
| `_timestamp_transformacao` | TIMESTAMP | Timestamp da transformação |
| `chave_uc` | STRING | Chave única (distribuidora + UC + ano) |

#### Transformações Aplicadas
1. **Pivot de formato longo para largo**: Cada indicador (DEC, FEC, variantes) vira uma coluna
2. **Chave composta para rastreabilidade**: `chave_uc = sigagente + dscconjundconsumidoras + anoindice`
3. **Deduplicação**: Remoção de registros duplicados pela chave

#### Estratégia de Gravação
- **Modo**: DELETE + APPEND por ano
- **Validações**:
  - Nulos em `sigagente`, `anoindice`, `dscconjundconsumidoras`
  - Unicidade da chave `chave_uc`

---

## Camada Gold

### Notebook 301: Tempo de Interrupção por Distribuidora e Mês
**Path**: `03_gold/301_tempo_interrupcao_distribuidora_mes.py`  
**Responsabilidade**: Agregação de tempo total de interrupção por distribuidora, ano e mês.

**Tabela de Origem**: `workspace.continuidade_aneel_silver.interrupcoes_distribuicao`  
**Tabela de Destino**: `workspace.continuidade_aneel_gold.tempo_interrupcao_distribuidora_mes`

#### Schema da Tabela

| Campo | Tipo | Descrição |
|-------|------|----------|
| `cod_distribuidora` | STRING | Código da distribuidora |
| `ano_referencia` | INT | Ano de referência |
| `mes_referencia` | INT | Mês de referência |
| `tempo_total_minutos` | DOUBLE | Tempo total de interrupção em minutos |
| `total_eventos` | BIGINT | Número total de eventos de interrupção |
| `data_primeiro_evento` | TIMESTAMP | Data do primeiro evento no período |
| `data_ultimo_evento` | TIMESTAMP | Data do último evento no período |
| `tempo_total_horas` | DOUBLE | Tempo total de interrupção em horas |
| `_gold_timestamp` | TIMESTAMP | Timestamp da transformação gold |
| `_ano_fonte` | INT | Ano extraído do arquivo fonte (para controle) |

#### Transformações Aplicadas
1. **Agregação**:
   - Agrupamento: `cod_distribuidora`, `ano_referencia`, `mes_referencia`
   - Métricas:
     - `SUM(tempo_interrupcao_minutos)` → `tempo_total_minutos`
     - `COUNT(*)` → `total_eventos`
     - `MIN(data_inicio)` → `data_primeiro_evento`
     - `MAX(data_fim)` → `data_ultimo_evento`
   - Derivação: `tempo_total_horas = tempo_total_minutos / 60`

2. **Chave analítica**: `(cod_distribuidora, ano_referencia, mes_referencia)`

#### Estratégia de Gravação
- **Modo**: DROP + APPEND por ano (DELETE all + INSERT por `_ano_fonte`)
- **Idempotência**: Garantida pela estratégia DROP+APPEND
- **Validações**:
  - Schema esperado (10 campos)
  - Completude: Nenhum nulo em campos de chave (`cod_distribuidora`, `ano_referencia`, `mes_referencia`)
  - Unicidade: Chave `(cod_distribuidora, ano_referencia, mes_referencia)` única
  - Reconciliação: Contagem de registros antes e depois da transformação

---

## Práticas Técnicas Gerais

### Validações de Qualidade
Todos os notebooks implementam:
- **Validação de schema**: Verificação de número de campos e nomes de colunas
- **Validação de completude**: Verificação de nulos em campos críticos de chave
- **Validação de unicidade**: Garantia de que chaves primárias não têm duplicatas
- **Reconciliação**: Comparação de contagens pré e pós-transformação

### Metadados de Linhagem
Todas as camadas preservam:
- `_ingest_timestamp`: Timestamp da ingestão bronze
- `_source_url`: URL do arquivo fonte original
- `_source_last_modified`: Data de modificação do arquivo fonte
- `_ano_fonte`: Ano extraído do arquivo, usado para particionamento lógico

Camadas adicionam timestamp de processamento:
- Silver: `_silver_timestamp`
- Gold: `_gold_timestamp`

### Estratégias de Gravação Resumidas

| Camada | Estratégia | Justificativa |
|--------|-----------|---------------|
| Bronze | APPEND com controle | Preserva histórico completo; idempotência via tabela de controle |
| Silver | DELETE + APPEND por ano | Permite reprocessamento de ano específico; mantém dados históricos de outros anos |
| Gold | DROP + APPEND por ano | Agregações idempotentes; reprocessamento completo do ano por segurança |

---

## Fluxo de Dados Completo

```
Fonte (Volume UC)
    ↓
[101_ingestao_aneel_interrupcoes.py]
    ↓
continuidade_aneel_bronze.aneel_interrupcoes
    ↓
[201_interrupcoes_distribuicao.py]
    ↓
continuidade_aneel_silver.interrupcoes_distribuicao
    ↓
[301_tempo_interrupcao_distribuidora_mes.py]
    ↓
continuidade_aneel_gold.tempo_interrupcao_distribuidora_mes
```

**Linhagem paralela** (indicadores DEC/FEC):
```
Fonte (Volume UC)
    ↓
[Notebook não identificado - ingestão]
    ↓
proj_aneel_cont_01_bronze.indicadores_continuidade
    ↓
[202_indicadores_continuidade.py]
    ↓
proj_aneel_cont_02_silver.indicadores_continuidade
```

---

## Limitações e Pendências Técnicas

1. **Nomenclatura inconsistente**: Dois padrões de schemas coexistindo (`continuidade_aneel_*` vs `proj_aneel_cont_*`)
2. **Notebook de ingestão de indicadores DEC/FEC**: Não identificado na estrutura atual
3. **Path do Volume UC**: Não especificado nos notebooks lidos
4. **Tabela gold para indicadores DEC/FEC**: Não existe em `proj_aneel_cont_03_gold` (vazio)
5. **Validação de unicidade em notebook 201**: Não explicitada no código (presumida pela estratégia)

---

**Data de criação**: 07/set/2026  
**Última atualização**: 07/set/2026  
**Responsável**: Documentação gerada automaticamente via Genie Code