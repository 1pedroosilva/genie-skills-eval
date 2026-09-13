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
  - `101_interrupcoes`: Dados brutos de interrupções da ANEEL — 500.000 registros (ano 2024)

#### **Camada Silver** (Parcialmente implementada)
- **Catálogo/Schema**: `workspace.proj_aneel_cont_01_silver`
- **Tabelas**:
  - `interrupcoes`: Dados tratados de interrupções prontos para análise por distribuidora e conjunto de unidades consumidoras — 495.350 registros
- Limpeza e transformações aplicadas:
  - Normalização de nomes (PascalCase → snake_case)
  - Trim de strings, cálculo de duração, decomposição do fato gerador
  - Formatação de CNPJ, conversão de tensão (V → kV), derivação temporal
  - Filtros de qualidade (duração ≤ 0, unidades = 0, tensão = 0)
  - Metadados de processamento silver

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

### ✅ Sessão: 12/09/2026 02:03

#### 3. Transformação Silver — Interrupções de Energia

**Notebook criado**: [Silver - Interrupções de Energia](#notebook-3059658603716946)

**Descrição**: Pipeline de transformação completo da camada bronze para a camada silver dos dados de interrupções de energia, aplicando limpeza, padronização e enriquecimento. O resultado é uma tabela pronta para análise por distribuidora e por conjunto de unidades consumidoras.

**Funcionalidades implementadas**:
- ✅ Normalização de nomes de colunas (PascalCase → snake_case, 18 colunas)
- ✅ Limpeza de strings (trim em 8 colunas texto)
- ✅ Cálculo de duração (segundos, minutos, horas)
- ✅ Decomposição do fato gerador em 4 níveis hierárquicos (origem, natureza, categoria, detalhe) com tratamento de separadores mistos (`;` e `-`)
- ✅ Formatação de CNPJ com zeros à esquerda (14 dígitos)
- ✅ Conversão de nível de tensão (volts → kV)
- ✅ Derivação temporal (data, mês, dia da semana, hora de início)
- ✅ Filtros de qualidade (remoção de duração ≤ 0, unidades consumidoras = 0, tensão = 0)
- ✅ Metadados de processamento silver (`_silver_ts`, `_silver_run_id`, `_origem_tabela`)
- ✅ Validação pós-transformação (cobertura temporal, distribuição por distribuidora, verificação de qualidade)
- ✅ Consultas analíticas de exemplo (DEC/FEC equivalente por distribuidora, ranking de conjuntos, análise por causa)

**Origem**: `workspace.proj_aneel_cont_01_bronze.101_interrupcoes`
**Destino**: `workspace.proj_aneel_cont_01_silver.interrupcoes`

**Modo de execução**: Full load (overwrite)

**Resultado da execução**:
- 500.000 registros lidos da bronze
- 4.650 registros removidos por filtros de qualidade (0,93%)
- 495.350 registros válidos gravados na silver
- 34 colunas no schema final
- 4 distribuidoras: EQUATORIAL MA (210.527), ETO (157.352), EAC (75.172), CEA (52.299)
- 98,01% interrupções Não Programadas, 1,99% Programadas
- Zero registros inválidos pós-filtro (duração, unidades, tensão, agente)
- Run ID: 20260912050353

**Correção aplicada durante execução**:
- Erro `INVALID_ARRAY_INDEX` na decomposição do fato gerador (registros com menos de 4 níveis separados por `;`/`-`). Corrigido com `F.when(F.size(partes) >= N, ...)` para acesso seguro ao array.

---

### ✅ Sessão: 12/09/2026 23:46 - 13/09/2026 00:01

#### 4. Análise de Qualidade de Dados — Interrupções Bronze

**Notebook criado**: [Análise de Qualidade - Interrupções Bronze](#notebook-4356906633627129)

**Descrição**: Investigação detalhada da qualidade dos dados na tabela bronze de interrupções (`workspace.proj_aneel_cont_01_bronze.101_interrupcoes`) para definir e validar regras de tratamento antes da transformação silver.

**Funcionalidades implementadas**:
- ✅ Análise de campos nulos em colunas críticas (NumOrdemInterrupcao, DatInicioInterrupcao, DatFimInterrupcao, DscTipoInterrupcao, NumUnidadeConsumidora, NumConsumidorConjunto)
- ✅ Identificação de durações inválidas (data fim ≤ data início)
- ✅ Análise de distribuição de durações (classificação em faixas: até 24h, 1-6 dias, 7-30 dias, >30 dias)
- ✅ Detecção de duplicatas na chave natural (NumOrdemInterrupcao + NumUnidadeConsumidora)
- ✅ Identificação de unidades consumidoras inválidas (NumUnidadeConsumidora = 0)
- ✅ Análise de tipos de interrupção (Programada vs Não Programada)
- ✅ Estatísticas gerais (durações média/mediana, consumidores afetados, distribuição temporal)

**Problemas identificados**:

| Problema | Quantidade | % do Total | Severidade | Ação Definida |
|----------|-----------|-----------|------------|---------------|
| Campos nulos críticos | 0 | 0,00% | - | ✅ Nenhuma |
| Durações zero (início = fim) | 2.702 | 0,54% | ⚠️ Média | Excluir |
| Durações > 30 dias | 5 | 0,00% | ⚠️ Média | Marcar com flag |
| Durações 7-30 dias | 298 | 0,06% | ⚠️ Baixa | Marcar com flag |
| Duplicatas (chave natural) | 3.608 | 0,72% | ⚠️ Alta | Deduplicar |
| UCs inválidas (=0) | 1.946 | 0,39% | ⚠️ Alta | Excluir |
| **Total com problemas** | **~8.559** | **1,71%** | - | - |

**Regras de tratamento definidas e validadas**:

1. **Exclusão de registros inválidos**:
   ```sql
   WHERE NumUnidadeConsumidora > 0  -- Excluir UCs inválidas
     AND DatFimInterrupcao > DatInicioInterrupcao  -- Excluir durações zero/negativas
   ```

2. **Deduplicação por chave natural**:
   ```sql
   ROW_NUMBER() OVER (
     PARTITION BY NumOrdemInterrupcao, NumUnidadeConsumidora 
     ORDER BY DatGeracaoConjuntoDados DESC, DatFimInterrupcao DESC
   ) = 1
   ```
   **Justificativa**: Registros com `DatGeracaoConjuntoDados` mais recente e `DatFimInterrupcao` mais recente representam as versões mais atualizadas do evento.

3. **Marcação de durações extremas** (flag para análise posterior, sem exclusão):
   ```sql
   CASE 
     WHEN TIMESTAMPDIFF(DAY, DatInicioInterrupcao, DatFimInterrupcao) > 30 
       THEN 'Duração extrema'
     WHEN TIMESTAMPDIFF(DAY, DatInicioInterrupcao, DatFimInterrupcao) BETWEEN 7 AND 30 
       THEN 'Duração longa'
     ELSE 'Normal'
   END as flag_duracao
   ```

4. **Cálculo de métricas derivadas**:
   - Duração em minutos: `TIMESTAMPDIFF(MINUTE, DatInicioInterrupcao, DatFimInterrupcao)`
   - Duração em horas (decimal): `duracao_minutos / 60.0`
   - Flag de tipo programado: `CASE WHEN DscTipoInterrupcao = 'Programada' THEN TRUE ELSE FALSE END`

**Validações realizadas (provas de qualidade)**:

- ✅ **Prova 1 — Zero duplicatas**: Confirmado que após aplicação da regra ROW_NUMBER, não restam duplicatas na chave natural (491.822 registros = 491.822 chaves únicas)
- ✅ **Prova 2 — Durações válidas**: Confirmado que após filtros, todas as durações são > 0 (duração mínima = 10 segundos, nenhum registro com duração ≤ 0)
- ✅ **Prova 3 — UCs válidas**: Confirmado que após filtros, todas as UCs são > 0 (UC mínima = 1, zero registros com UC = 0)

**Resultado da análise**:
- 500.000 registros no Bronze
- 491.822 registros válidos esperados no Silver (98,36% de aproveitamento)
- 8.178 registros a serem excluídos (1,64%)
- Durações: média 483 minutos (~8h), mediana 238 minutos (~4h)
- 93,24% das interrupções duram até 24 horas
- Consumidores afetados: média 28.740, máximo 92.606

**Conclusão**:
- ✅ Qualidade dos dados Bronze é adequada (98,36% dos registros são válidos)
- ✅ Problemas identificados são conhecidos e tratáveis
- ✅ Regras de tratamento definidas e validadas com queries SQL
- ✅ Dados prontos para transformação Silver com as regras documentadas
- ⏭️ Próximo passo: Implementar transformação Silver com as regras validadas

**Observações técnicas**:
- Chave natural identificada: `(NumOrdemInterrupcao, NumUnidadeConsumidora)` — uma mesma interrupção pode afetar múltiplas unidades consumidoras
- Duplicatas representam múltiplas versões/atualizações do mesmo evento na fonte (diferentes `DatGeracaoConjuntoDados` ou `DatFimInterrupcao`)
- Durações zero representam interrupções instantâneas ou erros de registro
- Alguns registros com durações extremas (>30 dias) seguem padrão "mês inteiro" (01/XX 00:01 até 31/XX 23:59), possivelmente dados sintéticos ou interrupções crônicas
- A análise considerou apenas registros do ano 2024

---

## Próximos Passos

### Camada Silver
- [x] Criar transformações de limpeza e padronização (interrupções)
- [x] Implementar validações de qualidade de dados (interrupções)
- [ ] Criar transformação silver dos indicadores de continuidade
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
│   ├── Silver - Interrupções de Energia                    # Notebook de transformação silver (interrupções)
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
| 12/09/2026 02:03 | Transformação silver de interrupções (495.350 registros, 34 colunas) |
| 12/09/2026 23:46 - 13/09/2026 00:01 | Análise de qualidade de dados bronze (interrupções): 98,36% válidos, regras de tratamento validadas |

---

*Última atualização: 13/09/2026*