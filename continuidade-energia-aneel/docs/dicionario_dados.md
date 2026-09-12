# Dicionário de Dados

## 1. Interrupções na Rede de Distribuição

### Fonte
* **Dataset**: [A ser preenchido com URL exata]
* **Última atualização**: [A ser preenchido]
* **Formato**: CSV
* **Codificação**: UTF-8

### Campos (exemplo - atualizar após análise dos dados reais)

| Campo | Tipo | Descrição | Exemplo |
|-------|------|-------------|----------|
| `cod_distribuidora` | String | Código único da distribuidora | "CEG" |
| `nome_distribuidora` | String | Nome da distribuidora | "Light SESA" |
| `uf` | String | Unidade Federativa | "RJ" |
| `data_inicio` | Date | Data/hora de início da interrupção | "2024-01-15 14:30:00" |
| `data_fim` | Date | Data/hora de fim da interrupção | "2024-01-15 16:45:00" |
| `duracao_minutos` | Integer | Duração em minutos | 135 |
| `consumidores_afetados` | Integer | Número de consumidores sem energia | 1250 |
| `tipo_interrupcao` | String | Classificação da interrupção | "Programada", "Emergencial" |
| `causa` | String | Causa da interrupção | "Manutenção", "Fenômeno natural" |
| `conjunto` | String | Conjunto elétrico afetado | "SE-001" |

### Regras de Negócio
* Interrupções program as podem não contar para cálculo do DEC
* Duração = data_fim - data_inicio
* Excluir registros com duracao_minutos <= 0

---

## 2. Indicadores Coletivos de Continuidade

### Fonte
* **Dataset**: [Indicadores Coletivos de Continuidade - ANEEL](https://dadosabertos.aneel.gov.br/dataset/indicadores-coletivos-de-continuidade)
* **URL do arquivo**: `https://dadosabertos.aneel.gov.br/dataset/d5f0712e-62f6-4736-8dff-9991f10758a7/resource/d7f70fb1-725c-4748-afeb-65c6a78df550/download/indicadores-continuidade-coletivos-2020-2029.parquet`
* **Última atualização**: 11/09/2026 (Run ID: 20260911032756)
* **Formato**: Parquet
* **Codificação**: UTF-8
* **Tabela bronze**: `workspace.proj_aneel_cont_01_bronze.indicadores_continuidade`
* **Total de registros**: 5.108.332
* **Cobertura temporal**: jan/2020 a ago/2026 (80 períodos mensais)
* **Distribuidoras**: 105 agentes

### Campos (schema da tabela bronze)

| Campo | Tipo | Descrição | Exemplo |
|-------|------|-------------|----------|
| `datgeracaoconjuntodados` | DATE | Data de geração do conjunto de dados pela ANEEL | 2026-09-05 |
| `ideconjundconsumidoras` | BIGINT | ID do conjunto de unidades consumidoras | 16658 |
| `dscconjundconsumidoras` | STRING | Descrição do conjunto de unidades consumidoras | "CERSAD" |
| `sigagente` | STRING | Sigla do agente (distribuidora) | "CEMIG-D" |
| `numcnpj` | BIGINT | CNPJ da distribuidora | 6981180000116 |
| `sigindicador` | STRING | Sigla do indicador (DEC, FEC, etc.) | "DEC" |
| `anoindice` | BIGINT | Ano de referência do índice | 2025 |
| `numperiodoindice` | BIGINT | Número do período (1-12, mensal) | 6 |
| `vlrindiceenviado` | DOUBLE | Valor do indicador enviado pela distribuidora | 0.63 |
| `_fonte_url` | STRING | URL do arquivo fonte baixado (metadado) | https://dadosabertos.aneel.gov.br/... |
| `_ingest_ts` | TIMESTAMP | Timestamp UTC da ingestão (metadado) | 2026-09-11T03:27:56Z |
| `_ingest_date` | DATE | Data da ingestão (metadado) | 2026-09-11 |
| `_run_id` | STRING | ID único da execução (metadado) | 20260911032756 |

### Indicadores disponíveis (23 tipos)

| Sigla | Descrição |
|-------|-----------|
| DEC | Duração Equivalente de Interrupção por Unidade Consumidora |
| FEC | Frequência Equivalente de Interrupção por Unidade Consumidora |
| DECIP / FECIP | DEC/FEC Individual Programada |
| DECIPC / FECIPC | DEC/FEC Individual Programada Contínua |
| DECIND / FECIND | DEC/FEC Individual Não Programada |
| DECINC / FECINC | DEC/FEC Individual Contínua |
| DECINE / FECINE | DEC/FEC Individual Não Programada Especial |
| DECINO / FECINO | DEC/FEC Individual Não Programada Outros |
| DECXP / FECXP | DEC/FEC Extra Programada |
| DECXPC / FECXPC | DEC/FEC Extra Programada Contínua |
| DECXN / FECXN | DEC/FEC Extra Não Programada |
| DECXNC / FECXNC | DEC/FEC Extra Não Programada Contínua |
| NumCon | Número de Consumidores |

### Notas sobre os dados
* Colunas originais da fonte em PascalCase (ex: `DatGeracaoConjuntoDados`), normalizadas para lowercase na bronze
* O valor `vlrindiceenviado` pode ser 0 (zero) para indicadores não aplicáveis ao período/conjunto
* `numperiodoindice` varia de 1 a 12 (períodos mensais)

### Fórmulas Oficiais

**DEC (Duração Equivalente de Interrupção por Unidade Consumidora)**
```
DEC = Σ(Ca(i) × t(i)) / Cs

Onde:
- Ca(i) = Número de consumidores afetados na interrupção i
- t(i) = Tempo de duração da interrupção i (em horas)
- Cs = Total de consumidores atendidos
```

**FEC (Frequência Equivalente de Interrupção por Unidade Consumidora)**
```
FEC = Σ(Ca(i)) / Cs

Onde:
- Ca(i) = Número de consumidores afetados na interrupção i
- Cs = Total de consumidores atendidos
```

### Notas
* DEC e FEC são os principais indicadores de qualidade de fornecimento
* Valores em horas para DEC
* Podem haver metas diferentes por região e tipo de distribuidora

---

## 3. Dados Processados

### `interrupcoes_processado.parquet`
* Dados de interrupções limpos e padronizados
* Campos adicionais: região, categoria_distribuidora
* Apenas interrupções válidas para cálculo de DEC

### `indicadores_processado.parquet`
* Indicadores oficiais consolidados
* Normalizado para comparação com DEC calculado

### `analise_dec.parquet`
* DEC calculado vs. DEC oficial por distribuidora
* Diferenças absolutas e percentuais
* Flags de discrepância

---

## Glossário

* **DEC**: Duração Equivalente de Interrupção por Unidade Consumidora (em horas)
* **FEC**: Frequência Equivalente de Interrupção por Unidade Consumidora (quantidade)
* **ANEEL**: Agência Nacional de Energia Elétrica
* **Distribuidora**: Concessionária responsável pela distribuição de energia em uma área
* **Conjunto**: Agrupamento de equipamentos da rede elétrica

---

**Última atualização**: 11/09/2026