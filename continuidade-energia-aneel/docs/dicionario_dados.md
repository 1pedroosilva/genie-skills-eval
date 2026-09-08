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
| `ano` | Integer | Ano de referência | 2024 |
| `mes` | Integer | Mês de referência (ou 0 para anual) | 12 |
| `periodo` | String | Período de apuração | "Anual", "Mensal" |
| `dec_apurado` | Float | DEC oficial em horas | 12.5 |
| `dec_meta` | Float | Meta regulatória de DEC | 10.0 |
| `fec_apurado` | Float | FEC oficial | 8.2 |
| `fec_meta` | Float | Meta regulatória de FEC | 7.0 |
| `total_consumidores` | Integer | Total de consumidores no período | 850000 |

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

**Última atualização**: [A ser preenchido após primeira extração de dados]