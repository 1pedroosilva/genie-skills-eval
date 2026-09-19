# Documentação: Notebook Gold - Tempo de Interrupção por Distribuidora e Mês

## 📋 Identificação

**Nome**: Gold - Tempo Interrupção por Distribuidora e Mês  
**Projeto**: Continuidade de Fornecimento de Energia (ANEEL)  
**Camada**: Gold (Analítica)  
**Localização**: `continuidade-energia-aneel/notebooks/`

---

## 🎯 Objetivo

Este notebook processa e agrega dados de interrupções de energia elétrica para gerar uma tabela analítica mensal por distribuidora. O objetivo principal é:

* **Agregar** tempo total de interrupção (DEC) e frequência (FEC) por distribuidora e mês
* **Calcular** métricas estatísticas (média, mínimo, máximo, total) dos indicadores
* **Analisar** evolução temporal com comparação mês a mês e ranking
* **Permitir** identificação de distribuidoras com maior tempo de interrupção e anomalias

---

## 🔄 Fluxo de Processamento

```
┌─────────────────────────────────────────────────────────┐
│  Origem: indicadores_continuidade (Silver)              │
│  workspace.proj_aneel_cont_02_silver                    │
└───────────────────────┬─────────────────────────────────┘
                        ↓
            ┌───────────────────────┐
            │  Extração Temporal    │
            │  (ano, mês, ano_mes)  │
            └───────────┬───────────┘
                        ↓
            ┌───────────────────────┐
            │  Agregação Mensal     │
            │  por Distribuidora    │
            └───────────┬───────────┘
                        ↓
            ┌───────────────────────┐
            │  Enriquecimento       │
            │  • Evolução temporal  │
            │  • Variação %         │
            │  • Ranking            │
            └───────────┬───────────┘
                        ↓
┌───────────────────────┴─────────────────────────────────┐
│  Destino: tempo_interrupcao_mensal (Gold)               │
│  workspace.proj_aneel_cont_03_gold                      │
└─────────────────────────────────────────────────────────┘
```

---

## 📊 Tabelas Envolvidas

### Entrada

**Tabela**: `workspace.proj_aneel_cont_02_silver.indicadores_continuidade`  
**Descrição**: Indicadores de continuidade processados da camada Silver, contendo métricas DEC, FEC e informações das distribuidoras.

### Saída

**Tabela**: `workspace.proj_aneel_cont_03_gold.tempo_interrupcao_mensal`  
**Descrição**: Tabela analítica agregada por distribuidora e mês.  
**Modo**: Overwrite (sobrescreve dados existentes a cada execução)  
**Formato**: Delta Lake

---

## 📈 Indicadores Calculados

### 1. DEC - Duração Equivalente de Interrupção

Mede o **tempo total** de interrupção por consumidor:

* `dec_medio`: Média do DEC no mês
* `dec_minimo`: Menor valor de DEC registrado
* `dec_maximo`: Maior valor de DEC registrado
* `dec_total`: Soma total do DEC

### 2. DEC Indicador (sem interrupções programadas)

* `decind_medio`: Média do DEC descontando interrupções programadas
* `decind_total`: Total do DEC indicador

### 3. DEC Conformidade

* `decinc_medio`: Média do DEC dentro dos limites regulatórios
* `decinc_total`: Total do DEC em conformidade

### 4. FEC - Frequência Equivalente de Interrupção

Mede a **frequência** de interrupções:

* `fec_medio`: Média de interrupções por consumidor
* `fec_total`: Total de interrupções

### 5. Consumidores

* `numcon_medio`: Número médio de consumidores
* `numcon_total`: Total de consumidores
* `qtd_conjuntos`: Quantidade de conjuntos agregados

### 6. Análise Temporal (Enriquecimento)

* `dec_medio_mes_anterior`: DEC médio do mês anterior (mesma distribuidora)
* `variacao_perc_mes_anterior`: Variação percentual mês a mês
* `ranking_dec_mes`: Posição no ranking mensal (distribuidoras com maior DEC)
* `timestamp_processamento`: Data/hora do processamento

---

## 🔑 Campos de Identificação

* `distribuidora`: Nome da distribuidora de energia
* `sigagente`: Sigla da distribuidora
* `codigo_distribuidora`: Código único da distribuidora
* `ano`: Ano de referência
* `mes`: Mês de referência (1-12)
* `ano_mes`: Data no formato ano-mês (primeiro dia do mês)

---

## 💡 Casos de Uso

### 1. Identificação de Distribuidoras Críticas
Listar as 10 distribuidoras com maior tempo médio de interrupção em um ano.

### 2. Análise de Tendências
Acompanhar a evolução mensal do DEC de uma distribuidora específica.

### 3. Detecção de Anomalias
Identificar aumentos súbitos (variação percentual alta) no tempo de interrupção.

### 4. Comparação com Metas Regulatórias
Comparar DEC observado com limites estabelecidos pela ANEEL.

### 5. Ranking Mensal
Visualizar quais distribuidoras apresentam os piores indicadores em cada mês.

---

## 🔍 Validações Incluídas

O notebook inclui células de validação SQL:

1. **Contagem por ano**: Registros e distribuidoras únicas por ano
2. **Top 10 distribuidoras**: Maiores valores de DEC médio anual
3. **Evolução temporal**: Série histórica de uma distribuidora específica

---

## ⚙️ Tecnologias e Métodos

* **PySpark**: Processamento distribuído dos dados
* **Window Functions**: Cálculo de métricas de evolução temporal e ranking
* **Delta Lake**: Formato de armazenamento com suporte a ACID
* **Agregação**: GroupBy com múltiplas métricas estatísticas
* **Overwrite Schema**: Permite evolução do schema da tabela

---

## 📅 Frequência de Execução

**Sugerida**: Mensal, após atualização da tabela Silver de indicadores.

---

## 👤 Responsável

Projeto Continuidade de Fornecimento ANEEL

---

## 📝 Observações

* O notebook utiliza modo `overwrite`, substituindo completamente os dados existentes a cada execução
* A análise de evolução temporal (mês anterior) depende de dados históricos na tabela
* O ranking é calculado dentro de cada mês (distribuidoras são ranqueadas mensalmente)
* Indicadores DEC são medidos em horas de interrupção por consumidor
* Indicadores FEC representam número de interrupções por consumidor

---

**Última atualização**: 13/09/2026  
**Versão**: 1.0
