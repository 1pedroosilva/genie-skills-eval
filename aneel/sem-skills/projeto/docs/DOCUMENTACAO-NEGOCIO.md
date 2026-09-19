# Documentação de Negócio — Continuidade de Fornecimento de Energia

## 1. Visão Executiva

O projeto **Continuidade de Fornecimento de Energia** é uma iniciativa de análise de dados que utiliza dados públicos da ANEEL (Agência Nacional de Energia Elétrica) para monitorar e avaliar a qualidade do fornecimento de energia elétrica no Brasil. O projeto responde à pergunta central:

> **O tempo de interrupção observado em cada distribuidora está compatível com o DEC apurado pelo regulador?**

A resposta a esta pergunta permite identificar discrepâncias entre os indicadores calculados a partir dos eventos de interrupção reais e os indicadores oficiais publicados pela ANEEL, suportando decisões regulatórias, operacionais e estratégicas.

**Status atual**: 🟢 Pipeline completa e operacional — todas as camadas Bronze → Silver → Gold funcionando com 9,2 milhões de registros de interrupções e 5,1 milhões de registros de indicadores processados.

---

## 2. Objetivos de Negócio

### Objetivo Principal
Analisar indicadores de continuidade do fornecimento de energia (DEC, FEC, DIC, FIC, DMIC e variantes) para:

* Monitorar a qualidade do serviço das distribuidoras de energia elétrica
* Identificar padrões de interrupções no fornecimento
* Suportar decisões estratégicas sobre continuidade de fornecimento
* Criar métricas e dashboards de acompanhamento

### Objetivos Específicos

1. **Comparar** o DEC observado (calculado a partir dos eventos de interrupção) com o DEC oficial (apurado pela ANEEL) por distribuidora
2. **Identificar** discrepâncias entre os valores observados e oficiais, investigando causas
3. **Ranquear** distribuidoras por qualidade de fornecimento, destacando as mais críticas
4. **Detectar** anomalias e tendências temporais nos indicadores de continuidade
5. **Disponibilizar** dados analíticos prontos para consumo em dashboards e relatórios

---

## 3. Escopo

### Cobertura Geográfica
* Brasil inteiro
* 105 distribuidoras de energia elétrica reguladas pela ANEEL

### Cobertura Temporal
* Interrupções: ano de 2024 (9.211.251 registros)
* Indicadores oficiais: janeiro/2020 a agosto/2026 (80 períodos mensais)
* Período de análise definido: 2 últimos anos fechados (2024 e 2025)

### Volume de Dados

| Conjunto de Dados | Volume | Período |
|---|---|---|
| Interrupções (Bronze) | 9.211.251 registros | 2024 |
| Indicadores Coletivos (Bronze) | 5.108.332 registros | jan/2020 a ago/2026 |
| Interrupções tratadas (Silver) | 9.086.036 registros | 2024 |
| Indicadores tratados (Silver) | 5.108.332 registros | jan/2020 a ago/2026 |
| Agregação mensal (Gold) | 8.082 registros | jan/2020 a ago/2026 |

---

## 4. Indicadores de Negócio

### Indicadores Principais

#### DEC — Duração Equivalente de Interrupção por Unidade Consumidora
Mede o **tempo médio** que cada unidade consumidora ficou sem energia em um período.

**Fórmula oficial:**
```
DEC = Σ(Ca(i) × t(i)) / Cs

Onde:
  Ca(i) = Número de consumidores afetados na interrupção i
  t(i)  = Tempo de duração da interrupção i (em horas)
  Cs   = Total de consumidores atendidos (do conjunto)
```
* **Unidade**: horas
* **Interpretação**: quanto maior o DEC, pior a qualidade do fornecimento

#### FEC — Frequência Equivalente de Interrupção por Unidade Consumidora
Mede o **número médio** de interrupções por unidade consumidora em um período.

**Fórmula oficial:**
```
FEC = Σ(Ca(i)) / Cs

Onde:
  Ca(i) = Número de consumidores afetados na interrupção i
  Cs   = Total de consumidores atendidos (do conjunto)
```
* **Unidade**: quantidade (adimensional)
* **Interpretação**: quanto maior o FEC, mais frequente são as interrupções

### Indicadores Complementares (23 tipos no total)

| Sigla | Descrição |
|---|---|
| DEC / FEC | Duração e Frequência Equivalente total |
| DECIP / FECIP | Individual Programada |
| DECIPC / FECIPC | Individual Programada Contínua |
| DECIND / FECIND | Individual Não Programada |
| DECINC / FECINC | Individual Contínua |
| DECINE / FECINE | Individual Não Programada Especial |
| DECINO / FECINO | Individual Não Programada Outros |
| DECXP / FECXP | Extra Programada |
| DECXPC / FECXPC | Extra Programada Contínua |
| DECXN / FECXN | Extra Não Programada |
| DECXNC / FECXNC | Extra Não Programada Contínua |
| NumCon | Número de Consumidores do conjunto |

### Métricas Calculadas na Camada Gold

A camada Gold agrega os indicadores por distribuidora e mês, calculando:

* **DEC**: médio, mínimo, máximo e total
* **DEC Indicador** (sem interrupções programadas): médio e total
* **DEC Conformidade** (dentro dos limites regulatórios): médio e total
* **FEC**: médio e total
* **Consumidores**: número médio, total e quantidade de conjuntos
* **Análise temporal**: variação percentual mês a mês, ranking mensal

---

## 5. Fontes de Dados

### ANEEL — Dados Abertos

Todas as fontes são públicas e gratuitas, disponibilizadas pela ANEEL em seu portal de dados abertos.

#### Fonte 1: Interrupções na Rede de Distribuição
* **Portal**: [Dados Abertos ANEEL](https://dadosabertos.aneel.gov.br/dataset/interrupcoes-na-rede-de-distribuicao)
* **Formato**: Parquet
* **Conteúdo**: Registros detalhados de cada interrupção no fornecimento de energia (evento individual), incluindo distribuidora, conjunto de unidades consumidoras, data/hora de início e fim, tipo (programada/não programada), causa (fato gerador), número de unidades afetadas, nível de tensão
* **Frequência de atualização**: Anual
* **Granularidade**: Uma linha por interrupção × unidade consumidora afetada

#### Fonte 2: Indicadores Coletivos de Continuidade
* **Portal**: [Dados Abertos ANEEL](https://dadosabertos.aneel.gov.br/dataset/indicadores-coletivos-de-continuidade)
* **URL do arquivo**: `https://dadosabertos.aneel.gov.br/dataset/d5f0712e-62f6-4736-8dff-9991f10758a7/resource/d7f70fb1-725c-4748-afeb-65c6a78df550/download/indicadores-continuidade-coletivos-2020-2029.parquet`
* **Formato**: Parquet (arquivo único, republicado a cada atualização)
* **Conteúdo**: Indicadores DEC, FEC e variantes apurados oficialmente pela ANEEL, por distribuidora, conjunto de unidades consumidoras, ano e mês
* **Frequência de atualização**: Mensal/Trimestral (conforme publicação da ANEEL)
* **Granularidade**: Uma linha por indicador × distribuidora × conjunto × ano × mês

---

## 6. Processos de Negócio

### Fluxo de Análise

```
1. Coletar dados de interrupções (eventos individuais) da ANEEL
       ↓
2. Tratar e limpar dados (remover inválidos, padronizar, enriquecer)
       ↓
3. Calcular DEC observado a partir dos eventos de interrupção
       ↓
4. Coletar DEC oficial apurado pela ANEEL
       ↓
5. Comparar DEC observado vs. DEC oficial por distribuidora
       ↓
6. Identificar discrepâncias e investigar causas
       ↓
7. Gerar relatórios, rankings e alertas
```

### Regras de Negócio

1. **Interrupções programadas** podem não contar para o cálculo do DEC — são excluídas nas análises comparativas
2. **Duração** = data/hora de fim − data/hora de início da interrupção
3. **Registros inválidos** são excluídos: duração ≤ 0, unidades consumidoras = 0, tensão = 0
4. **Duplicatas** são removidas pela chave natural, mantendo a versão mais recente
5. **Limiares de discrepância** (definidos em `config.yaml`):
   * Diferença > 10%: aviso (warning)
   * Diferença > 20%: alerta crítico
6. **Fato gerador** é decomposto em 4 níveis hierárquicos: origem, natureza, categoria e detalhe

### Tipos de Interrupção
* **Não Programada** (98,01% dos registros): interrupções imprevistas — são as que contam para o DEC/FEC
* **Programada** (1,99% dos registros): interrupções planejadas (manutenção, etc.) — excluídas do cálculo principal

---

## 7. Insights e Descobertas

### Principais Descobertas (EDA Completa — 14/09/2026)

1. **Distribuidora com mais interrupções**: CEMIG-D lidera com 975 mil interrupções no período
2. **Distribuidora com pior DEC médio**: ÂMBAR ENERGIA RR (5,41 horas de interrupção por consumidor)
3. **DEC médio geral**: 1,04 horas (todos os conjuntos/períodos)
4. **FEC médio geral**: 0,71 interrupções por consumidor
5. **Duração média das interrupções**: 7,52 horas (máximo: 133 dias)
6. **Qualidade dos dados**: 98,36% dos registros bronze são válidos após limpeza
7. **Volatilidade mensal**: variação média de +56,49% no DEC entre meses consecutivos
8. **Cobertura**: 105 distribuidoras em todo o território nacional

### Casos de Uso da Camada Gold

1. **Identificação de distribuidoras críticas**: Top 10 distribuidoras com maior tempo médio de interrupção
2. **Análise de tendências**: Evolução mensal do DEC de uma distribuidora específica
3. **Detecção de anomalias**: Identificar aumentos súbitos no tempo de interrupção
4. **Comparação com metas regulatórias**: Avaliar conformidade com limites estabelecidos pela ANEEL
5. **Ranking mensal**: Visualizar distribuidoras com piores indicadores em cada mês

---

## 8. Stakeholders

| Papel | Responsabilidade | Interesse |
|---|---|---|
| Autor do Projeto | Pedro Silva | Desenvolvimento, manutenção e evolução |
| Equipe de Regulação | Análise de conformidade | Discrepâncias DEC observado vs. oficial |
| Equipe de Operações | Monitoramento de qualidade | Identificação de distribuidoras críticas |
| Equipe de Análise | Exploração de dados | Insights, tendências e padrões |

---

## 9. Glossário de Termos de Negócio

| Termo | Definição |
|---|---|
| **DEC** | Duração Equivalente de Interrupção por Unidade Consumidora (em horas) |
| **FEC** | Frequência Equivalente de Interrupção por Unidade Consumidora (quantidade) |
| **ANEEL** | Agência Nacional de Energia Elétrica — órgão regulador do setor elétrico brasileiro |
| **Distribuidora** | Concessionária responsável pela distribuição de energia em uma área geográfica |
| **Conjunto** | Agrupamento de equipamentos da rede elétrica que atende um conjunto de unidades consumidoras |
| **Unidade Consumidora** | Consumidor final que recebe energia da distribuidora |
| **Interrupção Programada** | Interrupção planejada (manutenção, obras, etc.) |
| **Interrupção Não Programada** | Interrupção imprevista (falhas, eventos externos, etc.) |
| **Fato Gerador** | Causa da interrupção, estruturada em 4 níveis: origem, natureza, categoria, detalhe |
| **DECIP/DECIND/DECXP** | Variantes de DEC para diferentes categorias (programada, não programada, extra) |
| **NumCon** | Número de consumidores do conjunto |

---

## 10. Referências

* **Portal ANEEL Dados Abertos**: https://dadosabertos.aneel.gov.br
* **Indicadores Coletivos de Continuidade**: https://www.aneel.gov.br/indicadores-coletivos-de-continuidade
* **Dataset Interrupções**: https://dadosabertos.aneel.gov.br/dataset/interrupcoes-na-rede-de-distribuicao
* **Dataset Indicadores**: https://dadosabertos.aneel.gov.br/dataset/indicadores-coletivos-de-continuidade
* **Configuração do projeto**: `continuidade-energia-aneel/config.yaml`

---

## 11. Histórico de Versões

| Data | Versão | Descrição |
|---|---|---|
| 08/09/2026 | 0.1 | Criação do projeto e ingestão bronze de interrupções |
| 11/09/2026 | 0.2 | Ingestão bronze de indicadores coletivos (5.108.332 registros) |
| 12/09/2026 | 0.3 | Transformação silver de interrupções (495.350 registros) |
| 12/09/2026 | 0.4 | Análise de qualidade de dados bronze (98,36% válidos) |
| 13/09/2026 | 0.5 | Transformação silver de indicadores e documentação Gold |
| 14/09/2026 | 1.0 | Pipeline completa Bronze → Silver → Gold operacional, EDA concluída |
| 16/09/2026 | 1.1 | Documentação de negócio e técnica criada |

---

*Documento criado em: 16/09/2026*
*Autor: Pedro Silva*
*Status: 🟢 Projeto em operação*