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
- **Catálogo**: `main`
- **Schema**: `bronze_aneel`
- **Tabelas**:
  - `interrupcoes_energia`: Dados brutos de interrupções da ANEEL

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

### ANEEL - Indicadores de Continuidade
- **Portal**: [Dados Abertos ANEEL](https://dadosabertos.aneel.gov.br/dataset/indicadores-coletivos-de-continuidade)
- **Formato**: Parquet (arquivo anual, republicado a cada atualização)
- **Frequência de atualização**: Mensal/Trimestral (conforme publicação da ANEEL)
- **Indicadores incluídos**:
  - **DEC**: Duração Equivalente de Interrupção por Unidade Consumidora
  - **FEC**: Frequência Equivalente de Interrupção por Unidade Consumidora
  - **DIC**: Duração de Interrupção Individual
  - **FIC**: Frequência de Interrupção Individual
  - **DMIC**: Duração Máxima de Interrupção Contínua

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
continuidade-energia-aneel/docs/
├── PROJETO-Continuidade-Fornecimento.md    # Este arquivo
└── Ingestão ANEEL - Interrupções de Energia.ipynb    # Notebook de ingestão
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
| 08/09/2026 22:41 | Criação do projeto e implementação da ingestão bronze |

---

*Última atualização: 08/09/2026*