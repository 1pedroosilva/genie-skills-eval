# Projeto: Continuidade do Fornecimento de Energia Elétrica no Brasil

## Pergunta de Pesquisa

**O tempo de interrupção observado em cada distribuidora está compatível com o DEC apurado pelo regulador?**

## Contexto

Este projeto analisa a continuidade do fornecimento de energia elétrica no Brasil utilizando dados abertos da ANEEL (Agência Nacional de Energia Elétrica). O objetivo é comparar os indicadores de continuidade calculados a partir dos dados de interrupções na rede com os indicadores oficiais (DEC - Duração Equivalente de Interrupção por Unidade Consumidora) apurados e divulgados pelo regulador.

## Fontes de Dados

### 1. Interrupções na Rede de Distribuição
- **Descrição**: Registros detalhados de interrupções no fornecimento de energia
- **Formato**: Arquivo anual
- **Origem**: ANEEL - Dados Abertos
- **URL**: https://dadosabertos.aneel.gov.br/

### 2. Indicadores Coletivos de Continuidade
- **Descrição**: Indicadores DEC, FEC e outros apurados oficialmente
- **Formato**: Arquivo anual
- **Origem**: ANEEL - Dados Abertos
- **URL**: https://dadosabertos.aneel.gov.br/

## Escopo do Projeto

- **Período**: 2 últimos anos fechados (2024 e 2025)
- **Cobertura geográfica**: Brasil inteiro
- **Distribuidoras**: Todas as distribuidoras de energia elétrica reguladas pela ANEEL

## Estrutura do Projeto

```
continuidade-energia-aneel/
├── README.md                    # Este arquivo
├── notebooks/                   # Notebooks de análise
│   ├── 01_extracao_dados.py    # Extração e download dos dados
│   ├── 02_processamento.py     # Limpeza e transformação
│   └── 03_analise_dec.py       # Análise e comparação DEC
├── dados/                       # Diretório para dados brutos e processados
│   └── README.md               # Instruções sobre dados
└── docs/                        # Documentação adicional
    └── dicionario_dados.md     # Dicionário de dados
```

## Próximos Passos

1. ✅ Estrutura do projeto criada
2. ✅ Implementar extração de dados da ANEEL (interrupções e indicadores)
3. ✅ Análise de qualidade de dados de interrupções
4. ✅ Transformação silver de indicadores de continuidade
5. ⏳ Processar e limpar dados de interrupções (regras definidas)
6. ⏳ Calcular DEC observado por distribuidora a partir de interrupções
7. ⏳ Camada gold: comparar DEC observado vs. DEC oficial
8. ⏳ Gerar visualizações e relatórios analíticos



## Análise de Qualidade Concluída

**Data**: 12/09/2026 23:46 - 13/09/2026 00:01

- ✅ Análise detalhada de qualidade dos dados de interrupções bronze
- ✅ Identificação de 8.559 registros com problemas (1,71% do total)
- ✅ Definição e validação de regras de tratamento
- ✅ 98,36% dos dados são válidos e prontos para transformação silver
- 📊 Notebook: [Análise de Qualidade - Interrupções Bronze](#notebook-4356906633627129)
- 📄 Documentação completa: `docs/PROJETO-Continuidade-Fornecimento.md`

## Transformação Silver - Indicadores de Continuidade

**Data**: 13/09/2026 01:18 - 01:23

- ✅ Ingestão bronze dos Indicadores Coletivos de Continuidade ANEEL implementada
- ✅ Transformação silver completa criada e documentada
- ✅ Padronização de colunas (snake_case, nomes descritivos)
- ✅ Criação de data_apuracao (formato DATE padrão YYYY-MM-01)
- ✅ Limpeza de dados (remoção de espaços, validações)
- ✅ Metadados de processamento e rastreabilidade
- 📊 Notebook: [Silver - Indicadores de Continuidade](#notebook-4356906633627185)
- 🗄️ Tabela: `workspace.proj_aneel_cont_02_silver.indicadores_continuidade`
- 📈 Volume: ~5,1 milhões de registros (jan/2020 a ago/2026)
- 🎯 Indicadores: 23 tipos (DEC, FEC, DIC, FIC, DMIC, NumCon e variantes)

## Revisão de Code Review - Notebook de Ingestão

**Data**: 13/09/2026 15:03

- ✅ Revisão completa do notebook "Ingestão ANEEL - Interrupções de Energia"
- 🔍 Identificados **13 problemas** (5 críticos, 8 importantes) que bloqueiam produção
- 📊 Notebook revisado: [Ingestão ANEEL - Interrupções de Energia](#notebook-1581954047410310)
- ⚠️ **Status**: Bloqueado para produção até correções

### Problemas Críticos Identificados:
1. Location DBFS com mount point não configurado (falha garantida)
2. Escrita em `/dbfs` incompatível com serverless compute
3. Mode overwrite sem validações (risco de perda de dados)
4. Timezone não especificado em timestamps de auditoria
5. URL hardcoded frágil (sem validação HTTP)

### Problemas Importantes:
6. Falta de widgets para parametrização (ANO_REFERENCIA, CATALOG, SCHEMA)
7. Logging inadequado (print ao invés de logging estruturado)
8. Tratamento de erros genérico (sem distinguir tipos de exceção)
9. Validações insuficientes (sem checks de duplicatas, nulls, ranges)
10. Display() em código de produção (incompatível com jobs agendados)
11. Sem idempotência (re-execução sobrescreve sem controle)
12. Célula de exploração em notebook de produção
13. Sem estratégia de retry e notificação de falhas

### Ações Recomendadas:
- Corrigir itens 1-5 imediatamente (bloqueadores)
- Corrigir itens 6-13 antes do primeiro deploy
- Implementar validações com Great Expectations
- Adicionar notificações via job alerts
- Mover configurações para `config.yaml` externo
- Criar notebook de testes unitários separado

## Documentação Notebook Gold - Tempo de Interrupção

**Data**: 13/09/2026 15:50 - 15:54

- ✅ Documentação completa do notebook Gold criada
- 📊 Notebook documentado: [Gold - Tempo Interrupção por Distribuidora e Mês](#notebook-218687058620801)
- 📄 Arquivo de documentação: `docs/Gold - Tempo Interrupção - Documentação.md`
- 🎯 **Objetivo**: Agregar tempo de interrupção (DEC) e frequência (FEC) por distribuidora e mês
- 📊 **Tabela destino**: `workspace.proj_aneel_cont_03_gold.tempo_interrupcao_mensal`

### Conteúdo Documentado:
- Fluxo completo de processamento (Silver → Agregação → Enriquecimento → Gold)
- 17 indicadores calculados (DEC médio/mín/máx/total, FEC, consumidores, ranking)
- Métricas de evolução temporal (variação % mês a mês, ranking mensal)
- 5 casos de uso principais: distribuidoras críticas, tendências, anomalias, metas regulatórias, rankings
- Tecnologias: PySpark com Window Functions, Delta Lake, modo overwrite
- Validações SQL incluídas no notebook

### Observações Importantes:
- Notebook já está pronto e funcional (não foi criado nesta sessão, apenas documentado)
- Modo overwrite: sobrescreve dados a cada execução
- Frequência sugerida: mensal, após atualização da tabela Silver
- Ranking calculado dentro de cada mês

## Indicadores-Chave

- **DEC (Duração Equivalente de Interrupção por Unidade Consumidora)**: Tempo médio que cada unidade consumidora ficou sem energia
- **FEC (Frequência Equivalente de Interrupção por Unidade Consumidora)**: Número médio de interrupções por unidade consumidora

## Metodologia Prevista

1. Extrair dados de interrupções (eventos individuais)
2. Calcular DEC observado a partir dos eventos de interrupção
3. Extrair DEC oficial apurado pela ANEEL
4. Comparar DEC observado vs. DEC oficial por distribuidora
5. Identificar discrepâncias e investigar causas

---

**Status**: 🟢 Projeto em desenvolvimento - camada bronze implementada, transformação silver de indicadores concluída