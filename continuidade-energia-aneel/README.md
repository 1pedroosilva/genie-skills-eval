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
2. ⏳ Implementar extração de dados da ANEEL
3. ⏳ Processar e limpar dados de interrupções
4. ⏳ Calcular DEC observado por distribuidora
5. ⏳ Comparar com DEC oficial
6. ⏳ Gerar visualizações e relatórios

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

**Status**: 🟡 Projeto em estruturação - aguardando implementação dos notebooks