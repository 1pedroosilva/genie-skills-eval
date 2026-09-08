# Diretório de Dados

## Estrutura

```
dados/
├── raw/                    # Dados brutos baixados da ANEEL
│   ├── interrupcoes/      # Interrupções na rede de distribuição
│   │   ├── 2024.csv
│   │   └── 2025.csv
│   └── indicadores/       # Indicadores coletivos de continuidade
│       ├── 2024.csv
│       └── 2025.csv
└── processed/             # Dados processados e limpos
    ├── interrupcoes_processado.parquet
    ├── indicadores_processado.parquet
    └── analise_dec.parquet
```

## Fonte dos Dados

### Portal de Dados Abertos da ANEEL

**URL**: https://dadosabertos.aneel.gov.br/

### Datasets Necessários

1. **Interrupções na Rede de Distribuição**
   - Buscar por: "interrupções" ou "DEC" ou "continuidade"
   - Formato esperado: CSV ou Excel
   - Período: Anos 2024 e 2025 completos
   - Conteúdo: Eventos individuais de interrupção com:
     * Data/hora de início
     * Duração
     * Número de consumidores afetados
     * Código da distribuidora
     * Tipo de interrupção

2. **Indicadores Coletivos de Continuidade**
   - Buscar por: "DEC", "FEC", "indicadores de continuidade"
   - Formato esperado: CSV ou Excel
   - Período: Anos 2024 e 2025
   - Conteúdo: Indicadores consolidados:
     * DEC (Duração Equivalente de Interrupção)
     * FEC (Frequência Equivalente de Interrupção)
     * Código da distribuidora
     * Período de apuração

## Como Baixar os Dados

1. Acesse o portal: https://dadosabertos.aneel.gov.br/
2. Use a busca para encontrar os datasets relevantes
3. Baixe os arquivos para os respectivos diretórios em `raw/`
4. Documente as URLs exatas no dicionário de dados

## Notas Importantes

* **Não versionar dados brutos**: Arquivos grandes devem ser ignorados pelo Git
* **Documentação**: Sempre atualizar o dicionário de dados após baixar novos arquivos
* **Validação**: Verificar integridade dos arquivos após download
* **Licença**: Os dados da ANEEL são de domínio público

## Status

- [ ] Dados brutos de interrupções 2024
- [ ] Dados brutos de interrupções 2025
- [ ] Indicadores oficiais 2024
- [ ] Indicadores oficiais 2025
- [ ] Dados processados gerados