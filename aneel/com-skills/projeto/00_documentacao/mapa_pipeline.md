# Mapa do Pipeline: Continuidade ANEEL

## Linhagem

```
ANEEL interrupções → 101_interrupcoes_distribuicao → 201_interrupcoes_distribuicao → 301_interrupcoes_distribuicao → 303_comparativo_dec
ANEEL indicadores  → 102_indicadores_continuidade  → 202_indicadores_continuidade  → 302_indicadores_continuidade  → 303_comparativo_dec
```

## Etapas

| Notebook | Camada | Lê | Escreve | Status |
|----------|--------|----|---------|--------|
| 101_ingestao_aneel_interrupcoes | bronze | Arquivo anual ANEEL (interrupções) | main.continuidade_aneel_bronze.aneel_interrupcoes | construido |
| 102_indicadores_continuidade | bronze | Arquivo anual ANEEL (indicadores) | proj_aneel_cont_01_bronze.indicadores_continuidade | construido |
| 201_interrupcoes_distribuicao | silver | proj_aneel_cont_01_bronze.101_interrupcoes_distribuicao | proj_aneel_cont_02_silver.201_interrupcoes_distribuicao | planejado |
| 202_indicadores_continuidade | silver | proj_aneel_cont_01_bronze.102_indicadores_continuidade | proj_aneel_cont_02_silver.202_indicadores_continuidade | construido |
| 301_interrupcoes_distribuicao | gold | proj_aneel_cont_02_silver.201_interrupcoes_distribuicao | proj_aneel_cont_03_gold.301_interrupcoes_distribuicao | planejado |
| 302_indicadores_continuidade | gold | proj_aneel_cont_02_silver.202_indicadores_continuidade | proj_aneel_cont_03_gold.302_indicadores_continuidade | planejado |
| 303_comparativo_dec | gold | proj_aneel_cont_03_gold.301_interrupcoes_distribuicao + proj_aneel_cont_03_gold.302_indicadores_continuidade | proj_aneel_cont_03_gold.303_comparativo_dec | planejado |
