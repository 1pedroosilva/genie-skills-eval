# Continuidade ANEEL

Projeto de análise de continuidade do fornecimento de energia elétrica no Brasil utilizando bases abertas da ANEEL.

Arquitetura medalhão com camadas bronze, silver e gold. Compara o tempo de interrupção observado nas distribuidoras com o DEC (Duração Equivalente de Continuidade) apurado pelo regulador.

Escopo atual: ano de 2025, Brasil inteiro.

## Estado do pipeline

Pipeline bronze-silver-gold operacional via job `567314997539912` com parâmetro `anos=2025`.

| Tabela | Registros | Colunas |
| --- | --- | --- |
| `workspace.continuidade_aneel_bronze.aneel_interrupcoes` | 9.715.372 | 22 |
| `workspace.proj_aneel_cont_01_bronze.indicadores_continuidade` | 741.933 | 13 |
| `workspace.continuidade_aneel_silver.interrupcoes_distribuicao` | 8.669.725 | 12 |
| `workspace.proj_aneel_cont_02_silver.indicadores_continuidade` | 37.795 | 34 |
| `workspace.continuidade_aneel_gold.tempo_interrupcao_distribuidora_mes` | 620 | 10 |

## Fontes de dados

* **Interrupções:** CKAN ANEEL, dataset `interrupcoes-de-energia-eletrica-nas-redes-de-distribuicao`, um arquivo Parquet por ano.
* **Indicadores:** CKAN ANEEL, dataset `indicadores-coletivos-de-continuidade-dec-e-fec`, arquivo Parquet único 2020-2029 (formato longo: `SigIndicador` + `VlrIndiceEnviado`).

## Arquitetura técnica

* **Compute:** Serverless (Spark Connect). Restrições: sem acesso a `/tmp/`, sem `spark.sparkContext`, sem `spark.read.parquet(HTTP)`.
* **Carga de Parquet:** Download via `requests` para UC Volume (`/Volumes/.../raw_data/`) + `spark.read.parquet(volume_path)`.
* **Idempotência:** Bronze via tabela de controle com `Last-Modified`. Silver/Gold via `DROP TABLE IF EXISTS` + APPEND.
* **Parametrização:** Widget `anos` em todos os notebooks, com default `"2025"`.

## Estrutura de pastas

```
continuidade_aneel/
  00_documentacao/    -- evolucao_projeto.md, decisoes arquiteturais
  01_bronze/          -- 101_ingestao_aneel_interrupcoes.py, 102_indicadores_continuidade.py
  02_silver/          -- 201_interrupcoes_distribuicao.py, 202_indicadores_continuidade.py
  03_gold/            -- 301_tempo_interrupcao_distribuidora_mes.py
  99_chats-html/      -- transcrições de sessões de desenvolvimento
```
