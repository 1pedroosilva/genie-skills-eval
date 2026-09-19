# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Documentação
# MAGIC %md
# MAGIC # Ingestão de Dados - ANEEL Indicadores Coletivos de Continuidade
# MAGIC
# MAGIC ## Objetivo
# MAGIC Ingestão dos **Indicadores Coletivos de Continuidade** da ANEEL para a camada **bronze** do projeto de continuidade de fornecimento de energia elétrica.
# MAGIC
# MAGIC ## Fonte
# MAGIC - **Portal**: [Dados Abertos ANEEL](https://dadosabertos.aneel.gov.br/dataset/indicadores-coletivos-de-continuidade)
# MAGIC - **Dataset**: Indicadores Coletivos de Continuidade (DEC, FEC e variantes)
# MAGIC - **Formato**: Parquet (arquivo único, republicado a cada atualização)
# MAGIC - **Estratégia**: Carga completa (full load) a cada publicação
# MAGIC - **URL do arquivo**: `https://dadosabertos.aneel.gov.br/dataset/d5f0712e-62f6-4736-8dff-9991f10758a7/resource/d7f70fb1-725c-4748-afeb-65c6a78df550/download/indicadores-continuidade-coletivos-2020-2029.parquet`
# MAGIC
# MAGIC ## Destino
# MAGIC - **Catálogo**: `workspace`
# MAGIC - **Schema**: `proj_aneel_cont_01_bronze`
# MAGIC - **Tabela**: `indicadores_continuidade`
# MAGIC - **Formato**: Delta
# MAGIC
# MAGIC ## Indicadores Incluídos
# MAGIC | Sigla | Descrição |
# MAGIC |-------|-----------|
# MAGIC | DEC | Duração Equivalente de Interrupção por Unidade Consumidora |
# MAGIC | FEC | Frequência Equivalente de Interrupção por Unidade Consumidora |
# MAGIC | DECIP / FECIP | DEC/FEC Individual Programada |
# MAGIC | DECIPC / FECIPC | DEC/FEC Individual Programada Contínua |
# MAGIC | DECIND / FECIND | DEC/FEC Individual Não Programada |
# MAGIC | DECINC / FECINC | DEC/FEC Individual Contínua |
# MAGIC | DECINE / FECINE | DEC/FEC Individual Não Programada Especial |
# MAGIC | DECINO / FECINO | DEC/FEC Individual Não Programada Outros |
# MAGIC | DECXP / FECXP | DEC/FEC Extra Programada |
# MAGIC | DECXPC / FECXPC | DEC/FEC Extra Programada Contínua |
# MAGIC | DECXN / FECXN | DEC/FEC Extra Não Programada |
# MAGIC | DECXNC / FECXNC | DEC/FEC Extra Não Programada Contínua |
# MAGIC | NumCon | Número de Consumidores |
# MAGIC
# MAGIC ## Metadados de Ingestão
# MAGIC | Coluna | Tipo | Descrição |
# MAGIC |--------|------|-----------|
# MAGIC | `_fonte_url` | STRING | URL do arquivo fonte baixado |
# MAGIC | `_ingest_ts` | TIMESTAMP | Timestamp da ingestão |
# MAGIC | `_ingest_date` | DATE | Data da ingestão |
# MAGIC | `_run_id` | STRING | Identificador único da execução (YYYYMMDDHHMMSS) |

# COMMAND ----------

# DBTITLE 1,Configurações e Parâmetros
# ============================================================================
# CONFIGURAÇÕES E PARÂMETROS
# ============================================================================

from datetime import datetime, timezone

# --- Fonte de dados ---
# URL do arquivo Parquet no portal de dados abertos da ANEEL
# O arquivo é republicado a cada nova atualização (cobre o período 2020-2029)
FONTE_URL = (
    "https://dadosabertos.aneel.gov.br/dataset/"
    "d5f0712e-62f6-4736-8dff-9991f10758a7/resource/"
    "d7f70fb1-725c-4748-afeb-65c6a78df550/download/"
    "indicadores-continuidade-coletivos-2020-2029.parquet"
)

# --- Destino (camada Bronze) ---
CATALOG = "workspace"
SCHEMA_BRONZE = "proj_aneel_cont_01_bronze"
TABELA_BRONZE = "indicadores_continuidade"
TABELA_COMPLETA = f"{CATALOG}.{SCHEMA_BRONZE}.{TABELA_BRONZE}"

# --- Metadados de ingestão ---
# Timestamp UTC no formato ISO 8601 (compatível com o schema existente)
ingest_ts = datetime.now(timezone.utc).isoformat()
ingest_date = datetime.now(timezone.utc).date()
run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

print("=" * 80)
print("INGESTÃO - INDICADORES COLETIVOS DE CONTINUIDADE ANEEL")
print("=" * 80)
print(f"  Fonte       : {FONTE_URL}")
print(f"  Destino     : {TABELA_COMPLETA}")
print(f"  Ingest_ts   : {ingest_ts}")
print(f"  Ingest_date : {ingest_date}")
print(f"  Run ID      : {run_id}")
print(f"  Estratégia  : Full load (overwrite)")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Criar Schema Bronze (se não existir)
# MAGIC %sql
# MAGIC -- Garantir que o schema bronze existe
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.proj_aneel_cont_01_bronze
# MAGIC COMMENT 'Camada Bronze - Dados brutos do projeto de Continuidade de Fornecimento ANEEL'

# COMMAND ----------

# DBTITLE 1,Ingestão - Leitura e Escrita na Bronze
# ============================================================================
# INGESTÃO - LEITURA DO PARQUET E ESCRITA NA BRONZE
# ============================================================================

import os
import tempfile
import urllib.request
import pandas as pd
from pyspark.sql import functions as F

print(f"\n[1/5] Baixando arquivo Parquet da fonte...")
print(f"       URL: {FONTE_URL}")

# Serverless (Spark Connect) não suporta leitura direta de URLs HTTPS nem
# arquivos locais fora de /Workspace. Solução: baixar com Python, ler com
# pandas e converter para Spark DataFrame.
local_dir = tempfile.mkdtemp(prefix="aneel_ingest_")
local_path = os.path.join(local_dir, "indicadores-continuidade.parquet")

print(f"       Baixando para: {local_path}")
urllib.request.urlretrieve(FONTE_URL, local_path)
file_size_mb = os.path.getsize(local_path) / (1024 * 1024)
print(f"       ✓ Download concluído ({file_size_mb:.1f} MB)")

print(f"\n[2/5] Lendo arquivo Parquet com pandas...")
pdf = pd.read_parquet(local_path)
print(f"       ✓ Leitura concluída ({len(pdf):,} linhas, {len(pdf.columns)} colunas)")
print(f"       Colunas originais: {list(pdf.columns)}")

df_raw = spark.createDataFrame(pdf)
print(f"       ✓ Convertido para Spark DataFrame")

# ---------------------------------------------------------------------------
# Renomear colunas para lowercase (se necessário) e garantir tipos esperados
# ---------------------------------------------------------------------------
# O schema esperado na bronze (9 colunas de negócio + 4 de metadados):
#   datgeracaoconjuntodados  DATE
#   ideconjundconsumidoras   BIGINT
#   dscconjundconsumidoras   STRING
#   sigagente                STRING
#   numcnpj                  BIGINT
#   sigindicador             STRING
#   anoindice                BIGINT
#   numperiodoindice         BIGINT
#   vlrindiceenviado         DOUBLE
# ---------------------------------------------------------------------------

print(f"\n[3/5] Padronizando colunas e adicionando metadados...")

# Normalizar nomes de colunas para lowercase (caso venham em uppercase)
col_map = {c: c.lower() for c in df_raw.columns if c != c.lower()}
if col_map:
    for old_col, new_col in col_map.items():
        df_raw = df_raw.withColumnRenamed(old_col, new_col)
    print(f"       Colunas renomeadas: {col_map}")

# Adicionar colunas de metadados de ingestão
df_bronze = (
    df_raw
    .withColumn("_fonte_url", F.lit(FONTE_URL))
    .withColumn("_ingest_ts", F.lit(ingest_ts).cast("timestamp"))
    .withColumn("_ingest_date", F.lit(ingest_date).cast("date"))
    .withColumn("_run_id", F.lit(run_id))
)

print(f"       ✓ Metadados adicionados")
print(f"       Schema final ({len(df_bronze.columns)} colunas):")
df_bronze.printSchema()

# ---------------------------------------------------------------------------
# Contar registros antes da escrita
# ---------------------------------------------------------------------------
total_registros = df_bronze.count()
print(f"\n[4/5] Total de registros a serem escritos: {total_registros:,}")

# ---------------------------------------------------------------------------
# Escrita na camada Bronze (full load - overwrite)
# ---------------------------------------------------------------------------
print(f"\n[5/5] Escrevendo na tabela bronze: {TABELA_COMPLETA}")
print(f"       Modo: OVERWRITE (carga completa)")

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .option("mergeSchema", "true")
    .saveAsTable(TABELA_COMPLETA)
)

print(f"       ✓ Ingestão concluída com sucesso!")
print(f"       {total_registros:,} registros escritos em {TABELA_COMPLETA}")

# Limpar arquivo temporário
import shutil
shutil.rmtree(local_dir, ignore_errors=True)
print(f"       ✓ Arquivo temporário removido")

# COMMAND ----------

# DBTITLE 1,Validação e Qualidade dos Dados
# ============================================================================
# VALIDAÇÃO PÓS-INGESTÃO
# ============================================================================

print("=" * 80)
print("VALIDAÇÃO DOS DADOS INGERIDOS")
print("=" * 80)

# 1. Contagem total
total = spark.table(TABELA_COMPLETA).count()
print(f"\n1. Total de registros: {total:,}")

# 2. Cobertura temporal
print(f"\n2. Cobertura temporal:")
spark.sql(f"""
    SELECT 
        anoindice as ano,
        numperiodoindice as periodo,
        COUNT(*) as registros
    FROM {TABELA_COMPLETA}
    GROUP BY anoindice, numperiodoindice
    ORDER BY anoindice, numperiodoindice
""").display()

# 3. Distribuição por indicador
print(f"\n3. Distribuição por indicador:")
spark.sql(f"""
    SELECT 
        sigindicador,
        COUNT(*) as total_registros,
        COUNT(DISTINCT sigagente) as total_agentes
    FROM {TABELA_COMPLETA}
    GROUP BY sigindicador
    ORDER BY sigindicador
""").display()

# 4. Distribuidoras (agentes)
total_agentes = spark.sql(f"SELECT COUNT(DISTINCT sigagente) FROM {TABELA_COMPLETA}").collect()[0][0]
print(f"\n4. Total de agentes (distribuidoras): {total_agentes}")

# 5. Metadados de ingestão
print(f"\n5. Metadados de ingestão:")
spark.sql(f"""
    SELECT 
        _run_id,
        _ingest_ts,
        _ingest_date,
        _fonte_url,
        COUNT(*) as total_registros
    FROM {TABELA_COMPLETA}
    GROUP BY _run_id, _ingest_ts, _ingest_date, _fonte_url
""").display()

# 6. Verificação de nulos em colunas-chave
print(f"\n6. Verificação de nulos em colunas-chave:")
spark.sql(f"""
    SELECT 
        SUM(CASE WHEN sigagente IS NULL THEN 1 ELSE 0 END) as null_sigagente,
        SUM(CASE WHEN sigindicador IS NULL THEN 1 ELSE 0 END) as null_sigindicador,
        SUM(CASE WHEN anoindice IS NULL THEN 1 ELSE 0 END) as null_anoindice,
        SUM(CASE WHEN numperiodoindice IS NULL THEN 1 ELSE 0 END) as null_numperiodoindice,
        SUM(CASE WHEN vlrindiceenviado IS NULL THEN 1 ELSE 0 END) as null_vlrindiceenviado
    FROM {TABELA_COMPLETA}
""").display()

print(f"\n✓ Validação concluída")

# COMMAND ----------

# DBTITLE 1,Consulta de Exemplo - Exploração
# MAGIC %sql
# MAGIC -- Consulta de exemplo: indicadores DEC e FEC por agente e período
# MAGIC SELECT 
# MAGIC     sigagente,
# MAGIC     sigindicador,
# MAGIC     anoindice,
# MAGIC     numperiodoindice,
# MAGIC     vlrindiceenviado
# MAGIC FROM workspace.proj_aneel_cont_01_bronze.indicadores_continuidade
# MAGIC WHERE sigindicador IN ('DEC', 'FEC')
# MAGIC   AND anoindice = 2025
# MAGIC ORDER BY sigagente, sigindicador, numperiodoindice
# MAGIC LIMIT 20

# COMMAND ----------

# DBTITLE 1,Notas e Próximos Passos
# MAGIC %md
# MAGIC ## Notas Importantes
# MAGIC
# MAGIC ### Sobre a Fonte
# MAGIC - O arquivo Parquet é republicado pela ANEEL a cada nova atualização de dados
# MAGIC - O arquivo atual cobre o período de **2020 a 2029** (anos com dados disponíveis variam conforme publicação)
# MAGIC - A URL é estável e identifica o recurso no portal CKAN da ANEEL
# MAGIC - Caso a ANEEL publique um novo recurso (novo ID), a variável `FONTE_URL` deve ser atualizada
# MAGIC
# MAGIC ### Estratégia de Carga
# MAGIC - **Full load (overwrite)**: a cada execução, os dados completos substituem o conteúdo anterior
# MAGIC - Adequado para arquivos anuais republicados (não há acúmulo incremental)
# MAGIC - `overwriteSchema = true` permite que mudanças no schema da fonte sejam absorvidas
# MAGIC
# MAGIC ### Metadados de Rastreabilidade
# MAGIC - `_fonte_url`: URL exata do arquivo ingerido
# MAGIC - `_ingest_ts`: timestamp UTC da execução da ingestão
# MAGIC - `_ingest_date`: data da ingestão (para partições lógicas)
# MAGIC - `_run_id`: identificador único da execução (formato YYYYMMDDHHMMSS)
# MAGIC
# MAGIC ### Próximos Passos
# MAGIC 1. **Camada Silver**: Criar transformações para limpar, padronizar e enriquecer os dados
# MAGIC 2. **Camada Gold**: Criar agregações por distribuidora, região e período
# MAGIC 3. **Automação**: Agendar execução periódica para manter dados atualizados
# MAGIC 4. **Monitoramento**: Configurar alertas para falhas de ingestão
# MAGIC 5. **Qualidade**: Implementar checks de qualidade de dados na silver
# MAGIC 6. **Documentação**: Atualizar catálogo Unity Catalog com descrições de colunas

# COMMAND ----------

