# Databricks notebook source
# DBTITLE 1,DOCUMENTAÇÃO
# MAGIC %md
# MAGIC # EDA: Qualidade de Dados - Interrupções Bronze
# MAGIC
# MAGIC **Objetivo:** Investigar a qualidade dos dados de interrupções na camada bronze antes de implementar a transformação silver.
# MAGIC
# MAGIC **Tabela analisada:** `workspace.proj_aneel_cont_01_bronze.101_interrupcoes`
# MAGIC
# MAGIC **Data da investigação:** 06/09/2026
# MAGIC
# MAGIC **Escopo da análise:**
# MAGIC * Completude (nulos, vazios)
# MAGIC * Consistência temporal (timestamps)
# MAGIC * Duplicatas
# MAGIC * Distribuições e valores atípicos
# MAGIC * Qualidade de campos-chave

# COMMAND ----------

# DBTITLE 1,IMPORTS
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,CONFIGURAR PARÂMETROS
# Tabela a ser analisada
CATALOG = "workspace"
SCHEMA = "proj_aneel_cont_01_bronze"
TABELA = "101_interrupcoes"
TABLE_FQN = f"{CATALOG}.{SCHEMA}.{TABELA}"

# COMMAND ----------

# DBTITLE 1,CARREGAR DADOS
# Carregar tabela bronze
df_bronze = spark.table(TABLE_FQN)

# Cache para análises subsequentes
df_bronze.cache()

print(f"📊 Total de registros: {df_bronze.count():,}")
print(f"📊 Total de colunas: {len(df_bronze.columns)}")
print(f"\n🔍 Schema:")
df_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. COMPLETUDE: Análise de Nulos e Vazios

# COMMAND ----------

# DBTITLE 1,Análise de nulos por coluna
# Calcular percentual de nulos por coluna
nulls_analysis = []
for col_name in df_bronze.columns:
    null_count = df_bronze.filter(F.col(col_name).isNull()).count()
    total_count = df_bronze.count()
    null_pct = (null_count / total_count * 100) if total_count > 0 else 0
    nulls_analysis.append((col_name, null_count, null_pct))

# Criar DataFrame de análise
df_nulls = spark.createDataFrame(
    nulls_analysis, 
    ["coluna", "qtd_nulos", "pct_nulos"]
)

# Ordenar por percentual decrescente
df_nulls_sorted = df_nulls.orderBy(F.desc("pct_nulos"))

print("\n📋 ANÁLISE DE NULOS POR COLUNA:")
df_nulls_sorted.show(100, truncate=False)

# COMMAND ----------

# DBTITLE 1,Análise de strings vazias
# Verificar strings vazias em colunas string
string_cols = [f.name for f in df_bronze.schema.fields if isinstance(f.dataType, StringType)]

empty_strings_analysis = []
for col_name in string_cols:
    empty_count = df_bronze.filter(
        (F.col(col_name).isNotNull()) & 
        (F.trim(F.col(col_name)) == "")
    ).count()
    total_count = df_bronze.count()
    empty_pct = (empty_count / total_count * 100) if total_count > 0 else 0
    empty_strings_analysis.append((col_name, empty_count, empty_pct))

df_empty_strings = spark.createDataFrame(
    empty_strings_analysis,
    ["coluna", "qtd_vazias", "pct_vazias"]
).orderBy(F.desc("pct_vazias"))

print("\n📋 ANÁLISE DE STRINGS VAZIAS:")
df_empty_strings.filter(F.col("qtd_vazias") > 0).show(100, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. CONSISTÊNCIA TEMPORAL

# COMMAND ----------

# DBTITLE 1,Validar ordem temporal de interrupções
# Verificar registros onde DatFimInterrupcao <= DatInicioInterrupcao
df_temporal_inconsistent = df_bronze.filter(
    (F.col("DatFimInterrupcao").isNotNull()) &
    (F.col("DatInicioInterrupcao").isNotNull()) &
    (F.col("DatFimInterrupcao") <= F.col("DatInicioInterrupcao"))
)

qtd_inconsistent = df_temporal_inconsistent.count()
total = df_bronze.count()
pct_inconsistent = (qtd_inconsistent / total * 100) if total > 0 else 0

print(f"\n⏰ INCONSISTÊNCIAS TEMPORAIS:")
print(f"   Registros com DatFimInterrupcao <= DatInicioInterrupcao: {qtd_inconsistent:,} ({pct_inconsistent:.2f}%)")

if qtd_inconsistent > 0:
    print("\n🔍 Amostra de registros inconsistentes:")
    df_temporal_inconsistent.select(
        "NumOrdemInterrupcao",
        "DatInicioInterrupcao",
        "DatFimInterrupcao",
        "DscTipoInterrupcao"
    ).show(10, truncate=False)

# COMMAND ----------

# DBTITLE 1,Análise de durações
# Calcular duração das interrupções em minutos
df_with_duration = df_bronze.filter(
    (F.col("DatFimInterrupcao").isNotNull()) &
    (F.col("DatInicioInterrupcao").isNotNull())
).withColumn(
    "duracao_minutos",
    (F.unix_timestamp("DatFimInterrupcao") - F.unix_timestamp("DatInicioInterrupcao")) / 60
)

# Estatísticas descritivas
df_duration_stats = df_with_duration.select(
    F.min("duracao_minutos").alias("min_minutos"),
    F.max("duracao_minutos").alias("max_minutos"),
    F.avg("duracao_minutos").alias("media_minutos"),
    F.expr("percentile_approx(duracao_minutos, 0.50)").alias("mediana_minutos"),
    F.expr("percentile_approx(duracao_minutos, 0.95)").alias("p95_minutos"),
    F.expr("percentile_approx(duracao_minutos, 0.99)").alias("p99_minutos")
)

print("\n⏱️  ESTATÍSTICAS DE DURAÇÃO DAS INTERRUPÇÕES:")
df_duration_stats.show(truncate=False)

# Identificar durações atípicas (> 24h = 1440 min)
df_long_outages = df_with_duration.filter(F.col("duracao_minutos") > 1440)
qtd_long = df_long_outages.count()
pct_long = (qtd_long / df_with_duration.count() * 100) if df_with_duration.count() > 0 else 0

print(f"\n⚠️  Interrupções com duração > 24h: {qtd_long:,} ({pct_long:.2f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. DUPLICATAS

# COMMAND ----------

# DBTITLE 1,Identificar duplicatas por chave de negócio
# Chave de negócio: NumOrdemInterrupcao (número da ordem de interrupção)
df_duplicates = df_bronze.groupBy("NumOrdemInterrupcao").count().filter("count > 1")

qtd_duplicates = df_duplicates.count()
total_registros_duplicados = df_duplicates.agg(F.sum("count")).collect()[0][0] or 0

print(f"\n🔄 ANÁLISE DE DUPLICATAS:")
print(f"   Ordens de interrupção duplicadas: {qtd_duplicates:,}")
print(f"   Total de registros envolvidos em duplicação: {total_registros_duplicados:,}")

if qtd_duplicates > 0:
    print("\n🔍 Amostra de ordens duplicadas:")
    df_duplicates.orderBy(F.desc("count")).show(10, truncate=False)
    
    # Mostrar exemplo de duplicatas
    exemplo_ordem = df_duplicates.orderBy(F.desc("count")).first()["NumOrdemInterrupcao"]
    print(f"\n📄 Exemplo: Ordem {exemplo_ordem} (todos os registros):")
    df_bronze.filter(F.col("NumOrdemInterrupcao") == exemplo_ordem).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. DISTRIBUIÇÕES E CARDINALIDADE

# COMMAND ----------

# DBTITLE 1,Distribuição por tipo de interrupção
df_tipo_dist = df_bronze.groupBy("DscTipoInterrupcao").agg(
    F.count("*").alias("qtd_registros"),
    F.countDistinct("NumOrdemInterrupcao").alias("qtd_ordens_unicas")
).orderBy(F.desc("qtd_registros"))

print("\n📊 DISTRIBUIÇÃO POR TIPO DE INTERRUPÇÃO:")
df_tipo_dist.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Distribuição por distribuidora
df_agente_dist = df_bronze.groupBy("SigAgente", "NomAgenteRegulado").agg(
    F.count("*").alias("qtd_registros")
).orderBy(F.desc("qtd_registros"))

print("\n📊 DISTRIBUIÇÃO POR DISTRIBUIDORA:")
df_agente_dist.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Distribuição por nível de tensão
df_tensao_dist = df_bronze.groupBy("NumNivelTensao").agg(
    F.count("*").alias("qtd_registros")
).orderBy(F.desc("qtd_registros"))

print("\n📊 DISTRIBUIÇÃO POR NÍVEL DE TENSÃO:")
df_tensao_dist.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. SÍNTESE DE ACHADOS
# MAGIC
# MAGIC ### 🔍 Principais Descobertas:
# MAGIC
# MAGIC *(Esta seção deve ser preenchida após execução do notebook)*
# MAGIC
# MAGIC **COMPLETUDE:**
# MAGIC - [registrar colunas com nulos/vazios significativos]
# MAGIC
# MAGIC **CONSISTÊNCIA TEMPORAL:**
# MAGIC - [registrar inconsistências encontradas]
# MAGIC
# MAGIC **DUPLICATAS:**
# MAGIC - [registrar se há duplicatas e padrão]
# MAGIC
# MAGIC **VALORES ATÍPICOS:**
# MAGIC - [registrar outliers e padrões incomuns]
# MAGIC
# MAGIC ### ✅ Decisões Técnicas para Silver:
# MAGIC
# MAGIC 1. **Tratamento de nulos:** [definir estratégia]
# MAGIC 2. **Deduplicação:** [definir critério se aplicável]
# MAGIC 3. **Validação temporal:** [definir regra de negócio]
# MAGIC 4. **Filtragens:** [definir exclusões se aplicável]
# MAGIC
# MAGIC ### 📝 Próximos Passos:
# MAGIC
# MAGIC - [ ] Criar notebook de validação (VAL_001) para provar regras escolhidas
# MAGIC - [ ] Implementar transformação silver com regras validadas
# MAGIC - [ ] Documentar decisões em evolucao_projeto.md

# COMMAND ----------

