# Databricks notebook source
# DBTITLE 1,DOCUMENTAÇÃO
# MAGIC %md
# MAGIC # VAL: Validação de Regras de Transformação Silver - Interrupções
# MAGIC
# MAGIC **Objetivo:** Validar e provar que as regras de tratamento escolhidas para a transformação silver funcionam corretamente.
# MAGIC
# MAGIC **Fonte:** `workspace.proj_aneel_cont_01_bronze.101_interrupcoes`
# MAGIC
# MAGIC **Data da validação:** 06/09/2026
# MAGIC
# MAGIC **Achados do EDA (EDA_001):**
# MAGIC * [Serão preenchidos após execução do EDA]
# MAGIC
# MAGIC **Regras a validar:**
# MAGIC 1. Deduplicação por NumOrdemInterrupcao (manter registro mais recente)
# MAGIC 2. Filtro de consistência temporal (DatFimInterrupcao > DatInicioInterrupcao)
# MAGIC 3. Tratamento de nulos críticos
# MAGIC 4. Cálculo de duração em minutos
# MAGIC 5. Classificação de duração (curta/média/longa)

# COMMAND ----------

# DBTITLE 1,IMPORTS
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,CONFIGURAR PARÂMETROS
# Tabela bronze
CATALOG = "workspace"
SCHEMA_BRONZE = "proj_aneel_cont_01_bronze"
TABELA_BRONZE = "101_interrupcoes"
TABLE_BRONZE_FQN = f"{CATALOG}.{SCHEMA_BRONZE}.{TABELA_BRONZE}"

# Thresholds para classificação
DURACAO_CURTA_MAX_MIN = 60  # até 1h
DURACAO_LONGA_MIN_MIN = 1440  # acima de 24h

# COMMAND ----------

# DBTITLE 1,CARREGAR DADOS BRONZE
# Carregar dados bronze
df_bronze = spark.table(TABLE_BRONZE_FQN)

print(f"📊 Registros bronze: {df_bronze.count():,}")
print(f"\n📋 Amostra bronze:")
df_bronze.select(
    "NumOrdemInterrupcao",
    "DatInicioInterrupcao",
    "DatFimInterrupcao",
    "DscTipoInterrupcao",
    "SigAgente"
).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. VALIDAR REGRA: Deduplicação

# COMMAND ----------

# DBTITLE 1,Implementar deduplicação
# Verificar duplicatas ANTES
df_dup_antes = df_bronze.groupBy("NumOrdemInterrupcao").count().filter("count > 1")
qtd_dup_antes = df_dup_antes.count()
registros_dup_antes = df_dup_antes.agg(F.sum("count")).collect()[0][0] or 0

print(f"\n🔍 ANTES DA DEDUPLICAÇÃO:")
print(f"   Ordens duplicadas: {qtd_dup_antes:,}")
print(f"   Registros duplicados: {registros_dup_antes:,}")

# REGRA: Manter registro mais recente por NumOrdemInterrupcao
# Critério de desempate: DatGeracaoConjuntoDados (mais recente)
window_spec = Window.partitionBy("NumOrdemInterrupcao").orderBy(F.desc("DatGeracaoConjuntoDados"))

df_deduplicated = df_bronze.withColumn(
    "_row_num", 
    F.row_number().over(window_spec)
).filter(
    F.col("_row_num") == 1
).drop("_row_num")

print(f"\n✅ DEPOIS DA DEDUPLICAÇÃO:")
print(f"   Registros totais: {df_deduplicated.count():,}")
print(f"   Redução: {df_bronze.count() - df_deduplicated.count():,} registros removidos")

# Validar: não deve haver mais duplicatas
df_dup_depois = df_deduplicated.groupBy("NumOrdemInterrupcao").count().filter("count > 1")
qtd_dup_depois = df_dup_depois.count()

assert qtd_dup_depois == 0, f"❌ FALHA: Ainda há {qtd_dup_depois} duplicatas!"
print(f"\n✅ VALIDAÇÃO PASSOU: Nenhuma duplicata remanescente")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. VALIDAR REGRA: Consistência Temporal

# COMMAND ----------

# DBTITLE 1,Filtrar inconsistências temporais
# Contar inconsistências ANTES
df_temporal_invalid_antes = df_deduplicated.filter(
    (F.col("DatFimInterrupcao").isNotNull()) &
    (F.col("DatInicioInterrupcao").isNotNull()) &
    (F.col("DatFimInterrupcao") <= F.col("DatInicioInterrupcao"))
)
qtd_invalid_antes = df_temporal_invalid_antes.count()

print(f"\n🔍 ANTES DO FILTRO TEMPORAL:")
print(f"   Registros com inconsistência: {qtd_invalid_antes:,}")

# REGRA: Remover registros onde DatFimInterrupcao <= DatInicioInterrupcao
df_temporal_valid = df_deduplicated.filter(
    (F.col("DatFimInterrupcao").isNull()) |
    (F.col("DatInicioInterrupcao").isNull()) |
    (F.col("DatFimInterrupcao") > F.col("DatInicioInterrupcao"))
)

print(f"\n✅ DEPOIS DO FILTRO TEMPORAL:")
print(f"   Registros válidos: {df_temporal_valid.count():,}")
print(f"   Registros removidos: {qtd_invalid_antes:,}")

# Validar: não deve haver mais inconsistências
df_temporal_invalid_depois = df_temporal_valid.filter(
    (F.col("DatFimInterrupcao").isNotNull()) &
    (F.col("DatInicioInterrupcao").isNotNull()) &
    (F.col("DatFimInterrupcao") <= F.col("DatInicioInterrupcao"))
)
qtd_invalid_depois = df_temporal_invalid_depois.count()

assert qtd_invalid_depois == 0, f"❌ FALHA: Ainda há {qtd_invalid_depois} inconsistências temporais!"
print(f"\n✅ VALIDAÇÃO PASSOU: Todas as interrupções têm datas válidas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. VALIDAR REGRA: Cálculo de Duração

# COMMAND ----------

# DBTITLE 1,Calcular e validar duração
# REGRA: Calcular duração em minutos
df_with_duration = df_temporal_valid.withColumn(
    "duracao_minutos",
    F.when(
        (F.col("DatFimInterrupcao").isNotNull()) & 
        (F.col("DatInicioInterrupcao").isNotNull()),
        (F.unix_timestamp("DatFimInterrupcao") - F.unix_timestamp("DatInicioInterrupcao")) / 60
    ).otherwise(None)
)

print(f"\n📊 ESTATÍSTICAS DE DURAÇÃO CALCULADA:")
df_with_duration.select(
    F.min("duracao_minutos").alias("min"),
    F.max("duracao_minutos").alias("max"),
    F.avg("duracao_minutos").alias("media"),
    F.expr("percentile_approx(duracao_minutos, 0.50)").alias("mediana")
).show(truncate=False)

# Validar: duração deve ser sempre >= 0 (já garantido pelo filtro temporal)
df_duracao_negativa = df_with_duration.filter(
    (F.col("duracao_minutos").isNotNull()) &
    (F.col("duracao_minutos") < 0)
)
qtd_duracao_negativa = df_duracao_negativa.count()

assert qtd_duracao_negativa == 0, f"❌ FALHA: {qtd_duracao_negativa} registros com duração negativa!"
print(f"\n✅ VALIDAÇÃO PASSOU: Todas as durações são não-negativas")

# Validar: duração não deve ser nula quando ambas as datas estão presentes
df_duracao_nula_invalida = df_with_duration.filter(
    (F.col("DatFimInterrupcao").isNotNull()) &
    (F.col("DatInicioInterrupcao").isNotNull()) &
    (F.col("duracao_minutos").isNull())
)
qtd_duracao_nula_invalida = df_duracao_nula_invalida.count()

assert qtd_duracao_nula_invalida == 0, f"❌ FALHA: {qtd_duracao_nula_invalida} registros com duração nula inválida!"
print(f"✅ VALIDAÇÃO PASSOU: Duração calculada corretamente quando datas presentes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. VALIDAR REGRA: Classificação de Duração

# COMMAND ----------

# DBTITLE 1,Classificar e validar duração
# REGRA: Classificar duração
df_with_class = df_with_duration.withColumn(
    "classe_duracao",
    F.when(F.col("duracao_minutos").isNull(), "NAO_CALCULADA")
    .when(F.col("duracao_minutos") <= DURACAO_CURTA_MAX_MIN, "CURTA")
    .when(F.col("duracao_minutos") >= DURACAO_LONGA_MIN_MIN, "LONGA")
    .otherwise("MEDIA")
)

print(f"\n📊 DISTRIBUIÇÃO POR CLASSE DE DURAÇÃO:")
df_class_dist = df_with_class.groupBy("classe_duracao").agg(
    F.count("*").alias("qtd_registros"),
    F.round(F.count("*") * 100.0 / df_with_class.count(), 2).alias("pct")
).orderBy(F.desc("qtd_registros"))

df_class_dist.show(truncate=False)

# Validar: classificação deve cobrir 100% dos registros
total_classificado = df_class_dist.agg(F.sum("qtd_registros")).collect()[0][0]
total_esperado = df_with_class.count()

assert total_classificado == total_esperado, f"❌ FALHA: Classificação incompleta! {total_classificado} != {total_esperado}"
print(f"\n✅ VALIDAÇÃO PASSOU: Todos os registros foram classificados")

# Validar: não deve haver valores fora das classes esperadas
classes_validas = ["NAO_CALCULADA", "CURTA", "MEDIA", "LONGA"]
df_classe_invalida = df_with_class.filter(~F.col("classe_duracao").isin(classes_validas))
qtd_classe_invalida = df_classe_invalida.count()

assert qtd_classe_invalida == 0, f"❌ FALHA: {qtd_classe_invalida} registros com classe inválida!"
print(f"✅ VALIDAÇÃO PASSOU: Apenas classes válidas presentes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. VALIDAR REGRA: Tratamento de Nulos Críticos

# COMMAND ----------

# DBTITLE 1,Validar campos obrigatórios
# REGRA: Campos críticos não podem ser nulos
campos_criticos = [
    "NumOrdemInterrupcao",
    "NumAno",
    "SigAgente",
    "DscTipoInterrupcao"
]

print(f"\n🔍 VALIDAÇÃO DE CAMPOS CRÍTICOS:")
falhas_validacao = []

for campo in campos_criticos:
    qtd_nulos = df_with_class.filter(F.col(campo).isNull()).count()
    if qtd_nulos > 0:
        falhas_validacao.append(f"{campo}: {qtd_nulos} nulos")
        print(f"   ❌ {campo}: {qtd_nulos:,} nulos encontrados")
    else:
        print(f"   ✅ {campo}: sem nulos")

if falhas_validacao:
    print(f"\n⚠️  ATENÇÃO: Campos críticos com nulos detectados!")
    print("   Ação recomendada: Filtrar ou tratar esses registros antes de gravar silver")
else:
    print(f"\n✅ VALIDAÇÃO PASSOU: Todos os campos críticos estão preenchidos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. RECONCILIAÇÃO: Antes vs Depois

# COMMAND ----------

# DBTITLE 1,Resumo comparativo
# Criar DataFrame de reconciliação
resumo_reconciliacao = [
    ("Registros Bronze (original)", df_bronze.count()),
    ("Após deduplicação", df_deduplicated.count()),
    ("Após filtro temporal", df_temporal_valid.count()),
    ("Com duração calculada (não-nulos)", df_with_duration.filter(F.col("duracao_minutos").isNotNull()).count()),
    ("Com classificação (final)", df_with_class.count())
]

df_reconciliacao = spark.createDataFrame(
    resumo_reconciliacao,
    ["etapa", "qtd_registros"]
).withColumn(
    "reducao",
    F.when(
        F.col("qtd_registros") < F.first("qtd_registros").over(Window.orderBy(F.lit(1))),
        F.first("qtd_registros").over(Window.orderBy(F.lit(1))) - F.col("qtd_registros")
    ).otherwise(0)
).withColumn(
    "pct_retencao",
    F.round(
        F.col("qtd_registros") * 100.0 / F.first("qtd_registros").over(Window.orderBy(F.lit(1))),
        2
    )
)

print(f"\n📊 RECONCILIAÇÃO DE REGISTROS:")
df_reconciliacao.show(truncate=False)

# Validar taxa de retenção mínima (exemplo: pelo menos 95%)
taxa_retencao_final = df_reconciliacao.filter(F.col("etapa").contains("final")).select("pct_retencao").collect()[0][0]

if taxa_retencao_final >= 95.0:
    print(f"\n✅ Taxa de retenção aceitável: {taxa_retencao_final}%")
else:
    print(f"\n⚠️  Taxa de retenção baixa: {taxa_retencao_final}% (< 95%)")
    print("   Revisar se as regras de filtro não estão muito restritivas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. AMOSTRA DO RESULTADO FINAL

# COMMAND ----------

# DBTITLE 1,Exibir amostra transformada
print(f"\n📋 AMOSTRA DE DADOS TRANSFORMADOS (SILVER):")

df_silver_sample = df_with_class.select(
    "NumOrdemInterrupcao",
    "NumAno",
    "SigAgente",
    "DscTipoInterrupcao",
    "DatInicioInterrupcao",
    "DatFimInterrupcao",
    "duracao_minutos",
    "classe_duracao",
    "NumUnidadeConsumidora",
    "NumConsumidorConjunto"
).orderBy(F.desc("duracao_minutos"))

df_silver_sample.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. CONCLUSÃO DA VALIDAÇÃO
# MAGIC
# MAGIC ### ✅ Regras Validadas:
# MAGIC
# MAGIC 1. **Deduplicação:** ✅ Funciona corretamente - mantém registro mais recente por NumOrdemInterrupcao
# MAGIC 2. **Consistência Temporal:** ✅ Filtra corretamente registros com datas inválidas
# MAGIC 3. **Cálculo de Duração:** ✅ Duração calculada corretamente (sempre >= 0)
# MAGIC 4. **Classificação:** ✅ Todas as interrupções classificadas em CURTA/MEDIA/LONGA/NAO_CALCULADA
# MAGIC 5. **Campos Críticos:** [verificar resultado acima]
# MAGIC
# MAGIC ### 📊 Métricas de Qualidade:
# MAGIC
# MAGIC * Taxa de retenção: [ver reconciliação acima]
# MAGIC * Registros com duração válida: [ver estatísticas]
# MAGIC * Cobertura de classificação: 100%
# MAGIC
# MAGIC ### 📝 Próximos Passos:
# MAGIC
# MAGIC - [ ] Implementar transformação silver com regras validadas
# MAGIC - [ ] Documentar decisões em evolucao_projeto.md
# MAGIC - [ ] Adicionar guardrails no código de produção
# MAGIC - [ ] Configurar monitoramento de qualidade (Data Quality)

# COMMAND ----------

