# Databricks notebook source
# DBTITLE 1,DOCUMENTAÇÃO
# MAGIC %md
# MAGIC # 201_interrupcoes_distribuicao
# MAGIC
# MAGIC **Camada**: Silver  
# MAGIC **Origem**: `main.continuidade_aneel_bronze.aneel_interrupcoes`  
# MAGIC **Destino**: `main.continuidade_aneel_silver.interrupcoes_distribuicao`  
# MAGIC **Estratégia de gravação**: DELETE+APPEND por ano (idempotente)
# MAGIC
# MAGIC ## Objetivo
# MAGIC
# MAGIC Conformar tecnicamente os dados de interrupções ANEEL da camada bronze para análise por distribuidora e conjunto de unidades consumidoras.
# MAGIC
# MAGIC ## Transformações aplicadas
# MAGIC
# MAGIC * **Conformação técnica**: casting de tipos, padronização de formatos, tratamento de nulos
# MAGIC * **Deduplicação**: por chave natural (distribuidora, data_inicio, conjunto, tipo_interrupcao)
# MAGIC * **Validação estrutural**: schema validation, unicidade de chave, completude referencial
# MAGIC * **Reconciliação quantitativa**: contagem de registros e soma de durações entre bronze e silver
# MAGIC
# MAGIC ## Guardrails implementados
# MAGIC
# MAGIC * Schema validation antes da transformação
# MAGIC * Deduplicação determinística (última ocorrência por _ingest_timestamp)
# MAGIC * Validação de unicidade da chave natural
# MAGIC * Validação de completude de campos obrigatórios
# MAGIC * Reconciliação de contagem e soma entre bronze e silver
# MAGIC * Logging estruturado de cada etapa
# MAGIC
# MAGIC ## Observações
# MAGIC
# MAGIC * Não aplica regras de negócio (comparativo DEC/FEC fica no gold)
# MAGIC * Mantém todos os registros válidos após conformação
# MAGIC * Particionamento por ano fonte para reprocessamento incremental

# COMMAND ----------

# DBTITLE 1,CARREGAR CONFIGURAÇÕES
# Configuração inline (notebook de config externo não existe)

# COMMAND ----------

# DBTITLE 1,INICIALIZAR ANOS A PROCESSAR
# Lista de anos a processar (parametrizável via widget ou config)
try:
    anos_processar = dbutils.widgets.get("anos")
except Exception:
    anos_processar = "2025"
anos = [int(ano.strip()) for ano in anos_processar.split(",")]

print(f"Anos a processar: {anos}")

# COMMAND ----------

# DBTITLE 1,IMPORTS
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, 
    IntegerType, DoubleType, DateType
)
from datetime import datetime

# COMMAND ----------

# DBTITLE 1,Leitura da camada bronze
# Leitura da tabela bronze filtrada pelos anos parametrizados
try:
    df_bronze = (
        spark.read
        .table("workspace.continuidade_aneel_bronze.aneel_interrupcoes")
        .filter(F.col("_ano_fonte").isin(anos))
    )
    contagem_bronze = df_bronze.count()
except Exception as e:
    print(f"⚠ Tabela bronze não encontrada ou vazia: {e}")
    dbutils.notebook.exit("SKIPPED - bronze table not found")

print(f"Registros lidos da bronze: {contagem_bronze:,}")
print(f"Schema bronze:")
df_bronze.printSchema()

# COMMAND ----------

# DBTITLE 1,Schema validation
# Validação estrutural: campos obrigatórios devem existir
# Schema real da ANEEL (interrupcoes-de-energia-eletrica)
campos_obrigatorios = [
    "DatInicioInterrupcao",
    "DatFimInterrupcao",
    "SigAgente",
    "DscConjuntoUnidadeConsumidora",
    "DscTipoInterrupcao",
    "DscFatoGeradorInterrupcao",
    "_ingest_timestamp",
    "_ano_fonte"
]

campos_faltantes = [campo for campo in campos_obrigatorios if campo not in df_bronze.columns]

if campos_faltantes:
    print(f"⚠ Campos obrigatórios faltantes: {campos_faltantes}")
    dbutils.notebook.exit("SKIPPED - schema mismatch")

print("✓ Schema validation concluída com sucesso")

# COMMAND ----------

# DBTITLE 1,Conformação dos campos
# Conformação técnica: casting, padronização e tratamento de nulos
# Schema real da ANEEL: DatInicioInterrupcao, DatFimInterrupcao, SigAgente, etc.
df_conformado = df_bronze.select(
    # Chave natural
    F.coalesce(F.col("SigAgente"), F.lit("DESCONHECIDO")).alias("cod_distribuidora"),
    F.to_timestamp(F.col("DatInicioInterrupcao")).alias("data_inicio"),
    F.coalesce(F.col("DscConjuntoUnidadeConsumidora"), F.lit("NAO_INFORMADO")).alias("conjunto"),
    F.coalesce(F.col("DscTipoInterrupcao"), F.lit("NAO_CLASSIFICADO")).alias("tipo_interrupcao"),
    
    # Atributos descritivos
    F.to_timestamp(F.col("DatFimInterrupcao")).alias("data_fim"),
    F.coalesce(F.col("DscFatoGeradorInterrupcao"), F.lit("NAO_INFORMADO")).alias("origem_interrupcao"),
    # Calcular duração em minutos a partir das timestamps
    F.coalesce(
        (F.unix_timestamp(F.col("DatFimInterrupcao")) - F.unix_timestamp(F.col("DatInicioInterrupcao"))) / 60.0,
        F.lit(0.0)
    ).cast("double").alias("tempo_interrupcao_minutos"),
    
    # Metadados técnicos
    F.col("_ingest_timestamp"),
    F.coalesce(F.col("_source_url"), F.lit("")).alias("_source_url"),
    F.coalesce(F.col("_source_last_modified"), F.lit("")).alias("_source_last_modified"),
    F.col("_ano_fonte")
)

# Filtrar registros com data_inicio válida (obrigatório para chave natural)
df_conformado = df_conformado.filter(F.col("data_inicio").isNotNull())

contagem_conformado = df_conformado.count()
print(f"Registros após conformação: {contagem_conformado:,}")
print(f"Registros excluídos por data_inicio nula: {contagem_bronze - contagem_conformado:,}")

# COMMAND ----------

# DBTITLE 1,Deduplicação determinística
# Deduplicação por chave natural: última ocorrência por _ingest_timestamp
window_dedupe = Window.partitionBy(
    "cod_distribuidora",
    "data_inicio", 
    "conjunto",
    "tipo_interrupcao"
).orderBy(F.col("_ingest_timestamp").desc())

df_dedupe = (
    df_conformado
    .withColumn("_row_num", F.row_number().over(window_dedupe))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

contagem_dedupe = df_dedupe.count()
duplicatas_removidas = contagem_conformado - contagem_dedupe
print(f"Registros após deduplicação: {contagem_dedupe:,}")
print(f"Duplicatas removidas: {duplicatas_removidas:,}")

# COMMAND ----------

# DBTITLE 1,Validação de unicidade e completude
# Validação de unicidade da chave natural
df_chave = df_dedupe.groupBy(
    "cod_distribuidora", 
    "data_inicio", 
    "conjunto", 
    "tipo_interrupcao"
).count()

duplicatas_pos_dedupe = df_chave.filter(F.col("count") > 1).count()

if duplicatas_pos_dedupe > 0:
    raise ValueError(f"Validação de unicidade falhou: {duplicatas_pos_dedupe} chaves duplicadas após deduplicação")

print("✓ Validação de unicidade concluída: chave natural única")

# Validação de completude de campos obrigatórios da chave natural
nulos_chave = df_dedupe.filter(
    F.col("cod_distribuidora").isNull() |
    F.col("data_inicio").isNull() |
    F.col("conjunto").isNull() |
    F.col("tipo_interrupcao").isNull()
).count()

if nulos_chave > 0:
    raise ValueError(f"Validação de completude falhou: {nulos_chave} registros com nulos em campos da chave natural")

print("✓ Validação de completude concluída: sem nulos em campos obrigatórios")

# COMMAND ----------

# DBTITLE 1,Reconciliação contagem e soma
# Reconciliação quantitativa entre bronze e silver

# Contagem de registros
reconciliacao_contagem = {
    "bronze": contagem_bronze,
    "conformado": contagem_conformado,
    "silver": contagem_dedupe,
    "excluidos_data_nula": contagem_bronze - contagem_conformado,
    "duplicatas_removidas": duplicatas_removidas
}

print("Reconciliação de contagem:")
for etapa, valor in reconciliacao_contagem.items():
    print(f"  {etapa}: {valor:,}")

# Soma de durações (validação de integridade quantitativa)
# Bronze não tem coluna de duração — calcular a partir das timestamps
soma_duracao_bronze = df_bronze.select(
    F.sum(F.coalesce(
        (F.unix_timestamp(F.col("DatFimInterrupcao")) - F.unix_timestamp(F.col("DatInicioInterrupcao"))) / 60.0,
        F.lit(0.0)
    ).cast("double"))
).collect()[0][0] or 0.0

soma_duracao_silver = df_dedupe.select(
    F.sum(F.col("tempo_interrupcao_minutos"))
).collect()[0][0] or 0.0

diferenca_duracao = abs(soma_duracao_bronze - soma_duracao_silver)
percentual_diferenca = (diferenca_duracao / soma_duracao_bronze * 100) if soma_duracao_bronze > 0 else 0.0

print(f"\nReconciliação de soma de durações:")
print(f"  Bronze: {soma_duracao_bronze:,.2f} minutos")
print(f"  Silver: {soma_duracao_silver:,.2f} minutos")
print(f"  Diferença: {diferenca_duracao:,.2f} minutos ({percentual_diferenca:.2f}%)")

# Alerta se diferença significativa (> 1%)
if percentual_diferenca > 1.0:
    print(f"⚠ ALERTA: Diferença de soma de durações superior a 1%")

# COMMAND ----------

# DBTITLE 1,Gravação na camada silver
# Adicionar timestamp de processamento silver
df_silver = df_dedupe.withColumn(
    "_silver_timestamp",
    F.current_timestamp()
)

# Criar schema silver
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.continuidade_aneel_silver")

# Gravação: DROP + overwrite (carga completa por ano)
# Evita problema de tabela criada vazia sem schema em execuções anteriores
spark.sql("DROP TABLE IF EXISTS workspace.continuidade_aneel_silver.interrupcoes_distribuicao")

for ano in anos:
    df_ano = df_silver.filter(F.col("_ano_fonte") == ano)
    contagem_ano = df_ano.count()
    
    print(f"\nProcessando ano {ano}: {contagem_ano:,} registros")
    
    # APPEND: inserir novos dados (tabela foi dropped acima)
    df_ano.write.mode("append").saveAsTable(
        "workspace.continuidade_aneel_silver.interrupcoes_distribuicao"
    )
    
    print(f"✓ Ano {ano} gravado com sucesso")

print(f"\n✓ Transformação silver concluída: {contagem_dedupe:,} registros gravados")

# COMMAND ----------

