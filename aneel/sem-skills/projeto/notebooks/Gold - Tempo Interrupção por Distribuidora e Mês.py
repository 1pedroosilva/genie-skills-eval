# Databricks notebook source
# DBTITLE 1,Configuração e Importações
# Notebook: Tabela Analítica Gold - Tempo de Interrupção por Distribuidora e Mês
# Projeto: Continuidade de Fornecimento
# Origem: workspace.proj_aneel_cont_02_silver.indicadores_continuidade
# Destino: workspace.proj_aneel_cont_03_gold.tempo_interrupcao_mensal

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

print(f"Processamento iniciado em: {datetime.now()}")

# COMMAND ----------

# DBTITLE 1,Leitura dos Dados Silver
# Leitura da tabela silver de indicadores de continuidade
df_indicadores = spark.table("workspace.proj_aneel_cont_02_silver.indicadores_continuidade")

print(f"Total de registros lidos: {df_indicadores.count():,}")
print(f"\nSchema da tabela silver:")
df_indicadores.printSchema()

# COMMAND ----------

# DBTITLE 1,Transformação - Agregação por Distribuidora e Mês
# Pivotar indicadores: transformar de formato long para wide
# A tabela Silver tem colunas: indicador (DEC, FEC, etc) e valor
df_pivotado = df_indicadores.groupBy(
    "distribuidora",
    "cnpj",
    "data_apuracao",
    "id_conjunto_uc",
    "conjunto_uc"
).pivot("indicador").agg(
    F.first("valor")
)

# Extrair ano e mês do período de apuração
df_com_mes = df_pivotado.withColumn(
    "ano", F.year(F.col("data_apuracao"))
).withColumn(
    "mes", F.month(F.col("data_apuracao"))
).withColumn(
    "ano_mes", F.date_trunc("month", F.col("data_apuracao"))
)

# Agregação por distribuidora e mês
df_agregado = df_com_mes.groupBy(
    "distribuidora",
    "cnpj",
    "ano",
    "mes",
    "ano_mes"
).agg(
    # Tempo total de interrupção (DEC - Duração Equivalente de Interrupção)
    F.avg("DEC").alias("dec_medio"),
    F.min("DEC").alias("dec_minimo"),
    F.max("DEC").alias("dec_maximo"),
    F.sum("DEC").alias("dec_total"),
    
    # DEC Indicador (descontando interrupções programadas)
    F.avg("DECIND").alias("decind_medio"),
    F.sum("DECIND").alias("decind_total"),
    
    # DEC Conformidade (dentro dos limites)
    F.avg("DECINC").alias("decinc_medio"),
    F.sum("DECINC").alias("decinc_total"),
    
    # Frequência de interrupção (FEC)
    F.avg("FEC").alias("fec_medio"),
    F.sum("FEC").alias("fec_total"),
    
    # Número de consumidores
    F.avg("NumCon").alias("numcon_medio"),
    F.sum("NumCon").alias("numcon_total"),
    
    # Número de conjuntos (linhas agregadas)
    F.count("*").alias("qtd_conjuntos")
)

print(f"Total de registros após agregação: {df_agregado.count():,}")
print(f"\nAmostra dos dados agregados:")
df_agregado.orderBy("ano", "mes", "distribuidora").show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Enriquecimento - Métricas Adicionais
# Adicionar métricas de evolução temporal e ranking
window_distribuidora = Window.partitionBy("distribuidora").orderBy("ano_mes")
window_mes = Window.partitionBy("ano", "mes").orderBy(F.desc("dec_medio"))

df_enriquecido = df_agregado.withColumn(
    # Evolução mês a mês por distribuidora
    "dec_medio_mes_anterior",
    F.lag("dec_medio", 1).over(window_distribuidora)
).withColumn(
    # Variação percentual mês a mês
    "variacao_perc_mes_anterior",
    F.when(
        F.col("dec_medio_mes_anterior").isNotNull() & (F.col("dec_medio_mes_anterior") != 0),
        ((F.col("dec_medio") - F.col("dec_medio_mes_anterior")) / F.col("dec_medio_mes_anterior")) * 100
    ).otherwise(None)
).withColumn(
    # Ranking de distribuidoras com maior DEC no mês
    "ranking_dec_mes",
    F.dense_rank().over(window_mes)
).withColumn(
    # Timestamp de processamento
    "timestamp_processamento",
    F.current_timestamp()
)

print(f"\nAmostra dos dados enriquecidos:")
df_enriquecido.select(
    "distribuidora", "ano", "mes", "dec_medio", 
    "dec_medio_mes_anterior", "variacao_perc_mes_anterior", "ranking_dec_mes"
).orderBy("ano", "mes", "ranking_dec_mes").show(10, truncate=False)

# COMMAND ----------

# DBTITLE 1,Gravação na Camada Gold
# Nome da tabela destino
tabela_gold = "workspace.proj_aneel_cont_03_gold.tempo_interrupcao_mensal"

# Gravação na camada gold (modo overwrite - sobrescreve dados existentes)
df_enriquecido.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable(tabela_gold)

print(f"\n✅ Tabela gold criada com sucesso: {tabela_gold}")
print(f"Total de registros gravados: {df_enriquecido.count():,}")

# COMMAND ----------

# DBTITLE 1,Validação - Contagem por Ano
# MAGIC %sql
# MAGIC -- Validação: Contagem de registros por ano
# MAGIC SELECT 
# MAGIC   ano,
# MAGIC   COUNT(*) as qtd_registros,
# MAGIC   COUNT(DISTINCT distribuidora) as qtd_distribuidoras
# MAGIC FROM workspace.proj_aneel_cont_03_gold.tempo_interrupcao_mensal
# MAGIC GROUP BY ano
# MAGIC ORDER BY ano

# COMMAND ----------

# DBTITLE 1,Análise - Top 10 Distribuidoras por DEC
# MAGIC %sql
# MAGIC -- Top 10 distribuidoras com maior tempo de interrupção médio
# MAGIC SELECT 
# MAGIC   distribuidora,
# MAGIC   ROUND(AVG(dec_medio), 2) as dec_medio_anual,
# MAGIC   ROUND(AVG(decind_medio), 2) as decind_medio_anual,
# MAGIC   ROUND(AVG(fec_medio), 2) as fec_medio_anual,
# MAGIC   COUNT(*) as qtd_meses
# MAGIC FROM workspace.proj_aneel_cont_03_gold.tempo_interrupcao_mensal
# MAGIC WHERE ano = 2025
# MAGIC GROUP BY distribuidora
# MAGIC ORDER BY dec_medio_anual DESC
# MAGIC LIMIT 10

# COMMAND ----------

# DBTITLE 1,Análise - Evolução Temporal
# MAGIC %sql
# MAGIC -- Evolução mensal do tempo de interrupção por distribuidora
# MAGIC -- Altere o filtro WHERE conforme necessário
# MAGIC SELECT 
# MAGIC   distribuidora,
# MAGIC   ano,
# MAGIC   mes,
# MAGIC   ano_mes,
# MAGIC   ROUND(dec_medio, 2) as dec_medio,
# MAGIC   ROUND(dec_medio_mes_anterior, 2) as dec_mes_anterior,
# MAGIC   ROUND(variacao_perc_mes_anterior, 1) as variacao_perc,
# MAGIC   ranking_dec_mes,
# MAGIC   qtd_conjuntos,
# MAGIC   ROUND(numcon_total, 0) as total_consumidores
# MAGIC FROM workspace.proj_aneel_cont_03_gold.tempo_interrupcao_mensal
# MAGIC WHERE distribuidora = 'CASTRO-DIS'
# MAGIC ORDER BY ano_mes

# COMMAND ----------

# DBTITLE 1,Documentação
# MAGIC %md
# MAGIC # Tabela Gold: tempo_interrupcao_mensal
# MAGIC
# MAGIC ## Descrição
# MAGIC Tabela analítica com tempo total de interrupção agregado por distribuidora e mês.
# MAGIC
# MAGIC **Objetivo**: Permitir comparação do tempo observado com DEC apurado e acompanhar evolução ao longo do ano.
# MAGIC
# MAGIC ## Origem
# MAGIC `workspace.proj_aneel_cont_02_silver.indicadores_continuidade`
# MAGIC
# MAGIC ## Campos Principais
# MAGIC
# MAGIC ### Identificação
# MAGIC * `distribuidora`, `sigagente`, `codigo_distribuidora`
# MAGIC * `ano`, `mes`, `ano_mes` (período de referência)
# MAGIC
# MAGIC ### Indicadores DEC (Tempo de Interrupção)
# MAGIC * `dec_medio` / `dec_minimo` / `dec_maximo` / `dec_total`
# MAGIC * `decind_medio` / `decind_total` (sem interrupções programadas)
# MAGIC * `decinc_medio` / `decinc_total` (conformidade)
# MAGIC
# MAGIC ### Indicadores FEC (Frequência)
# MAGIC * `fec_medio` / `fec_total`
# MAGIC
# MAGIC ### Consumidores
# MAGIC * `numcon_medio` / `numcon_total`
# MAGIC * `qtd_conjuntos` (conjuntos agregados)
# MAGIC
# MAGIC ### Métricas de Evolução
# MAGIC * `dec_medio_mes_anterior` (DEC do mês anterior)
# MAGIC * `variacao_perc_mes_anterior` (variação % mês a mês)
# MAGIC * `ranking_dec_mes` (posição no ranking mensal)
# MAGIC
# MAGIC ## Casos de Uso
# MAGIC 1. Identificar distribuidoras com maiores interrupções
# MAGIC 2. Acompanhar tendências e variações mensais
# MAGIC 3. Comparar DEC observado vs. metas regulatórias
# MAGIC 4. Detectar anomalias e picos de interrupção