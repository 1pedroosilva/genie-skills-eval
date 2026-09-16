# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,EDA 1 - Bronze Interrupções
from pyspark.sql import functions as F
from pyspark.sql.window import Window

print("=" * 80)
print("EDA 1 - BRONZE INTERRUPÇÕES")
print("Tabela: workspace.proj_aneel_cont_01_bronze.101_interrupcoes")
print("=" * 80)

# Carregar tabela
df = spark.table("workspace.proj_aneel_cont_01_bronze.101_interrupcoes")

# 1. Total de registros
print("\n📊 TOTAL DE REGISTROS")
total = df.count()
print(f"Total: {total:,} interrupções registradas")

# 2. Período dos dados
print("\n📅 PERÍODO DOS DADOS")
periodo = df.select(
    F.min("DatInicioInterrupcao").alias("inicio"),
    F.max("DatInicioInterrupcao").alias("fim")
).collect()[0]
print(f"Início: {periodo['inicio']}")
print(f"Fim: {periodo['fim']}")

# 3. Top 5 distribuidoras por número de interrupções
print("\n🏆 TOP 5 DISTRIBUIDORAS POR NÚMERO DE INTERRUPÇÕES")
top_dist = df.groupBy("SigAgente").count() \
    .orderBy(F.desc("count")) \
    .limit(5)
display(top_dist)

# 4. Distribuição por tipo de interrupção
print("\n📋 DISTRIBUIÇÃO POR TIPO DE INTERRUPÇÃO")
tipo_dist = df.groupBy("DscTipoInterrupcao").count() \
    .orderBy(F.desc("count"))
display(tipo_dist)

# 5. Estatísticas de duração
print("\n⏱️ ESTATÍSTICAS DE DURAÇÃO (HORAS)")
df_duracao = df.withColumn(
    "duracao_horas",
    (F.unix_timestamp("DatFimInterrupcao") - F.unix_timestamp("DatInicioInterrupcao")) / 3600
)
estat_duracao = df_duracao.select(
    F.mean("duracao_horas").alias("media_horas"),
    F.min("duracao_horas").alias("min_horas"),
    F.max("duracao_horas").alias("max_horas"),
    F.stddev("duracao_horas").alias("desvio_padrao")
).collect()[0]
print(f"Média: {estat_duracao['media_horas']:.2f} horas")
print(f"Mínimo: {estat_duracao['min_horas']:.2f} horas")
print(f"Máximo: {estat_duracao['max_horas']:.2f} horas")
print(f"Desvio Padrão: {estat_duracao['desvio_padrao']:.2f} horas")

# 6. Amostra de 5 registros
print("\n📄 AMOSTRA DE 5 REGISTROS")
amostra = df.select(
    "SigAgente", "DscTipoInterrupcao", "DatInicioInterrupcao", 
    "DatFimInterrupcao", "DscFatoGeradorInterrupcao"
).limit(5)
display(amostra)

# COMMAND ----------

# DBTITLE 1,EDA 2 - Bronze Indicadores
print("=" * 80)
print("EDA 2 - BRONZE INDICADORES")
print("Tabela: workspace.proj_aneel_cont_01_bronze.indicadores_continuidade")
print("=" * 80)

# Carregar tabela
df = spark.table("workspace.proj_aneel_cont_01_bronze.indicadores_continuidade")

# 1. Total de registros
print("\n📊 TOTAL DE REGISTROS")
total = df.count()
print(f"Total: {total:,} indicadores registrados")

# 2. Período dos dados
print("\n📅 PERÍODO DOS DADOS")
periodo = df.select(
    F.min("datgeracaoconjuntodados").alias("inicio"),
    F.max("datgeracaoconjuntodados").alias("fim")
).collect()[0]
print(f"Início: {periodo['inicio']}")
print(f"Fim: {periodo['fim']}")

# 3. Quantidade de distribuidoras únicas
print("\n🏢 DISTRIBUIDORAS ÚNICAS")
dist_unicas = df.select("sigagente").distinct().count()
print(f"Total: {dist_unicas} distribuidoras")

# 4. Tipos de indicadores e contagem
print("\n📊 TIPOS DE INDICADORES E CONTAGEM")
indicadores = df.groupBy("sigindicador").count() \
    .orderBy(F.desc("count"))
display(indicadores)

# 5. Estatísticas de valores por indicador (top 10)
print("\n📈 ESTATÍSTICAS DE VALORES POR INDICADOR (TOP 10)")
estat_valores = df.groupBy("sigindicador").agg(
    F.mean("vlrindiceenviado").alias("media"),
    F.min("vlrindiceenviado").alias("minimo"),
    F.max("vlrindiceenviado").alias("maximo"),
    F.count("*").alias("quantidade")
).orderBy(F.desc("quantidade")).limit(10)
display(estat_valores)

# 6. Amostra de 5 registros
print("\n📄 AMOSTRA DE 5 REGISTROS")
amostra = df.select(
    "sigagente", "sigindicador", "vlrindiceenviado", 
    "anoindice", "numperiodoindice", "dscconjundconsumidoras"
).limit(5)
display(amostra)

# COMMAND ----------

# DBTITLE 1,EDA 3 - Silver Interrupções
print("=" * 80)
print("EDA 3 - SILVER INTERRUPÇÕES")
print("Tabela: workspace.proj_aneel_cont_01_silver.interrupcoes")
print("=" * 80)

# Carregar tabelas
df_silver = spark.table("workspace.proj_aneel_cont_01_silver.interrupcoes")
df_bronze = spark.table("workspace.proj_aneel_cont_01_bronze.101_interrupcoes")

# 1. Total de registros
print("\n📊 TOTAL DE REGISTROS")
total_silver = df_silver.count()
total_bronze = df_bronze.count()
perc_filtrado = ((total_bronze - total_silver) / total_bronze) * 100
print(f"Silver: {total_silver:,} interrupções")
print(f"Bronze: {total_bronze:,} interrupções")
print(f"Filtrados: {total_bronze - total_silver:,} ({perc_filtrado:.2f}%)")

# 2. Top 5 distribuidoras por tempo total de interrupção
print("\n🏆 TOP 5 DISTRIBUIDORAS POR TEMPO TOTAL DE INTERRUPÇÃO")
top_tempo = df_silver.groupBy("sig_agente").agg(
    F.sum("duracao_horas").alias("total_horas"),
    F.count("*").alias("qtd_interrupcoes")
).orderBy(F.desc("total_horas")).limit(5)
display(top_tempo)

# 3. Distribuição de fato_gerador_origem
print("\n📋 DISTRIBUIÇÃO DE FATO GERADOR ORIGEM")
fato_dist = df_silver.groupBy("fato_gerador_origem").count() \
    .orderBy(F.desc("count")).limit(10)
display(fato_dist)

# 4. Estatísticas de qualidade
print("\n✅ ESTATÍSTICAS DE QUALIDADE")
total_nulos = df_silver.select(
    F.sum(F.when(F.col("duracao_horas").isNull(), 1).otherwise(0)).alias("duracao_nula"),
    F.sum(F.when(F.col("fato_gerador_origem").isNull(), 1).otherwise(0)).alias("fato_nulo")
).collect()[0]
duracao_media = df_silver.select(F.mean("duracao_horas")).collect()[0][0]
print(f"Registros com duração nula: {total_nulos['duracao_nula']:,}")
print(f"Registros com fato gerador nulo: {total_nulos['fato_nulo']:,}")
print(f"Duração média: {duracao_media:.2f} horas")

# 5. Amostra de 5 registros
print("\n📄 AMOSTRA DE 5 REGISTROS")
amostra = df_silver.select(
    "sig_agente", "dsc_tipo_interrupcao", "dat_inicio_interrupcao", 
    "dat_fim_interrupcao", "duracao_horas", "fato_gerador_origem"
).limit(5)
display(amostra)

# COMMAND ----------

# DBTITLE 1,EDA 4 - Silver Indicadores
print("=" * 80)
print("EDA 4 - SILVER INDICADORES")
print("Tabela: workspace.proj_aneel_cont_02_silver.indicadores_continuidade")
print("=" * 80)

# Carregar tabela
df = spark.table("workspace.proj_aneel_cont_02_silver.indicadores_continuidade")

# 1. Total de registros
print("\n📊 TOTAL DE REGISTROS")
total = df.count()
print(f"Total: {total:,} indicadores")

# 2. Período de apuração
print("\n📅 PERÍODO DE APURAÇÃO")
periodo = df.select(
    F.min("data_apuracao").alias("inicio"),
    F.max("data_apuracao").alias("fim")
).collect()[0]
print(f"Início: {periodo['inicio']}")
print(f"Fim: {periodo['fim']}")

# 3. Top 5 distribuidoras com maior DEC médio
print("\n🏆 TOP 5 DISTRIBUIDORAS COM MAIOR DEC MÉDIO")
top_dec = df.filter(F.col("indicador") == "DEC").groupBy("distribuidora").agg(
    F.mean("valor").alias("dec_medio"),
    F.count("*").alias("qtd_medicoes")
).orderBy(F.desc("dec_medio")).limit(5)
display(top_dec)

# 4. Distribuição dos 5 indicadores mais comuns
print("\n📊 DISTRIBUIÇÃO DOS 5 INDICADORES MAIS COMUNS")
top_indicadores = df.groupBy("indicador").count() \
    .orderBy(F.desc("count")).limit(5)
display(top_indicadores)

# 5. Estatísticas: média de valor por indicador
print("\n📈 MÉDIA DE VALOR POR INDICADOR (TOP 10)")
media_indicador = df.groupBy("indicador").agg(
    F.mean("valor").alias("valor_medio"),
    F.stddev("valor").alias("desvio_padrao"),
    F.count("*").alias("quantidade")
).orderBy(F.desc("quantidade")).limit(10)
display(media_indicador)

# 6. Amostra de 5 registros
print("\n📄 AMOSTRA DE 5 REGISTROS")
amostra = df.select(
    "distribuidora", "indicador", "valor", 
    "data_apuracao", "ano", "mes", "conjunto_uc"
).limit(5)
display(amostra)

# COMMAND ----------

# DBTITLE 1,EDA 5 - Gold Tempo Interrupção
print("=" * 80)
print("EDA 5 - GOLD TEMPO INTERRUPÇÃO MENSAL")
print("Tabela: workspace.proj_aneel_cont_03_gold.tempo_interrupcao_mensal")
print("=" * 80)

# Carregar tabela
df = spark.table("workspace.proj_aneel_cont_03_gold.tempo_interrupcao_mensal")

# 1. Total de registros (agregações mensais)
print("\n📊 TOTAL DE REGISTROS (AGREGAÇÕES MENSAIS)")
total = df.count()
print(f"Total: {total:,} agregações mensais")

# 2. Período coberto
print("\n📅 PERÍODO COBERTO")
periodo = df.select(
    F.min("ano_mes").alias("inicio"),
    F.max("ano_mes").alias("fim")
).collect()[0]
print(f"Início: {periodo['inicio']}")
print(f"Fim: {periodo['fim']}")

# 3. Top 5 distribuidoras com maior dec_medio em 2025
print("\n🏆 TOP 5 DISTRIBUIDORAS COM MAIOR DEC MÉDIO EM 2025")
top_dec_2025 = df.filter(F.col("ano") == 2025).groupBy("distribuidora").agg(
    F.mean("dec_medio").alias("dec_medio_anual"),
    F.sum("numcon_total").alias("total_consumidores"),
    F.count("*").alias("meses_registrados")
).orderBy(F.desc("dec_medio_anual")).limit(5)
display(top_dec_2025)

# 4. Estatísticas gerais
print("\n📈 ESTATÍSTICAS GERAIS")
estat = df.select(
    F.mean("dec_medio").alias("media_dec_medio"),
    F.mean("fec_medio").alias("media_fec_medio"),
    F.mean("numcon_total").alias("media_consumidores")
).collect()[0]
print(f"Média DEC: {estat['media_dec_medio']:.2f} horas")
print(f"Média FEC: {estat['media_fec_medio']:.2f} interrupções")
print(f"Média de consumidores: {estat['media_consumidores']:,.0f}")

# 5. Tendência: variação percentual média mês a mês
print("\n📊 TENDÊNCIA: VARIAÇÃO PERCENTUAL MÉDIA MÊS A MÊS")
variacao = df.filter(F.col("variacao_perc_mes_anterior").isNotNull()).select(
    F.mean("variacao_perc_mes_anterior").alias("variacao_media"),
    F.stddev("variacao_perc_mes_anterior").alias("desvio_padrao"),
    F.min("variacao_perc_mes_anterior").alias("min_variacao"),
    F.max("variacao_perc_mes_anterior").alias("max_variacao")
).collect()[0]
print(f"Variação média: {variacao['variacao_media']:.2f}%")
print(f"Desvio padrão: {variacao['desvio_padrao']:.2f}%")
print(f"Min variação: {variacao['min_variacao']:.2f}%")
print(f"Max variação: {variacao['max_variacao']:.2f}%")

# 6. Amostra de 5 registros
print("\n📄 AMOSTRA DE 5 REGISTROS")
amostra = df.select(
    "distribuidora", "ano", "mes", "dec_medio", 
    "fec_medio", "numcon_total", "ranking_dec_mes"
).limit(5)
display(amostra)

# COMMAND ----------

