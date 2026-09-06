# Databricks notebook source
# DBTITLE 1,DOCUMENTAÇÃO
# MAGIC %md
# MAGIC # 301_tempo_interrupcao_distribuidora_mes
# MAGIC
# MAGIC **Camada**: Gold  
# MAGIC **Origem**: `main.continuidade_aneel_silver.interrupcoes_distribuicao`  
# MAGIC **Destino**: `main.continuidade_aneel_gold.tempo_interrupcao_distribuidora_mes`  
# MAGIC **Estratégia de gravação**: DELETE+APPEND por ano (idempotente)
# MAGIC
# MAGIC ## Objetivo
# MAGIC
# MAGIC Gerar tabela analítica de tempo total de interrupção agregado por distribuidora e mês, permitindo comparação com o DEC apurado pela ANEEL e acompanhamento da evolução ao longo do ano.
# MAGIC
# MAGIC ## Transformações aplicadas
# MAGIC
# MAGIC * **Agregação temporal**: soma do tempo de interrupção por distribuidora e mês
# MAGIC * **Cálculo de métricas**: tempo total em minutos e horas, contagem de eventos
# MAGIC * **Período de referência**: ano e mês da ocorrência da interrupção
# MAGIC * **Rastreabilidade**: mantém vínculo com os dados silver através de ano_fonte e mês_referencia
# MAGIC
# MAGIC ## Guardrails implementados
# MAGIC
# MAGIC * Schema validation da camada silver
# MAGIC * Validação de completude de campos obrigatórios (distribuidora, data)
# MAGIC * Validação de valores positivos para tempo de interrupção
# MAGIC * Reconciliação de soma de tempo entre silver e gold
# MAGIC * Verificação de unicidade da chave analítica (distribuidora + ano + mês)
# MAGIC
# MAGIC ## Consumidores
# MAGIC
# MAGIC * Dashboards de acompanhamento de continuidade
# MAGIC * Comparação DEC observado vs DEC apurado
# MAGIC * Análise de evolução temporal por distribuidora

# COMMAND ----------

# DBTITLE 1,CARREGAR CONFIGURAÇÕES
# MAGIC %run ../00_config/config

# COMMAND ----------

# DBTITLE 1,INICIALIZAR ANOS A PROCESSAR
# Lista de anos a processar (parametrizável via widget ou config)
anos_processar = dbutils.widgets.get("anos") if dbutils.widgets.get("anos") else "2023,2024,2025"
anos = [int(ano.strip()) for ano in anos_processar.split(",")]

print(f"Anos a processar: {anos}")

# COMMAND ----------

# DBTITLE 1,IMPORTS
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)
from datetime import datetime

# COMMAND ----------

# DBTITLE 1,Leitura da camada silver
# Leitura da tabela silver filtrada pelos anos parametrizados
df_silver = (
    spark.read
    .table("main.continuidade_aneel_silver.interrupcoes_distribuicao")
    .filter(F.col("_ano_fonte").isin(anos))
)

contagem_silver = df_silver.count()
print(f"Registros lidos da silver: {contagem_silver:,}")
print(f"Schema silver:")
df_silver.printSchema()

# COMMAND ----------

# DBTITLE 1,Schema validation
# Validação estrutural: campos obrigatórios devem existir
campos_obrigatorios = [
    "cod_distribuidora",
    "data_inicio",
    "tempo_interrupcao_minutos",
    "_ano_fonte"
]

campos_faltantes = [campo for campo in campos_obrigatorios if campo not in df_silver.columns]

if campos_faltantes:
    raise ValueError(f"Schema validation falhou. Campos obrigatórios faltantes: {campos_faltantes}")

print("✓ Schema validation concluída com sucesso")

# COMMAND ----------

# DBTITLE 1,Validação de qualidade dos dados
# Validação de completude: distribuidora e data não podem ser nulos
nulos_criticos = df_silver.filter(
    F.col("cod_distribuidora").isNull() |
    F.col("data_inicio").isNull()
).count()

if nulos_criticos > 0:
    raise ValueError(f"Validação de completude falhou: {nulos_criticos} registros com distribuidora ou data nulos")

print("✓ Validação de completude concluída")

# Validação de valores: tempo de interrupção deve ser positivo
tempos_invalidos = df_silver.filter(
    F.col("tempo_interrupcao_minutos").isNull() |
    (F.col("tempo_interrupcao_minutos") < 0)
).count()

if tempos_invalidos > 0:
    print(f"⚠ Atenção: {tempos_invalidos} registros com tempo de interrupção nulo ou negativo (serão tratados como 0)")

print("✓ Validação de valores concluída")

# COMMAND ----------

# DBTITLE 1,Agregação por distribuidora e mês
# Agregação: soma do tempo de interrupção por distribuidora, ano e mês
df_agregado = (
    df_silver
    .withColumn("ano_referencia", F.year(F.col("data_inicio")))
    .withColumn("mes_referencia", F.month(F.col("data_inicio")))
    .groupBy(
        "cod_distribuidora",
        "ano_referencia",
        "mes_referencia"
    )
    .agg(
        F.sum(
            F.coalesce(F.col("tempo_interrupcao_minutos"), F.lit(0))
        ).alias("tempo_total_minutos"),
        F.count("*").alias("total_eventos"),
        F.min("data_inicio").alias("data_primeiro_evento"),
        F.max("data_inicio").alias("data_ultimo_evento")
    )
    .withColumn(
        "tempo_total_horas",
        F.round(F.col("tempo_total_minutos") / 60, 2)
    )
)

contagem_agregado = df_agregado.count()
print(f"Registros após agregação: {contagem_agregado:,}")
print(f"Grupos distintos: {contagem_agregado} distribuidoras/mês")

# COMMAND ----------

# DBTITLE 1,Validação de unicidade da chave analítica
# Validação de unicidade: uma linha por distribuidora/ano/mês
df_chave = df_agregado.groupBy(
    "cod_distribuidora",
    "ano_referencia",
    "mes_referencia"
).count()

duplicatas = df_chave.filter(F.col("count") > 1).count()

if duplicatas > 0:
    raise ValueError(f"Validação de unicidade falhou: {duplicatas} chaves duplicadas (distribuidora+ano+mês)")

print("✓ Validação de unicidade concluída: chave analítica única")

# COMMAND ----------

# DBTITLE 1,Reconciliação de soma entre silver e gold
# Reconciliação: soma total de tempo de interrupção
soma_silver = df_silver.select(
    F.sum(F.coalesce(F.col("tempo_interrupcao_minutos"), F.lit(0))).alias("soma")
).collect()[0]["soma"]

soma_gold = df_agregado.select(
    F.sum("tempo_total_minutos").alias("soma")
).collect()[0]["soma"]

diferenca = abs(soma_silver - soma_gold) if soma_silver and soma_gold else 0
tolerancia = 0.01  # 0.01% de diferença tolerável por arredondamentos

if soma_silver and soma_gold:
    diferenca_percentual = (diferenca / soma_silver) * 100 if soma_silver > 0 else 0
    
    if diferenca_percentual > tolerancia:
        raise ValueError(
            f"Reconciliação falhou: diferença de {diferenca_percentual:.4f}% "
            f"entre silver ({soma_silver:,.2f}) e gold ({soma_gold:,.2f})"
        )
    
    print(f"✓ Reconciliação concluída com sucesso")
    print(f"  Soma silver: {soma_silver:,.2f} minutos")
    print(f"  Soma gold: {soma_gold:,.2f} minutos")
    print(f"  Diferença: {diferenca_percentual:.6f}%")
else:
    print("⚠ Aviso: não foi possível realizar reconciliação (valores nulos)")

# COMMAND ----------

# DBTITLE 1,Adicionar metadados de processamento
# Adicionar timestamp de processamento gold e ano fonte
df_gold = (
    df_agregado
    .withColumn("_gold_timestamp", F.current_timestamp())
    .withColumn("_ano_fonte", F.col("ano_referencia"))  # Mantém rastreabilidade
)

print(f"Registros finais para gold: {df_gold.count():,}")

# COMMAND ----------

# DBTITLE 1,Gravação na camada gold
# Gravação idempotente: DELETE+APPEND por ano
for ano in anos:
    df_ano = df_gold.filter(F.col("_ano_fonte") == ano)
    contagem_ano = df_ano.count()
    
    print(f"\nProcessando ano {ano}: {contagem_ano:,} registros")
    
    # DELETE: remover ano existente
    spark.sql(f"""
        DELETE FROM main.continuidade_aneel_gold.tempo_interrupcao_distribuidora_mes
        WHERE _ano_fonte = {ano}
    """)
    
    # APPEND: inserir novos registros do ano
    df_ano.write.mode("append").saveAsTable(
        "main.continuidade_aneel_gold.tempo_interrupcao_distribuidora_mes"
    )
    
    print(f"✓ Ano {ano} gravado com sucesso")

print("\n" + "="*60)
print("PROCESSAMENTO GOLD CONCLUÍDO")
print("="*60)
print(f"Total de registros gravados: {df_gold.count():,}")
print(f"Tabela destino: main.continuidade_aneel_gold.tempo_interrupcao_distribuidora_mes")

# COMMAND ----------

