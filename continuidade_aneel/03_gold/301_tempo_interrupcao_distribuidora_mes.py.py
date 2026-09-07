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
# MAGIC ## Propósito
# MAGIC
# MAGIC Agrega eventos de interrupção de distribuição de energia elétrica por distribuidora e mês, calculando tempo total de interrupção em minutos e horas. Permite análise temporal da continuidade do fornecimento e comparação com indicadores regulatórios (DEC).
# MAGIC
# MAGIC ## Transformações
# MAGIC
# MAGIC ### Agregação
# MAGIC
# MAGIC Agrupa registros de `interrupcoes_distribuicao` por:
# MAGIC * `cod_distribuidora`
# MAGIC * `ano_referencia` (extraído de `data_inicio`)
# MAGIC * `mes_referencia` (extraído de `data_inicio`)
# MAGIC
# MAGIC ### Métricas calculadas
# MAGIC
# MAGIC * `tempo_total_minutos`: soma de `tempo_interrupcao_minutos` (nulos tratados como 0)
# MAGIC * `tempo_total_horas`: `tempo_total_minutos / 60` arredondado para 2 casas
# MAGIC * `total_eventos`: contagem de registros
# MAGIC * `data_primeiro_evento`: menor `data_inicio` do grupo
# MAGIC * `data_ultimo_evento`: maior `data_inicio` do grupo
# MAGIC
# MAGIC ### Metadados
# MAGIC
# MAGIC * `_gold_timestamp`: timestamp de processamento
# MAGIC * `_ano_fonte`: cópia de `ano_referencia` para rastreabilidade
# MAGIC
# MAGIC ## Guardrails
# MAGIC
# MAGIC ### Validações estruturais
# MAGIC
# MAGIC * Campos obrigatórios presentes: `cod_distribuidora`, `data_inicio`, `tempo_interrupcao_minutos`, `_ano_fonte`
# MAGIC * Execução interrompida se campos faltantes
# MAGIC
# MAGIC ### Validações de qualidade
# MAGIC
# MAGIC * Nulos críticos: `cod_distribuidora` e `data_inicio` não podem ser nulos
# MAGIC * Valores inválidos: `tempo_interrupcao_minutos` nulo ou negativo tratado como 0 (com aviso)
# MAGIC * Execução interrompida se nulos críticos detectados
# MAGIC
# MAGIC ### Validações de consistência
# MAGIC
# MAGIC * Unicidade: uma linha por `(cod_distribuidora, ano_referencia, mes_referencia)`
# MAGIC * Reconciliação: soma de `tempo_total_minutos` (gold) deve corresponder à soma de `tempo_interrupcao_minutos` (silver) com tolerância de 0.01%
# MAGIC * Execução interrompida se duplicatas ou divergência acima da tolerância
# MAGIC
# MAGIC ## Estratégia de gravação
# MAGIC
# MAGIC Para cada ano processado:
# MAGIC 1. DELETE de registros do ano na tabela destino
# MAGIC 2. APPEND dos novos registros do ano
# MAGIC
# MAGIC Garantia de idempotência: reprocessamento do mesmo ano produz resultado idêntico.
# MAGIC
# MAGIC ## Parametrização
# MAGIC
# MAGIC Widget `anos`: lista de anos a processar separados por vírgula (ex: "2023,2024,2025")  
# MAGIC Fallback: `"2023,2024,2025"` se widget não configurado
# MAGIC
# MAGIC ## Dependências
# MAGIC
# MAGIC * Notebook de configuração: `../00_config/config`
# MAGIC * Tabela silver: `main.continuidade_aneel_silver.interrupcoes_distribuicao`
# MAGIC
# MAGIC ## Saída
# MAGIC
# MAGIC Tabela: `main.continuidade_aneel_gold.tempo_interrupcao_distribuidora_mes`
# MAGIC
# MAGIC Estrutura:
# MAGIC * `cod_distribuidora` (string)
# MAGIC * `ano_referencia` (int)
# MAGIC * `mes_referencia` (int)
# MAGIC * `tempo_total_minutos` (double)
# MAGIC * `tempo_total_horas` (double)
# MAGIC * `total_eventos` (long)
# MAGIC * `data_primeiro_evento` (timestamp)
# MAGIC * `data_ultimo_evento` (timestamp)
# MAGIC * `_gold_timestamp` (timestamp)
# MAGIC * `_ano_fonte` (int)

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

