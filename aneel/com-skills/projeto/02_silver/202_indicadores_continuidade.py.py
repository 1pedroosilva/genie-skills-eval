# Databricks notebook source
# MAGIC %md
# MAGIC # Transformação Silver - Indicadores de Continuidade ANEEL
# MAGIC
# MAGIC Este notebook padroniza os indicadores coletivos de continuidade (DEC, FEC, DIC, FIC, DMIC, DICRI) da camada bronze, aplicando limpezas técnicas e conformação de schema.
# MAGIC
# MAGIC **Origem:** `workspace.proj_aneel_cont_01_bronze.indicadores_continuidade`
# MAGIC **Estratégia:** DELETE WHERE + APPEND por ano
# MAGIC **Destino:** `workspace.proj_aneel_cont_02_silver.indicadores_continuidade`
# MAGIC **Camada:** Silver (conformação técnica, sem regras de negócio)

# COMMAND ----------

# DBTITLE 1,Carregar configurações
# Configuração inline (notebook de config externo não existe)
def inicializar_anos_processar():
    try:
        anos_str = dbutils.widgets.get("anos")
    except Exception:
        anos_str = "2025"
    return [int(ano.strip()) for ano in anos_str.split(",")]

# COMMAND ----------

# DBTITLE 1,Inicializar Anos a Processar
# Captura explícita do retorno para garantir que ANOS_PROCESSAR está definido
ANOS_PROCESSAR = inicializar_anos_processar()

if not ANOS_PROCESSAR:
    raise ValueError("❌ ANOS_PROCESSAR está vazio - verificar configuração")

print(f"✅ Anos a processar: {ANOS_PROCESSAR}")

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.functions import (
    col, lit, current_timestamp, trim, upper, lower, regexp_replace,
    to_date, make_date, coalesce, when, row_number
)
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,PARÂMETROS DE TABELAS
# df_params: Define catálogo, schemas e tabelas origem/destino
# Bronze → Silver: conformação técnica sem regras de negócio

CATALOGO = "workspace"
SCHEMA_BRONZE = "proj_aneel_cont_01_bronze"
SCHEMA_SILVER = "proj_aneel_cont_02_silver"
TABELA_ORIGEM = "indicadores_continuidade"
TABELA_DESTINO = "indicadores_continuidade"

TABELA_ORIGEM_FULL = f"{CATALOGO}.{SCHEMA_BRONZE}.{TABELA_ORIGEM}"
TABELA_DESTINO_FULL = f"{CATALOGO}.{SCHEMA_SILVER}.{TABELA_DESTINO}"

print(f"✅ Parâmetros carregados")
print(f"   Origem: {TABELA_ORIGEM_FULL}")
print(f"   Destino: {TABELA_DESTINO_FULL}")

# COMMAND ----------

# DBTITLE 1,CRIAR SCHEMA SILVER SE NÃO EXISTIR
# df_schema: Garante que o schema silver existe antes da gravação
# Validação de pré-requisito para evitar erro na escrita

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_SILVER}")
print(f"✅ Schema {SCHEMA_SILVER} verificado/criado")

# COMMAND ----------

# DBTITLE 1,LER DADOS BRONZE
# df_bronze_filtrado: Lê apenas os anos a processar da camada bronze
# Filtro por ano_referencia para otimizar leitura

anos_str = ",".join(map(str, ANOS_PROCESSAR))

try:
    df_bronze_filtrado = spark.sql(f"""
        SELECT *
        FROM {TABELA_ORIGEM_FULL}
        WHERE anoindice IN ({anos_str})
    """)
except Exception as e:
    print(f"⚠ Tabela bronze não encontrada: {e}")
    dbutils.notebook.exit("SKIPPED - bronze table not found")

total_bronze = df_bronze_filtrado.count()
print(f"✅ Registros lidos do bronze: {total_bronze:,}")

if total_bronze == 0:
    print(f"⚠ Nenhum registro encontrado no bronze para anos {ANOS_PROCESSAR}. Finalizando.")
    dbutils.notebook.exit("SKIPPED - no data in bronze")

# COMMAND ----------

# DBTITLE 1,PADRONIZAR TIPOS E NOMES DE COLUNAS
# df_tipado: Pivot de formato longo (sigindicador + vlrindiceenviado) para largo
# Dados ANEEL: cada linha = um indicador; pivot cria colunas DEC, FEC, etc.
from pyspark.sql.functions import first

df_pivoted = (
    df_bronze_filtrado
    .groupBy(
        "ideconjundconsumidoras", "dscconjundconsumidoras", "sigagente",
        "numcnpj", "anoindice", "numperiodoindice",
        "_fonte_url", "_ingest_ts", "_ingest_date", "_run_id"
    )
    .pivot("sigindicador")
    .agg(first("vlrindiceenviado"))
)

# Renomear colunas pivot para lowercase
for c in df_pivoted.columns:
    if c not in ["ideconjundconsumidoras", "dscconjundconsumidoras", "sigagente", "numcnpj", "anoindice", "numperiodoindice", "_fonte_url", "_ingest_ts", "_ingest_date", "_run_id"]:
        df_pivoted = df_pivoted.withColumnRenamed(c, c.lower())

df_tipado = (
    df_pivoted
    .withColumn("codigo_distribuidora", upper(trim(col("sigagente"))))
    .withColumn("distribuidora", trim(col("dscconjundconsumidoras")))
    .withColumn("ano_referencia", col("anoindice"))
    .withColumn("periodo_apuracao", make_date(col("anoindice"), lit(1), lit(1)))
    .withColumn("_timestamp_transformacao", current_timestamp())
)

print(f"✅ Pivot concluído: {df_tipado.count():,} registros, {len(df_tipado.columns)} colunas")
print(f"   Colunas: {df_tipado.columns}")

# COMMAND ----------

# DBTITLE 1,DEFINIR CHAVE COMPOSTA
# df_com_chave: Cria chave composta para identificação única
# Chave natural: conjunto de unidades consumidoras = (distribuidora + período)

df_com_chave = (
    df_tipado
    .withColumn(
        "chave_uc",
        coalesce(
            col("sigagente"),
            col("distribuidora")
        )
    )
)

print(f"✅ Chave composta criada (distribuidora + período)")

# COMMAND ----------

# DBTITLE 1,DEDUPLICAÇÃO (SE NECESSÁRIO)
# df_dedupe: Remove duplicatas mantendo a versão mais recente da ingestão
# Ordenação determinística: timestamp de ingestão descendente

w_dedupe = (
    Window
    .partitionBy("sigagente", "ideconjundconsumidoras", "anoindice", "numperiodoindice")
    .orderBy(col("_ingest_ts").desc())
)

df_dedupe = (
    df_com_chave
    .withColumn("_rn", row_number().over(w_dedupe))
    .filter(col("_rn") == 1)
    .drop("_rn")
)

total_apos_dedupe = df_dedupe.count()
duplicatas_removidas = total_bronze - total_apos_dedupe

print(f"✅ Deduplicação concluída")
print(f"   Registros após dedupe: {total_apos_dedupe:,}")
print(f"   Duplicatas removidas: {duplicatas_removidas:,}")

# COMMAND ----------

# DBTITLE 1,VALIDAR NULOS EM CAMPOS CRÍTICOS
# Guardrail: Validação de nulos em campos obrigatórios
# Chave natural e período de apuração não podem ser nulos

campos_obrigatorios = ["chave_uc", "periodo_apuracao"]

df_valido = df_dedupe
for campo in campos_obrigatorios:
    nulos = df_valido.filter(col(campo).isNull()).count()
    if nulos > 0:
        print(f"⚠️ ATENÇÃO: {nulos} registros com {campo} nulo serão removidos")
        df_valido = df_valido.filter(col(campo).isNotNull())

total_valido = df_valido.count()
registros_invalidos = total_apos_dedupe - total_valido

print(f"✅ Validação de nulos concluída")
print(f"   Registros válidos: {total_valido:,}")
print(f"   Registros inválidos removidos: {registros_invalidos:,}")

if total_valido == 0:
    raise ValueError("❌ Nenhum registro válido após validações")

# COMMAND ----------

# DBTITLE 1,VALIDAR UNICIDADE DA CHAVE
# Guardrail: Garantir unicidade da chave composta (distribuidora + período)
# Cada combinação deve aparecer uma única vez após deduplicação

duplicadas = (
    df_valido
    .groupBy("sigagente", "ideconjundconsumidoras", "anoindice", "numperiodoindice")
    .count()
    .filter(col("count") > 1)
    .count()
)

if duplicadas > 0:
    raise ValueError(
        f"❌ Falha na unicidade: {duplicadas} chaves duplicadas após dedupe. "
        "Verificar lógica de deduplicação."
    )

print(f"✅ Unicidade da chave validada (0 duplicatas)")

# COMMAND ----------

# DBTITLE 1,SELECIONAR COLUNAS FINAIS
# df_silver: Seleciona colunas finais para camada silver
# Incluir todas as colunas exceto metadados redundantes
colunas_excluir = ["_run_id", "_ingest_date", "ideconjundconsumidoras", "numcnpj", "numperiodoindice"]
colunas_selecionar = [c for c in df_valido.columns if c not in colunas_excluir]
df_silver = df_valido.select(*colunas_selecionar)

print(f"✅ Colunas finais selecionadas")
print(f"   Total de colunas: {len(df_silver.columns)}")
print(f"   Colunas: {df_silver.columns}")

# COMMAND ----------

# DBTITLE 1,GRAVAR EM SILVER (DELETE+APPEND POR ANO)
# Gravação: DROP + APPEND para idempotência
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_SILVER}")
spark.sql(f"DROP TABLE IF EXISTS {TABELA_DESTINO_FULL}")

# Inserir nova versão
df_silver.write.mode("append").saveAsTable(TABELA_DESTINO_FULL)
print(f"✅ {total_valido:,} registros gravados em silver")

# Validar gravação
total_pos_gravacao = spark.table(TABELA_DESTINO_FULL).count()
print(f"✅ Total de registros em silver: {total_pos_gravacao:,}")

# COMMAND ----------

# DBTITLE 1,SUMÁRIO DE EXECUÇÃO
# Sumário final da transformação silver
print("\n" + "="*60)
print("📊 SUMÁRIO DE EXECUÇÃO - SILVER INDICADORES CONTINUIDADE")
print("="*60)
print(f"✅ Anos processados: {ANOS_PROCESSAR}")
print(f"✅ Registros lidos do bronze: {total_bronze:,}")
print(f"✅ Duplicatas removidas: {duplicatas_removidas:,}")
print(f"✅ Registros inválidos removidos: {registros_invalidos:,}")
print(f"✅ Registros gravados em silver: {total_valido:,}")
print(f"✅ Total na tabela silver: {total_pos_gravacao:,}")
print(f"✅ Tabela destino: {TABELA_DESTINO_FULL}")
print("="*60)

# COMMAND ----------

