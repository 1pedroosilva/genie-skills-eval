# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestão de Indicadores Coletivos de Continuidade - ANEEL
# MAGIC
# MAGIC Este notebook realiza a ingestão dos indicadores coletivos de continuidade (DEC, FEC, DIC, FIC, DMIC, DICRI) publicados anualmente pela ANEEL no portal de dados abertos.
# MAGIC
# MAGIC **Fonte:** Arquivo anual publicado pela ANEEL (formato CSV/ZIP)
# MAGIC **Estratégia:** Carga completa a cada publicação (DELETE+APPEND por ano)
# MAGIC **Destino:** `proj_aneel_cont_01_bronze.indicadores_continuidade`
# MAGIC **Camada:** Bronze (raw com metadados de ingestão)

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
    col, lit, current_timestamp, current_date, input_file_name,
    to_date, trim, upper, regexp_replace, when
)
from pyspark.sql.types import StringType, IntegerType, DoubleType
from datetime import datetime
import requests
from io import BytesIO
from zipfile import ZipFile

# COMMAND ----------

# DBTITLE 1,PARÂMETROS DE FONTE
# df_params: Define URLs e metadados da fonte de indicadores coletivos ANEEL
# Dataset correto: indicadores-coletivos-de-continuidade-dec-e-fec
# Arquivo Parquet 2020-2029 (cobre todos os anos do parâmetro)

URL_PARQUET = "https://dadosabertos.aneel.gov.br/dataset/d5f0712e-62f6-4736-8dff-9991f10758a7/resource/d7f70fb1-725c-4748-afeb-65c6a78df550/download/indicadores-continuidade-coletivos-2020-2029.parquet"

# Catálogo e schema de destino
CATALOGO = "workspace"
SCHEMA_DESTINO = "proj_aneel_cont_01_bronze"
TABELA_DESTINO = "indicadores_continuidade"

print(f"✅ Parâmetros carregados")
print(f"   URL: {URL_PARQUET}")
print(f"   Destino: {CATALOGO}.{SCHEMA_DESTINO}.{TABELA_DESTINO}")

# COMMAND ----------

# DBTITLE 1,CRIAR SCHEMA SE NÃO EXISTIR
# df_schema: Garante que o schema bronze existe antes da gravação
# Validação de pré-requisito para evitar erro na escrita

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SCHEMA_DESTINO}")
print(f"✅ Schema {SCHEMA_DESTINO} verificado/criado")

# COMMAND ----------

# DBTITLE 1,BAIXAR E EXTRAIR ARQUIVO DA ANEEL
# Download do Parquet de indicadores para UC Volume (serverless-safe)
import os

# Garantir que o volume existe
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SCHEMA_DESTINO}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{SCHEMA_DESTINO}.raw_data")
VOLUME_PATH = f"/Volumes/{CATALOGO}/{SCHEMA_DESTINO}/raw_data"

print(f"📥 Baixando: {URL_PARQUET}")
try:
    r = requests.get(URL_PARQUET, timeout=300)
    r.raise_for_status()
    print(f"   Download: {len(r.content) / (1024*1024):.1f} MB")
    
    # Escrever no UC Volume
    parquet_path = f"{VOLUME_PATH}/indicadores.parquet"
    with open(parquet_path, "wb") as f:
        f.write(r.content)
    print(f"   Arquivo salvo em {parquet_path}")
    
    # Ler com Spark do volume
    df_raw = spark.read.parquet(parquet_path)
    
    # Filtrar pelos anos a processar (coluna AnoIndice)
    if "AnoIndice" in df_raw.columns:
        df_raw = df_raw.filter(col("AnoIndice").isin(ANOS_PROCESSAR))
    
    count = df_raw.count()
    print(f"✅ Spark DataFrame: {count:,} registros")
    print(f"   Colunas: {df_raw.columns}")
    
    del r
    
except Exception as e:
    print(f"❌ Erro no download: {e}")
    dbutils.notebook.exit("SKIPPED - download failed")

# COMMAND ----------

# DBTITLE 1,ADICIONAR METADADOS DE INGESTÃO
# Adicionar metadados de ingestão (bronze)
# O Parquet da ANEEL tem coluna AnoIndice para rastreabilidade
df_raw = (
    df_raw
    .withColumn("_fonte_url", lit(URL_PARQUET))
    .withColumn("_ingest_ts", lit(datetime.now()))
    .withColumn("_ingest_date", current_date())
    .withColumn("_run_id", lit(datetime.now().strftime("%Y%m%d%H%M%S")))
)

print(f"✅ Metadados adicionados: {df_raw.count():,} registros")

# COMMAND ----------

# DBTITLE 1,PADRONIZAR NOMES DE COLUNAS
# df_bronze: Padroniza nomes de colunas para snake_case
# Facilita consumo nas camadas downstream

import re

def padronizar_nome_coluna(nome: str) -> str:
    """
    Converte nome de coluna para snake_case.
    Remove acentos, espaços e caracteres especiais.
    """
    # Remover acentos
    nome = nome.lower()
    nome = (
        nome.replace('ã', 'a').replace('õ', 'o').replace('á', 'a')
        .replace('é', 'e').replace('í', 'i').replace('ó', 'o').replace('ú', 'u')
        .replace('â', 'a').replace('ê', 'e').replace('ô', 'o')
        .replace('à', 'a').replace('ç', 'c')
    )
    # Substituir espaços e caracteres especiais por underscore
    nome = re.sub(r'[^a-z0-9]+', '_', nome)
    # Remover underscores múltiplos e das pontas
    nome = re.sub(r'_+', '_', nome)
    nome = re.sub(r'^_|_$', '', nome)
    return nome

# Aplicar padronização
for coluna_antiga in df_raw.columns:
    if not coluna_antiga.startswith('_'):  # Preservar colunas de metadados
        coluna_nova = padronizar_nome_coluna(coluna_antiga)
        if coluna_antiga != coluna_nova:
            df_raw = df_raw.withColumnRenamed(coluna_antiga, coluna_nova)

df_bronze = df_raw

print(f"✅ Colunas padronizadas: {df_bronze.columns}")

# COMMAND ----------

# DBTITLE 1,VALIDAR SCHEMA E DADOS
# Guardrail: Validação de volume (schema validado no cell 12)
total_registros = df_bronze.count()
if total_registros == 0:
    raise ValueError("❌ DataFrame bronze está vazio - nenhum registro ingerido")

print(f"✅ Volume de dados validado: {total_registros} registros")
print(f"   Colunas: {df_bronze.columns}")

# COMMAND ----------

# DBTITLE 1,GRAVAR EM BRONZE
# Gravação: DROP + APPEND para idempotência
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SCHEMA_DESTINO}")
spark.sql(f"DROP TABLE IF EXISTS {CATALOGO}.{SCHEMA_DESTINO}.{TABELA_DESTINO}")

# Inserir nova versão
df_bronze.write.mode("append").saveAsTable(f"{CATALOGO}.{SCHEMA_DESTINO}.{TABELA_DESTINO}")
print(f"✅ {total_registros} registros gravados em {CATALOGO}.{SCHEMA_DESTINO}.{TABELA_DESTINO}")

# Validar gravação
total_pos_gravacao = spark.table(f"{CATALOGO}.{SCHEMA_DESTINO}.{TABELA_DESTINO}").count()
print(f"✅ Total de registros na tabela bronze: {total_pos_gravacao}")

# COMMAND ----------

# DBTITLE 1,VALIDAR SCHEMA E DADOS
# Guardrail: Validação de forma (bronze)
# Colunas esperadas após padronização snake_case
COLUNAS_ESPERADAS = [
    "anoindice",
    "sigagente",
    "sigindicador",
    "vlrindiceenviado",
]

colunas_faltantes = [col for col in COLUNAS_ESPERADAS if col not in df_bronze.columns]
if colunas_faltantes:
    print(f"⚠ Colunas faltantes no schema: {colunas_faltantes}")
else:
    print(f"✅ Schema validado: todas as colunas esperadas presentes")

total_registros = df_bronze.count()
if total_registros == 0:
    print("❌ DataFrame bronze está vazio")
    dbutils.notebook.exit("SKIPPED - no data")

print(f"✅ Volume de dados validado: {total_registros} registros")

df_bronze.groupBy("anoindice").count().orderBy("anoindice").show()

# COMMAND ----------

# DBTITLE 1,SUMÁRIO DE EXECUÇÃO
# Sumário final da execução
print("\n" + "="*60)
print("📊 SUMÁRIO DE EXECUÇÃO - INDICADORES COLETIVOS ANEEL")
print("="*60)
print(f"✅ Anos processados: {ANOS_PROCESSAR}")
print(f"✅ Registros ingeridos: {total_registros}")
print(f"✅ Tabela destino: {SCHEMA_DESTINO}.{TABELA_DESTINO}")
print(f"✅ Total na tabela bronze: {total_pos_gravacao}")
print("="*60)