# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestão de Dados de Interrupções de Energia da ANEEL
# MAGIC
# MAGIC Notebook responsável pela ingestão incremental de dados de interrupções de fornecimento de energia elétrica publicados pela ANEEL no portal de dados abertos.
# MAGIC
# MAGIC **Fonte:** Portal de Dados Abertos da ANEEL - arquivo Parquet anual
# MAGIC
# MAGIC **Destino:** Tabela bronze no Unity Catalog
# MAGIC
# MAGIC **Estratégia:** APPEND com verificação prévia para garantir idempotência

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
import requests

# COMMAND ----------

# DBTITLE 1,CONFIGURAR PARÂMETROS
# Configuração do catálogo e schema de destino
CATALOG = "workspace"
SCHEMA_BRONZE = "continuidade_aneel_bronze"
TABLE_INTERRUPCOES = "aneel_interrupcoes"
TABLE_CONTROL = "controle_ingestao"

# URLs dos arquivos Parquet da ANEEL (portal CKAN de dados abertos)
URLS_PARQUET = {
    2025: "https://dadosabertos.aneel.gov.br/dataset/ccb25653-f07b-4f28-84c2-62a89d1f5a56/resource/691de320-cb3d-471b-b9ec-8c1b86af8c83/download/interrupcoes-energia-eletrica-2025.parquet",
}

CHECKPOINT_PATH = "/tmp/checkpoints/aneel_interrupcoes"

# COMMAND ----------

# DBTITLE 1,CRIAR SCHEMAS E TABELAS DE CONTROLE
# Criar schema bronze se não existir
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_BRONZE}")

# Criar tabela de controle de ingestão se não existir
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA_BRONZE}.{TABLE_CONTROL} (
    fonte STRING,
    ano INT,
    url STRING,
    last_modified STRING,
    status STRING,
    timestamp_ingestao TIMESTAMP,
    num_registros BIGINT,
    mensagem STRING
  )
  USING DELTA
""")

print(f"✓ Schema {CATALOG}.{SCHEMA_BRONZE} e tabela de controle prontos")

# COMMAND ----------

# DBTITLE 1,LISTAR ARQUIVOS DISPONÍVEIS NA ANEEL
# df_arquivos_disponiveis: lista de arquivos Parquet disponíveis no portal da ANEEL
# Identificar arquivos não processados comparando com tabela de controle

# Esta célula deve ser adaptada conforme a estrutura real do portal da ANEEL
# Exemplo simplificado: lista de anos
anos_disponiveis = list(URLS_PARQUET.keys())

# Buscar anos já processados
df_processados = spark.sql(f"""
  SELECT DISTINCT ano
  FROM {CATALOG}.{SCHEMA_BRONZE}.{TABLE_CONTROL}
  WHERE fonte = 'interrupcoes'
    AND status = 'SUCCESS'
""")

anos_processados = [row.ano for row in df_processados.collect()]
anos_a_processar = [ano for ano in anos_disponiveis if ano not in anos_processados]

print(f"Anos disponíveis: {anos_disponiveis}")
print(f"Anos já processados: {anos_processados}")
print(f"Anos a processar: {anos_a_processar}")

# COMMAND ----------

# DBTITLE 1,PROCESSAR CADA ANO (LOOP PRINCIPAL)
# Processar cada ano disponível com verificação de idempotência
# Serverless: baixar via requests para UC Volume, depois spark.read.parquet
import os

# Garantir que o volume existe
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_BRONZE}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA_BRONZE}.raw_data")
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA_BRONZE}/raw_data"

for ano in anos_a_processar:
    try:
        print(f"\n{'='*60}")
        print(f"Processando ano: {ano}")
        print(f"{'='*60}")
        
        url_arquivo = URLS_PARQUET.get(ano)
        if not url_arquivo:
            print(f"⚠ Sem URL mapeada para ano {ano} - pulando")
            continue
        
        # Obter metadados HTTP (Last-Modified)
        response = requests.head(url_arquivo, allow_redirects=True, timeout=30)
        last_modified = response.headers.get('Last-Modified', '')
        
        # Verificar se já foi processado com este Last-Modified
        ja_processado = spark.sql(f"""
            SELECT COUNT(*) as count
            FROM {CATALOG}.{SCHEMA_BRONZE}.{TABLE_CONTROL}
            WHERE fonte = 'interrupcoes'
              AND ano = {ano}
              AND last_modified = '{last_modified}'
              AND status = 'SUCCESS'
        """).collect()[0]['count']
        
        if ja_processado > 0:
            print(f"⏭ Ano {ano} já processado (Last-Modified: {last_modified}) - pulando")
            continue
        
        # Download para UC Volume (serverless-safe: sem /tmp/, sem createDataFrame)
        print(f"📥 Baixando: {url_arquivo}")
        r = requests.get(url_arquivo, timeout=300)
        r.raise_for_status()
        print(f"   Download: {len(r.content) / (1024*1024):.1f} MB")
        
        parquet_path = f"{VOLUME_PATH}/interrupcoes_{ano}.parquet"
        with open(parquet_path, "wb") as f:
            f.write(r.content)
        print(f"   Arquivo salvo em {parquet_path}")
        
        # Ler com Spark do volume (sem createDataFrame, sem estouro de memória)
        df_raw = spark.read.parquet(parquet_path)
        num_registros = df_raw.count()
        print(f"📊 Registros lidos: {num_registros:,}")
        
        # Adicionar metadados de ingestão (Bronze)
        df_bronze = (
            df_raw
            .withColumn("_ingest_timestamp", F.current_timestamp())
            .withColumn("_source_url", F.lit(url_arquivo))
            .withColumn("_source_last_modified", F.lit(last_modified))
            .withColumn("_ano_fonte", F.lit(ano))
        )
        
        # Gravar em Bronze (APPEND)
        print(f"💾 Gravando em {CATALOG}.{SCHEMA_BRONZE}.{TABLE_INTERRUPCOES}")
        df_bronze.write.mode("append").saveAsTable(
            f"{CATALOG}.{SCHEMA_BRONZE}.{TABLE_INTERRUPCOES}"
        )
        
        # Registrar sucesso no controle
        df_controle = spark.createDataFrame([
            ("interrupcoes", ano, url_arquivo, last_modified, "SUCCESS", 
             datetime.now(), num_registros, "Ingestão concluída com sucesso")
        ], ["fonte", "ano", "url", "last_modified", "status", 
            "timestamp_ingestao", "num_registros", "mensagem"])
        
        df_controle.write.mode("append").saveAsTable(
            f"{CATALOG}.{SCHEMA_BRONZE}.{TABLE_CONTROL}"
        )
        
        print(f"✅ Ano {ano} processado com sucesso")
        
        del r
        
    except Exception as e:
        print(f"❌ Erro ao processar ano {ano}: {str(e)}")
        
        # Registrar erro no controle
        df_controle_erro = spark.createDataFrame([
            ("interrupcoes", ano, url_arquivo if 'url_arquivo' in locals() else '', 
             '', "ERROR", datetime.now(), 0, str(e))
        ], ["fonte", "ano", "url", "last_modified", "status", 
            "timestamp_ingestao", "num_registros", "mensagem"])
        
        df_controle_erro.write.mode("append").saveAsTable(
            f"{CATALOG}.{SCHEMA_BRONZE}.{TABLE_CONTROL}"
        )
        
        continue

print(f"\n{'='*60}")
print("Ingestão concluída")
print(f"{'='*60}")

# COMMAND ----------

# DBTITLE 1,VALIDAR INGESTÃO
# Exibir resumo da ingestão
df_resumo = spark.sql(f"""
  SELECT 
    fonte,
    COUNT(*) as total_ingestoes,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as sucessos,
    SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) as erros,
    SUM(num_registros) as total_registros,
    MAX(timestamp_ingestao) as ultima_ingestao
  FROM {CATALOG}.{SCHEMA_BRONZE}.{TABLE_CONTROL}
  WHERE fonte = 'interrupcoes'
  GROUP BY fonte
""")

print("\n📊 Resumo da Ingestão:")
display(df_resumo)

# Exibir amostra dos dados ingeridos
print(f"\n🔍 Amostra de dados em {CATALOG}.{SCHEMA_BRONZE}.{TABLE_INTERRUPCOES}:")
try:
    df_sample = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.{TABLE_INTERRUPCOES}").limit(10)
    display(df_sample)
except Exception as e:
    print(f"⚠ Tabela {CATALOG}.{SCHEMA_BRONZE}.{TABLE_INTERRUPCOES} não possui dados ou não existe: {e}")

# COMMAND ----------

