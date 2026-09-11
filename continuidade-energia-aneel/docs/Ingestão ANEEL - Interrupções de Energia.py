# Databricks notebook source
# DBTITLE 1,Documentação
# MAGIC %md
# MAGIC # Ingestão de Dados - ANEEL Interrupções de Energia
# MAGIC
# MAGIC ## Objetivo
# MAGIC Ingestão dos dados de interrupções de energia elétrica da ANEEL para a camada **bronze** do projeto de continuidade de fornecimento.
# MAGIC
# MAGIC ## Fonte
# MAGIC - **Portal**: Dados Abertos ANEEL
# MAGIC - **Dataset**: Indicadores de Continuidade (DEC, FEC, DIC, FIC, DMIC)
# MAGIC - **Formato**: Parquet (arquivo anual, republicado a cada atualização)
# MAGIC - **URL**: https://dadosabertos.aneel.gov.br/dataset/indicadores-coletivos-de-continuidade
# MAGIC
# MAGIC ## Destino
# MAGIC - **Camada**: Bronze
# MAGIC - **Catálogo**: `main`
# MAGIC - **Schema**: `bronze_aneel`
# MAGIC - **Tabela**: `interrupcoes_energia`
# MAGIC
# MAGIC ## Estratégia de Ingestão
# MAGIC - Leitura direta do arquivo Parquet publicado
# MAGIC - Escrita completa (full load) na bronze
# MAGIC - Adição de metadados de ingestão (timestamp, arquivo fonte)
# MAGIC - Schema evolution habilitado

# COMMAND ----------

# DBTITLE 1,Configurações e Parâmetros
# Parâmetros de configuração
ANO_REFERENCIA = 2024  # Ano dos dados a serem ingeridos

# URLs conhecidas do portal ANEEL (atualizar conforme necessário)
# Nota: A ANEEL publica os dados em diferentes formatos. Ajuste a URL conforme o dataset específico
URL_BASE_ANEEL = "https://dadosabertos.aneel.gov.br/dataset/"

# Para este exemplo, vamos usar a URL direta do arquivo Parquet
# A URL exata pode variar - ajuste conforme a estrutura atual do portal
URL_PARQUET = f"https://dadosabertos.aneel.gov.br/dataset/b6ace465-a8a7-4b8b-ba0e-a8f4ae746db1/resource/indicadores-continuidade-{ANO_REFERENCIA}.parquet"

# Configuração de destino
CATALOG = "main"
SCHEMA_BRONZE = "bronze_aneel"
TABELA_BRONZE = "interrupcoes_energia"
TABELA_COMPLETA = f"{CATALOG}.{SCHEMA_BRONZE}.{TABELA_BRONZE}"

print(f"Configurações:")
print(f"  Fonte: {URL_PARQUET}")
print(f"  Destino: {TABELA_COMPLETA}")

# COMMAND ----------

# DBTITLE 1,Criar Schema Bronze (se não existir)
# MAGIC %sql
# MAGIC -- Criar o schema bronze se não existir
# MAGIC CREATE SCHEMA IF NOT EXISTS main.bronze_aneel
# MAGIC COMMENT 'Camada Bronze - Dados brutos da ANEEL'
# MAGIC LOCATION 'dbfs:/mnt/bronze/aneel'

# COMMAND ----------

# DBTITLE 1,Ingestão - Leitura e Escrita na Bronze
from pyspark.sql import functions as F
from datetime import datetime
import requests

# Adicionar metadados de ingestão
timestamp_ingestao = datetime.now().isoformat()

print(f"Iniciando ingestão dos dados da ANEEL...")
print(f"Timestamp: {timestamp_ingestao}")

try:
    # Tentativa 1: Leitura direta do Parquet via HTTP
    print(f"\nTentando ler arquivo Parquet de: {URL_PARQUET}")
    
    df_aneel = (
        spark.read
        .format("parquet")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(URL_PARQUET)
    )
    
    print(f"✓ Leitura bem-sucedida via URL direta")
    
except Exception as e:
    print(f"⚠ Falha na leitura direta: {str(e)}")
    print(f"\nTentando download e leitura local...")
    
    # Tentativa 2: Download para DBFS e leitura local
    import urllib.request
    
    temp_path = f"/tmp/aneel_interrupcoes_{ANO_REFERENCIA}.parquet"
    dbfs_path = f"dbfs:/tmp/aneel_interrupcoes_{ANO_REFERENCIA}.parquet"
    
    # Download
    urllib.request.urlretrieve(URL_PARQUET, f"/dbfs{temp_path}")
    print(f"✓ Download concluído: {dbfs_path}")
    
    # Leitura do arquivo local
    df_aneel = spark.read.parquet(dbfs_path)
    print(f"✓ Leitura bem-sucedida do arquivo local")

# Adicionar colunas de metadados
df_bronze = (
    df_aneel
    .withColumn("_data_ingestao", F.lit(timestamp_ingestao).cast("timestamp"))
    .withColumn("_arquivo_fonte", F.lit(URL_PARQUET))
    .withColumn("_ano_referencia", F.lit(ANO_REFERENCIA))
)

print(f"\nSchema dos dados:")
df_bronze.printSchema()

print(f"\nTotal de registros: {df_bronze.count():,}")

# Escrever na tabela bronze
print(f"\nEscrevendo dados na tabela bronze: {TABELA_COMPLETA}")

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")  # Full load
    .option("overwriteSchema", "true")  # Permitir evolução de schema
    .option("mergeSchema", "true")
    .saveAsTable(TABELA_COMPLETA)
)

print(f"✓ Ingestão concluída com sucesso!")

# COMMAND ----------

# DBTITLE 1,Validação e Qualidade dos Dados
# Validações pós-ingestão
print("=" * 80)
print("VALIDAÇÃO DOS DADOS INGERIDOS")
print("=" * 80)

# 1. Contagem de registros
total_registros = spark.table(TABELA_COMPLETA).count()
print(f"\n1. Total de registros na bronze: {total_registros:,}")

# 2. Verificar colunas
colunas = spark.table(TABELA_COMPLETA).columns
print(f"\n2. Colunas disponíveis ({len(colunas)}):")
for col in sorted(colunas):
    print(f"   - {col}")

# 3. Amostra dos dados
print(f"\n3. Amostra dos dados (5 registros):")
spark.table(TABELA_COMPLETA).limit(5).display()

# 4. Verificar metadados de ingestão
print(f"\n4. Metadados de ingestão:")
spark.sql(f"""
    SELECT 
        _ano_referencia,
        _arquivo_fonte,
        MIN(_data_ingestao) as primeira_ingestao,
        MAX(_data_ingestao) as ultima_ingestao,
        COUNT(*) as total_registros
    FROM {TABELA_COMPLETA}
    GROUP BY _ano_referencia, _arquivo_fonte
""").display()

# 5. Estatísticas básicas (se houver colunas numéricas esperadas)
print(f"\n5. Estatísticas por distribuidora (se aplicável):")
try:
    spark.sql(f"""
        SELECT 
            COUNT(*) as total_interrupcoes,
            COUNT(DISTINCT CASE WHEN TRIM(distribuidora) != '' THEN distribuidora END) as total_distribuidoras
        FROM {TABELA_COMPLETA}
    """).display()
except:
    print("   ⚠ Colunas esperadas não encontradas - verificar schema")

print(f"\n✓ Validação concluída")

# COMMAND ----------

# DBTITLE 1,Consulta de Exemplo - Exploração
# MAGIC %sql
# MAGIC -- Consulta de exemplo para explorar os dados
# MAGIC -- Ajuste conforme as colunas reais do dataset
# MAGIC
# MAGIC SELECT *
# MAGIC FROM main.bronze_aneel.interrupcoes_energia
# MAGIC LIMIT 10

# COMMAND ----------

# DBTITLE 1,Notas e Próximos Passos
# MAGIC %md
# MAGIC ## Notas Importantes
# MAGIC
# MAGIC ### URLs da ANEEL
# MAGIC A URL exata do arquivo Parquet pode variar conforme:
# MAGIC - Ano de referência
# MAGIC - Estrutura do portal de dados abertos
# MAGIC - Formato de publicação (a ANEEL às vezes publica em CSV, Parquet ou outros formatos)
# MAGIC
# MAGIC **Ação recomendada**: Verificar manualmente em https://dadosabertos.aneel.gov.br e ajustar a URL se necessário.
# MAGIC
# MAGIC ### Dados Disponíveis
# MAGIC O dataset de interrupções da ANEEL tipicamente contém:
# MAGIC - DEC (Duração Equivalente de Interrupção por Unidade Consumidora)
# MAGIC - FEC (Frequência Equivalente de Interrupção por Unidade Consumidora)
# MAGIC - DIC (Duração de Interrupção Individual)
# MAGIC - FIC (Frequência de Interrupção Individual)
# MAGIC - DMIC (Duração Máxima de Interrupção Contínua)
# MAGIC
# MAGIC Informações por:
# MAGIC - Distribuidora
# MAGIC - Município
# MAGIC - Conjunto (agrupamento de consumidores)
# MAGIC - Período de apuração
# MAGIC
# MAGIC ## Próximos Passos
# MAGIC
# MAGIC 1. **Validar URL**: Confirmar que a URL do Parquet está correta para o ano desejado
# MAGIC 2. **Camada Silver**: Criar transformações para limpar e enriquecer os dados
# MAGIC 3. **Camada Gold**: Criar agregações e métricas de negócio
# MAGIC 4. **Automação**: Agendar execução periódica para manter dados atualizados
# MAGIC 5. **Monitoramento**: Configurar alertas para falhas de ingestão
# MAGIC 6. **Documentação**: Atualizar catálogo com descrições de colunas e métricas