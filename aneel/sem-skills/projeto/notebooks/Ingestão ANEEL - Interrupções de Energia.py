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
# MAGIC - **Catálogo**: `workspace`
# MAGIC - **Schema**: `proj_aneel_cont_01_bronze`
# MAGIC - **Tabela**: `101_interrupcoes`
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

# URL correta do dataset de Interrupções de Energia Elétrica da ANEEL
# Dataset: Interrupções de Energia Elétrica nas Redes de Distribuição
# Dataset ID: ccb25653-f07b-4f28-84c2-62a89d1f5a56
# Resource ID para 2024: fc5ca52c-329c-4443-a2d6-08ccec711ade
URL_PARQUET = "https://dadosabertos.aneel.gov.br/dataset/ccb25653-f07b-4f28-84c2-62a89d1f5a56/resource/fc5ca52c-329c-4443-a2d6-08ccec711ade/download/interrupcoes-energia-eletrica-2024.parquet"

# Configuração de destino
CATALOG = "workspace"
SCHEMA_BRONZE = "proj_aneel_cont_01_bronze"
TABELA_BRONZE = "101_interrupcoes"
TABELA_COMPLETA = f"{CATALOG}.{SCHEMA_BRONZE}.{TABELA_BRONZE}"

print(f"Configurações:")
print(f"  Fonte: {URL_PARQUET}")
print(f"  Destino: {TABELA_COMPLETA}")

# COMMAND ----------

# DBTITLE 1,Criar Schema Bronze (se não existir)
# MAGIC %sql
# MAGIC -- Criar o schema bronze se não existir
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.proj_aneel_cont_01_bronze
# MAGIC COMMENT 'Camada Bronze - Dados brutos da ANEEL'

# COMMAND ----------

# DBTITLE 1,Criar Volume de Staging
# MAGIC %sql
# MAGIC -- Criar volume para staging de arquivos temporários
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.proj_aneel_cont_01_bronze.staging_files
# MAGIC COMMENT 'Volume temporário para staging de arquivos externos durante ingestão'

# COMMAND ----------

# DBTITLE 1,Ingestão - Download e Carga
from pyspark.sql import functions as F
from datetime import datetime
import requests
import os

timestamp_ingestao = datetime.now().isoformat()

print("═" * 70)
print("INGESTÃO BRONZE - INTERRUPÇÕES")
print("═" * 70)
print(f"Timestamp: {timestamp_ingestao}\n")

# Verificar se tabela já existe
print("[1/6] Verificando tabela...")
if spark.catalog.tableExists(TABELA_COMPLETA):
    existing_count = spark.table(TABELA_COMPLETA).count()
    print(f"       ✓ Tabela já existe: {existing_count:,} registros")
    dbutils.notebook.exit("SUCCESS: Tabela já existe")

print("       ✓ Tabela não existe - iniciando\n")

VOLUME_PATH = "/Volumes/workspace/proj_aneel_cont_01_bronze/staging_files"
STAGING_FILE = f"{VOLUME_PATH}/aneel_interrupcoes_{ANO_REFERENCIA}.parquet"

try:
    print("[2/6] Download HTTP...")
    response = requests.get(URL_PARQUET, timeout=300)
    response.raise_for_status()
    file_size_mb = len(response.content) / (1024 * 1024)
    print(f"       ✓ Download: {file_size_mb:.2f} MB\n")
    
    print("[3/6] Gravando no volume...")
    with open(STAGING_FILE, 'wb') as f:
        f.write(response.content)
    print("       ✓ Arquivo no volume\n")
    
    print("[4/6] Lendo com Spark...")
    df_aneel = spark.read.parquet(STAGING_FILE)
    total_registros = df_aneel.count()
    print(f"       ✓ Leitura: {total_registros:,} registros\n")
    
    print("[5/6] Adicionando metadados...")
    df_bronze = (
        df_aneel
        .withColumn("_data_ingestao", F.lit(timestamp_ingestao).cast("timestamp"))
        .withColumn("_arquivo_fonte", F.lit(URL_PARQUET))
        .withColumn("_ano_referencia", F.lit(ANO_REFERENCIA))
    )
    print("       ✓ Metadados OK\n")
    
    print(f"[6/6] Gravando {TABELA_COMPLETA}...")
    df_bronze.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABELA_COMPLETA)
    print(f"       ✓ Sucesso: {total_registros:,} registros")
    
except Exception as e:
    print(f"\n❌ Erro: {str(e)}")
    raise
finally:
    try:
        if os.path.exists(STAGING_FILE):
            os.remove(STAGING_FILE)
            print("\n✓ Arquivo temp removido")
    except:
        pass

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