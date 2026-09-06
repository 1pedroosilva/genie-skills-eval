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
# MAGIC %run ../05_apoio/config_parametros

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
# URL base do portal de dados abertos da ANEEL para indicadores de continuidade

URL_BASE_ANEEL = "https://dadosabertos.aneel.gov.br/dataset/"
DATASET_ID = "indicadores-coletivos-de-continuidade"

# Padrão de nomenclatura dos arquivos (ajustar conforme estrutura real da ANEEL)
# Exemplo: indicadores-continuidade-2024.zip
PADRAO_ARQUIVO = "indicadores-continuidade-{ano}.zip"

# Catálogo e schema de destino
CATALOGO_DESTINO = "proj_aneel_cont"
SCHEMA_DESTINO = f"{CATALOGO_DESTINO}_01_bronze"
TABELA_DESTINO = "indicadores_continuidade"

print(f"✅ Parâmetros carregados")
print(f"   Dataset: {DATASET_ID}")
print(f"   Destino: {SCHEMA_DESTINO}.{TABELA_DESTINO}")

# COMMAND ----------

# DBTITLE 1,CRIAR SCHEMA SE NÃO EXISTIR
# df_schema: Garante que o schema bronze existe antes da gravação
# Validação de pré-requisito para evitar erro na escrita

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_DESTINO}")
print(f"✅ Schema {SCHEMA_DESTINO} verificado/criado")

# COMMAND ----------

# DBTITLE 1,BAIXAR E EXTRAIR ARQUIVO DA ANEEL
# df_arquivos_baixados: Baixa arquivo ZIP da ANEEL e extrai CSV para cada ano
# Retorna lista de tuplas (ano, conteúdo_csv, url_fonte, timestamp_download)

def baixar_arquivo_aneel(ano: int) -> tuple:
    """
    Baixa arquivo de indicadores da ANEEL para o ano especificado.
    
    Returns:
        tuple: (ano, conteúdo_csv, url_fonte, timestamp_download)
    """
    url_arquivo = f"{URL_BASE_ANEEL}{DATASET_ID}/resource/" + PADRAO_ARQUIVO.format(ano=ano)
    
    print(f"   Baixando {ano}: {url_arquivo}")
    
    try:
        response = requests.get(url_arquivo, timeout=60)
        response.raise_for_status()
        
        # Extrair CSV do ZIP
        with ZipFile(BytesIO(response.content)) as zip_file:
            # Assume que há um único CSV dentro do ZIP
            csv_filename = [f for f in zip_file.namelist() if f.endswith('.csv')][0]
            csv_content = zip_file.read(csv_filename).decode('utf-8')
        
        timestamp_download = datetime.now()
        print(f"   ✅ {ano} baixado com sucesso ({len(csv_content)} bytes)")
        
        return (ano, csv_content, url_arquivo, timestamp_download)
    
    except Exception as e:
        print(f"   ❌ Erro ao baixar {ano}: {str(e)}")
        raise

# Baixar arquivos para todos os anos
arquivos_baixados = []
for ano in ANOS_PROCESSAR:
    print(f"\n📥 Processando ano {ano}...")
    arquivo = baixar_arquivo_aneel(ano)
    arquivos_baixados.append(arquivo)

print(f"\n✅ Total de arquivos baixados: {len(arquivos_baixados)}")

# COMMAND ----------

# DBTITLE 1,CRIAR DATAFRAMES RAW POR ANO
# df_raw_por_ano: Converte CSV em DataFrame Spark para cada ano
# Preserva esquema original da fonte (bronze raw)

dfs_por_ano = []

for ano, csv_content, url_fonte, timestamp_download in arquivos_baixados:
    # Criar RDD a partir do conteúdo CSV
    rdd = spark.sparkContext.parallelize([csv_content])
    
    # Ler CSV com inferência de schema
    df_temp = spark.read.csv(
        rdd,
        header=True,
        inferSchema=True,
        sep=";",  # Ajustar separador conforme padrão ANEEL
        encoding="UTF-8"
    )
    
    # Adicionar metadados de ingestão (bronze)
    df_com_metadados = (
        df_temp
        .withColumn("ano_referencia", lit(ano))
        .withColumn("_fonte_url", lit(url_fonte))
        .withColumn("_ingest_ts", lit(timestamp_download))
        .withColumn("_ingest_date", current_date())
        .withColumn("_run_id", lit(spark.sparkContext.applicationId))
    )
    
    dfs_por_ano.append(df_com_metadados)
    print(f"✅ DataFrame criado para {ano}: {df_com_metadados.count()} registros")

# Unificar todos os anos em um único DataFrame
df_raw = dfs_por_ano[0]
for df in dfs_por_ano[1:]:
    df_raw = df_raw.unionByName(df, allowMissingColumns=True)

print(f"\n✅ DataFrame unificado: {df_raw.count()} registros totais")

# COMMAND ----------

# DBTITLE 1,PADRONIZAR NOMES DE COLUNAS
# df_bronze: Padroniza nomes de colunas para snake_case
# Facilita consumo nas camadas downstream

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
    nome = regexp_replace(nome, r'[^a-z0-9]+', '_')
    # Remover underscores múltiplos e das pontas
    nome = regexp_replace(nome, r'_+', '_')
    nome = regexp_replace(nome, r'^_|_$', '')
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
# Guardrail: Validação de forma (bronze)
# Verifica se colunas essenciais existem e se há dados

# Colunas esperadas (ajustar conforme estrutura real dos indicadores ANEEL)
COLUNAS_ESPERADAS = [
    "ano_referencia",
    "distribuidora",  # ou código da distribuidora
    "dec",  # Duração Equivalente de Interrupção por Unidade Consumidora
    "fec",  # Frequência Equivalente de Interrupção por Unidade Consumidora
]

# Validar existência de colunas
colunas_faltantes = [col for col in COLUNAS_ESPERADAS if col not in df_bronze.columns]
if colunas_faltantes:
    raise ValueError(f"❌ Colunas faltantes no schema: {colunas_faltantes}")

print(f"✅ Schema validado: todas as colunas esperadas presentes")

# Validar volume de dados
total_registros = df_bronze.count()
if total_registros == 0:
    raise ValueError("❌ DataFrame bronze está vazio - nenhum registro ingerido")

print(f"✅ Volume de dados validado: {total_registros} registros")

# Validar distribuição por ano
df_bronze.groupBy("ano_referencia").count().orderBy("ano_referencia").show()
print(f"✅ Distribuição por ano validada")

# COMMAND ----------

# DBTITLE 1,GRAVAR EM BRONZE (DELETE+APPEND POR ANO)
# Gravação: DELETE WHERE + APPEND para idempotência
# Remove anos reprocessados antes de inserir nova versão

# 1. Deletar anos sendo reprocessados
anos_str = ",".join(map(str, ANOS_PROCESSAR))
spark.sql(f"""
    DELETE FROM {SCHEMA_DESTINO}.{TABELA_DESTINO}
    WHERE ano_referencia IN ({anos_str})
""")
print(f"✅ Registros anteriores deletados para anos: {ANOS_PROCESSAR}")

# 2. Inserir nova versão
df_bronze.write.mode("append").saveAsTable(f"{SCHEMA_DESTINO}.{TABELA_DESTINO}")
print(f"✅ {total_registros} registros gravados em {SCHEMA_DESTINO}.{TABELA_DESTINO}")

# 3. Validar gravação
total_pos_gravacao = spark.table(f"{SCHEMA_DESTINO}.{TABELA_DESTINO}").count()
print(f"✅ Total de registros na tabela bronze: {total_pos_gravacao}")

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