# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Documentação
# MAGIC %md
# MAGIC # Silver — Transformação de Interrupções de Energia
# MAGIC
# MAGIC ## Objetivo
# MAGIC Transformar os dados brutos de interrupções de energia da camada **bronze** para a camada **silver**, aplicando limpeza, padronização e enriquecimento. O resultado é uma tabela pronta para análise por **distribuidora** e por **conjunto de unidades consumidoras**.
# MAGIC
# MAGIC ## Origem
# MAGIC - **Camada**: Bronze
# MAGIC - **Tabela**: `workspace.proj_aneel_cont_01_bronze.101_interrupcoes`
# MAGIC - **Registros**: ~500.000 (ano 2024)
# MAGIC - **Schema original**: PascalCase (preservado da fonte ANEEL)
# MAGIC
# MAGIC ## Destino
# MAGIC - **Camada**: Silver
# MAGIC - **Tabela**: `workspace.proj_aneel_cont_01_silver.interrupcoes`
# MAGIC - **Estratégia**: Full load (overwrite)
# MAGIC
# MAGIC ## Transformações aplicadas
# MAGIC 1. **Normalização de nomes**: PascalCase → snake_case (padrão brasileiro)
# MAGIC 2. **Limpeza de strings**: trim de espaços em todas as colunas texto
# MAGIC 3. **Cálculo de duração**: duração em minutos e horas (fim - início)
# MAGIC 4. **Decomposição do fato gerador**: separação dos níveis hierárquicos (origem, natureza, categoria, detalhe)
# MAGIC 5. **Formatação de CNPJ**: zeros à esquerda para 14 dígitos
# MAGIC 6. **Conversão de tensão**: volts → kV
# MAGIC 7. **Derivação temporal**: mês, data e dia da semana
# MAGIC 8. **Filtros de qualidade**: remoção de registros com duração ≤ 0, unidades consumidoras = 0 e tensão = 0
# MAGIC 9. **Metadados silver**: timestamp de processamento
# MAGIC
# MAGIC ## Análise pronta para consumo
# MAGIC A tabela silver permite análises diretas por:
# MAGIC - **Distribuidora** (`sig_agente`, `nom_agente_regulado`)
# MAGIC - **Conjunto de unidades consumidoras** (`ide_conjunto_unidade_consumidora`, `dsc_conjunto_unidade_consumidora`)
# MAGIC - **Período** (`num_ano`, `mes_interrupcao`, `data_interrupcao`)
# MAGIC - **Tipo de interrupção** (`dsc_tipo_interrupcao`)
# MAGIC - **Causa** (`fato_gerador_origem`, `fato_gerador_natureza`, `fato_gerador_categoria`, `fato_gerador_detalhe`)

# COMMAND ----------

# DBTITLE 1,Configurações e Parâmetros
# ============================================================================
# CONFIGURAÇÕES E PARÂMETROS
# ============================================================================

from datetime import datetime, timezone

# --- Origem (camada Bronze) ---
CATALOG_BRONZE = "workspace"
SCHEMA_BRONZE = "proj_aneel_cont_01_bronze"
TABELA_BRONZE = "101_interrupcoes"
TABELA_ORIGEM = f"{CATALOG_BRONZE}.{SCHEMA_BRONZE}.{TABELA_BRONZE}"

# --- Destino (camada Silver) ---
CATALOG_SILVER = "workspace"
SCHEMA_SILVER = "proj_aneel_cont_01_silver"
TABELA_SILVER = "interrupcoes"
TABELA_DESTINO = f"{CATALOG_SILVER}.{SCHEMA_SILVER}.{TABELA_SILVER}"

# --- Metadados de processamento ---
SILVER_TS = datetime.now(timezone.utc).isoformat()
SILVER_RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

print("=" * 80)
print("SILVER — TRANSFORMAÇÃO DE INTERRUPÇÕES DE ENERGIA")
print("=" * 80)
print(f"  Origem      : {TABELA_ORIGEM}")
print(f"  Destino     : {TABELA_DESTINO}")
print(f"  Processado  : {SILVER_TS}")
print(f"  Run ID      : {SILVER_RUN_ID}")
print(f"  Estratégia  : Full load (overwrite)")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Criar Schema Silver (se não existir)
# MAGIC %sql
# MAGIC -- Garantir que o schema silver existe
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.proj_aneel_cont_01_silver
# MAGIC COMMENT 'Camada Silver - Dados tratados do projeto de Continuidade de Fornecimento ANEEL'

# COMMAND ----------

# DBTITLE 1,Transformação — Leitura, Limpeza e Padronização
# ============================================================================
# TRANSFORMAÇÃO: LEITURA DA BRONZE E LIMPEZA/PADRONIZAÇÃO
# ============================================================================

from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, IntegerType, DoubleType, DateType, TimestampType

print("\n[1/4] Lendo dados da camada bronze...")
df_bronze = spark.table(TABELA_ORIGEM)
total_bronze = df_bronze.count()
print(f"       ✓ {total_bronze:,} registros lidos")

# ------------------------------------------------------------------
# 1. NORMALIZAÇÃO DE NOMES DE COLUNAS (PascalCase → snake_case)
# ------------------------------------------------------------------
# Mapeamento manual para garantir nomes em português claros
rename_map = {
    "DatGeracaoConjuntoDados":          "dat_geracao_conjunto_dados",
    "IdeConjuntoUnidadeConsumidora":    "ide_conjunto_unidade_consumidora",
    "DscConjuntoUnidadeConsumidora":    "dsc_conjunto_unidade_consumidora",
    "DscAlimentadorSubestacao":         "dsc_alimentador_subestacao",
    "DscSubestacaoDistribuicao":        "dsc_subestacao_distribuicao",
    "NumOrdemInterrupcao":              "num_ordem_interrupcao",
    "DscTipoInterrupcao":               "dsc_tipo_interrupcao",
    "IdeMotivoInterrupcao":             "ide_motivo_interrupcao",
    "DatInicioInterrupcao":             "dat_inicio_interrupcao",
    "DatFimInterrupcao":                "dat_fim_interrupcao",
    "DscFatoGeradorInterrupcao":        "dsc_fato_gerador_interrupcao",
    "NumNivelTensao":                   "num_nivel_tensao",
    "NumUnidadeConsumidora":            "num_unidade_consumidora",
    "NumConsumidorConjunto":            "num_consumidor_conjunto",
    "NumAno":                           "num_ano",
    "NomAgenteRegulado":               "nom_agente_regulado",
    "SigAgente":                        "sig_agente",
    "NumCPFCNPJ":                       "num_cnpj",
}

for old_name, new_name in rename_map.items():
    df_bronze = df_bronze.withColumnRenamed(old_name, new_name)

print(f"       ✓ Colunas renomeadas para snake_case ({len(rename_map)} colunas)")

# ------------------------------------------------------------------
# 2. LIMPEZA DE STRINGS (trim de espaços)
# ------------------------------------------------------------------
string_cols = ["dsc_conjunto_unidade_consumidora", "dsc_alimentador_subestacao",
               "dsc_subestacao_distribuicao", "num_ordem_interrupcao",
               "dsc_tipo_interrupcao", "dsc_fato_gerador_interrupcao",
               "nom_agente_regulado", "sig_agente"]

for c in string_cols:
    df_bronze = df_bronze.withColumn(c, F.trim(F.col(c)))

print(f"       ✓ Strings limpas (trim em {len(string_cols)} colunas)")

# ------------------------------------------------------------------
# 3. CÁLCULO DE DURAÇÃO
# ------------------------------------------------------------------
df_bronze = (
    df_bronze
    .withColumn("duracao_segundos",
        F.unix_timestamp("dat_fim_interrupcao") - F.unix_timestamp("dat_inicio_interrupcao"))
    .withColumn("duracao_minutos", F.round(F.col("duracao_segundos") / 60.0, 2))
    .withColumn("duracao_horas", F.round(F.col("duracao_segundos") / 3600.0, 4))
)

print(f"       ✓ Duração calculada (segundos, minutos, horas)")

# ------------------------------------------------------------------
# 4. DECOMPOSIÇÃO DO FATO GERADOR
# ------------------------------------------------------------------
# O campo dsc_fato_gerador_interrupcao contém níveis hierárquicos separados
# por ';' ou '-'. Ex: "INTERNA;NAO PROGRAMADA;PROPRIAS DO SISTEMA;FALHA..."
# Normalizamos o separador para ';' e dividimos em 4 níveis.

fato_normalizado = F.regexp_replace(F.col("dsc_fato_gerador_interrupcao"), r"[-]", ";")

# Split em até 4 partes (acesso seguro — retorna NULL se o índice não existir)
partes = F.split(fato_normalizado, ";")

df_bronze = (
    df_bronze
    .withColumn("fato_gerador_origem",    F.when(F.size(partes) >= 1, F.trim(F.element_at(partes, 1))))
    .withColumn("fato_gerador_natureza",  F.when(F.size(partes) >= 2, F.trim(F.element_at(partes, 2))))
    .withColumn("fato_gerador_categoria", F.when(F.size(partes) >= 3, F.trim(F.element_at(partes, 3))))
    .withColumn("fato_gerador_detalhe",   F.when(F.size(partes) >= 4, F.trim(F.element_at(partes, 4))))
)

print(f"       ✓ Fato gerador decomposto em 4 níveis (origem, natureza, categoria, detalhe)")

# ------------------------------------------------------------------
# 5. FORMATAÇÃO DE CNPJ (zeros à esquerda para 14 dígitos)
# ------------------------------------------------------------------
df_bronze = (
    df_bronze
    .withColumn("cnpj_formatado",
        F.lpad(F.col("num_cnpj").cast("string"), 14, "0"))
)

print(f"       ✓ CNPJ formatado com 14 dígitos")

# ------------------------------------------------------------------
# 6. CONVERSÃO DE NÍVEL DE TENSÃO (volts → kV)
# ------------------------------------------------------------------
df_bronze = (
    df_bronze
    .withColumn("nivel_tensao_kv", F.round(F.col("num_nivel_tensao") / 1000.0, 3))
)

print(f"       ✓ Nível de tensão convertido para kV")

# ------------------------------------------------------------------
# 7. DERIVAÇÃO TEMPORAL
# ------------------------------------------------------------------
df_bronze = (
    df_bronze
    .withColumn("data_interrupcao", F.to_date("dat_inicio_interrupcao"))
    .withColumn("mes_interrupcao", F.month("dat_inicio_interrupcao"))
    .withColumn("dia_semana_interrupcao", F.dayofweek("dat_inicio_interrupcao"))
    .withColumn("hora_inicio_interrupcao", F.hour("dat_inicio_interrupcao"))
)

print(f"       ✓ Campos temporais derivados (data, mês, dia da semana, hora)")

# ------------------------------------------------------------------
# 8. FILTROS DE QUALIDADE
# ------------------------------------------------------------------
# Remover registros inválidos:
#   - Duração <= 0 (data fim <= data início)
#   - Unidades consumidoras afetadas = 0
#   - Nível de tensão = 0

registros_antes = df_bronze.count()

df_silver = (
    df_bronze
    .filter(F.col("duracao_segundos") > 0)
    .filter(F.col("num_unidade_consumidora") > 0)
    .filter(F.col("num_nivel_tensao") > 0)
)

registros_depois = df_silver.count()
registros_removidos = registros_antes - registros_depois

print(f"\n[2/4] Filtros de qualidade aplicados:")
print(f"       Registros antes    : {registros_antes:,}")
print(f"       Registros removidos: {registros_removidos:,} ({registros_removidos/registros_antes*100:.2f}%)")
print(f"       Registros válidos  : {registros_depois:,}")

# ------------------------------------------------------------------
# 9. ADIÇÃO DE METADADOS SILVER
# ------------------------------------------------------------------
df_silver = (
    df_silver
    .withColumn("_silver_ts", F.lit(SILVER_TS).cast("timestamp"))
    .withColumn("_silver_run_id", F.lit(SILVER_RUN_ID))
    .withColumn("_origem_tabela", F.lit(TABELA_ORIGEM))
)

print(f"       ✓ Metadados silver adicionados")

# ------------------------------------------------------------------
# 10. ORDENAÇÃO DE COLUNAS (lógica: identificadores → dimensões → fatos → metadados)
# ------------------------------------------------------------------
colunas_ordenadas = [
    # Identificadores
    "num_ordem_interrupcao",
    "ide_conjunto_unidade_consumidora",
    "dsc_conjunto_unidade_consumidora",
    # Distribuidora
    "sig_agente",
    "nom_agente_regulado",
    "num_cnpj",
    "cnpj_formatado",
    # Localização elétrica
    "dsc_subestacao_distribuicao",
    "dsc_alimentador_subestacao",
    # Características da interrupção
    "dsc_tipo_interrupcao",
    "ide_motivo_interrupcao",
    "dsc_fato_gerador_interrupcao",
    "fato_gerador_origem",
    "fato_gerador_natureza",
    "fato_gerador_categoria",
    "fato_gerador_detalhe",
    # Datas e duração
    "dat_inicio_interrupcao",
    "dat_fim_interrupcao",
    "data_interrupcao",
    "mes_interrupcao",
    "dia_semana_interrupcao",
    "hora_inicio_interrupcao",
    "duracao_segundos",
    "duracao_minutos",
    "duracao_horas",
    # Tensão
    "num_nivel_tensao",
    "nivel_tensao_kv",
    # Unidades consumidoras
    "num_unidade_consumidora",
    "num_consumidor_conjunto",
    # Período
    "num_ano",
    # Geração do conjunto
    "dat_geracao_conjunto_dados",
    # Metadados silver
    "_silver_ts",
    "_silver_run_id",
    "_origem_tabela",
]

df_silver = df_silver.select(*colunas_ordenadas)

print(f"\n[3/4] Schema final ({len(df_silver.columns)} colunas):")
df_silver.printSchema()

# COMMAND ----------

# DBTITLE 1,Escrita na Camada Silver
# ============================================================================
# ESCRITA NA CAMADA SILVER
# ============================================================================

print("\n[4/4] Escrevendo dados na camada silver...")
print(f"       Destino: {TABELA_DESTINO}")

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABELA_DESTINO)
)

print(f"       ✓ Escrita concluída com sucesso!")
print(f"       ✓ {registros_depois:,} registros gravados")

# COMMAND ----------

# DBTITLE 1,Validação e Qualidade dos Dados Silver
# ============================================================================
# VALIDAÇÃO PÓS-TRANSFORMAÇÃO
# ============================================================================

print("=" * 80)
print("VALIDAÇÃO DOS DADOS SILVER")
print("=" * 80)

# 1. Contagem total
total_silver = spark.table(TABELA_DESTINO).count()
print(f"\n1. Total de registros na silver: {total_silver:,}")

# 2. Cobertura temporal
print(f"\n2. Cobertura temporal (por mês):")
spark.sql(f"""
    SELECT 
        num_ano,
        mes_interrupcao,
        COUNT(*) as total_interrupcoes,
        ROUND(SUM(duracao_horas), 2) as horas_totais_interrupcao,
        SUM(num_unidade_consumidora) as unidades_afetadas
    FROM {TABELA_DESTINO}
    GROUP BY num_ano, mes_interrupcao
    ORDER BY num_ano, mes_interrupcao
""").display()

# 3. Distribuição por distribuidora
print(f"\n3. Distribuição por distribuidora:")
spark.sql(f"""
    SELECT 
        sig_agente,
        nom_agente_regulado,
        COUNT(*) as total_interrupcoes,
        COUNT(DISTINCT ide_conjunto_unidade_consumidora) as conjuntos_afetados,
        ROUND(AVG(duracao_minutos), 2) as duracao_media_min,
        ROUND(MAX(duracao_horas), 2) as duracao_max_horas,
        SUM(num_unidade_consumidora) as total_unidades_afetadas
    FROM {TABELA_DESTINO}
    GROUP BY sig_agente, nom_agente_regulado
    ORDER BY total_interrupcoes DESC
""").display()

# 4. Validação de qualidade - sem registros inválidos
print(f"\n4. Verificação de qualidade (deve ser zero):")
spark.sql(f"""
    SELECT 
        SUM(CASE WHEN duracao_segundos <= 0 THEN 1 ELSE 0 END) as duracao_invalida,
        SUM(CASE WHEN num_unidade_consumidora <= 0 THEN 1 ELSE 0 END) as unidades_invalidas,
        SUM(CASE WHEN num_nivel_tensao = 0 THEN 1 ELSE 0 END) as tensao_invalida,
        SUM(CASE WHEN sig_agente IS NULL OR TRIM(sig_agente) = '' THEN 1 ELSE 0 END) as agente_nulo
    FROM {TABELA_DESTINO}
""").display()

# 5. Distribuição por tipo de interrupção
print(f"\n5. Distribuição por tipo de interrupção:")
spark.sql(f"""
    SELECT 
        dsc_tipo_interrupcao,
        COUNT(*) as total,
        ROUND(COUNT(*) * 100.0 / {total_silver}, 2) as percentual
    FROM {TABELA_DESTINO}
    GROUP BY dsc_tipo_interrupcao
    ORDER BY total DESC
""").display()

# 6. Top 10 conjuntos com mais interrupções
print(f"\n6. Top 10 conjuntos de unidades consumidoras com mais interrupções:")
spark.sql(f"""
    SELECT 
        sig_agente,
        ide_conjunto_unidade_consumidora,
        dsc_conjunto_unidade_consumidora,
        COUNT(*) as total_interrupcoes,
        ROUND(SUM(duracao_horas), 2) as horas_totais,
        ROUND(AVG(duracao_minutos), 2) as duracao_media_min,
        MAX(num_consumidor_conjunto) as consumidores_conjunto
    FROM {TABELA_DESTINO}
    GROUP BY sig_agente, ide_conjunto_unidade_consumidora, dsc_conjunto_unidade_consumidora
    ORDER BY total_interrupcoes DESC
    LIMIT 10
""").display()

# 7. Amostra dos dados finais
print(f"\n7. Amostra dos dados silver (5 registros):")
spark.table(TABELA_DESTINO).limit(5).display()

print(f"\n✓ Validação concluída")

# COMMAND ----------

# DBTITLE 1,Análise por Distribuidora — DEC/FEC Equivalente
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- CONSULTAS ANALÍTICAS DE EXEMPLO
# MAGIC -- ============================================================================
# MAGIC
# MAGIC -- 1. Análise por distribuidora: DEC equivalente (horas de interrupção por UC)
# MAGIC -- DEC = Σ(Ca × t) / Cs, onde Ca = consumidores afetados, t = duração em horas, Cs = consumidores do conjunto
# MAGIC SELECT 
# MAGIC     sig_agente,
# MAGIC     nom_agente_regulado,
# MAGIC     mes_interrupcao,
# MAGIC     COUNT(*) AS num_interrupcoes,
# MAGIC     ROUND(
# MAGIC         SUM(num_unidade_consumidora * duracao_horas) / NULLIF(SUM(num_consumidor_conjunto), 0),
# MAGIC         4
# MAGIC     ) AS dec_equivalente_horas,
# MAGIC     ROUND(
# MAGIC         SUM(num_unidade_consumidora) / NULLIF(SUM(num_consumidor_conjunto), 0),
# MAGIC         4
# MAGIC     ) AS fec_equivalente
# MAGIC FROM workspace.proj_aneel_cont_01_silver.interrupcoes
# MAGIC WHERE dsc_tipo_interrupcao != 'Programada'
# MAGIC GROUP BY sig_agente, nom_agente_regulado, mes_interrupcao
# MAGIC ORDER BY sig_agente, mes_interrupcao

# COMMAND ----------

# DBTITLE 1,Análise por Conjunto de Unidades Consumidoras
# MAGIC %sql
# MAGIC -- 2. Análise por conjunto de unidades consumidoras
# MAGIC -- Detalha cada conjunto com métricas de continuidade
# MAGIC SELECT 
# MAGIC     sig_agente,
# MAGIC     ide_conjunto_unidade_consumidora AS id_conjunto,
# MAGIC     dsc_conjunto_unidade_consumidora AS nome_conjunto,
# MAGIC     dsc_subestacao_distribuicao AS subestacao,
# MAGIC     COUNT(*) AS num_interrupcoes,
# MAGIC     ROUND(SUM(duracao_horas), 2) AS horas_totais_interrupcao,
# MAGIC     ROUND(AVG(duracao_minutos), 2) AS duracao_media_minutos,
# MAGIC     MAX(duracao_horas) AS maior_interrupcao_horas,
# MAGIC     SUM(num_unidade_consumidora) AS total_unidades_afetadas,
# MAGIC     MAX(num_consumidor_conjunto) AS consumidores_conjunto
# MAGIC FROM workspace.proj_aneel_cont_01_silver.interrupcoes
# MAGIC GROUP BY sig_agente, ide_conjunto_unidade_consumidora, dsc_conjunto_unidade_consumidora, dsc_subestacao_distribuicao
# MAGIC ORDER BY horas_totais_interrupcao DESC
# MAGIC LIMIT 20

# COMMAND ----------

# DBTITLE 1,Análise por Causa (Fato Gerador)
# MAGIC %sql
# MAGIC -- 3. Análise por causa (fato gerador)
# MAGIC -- Distribuição das causas raiz das interrupções
# MAGIC SELECT 
# MAGIC     fato_gerador_origem,
# MAGIC     fato_gerador_natureza,
# MAGIC     fato_gerador_categoria,
# MAGIC     COUNT(*) AS num_interrupcoes,
# MAGIC     ROUND(SUM(duracao_horas), 2) AS horas_totais,
# MAGIC     SUM(num_unidade_consumidora) AS unidades_afetadas
# MAGIC FROM workspace.proj_aneel_cont_01_silver.interrupcoes
# MAGIC GROUP BY fato_gerador_origem, fato_gerador_natureza, fato_gerador_categoria
# MAGIC ORDER BY num_interrupcoes DESC
# MAGIC LIMIT 15