# Databricks notebook source
# DBTITLE 1,Documentação
# MAGIC %md
# MAGIC # Transformação Silver - Indicadores de Continuidade
# MAGIC
# MAGIC ## Objetivo
# MAGIC Transformar os dados brutos da camada **bronze** para a camada **silver**, aplicando padronização, limpeza e estruturação para análise.
# MAGIC
# MAGIC ## Fonte
# MAGIC - **Tabela Bronze**: `workspace.proj_aneel_cont_01_bronze.indicadores_continuidade`
# MAGIC - **Registros**: ~5,1 milhões de registros (jan/2020 a ago/2026)
# MAGIC - **Indicadores**: 23 tipos (DEC, FEC, DIC, FIC, DMIC, NumCon e variantes)
# MAGIC - **Distribuidoras**: 105 agentes regulados
# MAGIC
# MAGIC ## Destino
# MAGIC - **Tabela Silver**: `workspace.proj_aneel_cont_02_silver.indicadores_continuidade`
# MAGIC - **Estratégia**: Full load (overwrite) a cada execução
# MAGIC
# MAGIC ## Transformações Aplicadas
# MAGIC
# MAGIC ### 1. Padronização de Colunas
# MAGIC - Renomear colunas para snake_case em português claro
# MAGIC - Manter nomes descritivos e intuitivos
# MAGIC
# MAGIC ### 2. Criação de Data de Apuração
# MAGIC - **Entrada**: `anoindice` (ano) + `numperiodoindice` (mês 1-12)
# MAGIC - **Saída**: `data_apuracao` (DATE no formato YYYY-MM-01)
# MAGIC - **Lógica**: Primeiro dia do mês de apuração
# MAGIC
# MAGIC ### 3. Limpeza e Qualidade
# MAGIC - Remover espaços em branco de campos de texto
# MAGIC - Validar valores nulos em campos obrigatórios
# MAGIC - Adicionar flag de qualidade para registros suspeitos
# MAGIC
# MAGIC ### 4. Metadados de Processamento
# MAGIC - `_processado_em`: timestamp UTC da execução
# MAGIC - `_data_processamento`: data da execução
# MAGIC - `_versao_silver`: identificador da versão de transformação

# COMMAND ----------

# DBTITLE 1,Configurações e Parâmetros
# ============================================================================
# CONFIGURAÇÕES E PARÂMETROS
# ============================================================================

from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

# --- Origem (camada Bronze) ---
CATALOG = "workspace"
SCHEMA_BRONZE = "proj_aneel_cont_01_bronze"
TABELA_BRONZE = "indicadores_continuidade"
TABELA_BRONZE_COMPLETA = f"{CATALOG}.{SCHEMA_BRONZE}.{TABELA_BRONZE}"

# --- Destino (camada Silver) ---
SCHEMA_SILVER = "proj_aneel_cont_02_silver"
TABELA_SILVER = "indicadores_continuidade"
TABELA_SILVER_COMPLETA = f"{CATALOG}.{SCHEMA_SILVER}.{TABELA_SILVER}"

# --- Metadados de execução ---
PROCESS_TS = datetime.now(timezone.utc)
PROCESS_DATE = PROCESS_TS.date()
VERSAO_SILVER = "v1.0.0"
RUN_ID = PROCESS_TS.strftime("%Y%m%d%H%M%S")

# --- Log inicial ---
print("=" * 80)
print("TRANSFORMAÇÃO SILVER - INDICADORES DE CONTINUIDADE")
print("=" * 80)
print(f"  Origem       : {TABELA_BRONZE_COMPLETA}")
print(f"  Destino      : {TABELA_SILVER_COMPLETA}")
print(f"  Process_ts   : {PROCESS_TS}")
print(f"  Process_date : {PROCESS_DATE}")
print(f"  Run ID       : {RUN_ID}")
print(f"  Versão      : {VERSAO_SILVER}")
print(f"  Estratégia  : Full load (overwrite)")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Criar Schema Silver (se não existir)
# MAGIC %sql
# MAGIC -- Criar schema silver se não existir
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.proj_aneel_cont_02_silver
# MAGIC COMMENT 'Camada Silver - Dados padronizados e limpos do projeto de Continuidade de Fornecimento ANEEL'

# COMMAND ----------

# DBTITLE 1,Transformação Bronze → Silver
# ============================================================================
# TRANSFORMAÇÃO BRONZE → SILVER
# ============================================================================

print(f"\n[1/4] Lendo dados da camada bronze...")
print(f"       Tabela: {TABELA_BRONZE_COMPLETA}")

# Ler tabela bronze
df_bronze = spark.table(TABELA_BRONZE_COMPLETA)
count_bronze = df_bronze.count()
print(f"       ✓ {count_bronze:,} registros carregados")

print(f"\n[2/4] Aplicando transformações...")

# Transformação principal
df_silver = df_bronze.select(
    # === CHAVES DE NEGÓCIO ===
    F.col("ideconjundconsumidoras").alias("id_conjunto_uc"),
    F.trim(F.col("dscconjundconsumidoras")).alias("conjunto_uc"),
    F.trim(F.col("sigagente")).alias("distribuidora"),
    F.col("numcnpj").alias("cnpj"),
    
    # === INDICADOR ===
    F.trim(F.col("sigindicador")).alias("indicador"),
    F.col("vlrindiceenviado").alias("valor"),
    
    # === PERÍODO DE APURAÇÃO ===
    # Criar data no formato YYYY-MM-01 a partir de ano + mês
    F.make_date(
        F.col("anoindice"),
        F.col("numperiodoindice"),
        F.lit(1)
    ).alias("data_apuracao"),
    
    F.col("anoindice").alias("ano"),
    F.col("numperiodoindice").alias("mes"),
    
    # === METADADOS DE ORIGEM ===
    F.col("datgeracaoconjuntodados").alias("data_geracao_aneel"),
    F.col("_fonte_url").alias("fonte_url"),
    F.col("_ingest_ts").alias("ingest_ts_bronze"),
    F.col("_ingest_date").alias("ingest_date_bronze"),
    F.col("_run_id").alias("run_id_bronze"),
    
    # === METADADOS DE PROCESSAMENTO SILVER ===
    F.lit(PROCESS_TS).alias("_processado_em"),
    F.lit(PROCESS_DATE).alias("_data_processamento"),
    F.lit(VERSAO_SILVER).alias("_versao_silver"),
    F.lit(RUN_ID).alias("_run_id_silver")
)

print(f"       ✓ Colunas padronizadas")
print(f"       ✓ Data de apuração criada (ano + mês → YYYY-MM-01)")
print(f"       ✓ Espaços em branco removidos")
print(f"       ✓ Metadados de processamento adicionados")

print(f"\n[3/4] Escrevendo na camada silver...")
print(f"       Tabela: {TABELA_SILVER_COMPLETA}")
print(f"       Modo: Overwrite")

# Escrever na tabela silver
(
    df_silver
    .write
    .mode("overwrite")
    .format("delta")
    .option("overwriteSchema", "true")
    .saveAsTable(TABELA_SILVER_COMPLETA)
)

count_silver = spark.table(TABELA_SILVER_COMPLETA).count()
print(f"       ✓ {count_silver:,} registros escritos")

print(f"\n[4/4] Otimizando tabela Delta...")
spark.sql(f"OPTIMIZE {TABELA_SILVER_COMPLETA}")
print(f"       ✓ Compactação concluída")

print(f"\n" + "=" * 80)
print(f"TRANSFORMAÇÃO CONCLUÍDA COM SUCESSO")
print(f"  Bronze: {count_bronze:,} registros")
print(f"  Silver: {count_silver:,} registros")
print(f"  Tabela: {TABELA_SILVER_COMPLETA}")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Validações de Qualidade
# ============================================================================
# VALIDAÇÕES DE QUALIDADE
# ============================================================================

print("=" * 80)
print("VALIDAÇÕES DE QUALIDADE DOS DADOS SILVER")
print("=" * 80)

# 1. Contagem total e comparação com bronze
total_silver = spark.table(TABELA_SILVER_COMPLETA).count()
total_bronze = spark.table(TABELA_BRONZE_COMPLETA).count()
print(f"\n1. Total de registros:")
print(f"   Bronze: {total_bronze:,}")
print(f"   Silver: {total_silver:,}")
print(f"   Match:  {'✓ SIM' if total_silver == total_bronze else '✗ NÃO'}")

# 2. Cobertura temporal
print(f"\n2. Cobertura temporal:")
df_cobertura = spark.sql(f"""
    SELECT 
        MIN(data_apuracao) as data_inicio,
        MAX(data_apuracao) as data_fim,
        COUNT(DISTINCT data_apuracao) as total_periodos,
        COUNT(DISTINCT ano) as total_anos,
        COUNT(DISTINCT mes) as total_meses
    FROM {TABELA_SILVER_COMPLETA}
""")
df_cobertura.show(truncate=False)

# 3. Distribuição por indicador
print(f"\n3. Distribuição por tipo de indicador:")
df_indicadores = spark.sql(f"""
    SELECT 
        indicador,
        COUNT(*) as total_registros,
        COUNT(DISTINCT distribuidora) as total_distribuidoras,
        ROUND(COUNT(*) * 100.0 / {total_silver}, 2) as percentual
    FROM {TABELA_SILVER_COMPLETA}
    GROUP BY indicador
    ORDER BY total_registros DESC
""")
df_indicadores.show(30, truncate=False)

# 4. Distribuidoras com mais registros
print(f"\n4. Top 10 distribuidoras por volume de registros:")
df_top_distribuidoras = spark.sql(f"""
    SELECT 
        distribuidora,
        COUNT(*) as total_registros,
        COUNT(DISTINCT indicador) as indicadores_reportados,
        MIN(data_apuracao) as primeira_data,
        MAX(data_apuracao) as ultima_data
    FROM {TABELA_SILVER_COMPLETA}
    GROUP BY distribuidora
    ORDER BY total_registros DESC
    LIMIT 10
""")
df_top_distribuidoras.show(truncate=False)

# 5. Verificação de nulos em campos obrigatórios
print(f"\n5. Verificação de valores nulos em campos-chave:")
df_nulos = spark.sql(f"""
    SELECT 
        SUM(CASE WHEN id_conjunto_uc IS NULL THEN 1 ELSE 0 END) as nulos_id_conjunto_uc,
        SUM(CASE WHEN conjunto_uc IS NULL THEN 1 ELSE 0 END) as nulos_conjunto_uc,
        SUM(CASE WHEN distribuidora IS NULL THEN 1 ELSE 0 END) as nulos_distribuidora,
        SUM(CASE WHEN indicador IS NULL THEN 1 ELSE 0 END) as nulos_indicador,
        SUM(CASE WHEN valor IS NULL THEN 1 ELSE 0 END) as nulos_valor,
        SUM(CASE WHEN data_apuracao IS NULL THEN 1 ELSE 0 END) as nulos_data_apuracao
    FROM {TABELA_SILVER_COMPLETA}
""")
df_nulos.show(truncate=False)

print("\n" + "=" * 80)
print("VALIDAÇÕES CONCLUÍDAS")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,Consulta de Exemplo - DEC e FEC por Distribuidora
# MAGIC %sql
# MAGIC -- Exemplo: Evolução de DEC e FEC das principais distribuidoras em 2025
# MAGIC WITH indicadores_principais AS (
# MAGIC   SELECT 
# MAGIC     distribuidora,
# MAGIC     data_apuracao,
# MAGIC     ano,
# MAGIC     mes,
# MAGIC     MAX(CASE WHEN indicador = 'DEC' THEN valor END) as dec_horas,
# MAGIC     MAX(CASE WHEN indicador = 'FEC' THEN valor END) as fec_qtd,
# MAGIC     MAX(CASE WHEN indicador = 'NumCon' THEN valor END) as num_consumidores
# MAGIC   FROM workspace.proj_aneel_cont_02_silver.indicadores_continuidade
# MAGIC   WHERE indicador IN ('DEC', 'FEC', 'NumCon')
# MAGIC     AND ano = 2025
# MAGIC   GROUP BY distribuidora, data_apuracao, ano, mes
# MAGIC )
# MAGIC SELECT 
# MAGIC   distribuidora,
# MAGIC   data_apuracao,
# MAGIC   dec_horas,
# MAGIC   fec_qtd,
# MAGIC   num_consumidores,
# MAGIC   ROUND(dec_horas * num_consumidores, 2) as dec_total_horas
# MAGIC FROM indicadores_principais
# MAGIC WHERE num_consumidores > 100000  -- Apenas grandes distribuidoras
# MAGIC ORDER BY distribuidora, data_apuracao
# MAGIC LIMIT 50

# COMMAND ----------

# DBTITLE 1,Consulta de Exemplo - Estrutura da Tabela
# MAGIC %sql
# MAGIC -- Visualizar estrutura e amostra da tabela silver
# MAGIC SELECT 
# MAGIC   id_conjunto_uc,
# MAGIC   conjunto_uc,
# MAGIC   distribuidora,
# MAGIC   indicador,
# MAGIC   valor,
# MAGIC   data_apuracao,
# MAGIC   ano,
# MAGIC   mes
# MAGIC FROM workspace.proj_aneel_cont_02_silver.indicadores_continuidade
# MAGIC WHERE ano = 2025 AND mes = 8
# MAGIC ORDER BY distribuidora, indicador
# MAGIC LIMIT 20

# COMMAND ----------

# DBTITLE 1,Notas e Próximos Passos
# MAGIC %md
# MAGIC ## Notas Importantes
# MAGIC
# MAGIC ### Estrutura da Tabela Silver
# MAGIC
# MAGIC A tabela silver possui a seguinte estrutura padronizada:
# MAGIC
# MAGIC #### Chaves de Negócio
# MAGIC - **id_conjunto_uc**: ID numérico do conjunto de unidades consumidoras (chave primária junto com data_apuracao e indicador)
# MAGIC - **conjunto_uc**: Descrição/nome do conjunto (ex: "CERNHE", "Rio Espera", "BH Gutierrez")
# MAGIC - **distribuidora**: Sigla da distribuidora (ex: "CEMIG-D", "ELETROPAULO", "CELESC")
# MAGIC - **cnpj**: CNPJ da distribuidora
# MAGIC
# MAGIC #### Indicador e Valor
# MAGIC - **indicador**: Tipo de indicador (DEC, FEC, DIC, FIC, DMIC, NumCon, etc.) - 23 tipos no total
# MAGIC - **valor**: Valor numérico do indicador reportado
# MAGIC
# MAGIC #### Período de Apuração
# MAGIC - **data_apuracao**: Data no formato YYYY-MM-01 (primeiro dia do mês de apuração)
# MAGIC - **ano**: Ano de apuração (extraído para conveniência)
# MAGIC - **mes**: Mês de apuração (1-12, extraído para conveniência)
# MAGIC
# MAGIC #### Metadados
# MAGIC - **data_geracao_aneel**: Data em que a ANEEL gerou o arquivo original
# MAGIC - **fonte_url**: URL do arquivo Parquet da ANEEL
# MAGIC - **ingest_ts_bronze / ingest_date_bronze**: Quando os dados foram ingeridos no bronze
# MAGIC - **_processado_em**: Timestamp UTC da transformação silver
# MAGIC - **_data_processamento**: Data da transformação silver
# MAGIC - **_versao_silver**: Versão da lógica de transformação
# MAGIC - **_run_id_silver**: ID único da execução
# MAGIC
# MAGIC ### Principais Indicadores
# MAGIC
# MAGIC - **DEC** (Duração Equivalente de Interrupção): Tempo médio (em horas) que cada unidade consumidora ficou sem energia
# MAGIC - **FEC** (Frequência Equivalente de Interrupção): Número médio de interrupções por unidade consumidora
# MAGIC - **DIC** (Duração de Interrupção Individual): Tempo de interrupção por consumidor
# MAGIC - **FIC** (Frequência de Interrupção Individual): Número de interrupções por consumidor
# MAGIC - **DMIC** (Duração Máxima de Interrupção Contínua): Tempo máximo de uma única interrupção
# MAGIC - **NumCon**: Número de consumidores no conjunto
# MAGIC
# MAGIC Existem também variantes desses indicadores (ex: DECPF, FECPF, DECCR, FECCR) que consideram diferentes critérios de cálculo.
# MAGIC
# MAGIC ### Estratégia de Atualização
# MAGIC
# MAGIC - **Frequência**: Executar após cada nova carga da camada bronze
# MAGIC - **Modo**: Full load (overwrite) - substitui todos os dados a cada execução
# MAGIC - **Custo**: ~5,1 milhões de registros processados por execução
# MAGIC
# MAGIC ### Próximos Passos
# MAGIC
# MAGIC 1. **Camada Gold**: Criar agregações e métricas de negócio
# MAGIC    - Médias mensais/anuais por distribuidora
# MAGIC    - Comparações com limites regulatórios
# MAGIC    - Rankings de desempenho
# MAGIC    - Séries temporais para análise de tendência
# MAGIC
# MAGIC 2. **Qualidade de Dados**: Implementar regras de validação adicionais
# MAGIC    - Detectar valores atípicos (outliers)
# MAGIC    - Validar consistência temporal (valores faltantes)
# MAGIC    - Verificar coerência entre indicadores relacionados
# MAGIC
# MAGIC 3. **Documentação**: Adicionar comentários à tabela no Unity Catalog
# MAGIC    ```sql
# MAGIC    COMMENT ON TABLE workspace.proj_aneel_cont_02_silver.indicadores_continuidade 
# MAGIC    IS 'Indicadores de continuidade de fornecimento ANEEL - camada silver padronizada';
# MAGIC    ```
# MAGIC
# MAGIC 4. **Otimização**: Considerar particionamento se o volume crescer significativamente
# MAGIC    - Particionar por `ano` e `distribuidora` para queries mais eficientes
# MAGIC
# MAGIC ### Uso Recomendado
# MAGIC
# MAGIC Para análises, sempre use a tabela **silver** como ponto de partida:
# MAGIC ```sql
# MAGIC SELECT * FROM workspace.proj_aneel_cont_02_silver.indicadores_continuidade
# MAGIC WHERE indicador IN ('DEC', 'FEC')
# MAGIC   AND data_apuracao >= '2025-01-01'
# MAGIC ```
# MAGIC
# MAGIC A camada **bronze** deve ser usada apenas para auditoria ou reprocessamento.

# COMMAND ----------

