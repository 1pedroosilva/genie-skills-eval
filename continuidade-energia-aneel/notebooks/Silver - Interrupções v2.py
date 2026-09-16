# Databricks notebook source
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

# COMMAND ----------

