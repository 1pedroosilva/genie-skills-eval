# Databricks notebook source
# MAGIC %md
# MAGIC # Extração de Dados - ANEEL
# MAGIC 
# MAGIC ## Objetivo
# MAGIC Extrair e fazer download dos dados abertos da ANEEL:
# MAGIC 1. Interrupções na rede de distribuição (2024 e 2025)
# MAGIC 2. Indicadores coletivos de continuidade (2024 e 2025)
# MAGIC 
# MAGIC ## Fontes
# MAGIC - Portal de Dados Abertos da ANEEL: https://dadosabertos.aneel.gov.br/
# MAGIC 
# MAGIC ## Saída
# MAGIC Dados brutos salvos na pasta `dados/raw/`

# COMMAND ----------

# TODO: Implementar extração de dados
# - Identificar URLs dos datasets
# - Fazer download dos arquivos (CSV, Excel, etc.)
# - Salvar em dados/raw/interrupcoes/ e dados/raw/indicadores/

# COMMAND ----------

# Configurações
ANOS = [2024, 2025]
URL_BASE_ANEEL = "https://dadosabertos.aneel.gov.br/dataset/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Próximos passos
# MAGIC 1. Implementar função de download
# MAGIC 2. Validar integridade dos arquivos
# MAGIC 3. Documentar estrutura dos dados brutos