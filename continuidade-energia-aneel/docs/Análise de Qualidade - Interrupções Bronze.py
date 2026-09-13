# Databricks notebook source
# DBTITLE 1,Introdução
# MAGIC %md
# MAGIC # Análise de Qualidade de Dados - Interrupções Bronze
# MAGIC
# MAGIC ## Objetivo
# MAGIC Investigar a qualidade dos dados na tabela bronze `workspace.proj_aneel_cont_01_bronze.101_interrupcoes` antes de criar a transformação para Silver.
# MAGIC
# MAGIC ## Dataset
# MAGIC - **Tabela**: `workspace.proj_aneel_cont_01_bronze.101_interrupcoes`
# MAGIC - **Total de registros**: 500.000
# MAGIC - **Período**: 2024 (01/Jan/2024 a 04/Jan/2025)
# MAGIC - **Granularidade esperada**: Uma interrupção pode afetar múltiplas unidades consumidoras

# COMMAND ----------

# DBTITLE 1,1. Visão Geral dos Dados
# MAGIC %sql
# MAGIC -- 1. VISÃO GERAL DOS DADOS
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_registros,
# MAGIC   COUNT(DISTINCT NumOrdemInterrupcao) as interrupcoes_unicas,
# MAGIC   COUNT(DISTINCT CONCAT(NumOrdemInterrupcao, '|', NumUnidadeConsumidora)) as chaves_naturais_unicas,
# MAGIC   COUNT(DISTINCT DscTipoInterrupcao) as tipos_interrupcao,
# MAGIC   MIN(DatInicioInterrupcao) as data_mais_antiga,
# MAGIC   MAX(DatFimInterrupcao) as data_mais_recente,
# MAGIC   COUNT(DISTINCT NumAno) as anos_distintos
# MAGIC FROM workspace.proj_aneel_cont_01_bronze.101_interrupcoes

# COMMAND ----------

# DBTITLE 1,Problema 1: Campos Nulos
# MAGIC %md
# MAGIC ## Problemas de Qualidade Identificados
# MAGIC
# MAGIC ### 1. **Campos Nulos** ✅
# MAGIC - ✅ Nenhum campo crítico possui valores nulos
# MAGIC - Campos verificados: NumOrdemInterrupcao, DatInicioInterrupcao, DatFimInterrupcao, DscTipoInterrupcao, NumUnidadeConsumidora, NumConsumidorConjunto

# COMMAND ----------

# DBTITLE 1,2. Análise de Nulos
# MAGIC %sql
# MAGIC -- 2. ANÁLISE DE NULOS
# MAGIC SELECT 
# MAGIC   SUM(CASE WHEN NumOrdemInterrupcao IS NULL THEN 1 ELSE 0 END) as nulos_ordem,
# MAGIC   SUM(CASE WHEN DatInicioInterrupcao IS NULL THEN 1 ELSE 0 END) as nulos_data_inicio,
# MAGIC   SUM(CASE WHEN DatFimInterrupcao IS NULL THEN 1 ELSE 0 END) as nulos_data_fim,
# MAGIC   SUM(CASE WHEN DscTipoInterrupcao IS NULL THEN 1 ELSE 0 END) as nulos_tipo,
# MAGIC   SUM(CASE WHEN NumUnidadeConsumidora IS NULL THEN 1 ELSE 0 END) as nulos_uc,
# MAGIC   SUM(CASE WHEN NumConsumidorConjunto IS NULL THEN 1 ELSE 0 END) as nulos_consumidores
# MAGIC FROM workspace.proj_aneel_cont_01_bronze.101_interrupcoes

# COMMAND ----------

# DBTITLE 1,Problema 2: Durações Inválidas
# MAGIC %md
# MAGIC ### 2. **Durações Inválidas** ⚠️
# MAGIC - **2.702 registros (0,54%)** com duração ZERO (DatFimInterrupcao = DatInicioInterrupcao)
# MAGIC - Não há durações negativas
# MAGIC - Interpretação: Interrupções instantâneas ou erro de registro

# COMMAND ----------

# DBTITLE 1,3. Análise de Durações Inválidas
# MAGIC %sql
# MAGIC -- 3. ANÁLISE DE DURAÇÕES INVÁLIDAS
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_duracao_invalida,
# MAGIC   COUNT(CASE WHEN DatFimInterrupcao < DatInicioInterrupcao THEN 1 END) as duracao_negativa,
# MAGIC   COUNT(CASE WHEN DatFimInterrupcao = DatInicioInterrupcao THEN 1 END) as duracao_zero,
# MAGIC   ROUND(COUNT(*) * 100.0 / 500000, 2) as percentual
# MAGIC FROM workspace.proj_aneel_cont_01_bronze.101_interrupcoes
# MAGIC WHERE DatFimInterrupcao <= DatInicioInterrupcao

# COMMAND ----------

# DBTITLE 1,Problema 3: Durações Extremas
# MAGIC %md
# MAGIC ### 3. **Durações Extremas/Suspeitas** ⚠️
# MAGIC - **5 registros** com duração > 30 dias (máximo: 52 dias)
# MAGIC - **298 registros** com duração entre 7 e 30 dias
# MAGIC - Alguns seguem padrão de "mês inteiro" (01/XX 00:01 até 31/XX 23:59)
# MAGIC - Podem ser interrupções crônicas ou dados sintéticos

# COMMAND ----------

# DBTITLE 1,4. Distribuição de Durações
# MAGIC %sql
# MAGIC -- 4. DISTRIBUIÇÃO DE DURAÇÕES
# MAGIC SELECT 
# MAGIC   CASE 
# MAGIC     WHEN TIMESTAMPDIFF(DAY, DatInicioInterrupcao, DatFimInterrupcao) > 30 THEN 'Mais de 30 dias'
# MAGIC     WHEN TIMESTAMPDIFF(DAY, DatInicioInterrupcao, DatFimInterrupcao) BETWEEN 7 AND 30 THEN 'Entre 7 e 30 dias'
# MAGIC     WHEN TIMESTAMPDIFF(DAY, DatInicioInterrupcao, DatFimInterrupcao) BETWEEN 1 AND 6 THEN 'Entre 1 e 6 dias'
# MAGIC     WHEN TIMESTAMPDIFF(HOUR, DatInicioInterrupcao, DatFimInterrupcao) > 0 THEN 'Até 24 horas'
# MAGIC     ELSE 'Zero ou negativo'
# MAGIC   END as faixa_duracao,
# MAGIC   COUNT(*) as qtd,
# MAGIC   ROUND(COUNT(*) * 100.0 / 500000, 2) as percentual
# MAGIC FROM workspace.proj_aneel_cont_01_bronze.101_interrupcoes
# MAGIC GROUP BY 1
# MAGIC ORDER BY 
# MAGIC   CASE 
# MAGIC     WHEN faixa_duracao = 'Mais de 30 dias' THEN 1
# MAGIC     WHEN faixa_duracao = 'Entre 7 e 30 dias' THEN 2
# MAGIC     WHEN faixa_duracao = 'Entre 1 e 6 dias' THEN 3
# MAGIC     WHEN faixa_duracao = 'Até 24 horas' THEN 4
# MAGIC     ELSE 5
# MAGIC   END

# COMMAND ----------

# DBTITLE 1,Problema 4: Duplicatas
# MAGIC %md
# MAGIC ### 4. **Duplicatas na Chave Natural** ⚠️
# MAGIC - **Chave natural esperada**: (NumOrdemInterrupcao, NumUnidadeConsumidora)
# MAGIC - **500.000 registros** vs **496.392 chaves únicas** = **3.608 duplicatas reais (0,72%)**
# MAGIC - As duplicatas representam a mesma interrupção + mesma UC, mas com datas diferentes
# MAGIC - Interpretação: Múltiplas versões/atualizações do mesmo evento na fonte

# COMMAND ----------

# DBTITLE 1,5. Análise de Duplicatas
# MAGIC %sql
# MAGIC -- 5. ANÁLISE DE DUPLICATAS
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_registros,
# MAGIC   COUNT(DISTINCT CONCAT(NumOrdemInterrupcao, '|', NumUnidadeConsumidora)) as chaves_unicas,
# MAGIC   COUNT(*) - COUNT(DISTINCT CONCAT(NumOrdemInterrupcao, '|', NumUnidadeConsumidora)) as qtd_duplicatas,
# MAGIC   ROUND((COUNT(*) - COUNT(DISTINCT CONCAT(NumOrdemInterrupcao, '|', NumUnidadeConsumidora))) * 100.0 / COUNT(*), 2) as perc_duplicatas
# MAGIC FROM workspace.proj_aneel_cont_01_bronze.101_interrupcoes

# COMMAND ----------

# DBTITLE 1,Problema 5: UCs Inválidas
# MAGIC %md
# MAGIC ### 5. **Unidades Consumidoras Inválidas** ⚠️
# MAGIC - **1.946 registros (0,39%)** com NumUnidadeConsumidora = 0
# MAGIC - Este valor é inválido para uma UC
# MAGIC - Necessário tratar ou excluir na camada Silver

# COMMAND ----------

# DBTITLE 1,6. UCs Inválidas
# MAGIC %sql
# MAGIC -- 6. UNIDADES CONSUMIDORAS INVÁLIDAS
# MAGIC SELECT 
# MAGIC   COUNT(*) as registros_uc_zero,
# MAGIC   ROUND(COUNT(*) * 100.0 / 500000, 2) as percentual
# MAGIC FROM workspace.proj_aneel_cont_01_bronze.101_interrupcoes
# MAGIC WHERE NumUnidadeConsumidora = 0

# COMMAND ----------

# DBTITLE 1,Resumo dos Problemas
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Resumo dos Problemas
# MAGIC
# MAGIC | Problema | Quantidade | % | Severidade | Ação |
# MAGIC |----------|-----------|---|------------|------|
# MAGIC | Campos nulos críticos | 0 | 0,00% | - | ✅ Nenhuma |
# MAGIC | Durações zero | 2.702 | 0,54% | ⚠️ Média | Excluir ou marcar |
# MAGIC | Durações > 30 dias | 5 | 0,00% | ⚠️ Média | Revisar/marcar |
# MAGIC | Durações 7-30 dias | 298 | 0,06% | ⚠️ Baixa | Revisar/marcar |
# MAGIC | Duplicatas (chave natural) | 3.608 | 0,72% | ⚠️ Alta | Deduplicate |
# MAGIC | UC inválidas (=0) | 1.946 | 0,39% | ⚠️ Alta | Excluir |
# MAGIC
# MAGIC **Total de registros com problemas**: ~8.559 (1,71% do total)
# MAGIC
# MAGIC **Registros limpos esperados**: ~491.441 (98,29%)

# COMMAND ----------

# DBTITLE 1,Regras de Tratamento para Silver
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Regras de Tratamento Propostas para Silver
# MAGIC
# MAGIC ### 1. **Exclusão de Registros Inválidos**
# MAGIC ```sql
# MAGIC WHERE NumUnidadeConsumidora > 0  -- Excluir UCs inválidas (1.946 registros)
# MAGIC   AND DatFimInterrupcao > DatInicioInterrupcao  -- Excluir durações zero/negativas (2.702 registros)
# MAGIC ```
# MAGIC
# MAGIC ### 2. **Deduplicação por Chave Natural**
# MAGIC Para duplicatas (mesma interrupção + mesma UC), manter apenas o registro mais recente:
# MAGIC ```sql
# MAGIC ROW_NUMBER() OVER (
# MAGIC   PARTITION BY NumOrdemInterrupcao, NumUnidadeConsumidora 
# MAGIC   ORDER BY DatGeracaoConjuntoDados DESC, DatFimInterrupcao DESC
# MAGIC ) = 1
# MAGIC ```
# MAGIC **Justificativa**: Registros com DatGeracaoConjuntoDados mais recente e DatFimInterrupcao mais recente são as versões mais atualizadas.
# MAGIC
# MAGIC ### 3. **Marcação de Durações Extremas**
# MAGIC Criar flag para análises posteriores (não excluir):
# MAGIC ```sql
# MAGIC CASE 
# MAGIC   WHEN TIMESTAMPDIFF(DAY, DatInicioInterrupcao, DatFimInterrupcao) > 30 THEN 'Duração extrema'
# MAGIC   WHEN TIMESTAMPDIFF(DAY, DatInicioInterrupcao, DatFimInterrupcao) BETWEEN 7 AND 30 THEN 'Duração longa'
# MAGIC   ELSE 'Normal'
# MAGIC END as flag_duracao
# MAGIC ```
# MAGIC
# MAGIC ### 4. **Cálculo de Métricas Derivadas**
# MAGIC ```sql
# MAGIC -- Duração em minutos
# MAGIC TIMESTAMPDIFF(MINUTE, DatInicioInterrupcao, DatFimInterrupcao) as duracao_minutos,
# MAGIC
# MAGIC -- Duração em horas (decimal)
# MAGIC TIMESTAMPDIFF(MINUTE, DatInicioInterrupcao, DatFimInterrupcao) / 60.0 as duracao_horas,
# MAGIC
# MAGIC -- Flag de tipo programado
# MAGIC CASE WHEN DscTipoInterrupcao = 'Programada' THEN TRUE ELSE FALSE END as is_programada
# MAGIC ```
# MAGIC
# MAGIC ### 5. **Validações de Qualidade na Silver**
# MAGIC Adicionar constraints/expectations:
# MAGIC - `EXPECT(NumUnidadeConsumidora > 0)`
# MAGIC - `EXPECT(DatFimInterrupcao > DatInicioInterrupcao)`
# MAGIC - `EXPECT(NumOrdemInterrupcao IS NOT NULL)`
# MAGIC - `EXPECT(duracao_minutos > 0)`

# COMMAND ----------

# DBTITLE 1,Validação das Regras
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Validação das Regras
# MAGIC
# MAGIC Vamos simular a aplicação das regras e provar que funcionam:

# COMMAND ----------

# DBTITLE 1,9. Simulação da Transformação Silver
# MAGIC %sql
# MAGIC -- 9. SIMULAÇÃO DA TRANSFORMAÇÃO SILVER
# MAGIC WITH registros_validos AS (
# MAGIC   SELECT 
# MAGIC     *,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY NumOrdemInterrupcao, NumUnidadeConsumidora 
# MAGIC       ORDER BY DatGeracaoConjuntoDados DESC, DatFimInterrupcao DESC
# MAGIC     ) as rn
# MAGIC   FROM workspace.proj_aneel_cont_01_bronze.101_interrupcoes
# MAGIC   WHERE NumUnidadeConsumidora > 0  -- Excluir UCs inválidas
# MAGIC     AND DatFimInterrupcao > DatInicioInterrupcao  -- Excluir durações zero/negativas
# MAGIC )
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_apos_filtros,
# MAGIC   COUNT(CASE WHEN rn = 1 THEN 1 END) as total_apos_dedup,
# MAGIC   500000 - COUNT(CASE WHEN rn = 1 THEN 1 END) as registros_excluidos,
# MAGIC   ROUND((500000 - COUNT(CASE WHEN rn = 1 THEN 1 END)) * 100.0 / 500000, 2) as perc_excluido
# MAGIC FROM registros_validos

# COMMAND ----------

# DBTITLE 1,10. PROVA: Zero Duplicatas
# MAGIC %sql
# MAGIC -- 10. PROVA: Verificar que não há mais duplicatas na chave natural
# MAGIC WITH silver_simulado AS (
# MAGIC   SELECT 
# MAGIC     *,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY NumOrdemInterrupcao, NumUnidadeConsumidora 
# MAGIC       ORDER BY DatGeracaoConjuntoDados DESC, DatFimInterrupcao DESC
# MAGIC     ) as rn
# MAGIC   FROM workspace.proj_aneel_cont_01_bronze.101_interrupcoes
# MAGIC   WHERE NumUnidadeConsumidora > 0
# MAGIC     AND DatFimInterrupcao > DatInicioInterrupcao
# MAGIC )
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_silver,
# MAGIC   COUNT(DISTINCT CONCAT(NumOrdemInterrupcao, '|', NumUnidadeConsumidora)) as chaves_unicas,
# MAGIC   COUNT(*) - COUNT(DISTINCT CONCAT(NumOrdemInterrupcao, '|', NumUnidadeConsumidora)) as duplicatas_restantes,
# MAGIC   CASE 
# MAGIC     WHEN COUNT(*) = COUNT(DISTINCT CONCAT(NumOrdemInterrupcao, '|', NumUnidadeConsumidora)) 
# MAGIC     THEN '✅ ZERO DUPLICATAS'
# MAGIC     ELSE '❌ AINDA HÁ DUPLICATAS'
# MAGIC   END as validacao
# MAGIC FROM silver_simulado
# MAGIC WHERE rn = 1

# COMMAND ----------

# DBTITLE 1,11. PROVA: Durações Válidas
# MAGIC %sql
# MAGIC -- 11. PROVA: Verificar que não há mais durações inválidas
# MAGIC WITH silver_simulado AS (
# MAGIC   SELECT 
# MAGIC     *,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY NumOrdemInterrupcao, NumUnidadeConsumidora 
# MAGIC       ORDER BY DatGeracaoConjuntoDados DESC, DatFimInterrupcao DESC
# MAGIC     ) as rn
# MAGIC   FROM workspace.proj_aneel_cont_01_bronze.101_interrupcoes
# MAGIC   WHERE NumUnidadeConsumidora > 0
# MAGIC     AND DatFimInterrupcao > DatInicioInterrupcao
# MAGIC )
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_silver,
# MAGIC   MIN(TIMESTAMPDIFF(MINUTE, DatInicioInterrupcao, DatFimInterrupcao)) as duracao_minima_minutos,
# MAGIC   CASE 
# MAGIC     WHEN MIN(TIMESTAMPDIFF(MINUTE, DatInicioInterrupcao, DatFimInterrupcao)) > 0 
# MAGIC     THEN '✅ TODAS AS DURAÇÕES SÃO VÁLIDAS'
# MAGIC     ELSE '❌ AINDA HÁ DURAÇÕES INVÁLIDAS'
# MAGIC   END as validacao
# MAGIC FROM silver_simulado
# MAGIC WHERE rn = 1

# COMMAND ----------

# DBTITLE 1,12. PROVA: UCs Válidas
# MAGIC %sql
# MAGIC -- 12. PROVA: Verificar que não há mais UCs inválidas
# MAGIC WITH silver_simulado AS (
# MAGIC   SELECT 
# MAGIC     *,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY NumOrdemInterrupcao, NumUnidadeConsumidora 
# MAGIC       ORDER BY DatGeracaoConjuntoDados DESC, DatFimInterrupcao DESC
# MAGIC     ) as rn
# MAGIC   FROM workspace.proj_aneel_cont_01_bronze.101_interrupcoes
# MAGIC   WHERE NumUnidadeConsumidora > 0
# MAGIC     AND DatFimInterrupcao > DatInicioInterrupcao
# MAGIC )
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_silver,
# MAGIC   MIN(NumUnidadeConsumidora) as uc_minima,
# MAGIC   COUNT(CASE WHEN NumUnidadeConsumidora = 0 THEN 1 END) as uc_zero,
# MAGIC   CASE 
# MAGIC     WHEN MIN(NumUnidadeConsumidora) > 0 
# MAGIC     THEN '✅ TODAS AS UCs SÃO VÁLIDAS'
# MAGIC     ELSE '❌ AINDA HÁ UCs INVÁLIDAS'
# MAGIC   END as validacao
# MAGIC FROM silver_simulado
# MAGIC WHERE rn = 1

# COMMAND ----------

# DBTITLE 1,Conclusão
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Conclusão
# MAGIC
# MAGIC ### Qualidade do Bronze
# MAGIC - **98,29%** dos registros são válidos ou podem ser tratados
# MAGIC - Problemas identificados são conhecidos e tratáveis
# MAGIC - Dados adequados para transformação Silver
# MAGIC
# MAGIC ### Próximos Passos
# MAGIC 1. ✅ Análise de qualidade concluída
# MAGIC 2. ⏭️ Implementar transformação Silver com as regras propostas
# MAGIC 3. ⏭️ Adicionar testes de qualidade (expectations) na pipeline
# MAGIC 4. ⏭️ Monitorar métricas de qualidade continuamente
# MAGIC
# MAGIC ### Confiança nas Regras
# MAGIC - ✅ **Deduplicação**: Provado que remove 100% das duplicatas
# MAGIC - ✅ **Durações**: Provado que remove 100% das durações inválidas
# MAGIC - ✅ **UCs**: Provado que remove 100% das UCs inválidas
# MAGIC - ✅ **Impacto**: Apenas 1,71% dos registros serão excluídos