# Dicionário de Dados

## 1. Interrupções na Rede de Distribuição

### Fonte
* **Dataset**: [Interrupções na Rede de Distribuição - ANEEL](https://dadosabertos.aneel.gov.br/dataset/interrupcoes-na-rede-de-distribuicao)
* **Última atualização**: 12/09/2026
* **Formato**: Parquet
* **Codificação**: UTF-8
* **Tabela bronze**: `workspace.proj_aneel_cont_01_bronze.101_interrupcoes`
* **Total de registros**: 500.000 (ano 2024)
* **Distribuidoras**: 4 agentes (CEA, EAC, ETO, EQUATORIAL MA)

### Campos (schema da tabela bronze — PascalCase original da fonte)

| Campo | Tipo | Descrição | Exemplo |
|-------|------|-------------|----------|
| `DatGeracaoConjuntoDados` | DATE | Data de geração do conjunto de dados pela ANEEL | 2026-07-24 |
| `IdeConjuntoUnidadeConsumidora` | LONG | ID do conjunto de unidades consumidoras | 14563 |
| `DscConjuntoUnidadeConsumidora` | STRING | Nome do conjunto de unidades consumidoras | "PORTO GRANDE" |
| `DscAlimentadorSubestacao` | STRING | Código do alimentador da subestação | "01N3" |
| `DscSubestacaoDistribuicao` | STRING | Código da subestação de distribuição | "SEI" |
| `NumOrdemInterrupcao` | STRING | Número de ordem único da interrupção | "2024-12-3447-14563-15950955" |
| `DscTipoInterrupcao` | STRING | Tipo da interrupção | "Não Programada", "Programada" |
| `IdeMotivoInterrupcao` | LONG | ID do motivo da interrupção | 0 |
| `DatInicioInterrupcao` | TIMESTAMP | Data/hora de início da interrupção | 2024-12-08T23:43:42Z |
| `DatFimInterrupcao` | TIMESTAMP | Data/hora de fim da interrupção | 2024-12-09T01:23:54Z |
| `DscFatoGeradorInterrupcao` | STRING | Causa hierárquica separada por `;` ou `-` | "INTERNA;NAO PROGRAMADA;PROPRIAS DO SISTEMA;FALHA..." |
| `NumNivelTensao` | LONG | Nível de tensão em volts | 13800 |
| `NumUnidadeConsumidora` | LONG | Número de unidades consumidoras afetadas | 1630 |
| `NumConsumidorConjunto` | LONG | Total de consumidores do conjunto | 14370 |
| `NumAno` | LONG | Ano de referência | 2024 |
| `NomAgenteRegulado` | STRING | Nome da distribuidora | "COMPANHIA DE ELETRICIDADE DO AMAPA CEA" |
| `SigAgente` | STRING | Sigla da distribuidora | "CEA" |
| `NumCPFCNPJ` | LONG | CNPJ da distribuidora | 5965546000109 |

### Regras de Negócio
* Interrupções programadas podem não contar para cálculo do DEC (excluídas na consulta analítica)
* Duração = DatFimInterrupcao - DatInicioInterrupcao
* Excluir registros com duração ≤ 0, unidades consumidoras = 0 e tensão = 0
* O campo `DscFatoGeradorInterrupcao` contém níveis hierárquicos separados por `;` ou `-` (4 níveis: origem, natureza, categoria, detalhe)
* Colunas originais em PascalCase, normalizadas para snake_case na silver

---

## 2. Indicadores Coletivos de Continuidade

### Fonte
* **Dataset**: [Indicadores Coletivos de Continuidade - ANEEL](https://dadosabertos.aneel.gov.br/dataset/indicadores-coletivos-de-continuidade)
* **URL do arquivo**: `https://dadosabertos.aneel.gov.br/dataset/d5f0712e-62f6-4736-8dff-9991f10758a7/resource/d7f70fb1-725c-4748-afeb-65c6a78df550/download/indicadores-continuidade-coletivos-2020-2029.parquet`
* **Última atualização**: 11/09/2026 (Run ID: 20260911032756)
* **Formato**: Parquet
* **Codificação**: UTF-8
* **Tabela bronze**: `workspace.proj_aneel_cont_01_bronze.indicadores_continuidade`
* **Total de registros**: 5.108.332
* **Cobertura temporal**: jan/2020 a ago/2026 (80 períodos mensais)
* **Distribuidoras**: 105 agentes

### Campos (schema da tabela bronze)

| Campo | Tipo | Descrição | Exemplo |
|-------|------|-------------|----------|
| `datgeracaoconjuntodados` | DATE | Data de geração do conjunto de dados pela ANEEL | 2026-09-05 |
| `ideconjundconsumidoras` | BIGINT | ID do conjunto de unidades consumidoras | 16658 |
| `dscconjundconsumidoras` | STRING | Descrição do conjunto de unidades consumidoras | "CERSAD" |
| `sigagente` | STRING | Sigla do agente (distribuidora) | "CEMIG-D" |
| `numcnpj` | BIGINT | CNPJ da distribuidora | 6981180000116 |
| `sigindicador` | STRING | Sigla do indicador (DEC, FEC, etc.) | "DEC" |
| `anoindice` | BIGINT | Ano de referência do índice | 2025 |
| `numperiodoindice` | BIGINT | Número do período (1-12, mensal) | 6 |
| `vlrindiceenviado` | DOUBLE | Valor do indicador enviado pela distribuidora | 0.63 |
| `_fonte_url` | STRING | URL do arquivo fonte baixado (metadado) | https://dadosabertos.aneel.gov.br/... |
| `_ingest_ts` | TIMESTAMP | Timestamp UTC da ingestão (metadado) | 2026-09-11T03:27:56Z |
| `_ingest_date` | DATE | Data da ingestão (metadado) | 2026-09-11 |
| `_run_id` | STRING | ID único da execução (metadado) | 20260911032756 |

### Indicadores disponíveis (23 tipos)

| Sigla | Descrição |
|-------|-----------|
| DEC | Duração Equivalente de Interrupção por Unidade Consumidora |
| FEC | Frequência Equivalente de Interrupção por Unidade Consumidora |
| DECIP / FECIP | DEC/FEC Individual Programada |
| DECIPC / FECIPC | DEC/FEC Individual Programada Contínua |
| DECIND / FECIND | DEC/FEC Individual Não Programada |
| DECINC / FECINC | DEC/FEC Individual Contínua |
| DECINE / FECINE | DEC/FEC Individual Não Programada Especial |
| DECINO / FECINO | DEC/FEC Individual Não Programada Outros |
| DECXP / FECXP | DEC/FEC Extra Programada |
| DECXPC / FECXPC | DEC/FEC Extra Programada Contínua |
| DECXN / FECXN | DEC/FEC Extra Não Programada |
| DECXNC / FECXNC | DEC/FEC Extra Não Programada Contínua |
| NumCon | Número de Consumidores |

### Notas sobre os dados
* Colunas originais da fonte em PascalCase (ex: `DatGeracaoConjuntoDados`), normalizadas para lowercase na bronze
* O valor `vlrindiceenviado` pode ser 0 (zero) para indicadores não aplicáveis ao período/conjunto
* `numperiodoindice` varia de 1 a 12 (períodos mensais)

### Fórmulas Oficiais

**DEC (Duração Equivalente de Interrupção por Unidade Consumidora)**
```
DEC = Σ(Ca(i) × t(i)) / Cs

Onde:
- Ca(i) = Número de consumidores afetados na interrupção i
- t(i) = Tempo de duração da interrupção i (em horas)
- Cs = Total de consumidores atendidos
```

**FEC (Frequência Equivalente de Interrupção por Unidade Consumidora)**
```
FEC = Σ(Ca(i)) / Cs

Onde:
- Ca(i) = Número de consumidores afetados na interrupção i
- Cs = Total de consumidores atendidos
```

### Notas
* DEC e FEC são os principais indicadores de qualidade de fornecimento
* Valores em horas para DEC
* Podem haver metas diferentes por região e tipo de distribuidora

---

## 3. Camada Silver — Interrupções Tratadas

### Tabela
* **Tabela silver**: `workspace.proj_aneel_cont_01_silver.interrupcoes`
* **Origem**: `workspace.proj_aneel_cont_01_bronze.101_interrupcoes`
* **Última atualização**: 12/09/2026 (Run ID: 20260912050353)
* **Total de registros**: 495.350 (após filtros de qualidade)
* **Colunas**: 34
* **Estratégia**: Full load (overwrite)

### Campos (schema da tabela silver — snake_case)

| Campo | Tipo | Descrição |
|-------|------|-------------|
| `num_ordem_interrupcao` | STRING | Número de ordem único da interrupção |
| `ide_conjunto_unidade_consumidora` | LONG | ID do conjunto de unidades consumidoras |
| `dsc_conjunto_unidade_consumidora` | STRING | Nome do conjunto de unidades consumidoras |
| `sig_agente` | STRING | Sigla da distribuidora (trim aplicado) |
| `nom_agente_regulado` | STRING | Nome da distribuidora |
| `num_cnpj` | LONG | CNPJ da distribuidora (valor numérico) |
| `cnpj_formatado` | STRING | CNPJ com zeros à esquerda (14 dígitos) |
| `dsc_subestacao_distribuicao` | STRING | Código da subestação de distribuição |
| `dsc_alimentador_subestacao` | STRING | Código do alimentador da subestacao |
| `dsc_tipo_interrupcao` | STRING | Tipo da interrupção ("Não Programada", "Programada") |
| `ide_motivo_interrupcao` | LONG | ID do motivo da interrupção |
| `dsc_fato_gerador_interrupcao` | STRING | Causa original completa (separada por `;`/`-`) |
| `fato_gerador_origem` | STRING | Nível 1 da causa: INTERNA, EXTERNA, etc. |
| `fato_gerador_natureza` | STRING | Nível 2 da causa: NAO PROGRAMADA, PROGRAMADA, etc. |
| `fato_gerador_categoria` | STRING | Nível 3 da causa: PROPRIAS DO SISTEMA, MEIO AMBIENTE, etc. |
| `fato_gerador_detalhe` | STRING | Nível 4 da causa: FALHA DE MATERIAL, ARVORE OU VEGETACAO, etc. |
| `dat_inicio_interrupcao` | TIMESTAMP | Data/hora de início da interrupção |
| `dat_fim_interrupcao` | TIMESTAMP | Data/hora de fim da interrupção |
| `data_interrupcao` | DATE | Data extraída de dat_inicio_interrupcao |
| `mes_interrupcao` | INT | Mês (1-12) extraído de dat_inicio_interrupcao |
| `dia_semana_interrupcao` | INT | Dia da semana (1=Domingo, 7=Sábado) |
| `hora_inicio_interrupcao` | INT | Hora (0-23) extraída de dat_inicio_interrupcao |
| `duracao_segundos` | LONG | Duração em segundos (fim - início) |
| `duracao_minutos` | DOUBLE | Duração em minutos (arredondado 2 casas) |
| `duracao_horas` | DOUBLE | Duração em horas (arredondado 4 casas) |
| `num_nivel_tensao` | LONG | Nível de tensão em volts |
| `nivel_tensao_kv` | DOUBLE | Nível de tensão em kV (arredondado 3 casas) |
| `num_unidade_consumidora` | LONG | Número de unidades consumidoras afetadas |
| `num_consumidor_conjunto` | LONG | Total de consumidores do conjunto |
| `num_ano` | LONG | Ano de referência |
| `dat_geracao_conjunto_dados` | DATE | Data de geração do conjunto de dados pela ANEEL |
| `_silver_ts` | TIMESTAMP | Timestamp UTC do processamento silver |
| `_silver_run_id` | STRING | ID único da execução silver (YYYYMMDDHHMMSS) |
| `_origem_tabela` | STRING | Tabela bronze de origem |

### Transformações aplicadas
1. Normalização PascalCase → snake_case (18 colunas originais)
2. Trim de espaços em 8 colunas texto
3. Cálculo de duração (segundos, minutos, horas)
4. Decomposição do fato gerador (separadores `;` e `-` normalizados para `;`, split em 4 níveis com acesso seguro)
5. Formatação de CNPJ com zeros à esquerda
6. Conversão de tensão (V → kV)
7. Derivação temporal (data, mês, dia da semana, hora)
8. Filtros de qualidade (removidos 4.650 registros: duração ≤ 0, unidades = 0, tensão = 0)
9. Metadados silver (_silver_ts, _silver_run_id, _origem_tabela)

---

## 4. Dados Processados

### `interrupcoes_processado.parquet`
* Dados de interrupções limpos e padronizados
* Campos adicionais: região, categoria_distribuidora
* Apenas interrupções válidas para cálculo de DEC

### `indicadores_processado.parquet`
* Indicadores oficiais consolidados
* Normalizado para comparação com DEC calculado

### `analise_dec.parquet`
* DEC calculado vs. DEC oficial por distribuidora
* Diferenças absolutas e percentuais
* Flags de discrepância

---

## Glossário

* **DEC**: Duração Equivalente de Interrupção por Unidade Consumidora (em horas)
* **FEC**: Frequência Equivalente de Interrupção por Unidade Consumidora (quantidade)
* **ANEEL**: Agência Nacional de Energia Elétrica
* **Distribuidora**: Concessionária responsável pela distribuição de energia em uma área
* **Conjunto**: Agrupamento de equipamentos da rede elétrica

---

**Última atualização**: 12/09/2026