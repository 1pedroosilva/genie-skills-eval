# Visão Geral do Projeto: Continuidade de Fornecimento

## Objetivo do Projeto

Comparar o tempo de interrupção das distribuidoras de energia elétrica com os indicadores regulatórios estabelecidos pela ANEEL (Agência Nacional de Energia Elétrica), possibilitando análises de conformidade, desempenho e identificação de padrões de interrupção no fornecimento de energia.

---

## Fontes de Dados

### 1. Dados de Interrupções da ANEEL
**Descrição**: Registros individualizados de eventos de interrupção de fornecimento de energia elétrica reportados pelas distribuidoras à ANEEL.

**Formato**: Arquivos Parquet  
**Localização**: Unity Catalog Volume (path não especificado)  
**Granularidade**: Evento individual de interrupção  
**Atualização**: Anual (um arquivo por ano civil)  

**Campos principais**:
- Identificação da distribuidora (nome, sigla, CNPJ)
- Temporal: Data/hora de início e fim da interrupção
- Localização: Subestação, alimentador, conjunto de UCs
- Classificação: Tipo de interrupção (programada, emergencial), motivo, fato gerador
- Impacto: Número de unidades consumidoras afetadas, nível de tensão

### 2. Indicadores de Continuidade da ANEEL
**Descrição**: Indicadores regulatórios de qualidade de fornecimento (DEC, FEC e variantes).

**Formato**: Parquet (presumido)  
**Localização**: Unity Catalog Volume (path não especificado)  
**Granularidade**: Distribuidora × Conjunto de UCs × Ano  
**Atualização**: Anual  

**Indicadores incluídos**:
- **DEC** (Duração Equivalente de Interrupção por Unidade Consumidora): Tempo médio de interrupção por consumidor
- **FEC** (Frequência Equivalente de Interrupção por Unidade Consumidora): Número médio de interrupções por consumidor
- Variantes: DEC/FEC por tipo de causa (índice de controle, desempenho, excelência, normal, padrão, penalidade)

---

## Escopo

### Inclui
- **Período temporal**: Dois últimos anos fechados (anos civis completos)
- **Cobertura geográfica**: Brasil inteiro (todas as distribuidoras reguladas pela ANEEL)
- **Métricas**:
  - Tempo total de interrupção (minutos e horas) por distribuidora, agregado mensalmente
  - Número de eventos de interrupção por distribuidora e período
  - Indicadores regulatórios DEC e FEC com suas variantes
- **Granularidade final (Gold)**:
  - Agregado por distribuidora, ano e mês
  - Preservação de dados granulares em camadas anteriores (bronze, silver)

### Não Inclui
**[PENDÊNCIA]**: Delimitação de escopo "não inclui" não foi especificada no documento `definicoes_projeto.md`.  
Exemplos esperados:
- Interrupções de transmissão (fora do escopo de distribuição)?
- Dados de anos anteriores aos dois últimos fechados?
- Indicadores não relacionados à continuidade (DRP, DIC, etc.)?

---

## Métricas de Negócio

### Métricas Primárias

#### 1. Tempo Total de Interrupção
**Definição**: Somatório da duração de todas as interrupções ocorridas em um período (mês/ano) para uma distribuidora.  
**Unidades**: Minutos, Horas  
**Tabela**: `workspace.continuidade_aneel_gold.tempo_interrupcao_distribuidora_mes`  
**Campos**: `tempo_total_minutos`, `tempo_total_horas`  
**Fórmula**: `SUM(data_fim - data_inicio)` agregado por distribuidora, ano e mês  

**Interpretação de negócio**: Quanto maior o tempo, pior o desempenho da distribuidora na manutenção da continuidade do fornecimento.

#### 2. Total de Eventos de Interrupção
**Definição**: Número de ocorrências de interrupção registradas em um período para uma distribuidora.  
**Unidade**: Contagem  
**Tabela**: `workspace.continuidade_aneel_gold.tempo_interrupcao_distribuidora_mes`  
**Campo**: `total_eventos`  
**Fórmula**: `COUNT(*)` agregado por distribuidora, ano e mês  

**Interpretação de negócio**: Indica a frequência de falhas. Muitos eventos curtos podem ter impacto diferente de poucos eventos longos.

#### 3. DEC (Duração Equivalente de Interrupção por UC)
**Definição**: Tempo médio de interrupção por unidade consumidora, em horas, conforme metodologia da ANEEL.  
**Unidade**: Horas  
**Tabela**: `workspace.proj_aneel_cont_02_silver.indicadores_continuidade`  
**Campo**: `dec`  

**Interpretação de negócio**: Indicador regulatório de qualidade. Distribuidoras com DEC acima do limite estabelecido pela ANEEL podem sofrer penalidades.

#### 4. FEC (Frequência Equivalente de Interrupção por UC)
**Definição**: Número médio de interrupções por unidade consumidora, conforme metodologia da ANEEL.  
**Unidade**: Contagem  
**Tabela**: `workspace.proj_aneel_cont_02_silver.indicadores_continuidade`  
**Campo**: `fec`  

**Interpretação de negócio**: Indicador regulatório de frequência de interrupções. FEC alto indica instabilidade frequente no fornecimento.

### Métricas Secundárias (Variantes DEC/FEC)

Indicadores segmentados por tipo de causa:
- **INC** (Índice de Controle): Interrupções previsíveis e controláveis
- **IND** (Índice de Desempenho): Reflete desempenho operacional
- **INE** (Índice de Nível de Excelência): Meta de excelência
- **INO** (Índice Normal): Operação normal
- **IP** (Índice Padrão): Meta mínima regulatória
- **IPC** (Índice de Penalidade Contínua): Penalidades por descumprimento
- **XN**, **XNC**, **XP**, **XPC**: Indicadores excluindo causas específicas

**Uso de negócio**: Permitem análise de causas raízes, identificação de oportunidades de melhoria e separação de causas controláveis vs. incontroláveis.

---

## Consumidores da Informação

**[PENDÊNCIA]**: Consumidores dos dados não foram especificados no documento `definicoes_projeto.md`.  

**Consumidores esperados** (a confirmar):
- Equipe de Regulação/Compliance
- Gerentes de Operação e Manutenção
- Analistas de Qualidade de Fornecimento
- Área de Planejamento Estratégico
- Auditoria Interna
- Stakeholders externos (relatórios regulatórios, investidores)

**Casos de uso esperados**:
1. **Dashboards de monitoramento**: Acompanhamento em tempo quase-real do desempenho de continuidade
2. **Relatórios regulatórios**: Comprovação de conformidade com metas da ANEEL
3. **Análises exploratórias**: Identificação de padrões, causas de degradação, benchmarking entre distribuidoras
4. **Alertas e ações**: Gatilhos para intervenções operacionais quando limiares são ultrapassados

---

## Perguntas de Negócio Respondidas

1. **Qual distribuidora teve o maior tempo de interrupção no último ano?**  
   Tabela: `continuidade_aneel_gold.tempo_interrupcao_distribuidora_mes`  
   Métrica: `SUM(tempo_total_horas)` por distribuidora, filtrado por ano

2. **Qual a tendência mensal de interrupções para uma distribuidora específica?**  
   Tabela: `continuidade_aneel_gold.tempo_interrupcao_distribuidora_mes`  
   Visualização: Série temporal de `tempo_total_horas` por mês

3. **Quantos eventos de interrupção ocorreram em determinado mês?**  
   Tabela: `continuidade_aneel_gold.tempo_interrupcao_distribuidora_mes`  
   Métrica: `SUM(total_eventos)` por mês

4. **Qual o DEC médio das distribuidoras em determinado ano?**  
   Tabela: `proj_aneel_cont_02_silver.indicadores_continuidade`  
   Métrica: `AVG(dec)` por ano

5. **Quais distribuidoras estão acima do limite regulatório de DEC?**  
   Tabela: `proj_aneel_cont_02_silver.indicadores_continuidade`  
   Lógica: Comparar `dec` com limites (requer tabela de referência de limites por distribuidora)

6. **Qual a distribuição de interrupções por tipo (programada vs. emergencial)?**  
   Tabela: `continuidade_aneel_silver.interrupcoes_distribuicao`  
   Agregação: `COUNT(*)` por `tipo_interrupcao`

7. **Qual a origem mais comum de interrupções para uma distribuidora?**  
   Tabela: `continuidade_aneel_silver.interrupcoes_distribuicao`  
   Agregação: `COUNT(*)` por `origem_interrupcao`, filtrado por distribuidora

8. **Comparativo de FEC entre distribuidoras de uma região**  
   Tabela: `proj_aneel_cont_02_silver.indicadores_continuidade`  
   Métrica: `AVG(fec)` por distribuidora (requer tabela de mapeamento distribuidora-região)

---

## Glossário de Termos de Negócio

**ANEEL**: Agência Nacional de Energia Elétrica, órgão regulador do setor elétrico brasileiro.

**DEC**: Duração Equivalente de Interrupção por Unidade Consumidora. Expressa o tempo (em horas) que, em média, cada unidade consumidora ficou sem energia durante um período.

**FEC**: Frequência Equivalente de Interrupção por Unidade Consumidora. Expressa o número de vezes que, em média, cada unidade consumidora sofreu interrupção durante um período.

**DIC**: Duração de Interrupção Individual por Unidade Consumidora (indicador não incluído no escopo atual).

**FIC**: Frequência de Interrupção Individual por Unidade Consumidora (indicador não incluído no escopo atual).

**DMIC**: Duração Máxima de Interrupção Contínua por Unidade Consumidora (indicador não incluído no escopo atual).

**UC**: Unidade Consumidora. Ponto de conexão entre a rede de distribuição e o consumidor final.

**Conjunto de UCs**: Agrupamento de unidades consumidoras (por exemplo, todas as UCs atendidas por um alimentador ou subestação).

**Distribuidora**: Empresa concessionária responsável pela distribuição de energia elétrica em uma área de concessão específica.

**Interrupção programada**: Interrupção planejada e comunicada previamente aos consumidores, para manutenção ou obras.

**Interrupção emergencial**: Interrupção não planejada, causada por falhas imprevistas no sistema de distribuição.

**Subestação de distribuição**: Instalação elétrica que transforma e distribui energia para alimentadores.

**Alimentador**: Circuito elétrico que parte de uma subestação e alimenta um conjunto de consumidores.

**Fato gerador**: Causa específica que originou a interrupção (ex: queda de árvore, vandalismo, falha de equipamento).

---

## Status do Projeto

**Estado atual**: Projeto em operação (pronto)  
**Data de início**: Não especificada  
**Data de conclusão**: Não especificada  
**Período de dados disponíveis**: Dois últimos anos civis (presumidamente 2024 e 2025, data base: 07/set/2026)

---

## Pendências de Documentação

1. **Consumidores dos dados**: Definir quais áreas/pessoas acessam os dados e para quais finalidades
2. **Delimitação de escopo ("não inclui")**: Especificar explicitamente o que está fora do escopo do projeto
3. **Limites regulatórios**: Incorporar tabela de referência com limites DEC/FEC por distribuidora (se aplicável)
4. **Cadeia de atualização**: Periodicidade de execução dos notebooks, dependências e cronograma de refresh
5. **SLA de disponibilidade**: Tempo esperado para disponibilização dos dados após publicação pela ANEEL

---

**Data de criação**: 07/set/2026  
**Última atualização**: 07/set/2026  
**Responsável**: Documentação gerada automaticamente via Genie Code