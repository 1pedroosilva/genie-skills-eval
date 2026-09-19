# Decisões Arquiteturais: Continuidade ANEEL

### DEC-001 -- Catálogo e schemas UC

**Data:** 2026-09-05
**Contexto:** Necessidade de definir onde as tabelas do projeto seriam armazenadas no Unity Catalog.
**Decisão:** Utilizar o catálogo `workspace` com três schemas seguindo o padrão `proj_aneel_cont_XX_camada`: `proj_aneel_cont_01_bronze`, `proj_aneel_cont_02_silver`, `proj_aneel_cont_03_gold`.
**Alternativas consideradas:** Outros catálogos disponíveis no workspace.
**Justificativa:** Catálogo `workspace` é o catálogo padrão com permissão de criação de schema confirmada.

### DEC-002 -- Estrutura do pipeline

**Data:** 2026-09-05
**Contexto:** Necessidade de definir o fluxo de processamento para comparar interrupções observadas com o DEC apurado pela ANEEL.
**Decisão:** Pipeline com 7 notebooks em duas linhagens que convergem no gold: linhagem de interrupções (101 → 201 → 301) e linhagem de indicadores (102 → 202 → 302), convergindo no notebook 303_comparativo_dec.
**Alternativas consideradas:** Notebook único de comparação lendo diretamente do bronze; separação em mais etapas intermediárias.
**Justificativa:** Duas linhagens independentes até o gold permitem processamento paralelo e rastreabilidade. A convergência no gold isola a lógica de comparação dos dados de cada fonte.

### DEC-003 -- Estratégia de gravação bronze idempotente

**Data:** 2026-09-06
**Contexto:** Bronze em arquitetura medalhão é tradicionalmente append-only para preservar histórico completo de ingestão, mas isso não é idempotente por design. Para garantir que reprocessamentos não dupliquem dados, é necessário adicionar verificação prévia.
**Decisão:** Implementar APPEND com verificação prévia via tabela de controle de ingestão. Antes de processar cada ano, verificar na tabela de controle se já foi processado com o mesmo Last-Modified HTTP da fonte. Se sim, pular. Se não, processar e registrar na tabela de controle.
**Alternativas consideradas:** APPEND puro sem controle (não idempotente), MERGE por chave natural (adiciona complexidade desnecessária no bronze), replaceWhere por ano (perde histórico de versões da fonte).
**Justificativa:** Verificação prévia garante idempotência sem perder histórico completo. Tabela de controle rastreia quando cada versão da fonte foi ingerida, detecta quando a ANEEL atualiza arquivo histórico, e previne duplicação em reprocessamentos. Alinha com o princípio de bronze como camada de captura fiel da fonte.

### DEC-004 -- Estratégia DELETE+APPEND para indicadores coletivos

**Data:** 2026-09-06
**Contexto:** Notebook 102_indicadores_continuidade ingere indicadores coletivos (DEC, FEC) da ANEEL. Diferentemente do notebook 101 (interrupções), esta fonte tem características distintas: arquivos menores, sem histórico de versionamento pela fonte (ANEEL não republica anos passados com correções), e granularidade anual por distribuidora.
**Decisão:** Implementar DELETE WHERE + APPEND por ano. Remove período sendo reprocessado, depois insere nova versão. Não utiliza tabela de controle de ingestão.
**Alternativas consideradas:** APPEND com tabela de controle (DEC-003), MERGE por chave natural, replaceWhere por ano.
**Justificativa:** DELETE+APPEND é mais simples e suficiente quando a fonte não versiona arquivos históricos. Evita overhead de tabela de controle auxiliar e verificação de Last-Modified HTTP. Mantém idempotência: rodar N vezes produz mesmo resultado. Estratégia adequada para ingestões batch periódicas de dados organizados em períodos fechados (ano).

### DEC-005 -- Chave natural da tabela silver de interrupções

**Data:** 2026-09-07
**Contexto:** A tabela silver `interrupcoes_distribuicao` precisava de uma chave natural que garantisse unicidade de cada evento de interrupção, permitindo deduplicação correta e rastreabilidade.
**Decisão:** Chave natural composta por quatro campos: `cod_distribuidora`, `data_inicio`, `conjunto` (código do conjunto de unidades consumidoras), `tipo_interrupcao`. Essa chave identifica univocamente cada evento de interrupção registrado pela ANEEL.
**Alternativas consideradas:** Chave simples apenas com código do conjunto; chave sem data de início; chave com hash dos campos.
**Justificativa:** A chave composta garante unicidade real dos eventos, pois um mesmo conjunto pode ter múltiplas interrupções em datas diferentes, e múltiplos conjuntos de uma distribuidora podem ter interrupções simultâneas. A inclusão de `tipo_interrupcao` diferencia interrupções programadas de emergenciais no mesmo conjunto e horário. Campos explícitos (sem hash) facilitam joins e análises posteriores.
