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
