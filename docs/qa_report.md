# Relatório de QA - Projeto Azure ETL

## Objetivo

Validar a integridade do pipeline de demonstração, garantindo que as
transformações em pandas reflitam um star schema consistente e
reprodutível. O relatório consolida as verificações automáticas e
manuais executadas.

## Escopo das Verificações

1. **Validações de Data Quality no pipeline**
   - Chaves naturais exclusivas nas dimensões (`quality_checks.run_quality_checks`).
   - Integridade referencial entre fato e dimensões.
   - Presença de colunas obrigatórias na tabela fato.
2. **Testes automatizados (pytest)**
   - `test_transform_to_star_schema_creates_expected_rows`
     garante o volume de linhas por dimensão e consistência de métricas.
   - `test_load_curated_tables_persists_outputs` assegura a escrita das
     tabelas `curated` com colunas corretas e valores monetários
     arredondados.
3. **Execução do pipeline em ambiente isolado**
   - `python etl/pipeline.py` gera as tabelas `gold` após aprovação das
     checagens de qualidade.

## Resultado

- Todas as validações internas foram executadas sem falhas.
- A suíte `pytest` passou com 100% de sucesso.
- A geração das tabelas `dim_*` e `fact_sales` apresenta números
  arredondados com precisão de duas casas decimais, evitando ruídos
  flutuantes.

## Próximos Passos Sugeridos

- Evoluir as verificações de Data Quality com regras adicionais (por
  exemplo, limites mínimos/máximos de preço e quantidade).
- Integrar as validações em pipelines do Azure Data Factory/Synapse com
  alertas automatizados.
- Versionar artefatos de dados intermediários (zona *staging*) para
  auditoria.
