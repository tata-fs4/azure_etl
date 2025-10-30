"""Funções de Data Quality utilizadas pelo pipeline ETL.

As verificações aqui implementadas são propositalmente simples para
demonstrar como regras de qualidade podem ser centralizadas e
reutilizadas. Em um ambiente Azure, estas validações poderiam ser
executadas em notebooks Databricks, em stored procedures do Synapse ou
mesmo via regras do Data Quality Services.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence

import pandas as pd


class DataQualityError(RuntimeError):
    """Erro lançado quando uma verificação de qualidade falha."""


@dataclass(frozen=True)
class QualityCheckResult:
    name: str
    passed: bool
    details: str | None = None


def _format_columns(columns: Sequence[str]) -> str:
    return ", ".join(columns)


def check_not_null(df: pd.DataFrame, columns: Sequence[str]) -> QualityCheckResult:
    """Verifica se as colunas informadas não possuem valores nulos."""

    null_counts = df[columns].isna().sum()
    offending = null_counts[null_counts > 0]
    if offending.empty:
        return QualityCheckResult(
            name=f"not_null:{_format_columns(columns)}",
            passed=True,
        )

    details = ", ".join(f"{col}={count}" for col, count in offending.items())
    return QualityCheckResult(
        name=f"not_null:{_format_columns(columns)}",
        passed=False,
        details=f"Valores nulos encontrados: {details}",
    )


def check_unique(df: pd.DataFrame, columns: Sequence[str]) -> QualityCheckResult:
    """Garante que a combinação de colunas é única no dataframe."""

    duplicated = df.duplicated(subset=list(columns))
    if not duplicated.any():
        return QualityCheckResult(
            name=f"unique:{_format_columns(columns)}",
            passed=True,
        )

    details = f"Foram encontradas {duplicated.sum()} linhas duplicadas"
    return QualityCheckResult(
        name=f"unique:{_format_columns(columns)}",
        passed=False,
        details=details,
    )


def check_foreign_key(
    fact_df: pd.DataFrame,
    dimension_df: pd.DataFrame,
    fact_column: str,
    dimension_column: str,
) -> QualityCheckResult:
    """Valida relacionamentos entre fato e dimensão."""

    missing_keys = set(fact_df[fact_column]).difference(dimension_df[dimension_column])
    if not missing_keys:
        return QualityCheckResult(
            name=f"fk:{fact_column}->{dimension_column}",
            passed=True,
        )

    details = f"Chaves inexistentes na dimensão: {sorted(missing_keys)[:5]}"
    return QualityCheckResult(
        name=f"fk:{fact_column}->{dimension_column}",
        passed=False,
        details=details,
    )


def ensure_quality(results: Iterable[QualityCheckResult]) -> None:
    """Lança um erro se alguma verificação falhar."""

    failed = [result for result in results if not result.passed]
    if not failed:
        return

    formatted = "; ".join(
        f"{result.name}: {result.details or 'sem detalhes'}" for result in failed
    )
    raise DataQualityError(f"Falhas de qualidade detectadas: {formatted}")


def run_quality_checks(
    dim_customer: pd.DataFrame,
    dim_product: pd.DataFrame,
    dim_store: pd.DataFrame,
    dim_date: pd.DataFrame,
    fact_sales: pd.DataFrame,
) -> None:
    """Executa as principais validações do star schema."""

    results = [
        check_unique(dim_customer, ["customer_code"]),
        check_unique(dim_product, ["product_code"]),
        check_unique(dim_store, ["store_code"]),
        check_unique(dim_date, ["date_key"]),
        check_not_null(fact_sales, [
            "sale_id",
            "date_sk",
            "store_sk",
            "product_sk",
            "customer_sk",
            "order_datetime",
        ]),
        check_foreign_key(fact_sales, dim_customer, "customer_sk", "customer_sk"),
        check_foreign_key(fact_sales, dim_product, "product_sk", "product_sk"),
        check_foreign_key(fact_sales, dim_store, "store_sk", "store_sk"),
        check_foreign_key(fact_sales, dim_date, "date_sk", "date_sk"),
    ]

    ensure_quality(results)
