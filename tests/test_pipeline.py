from __future__ import annotations

import shutil
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT.parent))

from azure_etl_project.etl.pipeline import (  # noqa: E402  pylint: disable=wrong-import-position
    PipelinePaths,
    extract_raw_sources,
    load_curated_tables,
    transform_to_star_schema,
)


def _copy_project_structure(tmp_dir: Path, project_root: Path) -> PipelinePaths:
    """Replica as pastas necessárias para executar o pipeline em isolamento."""

    data_src = project_root / "data"
    data_dst = tmp_dir / "data"
    shutil.copytree(data_src, data_dst)

    output_dir = tmp_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    return PipelinePaths(root=tmp_dir)


def test_transform_to_star_schema_creates_expected_rows(tmp_path: Path) -> None:
    project_root = PROJECT_ROOT
    paths = _copy_project_structure(tmp_path, project_root)

    datasets = extract_raw_sources(paths)
    dim_customer, dim_product, dim_store, dim_date, fact_sales = transform_to_star_schema(
        datasets
    )

    assert len(dim_customer) == 10
    assert len(dim_product) == 10
    assert len(dim_store) == 10
    assert len(dim_date) == 7  # datas distintas nas vendas
    assert len(fact_sales) == len(datasets["sales"])

    # Qualidade do modelo dimensional
    assert dim_customer["full_name"].str.contains(" ").all()
    assert (fact_sales["gross_amount"] >= fact_sales["net_amount"]).all()


def test_load_curated_tables_persists_outputs(tmp_path: Path) -> None:
    project_root = PROJECT_ROOT
    paths = _copy_project_structure(tmp_path, project_root)

    datasets = extract_raw_sources(paths)
    tables = transform_to_star_schema(datasets)
    dim_customer, dim_product, dim_store, dim_date, fact_sales = tables

    load_curated_tables(
        paths,
        {
            "dim_customer": dim_customer,
            "dim_product": dim_product,
            "dim_store": dim_store,
            "dim_date": dim_date,
            "fact_sales": fact_sales,
        },
    )

    curated_dir = paths.curated
    expected_files = {
        "dim_customer.csv",
        "dim_product.csv",
        "dim_store.csv",
        "dim_date.csv",
        "fact_sales.csv",
    }

    assert {f.name for f in curated_dir.iterdir()} == expected_files

    fact_df = pd.read_csv(curated_dir / "fact_sales.csv")
    assert set(fact_df.columns) == {
        "sale_sk",
        "sale_id",
        "date_sk",
        "store_sk",
        "product_sk",
        "customer_sk",
        "order_datetime",
        "quantity",
        "unit_price",
        "discount",
        "gross_amount",
        "net_amount",
        "payment_type",
        "channel",
    }

    assert fact_df["gross_amount"].round(2).equals(fact_df["gross_amount"])
