"""ETL pipeline de demonstração para um fluxo analítico em Azure.

O script extrai arquivos CSV "raw" simulando um Data Lake Bronze,
padroniza os dados para uma zona Silver e publica dimensões e fatos em
formato Star Schema na zona Gold. A execução local usa apenas pandas,
mas o desenho permite orquestração por Azure Data Factory ou Synapse
pipelines.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd

if __package__ in (None, ""):
    import sys

    sys.path.append(str(Path(__file__).resolve().parent))
    from quality_checks import run_quality_checks  # type: ignore  # noqa: E402
else:
    from .quality_checks import run_quality_checks

@dataclass(frozen=True)
class PipelinePaths:
    root: Path

    @property
    def raw(self) -> Path:
        return self.root / "data" / "raw"

    @property
    def reference(self) -> Path:
        return self.root / "data" / "reference"

    @property
    def staging(self) -> Path:
        return self.root / "output" / "staging"

    @property
    def curated(self) -> Path:
        return self.root / "output" / "curated"


# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------

def extract_raw_sources(paths: PipelinePaths) -> Dict[str, pd.DataFrame]:
    """Lê os CSVs da zona raw.

    Em produção, estes caminhos poderiam apontar para um Azure Data Lake
    Storage Gen2 montado via abfss://. Para simplificar a demonstração,
    utilizamos arquivos locais.
    """

    datasets = {
        "customers": pd.read_csv(paths.raw / "customers.csv"),
        "products": pd.read_csv(paths.raw / "products.csv"),
        "stores": pd.read_csv(paths.raw / "stores.csv"),
        "sales": pd.read_csv(paths.raw / "sales.csv"),
    }

    # Tabelas de referência podem residir em outro container (Silver ou
    # Azure SQL). Neste exemplo elas permanecem locais.
    datasets["calendar"] = pd.read_csv(paths.reference / "calendar.csv")
    return datasets


# ---------------------------------------------------------------------------
# Transform helpers
# ---------------------------------------------------------------------------

def _add_surrogate_key(df: pd.DataFrame, key_name: str) -> pd.DataFrame:
    df = df.copy()
    df.insert(0, key_name, range(1, len(df) + 1))
    return df


def build_dim_customer(customers: pd.DataFrame) -> pd.DataFrame:
    dim = customers.rename(
        columns={
            "customer_id": "customer_code",
            "first_name": "first_name",
            "last_name": "last_name",
            "email": "email",
            "city": "city",
            "state": "state",
            "loyalty_tier": "loyalty_tier",
            "signup_date": "signup_date",
        }
    )
    dim["full_name"] = dim["first_name"] + " " + dim["last_name"]
    dim = dim[[
        "customer_code",
        "full_name",
        "email",
        "city",
        "state",
        "loyalty_tier",
        "signup_date",
    ]]
    dim = _add_surrogate_key(dim, "customer_sk")
    return dim


def build_dim_product(products: pd.DataFrame) -> pd.DataFrame:
    dim = products.rename(
        columns={
            "product_id": "product_code",
            "product_name": "product_name",
            "category": "category",
            "sub_category": "sub_category",
            "brand": "brand",
            "unit_cost": "unit_cost",
        }
    )
    dim["premium_flag"] = dim["unit_cost"] >= 9.0
    dim = dim[[
        "product_code",
        "product_name",
        "category",
        "sub_category",
        "brand",
        "unit_cost",
        "premium_flag",
    ]]
    dim = _add_surrogate_key(dim, "product_sk")
    return dim


def build_dim_store(stores: pd.DataFrame) -> pd.DataFrame:
    dim = stores.rename(
        columns={
            "store_id": "store_code",
            "store_name": "store_name",
            "city": "city",
            "state": "state",
            "region": "region",
            "store_format": "store_format",
            "opening_date": "opening_date",
        }
    )
    dim = _add_surrogate_key(dim, "store_sk")
    return dim[[
        "store_sk",
        "store_code",
        "store_name",
        "city",
        "state",
        "region",
        "store_format",
        "opening_date",
    ]]


def build_dim_date(calendar: pd.DataFrame, sales: pd.DataFrame) -> pd.DataFrame:
    calendar = calendar.copy()
    calendar["full_date"] = pd.to_datetime(calendar["full_date"])

    # Garante cobertura das datas presentes em vendas
    sales_dates = pd.to_datetime(sales["order_datetime"]).dt.date
    sales_calendar = pd.DataFrame({"full_date": pd.to_datetime(sales_dates)})
    sales_calendar["date_key"] = sales_calendar["full_date"].dt.strftime("%Y%m%d").astype(int)
    sales_calendar["day"] = sales_calendar["full_date"].dt.day
    sales_calendar["month"] = sales_calendar["full_date"].dt.month
    month_name_pt = {
        1: "Janeiro",
        2: "Fevereiro",
        3: "Março",
        4: "Abril",
        5: "Maio",
        6: "Junho",
        7: "Julho",
        8: "Agosto",
        9: "Setembro",
        10: "Outubro",
        11: "Novembro",
        12: "Dezembro",
    }
    sales_calendar["month_name"] = sales_calendar["full_date"].dt.month.map(month_name_pt)
    sales_calendar["quarter"] = sales_calendar["full_date"].dt.quarter
    sales_calendar["year"] = sales_calendar["full_date"].dt.year
    sales_calendar["is_weekend"] = sales_calendar["full_date"].dt.weekday >= 5

    calendar = pd.concat([calendar, sales_calendar], ignore_index=True)
    calendar = calendar.drop_duplicates(subset="date_key")
    calendar = calendar.sort_values("date_key").reset_index(drop=True)
    calendar["is_weekend"] = calendar["is_weekend"].astype(int)
    calendar = calendar[[
        "date_key",
        "full_date",
        "day",
        "month",
        "month_name",
        "quarter",
        "year",
        "is_weekend",
    ]]
    calendar = _add_surrogate_key(calendar, "date_sk")
    return calendar


def build_fact_sales(
    sales: pd.DataFrame,
    dim_customer: pd.DataFrame,
    dim_product: pd.DataFrame,
    dim_store: pd.DataFrame,
    dim_date: pd.DataFrame,
) -> pd.DataFrame:
    # Converte datas e cria chaves
    sales = sales.copy()
    sales["order_datetime"] = pd.to_datetime(sales["order_datetime"])
    sales["date_key"] = sales["order_datetime"].dt.strftime("%Y%m%d").astype(int)

    fact = sales.merge(dim_customer[["customer_sk", "customer_code"]],
                       left_on="customer_id", right_on="customer_code")
    fact = fact.merge(dim_product[["product_sk", "product_code"]],
                      left_on="product_id", right_on="product_code")
    fact = fact.merge(dim_store[["store_sk", "store_code"]],
                      left_on="store_id", right_on="store_code")
    fact = fact.merge(dim_date[["date_sk", "date_key"]], on="date_key")

    fact["gross_amount"] = (fact["quantity"] * fact["unit_price"]).round(2)
    fact["net_amount"] = (fact["gross_amount"] - fact["discount"]).round(2)

    fact = fact[[
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
    ]]

    fact = _add_surrogate_key(fact, "sale_sk")
    return fact


# ---------------------------------------------------------------------------
# Transform orchestration
# ---------------------------------------------------------------------------

def transform_to_star_schema(datasets: Dict[str, pd.DataFrame]) -> Tuple[pd.DataFrame, ...]:
    customers = datasets["customers"]
    products = datasets["products"]
    stores = datasets["stores"]
    sales = datasets["sales"]
    calendar = datasets["calendar"]

    dim_customer = build_dim_customer(customers)
    dim_product = build_dim_product(products)
    dim_store = build_dim_store(stores)
    dim_date = build_dim_date(calendar, sales)
    fact_sales = build_fact_sales(sales, dim_customer, dim_product, dim_store, dim_date)

    run_quality_checks(dim_customer, dim_product, dim_store, dim_date, fact_sales)

    return dim_customer, dim_product, dim_store, dim_date, fact_sales


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------

def load_curated_tables(paths: PipelinePaths, tables: Dict[str, pd.DataFrame]) -> None:
    paths.staging.mkdir(parents=True, exist_ok=True)
    paths.curated.mkdir(parents=True, exist_ok=True)

    # Exporta CSVs na zona curated. Em projetos reais recomenda-se Parquet.
    for name, df in tables.items():
        df.to_csv(paths.curated / f"{name}.csv", index=False)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def run_pipeline(root_path: Path) -> None:
    paths = PipelinePaths(root=root_path)
    datasets = extract_raw_sources(paths)

    dim_customer, dim_product, dim_store, dim_date, fact_sales = transform_to_star_schema(datasets)

    tables = {
        "dim_customer": dim_customer,
        "dim_product": dim_product,
        "dim_store": dim_store,
        "dim_date": dim_date,
        "fact_sales": fact_sales,
    }

    load_curated_tables(paths, tables)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Executa o pipeline ETL localmente.")
    parser.add_argument(
        "--project-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="Caminho raiz do projeto (default: raiz do repo)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_pipeline(args.project_root)
