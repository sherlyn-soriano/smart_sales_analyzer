"""Smart Sales Analyzer - Local ETL pipeline with modern data stack touches."""

from __future__ import annotations

import logging
import json
from pathlib import Path
from typing import Dict, Optional

import duckdb
import polars as pl
from dotenv import load_dotenv
import kagglehub
import os
from great_expectations.dataset import PandasDataset

from synthetic_data_generator import SyntheticDataGenerator

# Configure logging once at module import
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("etl_pipeline.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


class SalesETL:
    """Local ETL pipeline that enriches data and produces analytical summaries."""

    def __init__(self, data_dir: str | Path = "data") -> None:
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.synthetic_generator = SyntheticDataGenerator()
        self.df: Optional[pl.DataFrame] = None
        self.warehouse_path = self.data_dir / "sales_analytics.duckdb"
        self._setup_kaggle_credentials()
        logger.debug("SalesETL initialized with data directory: %s", self.data_dir.resolve())

    # Data sourcing
    def load_base_dataset(self, csv_path: Optional[str | Path] = None) -> Optional[pl.DataFrame]:
        """Load the original dataset if it exists, otherwise return None."""
        candidates = []
        if csv_path is not None:
            candidates.append(Path(csv_path))
        candidates.extend(
            [
                self.data_dir / "train.csv",
                self.data_dir / "sales.csv",
                self.data_dir / "sales_raw.csv",
            ]
        )

        for path in candidates:
            if path.exists():
                logger.debug("Loading base dataset from %s", path)
                return pl.read_csv(path, try_parse_dates=True)

        logger.info("Base dataset not found; proceeding with fully synthetic data")
        return None

    def _setup_kaggle_credentials(self) -> None:
        """Load Kaggle credentials from .env (if present)."""
        load_dotenv()
        username = os.getenv("KAGGLE_USERNAME")
        key = os.getenv("KAGGLE_KEY")
        if username and key:
            os.environ["KAGGLE_USERNAME"] = username
            os.environ["KAGGLE_KEY"] = key
            logger.debug("Kaggle credentials loaded from environment.")
        else:
            logger.warning("Kaggle credentials not found; dataset download may fail.")

    def download_kaggle_dataset(
        self,
        dataset_slug: str = "rohitsahoo/sales-forecasting",
        force: bool = False,
    ) -> Optional[Path]:
        """Download dataset from Kaggle into the data directory."""
        target_csv = self.data_dir / "train.csv"
        if target_csv.exists() and not force:
            logger.debug("Existing Kaggle dataset found at %s; skipping download.", target_csv)
            return target_csv

        try:
            logger.info("Downloading dataset %s into %s", dataset_slug, self.data_dir)
            download_path = Path(
                kagglehub.dataset_download(dataset_slug, path=str(self.data_dir))
            )
            csv_path = download_path / "train.csv"
            if csv_path.exists():
                csv_path.replace(target_csv)
                logger.info("Dataset downloaded to %s", target_csv)
                return target_csv
            logger.warning("train.csv not found after download.")
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Kaggle download failed: %s", exc)
        return None

    def build_dataset(
        self,
        base_df: Optional[pl.DataFrame],
        num_synthetic_rows: int = 10_000,
        **kwargs,
    ) -> pl.DataFrame:
        """Combine base data with synthetic rows (or generate synthetic from scratch)."""
        if base_df is None or len(base_df) == 0:
            logger.info("Generating synthetic dataset with %s rows", num_synthetic_rows)
            return self.synthetic_generator.generate_synthetic_data(
                num_rows=num_synthetic_rows, **kwargs
            )

        logger.info(
            "Augmenting base dataset (%s rows) with %s synthetic rows",
            len(base_df),
            num_synthetic_rows,
        )
        return self.synthetic_generator.augment_dataframe(
            base_df, num_synthetic_rows=num_synthetic_rows, **kwargs
        )


    # Transformations

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """Clean column names, enforce types, and derive analytical columns."""
        rename_map: Dict[str, str] = {
            col: col.strip().lower().replace(" ", "_").replace("-", "_")
            for col in df.columns
        }
        transformed = df.rename(rename_map)

        with_columns = []

        if "order_date" in transformed.columns:
            with_columns.append(pl.col("order_date").cast(pl.Date))
            with_columns.append(pl.col("order_date").dt.year().alias("order_year"))
            with_columns.append(pl.col("order_date").dt.month().alias("order_month"))

        if "ship_date" in transformed.columns:
            with_columns.append(pl.col("ship_date").cast(pl.Date))

        if "sales" in transformed.columns:
            with_columns.append(pl.col("sales").cast(pl.Float64))

        if "postal_code" in transformed.columns:
            with_columns.append(pl.col("postal_code").cast(pl.Utf8))

        if with_columns:
            transformed = transformed.with_columns(with_columns)

        self.df = transformed
        logger.debug("Transformed dataset with %s rows and %s columns", len(transformed), len(transformed.columns))
        return transformed

    # Reporting

    def build_summaries(self, df: pl.DataFrame) -> Dict[str, pl.DataFrame]:
        """Create summary tables for analytics and reporting."""
        summaries: Dict[str, pl.DataFrame] = {}

        if "order_year" in df.columns:
            summaries["yearly"] = (
                df.group_by("order_year")
                .agg(
                    pl.count().alias("rows"),
                    pl.col("sales").sum().round(2).alias("revenue"),
                    pl.col("customer_id").n_unique().alias("unique_customers"),
                )
                .sort("order_year")
            )

        if {"order_year", "segment"}.issubset(df.columns):
            summaries["segment_yearly"] = (
                df.group_by(["order_year", "segment"])
                .agg(pl.col("sales").sum().round(2).alias("revenue"))
                .sort(["order_year", "revenue"], descending=[False, True])
            )

        if "region" in df.columns:
            summaries["regional_revenue"] = (
                df.group_by("region")
                .agg(pl.col("sales").sum().round(2).alias("revenue"))
                .sort("revenue", descending=True)
            )

        if "product_name" in df.columns:
            summaries["top_products"] = (
                df.group_by("product_name")
                .agg(
                    pl.col("sales").sum().round(2).alias("revenue"),
                    pl.count().alias("orders"),
                )
                .sort("revenue", descending=True)
                .head(15)
            )

        logger.debug("Built %s summary tables", len(summaries))
        return summaries

    def run_quality_checks(self, df: pl.DataFrame) -> Dict[str, bool]:
        """Run lightweight Great Expectations checks on critical columns."""
        dataset = PandasDataset(df.to_pandas())
        expectations: Dict[str, bool] = {}

        for column in ["order_id", "customer_id", "sales", "order_date"]:
            if column in dataset.columns:
                result = dataset.expect_column_values_to_not_be_null(column)
                expectations[f"{column}_not_null"] = bool(result.success)

        if "sales" in dataset.columns:
            result = dataset.expect_column_values_to_be_between("sales", min_value=0, strict_min=True)
            expectations["sales_positive"] = bool(result.success)

        if "order_year" in dataset.columns:
            result = dataset.expect_column_values_to_be_between(
                "order_year",
                min_value=int(df["order_year"].min()),
                max_value=int(df["order_year"].max()),
            )
            expectations["order_year_valid_range"] = bool(result.success)

        expectations["overall_success"] = all(expectations.values())
        logger.info("Data quality checks success=%s", expectations["overall_success"])
        return expectations

    # Persistence
    def persist_outputs(
        self,
        df: pl.DataFrame,
        summaries: Dict[str, pl.DataFrame],
        quality_results: Dict[str, bool],
    ) -> None:
        """Persist the dataset, summary tables, and quality report under the data directory."""
        dataset_path = self.data_dir / "sales_enriched.parquet"
        df.write_parquet(dataset_path)
        logger.info("Saved enriched dataset to %s", dataset_path)

        for name, table in summaries.items():
            output_path = self.data_dir / f"{name}.csv"
            table.write_csv(output_path)
            logger.info("Saved %s summary to %s", name, output_path)

        quality_path = self.data_dir / "quality_report.json"
        quality_path.write_text(json.dumps(quality_results, indent=2))
        logger.info("Saved data quality report to %s", quality_path)

    def load_into_warehouse(
        self,
        df: pl.DataFrame,
        summaries: Dict[str, pl.DataFrame],
    ) -> None:
        """Load datasets into DuckDB for interactive analytics."""
        with duckdb.connect(str(self.warehouse_path)) as conn:
            conn.execute("CREATE SCHEMA IF NOT EXISTS analytics")
            conn.register("sales_dataset", df.to_arrow())
            conn.execute(
                "CREATE OR REPLACE TABLE analytics.sales AS SELECT * FROM sales_dataset"
            )
            conn.unregister("sales_dataset")

            for name, table in summaries.items():
                view_name = f"{name}_summary"
                conn.register(view_name, table.to_arrow())
                conn.execute(
                    f"CREATE OR REPLACE TABLE analytics.{view_name} AS SELECT * FROM {view_name}"
                )
                conn.unregister(view_name)

        logger.info("Persisted analytics tables into %s", self.warehouse_path)

    # Entry point
    def run(
        self,
        csv_path: Optional[str | Path] = None,
        num_synthetic_rows: int = 5_000,
        **kwargs,
    ) -> pl.DataFrame:
        """Execute the full pipeline."""
        if csv_path is None:
            csv_candidate = self.download_kaggle_dataset()
        else:
            csv_candidate = Path(csv_path)

        base_df = self.load_base_dataset(csv_candidate)
        combined_df = self.build_dataset(base_df, num_synthetic_rows=num_synthetic_rows, **kwargs)
        transformed_df = self.transform(combined_df)
        summaries = self.build_summaries(transformed_df)
        quality_results = self.run_quality_checks(transformed_df)
        self.persist_outputs(transformed_df, summaries, quality_results)
        self.load_into_warehouse(transformed_df, summaries)

        return transformed_df


if __name__ == "__main__":
    etl = SalesETL(data_dir="data")
    etl.run()
