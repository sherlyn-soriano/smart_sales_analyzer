# Smart Sales Analyzer

Local analytics sandbox that blends a Kaggle sales dataset with synthetic records, runs automated data-quality checks, and persists everything into DuckDB for downstream reporting via Streamlit.

## What You Get
- **Modern ETL**: Polars + DuckDB pipeline with Great Expectations validation.
- **Flexible sourcing**: Pulls the Kaggle dataset (or reuses the cached copy) and fills gaps with synthetic data.
- **Ready-to-query warehouse**: Materialised tables in `data/sales_analytics.duckdb`.
- **Interactive reporting**: Streamlit dashboard for quick demos.

## Quickstart

```bash
# 1) Clone and enter the project
cd smart_sales_analyzer

# 2) Create/activate a virtual environment (optional but recommended)
python3 -m venv .venv
source .venv/bin/activate

# 3) Install dependencies
pip install -r requirements.txt

# 4) (Optional) Export Kaggle credentials if you want to re-download the dataset
export KAGGLE_USERNAME=your_username
export KAGGLE_KEY=your_key
```

## Run the ETL

```bash
source .venv/bin/activate
python src/etl.py
```

Outputs land in `data/`:
- `train.csv` – cleansed Kaggle source (cached copy if Kaggle API is unavailable)
- `sales_enriched.parquet` – blended dataset with synthetic augmentation
- `regional_revenue.csv`, `segment_yearly.csv`, `top_products.csv`, `yearly.csv`
- `quality_report.json` – Great Expectations results
- `sales_analytics.duckdb` – DuckDB file with analytics tables
- `etl_pipeline.log` – run history

## Launch the Streamlit Dashboard

```bash
source .venv/bin/activate
streamlit run src/dashboard.py
```

Use the URL printed in the terminal to open the interactive report (filters, charts, KPIs).

## Explore DuckDB Tables via CLI

```bash
duckdb data/sales_analytics.duckdb
```

Inside the prompt:

```sql
SHOW TABLES;
DESCRIBE analytics.sales;
SELECT order_year, SUM(sales) AS revenue
FROM analytics.sales
GROUP BY 1
ORDER BY 1;
```

Exit with `QUIT;`.  
Capture these commands/output for your project documentation or demo.

## Repository Layout

```
├── data/                      # Materialised outputs (created after running ETL)
│   ├── sales_analytics.duckdb
│   ├── sales_enriched.parquet
│   ├── *.csv (summary tables)
│   └── quality_report.json
├── src/
│   ├── etl.py                 # SalesETL pipeline
│   ├── dashboard.py           # Streamlit app
│   └── synthetic_data_generator.py
├── requirements.txt
└── README.md
```

## Suggested Screenshots / Images
For your GitHub repo or HR demo, consider adding:
1. **Pipeline run snippet** – terminal showing `python src/etl.py` completion and “Data quality checks success=True”.
2. **Streamlit dashboard** – screenshot of key charts/metrics.
3. **DuckDB query** – CLI output of `SELECT …` to show the analytics tables.
4. *(Optional)* A lightweight architecture diagram (ETL → DuckDB → Streamlit) if you want a visual.

Add these images under a `docs/` folder (e.g., `docs/streamlit-dashboard.png`) and reference them in the README once captured.

---

Questions or future ideas? Consider adding forecasting notebooks, CI checks around the ETL, or parametrised Great Expectations suites. Have fun showcasing your analytics pipeline!
