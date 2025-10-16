"""Smart Sales Analyzer - ETL pipeline
Modern Data Engineering stack: DuckDB + Polars + Great Expectations +  Dagster """

import polars as pl
import duckdb
import logging
from pathlib import Path
from datetime import datetime
import kagglehub
from dotenv import load_dotenv
import os
import json
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import FileDataContext
from great_expectations.checkpoint import SimpleCheckpoint
import pyarrow.parquet as pq

#Load enviroments variables
load_dotenv()
os.environ["KAGGLE_USERNAME"] = os.getenv("KAGGLE_USERNAME")
os.environ["KAGGLE_KEY"] = os.getenv("KAGGLE_KEY")

#Configure logging 
logging.basicConfig(
    level = logging.INFO,
    format = '%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SalesETL:
    """Modern ETL Pipeline with Data Quality & OLAP capabilities"""
    def __init__(self, base_dir = "./data_warehouse"):
        self.base_dir = Path(base_dir)
        self.raw_dir = self.base_dir / "raw"
        self.staging_dir = self.base_dir / "staging"
        self.processed_dir = self.base_dir / "processed"
        self.warehouse_dir = self.base_dir / "warehouse"
        self.reports_dir = self.base_dir / "reports"

        # Create directory structure
        for dir_path in [self.base_dir, self.raw_dir, self.staging_dir,
                         self.processed_dir, self.warehouse_dir, self.reports_dir]:
            dir_path.mkdir(parents = True, exist_ok = True)
        
        self.df = None 
        self.duckdb_path = self.warehouse_dir / "sales_analytics.duckdb"
        self.conn = duckdb.connect(str(self.duckdb_path))
        logger.info(f"Initialized data warehouse at: {self.base_dir}")

    
    def download_from_kaggle(self):
        """Donwload dataset from Kaggle"""
        logger.info("Donwloading dataset from Kaggle...")
        path = kagglehub.dataset_download("rohitsahoo/sales-forecasting")
        csv_path = Path(path,"train.csv")

        if not csv_path.exists():
            raise FileNotFoundError(f"CSV not found: {csv_path}")
        
        #Copy to raw directory
        raw_csv = self.raw_dir / f"sales_raw_{datetime.now():%Y%m%d_%H%M%S}.csv"
        import shutil
        shutil.copy(csv_path, raw_csv)
        logging.info("Raw data saved to: {raw_csv}")
        return str(raw_csv)
    
    def transform_data(self, csv_path):
        """Transform: Load and transform data with Polars"""
        logger.info("Processing data with Polars...")

        self.df = pl.read_csv(csv_path, try_parse_dates=True)

        # Data Tranformations
        self.df = self.df.with_columns([pl.col("Order Date").str.to_date("%d/%m/%Y").alias("Order Date"),
                                        pl.col("Ship Date").str.to_date("%d/%m/%Y").alias("Ship Date")])
        self.df = self.df.with_columns([pl.col("Order Date").dt.year().alias("year"),
                                        pl.col("Order Date").dt.month().alias("month"),
                                        (pl.col("Ship Date") - pl.col("Order Date")).dt.total_days().alias("delivery_days"),
                                        pl.col("Sales").round(2).alias("Sales"),
                                        (pl.col("Sales") * 0.4).round(2).alias("profit_estimate"),
                                        pl.col("Order Date").dt.strftime("%Y-%m").alias("order_month")])

        logger.info(f"Loaded {len(self.df):,}rows with {len(self.df.columns)} columns")
        staging_file = self.staging_dir / "sales_stating.parquet"
        self.df.write_parquet(staging_file)
        logger.info(f"Staging data saved: {staging_file}")

    def 

    


    
    def show_summary(self):
        """Display data summary"""
        self.conn.register("sales",self.df)

        summary = self.conn.execute(""" 
            SELECT year,
                COUNT(*) as rows,
                ROUND(SUM(Sales),2) as revenue,
                COUNT(DISTINCT "Customer ID") as customers
            FROM sales
            GROUP BY year
            ORDER BY year
        """).fetchall()
        for s in summary:
            logger.info(f"Year {s[0]}: {s[1]:,} rows | ${s[2]:,.2f} revenue | {s[3]:,} customers")
        total = self.conn.execute("SELECT COUNT(*), ROUND(SUM(Sales),2) FROM sales").fetchone
        logger.info(f" Total: {total[0]:,} rows | ${total[1]:,.2f} revenue")

    def upload_to_s3(self,parquet_path, bucket, s3_prefix):
        """Upload Parquet files to S3"""
        logger.info("Uploading to S3...")

        s3 = boto3.client('s3')
        files = list(Path(parquet_path).glob("*.parquet"))

        for file in files:
            s3_key = f"{s3_prefix}/{file.name}"
            s3.upload_file(str(file), bucket, s3_key)
        
        logger.info(f"Uploaded {len(files)} files")
        return f"s3://{bucket}/{s3_prefix}"
    
    def create_glue_table(self, database, table, s3_location):
        """Create Glue table for Athena/QuickSight"""
        logging.info("Creating Glue table: {database}.{table}")
        glue = boto3.client('glue')

        columns = [
            {'Name': 'Row ID', 'Type':'bigint'},
            {'Name': 'Order ID', 'Type':'string'},
            {'Name': 'Order Date', 'Type': 'date'},
            {'Name': 'Ship Date', 'Type': 'date'},
            {'Name': 'Ship Mode', 'Type': 'string'},
            {'Name': 'Customer ID', 'Type': 'string'},
            {'Name': 'Customer Name', 'Type': 'string'},
            {'Name': 'Segment', 'Type': 'string'},
            {'Name': 'Country', 'Type': 'string'},
            {'Name': 'City', 'Type': 'string'},
            {'Name': 'State', 'Type': 'string'},
            {'Name': 'Postal Code', 'Type': 'double'},
            {'Name': 'Region', 'Type': 'string'},
            {'Name': 'Product ID', 'Type': 'string'},
            {'Name': 'Category', 'Type': 'string'},
            {'Name': 'Sub-Category', 'Type': 'string'},
            {'Name': 'Product Name', 'Type': 'string'},
            {'Name': 'Sales', 'Type': 'double'},
            {'Name': 'year', 'Type': 'bigint'}
        ]
        try:
            try:
                glue.create_database(DatabaseInput={'Name':database})
            except glue.exceptions.AlreadyExistsException:
                pass

            glue.create_table(
                DatabaseName=database,
                TableInput={
                    'Name':table,
                    'StorageDescriptor':{
                        'Columns': columns,
                        'Location': s3_location,
                        'InputFormat':'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                        'OutputFormat':'org.apache.hive.ql.io.parquet.MapredParquetOutputFormat',
                        'SerdeInfo': {
                            'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                        }
                    },
                    'TableType':'EXTERNAL_TABLE'
                }
            )
            logger.info("Glue table cerated successfully")
        except glue.exceptions.AlreadyExistsException:
            logger.info("Table already exists")
        except Exception as e:
            logger.error(f"Error: {e}")
    
    def run(self, bucket, s3_prefix= "data/sales", glue_db= "sales_db", glue_table="sales", show_summary=False):
        """Run complete pipeline"""
        start = datetime().now()
        logger.info("Starting ETL pipeline")

        csv_path = self.download_from_kaggle()
        self.load_data(csv_path)

        if show_summary:
            self.show_summary()

        parquet_path =  self.to_parquet()
        s3_location = self.upload_to_s3(parquet_path, bucket, s3_prefix)
        self.create_glue_table(glue_db, glue_table, s3_location)
        elapsed = (datetime.now() - start).total_seconds()
        logger.info(f"Pipeline completed in {elapsed:.1f}s")
        logger.info(f"QuickSight ready: {glue_db}.{glue_table}")

if __name__ == "__main__":
    
    S3_BUCKET = "my-sales-analytics_project_portfolio_123"
    S3_PREFIX = "data/sales_by_year"
    GLUE_DATABASE = "sales_db"
    GLUE_TABLE = "sales"

    etl = SalesETL()
    etl.run( bucket= S3_BUCKET, s3_prefix= S3_PREFIX,
             glue_db= GLUE_DATABASE,
             glue_table= GLUE_TABLE,
             show_summary= False
    )
