"""Smart Sales Analyzer - ETL pipeline
Download sales CSV data then convert into to parquet and uploads to S3 for QuickSight analysis"""

import pandas as pd
import os
import duckdb
import boto3
import logging
from pathlib import Path
from dotenv import load_dotenv
import kagglehub
from datetime import datetime

#df["DeliveryDays"] = (df["Ship Date"]- df["Order Date"]).dt.days
#df["OrderMonth"] = df["Order Date"].dt.to_period("M").astype(str)

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
    def __init__(self):
        self.df = None
        self.conn = duckdb.connect(database=":memory:")
    
    def download_from_kaggle(self):
        """Donwload dataset from Kaggle"""
        logger.info("Donwloading dataset from Kaggle...")
        path = kagglehub.dataset_download("rohitsahoo/sales-forecasting")
        csv_path = os.path.join(path,"train.csv")

        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV not found: {csv_path}")

        return csv_path
    
    def load_data(self, csv_path):
        """ Load and prepare data"""
        logger.info("Loading data...")

        self.df = pd.read_csv(csv_path, dayfirst=True, parse_dates=["Order Date", "Ship Date"])
        self.df["year"] = self.df["Order Date"].dt.year

        logger.info(f"Loaded {len(self.df):,}rows")

        return self.df
    
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

    def to_parquet(self, output_dir = "data"):
        """Convert to partitioned Parquet (one file per year)"""
        logger.info("Converting to Parquet...")
        
        out_path = Path(output_dir)
        out_path.mkdir(parents=True, exist_ok=True)

        self.conn.register("sales", self.df)
        years = sorted(self.df['year'].unique())

        for year in years:
            year_file = out_path / f"{year}.parquet"
            self.conn.execute(f"""
                COPY (SELECT * FROM sales WHERE year = {year})
                TO '{year_file}'
                (FORMAT PARQUET, COMPRESSION 'SNAPPY')
                """)
        total_size = sum(f.stat().st_zise for f in out_path.glob("*.parquet")) / 1024 / 1024
        logger.info(f"Created {len(years)} files ({total_size:.2f} MB)")

        return str(out_path)
        
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
