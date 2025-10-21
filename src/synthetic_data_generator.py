import polars as pl
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random 
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

class SyntheticDataGenerator:
    """ Generate realistic synthetic sales data """
    SHIP_MODES = ["First Class", "Same Day", "Second Class", "Standard Class"]
    SEGMENTS = ["Consumer", "Corporate", "Home Office"]
    US_CITIES = {
        "New York": {"state": "New York", "zip_prefix": "100", "region": "East"},
        "Los Angeles": {"state": "California", "zip_prefix": "900", "region": "West"},
        "Chicago": {"state": "Illinois", "zip_prefix": "606", "region": "Central"},
        "Houston": {"state": "Texas", "zip_prefix": "770", "region": "Central"},
        "Phoenix": {"state": "Arizona", "zip_prefix": "850", "region": "West"},
        "Philadelphia": {"state": "Pennsylvania", "zip_prefix": "191", "region": "East"},
        "San Antonio": {"state": "Texas", "zip_prefix": "782", "region": "Central"},
        "San Diego": {"state": "California", "zip_prefix": "921", "region": "West"},
        "Dallas": {"state": "Texas", "zip_prefix": "752", "region": "Central"},
        "San Jose": {"state": "California", "zip_prefix": "951", "region": "West"},
        "Austin": {"state": "Texas", "zip_prefix": "787", "region": "Central"},
        "Jacksonville": {"state": "Florida", "zip_prefix": "322", "region": "South"},
        "Fort Worth": {"state": "Texas", "zip_prefix": "761", "region": "Central"},
        "Columbus": {"state": "Ohio", "zip_prefix": "432", "region": "East"},
        "Charlotte": {"state": "North Carolina", "zip_prefix": "282", "region": "South"},
        "Indianapolis": {"state": "Indiana", "zip_prefix": "462", "region": "Central"},
        "Seattle": {"state": "Washington", "zip_prefix": "981", "region": "West"},
        "Denver": {"state": "Colorado", "zip_prefix": "802", "region": "West"},
        "Boston": {"state": "Massachusetts", "zip_prefix": "021", "region": "East"},
        "Nashville": {"state": "Tennessee", "zip_prefix": "372", "region": "South"},
        "Detroit": {"state": "Michigan", "zip_prefix": "482", "region": "Central"},
        "Portland": {"state": "Oregon", "zip_prefix": "972", "region": "West"},
        "Las Vegas": {"state": "Nevada", "zip_prefix": "891", "region": "West"},
        "Miami": {"state": "Florida", "zip_prefix": "331", "region": "South"},
        "Atlanta": {"state": "Georgia", "zip_prefix": "303", "region": "South"}
    }

    CATEGORIES = {"Furniture": {"Bookcases", "Chairs", "Furnishings", "Tables"},
                  "Office Supplies": {"Appliances", "Art", "Binders", "Envelopes", "Fasteners", "Labels",
                                       "Paper", "Storage","Supplies" },
                  "Technology": {"Accessories", "Copiers", "Machines","Phones"}  }

    def __init__(self, seed: int= 42, locale: str = 'en_US'):
        """ Initialize the synthetic data generator """

        self.fake = Faker(locale)
        Faker.seed(seed)
        random.seed(seed)
        np.random.seed(seed)
        logger.info(f"Initialized Synthetic_Data_Generator with seed={seed}")
    
    def generate_customer_name(self) -> str:
        """ Generate customer name based on data of USA """
        name = self.fake.name()
        return name

    def generate_customer_id(self, customer_name: str) -> str:
        """ Generate customer ID from customer name example: (CG-12456 from Claire Gute) """
        parts = customer_name.split()
        initials = parts[0][0] + parts[1][0]
        number = random.randint(10000,99999)
        return f"{initials}-{number}"
    
    def generate_order_dates(self, start_date: datetime, end_date: datetime,
                            min_ship_days: int = 1, max_ship_days: int = 14) -> tuple:
        """ Generate the order date and the ship date with the condition that shipdate must be greater than the order date """
        order_date = self.fake.date_between(start_date = start_date, end_date = end_date)
        ship_date = self.fake.date_between(
            start_date = order_date + timedelta(days = min_ship_days),
            end_date = order_date + timedelta(days = max_ship_days)
        )
        return order_date, ship_date
    
    def generate_location_data(self) -> Dict[str,str]:
        """ Generate City, State, Postal Code and Region """
        city = random.choice(list(self.US_CITIES.keys()))
        city_info = self.US_CITIES[city]
        postal_code = f"{city_info['zip_prefix']}{random.randint(10,99)}"
        return {
            "City": city,
            "State": city_info['state'],
            "Postal_Code": postal_code,
            "Region": city_info['region']
        }
    def generate_categories(self) -> Dict[str,str]:
        """ Generate category and subcategory by random choice"""
        category = random.choice(list(self.CATEGORIES.keys()))
        sub_category = random.choice(list(self.CATEGORIES[category]))
        return { 
            "Category" : category,
            "Sub_Category" : sub_category
        }

    def generate_order_id(self, order_date: datetime) -> str:
        """ Generate order_id example: US-2016-118983 """
        order_id = f"US-{order_date.year}-{random.randint(100000,999999)}"
        return order_id
    
    def generate_sales_amount(self, min_amount: int = 20, max_amount: int =2000) -> float:
        """ Generate salesamount based on a range """
        sales_amount = random.randint(min_amount,max_amount)
        return sales_amount
    
    def generate_product_id(self, category: str, subcategory: str) -> str:
       return f"{category[0:3].upper()}-{subcategory[0:2].upper()}-100{random.randint(10000,99999)}"

    def generate_synthetic_data(self, original_df : pl.DataFrame, 
                                num_rows: int = 10000,
                                start_date: datetime = datetime(2020,1,1),
                                end_date: datetime =  datetime(2024,12,31))  -> pl.DataFrame :
        
        logging.info(f"Generating {num_rows} synthetics rows...")
        
        if original_df is None or len(original_df) == 0:
            raise ValueError("Original dataframe is empty")

        # Extract unique products for variety
        products_df = original_df.select(["Product Name"]).unique()
        
        # Get starting row ID
        start_row_id = original_df.select(pl.col("Row ID")).max()[0,0] + 1

        synthetic_rows = []

        # Get Order ID

        for i in range(num_rows):
            
            customer_name = self.generate_customer_name()
            customer_id = self.generate_customer_id(customer_name)
            order_date, ship_date = self.generate_order_dates(start_date = start_date, end_date = end_date)
            order_id = self.generate_order_id(order_date)
            location = self.generate_location_data()
            categories = self. generate_categories()
            product_id = self.generate_product_id(categories["Category"], categories["Sub_Category"])
            product_index = random.randint(0, len(products_df)+1)
            product_name = products_df[product_index]
            sales = self.generate_sales_amount(min_amount= 10 , max_amount=2900)

            row = {
                "Row ID" : start_row_id + i,
                "Order ID" : order_id,
                "Order Date" : order_date,
                "Ship Date" : ship_date,
                "Ship Mode": self.fake.random_element(self.SHIP_MODES),
                "Customer ID": customer_id,
                "Customer Name": customer_name,
                "Segment": self.fake.random_element(self.SEGMENTS),
                "Country":"United States",
                "City": location["City"],
                "State": location["State"],
                "Postal Code": location["Postal_Code"],
                "Region": location["Region"],
                "Category": categories["Category"] ,
                "Sub_category": categories["Sub_Category"],
                "Product ID": product_id,
                "Product_name": product_name ,
                "Sales": sales
            }

            synthetic_rows.append(row)
            synthetic_df = pl.DataFrame(synthetic_rows)
            logger.info(f"Sucessfully generated {len(synthetic_df)} synthetic rows")
            return synthetic_df
    def augment_dataframe(self, original_df : pl.DataFrame, 
                          num_synthetic_rows: int = 10000,
                          **kwargs ):
        """ Augment sales data from CSV Kaggle with synthetic data 
        Args:
            original_df: Original DataFrame
            num_synthetic_rows: Numbers of synthetics rows to add
        Returns: 
            Combined DataFrame with original + synthetic data"""
        
        synthetic_df = self.generate_synthetic_data(original_df, num_rows = num_synthetic_rows, **kwargs )
        combined_df = pl.contact([original_df, synthetic_df])
        logger.info(f"Augmented data: {len(original_df)} original + "
                    f"{len(synthetic_df)} synthetic = {len(combined_df)} total rows")
        return combined_df

