# =================================================

# Final Project

# Grup : 001

# Batch : CODA-RMT-008

# Python script ini digunakan untuk ekstraksi data dari api, dimana data tersebut akan disimpan ke dalam data warehouse sebagai raw dataset.

# =================================================

# Import packages.
from pyspark.sql import SparkSession  # Import SparkSession to work with Spark DataFrames.
from pyspark.sql.types import StructType, StructField, StringType  # Import types to define DataFrame schema.
import time  # Import time to add delays between API requests.
import requests  # Import requests to fetch data from APIs.

# Initialize Spark session for distributed data processing.
spark = (SparkSession.builder
         .appName("Supabase_Write")  # Set the application name.
         .config("spark.jars", "/opt/airflow/jars/postgresql-42.7.7.jar")  # Add PostgreSQL JDBC driver JAR.
         .getOrCreate()  # Start the Spark session.
)

# Define the schema for the DataFrame based on expected API response.
schema = StructType([
    StructField("vin_1_10", StringType(), True),  # First 10 characters of vehicle VIN.
    StructField("county", StringType(), True),  # Vehicle registration county.
    StructField("city", StringType(), True),  # Registration city.
    StructField("state", StringType(), True),  # Registration state.
    StructField("zip_code", StringType(), True),  # ZIP code.
    StructField("model_year", StringType(), True),  # Year of the vehicle model.
    StructField("make", StringType(), True),  # Vehicle manufacturer.
    StructField("model", StringType(), True),  # Vehicle model.
    StructField("ev_type", StringType(), True),  # Type of electric vehicle.
    StructField("cafv_type", StringType(), True),  # Clean alternative fuel vehicle type.
    StructField("electric_range", StringType(), True),  # Electric driving range in miles.
    StructField("base_msrp", StringType(), True),  # Manufacturer suggested retail price.
    StructField("legislative_district", StringType(), True),  # Legislative district code.
    StructField("dol_vehicle_id", StringType(), True),  # Department of Licensing vehicle ID.
    StructField("geocoded_column", StringType(), True),  # Geocoded location information.
    StructField("electric_utility", StringType(), True),  # Associated electric utility.
    StructField("_2020_census_tract", StringType(), True),  # Census tract from 2020.
])

# API endpoint for Washington State EV data.
base_url = "https://data.wa.gov/resource/f6w7-q2d2.json"
limit = 1000  # Number of records per request.
offset = 0  # Pagination offset.
all_data = []  # List to collect all API records.

# Fetch paginated data from API
while True:
    url = f"{base_url}?$limit={limit}&$offset={offset}"  # Construct URL with limit and offset.
    response = requests.get(url)  # Call API.
    batch = response.json()  # Parse JSON response.
    
    if not batch:  # Stop if no more data is returned.
        break
    
    all_data.extend(batch)  # Add batch to complete dataset.
    offset += limit  # Move offset to next page.
    time.sleep(1)  # Wait 1 second to respect API rate limits.

# Create a Spark DataFrame from collected API data using the defined schema.
df = spark.createDataFrame(all_data, schema=schema)

# Supabase PostgreSQL connection details.
jdbc_url = "jdbc:postgresql://aws-1-ap-southeast-1.pooler.supabase.com:5432/postgres"
connection_properties = {
    "user": "postgres.jinlmqdphisxprlvuiqb",  # Database username.
    "password": "jtT8IQ5dA8xPw0mQ",  # Database password.
    "driver": "org.postgresql.Driver"  # JDBC driver.
}

# Target table name in PostgreSQL.
table_name = "ev_population_data_raw"

# Write the DataFrame to PostgreSQL, overwriting the table if it exists.
df.write \
  .jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=connection_properties)

# Stop Spark session to release resources.
spark.stop()
