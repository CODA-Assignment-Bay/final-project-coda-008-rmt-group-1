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

# Start Spark application and set up PostgreSQL JDBC driver.
spark = (
    SparkSession.builder  # Create Spark session builder.
    .appName("supabase_add_write")  # Name of the Spark job.
    .config("spark.jars", "/opt/airflow/jars/postgresql-42.7.7.jar")  # Add PostgreSQL JDBC driver JAR.
    .getOrCreate()  # Start the Spark session.
)

# Supabase PostgreSQL connection details.
jdbc_url = "jdbc:postgresql://aws-1-ap-southeast-1.pooler.supabase.com:5432/postgres"
connection_properties = {
    "user": "postgres.jinlmqdphisxprlvuiqb",  # Database username.
    "password": "jtT8IQ5dA8xPw0mQ",  # Database password.
    "driver": "org.postgresql.Driver"  # JDBC driver.
}

# Define the schema for the DataFrame based on expected API response.
schema_est = StructType([
    StructField("sequence", StringType(), True),  # Sequence.
    StructField("filter", StringType(), True),  # Filter.
    StructField("county", StringType(), True),  # County.
    StructField("jurisdiction", StringType(), True),  # Jurisdiction.
    StructField("pop_1990", StringType(), True),  # pop_1990.
    StructField("pop_1991", StringType(), True),  # pop_1991.
    StructField("pop_1992", StringType(), True),  # pop_1992.
    StructField("pop_1993", StringType(), True),  # pop_1993.
    StructField("pop_1994", StringType(), True),  # pop_1994.
    StructField("pop_1995", StringType(), True),  # pop_1995.
    StructField("pop_1996", StringType(), True),  # pop_1996.
    StructField("pop_1997", StringType(), True),  # pop_1997.
    StructField("pop_1998", StringType(), True),  # pop_1998.
    StructField("pop_1999", StringType(), True),  # pop_1999.
    StructField("pop_2000", StringType(), True),  # pop_2000.
    StructField("pop_2001", StringType(), True),  # pop_2001.
    StructField("pop_2002", StringType(), True),  # pop_2002.
    StructField("pop_2003", StringType(), True),  # pop_2003.
    StructField("pop_2004", StringType(), True),  # pop_2004.
    StructField("pop_2005", StringType(), True),  # pop_2005.
    StructField("pop_2006", StringType(), True),  # pop_2006.
    StructField("pop_2007", StringType(), True),  # pop_2007.
    StructField("pop_2008", StringType(), True),  # pop_2008.
    StructField("pop_2009", StringType(), True),  # pop_2009.
    StructField("pop_2010", StringType(), True),  # pop_2010.
    StructField("pop_2011", StringType(), True),  # pop_2011.
    StructField("pop_2012", StringType(), True),  # pop_2012.
    StructField("pop_2013", StringType(), True),  # pop_2013.
    StructField("pop_2014", StringType(), True),  # pop_2014.
    StructField("pop_2015", StringType(), True),  # pop_2015.
    StructField("pop_2016", StringType(), True),  # pop_2016.
    StructField("pop_2017", StringType(), True),  # pop_2017.
    StructField("pop_2018", StringType(), True),  # pop_2018.
    StructField("pop_2019", StringType(), True),  # pop_2019.
    StructField("pop_2020", StringType(), True),  # pop_2020.
    StructField("pop_2021", StringType(), True),  # pop_2021.
    StructField("pop_2022", StringType(), True),  # pop_2022.
    StructField("pop_2023", StringType(), True),  # pop_2023.
    StructField("pop_2024", StringType(), True),  # pop_2024.
    StructField("pop_2025", StringType(), True)  # pop_2025.
])

# API endpoint for Washington Population Estimation.
base_url = "https://data.wa.gov/resource/2hia-rqet.json"
limit = 1000  # Number of records per request.
offset = 0  # Pagination offset.
all_data = []  # List to collect all API records.

# Fetch paginated data from API.
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
df1 = spark.createDataFrame(all_data, schema = schema_est)

# Target table name in PostgreSQL.
table_name = "population_est_data_raw"

# Write the DataFrame to PostgreSQL, overwriting the table if it exists.
df1.write \
  .jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=connection_properties)

# Define the schema for the DataFrame based on expected API response.
schema_den = StructType([
    StructField("county_name", StringType(), True),  # county_name.
    StructField("popden_2000", StringType(), True),  # popden_2000.
    StructField("popden_2001", StringType(), True),  # popden_2001.
    StructField("popden_2002", StringType(), True),  # popden_2002.
    StructField("popden_2003", StringType(), True),  # popden_2003.
    StructField("popden_2004", StringType(), True),  # popden_2004.
    StructField("popden_2005", StringType(), True),  # popden_2005.
    StructField("popden_2006", StringType(), True),  # popden_2006.
    StructField("popden_2007", StringType(), True),  # popden_2007.
    StructField("popden_2008", StringType(), True),  # popden_2008.
    StructField("popden_2009", StringType(), True),  # popden_2009.
    StructField("popden_2010", StringType(), True),  # popden_2010.
    StructField("popden_2011", StringType(), True),  # popden_2011.
    StructField("popden_2012", StringType(), True),  # popden_2012.
    StructField("popden_2013", StringType(), True),  # popden_2013.
    StructField("popden_2014", StringType(), True),  # popden_2014.
    StructField("popden_2015", StringType(), True),  # popden_2015.
    StructField("popden_2016", StringType(), True),  # popden_2016.
    StructField("popden_2017", StringType(), True),  # popden_2017.
    StructField("popden_2018", StringType(), True),  # popden_2018.
    StructField("popden_2019", StringType(), True),  # popden_2019.
    StructField("popden_2020", StringType(), True),  # popden_2020.
    StructField("popden_2021", StringType(), True),  # popden_2021.
    StructField("popden_2022", StringType(), True),  # popden_2022.
    StructField("popden_2023", StringType(), True),  # popden_2023.
    StructField("popden_2024", StringType(), True),  # popden_2024.
    StructField("popden_2025", StringType(), True)  # popden_2025.
])

# API endpoint for Washington Population Density.
base_url = "https://data.wa.gov/resource/c535-p92u.json"
limit = 1000  # Number of records per request.
offset = 0  # Pagination offset.
all_data = []  # List to collect all API records.

# Fetch paginated data from API.
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
df2 = spark.createDataFrame(all_data, schema = schema_den)

# Target table name in PostgreSQL.
table_name = "population_den_data_raw"

# Write the DataFrame to PostgreSQL, overwriting the table if it exists.
df2.write \
  .jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=connection_properties)

# Path to your CSV file.
csv_file_path = "/opt/airflow/data/model_car_sales_price_2025.csv"

# Read CSV into DataFrame.
df3 = spark.read.csv(
    csv_file_path,
    header=True, # Use first row as header.
    inferSchema=True # Automatically infer data types.
)

# Target table name in PostgreSQL.
table_name = "model_price_data_raw"

# Write the DataFrame to PostgreSQL, overwriting the table if it exists.
df3.write \
  .jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=connection_properties)

# Path to your CSV file.
csv_file_path = "/opt/airflow/data/HDPulse_data_export.csv"

# Read CSV into DataFrame.
df4 = spark.read.csv(
    csv_file_path,
    header=True, # Use first row as header.
    inferSchema=True # Automatically infer data types.
)

# Target table name in PostgreSQL.
table_name = "income_data_raw"

# Write the DataFrame to PostgreSQL, overwriting the table if it exists.
df4.write \
  .jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=connection_properties)