# =================================================

# Final Project

# Grup : 001

# Batch : CODA-RMT-008

# Python script ini digunakan untuk melakukan transformasi data dari supabase db, untuk melakukan proses cleaning data dari table raw menjadi table cleaned.

# =================================================

# Import packages.
# Import the SparkSession class, the entry point for using Spark.
from pyspark.sql import SparkSession

# Import schema-related classes to explicitly define column data types.
from pyspark.sql.types import StringType, IntegerType, DoubleType

# Import column expression builder functions for transformations.
from pyspark.sql.functions import col

# Create or get a Spark session, the main object used to work with Spark.
def get_spark_session():
    spark = (
        SparkSession.builder
        # Give the Spark job a descriptive name.
        .appName("supabase_transform_write")
        # Register the PostgreSQL JDBC driver so Spark can connect to the database.
        .config("spark.jars", "/opt/airflow/jars/postgresql-42.7.7.jar")
        # Start the session (or retrieve it if already running).
        .getOrCreate()
    )
    return spark # Return the Spark session.

spark = get_spark_session()

# Define the JDBC URL to connect to the PostgreSQL database on Supabase.
jdbc_url = "jdbc:postgresql://aws-1-ap-southeast-1.pooler.supabase.com:5432/postgres"

# Store authentication details and JDBC driver class in a dictionary.
db_properties = {
    "user": "postgres.jinlmqdphisxprlvuiqb", # Database username.
    "password": "jtT8IQ5dA8xPw0mQ", # Database password.
    "driver": "org.postgresql.Driver" # PostgreSQL JDBC driver.
}

# Read raw population estimate from PostgreSQL into a Spark DataFrame.
df = (
    spark.read # Use Spark’s read function.
    .format("jdbc") # Specify JDBC as the data source.
    .option("url", jdbc_url) # Connection string to the database.
    .option("dbtable", "population_est_data_raw") # Table to pull data from.
    .option("user", db_properties["user"]) # Pass in the username.
    .option("password", db_properties["password"]) # Pass in the password.
    .option("driver", db_properties["driver"]) # Driver to handle the connection.
    .load() # Load the data into a DataFrame.
)

# Convert and clean columns directly:
df_clean = (
    df.withColumn("county", col("county").cast(StringType())) # Cast county to string.
      .withColumn("pop_2025", col("pop_2025").cast(IntegerType())) # Cast pop_2025 to integer.
)

# Keep only the two relevant columns: 'county' and 'pop_2025'.
df_2025 = df_clean.select("county", "pop_2025")

# Group data by county and sum the 2025 population.
agg_df_2025 = df_2025.groupBy("county").sum("pop_2025")

# Rename the aggregated column 'sum(pop_2025)' back to 'pop_2025'.
agg_df_2025 = agg_df_2025.withColumnRenamed("sum(pop_2025)", "pop_2025")

# Select final columns in lowercase form.
df_clean = agg_df_2025.select("county", "pop_2025")

# Write the cleaned dataset back to PostgreSQL, replacing any existing table.
(df_clean.write
 .format("jdbc") # Use JDBC for writing data.
 .option("url", jdbc_url) # Database connection string.
 .option("dbtable", "population_est_data_cleaned") # Destination table name.
 .option("user", db_properties["user"]) # Username for authentication.
 .option("password", db_properties["password"]) # Password for authentication.
 .option("driver", db_properties["driver"]) # Driver for PostgreSQL.
 .mode("overwrite") # Overwrite the table if it exists.
 .save() # Save the DataFrame to the database.
)

# Read raw population density from PostgreSQL into a Spark DataFrame.
df = (
    spark.read # Use Spark’s read function.
    .format("jdbc") # Specify JDBC as the data source.
    .option("url", jdbc_url) # Connection string to the database.
    .option("dbtable", "population_den_data_raw") # Table to pull data from.
    .option("user", db_properties["user"]) # Pass in the username.
    .option("password", db_properties["password"]) # Pass in the password.
    .option("driver", db_properties["driver"]) # Driver to handle the connection.
    .load() # Load the data into a DataFrame.
)

# Convert and clean columns directly:
df_clean = (
    df.withColumn("county", col("county_name").cast(StringType())) # Cast county_name to string and rename it as 'county'.
      .withColumn("popden_2025", col("POPDEN_2025").cast(DoubleType())) # - Cast popden_2025.
)

# Keep only the two relevant columns: 'county' and 'popden_2025'.
df_clean = df_clean.select("county", "popden_2025")

# Write the cleaned dataset back to PostgreSQL, replacing any existing table.
(df_clean.write
 .format("jdbc") # Use JDBC for writing data.
 .option("url", jdbc_url) # Database connection string.
 .option("dbtable", "population_den_data_cleaned") # Destination table name.
 .option("user", db_properties["user"]) # Username for authentication.
 .option("password", db_properties["password"]) # Password for authentication.
 .option("driver", db_properties["driver"]) # Driver for PostgreSQL.
 .mode("overwrite") # Overwrite the table if it exists.
 .save() # Save the DataFrame to the database.
)

# Read raw model price from PostgreSQL into a Spark DataFrame.
df = (
    spark.read # Use Spark’s read function.
    .format("jdbc") # Specify JDBC as the data source.
    .option("url", jdbc_url) # Connection string to the database.
    .option("dbtable", "model_price_data_raw") # Table to pull data from.
    .option("user", db_properties["user"]) # Pass in the username.
    .option("password", db_properties["password"]) # Pass in the password.
    .option("driver", db_properties["driver"]) # Driver to handle the connection.
    .load() # Load the data into a DataFrame.
)

# Convert and clean columns directly:
df_clean = (
    df.withColumn("model", col("Model_Car").cast(StringType())) # Cast Model_Car to string and rename it as 'model'.
      .withColumn("price_usd", col("Sales_Price_2025 (usd)").cast(IntegerType())) # Cast Sales_Price_2025 (usd) to integer and rename it as 'price_usd'.
)

# Keep only the two relevant columns: 'model' and 'price_usd'.
df_clean = df_clean.select("model", "price_usd")

# Write the cleaned dataset back to PostgreSQL, replacing any existing table.
(df_clean.write
 .format("jdbc") # Use JDBC for writing data.
 .option("url", jdbc_url) # Database connection string.
 .option("dbtable", "model_price_data_cleaned") # Destination table name.
 .option("user", db_properties["user"]) # Username for authentication.
 .option("password", db_properties["password"]) # Password for authentication.
 .option("driver", db_properties["driver"]) # Driver for PostgreSQL.
 .mode("overwrite") # Overwrite the table if it exists.
 .save() # Save the DataFrame to the database.
)

# Read raw model price from PostgreSQL into a Spark DataFrame.
df = (
    spark.read # Use Spark’s read function.
    .format("jdbc") # Specify JDBC as the data source.
    .option("url", jdbc_url) # Connection string to the database.
    .option("dbtable", "income_data_raw") # Table to pull data from.
    .option("user", db_properties["user"]) # Pass in the username.
    .option("password", db_properties["password"]) # Pass in the password.
    .option("driver", db_properties["driver"]) # Driver to handle the connection.
    .load() # Load the data into a DataFrame.
)

# Convert and clean columns directly:
df_clean = (
    df.withColumn("county", col("County").cast(StringType())) # Cast County to string and rename it as 'county'.
      .withColumn("income", col("Value").cast(IntegerType())) # Cast Value to integer and rename it as 'income'.
)

# Keep only the two relevant columns: 'county' and 'income'.
df_clean = df_clean.select("county", "income")

# Write the cleaned dataset back to PostgreSQL, replacing any existing table.
(df_clean.write
 .format("jdbc") # Use JDBC for writing data.
 .option("url", jdbc_url) # Database connection string.
 .option("dbtable", "income_data_cleaned") # Destination table name.
 .option("user", db_properties["user"]) # Username for authentication.
 .option("password", db_properties["password"]) # Password for authentication.
 .option("driver", db_properties["driver"]) # Driver for PostgreSQL.
 .mode("overwrite") # Overwrite the table if it exists.
 .save() # Save the DataFrame to the database.
)