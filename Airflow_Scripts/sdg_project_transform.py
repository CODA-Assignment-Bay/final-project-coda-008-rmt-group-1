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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Import column expression builder and regex extractor functions for transformations.
from pyspark.sql.functions import col, regexp_extract

# Create or get a Spark session, the main object used to work with Spark.
spark = (
    SparkSession.builder
    # Give the Spark job a descriptive name.
    .appName("EV_Data_Processing_PySpark_JDBC")
    # Register the PostgreSQL JDBC driver so Spark can connect to the database.
    .config("spark.jars", "/opt/airflow/jars/postgresql-42.7.7.jar")
    # Start the session (or retrieve it if already running).
    .getOrCreate()
)

# Build a schema describing each column’s name and type in the dataset.
schema = StructType([
    StructField("vin_1_10", StringType(), True), # First 10 digits of the vehicle VIN.
    StructField("county", StringType(), True), # County where the vehicle is registered.
    StructField("city", StringType(), True), # City of registration.
    StructField("state", StringType(), True), # State of registration.
    StructField("zip_code", StringType(), True), # Postal code of the owner.
    StructField("model_year", StringType(), True), # Year the car was manufactured.
    StructField("make", StringType(), True), # Car brand.
    StructField("model", StringType(), True), # Car model name.
    StructField("ev_type", StringType(), True), # EV type.
    StructField("cafv_type", StringType(), True), # Clean Alternative Fuel Vehicle eligibility.
    StructField("electric_range", StringType(), True), # Battery range in miles.
    StructField("base_msrp", StringType(), True), # Manufacturer’s Suggested Retail Price.
    StructField("legislative_district", StringType(), True), # Political district.
    StructField("dol_vehicle_id", StringType(), True), # Department of Licensing ID.
    StructField("geocoded_column", StringType(), True), # String with coordinates.
    StructField("electric_utility", StringType(), True), # Utility provider.
    StructField("_2020_census_tract", StringType(), True) # Census tract information.
])

# Define the JDBC URL to connect to the PostgreSQL database on Supabase.
jdbc_url = "jdbc:postgresql://aws-1-ap-southeast-1.pooler.supabase.com:5432/postgres"

# Store authentication details and JDBC driver class in a dictionary.
db_properties = {
    "user": "postgres.jinlmqdphisxprlvuiqb", # Database username.
    "password": "jtT8IQ5dA8xPw0mQ", # Database password.
    "driver": "org.postgresql.Driver" # PostgreSQL JDBC driver.
}

# Read raw vehicle population data from PostgreSQL into a Spark DataFrame.
df = (
    spark.read # Use Spark’s read function.
    .format("jdbc") # Specify JDBC as the data source.
    .option("url", jdbc_url) # Connection string to the database.
    .option("dbtable", "ev_population_data_raw") # Table to pull data from.
    .option("user", db_properties["user"]) # Pass in the username.
    .option("password", db_properties["password"]) # Pass in the password.
    .option("driver", db_properties["driver"]) # Driver to handle the connection.
    .load() # Load the data into a DataFrame.
)

# Clean and transform the dataset.
df_clean = (
    df.filter(col("state") == "WA") # Keep only records from Washington state.
      .withColumn("electric_range", col("electric_range").cast(IntegerType())) # Convert electric range to integer.
      .withColumn("model_year", col("model_year").cast(IntegerType())) # Convert model year to integer.
      .withColumn("base_msrp", col("base_msrp").cast(IntegerType())) # Convert MSRP to integer.
      .withColumn(  # Extract the longitude value from the geocoded string.
          "longitude",
          regexp_extract(col("geocoded_column"), r"coordinates=\[([-\d.]+)", 1).cast("double")
      )
      .withColumn( # Extract the latitude value from the geocoded string.
          "latitude",
          regexp_extract(col("geocoded_column"), r"coordinates=\[[^,]+, ([-\d.]+)\]", 1).cast("double")
      )
      .drop("geocoded_column") # Remove the original text-based coordinates column.
)

# Write the cleaned dataset back to PostgreSQL, replacing any existing table.
(df_clean.write
 .format("jdbc") # Use JDBC for writing data.
 .option("url", jdbc_url) # Database connection string.
 .option("dbtable", "ev_population_data_cleaned") # Destination table name.
 .option("user", db_properties["user"]) # Username for authentication.
 .option("password", db_properties["password"]) # Password for authentication.
 .option("driver", db_properties["driver"]) # Driver for PostgreSQL.
 .mode("overwrite") # Overwrite the table if it exists.
 .save() # Save the DataFrame to the database.
)

# Stop the Spark session to release resources.
spark.stop()
