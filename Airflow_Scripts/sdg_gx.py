# Import Spark.
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col
import json
# Import Great Expectations for data validation.
import great_expectations as ge
from great_expectations.dataset import SparkDFDataset

# Create a SparkSession with PostgreSQL JDBC driver configuration.
spark = (
    SparkSession.builder
    .appName("EV_Data_Processing_PySpark_JDBC_GE")
    .config("spark.jars", "/opt/airflow/jars/postgresql-42.7.7.jar")
    .getOrCreate()
)

# Define the schema explicitly for clarity.
schema = StructType([
    StructField("vin_1_10", StringType(), True),  # Vehicle Identification Number (first 10 chars).
    StructField("county", StringType(), True),  # County name.
    StructField("city", StringType(), True),  # City name.
    StructField("state", StringType(), True),  # State abbreviation (should be WA).
    StructField("zip_code", StringType(), True),  # ZIP code.
    StructField("model_year", StringType(), True),  # Vehicle model year.
    StructField("make", StringType(), True),  # Vehicle manufacturer.
    StructField("model", StringType(), True),  # Vehicle model.
    StructField("ev_type", StringType(), True),  # Type of electric vehicle.
    StructField("cafv_type", StringType(), True),  # Clean Alternative Fuel Vehicle type.
    StructField("electric_range", StringType(), True),  # Electric driving range.
    StructField("base_msrp", StringType(), True),  # Manufacturer’s Suggested Retail Price.
    StructField("legislative_district", StringType(), True),  # Legislative district.
    StructField("dol_vehicle_id", StringType(), True),  # Vehicle ID from Department of Licensing.
    StructField("geocoded_column", StringType(), True),  # Geocoded column.
    StructField("electric_utility", StringType(), True),  # Electric utility provider.
    StructField("_2020_census_tract", StringType(), True)  # 2020 census tract.
])

# Define PostgreSQL JDBC connection URL.
jdbc_url = "jdbc:postgresql://aws-1-ap-southeast-1.pooler.supabase.com:5432/postgres"

# Database connection properties.
db_properties = {
    "user": "postgres.jinlmqdphisxprlvuiqb",  # Database username.
    "password": "jtT8IQ5dA8xPw0mQ",  # Database password.
    "driver": "org.postgresql.Driver"  # PostgreSQL JDBC driver.
}

# Read data from PostgreSQL into a Spark DataFrame.
df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)  # JDBC connection URL.
    .option("dbtable", "ev_population_data_cleaned")  # Target table name.
    .option("user", db_properties["user"])  # Database user.
    .option("password", db_properties["password"])  # Database password.
    .option("driver", db_properties["driver"])  # JDBC driver.
    .load()
)

# Convert the Spark DataFrame into a Great Expectations Dataset.
ge_df = SparkDFDataset(df)

# Add expectation that vin_1_10 column should not contain null values.
ge_df.expect_column_values_to_not_be_null("vin_1_10")

# Add expectation that model_year should match a 4-digit year format.
ge_df.expect_column_values_to_match_regex("model_year", r"^\d{4}$")

# Add expectation that state column should only contain "WA".
ge_df.expect_column_values_to_be_in_set("state", ["WA"])

# Add expectation that county column should not contain null values.
ge_df.expect_column_values_to_not_be_null("county")

# Run validation against all defined expectations.
results = ge_df.validate()

# Convert validation results into JSON dictionary.
results_dict = results.to_json_dict()

# Print validation results in formatted JSON.
print(json.dumps(results_dict, indent=4))
