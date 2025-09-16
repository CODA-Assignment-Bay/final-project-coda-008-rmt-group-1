# =================================================

# Final Project

# Grup : 001

# Batch : CODA-RMT-008

# Python script ini digunakan untuk melakukan load ke supabase, dari data table yang sudah di cleaning sebelumnya.

# =================================================

# Import packages.
from pyspark.sql import SparkSession  # Import SparkSession to start Spark.
from pyspark.sql.functions import monotonically_increasing_id  # Import function for generating surrogate keys.
from pyspark.sql import functions as F  # Import PySpark SQL functions for aggregation.

# Start Spark application and set up PostgreSQL JDBC driver.
spark = (
    SparkSession.builder  # Create Spark session builder.
    .appName("EVPopulationDW")  # Name of the Spark job.
    .config("spark.jars", "/opt/airflow/jars/postgresql-42.7.7.jar")  # Add PostgreSQL JDBC driver JAR.
    .getOrCreate()  # Start the Spark session.
)

# JDBC URL to connect with PostgreSQL database on Supabase.
jdbc_url = "jdbc:postgresql://aws-1-ap-southeast-1.pooler.supabase.com:5432/postgres"

# Dictionary storing database credentials and driver.
db_properties = {
    "user": "postgres.jinlmqdphisxprlvuiqb",  # Database username.
    "password": "jtT8IQ5dA8xPw0mQ",  # Database password.
    "driver": "org.postgresql.Driver"  # PostgreSQL JDBC driver class.
}

# Read cleaned EV population table from Supabase into Spark DataFrame.
df = (
    spark.read.format("jdbc")  # Use JDBC connector.
    .option("url", jdbc_url)  # Set JDBC connection URL.
    .option("dbtable", "ev_population_data_cleaned")  # Table name to read.
    .option("user", db_properties["user"])  # Database username.
    .option("password", db_properties["password"])  # Database password.
    .option("driver", "org.postgresql.Driver")  # PostgreSQL JDBC driver.
    .load()  # Load data into DataFrame.
)

# ================== VEHICLE DIMENSIONS ==================

# Extract distinct car brands and assign surrogate IDs.
dim_brand = (
    df.select("make").distinct()  # Select unique brand names.
    .withColumn("brand_id", monotonically_increasing_id())  # Add surrogate key.
)

# Extract distinct car models and assign surrogate IDs.
dim_model = (
    df.select("model").distinct()  # Select unique model names.
    .withColumn("model_id", monotonically_increasing_id())  # Add surrogate key.
)

# Extract distinct model years and assign surrogate IDs.
dim_model_year = (
    df.select("model_year").distinct()  # Select unique model years.
    .withColumn("model_year_id", monotonically_increasing_id())  # Add surrogate key.
)

# Extract distinct EV types and assign surrogate IDs.
dim_ev_type = (
    df.select("ev_type").distinct()  # Select unique EV types.
    .withColumn("ev_id", monotonically_increasing_id())  # Add surrogate key.
)

# Extract distinct CAFV types and assign surrogate IDs.
dim_cafv_type = (
    df.select("cafv_type").distinct()  # Select unique CAFV types.
    .withColumn("cafv_id", monotonically_increasing_id())  # Add surrogate key.
)

# Extract distinct VIN prefixes and assign surrogate IDs.
dim_vin = (
    df.select("vin_1_10").distinct()  # Select unique VIN prefixes.
    .withColumn("vin_id", monotonically_increasing_id())  # Add surrogate key.
)

# Build vehicle dimension by linking brand, model, type, year, and VIN.
dim_vehicle = (
    df.select("make", "model", "ev_type", "cafv_type", "model_year", "vin_1_10").distinct()  # Select unique combos.
    .join(dim_brand.select("make", "brand_id"), "make", "left")  # Attach brand surrogate key.
    .join(dim_model.select("model", "model_id"), "model", "left")  # Attach model surrogate key.
    .join(dim_ev_type.select("ev_type", "ev_id"), "ev_type", "left")  # Attach EV type surrogate key.
    .join(dim_cafv_type.select("cafv_type", "cafv_id"), "cafv_type", "left")  # Attach CAFV surrogate key.
    .join(dim_model_year.select("model_year", "model_year_id"), "model_year", "left")  # Attach year surrogate key.
    .join(dim_vin.select("vin_1_10", "vin_id"), "vin_1_10", "left")  # Attach VIN surrogate key.
    .select("brand_id", "model_id", "ev_id", "cafv_id", "model_year_id", "vin_id")  # Keep only IDs.
    .distinct()  # Ensure unique records.
    .withColumn("vehicle_id", monotonically_increasing_id())  # Add final vehicle surrogate key.
)

# ================== LOCATION DIMENSIONS ==================

# Extract distinct states with surrogate IDs.
dim_state = (
    df.select("state").distinct()  # Select unique states.
    .withColumn("state_id", monotonically_increasing_id())  # Add surrogate key.
)

# Extract distinct counties with surrogate IDs.
dim_county = (
    df.select("county").distinct()  # Select unique counties.
    .withColumn("county_id", monotonically_increasing_id())  # Add surrogate key.
)

# Extract distinct cities, zips, and coordinates with surrogate IDs.
dim_city = (
    df.select("city", "zip_code", "longitude", "latitude").distinct()  # Select unique cities.
    .withColumn("city_id", monotonically_increasing_id())  # Add surrogate key.
)

# Extract distinct legislative districts with surrogate IDs.
dim_district = (
    df.select("legislative_district").distinct()  # Select unique districts.
    .withColumn("district_id", monotonically_increasing_id())  # Add surrogate key.
)

# Build location dimension by combining state, county, city, and district.
dim_location = (
    df.select("state", "county", "city", "zip_code", "legislative_district").distinct()  # Unique combos.
    .join(dim_state.select("state", "state_id"), "state", "left")  # Attach state ID.
    .join(dim_county.select("county", "county_id"), "county", "left")  # Attach county ID.
    .join(dim_city.select("city", "zip_code", "city_id"), ["city", "zip_code"], "left")  # Attach city ID.
    .join(dim_district.select("legislative_district", "district_id"), "legislative_district", "left")  # Attach district ID.
    .select("state_id", "county_id", "city_id", "district_id")  # Keep only IDs.
    .distinct()  # Ensure uniqueness.
    .withColumn("location_id", monotonically_increasing_id())  # Add final location surrogate key.
)

# ================== UTILITY DIMENSIONS ==================

# Extract distinct electric utilities with surrogate IDs.
dim_utility = (
    df.select("electric_utility").distinct()  # Select unique utilities.
    .withColumn("utility_id", monotonically_increasing_id())  # Add surrogate key.
)

# ================== CENSUS DIMENSIONS ==================

# Extract distinct census tracts with surrogate IDs.
dim_census = (
    df.select("_2020_census_tract").distinct()  # Select unique census tracts.
    .withColumn("census_id", monotonically_increasing_id())  # Add surrogate key.
)

# ================== FACT TABLE ==================

# Build fact table with surrogate keys from all dimensions.
fact_temp = (df
    .join(dim_brand.select("make", "brand_id"), "make", "left")  # Map brand IDs.
    .join(dim_model.select("model", "model_id"), "model", "left")  # Map model IDs.
    .join(dim_ev_type.select("ev_type", "ev_id"), "ev_type", "left")  # Map EV type IDs.
    .join(dim_cafv_type.select("cafv_type", "cafv_id"), "cafv_type", "left")  # Map CAFV type IDs.
    .join(dim_model_year.select("model_year", "model_year_id"), "model_year", "left")  # Map year IDs.
    .join(dim_vin.select("vin_1_10", "vin_id"), "vin_1_10", "left")  # Map VIN IDs.
    .join(dim_state.select("state", "state_id"), "state", "left")  # Map state IDs.
    .join(dim_county.select("county", "county_id"), "county", "left")  # Map county IDs.
    .join(dim_city.select("city", "zip_code", "city_id"), ["city", "zip_code"], "left")  # Map city IDs.
    .join(dim_district.select("legislative_district", "district_id"), "legislative_district", "left")  # Map district IDs.
    .join(dim_utility.select("electric_utility", "utility_id"), "electric_utility", "left")  # Map utility IDs.
    .join(dim_census.select("_2020_census_tract", "census_id"), "_2020_census_tract", "left")  # Map census IDs.
    .join(dim_vehicle, ["brand_id", "model_id", "ev_id", "cafv_id", "model_year_id", "vin_id"], "left")  # Attach vehicle ID.
    .join(dim_location, ["state_id", "county_id", "city_id", "district_id"], "left")  # Attach location ID.
    .groupby(  # Group the dataset by surrogate keys and numeric fact attributes.
        "vehicle_id",  # Surrogate vehicle ID.
        "location_id",  # Surrogate location ID.
        "utility_id",  # Surrogate utility ID.
        "census_id",  # Surrogate census ID.
        "electric_range",  # Numeric fact: EV range.
        "base_msrp"  # Numeric fact: price.
    )
    .agg(F.count("dol_vehicle_id").alias("dol_vehicle_count")) # Count dol_vehicle_id per group
)

# Assign fact table DataFrame.
fact_ev_registration = fact_temp

# ================== SAVE TO SUPABASE ==================

# Define function to write DataFrame to Supabase.
def write_to_supabase(df, table_name):
    (
    df.coalesce(10).write  # Use DataFrame writer.
      .format("jdbc")  # Use JDBC format.
      .option("url", jdbc_url)  # Connection URL.
      .option("dbtable", table_name)  # Target table name.
      .option("user", db_properties["user"])  # Username.
      .option("password", db_properties["password"])  # Password.
      .option("driver", "org.postgresql.Driver")  # PostgreSQL driver.
      .option("batchsize", 1000) # Number of rows per JDBC batch.
      .option("isolationLevel", "NONE") # Optional: can reduce locking overhead.
      .mode("overwrite")  # Overwrite mode.
      .save()  # Save DataFrame.
    )

# Write each dimension and fact table back into Supabase.

# ================== VEHICLE DIMENSIONS ==================
write_to_supabase(dim_brand, "dim_brand")  # Save brand dimension.
write_to_supabase(dim_model, "dim_model")  # Save model dimension.
write_to_supabase(dim_model_year, "dim_model_year")  # Save model year dimension.
write_to_supabase(dim_ev_type, "dim_ev_type")  # Save EV type dimension.
write_to_supabase(dim_cafv_type, "dim_cafv_type")  # Save CAFV type dimension.
write_to_supabase(dim_vin, "dim_vin")  # Save VIN dimension.
write_to_supabase(dim_vehicle, "dim_vehicle")  # Save combined vehicle dimension.

# ================== LOCATION DIMENSIONS ==================
write_to_supabase(dim_state, "dim_state")  # Save state dimension.
write_to_supabase(dim_county, "dim_county")  # Save county dimension.
write_to_supabase(dim_city, "dim_city")  # Save city dimension.
write_to_supabase(dim_district, "dim_district")  # Save district dimension.
write_to_supabase(dim_location, "dim_location")  # Save combined location dimension.

# ================== UTILITY DIMENSIONS ==================
write_to_supabase(dim_utility, "dim_utility")  # Save utility dimension.

# ================== CENSUS DIMENSIONS ==================
write_to_supabase(dim_census, "dim_census")  # Save census dimension.

# ================== FACT TABLE ==================
write_to_supabase(fact_ev_registration, "fact_ev_registration")  # Save fact table.

# Stop Spark session to release resources.
spark.stop()