# =================================================

# Final Project

# Grup : 001

# Batch : CODA-RMT-008

# Python script ini digunakan untuk melakukan load ke supabase, dari data table yang sudah di cleaning sebelumnya.

# =================================================

# Import packages.
from pyspark.sql import SparkSession  # Import SparkSession to start Spark.
from pyspark.sql.functions import monotonically_increasing_id  # Import function for generating surrogate keys.
from pyspark.sql import functions as F  # Import Spark SQL functions for aggregation, count, etc.
from pyspark.sql.window import Window  # Import Window class to define window specifications.
from pyspark.sql.functions import row_number  # Import row_number function to generate sequential surrogate keys.

# Start Spark session and configure PostgreSQL JDBC driver.
spark = (
    SparkSession.builder  # Initialize SparkSession builder.
    .appName("EVPopulationDW")  # Set the application name to EVPopulationDW.
    .config("spark.jars", "/opt/airflow/jars/postgresql-42.7.7.jar")  # Add PostgreSQL JDBC driver JAR path.
    .getOrCreate()  # Create and start the Spark session.
)

# JDBC connection URL for Supabase PostgreSQL database.
jdbc_url = "jdbc:postgresql://aws-1-ap-southeast-1.pooler.supabase.com:5432/postgres"  # Set the JDBC URL.

# Dictionary containing PostgreSQL database credentials and driver.
db_properties = {
    "user": "postgres.jinlmqdphisxprlvuiqb",  # Set the database username.
    "password": "jtT8IQ5dA8xPw0mQ",  # Set the database password.
    "driver": "org.postgresql.Driver"  # Specify the PostgreSQL JDBC driver class.
}

# Load cleaned EV population table from Supabase into Spark DataFrame.
df = (
    spark.read.format("jdbc")  # Use JDBC data source.
    .option("url", jdbc_url)  # Set JDBC connection URL.
    .option("dbtable", "ev_population_data_cleaned")  # Set the database table name.
    .option("user", db_properties["user"])  # Set database username.
    .option("password", db_properties["password"])  # Set database password.
    .option("driver", "org.postgresql.Driver")  # Set PostgreSQL JDBC driver class.
    .load()  # Load the table into a Spark DataFrame.
)

# ================== VEHICLE DIMENSIONS ==================

# Extract distinct car brands and assign sequential surrogate IDs.
dim_brand = (
    df.select("make").distinct()  # Select unique car brand names.
    .withColumn("brand_id", row_number().over(Window.orderBy("make")))  # Assign sequential surrogate key.
)

# Extract distinct car models with associated brand and assign sequential surrogate IDs.
dim_model = (
    df.select("make", "model").distinct()  # Select unique model names per brand.
    .withColumn("model_id", row_number().over(Window.orderBy("make", "model")))  # Assign sequential surrogate key.
)

# Extract distinct model years and assign sequential surrogate IDs.
dim_model_year = (
    df.select("model_year").distinct()  # Select unique model years.
    .withColumn("model_year_id", row_number().over(Window.orderBy("model_year")))  # Assign sequential surrogate key.
)

# Extract distinct EV types and assign sequential surrogate IDs.
dim_ev_type = (
    df.select("ev_type").distinct()  # Select unique EV types.
    .withColumn("ev_id", row_number().over(Window.orderBy("ev_type")))  # Assign sequential surrogate key.
)

# Extract distinct CAFV types and assign sequential surrogate IDs.
dim_cafv_type = (
    df.select("cafv_type").distinct()  # Select unique CAFV types.
    .withColumn("cafv_id", row_number().over(Window.orderBy("cafv_type")))  # Assign sequential surrogate key.
)

# Extract distinct VIN prefixes and assign sequential surrogate IDs.
dim_vin = (
    df.select("vin_1_10").distinct()  # Select unique VIN prefixes.
    .withColumn("vin_id", row_number().over(Window.orderBy("vin_1_10")))  # Assign sequential surrogate key.
)

# Build combined vehicle dimension by joining brand, model, type, year, and VIN.
dim_vehicle = (
    df.select("make", "model", "ev_type", "cafv_type", "model_year", "vin_1_10").distinct()  # Select distinct vehicle attributes.
    .join(dim_brand.select("make", "brand_id"), "make", "left")  # Join to get brand_id.
    .join(dim_model.select("make", "model", "model_id"), ["make", "model"], "left")  # Join to get model_id.
    .join(dim_ev_type.select("ev_type", "ev_id"), "ev_type", "left")  # Join to get ev_id.
    .join(dim_cafv_type.select("cafv_type", "cafv_id"), "cafv_type", "left")  # Join to get cafv_id.
    .join(dim_model_year.select("model_year", "model_year_id"), "model_year", "left")  # Join to get model_year_id.
    .join(dim_vin.select("vin_1_10", "vin_id"), "vin_1_10", "left")  # Join to get vin_id.
    .select("brand_id", "model_id", "ev_id", "cafv_id", "model_year_id", "vin_id")  # Select only surrogate keys.
    .distinct()  # Ensure uniqueness.
    .withColumn("vehicle_id", row_number().over(Window.orderBy("brand_id", "model_id", "ev_id", "cafv_id", "model_year_id", "vin_id")))  # Assign final vehicle surrogate key.
)

# ================== LOCATION DIMENSIONS ==================

# Extract distinct states and assign sequential surrogate IDs.
dim_state = (
    df.select("state").distinct()  # Select unique states.
    .withColumn("state_id", row_number().over(Window.orderBy("state")))  # Assign sequential surrogate key.
)

# Extract distinct counties and assign sequential surrogate IDs.
dim_county = (
    df.select("county").distinct()  # Select unique counties.
    .withColumn("county_id", row_number().over(Window.orderBy("county")))  # Assign sequential surrogate key.
)

# Extract distinct cities with zip code and coordinates, and link to county.
dim_city = (
    df.select("county", "city", "zip_code", "longitude", "latitude").distinct()  # Select distinct city records.
    .join(dim_county.select("county", "county_id"), "county", "left")  # Join to get county_id.
    .withColumn("city_id", row_number().over(Window.orderBy("county_id", "city", "zip_code")))  # Assign sequential surrogate key.
)

# Extract distinct legislative districts and assign sequential surrogate IDs.
dim_district = (
    df.select("legislative_district").distinct()  # Select unique legislative districts.
    .withColumn("district_id", row_number().over(Window.orderBy("legislative_district")))  # Assign sequential surrogate key.
)

# Build combined location dimension by joining state, county, city, and district.
dim_location = (
    df.select("state", "county", "city", "zip_code", "legislative_district").distinct()  # Select distinct location attributes.
    .join(dim_state.select("state", "state_id"), "state", "left")  # Join to get state_id.
    .join(dim_county.select("county", "county_id"), "county", "left")  # Join to get county_id.
    .join(dim_city.select("county_id", "city", "zip_code", "city_id"), ["county_id", "city", "zip_code"], "left")  # Join to get city_id.
    .join(dim_district.select("legislative_district", "district_id"), "legislative_district", "left")  # Join to get district_id.
    .select("state_id", "county_id", "city_id", "district_id")  # Keep only surrogate keys.
    .distinct()  # Ensure uniqueness.
    .withColumn("location_id", row_number().over(Window.orderBy("state_id", "county_id", "city_id", "district_id")))  # Assign final location surrogate key.
)

# ================== UTILITY DIMENSIONS ==================

# Extract distinct electric utilities and assign sequential surrogate IDs.
dim_utility = (
    df.select("electric_utility").distinct()  # Select unique utilities.
    .withColumn("utility_id", row_number().over(Window.orderBy("electric_utility")))  # Assign sequential surrogate key.
)

# ================== CENSUS DIMENSIONS ==================

# Extract distinct census tracts and assign sequential surrogate IDs.
dim_census = (
    df.select("_2020_census_tract").distinct()  # Select unique census tracts.
    .withColumn("census_id", row_number().over(Window.orderBy("_2020_census_tract")))  # Assign sequential surrogate key.
)

# ================== FACT TABLE ==================

# Build fact table by joining all dimension surrogate keys and aggregating numeric attributes.
fact_ev_registration = (
    df
    # ================== VEHICLE KEYS ==================
    .join(dim_brand, "make", "left")  # Join brand dimension.
    .join(dim_model.select("make", "model", "model_id"), ["make", "model"], "left")  # Join model dimension.
    .join(dim_ev_type, "ev_type", "left")  # Join EV type dimension.
    .join(dim_cafv_type, "cafv_type", "left")  # Join CAFV type dimension.
    .join(dim_model_year, "model_year", "left")  # Join model year dimension.
    .join(dim_vin, "vin_1_10", "left")  # Join VIN dimension.
    # ================== LOCATION KEYS ==================
    .join(dim_state, "state", "left")  # Join state dimension.
    .join(dim_county, "county", "left")  # Join county dimension.
    .join(dim_city.select("county_id", "city", "zip_code", "city_id"), ["county_id", "city", "zip_code"], "left")  # Join city dimension.
    .join(dim_district, "legislative_district", "left")  # Join district dimension.
    # ================== UTILITY & CENSUS KEYS ==================
    .join(dim_utility, "electric_utility", "left")  # Join utility dimension.
    .join(dim_census, "_2020_census_tract", "left")  # Join census dimension.
    # ================== VEHICLE & LOCATION IDS ==================
    .join(dim_vehicle, ["brand_id", "model_id", "ev_id", "cafv_id", "model_year_id", "vin_id"], "left")  # Join vehicle dimension.
    .join(dim_location, ["state_id", "county_id", "city_id", "district_id"], "left")  # Join location dimension.
    # ================== FACT BUILD ==================
    .groupby(
        "vehicle_id",
        "location_id",
        "utility_id",
        "census_id",
        "electric_range",
        "base_msrp"
    )  # Group by surrogate keys and numeric facts.
    .agg(F.count("dol_vehicle_id").alias("dol_vehicle_count"))  # Count vehicles per group.
)

# ================== SAVE TO SUPABASE ==================

# Function to write a Spark DataFrame to Supabase PostgreSQL.
def write_to_supabase(df, table_name):
    (
        df.coalesce(10).write  # Reduce number of output files for writing.
          .format("jdbc")  # Use JDBC format.
          .option("url", jdbc_url)  # Set JDBC URL.
          .option("dbtable", table_name)  # Set target table name.
          .option("user", db_properties["user"])  # Set database username.
          .option("password", db_properties["password"])  # Set database password.
          .option("driver", "org.postgresql.Driver")  # Set JDBC driver.
          .option("batchsize", 1000)  # Set JDBC batch size.
          .option("isolationLevel", "NONE")  # Optional: reduce locking overhead.
          .mode("overwrite")  # Overwrite existing table.
          .save()  # Execute the write operation.
    )

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