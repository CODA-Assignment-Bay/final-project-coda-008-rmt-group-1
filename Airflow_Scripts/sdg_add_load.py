# =================================================

# Final Project

# Grup : 001

# Batch : CODA-RMT-008

# Python script ini digunakan untuk melakukan load ke supabase, dari data table yang sudah di cleaning sebelumnya.

# =================================================

# Import packages.
from pyspark.sql import SparkSession  # Import SparkSession to start Spark.

# Create or get a Spark session, the main object used to work with Spark.
def get_spark_session():
    spark = (
        SparkSession.builder
        # Give the Spark job a descriptive name.
        .appName("write_dim_dw")
        # Register the PostgreSQL JDBC driver so Spark can connect to the database.
        .config("spark.jars", "/opt/airflow/jars/postgresql-42.7.7.jar")
        # Start the session (or retrieve it if already running).
        .getOrCreate()
    )
    return spark # Return the Spark session.

spark = get_spark_session()

# JDBC URL to connect with PostgreSQL database on Supabase.
jdbc_url = "jdbc:postgresql://aws-1-ap-southeast-1.pooler.supabase.com:5432/postgres"

# Dictionary storing database credentials and driver.
db_properties = {
    "user": "postgres.jinlmqdphisxprlvuiqb",  # Database username.
    "password": "jtT8IQ5dA8xPw0mQ",  # Database password.
    "driver": "org.postgresql.Driver"  # PostgreSQL JDBC driver class.
}

# Read cleaned Population Estimate table from Supabase into Spark DataFrame.
df = (
    spark.read.format("jdbc")  # Use JDBC connector.
    .option("url", jdbc_url)  # Set JDBC connection URL.
    .option("dbtable", "population_est_data_cleaned")  # Table name to read.
    .option("user", db_properties["user"])  # Database username.
    .option("password", db_properties["password"])  # Database password.
    .option("driver", "org.postgresql.Driver")  # PostgreSQL JDBC driver.
    .load()  # Load data into DataFrame.
)

# Write the cleaned dataset back to PostgreSQL, replacing any existing table.
(df.write
 .format("jdbc") # Use JDBC for writing data.
 .option("url", jdbc_url) # Database connection string.
 .option("dbtable", "dim_pop_est") # Destination table name.
 .option("user", db_properties["user"]) # Username for authentication.
 .option("password", db_properties["password"]) # Password for authentication.
 .option("driver", db_properties["driver"]) # Driver for PostgreSQL.
 .mode("overwrite") # Overwrite the table if it exists.
 .save() # Save the DataFrame to the database.
)

# Read cleaned Population Density table from Supabase into Spark DataFrame.
df = (
    spark.read.format("jdbc")  # Use JDBC connector.
    .option("url", jdbc_url)  # Set JDBC connection URL.
    .option("dbtable", "population_den_data_cleaned")  # Table name to read.
    .option("user", db_properties["user"])  # Database username.
    .option("password", db_properties["password"])  # Database password.
    .option("driver", "org.postgresql.Driver")  # PostgreSQL JDBC driver.
    .load()  # Load data into DataFrame.
)

# Write the cleaned dataset back to PostgreSQL, replacing any existing table.
(df.write
 .format("jdbc") # Use JDBC for writing data.
 .option("url", jdbc_url) # Database connection string.
 .option("dbtable", "dim_pop_den") # Destination table name.
 .option("user", db_properties["user"]) # Username for authentication.
 .option("password", db_properties["password"]) # Password for authentication.
 .option("driver", db_properties["driver"]) # Driver for PostgreSQL.
 .mode("overwrite") # Overwrite the table if it exists.
 .save() # Save the DataFrame to the database.
)

# Read cleaned Model Price table from Supabase into Spark DataFrame.
df = (
    spark.read.format("jdbc")  # Use JDBC connector.
    .option("url", jdbc_url)  # Set JDBC connection URL.
    .option("dbtable", "model_price_data_cleaned")  # Table name to read.
    .option("user", db_properties["user"])  # Database username.
    .option("password", db_properties["password"])  # Database password.
    .option("driver", "org.postgresql.Driver")  # PostgreSQL JDBC driver.
    .load()  # Load data into DataFrame.
)

# Write the cleaned dataset back to PostgreSQL, replacing any existing table.
(df.write
 .format("jdbc") # Use JDBC for writing data.
 .option("url", jdbc_url) # Database connection string.
 .option("dbtable", "dim_price") # Destination table name.
 .option("user", db_properties["user"]) # Username for authentication.
 .option("password", db_properties["password"]) # Password for authentication.
 .option("driver", db_properties["driver"]) # Driver for PostgreSQL.
 .mode("overwrite") # Overwrite the table if it exists.
 .save() # Save the DataFrame to the database.
)

# Read cleaned Income table from Supabase into Spark DataFrame.
df = (
    spark.read.format("jdbc")  # Use JDBC connector.
    .option("url", jdbc_url)  # Set JDBC connection URL.
    .option("dbtable", "income_data_cleaned")  # Table name to read.
    .option("user", db_properties["user"])  # Database username.
    .option("password", db_properties["password"])  # Database password.
    .option("driver", "org.postgresql.Driver")  # PostgreSQL JDBC driver.
    .load()  # Load data into DataFrame.
)

# Write the cleaned dataset back to PostgreSQL, replacing any existing table.
(df.write
 .format("jdbc") # Use JDBC for writing data.
 .option("url", jdbc_url) # Database connection string.
 .option("dbtable", "dim_income") # Destination table name.
 .option("user", db_properties["user"]) # Username for authentication.
 .option("password", db_properties["password"]) # Password for authentication.
 .option("driver", db_properties["driver"]) # Driver for PostgreSQL.
 .mode("overwrite") # Overwrite the table if it exists.
 .save() # Save the DataFrame to the database.
)

# Stop Spark session to release resources.
spark.stop()