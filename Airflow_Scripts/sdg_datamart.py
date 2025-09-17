
# =================================================

# Final Project

# Grup : 001

# Batch : CODA-RMT-008

# Python script ini digunakan untuk melakukan automasi pembuatan data mart untuk keperluan dashboard, dimana menggunakan api dari data warehouse lalu api keluar ke google drive (google sheet).

# =================================================

import pandas as pd  # Import pandas library for data manipulation and analysis.
import gspread  # Import gspread to interact with Google Sheets via Python.
from google.oauth2.service_account import Credentials  # Import Credentials class for Google service account authentication.
from sqlalchemy import create_engine  # Import SQLAlchemy create_engine to connect to PostgreSQL.
import os  # Import os module to interact with the operating system (e.g., check if file exists).
from airflow.models import Variable  # Import Airflow Variable to store and retrieve configuration or secrets (e.g., credential paths) from the Airflow UI.


# Supabase (PostgreSQL) configuration.
JDBC_DRIVER_PATH = "/opt/airflow/jars/postgresql-42.7.7.jar"  # Path to PostgreSQL JDBC driver.
SUPABASE_HOST = "aws-1-ap-southeast-1.pooler.supabase.com"  # Supabase host endpoint.
SUPABASE_PORT = "5432"  # PostgreSQL default port.
SUPABASE_DB = "postgres"  # Name of the Supabase database.
SUPABASE_USER = "postgres.jinlmqdphisxprlvuiqb"  # Supabase user name.
SUPABASE_PASSWORD = "jtT8IQ5dA8xPw0mQ"  # Supabase password.
JDBC_URL = f"jdbc:postgresql://{SUPABASE_HOST}:{SUPABASE_PORT}/{SUPABASE_DB}"  # JDBC connection URL for PostgreSQL.

# Google Sheets configuration.
CREDENTIALS_FILE = Variable.get(
    "GOOGLE_CREDENTIALS_PATH",
    default_var="/opt/airflow/data/api_keys/apikeyweb-1541242050469-eeba1d2f811d.json" # Path to Google service account JSON key file.
)
GOOGLE_SHEET_NAME = "EV_Data_Mart"  # Name of the Google Sheet to write data into.
SHEET_NAME = "DM_Data"  # Name of the specific worksheet inside the Google Sheet.

# Verify credentials file exists.
if not os.path.exists(CREDENTIALS_FILE):  # Check if the JSON key file exists in the specified path.
    exit()  # Exit the script if credentials file is missing.

# Initialize Google Sheets API client.
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']  # Define scopes for Google Sheets and Drive access.
try:
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)  # Load credentials from the service account JSON file.
    client = gspread.authorize(creds)  # Authorize gspread client using the credentials.
except Exception as e:  # Catch any exception during authentication.
    exit()  # Exit the script if Google Sheets authentication fails.

# SQL query for joining tables.
SQL_QUERY = """
-- Calculate EV registrations per county with demographic and income information.
select
  c.county              as county,  -- County name.
  pe.pop_2025           as pop_2025,  -- Population estimate for 2025.
  inc.income            as avg_wage,  -- Average income for the county.
  sum(f.dol_vehicle_count) as count_ev,  -- Total number of EVs registered in the county.
  case 
    when pe.pop_2025 > 0 
      then (sum(f.dol_vehicle_count)::numeric / pe.pop_2025) * 1000  -- EVs per 1,000 people.
    else null 
  end as ev_penetration  -- EV penetration rate per 1,000 population.
from public.fact_ev_registration f  -- Fact table with EV registrations.
join public.dim_location l  on l.location_id = f.location_id  -- Join to get location dimension.
join public.dim_county c  on c.county_id = l.county_id  -- Join to get county information.
left join public.dim_pop_est pe  -- Join population estimates for the county if available.
       on pe.county = c.county
      and pe.pop_2025 is not null
left join public.dim_income inc  -- Join income data for the county if available.
       on inc.county = c.county
group by 
  c.county,  -- Group by county to aggregate EV counts.
  pe.pop_2025,  -- Include population in grouping to compute penetration.
  inc.income  -- Include income in grouping to associate demographic info.
;
"""

# Create SQLAlchemy engine for Supabase.
try:
    engine = create_engine(
        f"postgresql+psycopg2://{SUPABASE_USER}:{SUPABASE_PASSWORD}@{SUPABASE_HOST}:{SUPABASE_PORT}/{SUPABASE_DB}?sslmode=require"  # Connection string including SSL mode requirement.
    )
    print("Connected to PostgreSQL via SQLAlchemy!")  # Print success message.

    # Execute query and load into DataFrame.
    with engine.connect() as conn:  # Open connection context.
        df = pd.read_sql(SQL_QUERY, conn)  # Execute SQL query and load result into pandas DataFrame.
    
    # Check the results.
    print("\nFirst 5 rows of the query result:")  # Informative message.
    print(df.head())  # Display first 5 rows of DataFrame.
    print("\nDataFrame Info:")  # Informative message.
    print(df.info())  # Display DataFrame summary information.

    # Connect to Google Sheets.
    sheet = client.open(GOOGLE_SHEET_NAME)  # Open the target Google Sheet.
    
    # Try to access the worksheet, create if it doesn't exist.
    try:
        worksheet = sheet.worksheet(SHEET_NAME)  # Try to access existing worksheet.
    except gspread.exceptions.WorksheetNotFound:  # Handle case where worksheet does not exist.
        worksheet = sheet.add_worksheet(title=SHEET_NAME, rows=100, cols=20)  # Create a new worksheet with 100 rows and 20 columns.
        print(f"Created new worksheet: {SHEET_NAME}")  # Print confirmation of worksheet creation.
    
    # Clear existing content in the worksheet.
    worksheet.clear()  # Remove any existing data in the worksheet.
    print(f"Cleared existing content in {SHEET_NAME}")  # Print confirmation.

    # Convert DataFrame to list of lists (including headers).
    data = [df.columns.tolist()] + df.values.tolist()  # Convert DataFrame to format compatible with Google Sheets API.
    
    # Write data to Google Sheet.
    worksheet.update("A1", data)  # Upload data starting at cell A1.
    print(f"Data written to Google Sheet '{GOOGLE_SHEET_NAME}' in worksheet '{SHEET_NAME}'")  # Confirm data upload.

except Exception as e:  # Catch any exception during database or Google Sheets operations.
    print(f"Error: {e}")  # Print the error message.
finally:
    if 'engine' in locals():  # Check if the engine was created.
        engine.dispose()  # Dispose of the SQLAlchemy engine to close connection.
        print("Database engine disposed.")  # Confirm engine disposal.

# Google Sheets configuration for second worksheet.
GOOGLE_SHEET_NAME = "EV_Data_Mart"  # Name of the Google Sheet.
SHEET_NAME = "DM_Cars"  # Name of the second worksheet.

# Verify credentials file exists.
if not os.path.exists(CREDENTIALS_FILE):  # Check if JSON key file exists.
    exit()  # Exit script if not found.

# Initialize Google Sheets API client.
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']  # Define scopes.
try:
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)  # Load credentials.
    client = gspread.authorize(creds)  # Authorize client.
except Exception as e:  # Catch authentication errors.
    exit()  # Exit if authentication fails.

# SQL query for joining tables.
SQL_QUERY = """
-- Calculate EV registrations per county, including vehicle brand, model, and pricing information.
select
  c.county              as county,  -- County name.
  b.make                as brand,  -- Vehicle brand name.
  m.model               as model,  -- Vehicle model name.
  p.price_usd           as price,  -- Vehicle price in USD.
  sum(f.dol_vehicle_count) as count_ev  -- Total EV registrations for the brand and model in the county.
from public.fact_ev_registration f  -- Fact table containing EV registrations.
join public.dim_location l  on l.location_id = f.location_id  -- Join to get location information.
join public.dim_county c  on c.county_id = l.county_id  -- Join to get county information.
join public.dim_vehicle dv on dv.vehicle_id = f.vehicle_id  -- Join vehicle dimension to get vehicle surrogate keys.
join public.dim_brand   b  on b.brand_id = dv.brand_id  -- Join brand dimension to get brand name.
join public.dim_model   m  on m.model_id = dv.model_id  -- Join model dimension to get model name.
left join public.dim_price p on p.model = m.model  -- Left join ensures all EVs are included even if price is missing.
group by 
  c.county,  -- Group by county for aggregation.
  b.make,    -- Group by brand to aggregate registrations per brand.
  m.model,   -- Group by model to aggregate registrations per model.
  p.price_usd  -- Include price in grouping to associate pricing with each brand-model-county combination.
;
"""

# Create SQLAlchemy engine for Supabase.
try:
    engine = create_engine(
        f"postgresql+psycopg2://{SUPABASE_USER}:{SUPABASE_PASSWORD}@{SUPABASE_HOST}:{SUPABASE_PORT}/{SUPABASE_DB}?sslmode=require"  # Connection string with SSL.
    )
    print("Connected to PostgreSQL via SQLAlchemy!")  # Print success message.

    # Execute query and load into DataFrame.
    with engine.connect() as conn:  # Open database connection.
        df = pd.read_sql(SQL_QUERY, conn)  # Execute query and store results in DataFrame.
    
    # Check the results.
    print("\nFirst 5 rows of the query result:")  # Informative message.
    print(df.head())  # Display first 5 rows.
    print("\nDataFrame Info:")  # Informative message.
    print(df.info())  # Display DataFrame summary.

    # Connect to Google Sheets.
    sheet = client.open(GOOGLE_SHEET_NAME)  # Open Google Sheet.
    
    # Try to access the worksheet, create if it doesn't exist.
    try:
        worksheet = sheet.worksheet(SHEET_NAME)  # Try to access existing worksheet.
    except gspread.exceptions.WorksheetNotFound:  # Handle worksheet not found case.
        worksheet = sheet.add_worksheet(title=SHEET_NAME, rows=100, cols=20)  # Create new worksheet with 100 rows and 20 columns.
        print(f"Created new worksheet: {SHEET_NAME}")  # Confirm creation.
    
    # Clear existing content in the worksheet.
    worksheet.clear()  # Remove existing data.
    print(f"Cleared existing content in {SHEET_NAME}")  # Confirm clearing.

    # Convert DataFrame to list of lists (including headers).
    data = [df.columns.tolist()] + df.values.tolist()  # Prepare data for Google Sheets API.
    
    # Write data to Google Sheet.
    worksheet.update("A1", data)  # Upload data starting from A1.
    print(f"Data written to Google Sheet '{GOOGLE_SHEET_NAME}' in worksheet '{SHEET_NAME}'")  # Confirm upload.

except Exception as e:  # Catch exceptions.
    print(f"Error: {e}")  # Print error message.
finally:
    if 'engine' in locals():  # Check if engine exists.
        engine.dispose()  # Close database connection.
        print("Database engine disposed.")  # Confirm disposal.
