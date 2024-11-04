import requests
import zipfile
import pandas as pd
import os
from supabase import create_client, Client
from requests.adapters import HTTPAdapter
from urllib3.exceptions import IncompleteRead
import gc
import logging
import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration
import schedule
import time
from datetime import datetime

# Define constants
STORAGE_DIR = "gtfs_downloads"
ZIP_FILE_URL = "https://gtfsrt.api.translink.com.au/gtfs/SEQ_SCH_GTFS.zip"

## Define supabase keys, url and client
supabase_url = os.environ["SUPABASE_URL"]
supabase_key = os.environ["SUPABASE_KEY"]
supabase: Client = create_client(supabase_url, supabase_key)

## set up logging
class ColoredNumbersFormatter(logging.Formatter):
    def format(self, record):
        message = super().format(record)
        # Color numbers in purple and bold using ANSI escape codes
        import re
        return re.sub(r'(\d+)', '\033[35;1m\\1\033[0m', message)

# Configure root logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Create and configure custom logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = ColoredNumbersFormatter('%(asctime)s - %(levelname)s  - %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.handlers = [handler]

# Suppress unwanted logs
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('httpcore').setLevel(logging.WARNING)
logging.getLogger('aiohttp.client').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

# Prevent log propagation to root logger to avoid duplicate messages
logger.propagate = False


class CustomHTTPAdapter(HTTPAdapter):
    def build_response(self, req, resp):
        response = super(CustomHTTPAdapter, self).build_response(req, resp)
        original_read = response.raw.read
        
        def patched_read(*args, **kwargs):
            try:
                return original_read(*args, **kwargs)
            except IncompleteRead as e:
                return e.partial
        
        response.raw.read = patched_read
        return response

# Step 1: Download the Zip File
def download_zip(url, storage_dir):
    session = requests.Session()
    adapter = CustomHTTPAdapter()
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    response = session.get(url)
    print("Downloaded GTFS file successfully")
    
    zip_file_path = os.path.join(storage_dir, "downloaded_file.zip")
    with open(zip_file_path, "wb") as file:
        file.write(response.content)
        print("Saved Zip file to storage directory")
    
    return zip_file_path

# Step 2: Extract the Zip File
def extract_zip(zip_file_path, storage_dir):
    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        zip_ref.extractall(storage_dir)
        print("Extracted text files from zip file to storage directory")
    return storage_dir

# Step 3: Read and convert text files to individual DataFrames
def read_text_files_to_df(directory):
    excluded_files = {"feed_info.txt", "routes.txt", "shapes.txt", "agency.txt"}
    data_frames = []
    for file_name in os.listdir(directory):
        if file_name.endswith(".txt") and file_name not in excluded_files:
            file_path = os.path.join(directory, file_name)
            print(f"converting {file_name} to dataframe")
            df = pd.read_csv(file_path, delimiter=',')  # Adjust the delimiter as needed
            data_frames.append((file_name, df))
            print(f"added {file_name} successfully to data frame list")
    return data_frames

# Step 4: Clean the data
def clean_data(data_frames):
    def clean_df(df, name):
        # Replace NaN values with None/null
        df = df.replace({pd.NA: None, pd.NaT: None, float('nan'): None})
        
        if name == "calendar.txt":
            # Convert to string format that Supabase can handle
            df['start_date'] = pd.to_datetime(df['start_date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
            df['end_date'] = pd.to_datetime(df['end_date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
            print(f"Cleaned data successfully for {name}")
        if name == "stop_times.txt":
            df = df[df['trip_id'].str.contains('SBL|SUN')]
            print(f"Filtered stop_times successfully for {name}")
        if name == "trips.txt":
            df = df[df['trip_id'].str.contains('SBL|SUN')]
            print(f"Filtered trips successfully for {name}")
        return df

    cleaned_data_frames = [(name, clean_df(df, name)) for name, df in data_frames]
    return cleaned_data_frames

# Step 5: Upload to a Supabase Database 
def upload_to_db(cleaned_data_frames):
    def upload(df, table_name):
        try:
            # Call the truncate_table function to clear the table
            supabase.rpc('truncate_table', {'table_name_param': table_name}).execute()
            logger.info(f"Cleared table {table_name} successfully")
        except Exception as e:
            logger.error(f"Error clearing table {table_name}: {e}")
        
        try:
            # Convert DataFrame to records, handling NaN values
            df_dict = df.replace({pd.NA: None, pd.NaT: None}).to_dict(orient='records')
            
            # Insert data in smaller chunks 
            chunk_size = 10000  
            for i in range(0, len(df_dict), chunk_size):
                chunk = df_dict[i:i + chunk_size]
                supabase.table(table_name).insert(chunk).execute()
                logger.info(f"Uploaded chunk {i//chunk_size + 1} of {len(df_dict)//chunk_size + 1} to {table_name}")
            logger.info(f"Completed uploading all chunks to {table_name} successfully")
        except Exception as e:
            logger.error(f"Error uploading data to {table_name}: {e}")
            # Log the first row that caused the error for debugging
            if len(df_dict) > 0:
                logger.error(f"First row sample: {df_dict[0]}")

    for name, df in cleaned_data_frames:
        table_name = os.path.splitext(name)[0]
        upload(df, table_name)
        # Clear memory
        del df
        gc.collect()
    


def create_duty_time_table():
    try:
        # Clear the dty_sheet table first
        supabase.rpc('truncate_table', {'table_name_param': 'dty_sheet'}).execute()
        logger.info("Cleared dty_sheet table successfully")
    except Exception as e:
        logger.error(f"Error clearing dty_sheet table: {e}")
        raise e

    try:
        # Call the select_trip_data function and get the results
        response = supabase.rpc('select_trip_data').execute()
        
        if response.data:
            # Insert the results into dty_sheet table
            chunk_size = 10000
            data = response.data
            for i in range(0, len(data), chunk_size):
                chunk = data[i:i + chunk_size]
                supabase.table('dty_sheet').insert(chunk).execute()
                logger.info(f"Uploaded chunk {i//chunk_size + 1} of {len(data)//chunk_size + 1} to dty_sheet")
            
            logger.info(f"Successfully processed {len(data)} records into dty_sheet table")
        else:
            logger.warning("No data returned from select_trip_data function")
            
    except Exception as e:
        logger.error(f"Error creating dty_sheet table: {e}")
        raise e

    logger.info("Data uploaded to dty_sheet successfully")



def gtfs_upload():
    os.makedirs(STORAGE_DIR, exist_ok=True)
    zip_file_path = download_zip(ZIP_FILE_URL, STORAGE_DIR)
    extract_dir = extract_zip(zip_file_path, STORAGE_DIR)
    text_data_frames = read_text_files_to_df(extract_dir)
    cleaned_data_frames = clean_data(text_data_frames)
    upload_to_db(cleaned_data_frames)
    create_duty_time_table()

# Execute the flow
if __name__ == "__main__":
    # Initialize Sentry with additional configuration
    sentry_sdk.init(
        dsn="https://c0bcb112b40673561cb681dc57be4a58@o4508230906347520.ingest.us.sentry.io/4508230913818624",
        
        # Set traces_sample_rate to 1.0 to capture 100% of transactions for performance monitoring.
        # We recommend adjusting this value in production.
        traces_sample_rate=1.0,
        
        # Enable performance monitoring
        enable_tracing=True,
        
        # Set integrations
        integrations=[
            # Capture all logging messages as breadcrumbs
            LoggingIntegration(
                level=logging.INFO,
                event_level=logging.ERROR
            ),
        ],
        
        # Set environment
        environment="development",  # Change to "production" in prod
        
        # Enable performance monitoring of specific functions
        functions_to_trace=[
            {'qualified_name': 'gtfs_etl.download_zip'},
            {'qualified_name': 'gtfs_etl.extract_zip'}, 
            {'qualified_name': 'gtfs_etl.read_text_files_to_df'},
            {'qualified_name': 'gtfs_etl.clean_data'},
            {'qualified_name': 'gtfs_etl.upload_to_db'},
            {'qualified_name': 'gtfs_etl.create_duty_time_table'}
        ],
        
        # Configure sample rate for errors
        sample_rate=1.0,
        
        # Add release information (optional)
        release="1.0.0"  # You can use git commit hash or version number
    )

    logger.info("Starting GTFS ETL scheduler")
    
    def job():
        logger.info(f"Running GTFS ETL job at {datetime.now()}")
        with sentry_sdk.start_transaction(op="task", name="gtfs data upload"):
            try:
                gtfs_upload()
                logger.info("GTFS ETL job completed successfully")
            except Exception as e:
                logger.error(f"GTFS ETL job failed: {e}")
                sentry_sdk.capture_exception(e)

    # Schedule the job to run at 2 AM on Tuesday, Thursday, and Saturday
    schedule.every().tuesday.at("02:00").do(job)
    schedule.every().thursday.at("02:00").do(job)
    schedule.every().saturday.at("02:00").do(job)

    logger.info("Scheduler initialized. Waiting for next run time...")

    # Keep the script running
    while True:
        schedule.run_pending()
        time.sleep(3600)  # Check every hour (3600 seconds) instead of every minute