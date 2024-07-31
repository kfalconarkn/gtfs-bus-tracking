from prefect import flow, task
import requests
import zipfile
import pandas as pd
import os
import psycopg2
from requests.adapters import HTTPAdapter
from urllib3.exceptions import IncompleteRead
from sqlalchemy import create_engine
import gc

# Define constants
STORAGE_DIR = "gtfs_downloads"
ZIP_FILE_URL = "https://gtfsrt.api.translink.com.au/gtfs/SEQ_SCH_GTFS.zip"
DB_NAME = "maindb"
DB_USER = "kfalconar"
DB_PASSWORD = "Teckdeck"
DB_HOST = "localhost"
DB_PORT = "5432"
SCHEMA = "gtfs"

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
@task(log_prints=True, name="Download Zip file from open data portal", retries=5, retry_delay_seconds=30)
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
@task(log_prints=True, name="Extract text files from zip file")
def extract_zip(zip_file_path, storage_dir):
    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        zip_ref.extractall(storage_dir)
        print("Extracted text files from zip file to storage directory")
    return storage_dir

# Step 3: Read and convert text files to individual DataFrames
@task(log_prints=True, name="Read and convert text files to individual DataFrames")
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
@task(log_prints=True, name="Clean the data")
def clean_data(data_frames):
    def clean_df(df, name):
        if name == "calendar.txt":
            df['start_date'] = pd.to_datetime(df['start_date'], format='%Y%m%d')
            df['end_date'] = pd.to_datetime(df['end_date'], format='%Y%m%d')
            print(f"Cleaned data successfully for {name}")
        if name == "stop_times.txt":
            df = df[df['trip_id'].str.contains('SBL')]
            print(f"Filtered stop_times successfully for {name}")
        return df

    cleaned_data_frames = [(name, clean_df(df, name)) for name, df in data_frames]
    return cleaned_data_frames

# Step 5: Upload to a PostgreSQL Database
@task(log_prints=True, name="Upload text files to PostgreSQL Database")
def upload_to_db(cleaned_data_frames, db_name, db_user, db_password, db_host, db_port, schema):
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )
    cursor = conn.cursor()
    print("Connected to PostgreSQL database successfully")
    engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

    def upload(df, table_name):
        full_table_name = f"{schema}.{table_name}"
        cursor.execute(f"DELETE FROM {full_table_name}")
        print(f"cleared {full_table_name} successfully")
        conn.commit()

        # Upload in chunks to avoid memory issues
        print(f"Uploading new data chunks to {full_table_name}")
        df.to_sql(table_name, engine, schema=schema, if_exists='append', index=False, chunksize=10000)
        print(f"Uploaded new data to {full_table_name} successfully")

    for name, df in cleaned_data_frames:
        table_name = os.path.splitext(name)[0]  # Use the file name without extension as the table name
        upload(df, table_name)
        # Clear memory
        del df
        gc.collect()
    
    conn.close()


@task(log_prints=True, name="Creating Duty Time Table from GTFS Data")
def create_duty_time_table(db_name, db_user, db_password, db_host, db_port, schema):
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )
    cursor = conn.cursor()
    print("Connected to PostgreSQL database successfully")
    engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

    ## clear table gtfs.shift_combined
    cursor.execute("DELETE FROM gtfs.shift_combined")
    conn.commit()

    ##Query to upload resulting query data to table gtfs.shift_combined

    query = """
                SELECT 
            s.trip_id,
            s.dty_number,
            t.route_id,
            t.trip_headsign AS headsign,
            st.stop_name,
            stt.departure_time,
            stt.stop_sequence,
            stt.timepoint
        FROM 
            gtfs.shifts s
        JOIN 
            gtfs.trips t ON s.trip_id = t.trip_id
        JOIN 
            gtfs.stop_times stt ON s.trip_id = stt.trip_id
        JOIN 
            gtfs.stops st ON stt.stop_id::text = st.stop_id
        ORDER BY
            s.trip_id,
            stt.stop_sequence;
                """
    
     # Initialize the first chunk to the database
    first_chunk = True

    # Create a cursor for reading data in batches
    cursor.execute(query)

    while True:
        batch_size = 10000
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break

        # Convert to DataFrame
        df = pd.DataFrame(rows, columns=[desc[0] for desc in cursor.description])

        # Upload the batch to the database
        if first_chunk:
            df.to_sql('shift_combined', engine, schema=schema, if_exists='replace', index=False)
            first_chunk = False
        else:
            df.to_sql('shift_combined', engine, schema=schema, if_exists='append', index=False)
        

    print("Data uploaded to gtfs.shift_combined successfully")

    # Close the connection
    cursor.close()
    conn.close()

# Define the flow
@flow(name="Translink GTFS schedule data download flow")
def gtfs_upload():
    os.makedirs(STORAGE_DIR, exist_ok=True)
    zip_file_path = download_zip(ZIP_FILE_URL, STORAGE_DIR)
    extract_dir = extract_zip(zip_file_path, STORAGE_DIR)
    text_data_frames = read_text_files_to_df(extract_dir)
    cleaned_data_frames = clean_data(text_data_frames)
    upload_to_db(cleaned_data_frames, DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, SCHEMA)
    create_duty_time_table(DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, SCHEMA)

# Execute the flow
if __name__ == "__main__":
    gtfs_upload()