import requests
import zipfile
import pandas as pd
import os
from supabase import create_client, Client
from requests.adapters import HTTPAdapter
from urllib3.exceptions import IncompleteRead
import gc
import logging
from datetime import datetime
from tqdm import tqdm
import asyncio

# Define constants
STORAGE_DIR = "/tmp/gtfs_downloads"
ZIP_FILE_URL = "https://gtfsrt.api.translink.com.au/gtfs/SEQ_SCH_GTFS.zip"

## Define supabase keys, url and client
supabase_url = os.environ["SUPABASE_URL"]
supabase_key = os.environ["SUPABASE_KEY"]
supabase: Client = create_client(supabase_url, 
                                 supabase_key
                                 )

## set up logging
class ColoredFormatter(logging.Formatter):
    def format(self, record):
        message = super().format(record)
        # Color numbers in purple and bold using ANSI escape codes
        import re
        message = re.sub(r'(\d+)', '\033[35;1m\\1\033[0m', message)
        
        # Color log levels
        if record.levelname == 'INFO':
            # Blue INFO
            message = message.replace('INFO', '\033[34mINFO\033[0m')
        elif record.levelname == 'ERROR':
            # Red ERROR
            message = message.replace('ERROR', '\033[31mERROR\033[0m')
            
        return message

# Configure root logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Create and configure custom logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = ColoredFormatter('%(asctime)s - %(levelname)s  - %(message)s')
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
async def download_zip(url, storage_dir):
    session = requests.Session()
    adapter = CustomHTTPAdapter()
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    zip_file_path = os.path.join(storage_dir, "downloaded_file.zip")
    
    response = session.get(url, stream=True)
    total_length = int(response.headers.get('content-length', 0))
    
    with open(zip_file_path, "wb") as file:
        with tqdm(
            total=total_length,
            unit='iB',
            unit_scale=True,
            unit_divisor=1024,
            desc="Downloading GTFS file"
        ) as pbar:
            for chunk in response.iter_content(chunk_size=1024):
                size = file.write(chunk)
                pbar.update(size)

    logger.info("Downloaded GTFS file successfully")
    return zip_file_path

# Step 2: Extract the Zip File
async def extract_zip(zip_file_path, storage_dir):
    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        zip_ref.extractall(storage_dir)
        logger.info("Extracted text files from zip file to storage directory")
    return storage_dir

# Step 3: Read and convert text files to individual DataFrames
async def read_text_files_to_df(directory):
    excluded_files = {"feed_info.txt", "routes.txt", "shapes.txt", "agency.txt"}
    tasks = []
    
    async def read_csv_file(file_name, file_path):
        logger.info(f"converting {file_name} to dataframe")
        df = pd.read_csv(file_path, delimiter=',')  # Adjust the delimiter as needed
        logger.info(f"added {file_name} successfully to data frame list")
        return file_name, df
    
    for file_name in os.listdir(directory):
        if file_name.endswith(".txt") and file_name not in excluded_files:
            file_path = os.path.join(directory, file_name)
            tasks.append(asyncio.create_task(read_csv_file(file_name, file_path)))
    
    data_frames = await asyncio.gather(*tasks)
    return list(data_frames)

# Step 4: Clean the data
async def clean_data(data_frames):
    async def clean_df(df, name):
        # Create a copy to avoid modifying original
        df = df.copy()
        
        # Replace NaN values with None/null in one operation
        df = df.replace({pd.NA: None, pd.NaT: None, float('nan'): None})
        
        if name == "calendar.txt":
            # Batch process dates
            df = df[df['service_id'].str.contains('SBL|SUN', na=False)]
            logger.info(f"Filtered {name} successfully")
            df[['start_date', 'end_date']] = df[['start_date', 'end_date']].apply(
                lambda x: pd.to_datetime(x, format='%Y%m%d').dt.strftime('%Y-%m-%d')
            )
            logger.info(f"Ealiest calendar date: {df['start_date'].min()}, Latest calendar date: {df['end_date'].max()}")
            logger.info(f"Cleaned data successfully for {name}")
            
        elif name in ["stop_times.txt", "trips.txt"]:
            # Use boolean indexing for filtering
            mask = df['trip_id'].str.contains('SBL|SUN', na=False)
            df = df[mask]
            logger.info(f"Filtered {name} successfully")
            
        return df

    # Process dataframes concurrently
    tasks = [
        asyncio.create_task(clean_df(df, name)) 
        for name, df in data_frames
    ]
    
    # Gather results maintaining order
    cleaned_dfs = await asyncio.gather(*tasks)
    return list(zip([name for name, _ in data_frames], cleaned_dfs))

# Step 5: Upload to a Supabase Database 
async def upload_to_db(cleaned_data_frames):
    # Define table order for insertion (parent tables first, child tables last)
    insert_order = ['calendar', 'calendar_dates', 'trips', 'stops', 'stop_times']
    
    # Generate a timestamp for this update batch
    current_update = datetime.now().isoformat()
    
    # Organize data frames by table name and add timestamp
    data_frames_dict = {}
    for name, df in cleaned_data_frames:
        table_name = os.path.splitext(name)[0]
        # Add the timestamp column to track this update batch
        df['last_updated'] = current_update
        data_frames_dict[table_name] = df
    
    # Upload data to all tables in the proper order using upsert
    for table_name in insert_order:
        if table_name in data_frames_dict:
            try:
                df = data_frames_dict[table_name]
                # Convert DataFrame to records, handling NaN values
                df_dict = df.replace({pd.NA: None, pd.NaT: None}).to_dict(orient='records')
                
                # Use smaller chunk sizes for stop_times table
                chunk_size = 10000 if table_name == 'stop_times' else 25000
                
                for i in range(0, len(df_dict), chunk_size):
                    chunk = df_dict[i:i + chunk_size]
                    
                    # Use upsert with "on_conflict" to update existing records
                    if table_name == 'trips':
                        supabase.table(table_name).upsert(chunk, on_conflict='trip_id').execute()
                    elif table_name == 'stops':
                        supabase.table(table_name).upsert(chunk, on_conflict='stop_id').execute()
                    elif table_name == 'calendar':
                        supabase.table(table_name).upsert(chunk, on_conflict='service_id').execute()
                    elif table_name == 'calendar_dates':
                        supabase.table(table_name).upsert(chunk, on_conflict='id',).execute()
                    elif table_name == 'stop_times':
                        try:
                            # For stop_times, get unique trip_ids in this chunk
                            trip_ids = list(set([record['trip_id'] for record in chunk]))
                            
                            # Use much smaller batches for delete operations
                            batch_size = 100
                            for j in range(0, len(trip_ids), batch_size):
                                try:
                                    trip_batch = trip_ids[j:j+batch_size]
                                    supabase.from_(table_name).delete().in_('trip_id', trip_batch).execute()
                                    # Add a small delay after each delete operation
                                    await asyncio.sleep(0.2)
                                except Exception as delete_error:
                                    logger.error(f"Error deleting records for trip_ids batch {j//batch_size + 1}: {delete_error}")
                                    # Continue with the next batch instead of failing completely
                                    continue
                            
                            # Split the insert into even smaller chunks
                            insert_chunk_size = 5000
                            for k in range(0, len(chunk), insert_chunk_size):
                                insert_chunk = chunk[k:k + insert_chunk_size]
                                try:
                                    supabase.table(table_name).insert(insert_chunk).execute()
                                    # Add a delay after each insert operation
                                    await asyncio.sleep(0.3)
                                    logger.info(f"Inserted sub-chunk {k//insert_chunk_size + 1} of {len(chunk)//insert_chunk_size + 1} for chunk {i//chunk_size + 1}")
                                except Exception as insert_error:
                                    logger.error(f"Error inserting records for sub-chunk {k//insert_chunk_size + 1}: {insert_error}")
                                    # Try to continue with the next sub-chunk
                                    continue
                        except Exception as chunk_error:
                            logger.error(f"Error processing chunk {i//chunk_size + 1} for {table_name}: {chunk_error}")
                            # Continue with the next chunk instead of failing completely
                            continue
                    
                    logger.info(f"Processed chunk {i//chunk_size + 1} of {len(df_dict)//chunk_size + 1} for {table_name}")
                    
                    # Add a longer delay between chunks to avoid timeouts
                    if i + chunk_size < len(df_dict):
                        await asyncio.sleep(1.0)  # 1 second delay
                
                logger.info(f"Completed updating {table_name} successfully")
                
                # Clear memory
                del df
                gc.collect()
            except Exception as e:
                logger.error(f"Error updating data for {table_name}: {e}")
                # Log the first row that caused the error for debugging
                if 'df_dict' in locals() and len(df_dict) > 0:
                    logger.error(f"First row sample: {df_dict[0]}")
    
    # Delete old records that weren't part of this update with improved batching for trips table
    for table_name in ['trips', 'stops', 'calendar', 'calendar_dates']:
        try:
            if table_name == 'trips':
                # Handle the trips table in smaller batches to avoid timeout
                # First, get the current trip_ids to keep
                logger.info(f"Getting current trip_ids from {table_name} to handle deletion in batches")
                current_trips_response = supabase.from_(table_name).select('trip_id').eq('last_updated', current_update).execute()
                current_trip_ids = [item['trip_id'] for item in current_trips_response.data]
                logger.info(f"Found {len(current_trip_ids)} current trips to keep")
                
                # Now get all trip_ids in the table
                all_trips_response = supabase.from_(table_name).select('trip_id').execute()
                all_trip_ids = [item['trip_id'] for item in all_trips_response.data]
                logger.info(f"Found {len(all_trip_ids)} total trips in database")
                
                # Find trip_ids to delete (those not in current_trip_ids)
                trips_to_delete = [trip_id for trip_id in all_trip_ids if trip_id not in set(current_trip_ids)]
                logger.info(f"Identified {len(trips_to_delete)} trips to delete")
                
                # Delete in small batches with pauses to avoid timeout
                batch_size = 50  # Smaller batch size to prevent timeouts
                for i in range(0, len(trips_to_delete), batch_size):
                    batch = trips_to_delete[i:i + batch_size]
                    try:
                        supabase.from_(table_name).delete().in_('trip_id', batch).execute()
                        logger.info(f"Deleted batch {i//batch_size + 1} of {len(trips_to_delete)//batch_size + 1} from {table_name}")
                        await asyncio.sleep(0.5)  # Pause between batches
                    except Exception as e:
                        logger.error(f"Error deleting batch {i//batch_size + 1} from {table_name}: {e}")
                        # Continue with next batch instead of failing completely
                        await asyncio.sleep(1.0)  # Longer pause after error
                        continue
            else:
                # For other tables, use the original approach
                supabase.from_(table_name).delete().neq('last_updated', current_update).execute()
                logger.info(f"Deleted old records from {table_name}")
        except Exception as e:
            logger.error(f"Error deleting old records from {table_name}: {e}")
    
    logger.info("Database update completed")

async def gtfs_upload():
    os.makedirs(STORAGE_DIR, exist_ok=True)
    zip_file_path = await download_zip(ZIP_FILE_URL, STORAGE_DIR)
    extract_dir = await extract_zip(zip_file_path, STORAGE_DIR)
    text_data_frames = await read_text_files_to_df(extract_dir)
    cleaned_data_frames = await clean_data(text_data_frames)
    await upload_to_db(cleaned_data_frames)

# Execute the flow
if __name__ == "__main__":
    logger.info("Starting GTFS ETL process")
    
    # Run job immediately (no scheduling since GitHub Actions handles that)
    logger.info(f"Running GTFS ETL job at {datetime.now()}")
    try:
        asyncio.run(gtfs_upload())
        logger.info("GTFS ETL job completed successfully")
    except Exception as e:
        logger.error(f"GTFS ETL job failed: {e}")
        # Exit with error code to indicate failure to GitHub Actions
        exit(1) 