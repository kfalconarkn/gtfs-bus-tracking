import asyncio
import aiohttp
from google.transit import gtfs_realtime_pb2
from datetime import datetime
import pytz
from supabase import create_client, Client
from prefect import flow, task
from pydantic import BaseModel, ValidationError
from prefect.logging import get_run_logger
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine
import json



#### --------------------------------------------------------------------------------------------- ####
####    This Prefect flow downloads GTFS data and process the trip, stop and bus locations.
#### --------------------------------------------------------------------------------------------- ####

## define timezone
brisbane_tz = pytz.timezone('Australia/Brisbane')

## Define supabase keys, url and client
supabase_url = "https://zfzjlwllauahcldszhhz.supabase.co"
supabase_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inpmempsd2xsYXVhaGNsZHN6aGh6Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3MjcyNTAyODAsImV4cCI6MjA0MjgyNjI4MH0.H5oAX0_YbYVQWV0w3iLaroNXQVqpUmVVI9fvKl-Quyo"
supabase: Client = create_client(supabase_url, supabase_key)

# Define constants for postgres db
DB_NAME = "maindb"
DB_USER = "kfalconar"
DB_PASSWORD = "Teckdeck"
DB_HOST = "localhost"
DB_PORT = "5432"
SCHEMA = "gtfs"


## define data models for supabase upload
class StopUpdate(BaseModel):
    trip_id: str
    route_id: str
    vehicle_id: str | None
    stop_sequence: int
    stop_id: str
    arrival_delay: int | None
    arrival_time: str | None
    departure_delay: int | None
    departure_time: str | None
    date: str | None
    timepoint: int | None = None
    dty_number: str | None = None

    
class TripOTR(BaseModel):
    trip_id: str
    route_id: str | None
    departure_time: str | None
    date: str | None
    otr: float | None
    

async def fetch_trip_updates():
    """
    Asynchronously fetches GTFS real-time trip updates from the Translink API.

    This function sends a GET request to the Translink API endpoint for SEQ (South East Queensland)
    trip updates and returns the raw response data.

    Returns:
        bytes: The raw response data containing GTFS real-time trip updates.

    Raises:
        aiohttp.ClientError: If there's an error in making the HTTP request or receiving the response.
    """
    url = "https://gtfsrt.api.translink.com.au/api/realtime/SEQ/TripUpdates"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.read()

async def process_entity_trip_updates(entity):
    """
    Process GTFS real-time trip updates for a single entity.

    This function parses the given entity data, extracts relevant trip and stop information,
    and returns a list of dictionaries containing details about each stop update.

    Args:
        entity (FeedEntity): GTFS real-time entity data.

    Returns:
        list: A list of dictionaries, where each dictionary contains information about
        a stop update for a specific trip.
    """
    stop_updates = []
    if entity.HasField('trip_update'):
        trip_id = entity.trip_update.trip.trip_id
        if 'SBL' not in trip_id:
            return stop_updates
        
        for stop in entity.trip_update.stop_time_update:
            stop_info = {
                'trip_id': trip_id,
                'route_id': entity.trip_update.trip.route_id,
                'stop_sequence': stop.stop_sequence,
                'stop_id': stop.stop_id,
                'vehicle_id': entity.trip_update.vehicle.id if entity.trip_update.HasField('vehicle') else None,
                'arrival_delay': stop.arrival.delay if stop.HasField('arrival') else None,
                'arrival_time': datetime.fromtimestamp(stop.arrival.time, tz=pytz.UTC).astimezone(pytz.timezone('Australia/Brisbane')).strftime('%Y-%m-%d %H:%M:%S') if stop.HasField('arrival') else None,
                'departure_delay': stop.departure.delay if stop.HasField('departure') else None,
                'departure_time': datetime.fromtimestamp(stop.departure.time, tz=pytz.UTC).astimezone(pytz.timezone('Australia/Brisbane')).strftime('%Y-%m-%d %H:%M:%S') if stop.HasField('departure') else None,
                'date': datetime.strptime(entity.trip_update.trip.start_date, '%Y%m%d').strftime('%Y-%m-%d'),
                'start_time': entity.trip_update.trip.start_time if entity.trip_update.HasField('trip') else None
            }
            stop_updates.append(stop_info)

    return stop_updates 



async def fetch_and_process_trip_updates():
    """
    Fetches and processes GTFS real-time trip updates.

    This function performs the following steps:
    1. Fetches raw GTFS real-time data from the API.
    2. Parses the fetched data into a FeedMessage object.
    3. Processes each entity in the feed concurrently.
    4. Collects and organizes the processed data into stop updates.

    Returns:
        list: A list of processed stop update information. Each item in the list
              is a dictionary containing details about a specific stop update.

    Note:
        This function filters for trips with 'SBL' in their trip_id and uses the
        Brisbane timezone for time conversions.
    """
    content = await fetch_trip_updates()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(content)
    
    # Process entities directly instead of creating tasks
    stop_updates = []
    for entity in feed.entity:
        entity_stop_updates = await process_entity_trip_updates(entity)
        if entity_stop_updates:
            stop_updates.extend(entity_stop_updates)

    return stop_updates

## get timining point data and shift data from localhost postgres db
async def get_timepoint_shift(stop_ids, trip_ids):
    """Function returns timepoint and shift data for given stop_ids and trip_ids"""
    
    # Create async SQLAlchemy engine
    engine = create_async_engine(f'postgresql+asyncpg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    # Convert stop_ids to integers
    stop_ids = [int(stop_id) for stop_id in stop_ids]

    timepoint_query = text("""
        SELECT stop_id, timepoint
        FROM gtfs.stop_times
        WHERE stop_id = ANY(:stop_ids)
    """)

    shift_query = text("""
        SELECT trip_id, dty_number
        from gtfs.shifts
        where trip_id = ANY(:trip_ids)
    """)
    
    # Execute query asynchronously
    async with engine.connect() as conn:
        result = await conn.execute(timepoint_query, {"stop_ids": stop_ids})
        timing_point_data = result.fetchall()

        result = await conn.execute(shift_query, {"trip_ids": trip_ids})
        shift_data = result.fetchall()

    # Convert result to dictionaries
    timing_point_dict = {str(row[0]): row[1] for row in timing_point_data}
    shift_dict = {row[0]: row[1] for row in shift_data}
    
    await engine.dispose()
    return timing_point_dict, shift_dict



@task(name="Process Trip Updates", log_prints=True)
async def process_trip_updates():
    print("Starting to process trip updates...")
    stop_updates = await fetch_and_process_trip_updates()
    print(f"Fetched {len(stop_updates)} stop updates")
    
    # Extract unique stop_ids and trip_ids
    stop_ids = set(update['stop_id'] for update in stop_updates)
    trip_ids = set(update['trip_id'] for update in stop_updates)
    print(f"Extracted {len(stop_ids)} unique stop IDs and {len(trip_ids)} unique trip IDs")
    
    # Get timing point and shift data
    timing_point_dict, shift_dict = await get_timepoint_shift(list(stop_ids), list(trip_ids))
    print(f"Fetched timing point data for {len(timing_point_dict)} stops and shift data for {len(shift_dict)} trips")
    
    # Joining timepoint and shift data to stop_updates
    print("Joining timepoint and shift data to stop_updates...")
    enriched_updates = []
    for update in stop_updates:
        enriched_update = update.copy()  # Create a copy of the original update
        timepoint = timing_point_dict.get(update['stop_id'])
        dty_number = shift_dict.get(update['trip_id'])
        enriched_update['timepoint'] = timepoint
        enriched_update['dty_number'] = dty_number
        enriched_updates.append(enriched_update)
    
    print(f"Processed and joined shift and timing point data for {len(enriched_updates)} trip_id's")
    return enriched_updates



@task(name="Upload Trip Updates to Supabase", log_prints=True)
async def upload_trip_updates_to_supabase(stop_updates):
    logger = get_run_logger()
    
    valid_updates = []
    invalid_updates = 0
    for update in stop_updates:
        try:
            StopUpdate(**update)
            valid_updates.append(update)
        except ValidationError as e:
            logger.warning(f"Validation error in stop_update data: {e}")
            logger.warning(f"Skipping problematic update: {update}")
            invalid_updates += 1
    
    logger.info(f"Valid updates: {len(valid_updates)}, Invalid updates: {invalid_updates}")
    
    if not valid_updates:
        logger.warning("No valid updates to upload")
        return

    try:
        response = supabase.table('stops_update').upsert(
            valid_updates,
            on_conflict='trip_id,stop_sequence,departure_time'
        ).execute()
        logger.info(f"Bulk updated: {len(response.data)} stops data")
    except Exception as e:
        logger.error(f"Error bulk updating stops data: {e}")
    


@flow(name="GTFS Trip update process", log_prints=True)
async def main_flow():
    stop_updates = await process_trip_updates()
    await upload_trip_updates_to_supabase(stop_updates)


if __name__ == "__main__":
    asyncio.run(main_flow())
