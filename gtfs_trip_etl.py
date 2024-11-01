import asyncio
import aiohttp
from google.transit import gtfs_realtime_pb2
from datetime import datetime
import pytz
from supabase import create_client, Client
from prefect import flow, task
from pydantic import BaseModel, ValidationError
from prefect.logging import get_run_logger
from prefect.blocks.system import Secret
import json

### To-do
# - convert secrets to prefect secret blocks
# - change postgres db creds to supabase creds and extract trip shift data from there

#### --------------------------------------------------------------------------------------------- ####
####    This Prefect flow downloads GTFS data and process the trip, stop and bus locations.
#### --------------------------------------------------------------------------------------------- ####

## define timezone
brisbane_tz = pytz.timezone('Australia/Brisbane')



## Define supabase keys, url and client
secret_block_url = Secret.load("supabase-url")
supabase_url = secret_block_url.get()
secret_block_key = Secret.load("supabase-key")
supabase_key = secret_block_key.get()
supabase: Client = create_client(supabase_url, supabase_key)


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
    logger = get_run_logger()
    
    # Convert stop_ids to integers and deduplicate
    try:
        int_stop_ids = list(set(int(sid) for sid in stop_ids))
        logger.info(f"Total unique stop_ids from GTFS: {len(int_stop_ids)}")
    except ValueError as e:
        logger.error(f"Error converting stop_ids to integers: {e}")
        return {}, {}

    try:
        # Query for unique stop_id and timepoint combinations
        timing_point_result = supabase.table('stop_times')\
            .select('stop_id, timepoint')\
            .in_('stop_id', int_stop_ids)\
            .execute()\
            .data

        # Get unique combinations using a dictionary comprehension
        timingpoint_data = {
            str(row['stop_id']): row['timepoint'] 
            for row in timing_point_result
        }
        
        logger.info(f"Total timepoints retrieved: {len(timingpoint_data)}")
        
    except Exception as e:
        logger.error(f"Error fetching timing point data: {e}")
        logger.error(f"Error details: {str(e)}")
        timingpoint_data = {}

    # Process shift data in a single query
    try:
        shift_result = supabase.table('shifts')\
            .select('trip_id, dty_number')\
            .in_('trip_id', list(trip_ids))\
            .execute()
        shift_data = {row['trip_id']: row['dty_number'] for row in shift_result.data}
        logger.info(f"Retrieved shift data for {len(shift_data)} trips")
    except Exception as e:
        logger.error(f"Error fetching shift data: {e}")
        shift_data = {}
    
    return timingpoint_data, shift_data



@task(name="Process Trip Updates", log_prints=True)
async def process_trip_updates():
    logger = get_run_logger()
    logger.info("Starting to process trip updates...")
    stop_updates = await fetch_and_process_trip_updates()
    logger.info(f"Fetched {len(stop_updates)} stop updates")
    
    # Extract unique stop_ids and trip_ids
    stop_ids = set(update['stop_id'] for update in stop_updates)
    trip_ids = set(update['trip_id'] for update in stop_updates)
    logger.info(f"Extracted {len(stop_ids)} unique stop IDs and {len(trip_ids)} unique trip IDs")
    
    # Get timing point and shift data
    timingpoint_data, shift_data = await get_timepoint_shift(list(stop_ids), list(trip_ids))
    logger.info(f"Fetched timing point data for {len(timingpoint_data)} stops and shift data for {len(shift_data)} trips")
    
    # Joining timepoint and shift data to stop_updates
    logger.info("Joining timepoint and shift data to stop_updates...")
    enriched_updates = []
    missing_timepoints = set()
    
    for update in stop_updates:
        enriched_update = update.copy()
        stop_id = update['stop_id']
        trip_id = update['trip_id']
        
        timepoint = timingpoint_data.get(stop_id)
        if timepoint is None:
            missing_timepoints.add(stop_id)
            if len(missing_timepoints) <= 5:  # Log only first 5 missing timepoints
                logger.warning(f"No timepoint found for stop_id: {stop_id}")
        
        dty_number = shift_data.get(trip_id)
        
        enriched_update['timepoint'] = timepoint
        enriched_update['dty_number'] = dty_number
        enriched_updates.append(enriched_update)
    
    logger.info(f"Processed and joined data. Missing timepoints: {len(missing_timepoints)}")
    if missing_timepoints:
        logger.warning(f"Sample of stops missing timepoints: {list(missing_timepoints)[:10]}")
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
