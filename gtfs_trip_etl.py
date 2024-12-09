import asyncio
import aiohttp
from google.transit import gtfs_realtime_pb2
from datetime import datetime
import pytz
from supabase import create_client, Client
from pydantic import BaseModel, ValidationError
import logging
import os
import sentry_sdk
from sentry_sdk.integrations.asyncio import AsyncioIntegration
from sentry_sdk.integrations.logging import LoggingIntegration
import time



#### --------------------------------------------------------------------------------------------- ####
####    This script downloads GTFS data and stores the tables into a supabase database,
####    it also creates the duty sheets for both Surfside and Sunbus SunshineCoast
#### --------------------------------------------------------------------------------------------- ####

## define timezone
brisbane_tz = pytz.timezone('Australia/Brisbane')

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

## Define supabase keys, url and client
supabase_url = os.environ["SUPABASE_URL"]
supabase_key = os.environ["SUPABASE_KEY"]
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
    

@sentry_sdk.trace
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

@sentry_sdk.trace
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



@sentry_sdk.trace
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
@sentry_sdk.trace
async def get_timepoint_shift(stop_ids, trip_ids):
    """Function returns timepoint and shift data for given stop_ids and trip_ids"""
    
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

@sentry_sdk.trace
async def get_trip_headsign(trip_ids):
    """Function returns trip_headsign data for given trip_ids"""
    try:
        result = supabase.table('trips').select('trip_id, trip_headsign').in_('trip_id', list(trip_ids)).execute()
        return {row['trip_id']: row['trip_headsign'] for row in result.data}
    except Exception as e:
        logger.error(f"Error fetching trip headsign data: {e}")
        return {}


@sentry_sdk.trace
async def process_trip_updates():
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
    
    # Get trip headsign data
    trip_headsign_data = await get_trip_headsign(list(trip_ids))
    logger.info(f"Fetched trip headsign data for {len(trip_headsign_data)} trips")
    
    # Joining timepoint and shift data to stop_updates. also join trip_headsign data
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
        enriched_update['headsign'] = trip_headsign_data.get(trip_id)
        enriched_updates.append(enriched_update)
    
    logger.info(f"Processed and joined data. Missing timepoints: {len(missing_timepoints)}")
    if missing_timepoints:
        logger.warning(f"Sample of stops missing timepoints: {list(missing_timepoints)[:10]}")
    return enriched_updates


@sentry_sdk.trace
async def upload_trip_updates_to_supabase(stop_updates):
    
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
    
    ##data cleanup step before upsert
    ## clean the route_id field to remove everything after the first 3 characters - optimized using list comprehension
    valid_updates = [{**update, 'route_id': update['route_id'][:3]} for update in valid_updates]

    try:
        response = supabase.table('stops_update').upsert(
            valid_updates,
            on_conflict='trip_id,stop_sequence,stop_id,date'
        ).execute()
        logger.info(f"Bulk updated: {len(response.data)} stops data")
    except Exception as e:
        logger.error(f"Error bulk updating stops data: {e}")
    


@sentry_sdk.trace
async def main_flow():
    with sentry_sdk.start_span(op="process_trip_updates") as span:
        stop_updates = await process_trip_updates()
        span.set_data("update_count", len(stop_updates))
    
    with sentry_sdk.start_span(op="upload_trip_updates_to_supabase"):
        await upload_trip_updates_to_supabase(stop_updates)
        
    with sentry_sdk.start_span(op="aggregate & upsert trip & shift otr data"):
        ##trigger supabase database function
        try:
            supabase.rpc('calculate_trip_otr').execute()
            logger.info(f"trip otr data aggregated and upserted successfully")
            time.sleep(3)
            supabase.rpc('calculate_shift_otr').execute()
            logger.info(f"shift otr data aggregated and upserted successfully")
        except Exception as e:
            logger.error(f"Error upserting trip otr data: {e}")

if __name__ == "__main__":
    # Initialize Sentry with additional configuration
    sentry_sdk.init(
        dsn="https://c0bcb112b40673561cb681dc57be4a58@o4508230906347520.ingest.us.sentry.io/4508230913818624",
        
        # Set traces_sample_rate to 1.0 to capture 100% of transactions for performance monitoring.
        # We recommend adjusting this value in production.
        traces_sample_rate=1.0,
        
        # Enable performance monitoring
        enable_tracing=True,
        
        # Integrate with asyncio
        integrations=[
            AsyncioIntegration(),
            # Capture all logging messages as breadcrumbs
            LoggingIntegration(
                level=logging.INFO,        # Capture info and above as breadcrumbs
                event_level=logging.ERROR  # Send errors as events
            ),
        ],
        
        # Set environment
        environment="development",  # Change to "production" in prod
        
        # Enable performance monitoring of specific functions
        functions_to_trace=[
            {'qualified_name': 'gtfs_trip_etl.fetch_and_process_trip_updates'},
            {'qualified_name': 'gtfs_trip_etl.process_entity_trip_updates'},
            {'qualified_name': 'gtfs_trip_etl.get_timepoint_shift'},
            {'qualified_name': 'gtfs_trip_etl.process_trip_updates'},
            {'qualified_name': 'gtfs_trip_etl.upload_trip_updates_to_supabase'}
        ],
        
        # Configure sample rate for errors
        sample_rate=1.0,
        
        # Add release information (optional)
        release="1.0.1"  # You can use git commit hash or version number
    )

    # Wrap the main loop in a try-except with explicit Sentry error capture
    while True:
        try:
            start_time = time.time()
            logger.info("initializing GTFS trip stop updates...")
            
            # Create a transaction for the main flow
            with sentry_sdk.start_transaction(op="task", name="main_flow"):
                asyncio.run(main_flow())
            
            end_time = time.time()
            execution_time = end_time - start_time
            logger.info(f"cycle execution time: {execution_time:.2f} seconds")
            logger.info("Waiting 60 seconds before running next ETL flow")
            asyncio.run(asyncio.sleep(60))
            
        except Exception as e:
            with sentry_sdk.push_scope() as scope:
                scope.set_extra("execution_time", time.time() - start_time)
                scope.set_tag("process", "gtfs_etl")
                sentry_sdk.capture_exception(e)
            asyncio.run(asyncio.sleep(15))
