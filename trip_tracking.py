import asyncio
import aiohttp
from google.transit import gtfs_realtime_pb2
from datetime import datetime, UTC
import pytz
import json
import supabase
import pandas as pd
from prefect import flow, task
from pydantic import BaseModel, ValidationError
from typing import List
import postgrest
from itertools import islice


#### --------------------------------------------------------------------------------------------- ####
####    This Prefect flow downloads GTFS data and process the trip, stop and bus locations.
####    Data is uploaded to three tables in supabase, trip_updates, bus_locations and stop_updates
#### --------------------------------------------------------------------------------------------- ####

## define timezone
brisbane_tz = pytz.timezone('Australia/Brisbane')

## Define supabase keys, url and client
supabase_url = "https://zfzjlwllauahcldszhhz.supabase.co"
supabase_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inpmempsd2xsYXVhaGNsZHN6aGh6Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3MjcyNTAyODAsImV4cCI6MjA0MjgyNjI4MH0.H5oAX0_YbYVQWV0w3iLaroNXQVqpUmVVI9fvKl-Quyo"
Client = supabase.create_client(supabase_url, supabase_key)

## define data models for supabase upload
class StopUpdate(BaseModel):
    trip_id: str
    stop_sequence: int
    stop_id: str
    arrival_delay: int | None
    arrival_time: str | None
    departure_delay: int | None
    departure_time: str | None
    uncertainty: int | None

class BusLocation(BaseModel):
    trip_id: str 
    route_id: str
    vehicle_id: str | None
    latitude: float
    longitude: float
    timestamp: str

class Trips(BaseModel):
    trip_id: str  ##varchar
    start_time: str  # time (HH:MM:SS format)
    start_date: str 
    route_id: str #varchar
    vehicle_id: str | None
    timestamp: str
    
    # Helper function (place this outside the upload_to_supabase function)
def compute_distance(lat1, lon1, lat2, lon2):
    import math
    R = 6371000  # Radius of the Earth in meters
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    
    a = math.sin(delta_phi / 2) ** 2 + \
        math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c
    

###### Trip Updates API #####

async def fetch_trip_updates():
    url = "https://gtfsrt.api.translink.com.au/api/realtime/SEQ/TripUpdates"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.read()

async def process_entity_trip_updates(entity, brisbane_tz):
    trip_id = entity.trip_update.trip.trip_id
    if 'SBL' not in trip_id:
        return None, None
    
    trip_info = {
        'trip_id': str(trip_id),
        'start_time': str(entity.trip_update.trip.start_time),
        'start_date': str(entity.trip_update.trip.start_date),
        'route_id': str(entity.trip_update.trip.route_id),
        'vehicle_id': str(entity.trip_update.vehicle.id) if entity.trip_update.HasField('vehicle') else None,
        'timestamp': datetime.fromtimestamp(entity.trip_update.timestamp, tz=UTC).astimezone(brisbane_tz).strftime('%Y-%m-%d %H:%M:%S')
    }

    stop_updates = []
    for stop in entity.trip_update.stop_time_update:
        stop_info = {
            'trip_id': trip_id,
            'stop_sequence': stop.stop_sequence,
            'stop_id': stop.stop_id,
            'arrival_delay': stop.arrival.delay if stop.HasField('arrival') else None,
            'arrival_time': datetime.fromtimestamp(stop.arrival.time, tz=UTC).astimezone(brisbane_tz).strftime('%Y-%m-%d %H:%M:%S') if stop.HasField('arrival') else None,
            'departure_delay': stop.departure.delay if stop.HasField('departure') else None,
            'departure_time': datetime.fromtimestamp(stop.departure.time, tz=UTC).astimezone(brisbane_tz).strftime('%Y-%m-%d %H:%M:%S') if stop.HasField('departure') else None,
            'uncertainty': stop.arrival.uncertainty if stop.HasField('arrival') else None
        }
        stop_updates.append(stop_info)

    return trip_info, stop_updates

async def fetch_and_process_trip_updates():
    content = await fetch_trip_updates()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(content)

    brisbane_tz = pytz.timezone('Australia/Brisbane')
    
    tasks = [process_entity_trip_updates(entity, brisbane_tz) for entity in feed.entity]
    results = await asyncio.gather(*tasks)

    trip_updates = []
    stop_updates = []

    for trip_info, entity_stop_updates in results:
        if trip_info:
            trip_updates.append(trip_info)
            stop_updates.extend(entity_stop_updates)

    print(f"Processed {len(trip_updates)} trip updates and {len(stop_updates)} stop updates")
    return trip_updates, stop_updates

async def fetch_bus_data():
    url = "https://gtfsrt.api.translink.com.au/api/realtime/SEQ/VehiclePositions"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                return await response.read()
            else:
                print(f"Error fetching bus data: {response.status}")
                return None

def process_entity_bus_location(entity, brisbane_tz):
    vehicle = entity.vehicle
    if 'SBL' in vehicle.trip.trip_id:
        # Convert the timestamp to datetime and then to Brisbane timezone
        timestamp_utc = pd.to_datetime(vehicle.timestamp, unit='s', utc=True)
        timestamp_brisbane = timestamp_utc.astimezone(brisbane_tz)
        # Format the datetime to include timezone information for timestamptz
        formatted_timestamp = timestamp_brisbane.strftime('%Y-%m-%d %H:%M:%S%z')
        return {
            'trip_id': str(vehicle.trip.trip_id),
            'route_id': str(vehicle.trip.route_id),
            'vehicle_id': str(vehicle.vehicle.label),
            'latitude': float(vehicle.position.latitude),
            'longitude': float(vehicle.position.longitude),
            'timestamp': formatted_timestamp
        }

async def get_bus_location():
    raw_data = await fetch_bus_data()
    if raw_data is None:
        print("Failed to fetch bus data")
        return []
    
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(raw_data)
    tasks = [asyncio.ensure_future(asyncio.to_thread(process_entity_bus_location, entity, brisbane_tz)) for entity in feed.entity]
    data = await asyncio.gather(*tasks)
    data = [item for item in data if item]  # filter out None values
    print(f"Processed {len(data)} bus locations")
    return data




@task(name="Process Trip and stop Updates", log_prints=True)
async def download_trip_updates():
    return await fetch_and_process_trip_updates()
    


@task(name="Process Bus Location", log_prints=True)
async def update_bus_location():
    return await get_bus_location()


def serialize_datetime(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

@task(name="Upload to Supabase", log_prints=True)
async def upload_to_supabase(trip_updates, stop_updates, bus_locations):
    trip_update_table = "trips"
    stop_update_table = "stops_update"
    bus_location_table = "bus_location"
    
    print(f"Starting upload to Supabase: {len(trip_updates)} trips, {len(stop_updates)} stop updates, {len(bus_locations)} bus locations")
    
    async def fetch_supabase_data(table, column, values):
        response = Client.table(table).select("*").in_(column, values).execute()
        return response.data

    async def batch_upsert(table, data, primary_keys, chunk_size=5000):
        total_chunks = len(data) // chunk_size + (1 if len(data) % chunk_size else 0)
        print(f"Upserting {len(data)} records into {table} in {total_chunks} chunks")
        serialized_data = json.loads(json.dumps(data, default=serialize_datetime))
        
        for i in range(0, len(serialized_data), chunk_size):
            chunk = serialized_data[i:i + chunk_size]
            try:
                # Ensure the on_conflict parameter matches the exact column names in your table
                Client.table(table).upsert(chunk, on_conflict=','.join(primary_keys)).execute()
            except postgrest.exceptions.APIError as e:
                print(f"Error upserting chunk into {table}: {e}")
        print(f"Completed upserting {len(data)} records into {table}")

    async def batch_insert(table, data, chunk_size=200):
        total_chunks = len(data) // chunk_size + (1 if len(data) % chunk_size else 0)
        print(f"Inserting {len(data)} records into {table} in {total_chunks} chunks")
        serialized_data = json.loads(json.dumps(data, default=serialize_datetime))
        
        for i in range(0, len(serialized_data), chunk_size):
            chunk = serialized_data[i:i + chunk_size]
            try:
                Client.table(table).insert(chunk).execute()
                print(f"Completed inserting {len(chunk)} records into {table}")
            except postgrest.exceptions.APIError as e:
                print(f"Error inserting chunk into {table}: {e}")
        

    print("Fetching existing data from Supabase")
    tasks = [
        fetch_supabase_data(bus_location_table, "trip_id", [loc['trip_id'] for loc in bus_locations]),
        fetch_supabase_data(trip_update_table, "trip_id", [trip['trip_id'] for trip in trip_updates]),
        fetch_supabase_data(stop_update_table, "trip_id", list(set(stop['trip_id'] for stop in stop_updates)))
    ]

    supabase_bus_locations, supabase_trips, supabase_stops = await asyncio.gather(*tasks)

    print("Processing data for upload")
    latest_supabase = {loc['trip_id']: loc for loc in supabase_bus_locations}
    bus_inserts = []
    for loc in bus_locations:
        try:
            validated_loc = BusLocation(**loc).model_dump()
            if loc['trip_id'] not in latest_supabase:
                # New trip_id, always insert
                bus_inserts.append(validated_loc)
                print(f"New bus location for trip_id: {loc['trip_id']}")
            else:
                last_known = latest_supabase[loc['trip_id']]
                distance = compute_distance(
                    loc['latitude'], loc['longitude'],
                    last_known['latitude'], last_known['longitude']
                )
                
                if distance > 5:  # 5 meters threshold
                    bus_inserts.append(validated_loc)
                    print(f"Updated bus location for trip_id: {loc['trip_id']} (Distance: {distance:.2f}m)")
                else:
                    print(f"Skipping unchanged bus location for trip_id: {loc['trip_id']}")
        except ValidationError as e:
            print(f"Validation error for bus location: {e}")

    existing_trips = {(trip['trip_id'], trip['start_date']): trip for trip in supabase_trips}
    trip_upserts = [Trips(**trip).model_dump() for trip in trip_updates if (trip['trip_id'], trip['start_date']) not in existing_trips or trip != existing_trips[(trip['trip_id'], trip['start_date'])]]

    existing_stops = {(stop['trip_id'], stop['stop_sequence']): stop for stop in supabase_stops}
    stop_upserts = [StopUpdate(**stop).model_dump() for stop in stop_updates if (stop['trip_id'], stop['stop_sequence']) not in existing_stops or stop != existing_stops[(stop['trip_id'], stop['stop_sequence'])]]

    print("Performing batch operations")
    upsert_tasks = [
        batch_upsert(trip_update_table, trip_upserts, ['trip_id', 'start_date','vehicle_id']),
        batch_upsert(stop_update_table, stop_upserts, ['trip_id', 'stop_sequence'])
    ]
    
    insert_task = batch_insert(bus_location_table, bus_inserts)

    await asyncio.gather(*upsert_tasks, insert_task)
    print(f"Upload complete: {len(bus_inserts)} bus positions, {len(trip_upserts)} trip updates, {len(stop_upserts)} stop updates")



@flow(name="GTFS-Bus-Tracking, Trip update process", log_prints=True)
async def main_flow():
    print("Starting GTFS data processing and upload")
    trip_updates_task = asyncio.create_task(download_trip_updates())
    bus_locations_task = asyncio.create_task(update_bus_location())
    
    trip_updates, stop_updates = await trip_updates_task
    bus_locations = await bus_locations_task
    
    await upload_to_supabase(trip_updates, stop_updates, bus_locations)
    print("GTFS data processing and upload completed")

if __name__ == "__main__":
    asyncio.run(main_flow())
