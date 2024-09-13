import requests
import pandas as pd
from google.transit import gtfs_realtime_pb2
import os
from sqlalchemy import create_engine, text
import pytz
import json
import aiohttp
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.future import select
import math
import time


# Define constants
DB_NAME = "maindb"
DB_USER = "kfalconar"
DB_PASSWORD = "Teckdeck"
DB_HOST = "localhost"
DB_PORT = "5432"
SCHEMA = "gtfs"

# Create an async engine
#DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

DATABASE_URL = "sqlite+aiosqlite:///mydatabase.db"
engine = create_async_engine(DATABASE_URL, echo=False)

# Create a sessionmaker for async operations
async_session = sessionmaker(
    engine, expire_on_commit=False, class_=AsyncSession
)

# Define Brisbane timezone
brisbane_tz = pytz.timezone('Australia/Brisbane')


previous_positions = {}

async def fetch_bus_data():
    async with aiohttp.ClientSession() as session:
        async with session.get("https://gtfsrt.api.translink.com.au/api/realtime/SEQ/VehiclePositions") as response:
            if response.status == 200:
                return await response.read()

def process_entity(entity, brisbane_tz):
    vehicle = entity.vehicle
    if 'SBL' in vehicle.trip.trip_id:
        # Convert the timestamp to datetime and then to Brisbane timezone
        timestamp_utc = pd.to_datetime(vehicle.timestamp, unit='s', utc=True)
        timestamp_brisbane = timestamp_utc.astimezone(brisbane_tz)
        # Format the datetime to exclude "Timestamp()"
        formatted_timestamp = timestamp_brisbane.strftime('%Y-%m-%d %H:%M:%S')
        return {
            'trip_id': vehicle.trip.trip_id,
            'route_id': vehicle.trip.route_id,
            'vehicle_id': vehicle.vehicle.label,
            'latitude': vehicle.position.latitude,
            'longitude': vehicle.position.longitude,
            'timestamp': formatted_timestamp
        }

async def get_bus_location():
    raw_data = await fetch_bus_data()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(raw_data)

    tasks = []
    for entity in feed.entity:
        task = asyncio.ensure_future(asyncio.to_thread(process_entity, entity, brisbane_tz))
        tasks.append(task)

    data = await asyncio.gather(*tasks)
    data = [item for item in data if item]  # filter out None values
    return data


## upload bus locations to database
async def upload_bus_location():
    ##get bus location 
    bus_location = await get_bus_location()
    print("Uploading inital location of buses to database")
    async with async_session() as session:
        try:
            query = text("""
                INSERT INTO vehicles (trip_id, route_id, vehicle_id, latitude, longitude, timestamp)
                VALUES (:trip_id, :route_id, :vehicle_id, :latitude, :longitude, :timestamp)
            """)

            # Prepare the data for insertion
            insert_data = [
                {
                    'trip_id': bus['trip_id'],
                    'route_id': bus['route_id'],
                    'vehicle_id': bus['vehicle_id'],
                    'latitude': bus['latitude'],
                    'longitude': bus['longitude'],
                    'timestamp': bus['timestamp']
                }
                for bus in bus_location
            ]

            # Execute the insert
            await session.execute(query, insert_data)
            await session.commit()

            print("Location of buses uploaded to database successfully.")

        except Exception as e:
            print(f"An error occurred while inserting data into the database: {e}")
            await session.rollback()

        finally:
            await session.close()

async def get_db_data(live_bus):
    # Extract all trip_ids from the live_bus JSON object
    print("Getting all live trip_ids on the network")
    trip_ids = [bus['trip_id'] for bus in live_bus]

    async with async_session() as session:
        try:
            # Create named parameters for each trip_id
            placeholders = ', '.join(f':trip_id_{i}' for i in range(len(trip_ids)))
            
            query = text(f"""
                SELECT trip_id, route_id, vehicle_id, latitude, longitude, timestamp
                FROM vehicles
                WHERE trip_id IN ({placeholders})
                AND datetime(timestamp) = (
                    SELECT MAX(datetime(timestamp))
                    FROM vehicles v2
                    WHERE v2.trip_id = vehicles.trip_id
                )
            """)

            # Create a dictionary of parameters
            params = {f'trip_id_{i}': trip_id for i, trip_id in enumerate(trip_ids)}

            print(f"Getting recent positions for {len(trip_ids)} trip_ids")
            result = await session.execute(query, params)
            rows = result.fetchall()

            # Convert the result to a pandas DataFrame
            df = pd.DataFrame(rows, columns=['trip_id', 'route_id', 'vehicle_id', 'latitude', 'longitude', 'timestamp'])
            return df

        except Exception as e:
            print(f"An error occurred while fetching data from the database: {e}")
            print(f"Query: {query}")
            print(f"Parameters: {params}")
            return pd.DataFrame()  # Return an empty DataFrame instead of None

        finally:
            await session.close()
                
        


def cal_geofence(data):
    """Takes bus location json data object, uses the latitude and longitude to calculate if a bus has passed through a geofence.
       Each bus has a trip_id which will be used to filter only matching stop_id's to record the timestamp of the bus leaving the geofence """
    
def calculate_bearing(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    
    dlon = lon2 - lon1
    y = math.sin(dlon) * math.cos(lat2)
    x = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dlon)
    
    initial_bearing = math.atan2(y, x)
    initial_bearing = math.degrees(initial_bearing)
    bearing = (initial_bearing + 360) % 360
    
    return bearing


async def update_bearings_in_db(bearings_data):
    async with async_session() as session:
        try:
            query = text("""
                UPDATE vehicles
                SET bearings = :bearings
                WHERE trip_id = :trip_id
                AND timestamp = (
                    SELECT MAX(timestamp)
                    FROM vehicles v2
                    WHERE v2.trip_id = vehicles.trip_id
                )
            """)

            for _, row in bearings_data.iterrows():
                await session.execute(query, {
                    'bearings': row['bearings'],
                    'trip_id': row['trip_id']
                })

            await session.commit()
            print(f"Bearings updated in the database for {len(bearings_data)} vehicles.")

        except Exception as e:
            print(f"An error occurred while updating bearings in the database: {e}")
            await session.rollback()

        finally:
            await session.close()



async def calculate_bearing_for_row(row):
    return calculate_bearing(
        row['latitude_last'], row['longitude_last'],
        row['latitude_real_time'], row['longitude_real_time']
    )

async def calculate_bearing_for_buses(df, real_time_json):
    global previous_positions
    
    # Convert JSON to DataFrame
    real_time_df = pd.json_normalize(real_time_json)
    
    # Merge DataFrames on 'trip_id'
    merged_df = pd.merge(df, real_time_df, on='trip_id', suffixes=('_db', '_real_time'))

    # Calculate bearings
    print("Calculating bearings for buses")
    
    def calculate_and_debug_bearing(row):
        trip_id = row['trip_id']
        lat2, lon2 = row['latitude_real_time'], row['longitude_real_time']
        
        if trip_id in previous_positions:
            lat1, lon1 = previous_positions[trip_id]
            if (lat1, lon1) != (lat2, lon2):
                bearing = calculate_bearing(lat1, lon1, lat2, lon2)
            else:
                bearing = None  # Bus hasn't moved
        else:
            bearing = None  # No previous position available
        
        # Update previous position
        previous_positions[trip_id] = (lat2, lon2)
        
        # Debug output for the first 5 buses
        if row.name < 5:
            print(f"Debug for bus {trip_id}:")
            print(f"  Previous position: {previous_positions.get(trip_id, 'N/A')}")
            print(f"  Current position: ({lat2}, {lon2})")
            print(f"  Calculated bearing: {bearing}")
        
        return bearing
    
    merged_df['bearings'] = merged_df.apply(calculate_and_debug_bearing, axis=1)
    
    # Remove None values for statistics calculation
    bearings_not_none = merged_df['bearings'].dropna()
    
    # Print summary statistics
    print("\nBearing calculation summary:")
    print(f"Number of buses: {len(merged_df)}")
    print(f"Number of buses with calculated bearings: {len(bearings_not_none)}")
    if not bearings_not_none.empty:
        print(f"Mean bearing: {bearings_not_none.mean():.2f}")
        print(f"Min bearing: {bearings_not_none.min():.2f}")
        print(f"Max bearing: {bearings_not_none.max():.2f}")
    
    # Check for zero bearings
    zero_bearings = merged_df[merged_df['bearings'] == 0]
    print(f"\nNumber of buses with zero bearing: {len(zero_bearings)}")
    
    if not zero_bearings.empty:
        print("\nExample of a bus with zero bearing:")
        example = zero_bearings.iloc[0]
        print(f"Trip ID: {example['trip_id']}")
        print(f"Previous position: {previous_positions.get(example['trip_id'], 'N/A')}")
        print(f"Current position: ({example['latitude_real_time']}, {example['longitude_real_time']})")
    
    # Update bearings in the database (only for non-None bearings)
    bearings_to_update = merged_df[merged_df['bearings'].notnull()][['trip_id', 'bearings']]
    if not bearings_to_update.empty:
        await update_bearings_in_db(bearings_to_update)
    
    return merged_df
   

async def main():
    await upload_bus_location()

    previous_bus_count = 0
    while True:
        print("Fetching new bus locations...")
        location = await get_bus_location()
        
        print("Fetching recent database entries...")
        db_data = await get_db_data(location)

        # Convert location (JSON) to DataFrame for easy comparison
        location_df = pd.DataFrame(location)
        
        # Merge the DataFrames on 'trip_id' to compare lat and long
        merged_df = pd.merge(db_data, location_df, on='trip_id', suffixes=('_db', ''))

        # Convert timestamps to datetime objects
        merged_df['timestamp_db'] = pd.to_datetime(merged_df['timestamp_db'])
        merged_df['timestamp'] = pd.to_datetime(merged_df['timestamp'])
        
        # Check for changes in latitude, longitude, or timestamp
        changed_trips = merged_df[
            (merged_df['latitude_db'] != merged_df['latitude']) |
            (merged_df['longitude_db'] != merged_df['longitude']) |
            (merged_df['timestamp_db'] != merged_df['timestamp'])
        ]
        
        if not changed_trips.empty:
            print(f"Found {len(changed_trips)} buses with updated positions or timestamps.")
            # Calculate bearings for all buses, not just changed ones
            await calculate_bearing_for_buses(merged_df[db_data.columns], location)
            
            # Update database with new positions
            await upload_bus_location()
        else:
            print("No changes in bus positions or timestamps detected.")
        
        # Wait for a short interval before the next check
        print("Waiting 10 seconds before the next check...")
        await asyncio.sleep(10)  

    
    

# Run the async main function in the event loop
asyncio.run(main())