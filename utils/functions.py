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


async def fetch_bus_data():
    async with aiohttp.ClientSession() as session:
        async with session.get("https://gtfsrt.api.translink.com.au/api/realtime/SEQ/VehiclePositions") as response:
            if response.status == 200:
                return await response.read()

def process_entity_bus_location(entity, brisbane_tz):
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
        task = asyncio.ensure_future(asyncio.to_thread(process_entity_bus_location, entity, brisbane_tz))
        tasks.append(task)

    data = await asyncio.gather(*tasks)
    data = [item for item in data if item]  # filter out None values
    return data

if __name__ == "__main__":
    result = asyncio.run(get_bus_location())
    print(json.dumps(result, indent=2))
