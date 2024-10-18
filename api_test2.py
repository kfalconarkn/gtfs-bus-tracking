import asyncio
import aiohttp
from google.transit import gtfs_realtime_pb2
from datetime import datetime, UTC
import pytz
import json



import requests

url = "https://gtfsrt.api.translink.com.au/api/realtime/SEQ/TripUpdates"

def fetch_trip_updates():
    response = requests.get(url)
    ##decode the response
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    return feed.entity[0].trip_update.trip.start_time



def main():
    data = fetch_trip_updates()
    print(data)

if __name__ == "__main__":
    main()



