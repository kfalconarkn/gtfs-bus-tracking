import json
import datetime
import aiohttp
import asyncio
from datetime import datetime
from google.transit import gtfs_realtime_pb2
import pytz
async def fetch_trip_updates():
    url = "https://gtfsrt.api.translink.com.au/api/realtime/SEQ/TripUpdates"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.read()

async def process_entity_trip_updates(entity):
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(entity)
    
    trip_updates = []
    for entity in feed.entity:
        if entity.HasField('trip_update'):
            trip_id = entity.trip_update.trip.trip_id
            if 'SBL' not in trip_id:
                continue
            
            for stop in entity.trip_update.stop_time_update:
                stop_info = {
                    'trip_id': trip_id,
                    'route_id': entity.trip_update.trip.route_id,
                    'stop_sequence': stop.stop_sequence,
                    'stop_id': stop.stop_id,
                    'arrival_delay': stop.arrival.delay if stop.HasField('arrival') else None,
                    'arrival_time': datetime.fromtimestamp(stop.arrival.time, tz=pytz.UTC).astimezone(pytz.timezone('Australia/Brisbane')).strftime('%Y-%m-%d %H:%M:%S') if stop.HasField('arrival') else None,
                    'departure_delay': stop.departure.delay if stop.HasField('departure') else None,
                    'departure_time': datetime.fromtimestamp(stop.departure.time, tz=pytz.UTC).astimezone(pytz.timezone('Australia/Brisbane')).strftime('%Y-%m-%d %H:%M:%S') if stop.HasField('departure') else None,
                    'date': datetime.fromtimestamp(stop.arrival.time, tz=pytz.UTC).astimezone(pytz.timezone('Australia/Brisbane')).strftime('%Y-%m-%d') if stop.HasField('arrival') else None
                }
                trip_updates.append(stop_info)

    return trip_updates 

async def calculate_otr(trip_updates):
    """
    Calculates the On-Time Running (OTR) metric for each trip based on the provided trip updates.

    This function processes a list of trip updates, calculates the OTR for each stop within a trip,
    and then aggregates the OTR for each unique trip_id. The OTR is determined based on the departure
    delay at each stop.

    Args:
        trip_updates (list): A list of dictionaries, where each dictionary contains information about
                             a stop update for a specific trip. Expected keys include 'trip_id', 
                             'route_id', 'stop_sequence', 'departure_delay', 'departure_time', and 
                             'arrival_time'.

    Returns:
        list: A list of dictionaries, where each dictionary represents the OTR information for a 
              unique trip. Each dictionary contains the following keys:
              - 'trip_id': The unique identifier for the trip.
              - 'route_id': The identifier for the route of the trip.
              - 'departure_time': The departure time of the first stop in the trip.
              - 'date': The date of the trip, taken from the arrival time of the last processed stop.
              - 'otr': The calculated OTR value for the trip, as a float between 0 and 1.

    OTR Calculation:
        For each stop in a trip:
        - If the departure delay is between -60 and 300 seconds (inclusive), it's considered on time (1).
        - Otherwise, it's considered not on time (0).
        The final OTR for a trip is the average of these values across all stops for the same trip_id.

    Note:
        - The function assumes that the input 'trip_updates' is sorted by 'stop_sequence' for each trip.
        - If a trip has no valid OTR values, its OTR will be None.
        - The 'departure_time' is taken from the first stop (stop_sequence = 1) of each trip.
        - The 'date' is taken from the 'arrival_time' of the last processed stop for each trip.
    """
    #empty object to append to
    otr = {
        'trip_id': None,
        'route_id': None,
        'departure_time': None, ## take from depature_time key and if associated stop_sequence = 1 then take that as the departure time for that associated trip_id
        'date': None, ## taken from arrival_time key
        'otr': None ## calculated as average of 1's and 0's for each trip_id
    }
    
    trip_otr = {}
    
    for update in trip_updates:
        trip_id = update['trip_id']
        if trip_id not in trip_otr:
            trip_otr[trip_id] = {
                'otr_values': [],
                'departure_time': None,
                'date': None,
                'route_id': None
            }
        
        # Calculate OTR for this stop
        departure_delay = update['departure_delay']
        otr_value = 1 if departure_delay is not None and -60 <= departure_delay <= 300 else 0
        trip_otr[trip_id]['otr_values'].append(otr_value)
        
        # Set departure time if it's the first stop
        if update['stop_sequence'] == 1:
            trip_otr[trip_id]['departure_time'] = update['departure_time']
        
        # Set date (we'll use the last available date)
        if update['arrival_time']:
            trip_otr[trip_id]['date'] = update['arrival_time'][:10] if update['arrival_time'] else None
            trip_otr[trip_id]['route_id'] = update['route_id']

    
    results = []
    for trip_id, data in trip_otr.items():
        result = otr.copy()
        result['trip_id'] = trip_id
        result['departure_time'] = data['departure_time']
        result['date'] = data['date']
        result['route_id'] = data['route_id']
        result['otr'] = round(sum(data['otr_values']) / len(data['otr_values']), 4) if data['otr_values'] else None
        results.append(result)
    
    return results
    

            



async def main():
    entity_data = await fetch_trip_updates()
    trip_updates = await process_entity_trip_updates(entity_data)
    otr_results = await calculate_otr(trip_updates)
    print(json.dumps(otr_results, indent=4))


if __name__ == "__main__":
   asyncio.run(main())

