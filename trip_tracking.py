import requests
import pandas as pd
from google.transit import gtfs_realtime_pb2
import os

url = "https://gtfsrt.api.translink.com.au/api/realtime/SEQ/VehiclePositions"

#create mapping
status_mapping = {
    0: 'Stopped at',
    1: 'Incoming at',
    2: 'In transit to'
}

response = requests.get(url)
if response.status_code == 200:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    data = []
    for entity in feed.entity:
        vehicle = entity.vehicle
        data.append({
            'trip_id': vehicle.trip.trip_id,
            'route_id': vehicle.trip.route_id.split('-',1)[0],
            'vehicle_id': vehicle.vehicle.label,
            'latitude': vehicle.position.latitude,
            'longitude': vehicle.position.longitude,
            'stop_id': vehicle.stop_id,
            'current_status': vehicle.current_status,
            'timestamp': pd.to_datetime(vehicle.timestamp, unit='s')
        })

    df_1 = pd.DataFrame(data)


    ## filter out rows that don't contain SBL in the current_status column
    realTime_df = df_1[df_1['trip_id'].str.contains('SBL')].copy()

    # Localize the naive datetime object to UTC
    realTime_df['timestamp'] = realTime_df['timestamp'].dt.tz_localize('UTC')

    # Convert the UTC datetime object to the Australian/Brisbane timezone
    realTime_df['timestamp'] = realTime_df['timestamp'].dt.tz_convert('Australia/Brisbane')

    #Remove the timezone information to make the datetime object naive again
    realTime_df['timestamp'] = realTime_df['timestamp'].dt.tz_localize(None)

    #Format the datetime object as a string if you need it in that format
    realTime_df['timestamp'] = realTime_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Before applying the map, ensure 'current_status' is of type integer if it's not already
    realTime_df['current_status'] = realTime_df['current_status'].astype(int)
    
    # During data processing
    realTime_df.loc[:, 'current_status'] = realTime_df['current_status'].map(status_mapping)

print(realTime_df)
