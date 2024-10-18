# gtfs-bus-tracking

### gtfs_etl.py

- This prefect flow file downloads the gtfs scheduale and uploads the files into a postgres SQL database (maindb)
- File needs to be run manually to run prefect flow (Need to deploy and trigger on a scheduale based on gtfs release schedule)

### gtfs_trip_etl.py 

- Using prefect to deploy and schedule a flow to record the status and data of bus stops for corresponding trip_id's
- using data from stops_update, the calculate_otr function calculates the On-Time Running (OTR) metric for each trip based on the provided trip updates.
- This flow schedules to run every 10 seconds to upload data to stop_updates table. The process updates records for each stop for each trip_id using the cobination of columns trip_id, stop_sequence and departure_time 

### to do

- check the depature time for trip_otr is actually the first stop within the trip (stop sequence 1)
