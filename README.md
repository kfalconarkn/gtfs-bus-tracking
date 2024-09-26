# gtfs-bus-tracking

### gtfs_etl.py

- This prefect flow file downloads the gtfs scheduale and uploads the files into a postgres SQL database (maindb)
- File needs to be run manually to run prefect flow (Need to deploy and trigger on a scheduale based on gtfs release schedule)

### trip_tracking.py 

- Using prefect to deploy and schedule a flow to record the status of bus stops for corresponding trip_id's
- This flow schedules to run every 5 seconds to upload to two tables, trip_updates, bus location and stop update