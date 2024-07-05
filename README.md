# gtfs-bus-tracking

### gtfs_etl.py

- This prefect flow file downloads the gtfs scheduale and uploads the files into a postgres SQL database (maindb)
- File needs to be run manually to run prefect flow (Need to deploy and trigger on a scheduale based on gtfs release schedule)

### trip_tracking.py ( in development )

- This file runs every 5 seconds, using the realtime bus location feed and the gtfs stop locations to track the bus. Only timing points are tracked and OTR is calculated
