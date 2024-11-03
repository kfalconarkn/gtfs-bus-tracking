# GTFS Trip ETL Service

This service processes GTFS (General Transit Feed Specification) real-time data from Translink's API for South East Queensland (SEQ) transit services, specifically focusing on Surfside and Sunbus Sunshine Coast operations.

## Overview

The script (`gtfs_trip_etl.py`) performs the following operations:
- Fetches real-time GTFS trip updates from Translink's API
- Processes and filters the data for specific bus services (SBL)
- Enriches the data with timing point and shift information
- Uploads the processed data to a Supabase database

## Features

- **Real-time Processing**: Runs every 15 seconds to maintain current transit data
- **Data Filtering**: Focuses on SBL (specific bus line) trip IDs
- **Data Enrichment**: Adds timing point and duty shift information
- **Error Handling**: Includes comprehensive logging and Sentry integration for error tracking
- **Data Validation**: Uses Pydantic models to ensure data integrity

## Requirements

- Python 3.x
- Required Python packages:
  - asyncio
  - aiohttp
  - google.transit
  - pytz
  - supabase
  - pydantic
  - sentry_sdk

## Environment Variables

The following environment variables need to be set:
