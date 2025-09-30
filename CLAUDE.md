# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a GTFS (General Transit Feed Specification) ETL pipeline for downloading, processing, and uploading transit data from TransLink's Southeast Queensland (SEQ) API to a Supabase database. The pipeline handles both static GTFS data and real-time trip updates.

## Commands

### Environment Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables (required)
export SUPABASE_URL=your_supabase_url
export SUPABASE_KEY=your_supabase_key
```

### Running the Pipeline
```bash
# Run the full GTFS static data ETL (with scheduling)
python gtfs_etl_sup.py

# Run GTFS static data ETL once (GitHub Actions version)
python gtfs_etl_github.py

# Run real-time trip updates ETL (continuous processing)
python gtfs_trip_etl.py
```

### Testing API endpoints
```bash
# Test API connectivity
python api_test.py
python api_test2.py
```

### Manual GitHub Actions Trigger
The pipeline can be triggered manually via GitHub Actions in the "Actions" tab of the repository.

## Architecture

### Core Components

1. **Static GTFS Data Pipeline** (`gtfs_etl_sup.py` / `gtfs_etl_github.py`):
   - Downloads ZIP file from TransLink API
   - Extracts and processes CSV files (calendar, trips, stops, stop_times, calendar_dates)
   - Filters data for Surfside (SBL) and Sunbus (SUN) services only
   - Uploads to Supabase using batch upsert operations
   - Handles data cleanup and memory management

2. **Real-time Trip Updates Pipeline** (`gtfs_trip_etl.py`):
   - Continuously fetches GTFS-realtime protobuf data
   - Processes trip updates and stop time predictions
   - Enriches data with timepoint and shift information
   - Calculates On-Time Running (OTR) metrics via database functions
   - Runs every 60 seconds

3. **Database Schema**:
   - `calendar` (service_id as PK)
   - `calendar_dates` (id as PK)
   - `trips` (trip_id as PK)
   - `stops` (stop_id as PK)
   - `stop_times` (id as PK, trip_id as FK)
   - `stops_update` (real-time data with composite PK)
   - `shifts` (duty/shift information)

### Data Flow

1. **Static Data**: TransLink API → ZIP download → CSV extraction → DataFrame processing → Supabase batch upload
2. **Real-time Data**: TransLink GTFS-RT API → Protobuf parsing → Data enrichment → Supabase upsert → OTR calculation

### Key Design Patterns

- **Async/await**: All major operations use asyncio for concurrent processing
- **Batch Processing**: Large datasets are processed in configurable chunks (1000-25000 records)
- **Error Resilience**: Comprehensive error handling with continued processing on failures
- **Memory Management**: Explicit garbage collection and dataframe cleanup
- **Monitoring**: Sentry integration for error tracking and performance monitoring

### Environment Differences

- `gtfs_etl_sup.py`: Local development version with scheduling and Sentry integration
- `gtfs_etl_github.py`: GitHub Actions version (single run, no scheduling, simpler error handling)
- Storage directories: `/tmp/gtfs_downloads` (GitHub) vs `gtfs_downloads` (local)

### Critical Configuration

- **Chunk Sizes**: Carefully tuned for different tables to avoid timeouts
  - `stop_times`: 1000-5000 records (most sensitive)
  - Other tables: 10000-25000 records
- **Service Filtering**: Only processes trips containing 'SBL' (Surfside) or 'SUN' (Sunbus)
- **Timezone**: All times converted to Australia/Brisbane timezone
- **Database Functions**: `calculate_trip_otr()` and `calculate_shift_otr()` for metrics calculation

### Dependencies

Key packages: pandas, supabase, asyncio, requests, tqdm, sentry-sdk, gtfs-realtime-bindings, pydantic