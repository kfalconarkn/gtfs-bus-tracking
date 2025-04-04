# GTFS ETL Pipeline

This repository contains a pipeline for downloading, processing, and uploading GTFS (General Transit Feed Specification) data to a Supabase database.

## Overview

The pipeline performs the following steps:
1. Downloads the latest GTFS data from the TransLink API
2. Extracts the zip file
3. Processes and cleans the data
4. Uploads the data to Supabase

## GitHub Actions

The pipeline is automated using GitHub Actions, which runs on a schedule (Tuesday, Thursday, and Saturday at 2 AM UTC) or can be triggered manually.

### Environment Setup

To use this pipeline, you need to configure the following secrets in your GitHub repository:

1. `SUPABASE_URL`: Your Supabase project URL
2. `SUPABASE_KEY`: Your Supabase API key

### Manual Trigger

You can manually trigger the workflow by:
1. Going to the "Actions" tab in your GitHub repository
2. Selecting the "GTFS ETL Process" workflow
3. Clicking "Run workflow"

## Local Development

To run the pipeline locally:

1. Clone the repository
2. Install the dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Set up environment variables:
   ```
   export SUPABASE_URL=your_supabase_url
   export SUPABASE_KEY=your_supabase_key
   ```
4. Run the script:
   ```
   python gtfs_etl_github.py
   ```

## Database Schema

The pipeline works with the following tables in Supabase:
- `calendar`
- `calendar_dates`
- `trips`
- `stops`
- `stop_times`

The primary key relationships are:
- `calendar`: `service_id` (primary key)
- `trips`: `trip_id` (primary key) 
- `stops`: `stop_id` (primary key)
- `stop_times`: `id` (primary key) with `trip_id` as a foreign key to `trips.trip_id`
- `calendar_dates`: `id` (primary key)

## Troubleshooting

Check the GitHub Actions logs for detailed information if the pipeline fails. The script includes extensive logging with Sentry integration for error tracking.
