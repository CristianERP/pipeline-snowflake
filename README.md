# Meetup Data Pipeline

A data pipeline project built with **Airflow, Snowflake, AWS S3, and Slack**.

## Objective

Load a Meetup dataset into Snowflake, automate its processing with Airflow, build analytical tables, and export results to S3.

## Architecture Diagram

This project orchestrates a Meetup data pipeline with Airflow, Snowflake, AWS S3, and Slack.

```mermaid
flowchart LR
    A[Meetup CSV dataset] --> B[S3 raw files]

    subgraph C[Airflow with Docker Compose]
        C1[meetup_structure_snowflake]
        C2[meetup_raw_load_from_s3]
        C3[meetup_incremental_15m]
    end

    B --> C2
    C1 --> D

    subgraph D[Snowflake]
        D1[RAW]
        D2[MONITORING]
        D3[ANALYTICS]
    end

    C2 --> D1
    C3 --> E[S3 incremental delta]
    E --> F[Snowflake stage]
    F --> D1
    C3 --> D2
    D1 --> D3
    D3 --> G[S3 processed exports]

    C3 --> H[Slack notifications]
```

## Technologies Used

- Apache Airflow
- Snowflake
- AWS S3
- Slack
- Docker Compose
- Python

## High-Level Structure

- **DAG 1:** creates the base structure in Snowflake
- **DAG 2:** performs the initial load of CSV files into RAW tables
- **DAG 3:** runs an incremental process every 15 minutes for events, updates data with `MERGE`, rebuilds analytical tables, and exports results to S3

## Project Flow

1. Source files are stored in S3.
2. Airflow runs the initial load into Snowflake.
3. RAW tables are created with the base dataset.
4. Analytical tables are built for downstream processing and analysis.
5. Every 15 minutes, an incremental DAG runs and:
   - generates new event data
   - loads a delta into Snowflake
   - updates the main table with `MERGE`
   - rebuilds analytical tables
   - exports results to S3
   - sends a Slack notification

## Snowflake Schemas

### RAW
Base tables loaded from the source CSV files.

Source dataset:  
https://www.kaggle.com/megelon/meetup

### MONITORING

Stores data quality check results generated during pipeline execution.

### ANALYTICS
Processed tables used for analysis, for example:
- `EVENTS_ENRICHED`
- `AGG_EVENTS_BY_CITY`
- `AGG_GROUPS_BY_CATEGORY`
- `AGG_GROUPS_BY_TOPIC`
- `AGG_EVENTS_BY_GROUP`

## Execution

The project runs with Airflow using Docker Compose.

After starting the Airflow environment, the DAGs should be executed in the following order:

1. `meetup_structure`
2. `meetup_load_raw`
3. `meetup_incremental_15m`

## Notes

- Slack integration was implemented for success and failure notifications.
- Processed tables are exported from Snowflake to S3.
- The `members.csv` file was identified as a special case due to its size and may require an additional strategy for production-scale handling.

## Author

Cristian Rojas