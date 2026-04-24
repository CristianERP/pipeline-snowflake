import os
from dataclasses import dataclass


@dataclass(frozen=True)
class S3Settings:
    bucket: str = os.getenv("MEETUP_S3_BUCKET", "meetup-pipeline-2026")
    source_prefix: str = os.getenv("MEETUP_SOURCE_PREFIX", "landing/")
    normalized_prefix: str = os.getenv("MEETUP_NORMALIZED_PREFIX", "landing_normalized/")
    incremental_prefix: str = os.getenv("MEETUP_INCREMENTAL_PREFIX", "incremental/events/")


@dataclass(frozen=True)
class SnowflakeSettings:
    conn_id: str = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_default")
    database: str = os.getenv("SNOWFLAKE_DATABASE", "MEETUP_DE")
    raw_schema: str = os.getenv("SNOWFLAKE_RAW_SCHEMA", "RAW")


@dataclass(frozen=True)
class PipelineSettings:
    s3: S3Settings = S3Settings()
    snowflake: SnowflakeSettings = SnowflakeSettings()


settings = PipelineSettings()