"""Schema generation utilities for CSV files."""

import sys
from typing import Dict, List

from nekt_singer_sdk import typing as th  # JSON schema typing helpers
from nekt_singer_sdk.custom_logger import user_logger

from tap_s3_csv.csv_processing import (
    LAST_MODIFIED_COLUMN,
    SDC_EXTRA_COLUMN,
    SDC_SOURCE_BUCKET_COLUMN,
    SDC_SOURCE_FILE_COLUMN,
    SDC_SOURCE_LINENO_COLUMN,
    sample_csv_file,
)


def generate_csv_schema(samples: List[Dict], table_config: Dict) -> Dict:
    """
    Generate JSON schema from CSV samples.

    Args:
        samples: List of sample CSV records
        table_config: Table configuration with date_overrides

    Returns:
        JSON schema dictionary
    """
    if not samples:
        user_logger.error("No samples available for schema generation")
        sys.exit(1)

    properties = {}
    date_overrides = set(table_config.get("date_overrides", []))

    # Collect all unique field names from samples
    all_fieldnames = set()
    for sample in samples:
        all_fieldnames.update(sample.keys())

    # Generate schema for each field
    properties = th.PropertiesList()
    for field_name in all_fieldnames:
        properties.append(th.Property(field_name, th.StringType))

        # Add datetime format for date overrides
        if field_name in date_overrides:
            properties.append(th.Property(field_name, th.DateTimeType))

    # Add metadata columns
    properties.append(th.Property(LAST_MODIFIED_COLUMN, th.DateTimeType))
    properties.append(th.Property(SDC_SOURCE_BUCKET_COLUMN, th.StringType))
    properties.append(th.Property(SDC_SOURCE_FILE_COLUMN, th.StringType))
    properties.append(th.Property(SDC_SOURCE_LINENO_COLUMN, th.IntegerType))

    schema = properties.to_dict()
    return schema


def get_sampled_schema_for_table(
    aws_client,
    table_config: Dict,
    modified_since=None,
    max_files: int = 5,
    sample_rate: int = 5,
    max_records_per_file: int = 200,
) -> Dict:
    """
    Generate schema by sampling CSV files.

    Args:
        aws_client: AWS client instance
        table_config: Table configuration
        modified_since: Only sample files modified after this date
        max_files: Maximum number of files to sample
        sample_rate: Sample every Nth record
        max_records_per_file: Maximum records to sample per file

    Returns:
        JSON schema dictionary
    """
    user_logger.info("Sampling CSV files to determine schema")

    # Get files for sampling
    files_generator = aws_client.get_input_files_for_table(table_config, modified_since)
    files_to_sample = list(files_generator)

    if not files_to_sample:
        user_logger.error("No files found for schema sampling")
        sys.exit(1)

    # Sample from the most recent files (up to max_files)
    files_to_sample = sorted(files_to_sample, key=lambda x: x["last_modified"], reverse=True)[:max_files]

    all_samples = []

    for file_info in files_to_sample:
        file_path = file_info["key"]
        user_logger.info(f"Sampling file: {file_path} (max records: {max_records_per_file})")

        try:
            with aws_client.get_file_handle(file_path) as file_handle:
                samples = list(
                    sample_csv_file(
                        file_handle, table_config, sample_rate=sample_rate, max_records=max_records_per_file
                    )
                )
                all_samples.extend(samples)

                user_logger.info(f"Collected {len(samples)} samples from {file_path}")

        except Exception as e:
            user_logger.warning(f"Failed to sample file {file_path}: {e}")
            continue

    if not all_samples:
        user_logger.error("No valid samples could be collected from CSV files")
        sys.exit(1)

    return generate_csv_schema(all_samples, table_config)


def merge_schemas(base_schema: Dict, additional_schema: Dict) -> Dict:
    """
    Merge two JSON schemas, combining properties.

    Args:
        base_schema: Base schema dictionary
        additional_schema: Additional schema to merge

    Returns:
        Merged schema dictionary
    """
    if not base_schema:
        return additional_schema

    if not additional_schema:
        return base_schema

    merged = base_schema.copy()

    # Merge properties
    base_properties = merged.get("properties", {})
    additional_properties = additional_schema.get("properties", {})

    for key, value in additional_properties.items():
        if key not in base_properties:
            base_properties[key] = value
        else:
            # For now, keep the base property definition
            # In the future, could implement more sophisticated merging
            pass

    merged["properties"] = base_properties
    return merged
