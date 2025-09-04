"""Stream classes for S3 CSV extraction."""

import sys
from functools import cached_property
from typing import Any, Dict, Iterable, Optional

from nekt_singer_sdk.custom_logger import user_logger
from nekt_singer_sdk.streams import Stream

from tap_s3_csv.aws_client import AwsClient
from tap_s3_csv.csv_processing import (
    LAST_MODIFIED_COLUMN,
    generate_csv_metadata_columns,
    get_csv_row_iterator,
)
from tap_s3_csv.schema_helper import get_sampled_schema_for_table


class S3CSVStream(Stream):
    """Base stream class for S3 CSV processing."""

    replication_key = LAST_MODIFIED_COLUMN
    is_timestamp_replication_key = True

    def __init__(self, tap, name: str, table_config: Dict, **kwargs):
        """Initialize the CSV stream."""
        self.table_config = table_config
        super().__init__(tap=tap, name=name, **kwargs)

    @cached_property
    def aws_client(self) -> AwsClient:
        """Get AWS client instance."""
        return AwsClient(self.config)

    @property
    def primary_keys(self) -> list[str]:
        """Return primary key properties."""
        return self.table_config.get("key_properties", [])


class S3MultipleCSVFilesStream(S3CSVStream):
    """Stream for processing multiple CSV files from S3."""

    @property
    def schema(self) -> Dict:
        """Dynamically generate schema from CSV files."""
        if not hasattr(self, "_schema") or self._schema is None:
            self._schema = self._generate_schema()
        return self._schema

    def _generate_schema(self) -> Dict:
        """Generate schema by sampling CSV files."""

        start_date = self.get_starting_timestamp(self.context)

        schema = get_sampled_schema_for_table(
            aws_client=self.aws_client, table_config=self.table_config, modified_since=start_date
        )

        if not schema:
            user_logger.error(f"Failed to generate schema for stream: {self.name}")
            sys.exit(1)

        user_logger.info(f"Schema generated successfully for stream: {self.name}")
        user_logger.info(f"Schema: {schema}")
        return schema

    def get_records(self, context: Optional[Dict] = None) -> Iterable[Dict[str, Any]]:
        """Extract records from S3 CSV files."""
        start_date = self.get_starting_timestamp(context)

        if start_date:
            user_logger.info(f"Extracting files modified since: {start_date}")

        # Get files to process
        files_generator = self.aws_client.get_input_files_for_table(
            table_config=self.table_config, modified_since=start_date
        )

        # Sort files by modification date for consistent processing
        files_to_process = sorted(list(files_generator), key=lambda x: x["last_modified"])

        total_files = len(files_to_process)
        user_logger.info(f"Found {total_files} files to process")

        if total_files == 0:
            user_logger.info("No new files to process")
            return

        # Process each file
        for file_index, file_info in enumerate(files_to_process, 1):
            file_path = file_info["key"]
            file_modified = file_info["last_modified"]

            user_logger.info(f"Processing file {file_index} of {total_files}: {file_path}")

            try:
                # Process the file and yield records
                records_processed = 0

                with self.aws_client.get_file_handle(file_path) as file_handle:
                    csv_iterator = get_csv_row_iterator(file_handle, self.table_config)

                    for row_index, row in enumerate(csv_iterator):
                        # Add metadata columns
                        metadata = generate_csv_metadata_columns(
                            bucket=self.aws_client.bucket,
                            file_path=file_path,
                            row_number=row_index,
                            file_last_modified=file_modified,
                        )

                        # Merge row data with metadata
                        record = {**row, **metadata}

                        yield record
                        records_processed += 1

                        # Log progress periodically
                        if records_processed % 10000 == 0:
                            user_logger.info(f"Processed {records_processed} records from {file_path}")

                user_logger.info(f"Completed processing {file_path}: {records_processed} records")

            except Exception as e:
                user_logger.error(f"Failed to process file {file_path}: {e}")
                sys.exit(1)
