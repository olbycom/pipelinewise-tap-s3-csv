"""Main tap class for S3 CSV extraction."""

from typing import List

import nekt_singer_sdk.typing as th
from nekt_singer_sdk import Tap

from tap_s3_csv.streams import S3MultipleCSVFilesStream


class TapS3CSV(Tap):
    """S3 CSV tap class."""

    name = "tap-s3-csv"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "aws_access_key_id",
            th.StringType,
            description="AWS access key ID for authentication",
        ),
        th.Property(
            "aws_secret_access_key",
            th.StringType,
            description="AWS secret access key for authentication",
        ),
        th.Property(
            "aws_session_token",
            th.StringType,
            description="AWS session token for authentication",
        ),
        th.Property(
            "aws_profile",
            th.StringType,
            description="AWS profile name for authentication",
        ),
        th.Property(
            "aws_endpoint_url",
            th.StringType,
            description="AWS endpoint URL for non-AWS S3 services",
        ),
        th.Property(
            "assume_role_arn",
            th.StringType,
            description="AWS IAM role ARN to assume",
        ),
        th.Property(
            "bucket",
            th.StringType,
            required=True,
            description="S3 bucket name",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            required=True,
            description="Start date for file filtering by modification timestamp",
        ),
        th.Property(
            "tables",
            th.ArrayType(
                th.ObjectType(
                    th.Property(
                        "table_name",
                        th.StringType,
                        required=True,
                        description="Name of the table/stream",
                    ),
                    th.Property(
                        "search_pattern",
                        th.StringType,
                        required=True,
                        description="Regex pattern to match files",
                    ),
                    th.Property(
                        "search_prefix",
                        th.StringType,
                        description="S3 prefix to narrow search scope",
                    ),
                    th.Property(
                        "key_properties",
                        th.ArrayType(th.StringType),
                        description="List of primary key column names",
                    ),
                    th.Property(
                        "delimiter",
                        th.StringType,
                        description="CSV delimiter character",
                        default=",",
                    ),
                    th.Property(
                        "date_overrides",
                        th.ArrayType(th.StringType),
                        description="List of column names to treat as datetime",
                    ),
                )
            ),
            required=True,
            description="List of table configurations",
        ),
    ).to_dict()

    def discover_streams(self) -> List[S3MultipleCSVFilesStream]:
        """Return a list of discovered streams."""
        streams = []
        
        for table_config in self.config.get("tables", []):
            stream = S3MultipleCSVFilesStream(
                tap=self,
                name=table_config["table_name"],
                table_config=table_config,
            )
            streams.append(stream)
        
        return streams