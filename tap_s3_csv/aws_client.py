"""AWS S3 client with authentication and file operations."""

import os
import re
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Dict, Generator, Iterator, Optional

import backoff
import boto3
import s3fs
from aws_assume_role_lib import assume_role
from botocore.exceptions import ClientError
from nekt_singer_sdk.custom_logger import internal_logger, user_logger


class AwsClient:
    """AWS S3 client with authentication and retry logic."""

    def __init__(self, config: Dict):
        """Initialize AWS client with configuration."""
        self.config = config
        self.bucket = config["bucket"]
        self.aws_endpoint_url = config.get("aws_endpoint_url")

        # Initialize credentials
        self._credentials = None
        self._credentials_expiry = None
        self._s3_client = None
        self._s3_resource = None
        self._s3fs_client = None

        self._setup_aws_credentials()

    def _setup_aws_credentials(self) -> None:
        """Set up AWS credentials with role assumption if needed."""
        internal_logger.info("Setting up AWS credentials")

        assume_role_arn = self.config.get("assume_role_arn")

        if assume_role_arn:
            self._setup_assumed_role_credentials(assume_role_arn)
        else:
            self._setup_direct_credentials()

    def _setup_assumed_role_credentials(self, role_arn: str) -> None:
        """Set up credentials with role assumption."""
        internal_logger.info(f"Assuming role: {role_arn}")

        session_name = f"tap-s3-csv-{datetime.now().strftime('%Y%m%d%H%M%S')}"

        try:
            assumed_role = assume_role(
                role_arn=role_arn,
                session_name=session_name,
                duration_seconds=3600,  # 1 hour
            )

            self._credentials = {
                "aws_access_key_id": assumed_role["Credentials"]["AccessKeyId"],
                "aws_secret_access_key": assumed_role["Credentials"]["SecretAccessKey"],
                "aws_session_token": assumed_role["Credentials"]["SessionToken"],
            }

            # Set expiry time (refresh 5 minutes before actual expiry)
            expiry = assumed_role["Credentials"]["Expiration"]
            self._credentials_expiry = expiry - timedelta(minutes=5)

            internal_logger.info(f"Successfully assumed role, credentials expire at {expiry}")

        except Exception as e:
            internal_logger.error(f"Failed to assume role {role_arn}: {e}")
            raise

    def _setup_direct_credentials(self) -> None:
        """Set up direct AWS credentials."""
        aws_access_key_id = self.config.get("aws_access_key_id") or os.environ.get("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = self.config.get("aws_secret_access_key") or os.environ.get("AWS_SECRET_ACCESS_KEY")
        aws_session_token = self.config.get("aws_session_token") or os.environ.get("AWS_SESSION_TOKEN")
        aws_profile = self.config.get("aws_profile") or os.environ.get("AWS_PROFILE")

        if aws_access_key_id and aws_secret_access_key:
            self._credentials = {"aws_access_key_id": aws_access_key_id, "aws_secret_access_key": aws_secret_access_key}
            if aws_session_token:
                self._credentials["aws_session_token"] = aws_session_token

            internal_logger.info("Using direct AWS credentials")
        else:
            # Use default session with profile or environment
            session_kwargs = {}
            if aws_profile:
                session_kwargs["profile_name"] = aws_profile

            self._session = boto3.Session(**session_kwargs)
            internal_logger.info(f"Using AWS profile: {aws_profile or 'default'}")

    def _refresh_credentials_if_needed(self) -> None:
        """Refresh credentials if they are about to expire."""
        if self._credentials_expiry and datetime.utcnow() >= self._credentials_expiry:
            internal_logger.info("Refreshing AWS credentials")
            self._setup_aws_credentials()
            # Reset clients to use new credentials
            self._s3_client = None
            self._s3_resource = None
            self._s3fs_client = None

    @property
    def s3_client(self):
        """Get boto3 S3 client with current credentials."""
        self._refresh_credentials_if_needed()

        if not self._s3_client:
            client_kwargs = {}
            if self.aws_endpoint_url:
                client_kwargs["endpoint_url"] = self.aws_endpoint_url

            if hasattr(self, "_session"):
                self._s3_client = self._session.client("s3", **client_kwargs)
            else:
                self._s3_client = boto3.client("s3", **self._credentials, **client_kwargs)

        return self._s3_client

    @property
    def s3_resource(self):
        """Get boto3 S3 resource with current credentials."""
        self._refresh_credentials_if_needed()

        if not self._s3_resource:
            resource_kwargs = {}
            if self.aws_endpoint_url:
                resource_kwargs["endpoint_url"] = self.aws_endpoint_url

            if hasattr(self, "_session"):
                self._s3_resource = self._session.resource("s3", **resource_kwargs)
            else:
                self._s3_resource = boto3.resource("s3", **self._credentials, **resource_kwargs)

        return self._s3_resource

    @property
    def s3fs_client(self):
        """Get s3fs client with current credentials."""
        self._refresh_credentials_if_needed()

        if not self._s3fs_client:
            s3fs_kwargs = {
                "anon": False,
            }

            if self.aws_endpoint_url:
                s3fs_kwargs["endpoint_url"] = self.aws_endpoint_url

            if hasattr(self, "_session"):
                # Use session credentials
                creds = self._session.get_credentials()
                s3fs_kwargs.update(
                    {
                        "key": creds.access_key,
                        "secret": creds.secret_key,
                    }
                )
                if creds.token:
                    s3fs_kwargs["token"] = creds.token
            else:
                s3fs_kwargs.update(self._credentials)

            self._s3fs_client = s3fs.S3FileSystem(**s3fs_kwargs)

        return self._s3fs_client

    def retry_pattern(self):
        """Return retry decorator for AWS operations."""
        return backoff.on_exception(
            backoff.expo, ClientError, max_tries=5, on_backoff=self._log_backoff_attempt, factor=10
        )

    def _log_backoff_attempt(self, details):
        """Log backoff attempts."""
        internal_logger.info(f"AWS operation failed, retrying (attempt {details.get('tries')})")

    @backoff.on_exception(backoff.expo, ClientError, max_tries=5, factor=10)
    def list_files_in_bucket(self, search_prefix: Optional[str] = None) -> Generator[Dict, None, None]:
        """List all files in the bucket matching the prefix."""
        internal_logger.info(f'Listing files in bucket "{self.bucket}"')

        s3_object_count = 0
        max_results = 1000

        paginator_kwargs = {
            "Bucket": self.bucket,
            "MaxKeys": max_results,
        }

        if search_prefix:
            paginator_kwargs["Prefix"] = search_prefix

        paginator = self.s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(**paginator_kwargs)

        # Only return objects with STANDARD storage class
        filtered_s3_objects = page_iterator.search("Contents[?StorageClass=='STANDARD']")

        for s3_obj in filtered_s3_objects:
            if s3_obj:  # Filter out None results
                s3_object_count += 1
                yield s3_obj

        internal_logger.info(f"Found {s3_object_count} files in bucket")

    def get_input_files_for_table(
        self, table_config: Dict, modified_since: Optional[datetime] = None
    ) -> Generator[Dict, None, None]:
        """Get files matching table configuration."""
        search_prefix = table_config.get("search_prefix")
        search_pattern = table_config["search_pattern"]
        table_name = table_config["table_name"]

        try:
            matcher = re.compile(search_pattern)
        except re.error:
            user_logger.error(f"search_pattern for table `{table_name}` is not a valid regular expression")
            sys.exit(1)

        user_logger.info(f'Looking for files in bucket "{self.bucket}" matching pattern "{search_pattern}"')

        if modified_since:
            user_logger.info(f"Filtering files modified after {modified_since}")

        matched_files_count = 0
        unmatched_files_count = 0

        # Get all files and sort by modification time
        all_files = list(self.list_files_in_bucket(search_prefix))
        sorted_files = sorted(all_files, key=lambda x: x["LastModified"])

        for s3_object in sorted_files:
            key = s3_object["Key"]
            last_modified = s3_object["LastModified"]

            # Skip empty files
            if s3_object["Size"] == 0:
                internal_logger.info(f"Skipping empty file: {key}")
                unmatched_files_count += 1
                continue

            # Check if file matches pattern
            if matcher.search(key):
                matched_files_count += 1

                # Check modification time
                if not modified_since or last_modified > modified_since:
                    internal_logger.info(f"Found matching file: {key} (modified: {last_modified})")
                    yield {
                        "key": key,
                        "last_modified": last_modified,
                        "size": s3_object["Size"],
                    }
                else:
                    internal_logger.info(f"Skipping file {key} - too old (modified: {last_modified})")
            else:
                unmatched_files_count += 1

        if matched_files_count == 0:
            msg = (
                f'No files found in bucket "{self.bucket}" matching '
                f'prefix "{search_prefix}" and pattern "{search_pattern}"'
                if search_prefix
                else f'No files found in bucket "{self.bucket}" matching pattern "{search_pattern}"'
            )
            user_logger.error(msg)
            sys.exit(1)

        internal_logger.info(f"Found {matched_files_count} matching files, {unmatched_files_count} non-matching files")

    @contextmanager
    def get_file_handle(self, s3_path: str) -> Iterator:
        """Get file handle for S3 object."""
        internal_logger.info(f"Opening file: {s3_path}")

        try:
            s3_object = self.s3_resource.Bucket(self.bucket).Object(s3_path)
            file_handle = s3_object.get()["Body"]
            yield file_handle
        except Exception as e:
            internal_logger.error(f"Failed to open file {s3_path}: {e}")
            raise
        finally:
            internal_logger.info(f"Closed file: {s3_path}")
