"""CSV processing utilities with compression and encoding detection."""

import codecs
import csv
import gzip
import io
import sys
import zipfile
from typing import Dict, Iterator, List

from nekt_singer_sdk.custom_logger import internal_logger, user_logger

# Constants for metadata columns
SDC_EXTRA_COLUMN = "_sdc_extra"
SDC_SOURCE_BUCKET_COLUMN = "_sdc_source_bucket"
SDC_SOURCE_FILE_COLUMN = "_sdc_source_file"
SDC_SOURCE_LINENO_COLUMN = "_sdc_source_lineno"
LAST_MODIFIED_COLUMN = "_file_last_modified"


def get_csv_row_iterator(file_handle: io.IOBase, table_config: Dict) -> Iterator[Dict]:
    """
    Get CSV row iterator with support for compression and encoding detection.

    Args:
        file_handle: File-like object from S3
        table_config: Table configuration with CSV settings

    Returns:
        Iterator of CSV rows as dictionaries
    """
    # Set CSV field size limit to handle large fields
    csv.field_size_limit(sys.maxsize)

    # Read the stream data
    if hasattr(file_handle, "read"):
        data = file_handle.read()
    else:
        # Handle streaming objects
        data = b"".join(file_handle)

    internal_logger.info(f"Processing file data, size: {len(data)} bytes")

    # Decompress if needed
    decompressed_data = _decompress_data(data)

    # Create file-like object for CSV reading
    file_stream = io.BytesIO(decompressed_data)

    # Detect encoding and create text stream
    text_stream = _create_text_stream(file_stream)

    # Create CSV reader
    delimiter = table_config.get("delimiter", ",")
    key_properties = table_config.get("key_properties", [])
    date_overrides = table_config.get("date_overrides", [])

    reader = csv.DictReader(
        (_clean_csv_line(line) for line in text_stream),
        fieldnames=None,
        restkey=SDC_EXTRA_COLUMN,
        delimiter=delimiter,
    )

    # Validate headers
    _validate_csv_headers(reader.fieldnames, key_properties, date_overrides)

    return reader


def _decompress_data(data: bytes) -> bytes:
    """
    Decompress data if it's compressed (gzip or zip).

    Args:
        data: Raw file data

    Returns:
        Decompressed data
    """
    # Check for gzip magic number (1f 8b)
    if data[:2] == b"\x1f\x8b":
        user_logger.info("Detected gzip compression, decompressing")
        try:
            return gzip.decompress(data)
        except Exception as e:
            user_logger.warning(f"Failed to decompress gzip data: {e}")
            return data

    # Check for ZIP magic number (PK)
    elif data[:2] == b"PK":
        user_logger.info("Detected ZIP compression, extracting first available CSV file")
        try:
            with zipfile.ZipFile(io.BytesIO(data), "r") as zip_file:
                # Find the first CSV file in the ZIP
                csv_files = [name for name in zip_file.namelist() if name.lower().endswith(".csv")]

                if csv_files:
                    user_logger.info(f"Extracting {csv_files[0]} from ZIP")
                    return zip_file.read(csv_files[0])
                else:
                    user_logger.warning("No CSV file found in ZIP archive")
                    return data

        except Exception as e:
            user_logger.warning(f"Failed to extract ZIP data: {e}")
            return data

    # Not compressed
    internal_logger.info("No compression detected")
    return data


def _create_text_stream(file_stream: io.BytesIO) -> Iterator[str]:
    """
    Create text stream from bytes with encoding detection.

    Args:
        file_stream: BytesIO stream

    Returns:
        Iterator of text lines
    """
    # Try UTF-8 with BOM first (most common)
    try:
        file_stream.seek(0)
        text_stream = codecs.iterdecode(file_stream, encoding="utf-8-sig")
        # Test by reading first line
        first_line = next(text_stream)
        file_stream.seek(0)
        text_stream = codecs.iterdecode(file_stream, encoding="utf-8-sig")
        internal_logger.info("Using UTF-8 with BOM encoding")
        return text_stream
    except (UnicodeDecodeError, StopIteration):
        pass

    # Try regular UTF-8
    try:
        file_stream.seek(0)
        text_stream = codecs.iterdecode(file_stream, encoding="utf-8")
        # Test by reading first line
        first_line = next(text_stream)
        file_stream.seek(0)
        text_stream = codecs.iterdecode(file_stream, encoding="utf-8")
        internal_logger.info("Using UTF-8 encoding")
        return text_stream
    except (UnicodeDecodeError, StopIteration):
        pass

    # Fallback to latin-1 (always works)
    file_stream.seek(0)
    text_stream = codecs.iterdecode(file_stream, encoding="latin-1")
    internal_logger.info("Using latin-1 encoding as fallback")
    return text_stream


def _clean_csv_line(line: str) -> str:
    """
    Clean CSV line by removing null bytes and other problematic characters.

    Args:
        line: Raw CSV line

    Returns:
        Cleaned CSV line
    """
    # Remove null bytes and other control characters that can cause issues
    return line.replace("\0", "").replace("\r", "")


def _validate_csv_headers(fieldnames: List[str], key_properties: List[str], date_overrides: List[str]) -> None:
    """
    Validate that required headers are present in the CSV.

    Args:
        fieldnames: List of CSV column names
        key_properties: Required key property columns
        date_overrides: Date override columns

    Raises:
        Exception: If required headers are missing
    """
    if not fieldnames:
        raise Exception("CSV file has no headers")

    headers = set(fieldnames)

    # Check key properties
    if key_properties:
        key_properties_set = set(key_properties)
        missing_keys = key_properties_set - headers
        if missing_keys:
            raise Exception(f"CSV file missing required key_properties headers: {missing_keys}")

    # Check date overrides
    if date_overrides:
        date_overrides_set = set(date_overrides)
        missing_dates = date_overrides_set - headers
        if missing_dates:
            raise Exception(f"CSV file missing date_overrides headers: {missing_dates}")


def generate_csv_metadata_columns(
    bucket: str, file_path: str, row_number: int, file_last_modified: str
) -> Dict[str, any]:
    """
    Generate metadata columns for CSV rows.

    Args:
        bucket: S3 bucket name
        file_path: S3 file path
        row_number: Row number (1-based, excluding header)
        file_last_modified: File last modified timestamp
    Returns:
        Dictionary of metadata columns
    """
    return {
        SDC_SOURCE_BUCKET_COLUMN: bucket,
        SDC_SOURCE_FILE_COLUMN: file_path,
        SDC_SOURCE_LINENO_COLUMN: row_number + 2,  # +2 for header and 1-based indexing
        LAST_MODIFIED_COLUMN: file_last_modified,
    }


def sample_csv_file(
    file_handle: io.IOBase, table_config: Dict, sample_rate: int = 5, max_records: int = 1000
) -> Iterator[Dict]:
    """
    Sample records from a CSV file for schema detection.

    Args:
        file_handle: File handle from S3
        table_config: Table configuration
        sample_rate: Sample every Nth record
        max_records: Maximum records to sample

    Returns:
        Iterator of sampled records
    """
    row_iterator = get_csv_row_iterator(file_handle, table_config)

    current_row = 0
    sampled_count = 0

    for row in row_iterator:
        if current_row % sample_rate == 0 and sampled_count < max_records:
            # Remove extra columns from sample for schema detection
            if SDC_EXTRA_COLUMN in row:
                row.pop(SDC_EXTRA_COLUMN)

            yield row
            sampled_count += 1

            if sampled_count % 100 == 0:
                internal_logger.info(f"Sampled {sampled_count} records")

        current_row += 1

    internal_logger.info(f"Sampling complete: {sampled_count} records from {current_row} total rows")
