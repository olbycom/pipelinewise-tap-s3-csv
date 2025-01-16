"""
Tap S3 csv main script
"""

import sys
from typing import Dict

import custom_logger

_ = custom_logger


import singer
import ujson
from custom_logger import internal_logger, user_logger
from singer import metadata

from tap_s3_csv import s3
from tap_s3_csv.config import CONFIG_CONTRACT
from tap_s3_csv.discover import discover_streams
from tap_s3_csv.sync import sync_stream

REQUIRED_CONFIG_KEYS = ["start_date", "bucket"]


def do_discover(config: Dict) -> None:
    """
    Discovers the source by connecting to the it and collecting information about the given tables/streams,
    it dumps the information to stdout
    :param config: connection and streams information
    :return: nothing
    """
    internal_logger.info("Starting discover.")
    streams = discover_streams(config)
    if not streams:
        user_logger.error("No streams found.")
        sys.exit(1)
    catalog = {"streams": streams}
    ujson.dump(catalog, sys.stdout, indent=2)
    internal_logger.info("Finished discover.")


def stream_is_selected(meta_data: Dict) -> bool:
    """
    Detects whether the stream is selected to be synced
    :param meta_data: stream metadata
    :return: True if selected, False otherwise
    """
    return meta_data.get((), {}).get("selected", False)


def do_sync(config: Dict, catalog: Dict, state: Dict) -> None:
    """
    Syncs every selected stream in the catalog and updates the state
    :param config: connection and streams information
    :param catalog: Streams catalog
    :param state: current state
    :return: Nothing
    """
    for stream in catalog["streams"]:
        stream_name = stream["tap_stream_id"]
        mdata = metadata.to_map(stream["metadata"])
        table_spec = next(s for s in config["tables"] if s["table_name"] == stream_name)
        if not stream_is_selected(mdata):
            continue

        singer.write_state(state)
        key_properties = metadata.get(mdata, (), "table-key-properties")
        singer.write_schema(stream_name, stream["schema"], key_properties)

        user_logger.info(f"{stream_name}: Starting sync")
        counter_value = sync_stream(config, state, table_spec, stream)


@singer.utils.handle_top_exception(internal_logger)
def main() -> None:
    """
    Main function
    :return: None
    """
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config

    # Reassign the config tables to the validated object
    config["tables"] = CONFIG_CONTRACT(config.get("tables", {}))

    try:
        for _ in s3.list_files_in_bucket(config["bucket"]):
            break
        internal_logger.warning("I have direct access to the bucket without assuming the configured role.")
    except Exception:
        s3.setup_aws_client(config)

    if args.discover:
        do_discover(args.config)
    elif args.properties:
        do_sync(config, args.properties, args.state)


if __name__ == "__main__":
    main()
