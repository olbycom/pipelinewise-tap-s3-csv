"""Main entrypoint for tap-s3-csv CLI."""

from tap_s3_csv.tap import TapS3CSV

if __name__ == "__main__":
    TapS3CSV.cli()