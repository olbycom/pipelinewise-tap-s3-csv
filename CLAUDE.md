# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Singer tap that extracts CSV files from AWS S3 buckets. It's a PipelineWise-compatible data connector that reads CSV files from S3 and outputs JSON-formatted data following the Singer specification.

## Core Architecture

### Main Components
- `tap_s3_csv/__init__.py`: Entry point with main() function, handles discovery and sync modes
- `tap_s3_csv/s3.py`: AWS S3 client setup and file operations with retry logic
- `tap_s3_csv/sync.py`: Stream synchronization logic, processes CSV files into Singer records  
- `tap_s3_csv/config.py`: Configuration validation using voluptuous
- `tap_s3_csv/discover.py`: Stream discovery and schema generation
- `tap_s3_csv/encodings.py`: CSV encoding detection and row processing

### Key Functionality
- Supports both profile-based and credential-based AWS authentication
- Automatic encoding detection (UTF-8, UTF-8 with BOM, latin-1)
- Configurable search patterns and prefixes for S3 file discovery
- State bookmarking using file modification timestamps
- Custom delimiter support for CSV parsing

## Development Commands

### Environment Setup
```bash
make venv  # Creates virtual environment and installs dependencies
```

### Testing
```bash
make unit_tests        # Run unit tests (30% coverage minimum)
make integration_tests # Run integration tests (84% coverage minimum)
```

### Code Quality
```bash
make pylint  # Run linting with .pylintrc configuration
```

### Integration Test Setup
Integration tests require Minio server:
```bash
mkdir -p ./minio/data/awesome_bucket
UID=$(id -u) GID=$(id -g) docker-compose up -d
```

## Configuration

The tap uses `config.json` with the following key structure:
- `tables`: Array of table configurations with search patterns, prefixes, and CSV settings
- AWS credentials via profile or direct keys
- `start_date`: Filters files by modification timestamp

Sample configuration available in `config.sample.json`.

## Entry Point

The main entry point is `tap-s3-csv` command, defined in setup.py, which calls `tap_s3_csv:main`.