# Architecture Documentation

## Overview

The S3 ES Downloader & Parser is designed to efficiently download Elementary Stream (ES) files from AWS S3 and parse them into a single Transport Stream (TS) file. The application is structured to handle large volumes of data with minimal memory footprint and provide robust error handling and recovery mechanisms.

## Component Diagram

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │     │                 │
│  Configuration  │────▶│    S3 Reader    │────▶│    ES Parser    │
│                 │     │                 │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘
         │                      │                      │
         │                      │                      │
         ▼                      ▼                      ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │     │                 │
│  Config Manager │     │   AWS S3 API    │     │   Output File   │
│                 │     │                 │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

## Key Components

### 1. Main Module (`main.py`)

The entry point and orchestrator for the application. It:
- Parses command line arguments
- Initializes the configuration manager
- Sets up AWS credentials
- Creates temporary directories
- Coordinates the S3 reader and ES parser
- Handles signals and exceptions

### 2. Configuration Manager (`config_manager.py`)

Responsible for loading and validating the configuration file. It:
- Loads the JSON configuration file
- Validates the configuration parameters
- Provides accessor methods for configuration values
- Handles AWS credential configuration

### 3. S3 Reader (`s3_reader.py`)

Handles the downloading of ES files from S3. It:
- Generates a list of files to download based on the time range
- Downloads files in parallel using a thread pool
- Handles retries and error cases
- Tracks download progress and statistics
- Supports resuming interrupted downloads

### 4. ES Parser (`es_parser.py`)

Parses the downloaded ES files and extracts the TS payload. It:
- Processes ES files sequentially in chronological order
- Parses the ES file headers
- Extracts and writes the TS payload to the output file
- Uses buffered I/O for efficient memory usage
- Tracks parsing progress and statistics
- Supports resuming interrupted parsing operations

### 5. Utilities (`utils.py`)

Provides common utility functions used throughout the application:
- Date and time formatting
- S3 path manipulation
- Progress visualization
- Configuration validation
- State persistence for resuming operations

## Data Flow

1. The user provides a configuration file and output path via command line arguments
2. The configuration manager loads and validates the configuration
3. The S3 reader generates a list of files to download based on the time range
4. The S3 reader downloads the files in parallel to a temporary directory
5. The ES parser processes the downloaded files sequentially
6. For each ES file, the parser:
   - Reads the ES header
   - Extracts the TS payload
   - Writes the TS payload to the output file
7. The application reports progress and statistics throughout the process

## Error Handling and Recovery

- **Network Errors**: The S3 reader implements retry logic for transient network errors
- **File Not Found**: The S3 reader handles missing files gracefully
- **Permission Errors**: The application reports permission issues clearly
- **Interrupted Operations**: The application supports resuming from interruptions
- **Corrupt Files**: The ES parser detects and reports corrupt or truncated files

## Performance Considerations

- **Parallel Downloads**: Multiple S3 files are downloaded concurrently
- **Buffered I/O**: The ES parser uses buffered I/O to minimize memory usage
- **Progress Tracking**: The application provides real-time progress information
- **Resource Cleanup**: Temporary files are cleaned up after successful processing

## Security Considerations

- **AWS Credentials**: The application supports multiple secure methods for AWS authentication
- **Credential Warning**: The application warns if credentials are found in the configuration file
- **Temporary File Handling**: Temporary files are stored in secure locations

## Future Enhancements

- **Streaming Mode**: Process files as they are downloaded without storing them locally
- **Checksum Validation**: Validate file integrity using checksums
- **Compression Support**: Support for compressed ES files
- **Multi-Region Support**: Enhanced support for cross-region operations
- **Web Interface**: A web-based UI for monitoring and controlling the process