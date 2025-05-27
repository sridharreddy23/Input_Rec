
# Input_Rec

This Bash script downloads, processes, and merges video and audio segments from an AWS S3 bucket within a specified time range. Here's a breakdown of your documentation:
Purpose:
The script automates the process of downloading segmented video and audio files from S3, concatenating them, and merging them into a final MP4 file. This is useful for reconstructing a continuous recording from fragmented segments.
Configuration:
The script starts with several configuration variables that you'll need to adjust:
CONCURRENCY: Number of parallel downloads to speed up the process.
S3_BUCKET: The name of your S3 bucket.
S3_BASE_PATH: The path within the bucket where the media files are stored.
AWS_REGION: The AWS region of your bucket.
START_EPOCH and END_EPOCH: The start and end timestamps (Unix epoch seconds) define the desired time range. A 60-second buffer is added to these times.
Execution Flow:
Time Range Setup: Calculates the actual start and end times, including the buffer.
Output Directory: Creates a timestamped directory to store downloaded files and changes to that directory. This keeps downloads organized.
Progress Tracking: Creates a file to track download progress for the progress bar.
download_files_parallel() Function: This key function handles the download process:
File Search: Lists all files recursively within the S3 path and filters them based on the provided file type (video or audio), extension, and epoch time. It specifically looks for files in the "00/video" and "00/audio" subdirectories.
Parallel Downloads: Uses xargs to download the matching files in parallel, improving efficiency. It uses aws s3 cp for the downloads.
Progress Bar: Displays a progress bar during the download process.
File List Population: Stores the names of the downloaded files in an array.
Main Execution:
Prints the date range being processed.
Calls download_files_parallel() twice, once for video files (h264) and once for audio files (wav).
Processing Downloaded Files:
Video Conversion: Converts the downloaded .h264 video files to .ts format using ffmpeg for easier concatenation.
Video Concatenation: Creates a list file and uses ffmpeg to concatenate the .ts files into a single .ts file.
Audio Concatenation: Similar to video, concatenates the .wav files into one.
Merging Video and Audio: Uses ffmpeg to merge the final video and audio files into a single MP4 file.
Cleanup: Moves all the intermediate files (video and audio chunks, list files, etc.) to the trash using Gio trash. This keeps the output directory clean.
Return to Original Directory: Changes back to the directory from which the script was run.
Completion Message: Prints a message indicating completion and the output directory.
Key Technologies Used:
aws s3: AWS CLI for interacting with S3.
xargs: For parallel processing.
ffmpeg: For video and audio conversion and merging.
gio trash: This is used to move files to the trash.
Bash scripting: The core scripting language.
How to Use:
Install Dependencies: Ensure you have installed the AWS CLI, ffmpeg, and gio.
Configure: Modify the configuration variables at the beginning of the script to match your environment.
Run: Execute the script: ./your_script_name.sh
Error Handling:
The script includes basic error handling, such as checking for empty file lists and failed downloads. More robust error handling could be added.
Improvements:
Logging: Add logging to a file for better tracking and debugging.
More Robust Error Handling: Implement more comprehensive error checks.
Input Validation: Validate the configuration variables.
Configuration File: Allow configuration to be read from a file instead of being hardcoded in the script.
This documentation should help you understand and maintain the script. Remember to tailor it to your specific audience and their technical level.
This Bash script automates the downloading, processing, and merging of segmented video and audio files from an AWS S3 bucket within a specified time range, enabling the reconstruction of a continuous recording from fragmented segments.

Configuration
The script is configured by setting several variables:
CONCURRENCY: Sets the number of parallel downloads to optimize speed
S3_BUCKET: The name of your S3 bucket
S3_BASE_PATH: The path within the bucket where the media files are stored
AWS_REGION: The AWS region of your bucket
START_EPOCH and END_EPOCH: The start and end timestamps (Unix epoch seconds) defining the desired time range; a 60-second buffer is added to these times
Execution Flow
Time Range Setup: Calculates the actual start and end times, including the buffer.
Output Directory: Creates a timestamped directory for storing downloaded files and navigates to it, ensuring downloads are organized.
Progress Tracking: Creates a file to track download progress for the progress bar.
download_files_parallel() Function: This function manages the download process:
File Search: Lists all files recursively within the S3 path, filtering them by file type (video or audio), extension, and epoch time. It specifically targets files in the "00/video" and "00/audio" subdirectories.
Parallel Downloads: Uses xargs to download matching files in parallel, using aws s3 cp.
Progress Bar: Displays a progress bar during the download process.
File List Population: Stores the names of the downloaded files in an array.
Main Execution:
Prints the date range being processed.
Calls download_files_parallel() twice, once for video files (h264) and once for audio files (wav).
Processing Downloaded Files:
Video Conversion: Converts downloaded .h264 video files to .ts format using ffmpeg.
Video Concatenation: Creates a list file and uses ffmpeg to concatenate .ts files into a single .ts file.
Audio Concatenation: Concatenates .wav files into one.
Merging Video and Audio: Uses ffmpeg to merge the final video and audio files into a single MP4 file.
Cleanup: Moves intermediate files to the trash using Gio trash.
Return to Original Directory: Changes back to the starting directory.
Completion Message: Prints a completion message and the output directory.
Key Technologies
aws s3: AWS CLI for S3 interaction
xargs: For parallel processing
ffmpeg: For video and audio conversion and merging
gio trash: For file deletion
Bash scripting
How to Use
Install Dependencies: AWS CLI, ffmpeg, and gio.
Configure: Modify configuration variables.
Run: Execute the script: ./your_script_name.sh
Error Handling
Includes basic error handling for empty file lists and failed downloads.

Improvements
Logging
More robust error handling
Input validation
Configuration file

High-Level Explanation: 
Let's break down the Bash script function by function:
1. progress_bar():
Bash


progress_bar() {
    local total=$1
    local current=$2
    local width=50
    local percent=$((current * 100 / total))
    local completed=$((current * width / total))
    printf "\r[%-${width}s] %d%%" "$(printf '#%.0s' $(seq 1 $completed))" "$percent"
}


Purpose: Displays a dynamic progress bar in the terminal.
Parameters:
total: The total number of items to process.
current: The number of items currently processed.
Logic:
Calculates the percentage of completion.
Calculate the number of '#' characters to represent the completed portion of the bar.
Uses printf to create and update the progress bar on a single line (\r moves the cursor back to the beginning of the line).
2. download_files_parallel():
Bash


download_files_parallel() {
    local FILE_TYPE=$1
    local EXTENSION=$2
    local -n FILE_LIST=$3

    # ... (rest of the function)
}


Purpose: Downloads files from S3 in parallel, filtering by type, extension, and time range.
Parameters:
FILE_TYPE: "video" or "audio".
EXTENSION: The file extension (e.g., "h264", "wav").
FILE_LIST: A name reference to an array variable where the downloaded filenames will be stored. This is how the function modifies the VIDEO_FILES and AUDIO_FILES arrays in the main script.
Logic:
File Search: Lists all files in the S3 path and filters them based on FILE_TYPE, EXTENSION, and whether the filename's epoch part falls within the START_EPOCH and END_EPOCH.
Parallel Downloads: Uses xargs with the -P option to download files in parallel using the aws s3 cp command. The >/dev/null 2>&1 suppresses output from the aws command, but error messages are still printed to stderr.
Progress Bar: Calls the progress_bar() function to display download progress.
File List Population: Appends the base filenames of the downloaded files to the array whose name was passed as the FILE_LIST argument.
Main Script Execution (Not a function, but the core logic):
Configuration: Sets up all the configuration variables.
Time Range Setup: Calculates the start and end times, adding the buffer.
Output Directory Creation: Creates the output directory.
Download Calls: Calls download_files_parallel() twice:
Once for video files (h264 extension), storing the results in the VIDEO_FILES array.
Once for audio files (wav extension), storing the results in the AUDIO_FILES array.
File Processing (Conditional): If both VIDEO_FILES and AUDIO_FILES arrays have elements (meaning files were downloaded), the script proceeds with processing:
Video Conversion: Converts .h264 files to .ts files using ffmpeg.
Video/Audio Concatenation: Creates list files and uses ffmpeg's concat demuxer to join the .ts and .wav files into single files.
Merging: Merges the final .ts and .wav files into a single .mp4 file using ffmpeg.
Cleanup: Moves all intermediate files to the trash using gio trash.
Return to Original Directory: Changes back to the directory where the script was started.
Completion Message: Prints a final message.
=======
# S3 ES Downloader & Parser

A robust tool for downloading Elementary Stream (ES) files from AWS S3 and parsing them into Transport Stream (TS) files.

## Features

- **Parallel S3 Downloads**: Efficiently download multiple files concurrently
- **Robust Error Handling**: Comprehensive error handling and retry mechanisms
- **Resume Capability**: Resume interrupted downloads and parsing operations
- **Progress Tracking**: Visual progress indicators and detailed logging
- **Memory Efficient**: Buffered I/O for handling large files
- **Configurable**: Extensive configuration options

## Architecture

The application is structured into several modules:

- **main.py**: Entry point and orchestration
- **config_manager.py**: Configuration loading and validation
- **s3_reader.py**: S3 file listing and downloading
- **es_parser.py**: ES file parsing and TS extraction
- **utils.py**: Utility functions

## Installation


# Clone the repository
git clone https://github.com/yourusername/s3-es-downloader.git
cd s3-es-downloader

# Install dependencies
pip install -r requirements.txt


## Usage
python main.py config.json output.ts [options]
### Command Line Options

- `config.json`: Path to the configuration file
- `output.ts`: Path for the output TS file
- `--debug`, `-d`: Enable debug logging
- `--resume`, `-r`: Resume from previous state if available
- `--cleanup`, `-c`: Clean up temporary files after processing
- `--temp-dir`, `-t`: Custom temporary directory path
- `--buffer-size`, `-b`: Buffer size in bytes (default: 1MB)

### Configuration File

```json
{
  "start_utc": 1609459200,
  "end_utc": 1609459300,
  "s3_prefix": "path/to/prefix",
  "aws_conf": {
    "aws_region": "us-east-1",
    "s3_bucket": "my-bucket"
  }
}
```

python main.py config.json output.ts
## AWS Credentials

The application uses the standard AWS credential providers:

1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. Shared credential file (`~/.aws/credentials`)
3. IAM role for EC2 instances

**Note**: It's recommended to use environment variables or IAM roles instead of putting credentials in the configuration file.```
.
├── src/
│   ├── __init__.py
│   ├── main.py
│   ├── config_manager.py
│   ├── s3_reader.py
│   ├── es_parser.py
│   └── utils.py
├── tests/
│   ├── __init__.py
│   ├── test_utils.py
│   └── test_config_manager.py
├── docs/
│   └── architecture.png
├── main.py
├── config.json
└── README.md
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

