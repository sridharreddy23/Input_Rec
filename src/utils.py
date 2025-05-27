#!/usr/bin/env python3
"""
Utility functions for the ES Downloader and Parser.
"""
import os
import re
import datetime as dt
import logging
from typing import Tuple, Dict, Any, Optional, List
from colorama import Fore, Style, init
import sys

# Initialize colorama
init(autoreset=True)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)

def format_datetime(utc_timestamp: int) -> str:
    """
    Formats a UTC timestamp into a human-readable string.
    
    Args:
        utc_timestamp: Unix timestamp in seconds
        
    Returns:
        Formatted datetime string in UTC
    """
    try:
        return dt.datetime.fromtimestamp(utc_timestamp, dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return f"Invalid Timestamp ({utc_timestamp})"

def get_bucket_name_path_from_url(s3_url: str) -> Tuple[str, str]:
    """
    Extracts S3 bucket name and object path from an S3 URL.
    
    Args:
        s3_url: S3 URL in the format s3://bucket-name/path/to/object
        
    Returns:
        Tuple of (bucket_name, object_path)
        
    Raises:
        ValueError: If the URL format is invalid
    """
    if not s3_url.startswith("s3://"):
        raise ValueError(f"Invalid S3 URL format: {s3_url}")
    parts = s3_url[5:].split('/', 1)
    bucket_name = parts[0]
    bucket_path = parts[1] if len(parts) > 1 else ""
    return bucket_name, bucket_path

def get_s3_path(s3_prefix: str, rel_path: str) -> str:
    """
    Constructs the full S3 object path.
    
    Args:
        s3_prefix: S3 prefix (s3://bucket-name/path)
        rel_path: Relative path to append
        
    Returns:
        Full S3 path
    """
    # Ensure prefix ends with a slash
    if not s3_prefix.endswith('/'):
        s3_prefix += "/"
    # Ensure relative path doesn't start with a slash
    if rel_path.startswith('/'):
        rel_path = rel_path[1:]
    return s3_prefix + rel_path

def get_file_duration(file_path_or_name: str) -> int:
    """
    Estimates file duration in seconds based on filename pattern 'start-end.es'.
    
    Args:
        file_path_or_name: Path or filename to parse
        
    Returns:
        Duration in seconds, or default (4 seconds) if parsing fails
    """
    filename = os.path.basename(file_path_or_name)
    match = re.match(r'(\d+)-(\d+)\.es$', filename)
    if match:
        try:
            start_epoch = int(match.group(1))
            end_epoch = int(match.group(2))
            duration = end_epoch - start_epoch
            return duration if duration > 0 else 4  # Basic sanity check
        except ValueError:
            log.warning(f"Could not parse epochs from filename: {filename}. Using default duration 4s.")
            return 4
    else:
        log.warning(f"Filename '{filename}' does not match expected pattern 'start-end.es'. Using default duration 4s.")
        return 4  # Default duration if pattern doesn't match

def get_file_path_to_read(base_utc: int) -> str:
    """
    Calculates the expected relative file path based on a UTC timestamp.
    Assumes 4-second file chunks aligned to multiples of 4 seconds.
    
    Args:
        base_utc: UTC timestamp in seconds
        
    Returns:
        Relative file path (e.g., "20230101/12/1672567200-1672567204.es")
    """
    # Find the start of the 4-second interval the timestamp falls into
    seconds_past_hour = base_utc % 3600
    seconds_into_interval = seconds_past_hour % 4
    interval_start_utc = base_utc - seconds_into_interval

    # Use the *start* of the interval to determine the file name
    start_epoch = interval_start_utc
    end_epoch = start_epoch + 4  # Assuming 4 second files

    dt_obj = dt.datetime.fromtimestamp(start_epoch, dt.timezone.utc)
    date_str = dt_obj.strftime("%d%m%Y")
    hour_str = dt_obj.strftime("%H")  # Zero-padded hour

    file_path = f"{date_str}/{hour_str}/{start_epoch}-{end_epoch}.es"
    return file_path

def get_start_utc_from_filename(filename: str) -> int:
    """
    Extracts the start UTC epoch second from a filename like '.../12345-67890.es'.
    
    Args:
        filename: Path or filename to parse
        
    Returns:
        Start UTC timestamp in seconds, or 0 if parsing fails
    """
    basename = os.path.basename(filename)
    match = re.match(r'(\d+)-(\d+)\.es$', basename)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            log.error(f"Could not parse start epoch from filename: {basename}")
            return 0  # Or raise error, returning 0 might cause sorting issues
    log.error(f"Filename pattern not matched for start epoch extraction: {basename}")
    return 0  # Or raise error

def convert_pcr_27mhz_to_pcr_ns(pcr_27mhz: int) -> int:
    """
    Converts a 27MHz PCR value to nanoseconds.
    
    Args:
        pcr_27mhz: PCR value at 27MHz
        
    Returns:
        PCR value in nanoseconds
    """
    if pcr_27mhz < 0: 
        return 0  # Handle potential weird values
    return (int(pcr_27mhz) * 1000) // 27  # Integer division for nanoseconds

def print_banner():
    """Prints a formatted application banner."""
    banner = f"""
{Fore.CYAN}╔═══════════════════════════════════════════════════╗
{Fore.CYAN}║ {Fore.YELLOW}    _      _                                   {Fore.CYAN} ║
{Fore.CYAN}║ {Fore.YELLOW}   | |    (_)                                  {Fore.CYAN} ║
{Fore.CYAN}║ {Fore.YELLOW}   | |     _ _   _____  ___                    {Fore.CYAN} ║
{Fore.CYAN}║ {Fore.YELLOW}   | |    | | | / / _ \\/ _ \\                   {Fore.CYAN} ║
{Fore.CYAN}║ {Fore.YELLOW}   | |____| | |/ /  __/  __/                   {Fore.CYAN} ║
{Fore.CYAN}║ {Fore.YELLOW}   |______|_|___/ \\___|\\___/                   {Fore.CYAN} ║
{Fore.CYAN}║ {Fore.YELLOW}      S3 ES Downloader & Parser                {Fore.CYAN} ║
{Fore.CYAN}║ {Fore.GREEN}             Enhanced Version v3.0              {Fore.CYAN} ║
{Fore.CYAN}╚═══════════════════════════════════════════════════╝
{Fore.YELLOW}[Press Ctrl+C at any time to exit the program]
"""
    print(banner)

def print_section_header(title: str):
    """
    Prints a formatted section header.
    
    Args:
        title: Header title text
    """
    print(f"\n{Fore.CYAN}╔{'═' * (len(title) + 8)}╗")
    print(f"{Fore.CYAN}║    {Fore.YELLOW}{title}{Fore.CYAN}    ║")
    print(f"{Fore.CYAN}╚{'═' * (len(title) + 8)}╝{Style.RESET_ALL}")

def print_final_success():
    """Prints a final success message box."""
    print(f"\n{Fore.GREEN}╔{'═' * 45}╗")
    print(f"{Fore.GREEN}║{' ' * 45}║")
    print(f"{Fore.GREEN}║   {Fore.YELLOW}Process completed successfully!{' ' * 10}{Fore.GREEN}║")
    print(f"{Fore.GREEN}║{' ' * 45}║")
    print(f"{Fore.GREEN}╚{'═' * 45}╝{Style.RESET_ALL}")

def print_progress(current: int, total: int, prefix: str = "", suffix: str = "", length: int = 50):
    """
    Prints or updates a console progress bar.
    
    Args:
        current: Current progress value
        total: Total value for 100% progress
        prefix: Text before the progress bar
        suffix: Text after the progress bar
        length: Width of the progress bar in characters
    """
    if total == 0:  # Avoid division by zero
        percent = 100
        bar_fill = length
    else:
        percent = int(100 * (current / float(total)))
        bar_fill = int(length * current // total)

    bar = f"{Fore.GREEN}{'█' * bar_fill}{Fore.WHITE}{'░' * (length - bar_fill)}"
    # Use \r to return to the start of the line, overwrite previous progress
    sys.stdout.write(f"\r{prefix} |{bar}| {percent}% {suffix}")
    sys.stdout.flush()
    if current >= total:  # Print newline when done
        sys.stdout.write("\n")
        sys.stdout.flush()

def validate_config(config: Dict[str, Any]) -> bool:
    """
    Validates the configuration dictionary.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        True if valid, False otherwise
        
    Raises:
        ValueError: If configuration is invalid
    """
    # Check required keys
    required_keys = ["start_utc", "end_utc", "s3_prefix", "aws_conf"]
    for key in required_keys:
        if key not in config:
            raise ValueError(f"Missing required configuration key: {key}")
    
    # Check AWS configuration
    aws_conf = config.get("aws_conf", {})
    if "s3_bucket" not in aws_conf:
        raise ValueError("Missing required AWS configuration key: s3_bucket")
    
    # Validate time range
    start_utc = int(config["start_utc"])
    end_utc = int(config["end_utc"])
    if start_utc >= end_utc:
        raise ValueError(f"Invalid time range: start_utc ({start_utc}) must be less than end_utc ({end_utc})")
    
    # Validate S3 prefix format
    s3_prefix = config["s3_prefix"]
    if not isinstance(s3_prefix, str):
        raise ValueError(f"S3 prefix must be a string, got {type(s3_prefix)}")
    
    # Check for credentials in config (warning only)
    if aws_conf.get("access_key") or aws_conf.get("secret_key"):
        log.warning("AWS credentials found in config file. Consider using environment variables, "
                   "~/.aws/credentials, or IAM roles instead.")
    
    return True

def save_progress_state(state_file: str, progress_data: Dict[str, Any]) -> bool:
    """
    Saves the current progress state to a file for potential resume.
    
    Args:
        state_file: Path to the state file
        progress_data: Dictionary of progress data to save
        
    Returns:
        True if successful, False otherwise
    """
    import json
    try:
        with open(state_file, 'w') as f:
            json.dump(progress_data, f)
        return True
    except Exception as e:
        log.error(f"Failed to save progress state: {e}")
        return False

def load_progress_state(state_file: str) -> Dict[str, Any]:
    """
    Loads the progress state from a file.
    
    Args:
        state_file: Path to the state file
        
    Returns:
        Dictionary of progress data, or empty dict if file not found or invalid
    """
    import json
    try:
        with open(state_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        log.info(f"No progress state file found at {state_file}")
        return {}
    except json.JSONDecodeError:
        log.warning(f"Invalid JSON in progress state file {state_file}")
        return {}
    except Exception as e:
        log.error(f"Error loading progress state: {e}")
        return {}