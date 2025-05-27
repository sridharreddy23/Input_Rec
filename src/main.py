#!/usr/bin/env python3
"""
Main module for the ES Downloader and Parser.
"""
import os
import sys
import argparse
import logging
import tempfile
import signal
import time
from typing import Dict, Any

import boto3
from botocore.exceptions import ClientError

from .utils import (
    print_banner, print_section_header, print_final_success,
    format_datetime, load_progress_state, log
)
from .config_manager import ConfigManager
from .s3_reader import S3Reader
from .es_parser import ESParser

def signal_handler(sig, frame):
    """
    Handle interrupt signals.
    
    Args:
        sig: Signal number
        frame: Current stack frame
    """
    log.warning("\n\nProcess interrupted by user. Exiting...")
    # Perform any necessary cleanup here if needed (temp dirs usually handled by context managers)
    sys.exit(0)

def setup_aws_credentials(config_manager: ConfigManager):
    """
    Set up AWS credentials from the configuration.
    
    Args:
        config_manager: Configuration manager
    """
    credentials = config_manager.get_aws_credentials()
    if credentials:
        if "access_key" in credentials:
            os.environ["AWS_ACCESS_KEY_ID"] = credentials["access_key"]
        if "secret_key" in credentials:
            os.environ["AWS_SECRET_ACCESS_KEY"] = credentials["secret_key"]
        if "session_token" in credentials:
            os.environ["AWS_SESSION_TOKEN"] = credentials["session_token"]
    
    region = config_manager.get_aws_region()
    if region:
        os.environ["AWS_DEFAULT_REGION"] = region

def main():
    """
    Main entry point for the ES Downloader and Parser.
    """
    # Set up signal handler
    signal.signal(signal.SIGINT, signal_handler)
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="S3 ES Downloader & Parser v3.0",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Example: python -m src.main config.json /path/to/output.ts"
    )
    parser.add_argument('config', help="Path to the configuration JSON file")
    parser.add_argument('output', help="Path for the final concatenated TS output file")
    parser.add_argument('--debug', '-d', action='store_true', help="Enable debug logging")
    parser.add_argument('--resume', '-r', action='store_true', help="Resume from previous state if available")
    parser.add_argument('--cleanup', '-c', action='store_true', help="Clean up temporary files after processing")
    parser.add_argument('--temp-dir', '-t', help="Custom temporary directory path")
    parser.add_argument('--buffer-size', '-b', type=int, default=1024*1024, help="Buffer size in bytes (default: 1MB)")

    if len(sys.argv) == 1:  # No arguments provided
        print_banner()
        parser.print_help(sys.stderr)
        sys.exit(1)

    args = parser.parse_args()

    if args.debug:
        log.setLevel(logging.DEBUG)
        log.info("Debug logging enabled.")

    print_banner()  # Show banner after potential debug enabling

    try:
        # --- Load Configuration ---
        config_manager = ConfigManager(args.config)
        start_utc = config_manager.get_start_utc()
        end_utc = config_manager.get_end_utc()
        full_s3_prefix = config_manager.get_s3_prefix()
        output_filepath = args.output
        
        # Set up AWS credentials
        setup_aws_credentials(config_manager)

        print_section_header("Processing Setup")
        log.info(f"Input Config: {args.config}")
        log.info(f"Output File: {output_filepath}")
        log.info(f"Time Range: {format_datetime(start_utc)} -> {format_datetime(end_utc)}")
        log.info(f"Target S3 Prefix: {full_s3_prefix}")
        log.info(f"Buffer Size: {args.buffer_size / 1024:.1f} KiB")
        
        # Set up resume state file if resuming
        resume_state_file = None
        if args.resume:
            resume_state_file = f"{output_filepath}.state"
            log.info(f"Resume state file: {resume_state_file}")
        
        # Load previous state if resuming
        previous_state = None
        if args.resume and os.path.exists(f"{output_filepath}.state"):
            previous_state = load_progress_state(f"{output_filepath}.state")
            if previous_state:
                log.info(f"Found previous state from {time.ctime(previous_state.get('timestamp', 0))}")
                
                # Check if the previous run was completed
                if previous_state.get("completed", False):
                    log.info("Previous run was completed successfully. Nothing to do.")
                    print_final_success()
                    return 0

        # --- Create Temporary Directory ---
        temp_dir_context = None
        if args.temp_dir:
            # Use custom temp directory
            temp_dir = args.temp_dir
            os.makedirs(temp_dir, exist_ok=True)
            log.info(f"Using custom temporary directory: {temp_dir}")
        else:
            # Use auto-cleanup temporary directory
            temp_dir_context = tempfile.TemporaryDirectory(prefix="s3_es_parser_")
            temp_dir = temp_dir_context.name
            log.info(f"Using temporary directory for downloads: {temp_dir}")

        try:
            # --- Step 1: Download Files ---
            s3_reader = S3Reader(start_utc, end_utc, full_s3_prefix, temp_dir, resume_state_file)
            
            # Resume from previous state if available
            if previous_state and "downloaded_files" in previous_state:
                s3_reader.resume_from_state(previous_state)
            
            available_files = s3_reader.download_files_parallel()

            if not available_files:
                log.error("No files were available for parsing. Exiting.")
                return 1

            # --- Step 2: Parse Files ---
            es_parser = ESParser(start_utc, end_utc, output_filepath, args.buffer_size, resume_state_file)
            
            # Resume parsing from previous state if available
            if previous_state and "total_packets_processed" in previous_state:
                es_parser.resume_from_state(previous_state)
            
            es_parser.process_files(available_files, cleanup_after_processing=args.cleanup)
        finally:
            # If using a context manager for temp_dir, it will be cleaned up automatically
            if temp_dir_context:
                log.info(f"Cleaning up temporary directory: {temp_dir}")
            elif args.cleanup:
                log.info(f"Keeping custom temporary directory: {temp_dir}")

        # --- Final Success ---
        print_final_success()
        log.info("Script finished successfully.")
        return 0

    except FileNotFoundError as e:
        log.error(f"File not found: {e}")
        return 1
    except PermissionError as e:
        log.error(f"Permission error: {e}")
        return 1
    except ClientError as e:
        log.error(f"AWS Client Error: {e}", exc_info=args.debug)  # Show trace only in debug
        return 1
    except KeyboardInterrupt:
        # Already handled by signal handler, but catch here just in case
        log.warning("Operation cancelled by user (KeyboardInterrupt).")
        return 1
    except Exception as e:
        log.exception("An unexpected fatal error occurred:")  # Log full traceback
        return 1

if __name__ == "__main__":
    sys.exit(main())