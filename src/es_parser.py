#!/usr/bin/env python3
"""
ES Parser module for parsing ES files and extracting TS payloads.
"""
import os
import logging
from collections import OrderedDict
from typing import List, Dict, Any, Optional, BinaryIO
import time

from .utils import (
    print_section_header, print_progress, get_start_utc_from_filename, log
)

# Header size in ES file
ES_HEADER_SIZE_BYTES = 27

class ESParser:
    """
    Parses ES files and writes TS payload to an output file.
    
    Attributes:
        start_utc_s: Start time in UTC seconds
        end_utc_s: End time in UTC seconds
        ts_dump_file_path: Path to the output TS file
        output_file_handle: File handle for the output file
        pcr_utc_map: Map of PCR values to UTC timestamps
        last_pcr_utc_ns: Last PCR UTC timestamp in nanoseconds
        pcr_utc_diff: List of PCR-UTC differences
        total_packets_processed: Count of processed packets
        total_bytes_processed: Count of processed bytes
        total_files_processed: Count of processed files
        total_files_skipped: Count of skipped files
        total_files_failed: Count of failed files
        buffer_size: Size of the write buffer in bytes
        resume_state_file: Path to the resume state file
    """
    
    def __init__(self, start_utc_s: int, end_utc_s: int, ts_dump_file_path: str, 
                 buffer_size: int = 1024*1024, resume_state_file: Optional[str] = None):
        """
        Initialize the ESParser.
        
        Args:
            start_utc_s: Start time in UTC seconds
            end_utc_s: End time in UTC seconds
            ts_dump_file_path: Path to the output TS file
            buffer_size: Size of the write buffer in bytes
            resume_state_file: Optional path to the resume state file
        """
        self.start_utc_s = start_utc_s
        self.end_utc_s = end_utc_s
        self.ts_dump_file_path = ts_dump_file_path
        self.output_file_handle = None
        self.pcr_utc_map = OrderedDict()  # Optional: for analysis
        self.last_pcr_utc_ns = -1        # Optional: for analysis diffs
        self.pcr_utc_diff = []           # Optional: for analysis
        self.buffer_size = buffer_size
        self.resume_state_file = resume_state_file
        self._write_buffer = bytearray(buffer_size)
        self._buffer_position = 0

        # Statistics
        self.total_packets_processed = 0
        self.total_bytes_processed = 0  # Input ES bytes
        self.total_files_processed = 0
        self.total_files_skipped = 0  # Files listed but not found on disk
        self.total_files_failed = 0  # Files found but failed during parsing
        self.output_bytes_written = 0  # Output TS bytes

        log.info("ES Parser Initialized:")
        log.info(f"  Output File: {ts_dump_file_path}")
        log.info(f"  Time Range: {start_utc_s} -> {end_utc_s}")
        log.info(f"  Buffer Size: {buffer_size / 1024:.1f} KiB")
        
        # Create output directory if it doesn't exist
        output_dir = os.path.dirname(ts_dump_file_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

    def _flush_buffer(self):
        """
        Flush the write buffer to the output file.
        """
        if self._buffer_position > 0 and self.output_file_handle:
            self.output_file_handle.write(memoryview(self._write_buffer)[:self._buffer_position])
            self.output_bytes_written += self._buffer_position
            self._buffer_position = 0

    def _write_ts_chunk(self, ts_payload: bytes):
        """
        Writes a chunk of TS payload to the output file.
        
        Args:
            ts_payload: TS payload bytes
            
        Raises:
            IOError: If writing to the output file fails
            RuntimeError: If the output file handle is invalid
        """
        if self.output_file_handle and not self.output_file_handle.closed:
            try:
                # If payload is larger than remaining buffer space, flush buffer first
                if len(ts_payload) + self._buffer_position > self.buffer_size:
                    self._flush_buffer()
                
                # If payload is larger than buffer, write directly
                if len(ts_payload) > self.buffer_size:
                    self.output_file_handle.write(ts_payload)
                    self.output_bytes_written += len(ts_payload)
                else:
                    # Copy payload to buffer
                    self._write_buffer[self._buffer_position:self._buffer_position + len(ts_payload)] = ts_payload
                    self._buffer_position += len(ts_payload)
                    
                    # Flush if buffer is full
                    if self._buffer_position >= self.buffer_size:
                        self._flush_buffer()
            except IOError as e:
                log.exception(f"FATAL: IOError writing to output file {self.ts_dump_file_path}")
                # Re-raise or handle as appropriate (e.g., exit)
                raise
        else:
            log.error("Attempted to write TS chunk but output file handle is not open.")
            # This indicates a programming error in how process_files manages the handle
            raise RuntimeError("Output file handle is invalid during write operation.")

    def _process_single_es_file(self, file_path: str) -> bool:
        """
        Reads and parses a single ES file chunk by chunk.
        
        Args:
            file_path: Path to the ES file
            
        Returns:
            True if successful, False otherwise
        """
        log.info(f"Parsing file: {os.path.basename(file_path)}")
        packets_in_file = 0
        bytes_in_file = 0
        success = False
        try:
            with open(file_path, "rb") as f_in:
                while True:
                    header_bytes = f_in.read(ES_HEADER_SIZE_BYTES)
                    if not header_bytes:
                        log.debug(f"End of file reached for {os.path.basename(file_path)}")
                        break  # Normal end of file

                    if len(header_bytes) < ES_HEADER_SIZE_BYTES:
                        log.warning(f"Incomplete header ({len(header_bytes)} bytes) "
                                    f"at end of {os.path.basename(file_path)}. Stopping parse for this file.")
                        break  # Likely truncated file

                    # --- Parse Header ---
                    try:
                        # Assuming header format: 1B start code, 2B header len (unused?),
                        # 8B UTC ns (little-endian), 8B PCR 27MHz (little-endian), 8B Size (little-endian)
                        utc_ns = int.from_bytes(header_bytes[3:11], "little")
                        pcr_27mhz = int.from_bytes(header_bytes[11:19], "little")
                        payload_size = int.from_bytes(header_bytes[19:27], "little")
                    except Exception as e:
                        log.error(f"Failed to parse header bytes in {os.path.basename(file_path)}: {e}", exc_info=True)
                        break  # Cannot continue if header is corrupt

                    # --- Validate Payload Size ---
                    if payload_size < 0:  # Allow 0 size? Decide based on spec. Assume invalid here.
                        log.warning(f"Invalid payload size ({payload_size}) encountered in "
                                    f"{os.path.basename(file_path)}. Stopping parse for this file.")
                        break
                    if payload_size == 0:
                        log.debug(f"Zero payload size packet found in {os.path.basename(file_path)}, skipping payload read.")
                        # Still process header info if needed, then continue loop
                        # If 0 size means EOF marker, adjust logic here
                        packets_in_file += 1
                        bytes_in_file += ES_HEADER_SIZE_BYTES
                        self.total_packets_processed += 1
                        self.total_bytes_processed += ES_HEADER_SIZE_BYTES
                        continue  # Skip payload read/write

                    # --- Read Payload ---
                    try:
                        ts_payload = f_in.read(payload_size)
                    except IOError as e:
                        log.error(f"IOError reading payload ({payload_size} bytes) from {os.path.basename(file_path)}: {e}")
                        break  # Cannot continue

                    if len(ts_payload) < payload_size:
                        log.warning(f"Incomplete payload read in {os.path.basename(file_path)} "
                                    f"(expected {payload_size}, got {len(ts_payload)}). Stopping parse for this file.")
                        break  # Truncated file

                    # --- Process Data (Write TS, Optional Analysis) ---
                    self._write_ts_chunk(ts_payload)

                    # Optional Analysis (can be commented out if not needed)
                    # from .utils import convert_pcr_27mhz_to_pcr_ns
                    # pcr_ns = convert_pcr_27mhz_to_pcr_ns(pcr_27mhz)
                    # self.pcr_utc_map[utc_ns] = pcr_ns
                    # if self.last_pcr_utc_ns != -1:
                    #     time_diff_ms = abs(utc_ns - self.last_pcr_utc_ns) / 1e6
                    #     self.pcr_utc_diff.append(time_diff_ms)
                    # self.last_pcr_utc_ns = utc_ns

                    # --- Update Stats ---
                    packets_in_file += 1
                    current_packet_total_bytes = ES_HEADER_SIZE_BYTES + payload_size
                    bytes_in_file += current_packet_total_bytes
                    self.total_packets_processed += 1
                    self.total_bytes_processed += current_packet_total_bytes

            log.info(f"Finished parsing {os.path.basename(file_path)} ({packets_in_file} packets, {bytes_in_file / 1024:.1f} KiB)")
            success = True

        except IOError as e:
            log.error(f"IOError opening/reading file {file_path}: {e}")
            success = False
        except Exception as e:
            log.exception(f"Unexpected error processing file {file_path}")  # Log full traceback
            success = False

        return success

    def process_files(self, file_list: List[str], cleanup_after_processing: bool = False):
        """
        Processes a list of ES file paths sequentially.
        
        Args:
            file_list: List of ES file paths
            cleanup_after_processing: Whether to delete files after processing
        """
        print_section_header("ES Parsing Process")

        if not file_list:
            log.warning("No files provided to the ES parser. Nothing to do.")
            return

        total_files_to_process = len(file_list)
        log.info(f"Starting to process {total_files_to_process} downloaded/local ES files.")

        # Ensure output directory exists and open output file ONCE
        try:
            output_dir = os.path.dirname(self.ts_dump_file_path)
            if output_dir:  # Handle case where output is in current dir
                os.makedirs(output_dir, exist_ok=True)
                
            # Check if we should append to existing file (resume)
            file_mode = "ab" if os.path.exists(self.ts_dump_file_path) and self.resume_state_file else "wb"
            
            # Open in binary append/write mode
            with open(self.ts_dump_file_path, file_mode) as self.output_file_handle:
                if file_mode == "ab":
                    self.output_bytes_written = os.path.getsize(self.ts_dump_file_path)
                    log.info(f"Appending to existing output file ({self.output_bytes_written / (1024*1024):.2f} MiB)")
                
                print_progress(0, total_files_to_process, prefix="Parsing: ", suffix="(0 / {})".format(total_files_to_process))

                for i, file_path in enumerate(file_list):
                    if os.path.exists(file_path):
                        parse_success = self._process_single_es_file(file_path)
                        if parse_success:
                            self.total_files_processed += 1
                            # Optionally remove the temp file after successful processing
                            if cleanup_after_processing:
                                try:
                                    os.remove(file_path)
                                    log.debug(f"Removed processed temp file: {file_path}")
                                except OSError as e:
                                    log.warning(f"Could not remove temp file {file_path}: {e}")
                        else:
                            self.total_files_failed += 1
                            log.warning(f"Keeping failed file for inspection: {file_path}")
                    else:
                        log.warning(f"File listed for processing not found on disk: {file_path}")
                        self.total_files_skipped += 1

                    # Update progress bar
                    processed_count = self.total_files_processed + self.total_files_failed + self.total_files_skipped
                    print_progress(i + 1, total_files_to_process,
                                  prefix="Parsing: ",
                                  suffix=f"({i+1} / {total_files_to_process})")
                    
                    # Save progress state periodically if resume file is specified
                    if self.resume_state_file and i % 10 == 0:  # Save every 10 files
                        from .utils import save_progress_state
                        progress_data = {
                            "total_packets_processed": self.total_packets_processed,
                            "total_bytes_processed": self.total_bytes_processed,
                            "total_files_processed": self.total_files_processed,
                            "output_bytes_written": self.output_bytes_written,
                            "processed_files": file_list[:i+1],
                            "timestamp": time.time()
                        }
                        save_progress_state(self.resume_state_file, progress_data)
                
                # Make sure to flush any remaining data in the buffer
                self._flush_buffer()

            self.output_file_handle = None  # Clear handle after 'with' block

        except IOError as e:
            log.exception(f"FATAL: Could not open or write to output file {self.ts_dump_file_path}")
            # Exit or handle appropriately - cannot continue without output file
            raise
        except Exception as e:
            log.exception("An unexpected error occurred during the file processing loop.")
            # Ensure handle is cleared if error happened before 'with' finished
            self.output_file_handle = None

        # Final Summary
        print_section_header("ES Parsing Summary")
        log.info(f"Total files provided for processing: {total_files_to_process}")
        log.info(f"Files successfully processed: {self.total_files_processed}")
        log.info(f"Files skipped (not found): {self.total_files_skipped}")
        log.info(f"Files failed during parsing: {self.total_files_failed}")
        log.info(f"Total ES packets processed: {self.total_packets_processed}")
        log.info(f"Total ES bytes processed: {self.total_bytes_processed / (1024*1024):.2f} MiB")
        try:
            output_size = os.path.getsize(self.ts_dump_file_path)
            log.info(f"Final output file size: {output_size / (1024*1024):.2f} MiB")
        except FileNotFoundError:
            log.warning("Output file not found after processing.")
        except Exception as e:
            log.warning(f"Could not get size of output file: {e}")

        log.info("ES parsing completed.")
        
        # Save final progress state if resume file is specified
        if self.resume_state_file:
            from .utils import save_progress_state
            progress_data = {
                "total_packets_processed": self.total_packets_processed,
                "total_bytes_processed": self.total_bytes_processed,
                "total_files_processed": self.total_files_processed,
                "output_bytes_written": self.output_bytes_written,
                "completed": True,
                "timestamp": time.time()
            }
            save_progress_state(self.resume_state_file, progress_data)
    
    def resume_from_state(self, state_data: Dict) -> bool:
        """
        Resume parsing from a saved state.
        
        Args:
            state_data: Dictionary containing saved state
            
        Returns:
            True if state was successfully loaded, False otherwise
        """
        if not state_data:
            return False
            
        try:
            # Only resume if the output file exists
            if not os.path.exists(self.ts_dump_file_path):
                log.warning(f"Cannot resume parsing: output file {self.ts_dump_file_path} not found")
                return False
                
            self.total_packets_processed = state_data.get("total_packets_processed", 0)
            self.total_bytes_processed = state_data.get("total_bytes_processed", 0)
            self.total_files_processed = state_data.get("total_files_processed", 0)
            self.output_bytes_written = state_data.get("output_bytes_written", 0)
            
            # Verify output file size matches what we expect
            actual_size = os.path.getsize(self.ts_dump_file_path)
            if actual_size != self.output_bytes_written:
                log.warning(f"Output file size mismatch: expected {self.output_bytes_written}, got {actual_size}")
                # We'll continue anyway, but this might indicate corruption
            
            log.info(f"Resumed parsing state with {self.total_files_processed} files already processed")
            return True
        except Exception as e:
            log.error(f"Failed to resume parsing state: {e}")
            return False