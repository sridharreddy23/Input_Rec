#!/bin/bash
# Convenience script to run the ES Downloader & Parser

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Run the main script with system Python (no virtualenv needed)
python3 -m src.main "$@"

