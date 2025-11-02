import boto3
import json
import os
import sys
import subprocess
import re
from datetime import datetime, timezone, timedelta

# Define IST timezone offset (UTC+5:30)
IST_OFFSET = timedelta(hours=5, minutes=30)

# Load config
try:
    with open("config.json") as f:
        config = json.load(f)
except Exception as e:
    print(f"[ERROR] Failed to load config.json: {e}")
    sys.exit(1)

start_utc = config["start_utc"]
end_utc = config["end_utc"]
bucket = config["aws_conf"]["s3_bucket"]
region = config["aws_conf"]["aws_region"]
# Optimize prefix for September 26, 2025
prefix = "kcdok_001/abscbn-kcdok-001-dd/2025/09/26/"

print("========== CONFIG ==========")
print(f"Bucket : {bucket}")
print(f"Prefix : {prefix}")
print(f"Region : {region}")
print(f"Start Time : {start_utc} ({datetime.fromtimestamp(start_utc, tz=timezone.utc)} UTC / "
      f"{datetime.fromtimestamp(start_utc, tz=timezone.utc) + IST_OFFSET} IST)")
print(f"End Time : {end_utc} ({datetime.fromtimestamp(end_utc, tz=timezone.utc)} UTC / "
      f"{datetime.fromtimestamp(end_utc, tz=timezone.utc) + IST_OFFSET} IST)")
print("============================")

# Initialize S3 client
try:
    s3 = boto3.client("s3", region_name=region)
except Exception as e:
    print(f"[ERROR] Could not initialize S3 client: {e}")
    sys.exit(1)

# Create temp dir for chunks
os.makedirs("chunks", exist_ok=True)

# List objects using aws s3 ls
print("[INFO] Scanning S3 objects with aws s3 ls…")
try:
    # Construct the S3 URI
    s3_uri = f"s3://{bucket}/{prefix}"
    # Run aws s3 ls command
    cmd = ["aws", "s3", "ls", s3_uri, "--recursive"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"[ERROR] Failed to list objects with aws s3 ls: {result.stderr}")
        sys.exit(1)
    
    # Parse output
    ts_files = []
    lines = result.stdout.splitlines()
    for line in lines:
        # Example line: "2025-09-26 08:29:05      12345 kcdok_001/abscbn-kcdok-001-dd/2025/09/26/02/1758894545_5005.ts"
        parts = line.strip().split(maxsplit=3)
        if len(parts) != 4:
            continue  # Skip malformed lines
        date_str, time_str, size, key = parts
        if not key.endswith(".ts"):
            continue  # Skip non-.ts files
        # Extract epoch timestamp from file name
        try:
            # Match epoch timestamp (e.g., 1758894545 from 1758894545_5005.ts)
            match = re.search(r"(\d{10})_", os.path.basename(key))
            if not match:
                print(f"[WARN] Skipping {key}: No epoch timestamp in file name")
                continue
            file_epoch = int(match.group(1))
            # Filter based on file name epoch timestamp
            if start_utc <= file_epoch <= end_utc:
                # Get LastModified for logging
                last_modified_utc = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")
                last_modified_utc = last_modified_utc.replace(tzinfo=timezone.utc)
                last_modified_ist = last_modified_utc + IST_OFFSET
                ts_files.append((file_epoch, key))
                print(f"[DEBUG] Included {key}: FileEpoch={file_epoch} ({datetime.fromtimestamp(file_epoch, tz=timezone.utc)} UTC / "
                      f"{datetime.fromtimestamp(file_epoch, tz=timezone.utc) + IST_OFFSET} IST), "
                      f"LastModified={last_modified_utc} UTC / {last_modified_ist} IST")
            else:
                print(f"[DEBUG] Skipped {key}: FileEpoch={file_epoch} ({datetime.fromtimestamp(file_epoch, tz=timezone.utc)} UTC / "
                      f"{datetime.fromtimestamp(file_epoch, tz=timezone.utc) + IST_OFFSET} IST) "
                      f"(outside {start_utc}-{end_utc})")
        except (ValueError, re.error) as e:
            print(f"[WARN] Skipping {key}: Invalid epoch timestamp in file name: {e}")
            continue
except subprocess.CalledProcessError as e:
    print(f"[ERROR] Failed to execute aws s3 ls: {e}")
    sys.exit(1)

# Sort chunks by file epoch timestamp
ts_files.sort()
if not ts_files:
    print("[WARN] No TS chunks found in given time range.")
    sys.exit(0)

print(f"[INFO] Found {len(ts_files)} chunks in range.\n")

# Download chunks
local_files = []
for idx, (file_epoch, key) in enumerate(ts_files, start=1):
    utc_time = datetime.fromtimestamp(file_epoch, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    ist_time = (datetime.fromtimestamp(file_epoch, tz=timezone.utc) + IST_OFFSET).strftime("%Y-%m-%d %H:%M:%S")
    local_path = os.path.join("chunks", os.path.basename(key))
    try:
        print(f"[INFO] Downloading {idx}/{len(ts_files)}: {key} "
              f"(FileEpoch={file_epoch} ({utc_time} UTC / {ist_time} IST))")
        s3.download_file(bucket, key, local_path)
        local_files.append(local_path)
    except ClientError as e:
        print(f"[ERROR] Failed to download {key}: {e}")
        continue  # Continue with next file

# Concatenate into single TS file
output_file = "output.ts"
try:
    with open(output_file, "wb") as outfile:
        for fname in local_files:
            with open(fname, "rb") as infile:
                outfile.write(infile.read())
    print(f"\n[SUCCESS] Concatenated {len(local_files)} chunks into {output_file}")
except Exception as e:
    print(f"[ERROR] Failed to concatenate files: {e}")
    sys.exit(1)
