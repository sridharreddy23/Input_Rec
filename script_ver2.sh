#!/bin/bash

# Configuration
CONCURRENCY=5  # Number of parallel downloads
S3_BUCKET="amagilive-aws-use1-cprod-cp305"
S3_BASE_PATH="s3://$S3_BUCKET/dooya.cloudport.amagi.tv/dooya/Recordings/02305d30-e280-11ef-9da0-0315db859e71/live-stream/livestream/Conditioned/08022025/"
AWS_REGION="us-east-1"
START_EPOCH=1738972830
END_EPOCH=1738973916

# Add a 60-second buffer
START_EPOCH=$((START_EPOCH - 60))
END_EPOCH=$((END_EPOCH + 60))

# Create output directory
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
OUTPUT_DIR="download_${TIMESTAMP}"
mkdir -p "$OUTPUT_DIR"
cd "$OUTPUT_DIR" || exit 1

# Progress tracking file
PROGRESS_FILE=".progress"
touch "$PROGRESS_FILE"

# Function to display progress bar
progress_bar() {
    local total=$1
    local current=$2
    local width=50
    local percent=$((current * 100 / total))
    local completed=$((current * width / total))
    printf "\r[%-${width}s] %d%%" "$(printf '#%.0s' $(seq 1 $completed))" "$percent"
}

# Enhanced download function
download_files_parallel() {
    local FILE_TYPE=$1
    local EXTENSION=$2
    local -n FILE_LIST=$3

    echo "🔍 Searching for $FILE_TYPE files..."
    DOWNLOAD_LIST=()

    # Fetch all files from S3
    echo "🔄 Fetching file list from S3..."
    ALL_FILES=$(aws s3 ls --recursive "$S3_BASE_PATH" --region "$AWS_REGION" | awk '{print $4}')
    if [ -z "$ALL_FILES" ]; then
        echo "❌ No files found in S3 path: $S3_BASE_PATH"
        return
    fi

    # Filter files based on epoch and extension
    for file in $ALL_FILES; do
        FILENAME=$(basename "$file")
        EPOCH_START_PART=$(echo "$FILENAME" | grep -oE '^[0-9]+\.[0-9]+' | cut -d'.' -f1)
        EXT_LOWER=$(echo "$FILENAME" | awk -F. '{print tolower($NF)}')
        
        if [[ ("$file" == *"/00/video/"* && "$EXT_LOWER" == "$EXTENSION") || \
             ("$file" == *"/00/audio/"* && "$EXT_LOWER" == "$EXTENSION") ]]; then
            if [[ "$EPOCH_START_PART" =~ ^[0-9]+$ ]] && \
               [ "$EPOCH_START_PART" -ge "$START_EPOCH" ] && \
               [ "$EPOCH_START_PART" -le "$END_EPOCH" ]; then
                DOWNLOAD_LIST+=("s3://$S3_BUCKET/$file")
            fi
        fi
    done

    TOTAL_FILES=${#DOWNLOAD_LIST[@]}
    if [ "$TOTAL_FILES" -eq 0 ]; then
        echo "⚠️ No matching $FILE_TYPE files found in the specified time range."
        return
    fi

    echo "📥 Found $TOTAL_FILES $FILE_TYPE files to download."
    > "$PROGRESS_FILE"  # Reset progress file

    # Download files in parallel
    printf '%s\n' "${DOWNLOAD_LIST[@]}" | \
    xargs -P "$CONCURRENCY" -I {} bash -c '
        file={}
        if aws s3 cp "$file" . --region "'"$AWS_REGION"'" >/dev/null 2>&1; then
            echo "✅ Success: $(basename "$file")"
            echo "1" >> "'"$PROGRESS_FILE"'"
        else
            echo "❌ Failed: $(basename "$file")" >&2
        fi
    '

    # Update progress bar
    CURRENT_FILE=0
    while [ "$CURRENT_FILE" -lt "$TOTAL_FILES" ]; do
        CURRENT_FILE=$(wc -l < "$PROGRESS_FILE" 2>/dev/null)
        progress_bar "$TOTAL_FILES" "$CURRENT_FILE"
        sleep 0.5
    done
    echo

    # Add downloaded files to list
    for file in "${DOWNLOAD_LIST[@]}"; do
        FILE_LIST+=("$(basename "$file")")
    done
}

# Main script execution
echo "📅 Time Range: $(date -d @"$START_EPOCH" "+%Y-%m-%d %H:%M:%S") → $(date -d @"$END_EPOCH" "+%Y-%m-%d %H:%M:%S")"

# Download video and audio files
VIDEO_FILES=()
AUDIO_FILES=()
download_files_parallel "video" "h264" VIDEO_FILES
download_files_parallel "audio" "wav" AUDIO_FILES

# Process downloaded files
if [ ${#VIDEO_FILES[@]} -gt 0 ] && [ ${#AUDIO_FILES[@]} -gt 0 ]; then
    echo "🛠 Processing video and audio files"

    # Convert .h264 video to .ts format for concatenation
    VIDEO_TS_LIST=()
    for video in "${VIDEO_FILES[@]}"; do
        OUTPUT_TS="${video%.*}.ts"
        ffmpeg -i "$video" -c copy -bsf:v h264_mp4toannexb "$OUTPUT_TS"
        VIDEO_TS_LIST+=("$OUTPUT_TS")
    done

    # Concatenate video .ts files
    VIDEO_LIST_FILE="video_list.txt"
    for ts in "${VIDEO_TS_LIST[@]}"; do
        echo "file '$ts'" >> "$VIDEO_LIST_FILE"
    done
    FINAL_VIDEO_TS="final_video.ts"
    ffmpeg -f concat -safe 0 -i "$VIDEO_LIST_FILE" -c copy "$FINAL_VIDEO_TS"

    # Concatenate audio .wav files
    AUDIO_LIST_FILE="audio_list.txt"
    for audio in "${AUDIO_FILES[@]}"; do
        echo "file '$audio'" >> "$AUDIO_LIST_FILE"
    done
    FINAL_AUDIO_WAV="final_audio.wav"
    ffmpeg -f concat -safe 0 -i "$AUDIO_LIST_FILE" -c copy "$FINAL_AUDIO_WAV"

    # Merge video and audio into final MP4
    FINAL_OUTPUT_MP4="final_output.mp4"
    if [ -f "$FINAL_VIDEO_TS" ] && [ -f "$FINAL_AUDIO_WAV" ]; then
        echo "🎥 Merging video and audio into final MP4..."
        ffmpeg -i "$FINAL_VIDEO_TS" -i "$FINAL_AUDIO_WAV" -c:v copy -c:a aac -strict experimental "$FINAL_OUTPUT_MP4"
        
        if [ $? -eq 0 ]; then
            echo "✅ Final MP4 created: $FINAL_OUTPUT_MP4"

            # Move intermediate files to trash
            echo "🗑 Flushing temporary chunks to trash..."
            gio trash "${VIDEO_TS_LIST[@]}" "$FINAL_VIDEO_TS" "$FINAL_AUDIO_WAV" "${VIDEO_FILES[@]}" "${AUDIO_FILES[@]}" "$VIDEO_LIST_FILE" "$AUDIO_LIST_FILE"
        else
            echo "❌ Failed to create final MP4"
        fi
    else
        echo "⚠️ Video or Audio files are missing. Muxing skipped."
    fi
else
    echo "⚠️ No matching audio/video files found for merging."
fi

# Return to the original directory
cd - || exit
echo "✅ All files processed in $OUTPUT_DIR"

