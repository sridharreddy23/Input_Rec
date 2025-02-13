#!/bin/bash

# Configuration
CONCURRENCY=5  # Number of parallel downloads
S3_BUCKET="elic20221103184258289600000005"
S3_BASE_PATH="s3://$S3_BUCKET/Recordings/processed/WPXI_PITTSBURG/amagistudio/RawTs/01022025/"
AWS_REGION="us-east-1"
START_EPOCH=1738371480
END_EPOCH=1738371600
PROCESS_AUDIO=false  # Set to true to process audio, false to use .es files directly

# Apply buffer
START_EPOCH=$((START_EPOCH - 60))
END_EPOCH=$((END_EPOCH + 60))

# Create output directory
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
OUTPUT_DIR="download_${TIMESTAMP}"
mkdir -p "$OUTPUT_DIR"
cd "$OUTPUT_DIR" || exit 1

# Fetch file list from all subdirectories
ALL_FILES=$(aws s3 ls --recursive "$S3_BASE_PATH" --region "$AWS_REGION" | awk '{print $4}')
if [ -z "$ALL_FILES" ]; then
    echo "❌ No files found in S3 path: $S3_BASE_PATH"
    exit 1
fi

# Find closest matching files
DOWNLOAD_LIST=()
for file in $ALL_FILES; do
    FILENAME=$(basename "$file")
    EXTENSION="${FILENAME##*.}"
    EPOCH_START=$(echo "$FILENAME" | grep -oE '^[0-9]+')
    if [[ "$EPOCH_START" =~ ^[0-9]+$ ]] && \
       [ "$EPOCH_START" -ge "$START_EPOCH" ] && \
       [ "$EPOCH_START" -le "$END_EPOCH" ]; then
        DOWNLOAD_LIST+=("s3://$S3_BUCKET/$file")
    fi
done

# Download files
TOTAL_FILES=${#DOWNLOAD_LIST[@]}
if [ "$TOTAL_FILES" -eq 0 ]; then
    echo "⚠️ No matching files found in the specified time range."
    exit 1
fi

echo "📥 Found $TOTAL_FILES files to download."
> ".progress"
printf '%s\n' "${DOWNLOAD_LIST[@]}" | xargs -P "$CONCURRENCY" -I {} aws s3 cp "{}" . --region "$AWS_REGION"

# Convert .es files to .ts
echo "🔄 Converting .es files to .ts..."
VIDEO_TS_LIST=()
for f in *.es; do
    OUTPUT_TS="${f%.*}.ts"
    if ffmpeg -i "$f" -c copy -bsf:v h264_mp4toannexb "$OUTPUT_TS"; then
        VIDEO_TS_LIST+=("$OUTPUT_TS")
    else
        echo "❌ Failed to convert $f"
    fi
done

# Create a list file for concatenation
VIDEO_LIST_FILE="video_list.txt"
> "$VIDEO_LIST_FILE"
for ts in "${VIDEO_TS_LIST[@]}"; do
    echo "file '$PWD/$ts'" >> "$VIDEO_LIST_FILE"
done

# Concatenate .ts files into final .ts file
FINAL_VIDEO_TS="final_video.ts"
if ffmpeg -f concat -safe 0 -i "$VIDEO_LIST_FILE" -c copy "$FINAL_VIDEO_TS"; then
    echo "✅ Concatenation successful."
else
    echo "❌ Failed to concatenate video files."
    exit 1
fi

# Process audio if needed
if $PROCESS_AUDIO; then
    echo "🔊 Processing audio files..."
    AUDIO_LIST_FILE="audio_list.txt"
    > "$AUDIO_LIST_FILE"
    for audio in *.wav; do
        echo "file '$PWD/$audio'" >> "$AUDIO_LIST_FILE"
    done
    FINAL_AUDIO_WAV="final_audio.wav"
    if ffmpeg -f concat -safe 0 -i "$AUDIO_LIST_FILE" -c copy "$FINAL_AUDIO_WAV"; then
        echo "✅ Audio concatenation successful."
    else
        echo "❌ Failed to concatenate audio files."
        exit 1
    fi

    # Merge video and audio into final MP4
    FINAL_OUTPUT="final_output.mp4"
    if ffmpeg -i "$FINAL_VIDEO_TS" -i "$FINAL_AUDIO_WAV" -c:v copy -c:a aac -strict experimental "$FINAL_OUTPUT"; then
        echo "✅ Final MP4 created: $FINAL_OUTPUT"
    else
        echo "❌ Failed to create MP4."
        exit 1
    fi
else
    # Convert video-only .ts to .mp4
    FINAL_OUTPUT="final_output.mp4"
    if ffmpeg -i "$FINAL_VIDEO_TS" -c:v copy "$FINAL_OUTPUT"; then
        echo "✅ Final MP4 (video-only) created: $FINAL_OUTPUT"
    else
        echo "❌ Failed to create MP4 from video."
        exit 1
    fi
fi

# Clean up intermediate files
echo "🗑 Cleaning up temporary files..."
rm -f "$VIDEO_LIST_FILE" "$FINAL_VIDEO_TS" "$AUDIO_LIST_FILE" "$FINAL_AUDIO_WAV"

# Remove all .es and .ts files EXCEPT the final MP4
find . -type f \( -name "*.es" -o -name "*.ts" \) ! -name "final_output.mp4" -exec rm -f {} +

echo "✅ All processing complete!"
cd - || exit

