# Data Flow

```
┌─────────────────┐
│                 │
│  User Input     │
│  (CLI Args)     │
│                 │
└────────┬────────┘
         │
         ▼
┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │
│  Config Manager │────▶│  Config File    │
│                 │     │  (JSON)         │
└────────┬────────┘     └─────────────────┘
         │
         ▼
┌─────────────────┐
│                 │
│  S3 Reader      │
│                 │
└────────┬────────┘
         │
         ▼
┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │
│  AWS S3 API     │────▶│  ES Files       │
│                 │     │  (Temp Dir)     │
└─────────────────┘     └────────┬────────┘
                                 │
                                 ▼
                        ┌─────────────────┐
                        │                 │
                        │  ES Parser      │
                        │                 │
                        └────────┬────────┘
                                 │
                                 ▼
                        ┌─────────────────┐
                        │                 │
                        │  Output File    │
                        │  (TS)           │
                        │                 │
                        └─────────────────┘
```

## Process Flow

1. **User Input**: Command line arguments specify config file and output path
2. **Config Loading**: Configuration manager loads and validates the JSON config
3. **File Identification**: S3 Reader identifies files needed based on time range
4. **Parallel Download**: Files are downloaded in parallel from S3 to temp directory
5. **Sequential Parsing**: ES Parser processes files in chronological order
6. **TS Extraction**: For each ES file, TS payload is extracted and written to output
7. **Progress Tracking**: Progress is displayed throughout the process
8. **Cleanup**: Temporary files are removed after successful processing