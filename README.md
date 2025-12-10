# MongoDB Inconsistency Checker

A Go program that analyzes CSV log files to identify and verify document consistency between source and destination MongoDB instances.

## Overview

This tool parses log files for "Isolated retry still failed" errors, extracts namespace and document `_id` information, and verifies whether the documents at the source and destination databases match.

## Prerequisites

- Go 1.16 or higher
- Access to source and destination MongoDB instances
- CSV log file in the expected format

## Installation

```bash
# Install dependencies
go mod tidy

# Build the binary
go build -o error_checker .
```

## Usage

```bash
./error_checker -logfile <path_to_csv> -source <source_mongo_uri> -dest <dest_mongo_uri>
```

### Arguments

- `-logfile`: Path to the CSV log file
- `-source`: Source MongoDB connection string (e.g., `mongodb://localhost:27017`)
- `-dest`: Destination MongoDB connection string

### Example

```bash
./error_checker \
  -logfile sample_input/test.csv \
  -source "mongodb://source-host:27017/mydb" \
  -dest "mongodb://dest-host:27017/mydb"
```

## Sample Output

```
=== Analysis Report ===

Namespace: testshard.col2
  Total Checks: 2
  Matches: 1
  Mismatches: 0
  Missing in Source: 0
  Missing in Dest: 1
  Errors: 0

=== Discrepancies ===
[testshard.col2] ID: ObjectID("693885e2f227ce8067db8d34") | Status: MissingInDest | Details: 
```

## Log File Format

The tool expects a CSV file with the following columns:
- Date
- Pod Name
- @processKey
- Message

The Message column should contain log entries with "Isolated retry still failed" errors that include:
- `collection: <namespace>` - The database.collection name
- `id=""{\""$oid\"":\""<object_id>\""}""` - The document ObjectID in Extended JSON format

Example log entry:
```csv
2025-10-15T17:32:48.521Z,dsync,col2,"Dec  9 12:26:13.446 ERR Isolated retry still failed retryErr=""..."" err=""..."" index=0 id=""{\""$oid\"":\""693885e2f227ce8067db8d33\""}"" key=1765311970851576000"
```

## Statistics Explained

- **Total Checks**: Number of document IDs processed
- **Matches**: Documents that are identical in both databases (or missing from both)
- **Mismatches**: Documents that exist in both databases but have different content
- **Missing in Source**: Documents that exist in destination but not in source
- **Missing in Dest**: Documents that exist in source but not in destination
- **Errors**: Failed queries due to connection issues or other errors

## License

This project is provided as-is for internal use.
