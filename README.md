# Federal Register Scraper

A Python tool that scrapes notices from the U.S. Federal Register API and stores them in a MinIO object store. The scraper focuses on environmental and natural resource agencies and maintains an index of abstracts for quick reference.

## Features

- Scrapes notices from specified Federal Register agencies
- Stores PDFs in MinIO with organized folder structure
- Maintains a searchable index of abstracts
- Incremental updates by default (skips already processed notices)
- Optional full refresh mode with `--all` flag
- Detailed logging with timing information
- Configurable via YAML file

## Prerequisites

- Python 3.7+
- Access to a MinIO instance
- Federal Register API access (no authentication required)

## Installation

1. Clone the repository:
```bash
git clone https://sij.ai/sij/fedreg-scraper
cd fedreg-scraper
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

1. Copy the example configuration file:
```bash
cp example-config.yaml config.yaml
```

2. Edit `config.yaml` with your settings:
```yaml
# MinIO connection settings
minio:
  endpoint: "your-minio-endpoint"
  access_key: "your-access-key"
  secret_key: "your-secret-key"
  region: "us-east-1"
  secure: true  # Use HTTPS

# Storage settings
bucket_name: "your-bucket-name"
parent_folder: "federal-register"

# Agencies to scrape (use agency short names or full names)
agencies:
  - "APHIS"
  - "BLM"
  - "EPA"
  # Add more agencies as needed
```

### MinIO Setup

1. Create a new bucket in your MinIO instance
2. Create an access policy for the scraper (example below)
3. Create access credentials and note the access/secret keys

Example MinIO access policy:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::your-bucket-name/*",
                "arn:aws:s3:::your-bucket-name"
            ]
        }
    ]
}
```

## Usage

### Basic Usage

Run the scraper in incremental mode (stops when it encounters existing documents):
```bash
python frscraper.py
```

### Full Refresh

Run the scraper and process all documents, even if they already exist:
```bash
python frscraper.py --all
```

## Storage Structure

The scraper organizes documents in MinIO as follows:

```
bucket_name/
└── federal-register/
    ├── abstracts.json
    ├── APHIS/
    │   ├── 2024-00123 - Notice Title.pdf
    │   └── ...
    ├── EPA/
    │   ├── 2024-00456 - Another Notice.pdf
    │   └── ...
    └── ...
```

- Each agency gets its own folder
- PDFs are named with their Federal Register document number and truncated title
- `abstracts.json` contains metadata for all documents

## Performance Considerations

- Initial load of abstracts.json can take 2-3 minutes for large collections
- Saving updated abstracts typically takes 20-30 seconds
- Each agency check takes less than 1 second
- Use `--all` flag judiciously as it will check every document

## Logging

The scraper provides detailed logging with timestamps for:
- Script startup and configuration
- Agency processing progress
- Document downloads and uploads
- Performance metrics
- Error conditions

Example log output:
```
[2025-02-20 06:32:31.042] INFO: Starting Federal Register document scraper
[2025-02-20 06:32:31.042] INFO: Running with --all=False
[2025-02-20 06:32:31.043] INFO: Loading config from config.yaml
...
```

## Error Handling

The scraper handles several common error conditions:
- Invalid agency names in config
- Network connectivity issues
- MinIO access problems
- Missing PDFs on Federal Register

Check the logs for detailed error messages if issues occur.

## Contributing

Contributions are welcome! Please submit pull requests with:
- Clear description of changes
- Updated documentation
- Additional test coverage if applicable