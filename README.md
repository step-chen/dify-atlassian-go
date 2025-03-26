# Dify Confluence Sync Tool

This tool synchronizes Confluence documents and attachments to Dify knowledge bases.

## Features

- **Support Dify 1.0.3 API, Confluence REST API v1**
- Synchronize Confluence pages and attachments to Dify
- Support for multiple Confluence spaces
- Concurrent processing with configurable worker pool
- Automatic retry for failed operations
- Metadata synchronization including:
  - Title
  - Content
  - Author
  - Last modified date
  - Keywords
  - Description
  - URL

## Configuration

Create a `config.yaml` file based on the example configuration:

```yaml
dify:
  base_url: "https://your-dify-instance.com"
  api_key: "your-api-key"
  datasets:
    SPACE_KEY_1: "dataset-id-1"
    SPACE_KEY_2: "dataset-id-2"
  rag_setting:
    indexing_technique: "high_quality"
    doc_form: "text"

confluence:
  base_url: "https://your-confluence-instance.com"
  api_key: "your-confluence-api-key"
  space_keys: ["SPACE_KEY_1", "SPACE_KEY_2"]

concurrency:
  workers: 5
  queue_size: 100
  batch_pool_size: 50
  indexing_timeout: 2
  max_retries: 3
```
**See cmd/dify-confluence/config.yaml.example for detail**

## Usage

1. Build the tool:
   ```bash
   go build -o dify-confluence ./cmd/dify-confluence
   ```

2. Run the sync:
   ```bash
   ./dify-confluence
   ```

## Requirements

- Go 1.20+
- Confluence API access
- Dify API access

## Development

### Build
```bash
go build -o dify-confluence ./cmd/dify-confluence
```

### Run
```bash
./dify-confluence
```

### Test
```bash
go test ./...
```

## Project Structure

```
.
├── cmd
│   └── dify-confluence        # Main application
├── internal
│   ├── concurrency            # Concurrency utilities
│   ├── config                 # Configuration management
│   ├── confluence             # Confluence API client
│   ├── dify                   # Dify API client
│   └── utils                  # Utility functions
└── sample                     # Sample files
```

## License

MIT License
