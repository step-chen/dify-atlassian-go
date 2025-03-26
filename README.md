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
