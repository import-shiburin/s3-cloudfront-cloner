# S3 CloudFront Cloner

> Built with [Claude Code](https://claude.ai/code) in ~10 minutes

A CLI tool that clones S3 objects via CloudFront using signed cookies. Download objects to local storage or upload to another S3 bucket with optional metadata preservation and verification.

## Installation

```bash
go install github.com/yourusername/s3-cloudfront-cloner@latest
```

Or build from source:

```bash
git clone https://github.com/yourusername/s3-cloudfront-cloner.git
cd s3-cloudfront-cloner
go build -o s3-cloudfront-cloner .
```

## Usage

### List objects

```bash
s3-cloudfront-cloner list --bucket my-bucket --prefix images/
```

### Clone to local filesystem

```bash
s3-cloudfront-cloner clone \
  --source-bucket my-bucket \
  --prefix images/ \
  --cloudfront-domain d123.cloudfront.net \
  --private-key /path/to/private_key.pem \
  --key-pair-id APKAXXXXXXXX \
  --dest-local ./downloads/
```

### Clone to another S3 bucket

```bash
s3-cloudfront-cloner clone \
  --source-file objects.json \
  --source-bucket original-bucket \
  --cloudfront-domain d123.cloudfront.net \
  --private-key /path/to/private_key.pem \
  --key-pair-id APKAXXXXXXXX \
  --dest-bucket destination-bucket \
  --dest-prefix backup/
```

## CLI Reference

### Global Flags

| Flag | Short | Description |
|------|-------|-------------|
| `--config` | | Config file (default: `$HOME/.s3-cloudfront-cloner.yaml`) |
| `--verbose` | `-v` | Enable verbose output |

### `list` Command

| Flag | Description |
|------|-------------|
| `--bucket` | S3 bucket name (required) |
| `--prefix` | Prefix to filter objects |
| `--output`, `-o` | Output file (default: stdout) |

### `clone` Command

#### Source

| Flag | Description |
|------|-------------|
| `--source-bucket` | Source S3 bucket name |
| `--source-file` | JSON file with object list (AWS CLI `list-objects-v2` format) |
| `--prefix` | Prefix to filter objects |
| `--strip-prefix` | Leading prefix to strip from each source key before joining with `--dest-prefix`. E.g. with `--strip-prefix bbb/ccc/`, source key `bbb/ccc/ddd/file` lands at `<dest-prefix>ddd/file` instead of `<dest-prefix>bbb/ccc/ddd/file`. |

#### CloudFront

| Flag | Description |
|------|-------------|
| `--cloudfront-domain` | CloudFront distribution domain (required) |
| `--private-key` | Path to CloudFront private key PEM file (required) |
| `--key-pair-id` | CloudFront key pair ID (required) |
| `--cookie-expiry` | Cookie expiration duration (default: `24h`) |

#### Destination

| Flag | Description |
|------|-------------|
| `--dest-local` | Local directory for downloads |
| `--dest-bucket` | Destination S3 bucket |
| `--dest-prefix` | Prefix for destination objects |

#### Options

| Flag | Description |
|------|-------------|
| `--concurrency` | Number of parallel downloads (default: `10`) |
| `--verify` | Verify ETag/checksum after download |
| `--preserve-metadata` | Preserve object metadata (Content-Type, Cache-Control, etc.) |
| `--dry-run` | List what would be cloned without cloning |
| `--skip-existing` | Skip objects whose destination already matches source size + checksum. Requires `--source-bucket` to fetch source checksums. For local destinations, the existing file is hashed (one pass over disk). For S3 destinations, a HeadObject is issued and any common S3 native checksum is compared (ETag as a fallback). |
| `--range-threshold` | File size (bytes) above which chunked Range downloads are used (default: `5 GiB`). Applies to both local and S3 destinations. |
| `--chunk-size` | Chunk size (bytes) for Range downloads (default: `256 MiB`). For S3 destinations each chunk maps to one multipart part, so this must be between 5 MiB and 5 GiB. |

## AWS Credentials

Uses the standard AWS SDK credential chain:
- Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
- Shared credentials file (`~/.aws/credentials`)
- IAM role (EC2/ECS/Lambda)

## License

MIT
