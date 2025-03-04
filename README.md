# tap-sentry

This is a [Singer](https://singer.io) tap that extracts data from the [Sentry API](https://docs.sentry.io/api/) and produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

## Installation

```bash
pip install tap-sentry
```

## Configuration

This tap requires a `config.json` file which specifies:

```json
{
  "api_token": "YOUR_SENTRY_API_TOKEN",
  "start_date": "2020-01-01T00:00:00Z"  // date to start syncing from
}
```

To generate a Sentry API token, visit your Sentry organization settings and create a new API token with the appropriate permissions.

## Usage

```bash
# Discovery mode
tap-sentry --config config.json --discover > catalog.json

# Sync mode with catalog
tap-sentry --config config.json --catalog catalog.json
```

## Supported Streams

| Stream | Key Properties | Schema |
| ------ | ------------- | ------ |
| Projects | id | Information about Sentry projects |
| Issues | id | Error issues tracked in Sentry |
| Events | eventID | Individual error events |
| Teams | id | Team information |
| Users | id | User information |
| Releases | version | Release information |

## Replication

This tap uses [Sentry's REST API](https://docs.sentry.io/api/) and tracks bookmark information to enable incremental sync for supported streams:

- Issues: Incremental sync based on start date
- Events: Incremental sync based on start date
- Releases: Incremental sync based on start date
- Projects: Full table sync
- Teams: Full table sync
- Users: Full table sync

## State

This tap maintains state in the format:

```json
{
  "bookmarks": {
    "issues": {
      "start": "2023-01-01T00:00:00Z" 
    },
    "events": {
      "start": "2023-01-01T00:00:00Z"
    },
    "releases": {
      "start": "2023-01-01T00:00:00Z"
    }
  }
}
```

## Meltano Integration

You can easily use this tap with Meltano by adding the following to your `meltano.yml`:

```yaml
plugins:
  extractors:
    - name: tap-sentry
      namespace: tap_sentry
      pip_url: tap-sentry
      executable: tap-sentry
      capabilities:
        - catalog
        - discover
        - state
      settings:
        - name: api_token
          kind: password
          label: Sentry API Token
          description: Bearer token for Sentry API authentication
          protected: true
        - name: start_date
          kind: date_iso8601
          label: Start Date
          description: The earliest record date to sync
          value: '2020-01-01T00:00:00Z'
      select:
        - projects.*
        - issues.*
        - events.*
        - teams.*
        - users.*
        - releases.*
```

## Limitations

- The tap uses the Sentry organization name "split-software" hardcoded in the API endpoints. For different organizations, this would need to be updated.
- API rate limits from Sentry may apply.

## Development

```bash
# Install in development mode
pip install -e .

# Run tests
python -m unittest discover tests
```

### Testing

This tap has unit tests in the `tests/` directory. To run them:

```bash
python -m unittest discover tests
```

## License

MIT
```

This README.md provides comprehensive documentation on installation, configuration, usage, supported streams, data replication, state management, Meltano integration, limitations, and development instructions for the tap-sentry Singer connector.
