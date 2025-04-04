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
  "start_date": "2020-01-01T00:00:00Z",  // date to start syncing from
  "base_url": "https://sentry.io/api/0/",  // optional, default shown
  "organization": "your-organization-slug",  // optional, defaults to "split-software"
  "rate_limit": 10,  // optional, default is 10 requests per second
  "sample_fraction": 0.1,  // optional, sample 10% of events
  "max_events_per_project": 1000,  // optional, limit to 1000 events per project
  "min_events_per_issue": 5,  // optional, ensure at least 5 events per issue
  "max_events_per_issue": 100  // optional, limit to 100 events per issue
}
```

To generate a Sentry API token, visit your Sentry organization settings and create a new API token with the appropriate permissions.

### Configuration Options

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| api_token | Yes | | Your Sentry API token |
| start_date | Yes | | Date to start syncing data from (ISO-8601 format) |
| base_url | No | https://sentry.io/api/0/ | Your Sentry instance URL (for self-hosted or regional instances) |
| organization | No | split-software | Your Sentry organization slug |
| rate_limit | No | 10 | Maximum number of API requests per second |
| sample_fraction | No | None | Fraction of events to sample (0.0-1.0) |
| max_events_per_project | No | None | Maximum number of events to fetch per project |
| min_events_per_issue | No | 5 | Minimum number of events to sample per issue |
| max_events_per_issue | No | 100 | Maximum number of events to sample per issue |
| fetch_event_details | No | false | Whether to fetch detailed event data for each event |

## Usage

```bash
# Discovery mode
tap-sentry --config config.json --discover > catalog.json

# Sync mode with catalog
tap-sentry --config config.json --catalog catalog.json

# With debug logging
tap-sentry --config config.json --catalog catalog.json -v
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
| Project Detail | id | Detailed project information including settings, statistics, and configurations |
| Release | id | Release information including versions, commits, and deployments |

## Stream Details

### project_detail

The project_detail stream provides detailed information about each Sentry project. Unlike the basic projects stream, this includes comprehensive details about project settings, configurations, and statistics.

**Note**: This stream is processed in a non-async manner due to the detailed nature of the API calls.

### release

The release stream provides information about releases in your Sentry projects, including:
- Version information
- Commit data
- Deployment status
- Project associations
- Release statistics

**Note**: This stream is processed in a non-async manner.

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

## Debugging

This tap supports debug logging which shows detailed information about API requests and responses. To enable debug logging, use the `-v` flag or set the environment variable `SINGER_LOG_LEVEL=DEBUG`.

The debug logs include:
- Request URLs with parameters
- Response status codes
- Pagination details
- Error information

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
        - name: base_url
          kind: string
          label: Sentry API Base URL
          description: Base URL for Sentry API (for self-hosted or regional instances)
          value: 'https://sentry.io/api/0/'
        - name: organization
          kind: string
          label: Sentry Organization
          description: Your Sentry organization slug
          value: 'split-software'
        - name: rate_limit
          kind: integer
          label: Rate Limit
          description: Maximum number of API requests per second
          value: 10
        - name: sample_fraction
          kind: float
          label: Sample Fraction
          description: Fraction of events to sample (0.0-1.0)
          value: null
        - name: max_events_per_project
          kind: integer
          label: Max Events Per Project
          description: Maximum number of events to fetch per project
          value: null
        - name: min_events_per_issue
          kind: integer
          label: Min Events Per Issue
          description: Minimum number of events to sample per issue
          value: 5
        - name: max_events_per_issue
          kind: integer
          label: Max Events Per Issue
          description: Maximum number of events to sample per issue
          value: 100
        - name: fetch_event_details
          kind: boolean
          label: Fetch Event Details
          description: Whether to fetch detailed event data for each event
          value: false
      select:
        - projects.*
        - issues.*
        - events.*
        - teams.*
        - users.*
        - releases.*
        - project_detail.*
        - release.*
```

## Data Flow and Sequence Diagram

The following sequence diagram illustrates the data flow between components when using tap-sentry with Meltano:

[View Sequence Diagram](meltano-sequence-diagram.md)

This diagram shows how the tap interacts with the Sentry API and how rate limiting and sampling are applied to the data flow.

## Sentry API Endpoints

The tap uses the following Sentry API endpoints:

1. **Projects List**
   - Endpoint: `/organizations/{organization_slug}/projects/`
   - Purpose: Fetch all projects for an organization

2. **Project Issues**
   - Endpoint: `/projects/{organization_slug}/{project_slug}/issues/`
   - Purpose: Fetch all issues for a specific project

3. **Issue Detail**
   - Endpoint: `/organizations/{organization_slug}/issues/{issue_id}/`
   - Purpose: Get detailed information about a specific issue

4. **Issue Events**
   - Endpoint: `/organizations/{organization_slug}/issues/{issue_id}/events/`
   - Purpose: Get all events associated with a specific issue

5. **Project Events**
   - Endpoint: `/projects/{organization_slug}/{project_slug}/events/`
   - Purpose: Get all events for a specific project

6. **Event Detail**
   - Endpoint: `/projects/{organization_slug}/{project_slug}/events/{event_id}/`
   - Purpose: Get detailed information about a specific event

7. **Teams**
   - Endpoint: `/organizations/{organization_slug}/teams/`
   - Purpose: Get all teams in an organization

8. **Users**
   - Endpoint: `/organizations/{organization_slug}/users/`
   - Purpose: Get all users in an organization

9. **Releases**
   - Endpoint: `/organizations/{organization_slug}/releases/`
   - Purpose: Get all releases for an organization

## Data Flow

1. The tap first fetches all projects for the configured organization
2. For each project, it fetches all issues
3. For each issue:
   - Fetches issue details
   - Fetches all events associated with the issue
   - If `fetch_event_details` is enabled, fetches detailed data for each event
4. All records are written to the target system
5. State is maintained for incremental syncs

## Error Handling

The tap includes error handling for common HTTP errors:
- 404 Not Found: Handled gracefully for missing resources
- 429 Rate Limit: Included in tolerated errors
- Other errors are logged with detailed information

## Incremental Sync

The tap supports incremental syncs using bookmarks for:
- Issues
- Events
- Teams
- Users
- Releases

Each stream maintains its own state to ensure efficient data extraction in subsequent runs.

## Limitations

1. Rate Limiting: Sentry API has rate limits that need to be respected
2. Event History: Some events might not be available after a certain time period
3. Project Access: The API token must have appropriate permissions for all projects
4. Data Volume: Large organizations with many events might need to consider data volume and sync frequency

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

## Rate Limiting

This tap includes built-in rate limiting to prevent overwhelming the Sentry API. The rate limit can be configured using the `rate_limit` parameter in your configuration.

### How Rate Limiting Works

- The tap automatically spaces out API requests to maintain the specified rate
- Default rate limit is 10 requests per second
- You can adjust this value based on your needs:
  - Lower values (1-5) for more conservative processing
  - Higher values (10-20) for faster processing
  - Match it to Sentry's API rate limits to maximize throughput

### Logging Rate Limit Information

When running with debug logging enabled, you'll see messages like:
```
Rate limiting: sleeping for 0.050 seconds
```

This indicates that the tap is waiting to maintain the configured rate limit.

### Adjusting Rate Limits

If you're experiencing rate limit errors from the Sentry API, try reducing the rate limit value. Conversely, if you want to speed up the sync process and your API quota allows it, you can increase the rate limit.

## Event Sampling

This tap includes an event sampling mechanism to control data ingestion and ETL costs. When working with Sentry projects that produce a large volume of events, sampling can significantly reduce processing time and storage requirements.

### Sampling Options

You can configure event sampling in two ways:

1. **Fractional Sampling**: Sample a percentage of all events
   ```json
   {
     "sample_fraction": 0.1  // Sample 10% of events
   }
   ```

2. **Maximum Event Limit**: Limit the number of events per project
   ```json
   {
     "max_events_per_project": 1000  // Limit to 1000 events per project
   }
   ```

You can use either option independently or combine them. When both are specified, the maximum limit is applied first, then the sampling fraction is applied to the limited set.

### How Sampling Works

The tap uses a random sampling approach to ensure a representative subset of events. Sampling is applied consistently to **both issue events and project events**:

1. All events are fetched from the Sentry API as usual
2. After fetching, the sampling logic is applied:
   - If `max_events_per_project` is set, only the first N events are kept
   - If `sample_fraction` is set, a random sample of the specified fraction is selected
3. Only the sampled events are written to the target system

The sampling is applied at two levels:
- **Project Level**: When fetching events directly from a project
- **Issue Level**: When fetching events associated with specific issues

This ensures consistent sampling across all event sources, maintaining statistical representativeness.

### Logging

When sampling is enabled, the tap logs information about the sampling process:

```
Event sampling enabled with fraction: 0.1
Total events fetched: 5000
Sampling 500 out of 5000 events for project my-project (10.0%)
After sampling: 500 events
```

The logs will show sampling information for both project events and issue events, giving you visibility into how sampling is affecting each event type.

### Benefits of Event Sampling

- **Reduced Processing Time**: Processing fewer events means faster ETL runs
- **Lower Storage Costs**: Storing fewer events reduces data warehouse costs
- **Focused Analysis**: Concentrate on the most important events
- **API Efficiency**: Reduce the number of API calls for detailed event data
- **Improved Reliability**: Less data to process means fewer potential points of failure
- **Consistent Data**: Same sampling rules applied to all event types

### Considerations

- **Data Completeness**: Sampling reduces the total number of events processed, which may affect analysis that requires complete data
- **Representativeness**: Random sampling provides a statistically representative subset, but may miss rare events
- **API Usage**: The tap still fetches all events from the API before sampling, so API usage is not reduced
- **Deterministic Results**: For reproducible results, consider setting a random seed in your environment
- **Event Type Balance**: Sampling is applied uniformly across all event types, which may affect the relative proportion of different event types in your data

### When to Use Sampling

Sampling is recommended when:
- You have projects with a very large number of events
- You need to reduce ETL costs or processing time
- Your analysis doesn't require every single event
- You're hitting API rate limits or timeouts
- You want to maintain a consistent dataset across different event types

### Recommended Sampling Values

For different scenarios, consider these sampling values:

| Scenario | sample_fraction | max_events_per_project | Description |
|----------|----------------|------------------------|-------------|
| Development | 0.1 | 100 | Quick testing with minimal data |
| Production (Balanced) | 0.2 | 1000 | Good balance of data volume and processing time |
| Production (Detailed) | 0.5 | 5000 | More comprehensive data for important projects |
| Production (Complete) | 1.0 | None | Process all events (no sampling) |

### Adjusting Sampling Parameters

You can adjust sampling parameters based on your needs:

1. **For more data**: Increase `sample_fraction` or `max_events_per_project`
2. **For faster processing**: Decrease `sample_fraction` or `max_events_per_project`
3. **For specific projects**: Consider using different sampling values for different projects

### Monitoring Sampling Effectiveness

To monitor the effectiveness of your sampling strategy:

1. Check the logs to see how many events are being sampled
2. Compare the distribution of event types in sampled vs. complete data
3. Verify that your analysis results are consistent with expectations
4. Monitor sampling rates for both project events and issue events to ensure balanced representation