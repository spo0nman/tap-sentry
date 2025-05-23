import asyncio
import urllib
import singer
import requests
from singer.bookmarks import get_bookmark
import json
from dateutil import parser

LOGGER = singer.get_logger()

# Global dictionary of Sentry API endpoints
SENTRY_API_ENDPOINTS = {
    "projects": "/organizations/{organization_slug}/projects/",
    "project_issues": "/projects/{organization_slug}/{project_slug}/issues/",
    "issue_detail": "/organizations/{organization_slug}/issues/{issue_id}/",
    "issue_events": "/organizations/{organization_slug}/issues/{issue_id}/events/",
    "project_events": "/projects/{organization_slug}/{project_slug}/events/",
    "event_detail": "/projects/{organization_slug}/{project_slug}/events/{event_id}/",
    "teams": "/organizations/{organization_slug}/teams/",
    "users": "/organizations/{organization_slug}/users/",
    "releases": "/organizations/{organization_slug}/releases/",
}


class SentryAuthentication(requests.auth.AuthBase):
    def __init__(self, api_token: str):
        self.api_token = api_token

    def __call__(self, req):
        req.headers.update({"Authorization": " Bearer " + self.api_token})

        return req


class SentryClient:
    def __init__(
        self,
        auth: SentryAuthentication,
        url="https://sentry.io/api/0/",
        organization="split-software",
        rate_limit=10,  # Default to 10 requests per second
        sample_fraction=None,  # Optional sampling fraction (0.0-1.0)
        max_events_per_project=None,  # Optional maximum events per project
        min_events_per_issue=5,  # Minimum events to sample per issue
        max_events_per_issue=100,  # Maximum events to sample per issue
        state={},
        projects=None,
        fetch_event_details=False,
    ):
        self._base_url = url
        self._auth = auth
        self._session = None
        self._organization = organization
        self._rate_limit = rate_limit
        self._last_request_time = 0

        # Convert sample_fraction to float if it's a string
        if sample_fraction is not None and isinstance(sample_fraction, str):
            try:
                self._sample_fraction = float(sample_fraction)
            except ValueError:
                raise ValueError(
                    f"Invalid sample_fraction value: {sample_fraction}. Must be a number between 0.0 and 1.0"
                )
        else:
            self._sample_fraction = sample_fraction

        self._max_events_per_project = max_events_per_project
        self._min_events_per_issue = min_events_per_issue
        self._max_events_per_issue = max_events_per_issue
        self._state = state
        self._projects = projects
        self.fetch_event_details = fetch_event_details
        self._processed_event_ids = set()  # Initialize set to track unique events

        # Validate sampling parameters
        if self._sample_fraction is not None:
            if not 0.0 <= self._sample_fraction <= 1.0:
                raise ValueError("sample_fraction must be between 0.0 and 1.0")
            LOGGER.info(
                f"Event sampling enabled with fraction: {self._sample_fraction}"
            )

        if self._max_events_per_project is not None:
            if self._max_events_per_project < 1:
                raise ValueError("max_events_per_project must be at least 1")
            LOGGER.info(
                f"Event limit enabled: {self._max_events_per_project} events per project"
            )

        if self._min_events_per_issue < 1:
            raise ValueError("min_events_per_issue must be at least 1")
        LOGGER.info(f"Minimum events per issue: {self._min_events_per_issue}")

        if self._max_events_per_issue < self._min_events_per_issue:
            raise ValueError(
                "max_events_per_issue must be greater than or equal to min_events_per_issue"
            )
        LOGGER.info(f"Maximum events per issue: {self._max_events_per_issue}")

        LOGGER.debug(
            f"Initialized SentryClient with base URL: {url}, organization: {organization}, rate limit: {rate_limit}/s"
        )

    @property
    def session(self):
        if not self._session:
            self._session = requests.Session()
            self._session.auth = self._auth
            self._session.headers.update({"Accept": "application/json"})

        return self._session

    def _wait_for_rate_limit(self):
        """Implement rate limiting between requests."""
        import time

        current_time = time.time()
        time_since_last_request = current_time - self._last_request_time

        # Calculate minimum time between requests based on rate limit
        min_interval = 1.0 / self._rate_limit

        if time_since_last_request < min_interval:
            sleep_time = min_interval - time_since_last_request
            LOGGER.debug(f"Rate limiting: sleeping for {sleep_time:.3f} seconds")
            time.sleep(sleep_time)

        self._last_request_time = time.time()

    def _apply_sampling(self, events, project_slug=None):
        """Apply sampling to a list of events based on configuration.

        Args:
            events: List of events to sample
            project_slug: Optional project slug for logging

        Returns:
            Sampled list of events
        """
        if not events:
            return events

        original_count = len(events)
        sampled_events = events

        # Apply max events limit if configured
        if (
            self._max_events_per_project is not None
            and original_count > self._max_events_per_project
        ):
            LOGGER.info(
                f"Limiting events for project {project_slug} from {original_count} to {self._max_events_per_project}"
            )
            sampled_events = events[: self._max_events_per_project]
            original_count = len(sampled_events)

        # Apply sampling fraction if configured
        if self._sample_fraction is not None and self._sample_fraction < 1.0:
            import random
            import hashlib

            # Create a deterministic seed based on project slug and event IDs
            # This ensures the same events are selected in each run
            seed_data = project_slug or "default"

            # Add event IDs to the seed data to ensure consistent sampling
            # Sort the event IDs to ensure consistent ordering
            event_ids = sorted(
                [
                    str(event.get("id") or event.get("eventID", ""))
                    for event in sampled_events
                ]
            )
            seed_data += "".join(event_ids)

            # Create a hash of the seed data to use as the random seed
            seed = int(hashlib.md5(seed_data.encode()).hexdigest(), 16) % (2**32)

            # Set the random seed
            random.seed(seed)

            # Calculate sample size based on fraction
            sample_size = int(original_count * self._sample_fraction)

            # Apply min/max event limits
            if (
                sample_size < self._min_events_per_issue
                and original_count >= self._min_events_per_issue
            ):
                sample_size = self._min_events_per_issue
                LOGGER.info(
                    f"Adjusted sample size to minimum of {sample_size} events for project {project_slug}"
                )
            elif sample_size > self._max_events_per_issue:
                sample_size = self._max_events_per_issue
                LOGGER.info(
                    f"Adjusted sample size to maximum of {sample_size} events for project {project_slug}"
                )
            elif sample_size < 1 and original_count > 0:
                sample_size = 1  # Ensure at least one event if we have any

            LOGGER.info(
                f"Sampling {sample_size} out of {original_count} events for project {project_slug} ({self._sample_fraction*100:.1f}%)"
            )
            sampled_events = random.sample(sampled_events, sample_size)

            # Reset the random seed to avoid affecting other random operations
            random.seed()

        return sampled_events

    def _get(self, path, params=None):
        """Make a GET request with rate limiting."""
        self._wait_for_rate_limit()

        url = self._base_url + path
        LOGGER.debug(f"Making GET request to: {url} with params: {params}")
        LOGGER.debug(f"Request headers: {self.session.headers}")

        # Use params argument for query parameters instead of appending to URL
        response = self.session.get(url, params=params)
        LOGGER.debug(f"Response status: {response.status_code}")
        LOGGER.debug(f"Response headers: {response.headers}")

        # Log the full URL with parameters for debugging
        if params:
            full_url = response.url
            LOGGER.debug(f"Full URL with parameters: {full_url}")

        try:
            response.raise_for_status()
            # Log a sample of the response content for debugging
            response_json = response.json()
            if isinstance(response_json, list):
                LOGGER.debug(f"Response is a list with {len(response_json)} items")
                if len(response_json) > 0:
                    LOGGER.debug(f"First item sample: {response_json[0]}")
            elif isinstance(response_json, dict):
                LOGGER.debug(f"Response keys: {list(response_json.keys())}")
            return response
        except requests.exceptions.HTTPError as e:
            LOGGER.error(f"HTTP error occurred: {e}")
            LOGGER.debug(f"Response content: {response.text}")
            raise
        except ValueError as e:
            LOGGER.error(f"Error parsing JSON response: {e}")
            LOGGER.debug(f"Raw response content: {response.text}")
            raise
        except Exception as e:
            LOGGER.error(f"Unexpected error in _get: {e}")
            raise

    def projects(self):
        try:
            url_path = SENTRY_API_ENDPOINTS["projects"].format(
                organization_slug=self._organization
            )
            full_url = self._base_url + url_path

            LOGGER.debug(f"Fetching projects from: {full_url}")

            try:
                LOGGER.debug(f"Making request with headers: {self.session.headers}")
                projects = self._get(url_path)
                result = projects.json()
                LOGGER.info(f"Found {len(result)} projects")
                return result
            except Exception as e:
                LOGGER.error(f"Error making request to {full_url}: {str(e)}")
                return None
        except Exception as e:
            LOGGER.error(f"Error fetching projects: {str(e)}")
            return None

    def issues(self, project_id, state):
        try:
            bookmark = get_bookmark(state, "issues", "start")

            # First, get the project slug for this project ID
            project_slug = None
            projects_list = self.projects()
            if projects_list:
                for project in projects_list:
                    if project["id"] == project_id:
                        project_slug = project["slug"]
                        LOGGER.debug(
                            f"Found project slug '{project_slug}' for project ID {project_id}"
                        )
                        break

            if not project_slug:
                LOGGER.error(
                    f"Could not find project slug for project ID: {project_id}"
                )
                return None

            # Use the correct URL structure according to Sentry API documentation
            query = SENTRY_API_ENDPOINTS["project_issues"].format(
                organization_slug=self._organization, project_slug=project_slug
            )

            # Add query parameters
            params = {}
            if bookmark:
                params["start"] = bookmark
                params["utc"] = "true"
                params["end"] = singer.utils.strftime(singer.utils.now())

            LOGGER.debug(f"Fetching issues from: {self._base_url + query}")
            LOGGER.debug(f"With parameters: {params}")

            response = self._get(query, params=params)
            issues = response.json()
            LOGGER.info(f"Found {len(issues)} issues for project {project_slug}")

            url = response.url
            LOGGER.debug(f"Initial issues response URL: {url}")

            while (
                response.links is not None
                and response.links.__len__() > 0
                and response.links["next"]["results"] == "true"
            ):
                url = response.links["next"]["url"]
                LOGGER.debug(f"Fetching next page of issues from: {url}")
                response = self.session.get(url)
                new_issues = response.json()
                LOGGER.debug(f"Found {len(new_issues)} additional issues on next page")
                issues += new_issues
            LOGGER.info(f"Total issues found: {len(issues)}")
            return issues

        except Exception as e:
            LOGGER.error(f"Error fetching issues: {str(e)}")
            return None

    def event_detail(self, organization_slug, project_slug, event_id):
        """Fetch detailed information for a specific event."""
        try:
            path = SENTRY_API_ENDPOINTS["event_detail"].format(
                organization_slug=organization_slug,
                project_slug=project_slug,
                event_id=event_id,
            )
            LOGGER.debug(f"Fetching event detail from: {self._base_url + path}")

            response = self._get(path)
            if response.status_code == 200:
                LOGGER.debug(f"Successfully fetched detailed event data for {event_id}")
                return response.json()
            else:
                LOGGER.error(f"Error fetching event detail: {response.status_code}")
                return None
        except Exception as e:
            LOGGER.error(f"Error fetching event detail for {event_id}: {str(e)}")
            return None

    def events(self, project_id, state):
        try:
            bookmark = get_bookmark(state, "events", "start")
            LOGGER.debug(f"Starting events fetch with bookmark: {bookmark}")

            # Use project slug instead of project ID
            project_slug = None
            projects_list = self.projects()
            if projects_list:
                for project in projects_list:
                    if project["id"] == project_id:
                        project_slug = project["slug"]
                        LOGGER.debug(
                            f"Found project slug '{project_slug}' for project ID {project_id}"
                        )
                        break

            if not project_slug:
                LOGGER.error(
                    f"Could not find project slug for project ID: {project_id}"
                )
                return None

            query = SENTRY_API_ENDPOINTS["project_events"].format(
                organization_slug=self._organization, project_slug=project_slug
            )

            params = {}
            if bookmark:
                params["start"] = bookmark
                params["utc"] = "true"
                params["end"] = singer.utils.strftime(singer.utils.now())

            LOGGER.debug(f"Making request to events endpoint: {self._base_url + query}")
            LOGGER.debug(f"With parameters: {params}")

            response = self._get(query, params=params)
            events = response.json()
            LOGGER.info(f"Initial response contains {len(events)} events")

            url = response.url
            LOGGER.debug(f"Initial events response URL: {url}")

            page_count = 1
            while (
                response.links is not None
                and response.links.__len__() > 0
                and response.links["next"]["results"] == "true"
            ):
                page_count += 1
                url = response.links["next"]["url"]
                LOGGER.debug(f"Fetching page {page_count} of events from: {url}")
                response = self.session.get(url)
                new_events = response.json()
                events += new_events
                LOGGER.debug(f"Added {len(new_events)} events from page {page_count}")

            LOGGER.info(f"Total events fetched: {len(events)}")

            # Apply sampling if configured
            if (
                self._sample_fraction is not None
                or self._max_events_per_project is not None
            ):
                events = self._apply_sampling(events, project_slug)
                LOGGER.info(f"After sampling: {len(events)} events")

            return events

        except Exception as e:
            LOGGER.error(f"Error fetching events: {str(e)}")
            return None

    def issue_events(self, organization_slug, issue_id, state):
        """Fetch events for a specific issue."""
        try:
            bookmark = get_bookmark(state, "issue_events", "start")
            LOGGER.debug(
                f"Starting issue events fetch for issue {issue_id} with bookmark: {bookmark}"
            )

            # First, get the issue details to verify it exists
            issue_query = SENTRY_API_ENDPOINTS["issue_detail"].format(
                organization_slug=organization_slug, issue_id=issue_id
            )

            try:
                issue_response = self._get(issue_query)
                if issue_response.status_code == 200:
                    issue_data = issue_response.json()
                    project_slug = issue_data.get("project", {}).get("slug")
                    if not project_slug:
                        LOGGER.error(
                            f"Could not find project slug for issue {issue_id}"
                        )
                        return None
                    LOGGER.debug(
                        f"Found project slug '{project_slug}' for issue {issue_id}"
                    )
                else:
                    LOGGER.error(
                        f"Error fetching issue details: {issue_response.status_code}"
                    )
                    return None
            except Exception as e:
                LOGGER.error(f"Error fetching issue details: {str(e)}")
                return None

            query = SENTRY_API_ENDPOINTS["issue_events"].format(
                organization_slug=organization_slug, issue_id=issue_id
            )

            params = {}
            if bookmark:
                params["start"] = bookmark
                params["utc"] = "true"
                params["end"] = singer.utils.strftime(singer.utils.now())

            LOGGER.debug(
                f"Making request to issue events endpoint: {self._base_url + query}"
            )
            LOGGER.debug(f"With parameters: {params}")

            response = self._get(query, params=params)
            events = response.json()
            LOGGER.info(
                f"Initial response contains {len(events)} events for issue {issue_id}"
            )

            url = response.url
            LOGGER.debug(f"Initial issue events response URL: {url}")

            page_count = 1
            while (
                response.links is not None
                and response.links.__len__() > 0
                and response.links["next"]["results"] == "true"
            ):
                page_count += 1
                url = response.links["next"]["url"]
                LOGGER.debug(
                    f"Fetching page {page_count} of events for issue {issue_id} from: {url}"
                )
                response = self.session.get(url)
                new_events = response.json()
                events += new_events
                LOGGER.debug(f"Added {len(new_events)} events from page {page_count}")

            LOGGER.info(f"Total events fetched for issue {issue_id}: {len(events)}")

            # Apply sampling if configured
            if (
                self._sample_fraction is not None
                or self._max_events_per_project is not None
            ):
                events = self._apply_sampling(events, project_slug)
                LOGGER.info(
                    f"After sampling: {len(events)} events for issue {issue_id}"
                )

            return events

        except Exception as e:
            LOGGER.error(f"Error fetching issue events: {str(e)}")
            return None

    def teams(self, state):
        try:
            bookmark = get_bookmark(state, "teams", "start")
            url_path = f"/organizations/{self._organization}/teams/"

            # Add date filtering if bookmark exists
            if bookmark:
                url_path += f"?start={urllib.parse.quote(bookmark)}&utc=true&end={urllib.parse.quote(singer.utils.strftime(singer.utils.now()))}"

            LOGGER.debug(f"Fetching teams from: {self._base_url + url_path}")
            response = self._get(url_path)
            teams = response.json()
            url = response.url
            LOGGER.debug(f"Initial teams response URL: {url}")

            while (
                response.links is not None
                and response.links.__len__() > 0
                and response.links["next"]["results"] == "true"
            ):
                url = response.links["next"]["url"]
                LOGGER.debug(f"Fetching next page of teams from: {url}")
                response = self.session.get(url)
                teams += response.json()
            return teams
        except Exception as e:
            LOGGER.debug(f"Error fetching teams: {str(e)}")
            return None

    def users(self, state):
        try:
            bookmark = get_bookmark(state, "users", "start")
            url_path = f"/organizations/{self._organization}/users/"

            # Add date filtering if bookmark exists
            if bookmark:
                url_path += f"?start={urllib.parse.quote(bookmark)}&utc=true&end={urllib.parse.quote(singer.utils.strftime(singer.utils.now()))}"

            LOGGER.debug(f"Fetching users from: {self._base_url + url_path}")
            response = self._get(url_path)
            users = response.json()
            url = response.url
            LOGGER.debug(f"Initial users response URL: {url}")

            # Handle pagination if available
            while (
                response.links is not None
                and response.links.__len__() > 0
                and response.links["next"]["results"] == "true"
            ):
                url = response.links["next"]["url"]
                LOGGER.debug(f"Fetching next page of users from: {url}")
                response = self.session.get(url)
                users += response.json()
            return users
        except Exception as e:
            LOGGER.debug(f"Error fetching users: {str(e)}")
            return None

    def releases(self, project_id, state):
        try:
            bookmark = get_bookmark(state, "releases", "start")

            # Use the correct URL structure from SENTRY_API_ENDPOINTS
            query = SENTRY_API_ENDPOINTS["releases"].format(
                organization_slug=self._organization
            )

            # Add project filter and query parameters
            params = {"project": project_id}
            if bookmark:
                params["start"] = bookmark
                params["utc"] = "true"
                params["end"] = singer.utils.strftime(singer.utils.now())

            LOGGER.debug(f"Fetching releases from: {self._base_url + query}")
            LOGGER.debug(f"With parameters: {params}")

            response = self._get(query, params=params)
            releases = response.json()
            LOGGER.info(f"Initial response contains {len(releases)} releases")

            url = response.url
            LOGGER.debug(f"Initial releases response URL: {url}")

            while (
                response.links is not None
                and response.links.__len__() > 0
                and response.links["next"]["results"] == "true"
            ):
                url = response.links["next"]["url"]
                LOGGER.debug(f"Fetching next page of releases from: {url}")
                response = self.session.get(url)
                new_releases = response.json()
                releases += new_releases
                LOGGER.debug(f"Added {len(new_releases)} releases from next page")

            LOGGER.info(f"Total releases fetched: {len(releases)}")
            return releases
        except Exception as e:
            LOGGER.error(f"Error fetching releases: {str(e)}")
            return None

    async def fetch_single_data(self, url, headers):
        """Fetch a single object from the Sentry API."""
        try:
            async with self.session.get(url, headers=headers) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    self.logger.error(
                        f"Error fetching data from {url}: {response.status}"
                    )
                    return None
        except Exception as e:
            self.logger.error(f"Exception fetching data from {url}: {str(e)}")
            return None

    async def sync_events(self, schema, stream):
        """Sync events from Sentry API."""
        with singer.metrics.job_timer(job_type=f"sync_{stream}"):
            # Fix schema format
            schema_dict = self._get_formatted_schema(schema)
            singer.write_schema(stream, schema_dict, ["id"])

            extraction_time = singer.utils.now()
            if self.projects:
                loop = asyncio.get_event_loop()

                # Track event counts for summary
                event_counts = {}

                # Get list of events already processed through issues
                processed_events = set()
                if (
                    "metadata" in self._state
                    and "processed_events" in self._state["metadata"]
                ):
                    processed_events = set(self._state["metadata"]["processed_events"])
                    LOGGER.info(
                        f"Found {len(processed_events)} events already processed through issues"
                    )

                for project in self.projects:
                    project_id = project.get("id")
                    project_slug = project.get("slug")

                    LOGGER.info(f"Fetching events for project {project_slug}")

                    # Get project-specific bookmark
                    project_bookmark = singer.get_bookmark(
                        self._state, stream, project_id, {}
                    ).get("start")

                    if project_bookmark:
                        LOGGER.info(
                            f"Starting event sync for project {project_slug} from bookmark: {project_bookmark}"
                        )
                    else:
                        LOGGER.info(
                            f"No project bookmark found for {project_slug}, performing full sync"
                        )

                    # Fetch project events
                    events = await loop.run_in_executor(
                        None,
                        self.events,
                        project_id,
                        self._state,
                    )

                    if events:
                        # Filter out events already processed through issues
                        events = [
                            event
                            for event in events
                            if event.get("id") not in processed_events
                        ]
                        LOGGER.info(
                            f"After filtering processed events: {len(events)} events for project {project_slug}"
                        )

                        # Apply sampling if configured
                        if self._sample_fraction is not None:
                            LOGGER.info(
                                f"Applying sampling to events for project {project_slug}"
                            )
                            events = self._apply_sampling(events, project_slug)
                            LOGGER.info(
                                f"After sampling: {len(events)} events for project {project_slug}"
                            )

                        # Apply max events limit if configured
                        if (
                            self._max_events_per_project is not None
                            and len(events) > self._max_events_per_project
                        ):
                            LOGGER.info(
                                f"Limiting events for project {project_slug} to {self._max_events_per_project}"
                            )
                            events = events[: self._max_events_per_project]

                        event_count = len(events)
                        LOGGER.info(
                            f"Found {event_count} events for project {project_slug}"
                        )

                        # Track event count for this project
                        event_counts[project_slug] = event_count

                        for event in events:
                            event_id = event.get("id") or event.get("eventID")
                            LOGGER.debug(
                                f"Processing event {event_id} from project {project_slug}"
                            )

                            # Add project context
                            event["project_slug"] = project_slug
                            event["project_id"] = project_id
                            event["event_type"] = "project"

                            # Fetch detailed event data if enabled
                            if self.fetch_event_details and event_id:
                                try:
                                    LOGGER.debug(
                                        f"Fetching detailed data for event {event_id} in project {project_slug}"
                                    )
                                    detailed_event = await loop.run_in_executor(
                                        None,
                                        self.event_detail,
                                        self._organization,
                                        project_slug,
                                        event_id,
                                    )
                                    if detailed_event:
                                        event.update(detailed_event)
                                        LOGGER.debug(
                                            f"Successfully merged detailed data for event {event_id}"
                                        )
                                    else:
                                        LOGGER.warning(
                                            f"No detailed data found for event {event_id}"
                                        )
                                except Exception as detail_e:
                                    LOGGER.error(
                                        f"Error fetching detailed data for event {event_id}: {str(detail_e)}"
                                    )

                            # Write the event to the stream
                            LOGGER.debug(f"Writing event {event_id} to stream")
                            singer.write_record(stream, event)

                            # Update event-specific bookmark
                            self._state = singer.write_bookmark(
                                self._state,
                                stream,
                                event_id,
                                {"start": singer.utils.strftime(extraction_time)},
                            )
                    else:
                        LOGGER.warning(f"No events found for project {project_slug}")

                    # Update project-specific bookmark
                    self._state = singer.write_bookmark(
                        self._state,
                        stream,
                        project_id,
                        {"start": singer.utils.strftime(extraction_time)},
                    )

            # Update global bookmark
            self._state = singer.write_bookmark(
                self._state, stream, "start", singer.utils.strftime(extraction_time)
            )

            # Add metadata to state
            self._state["metadata"] = {
                "last_sync": singer.utils.strftime(extraction_time),
                "sync_type": "full",
                "streams_synced": ["events"],
                "projects_processed": [p.get("slug") for p in self.projects]
                if self.projects
                else [],
                "events_processed": sum(event_counts.values()),
                "project_event_counts": event_counts,
            }


class SentrySync:
    def __init__(self, client: SentryClient, state={}, config={}):
        self._client = client
        self._state = state
        self._config = config
        self.projects = self.client.projects()

        # Read config settings with defaults
        self.fetch_event_details = self._config.get("fetch_event_details", False)
        LOGGER.info(
            f"Event detail fetching is {'enabled' if self.fetch_event_details else 'disabled'}"
        )

        # Log sampling configuration
        sample_fraction = self._config.get("sample_fraction")
        max_events = self._config.get("max_events_per_project")

        if sample_fraction is not None:
            LOGGER.info(f"Event sampling configured with fraction: {sample_fraction}")

        if max_events is not None:
            LOGGER.info(f"Event limit configured: {max_events} events per project")

    @property
    def client(self):
        return self._client

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        singer.write_state(value)
        self._state = value

    async def sync(self, stream, schema):
        """Generic sync method that routes to specific sync methods."""
        LOGGER.info(f"Starting sync for {stream}")

        # Get the appropriate sync method for this stream
        sync_method_name = f"sync_{stream}"

        if hasattr(self, sync_method_name):
            sync_method = getattr(self, sync_method_name)
            return await sync_method(schema, stream)
        else:
            LOGGER.warning(f"No sync method found for {stream}")
            return None

    async def sync_issues(self, schema, stream):
        """Sync issues from Sentry API and their associated events."""
        with singer.metrics.job_timer(job_type=f"sync_{stream}"):
            # Fix schema format
            schema_dict = self._get_formatted_schema(schema)
            singer.write_schema(stream, schema_dict, ["id"])

            # Also write schema for events since we'll be syncing them
            events_schema = self._get_formatted_schema(
                schema
            )  # We'll need to update this with proper events schema
            singer.write_schema("events", events_schema, ["eventID"])

            extraction_time = singer.utils.now()
            if self.projects:
                loop = asyncio.get_event_loop()

                # Track event counts by issue for summary
                issue_event_counts = {}

                for project in self.projects:
                    project_id = project.get("id")
                    project_slug = project.get("slug")
                    LOGGER.info(f"Processing project {project_slug} (ID: {project_id})")

                    try:
                        # Get project-specific bookmark
                        project_bookmark = singer.get_bookmark(
                            self.state, "issues", project_id, {}
                        ).get("start")

                        LOGGER.info(
                            f"Project {project_slug} bookmark: {project_bookmark}"
                        )

                        issues = await loop.run_in_executor(
                            None, self.client.issues, project_id, self.state
                        )

                        if issues:
                            LOGGER.info(
                                f"Found {len(issues)} issues for project {project_slug}"
                            )
                            for issue in issues:
                                # Write the issue record
                                singer.write_record(stream, issue)

                                # Now fetch and write events for this issue
                                issue_id = issue.get("id")
                                if issue_id:
                                    LOGGER.info(
                                        f"Fetching events for Issue {issue_id} in project {project_slug}"
                                    )
                                    issue_events = await loop.run_in_executor(
                                        None,
                                        self.client.issue_events,
                                        self.client._organization,
                                        issue_id,
                                        self.state,
                                    )

                                    if issue_events:
                                        event_count = len(issue_events)
                                        LOGGER.info(
                                            f"Found {event_count} events for Issue {issue_id}"
                                        )

                                        # Track event count for this issue
                                        issue_event_counts[issue_id] = event_count

                                        for event in issue_events:
                                            event_id = event.get("id") or event.get(
                                                "eventID"
                                            )
                                            LOGGER.debug(
                                                f"Processing event {event_id} from Issue {issue_id}"
                                            )

                                            # Add issue context to the event
                                            event["issue_id"] = issue_id
                                            event["issue_title"] = issue.get("title")

                                            # Fetch detailed event data if enabled
                                            if (
                                                self.fetch_event_details
                                                and event_id
                                                and project_slug
                                            ):
                                                try:
                                                    LOGGER.debug(
                                                        f"Fetching detailed data for event {event_id} in project {project_slug}"
                                                    )
                                                    detailed_event = (
                                                        await loop.run_in_executor(
                                                            None,
                                                            self.client.event_detail,
                                                            self.client._organization,
                                                            project_slug,
                                                            event_id,
                                                        )
                                                    )
                                                    if detailed_event:
                                                        event.update(detailed_event)
                                                        LOGGER.debug(
                                                            f"Successfully merged detailed data for event {event_id}"
                                                        )
                                                    else:
                                                        LOGGER.warning(
                                                            f"No detailed data found for event {event_id}"
                                                        )
                                                except Exception as detail_e:
                                                    LOGGER.error(
                                                        f"Error fetching detailed data for event {event_id}: {str(detail_e)}"
                                                    )

                                            # Write the event to the stream
                                            LOGGER.debug(
                                                f"Writing event {event_id} to stream"
                                            )
                                            singer.write_record("events", event)

                                            # Update event-specific bookmark
                                            self.state = singer.write_bookmark(
                                                self.state,
                                                "events",
                                                event_id,
                                                {
                                                    "start": singer.utils.strftime(
                                                        extraction_time
                                                    )
                                                },
                                            )
                                    else:
                                        LOGGER.warning(
                                            f"No events found for Issue {issue_id} in project {project_slug}"
                                        )

                                    # Update issue-specific bookmark
                                    self.state = singer.write_bookmark(
                                        self.state,
                                        "issues",
                                        issue_id,
                                        {
                                            "start": singer.utils.strftime(
                                                extraction_time
                                            )
                                        },
                                    )
                                    LOGGER.info(
                                        f"Updated bookmark for Issue {issue_id} ({event_count} events)"
                                    )
                        else:
                            LOGGER.warning(
                                f"No issues found for project {project_slug}"
                            )

                        # Update project-specific bookmark
                        self.state = singer.write_bookmark(
                            self.state,
                            "issues",
                            project_id,
                            {"start": singer.utils.strftime(extraction_time)},
                        )
                        LOGGER.info(f"Updated bookmark for project {project_slug}")
                    except Exception as e:
                        LOGGER.error(
                            f"Error processing project {project_slug}: {str(e)}"
                        )
                        LOGGER.debug(f"Full exception details: {str(e)}", exc_info=True)

            # Update global bookmarks
            self.state = singer.write_bookmark(
                self.state, "issues", "start", singer.utils.strftime(extraction_time)
            )
            self.state = singer.write_bookmark(
                self.state, "events", "start", singer.utils.strftime(extraction_time)
            )

            # Log summary of processed issues and events
            LOGGER.info("=== Sync Summary ===")
            LOGGER.info(f"Total issues processed: {len(issue_event_counts)}")
            LOGGER.info(f"Total events processed: {sum(issue_event_counts.values())}")

            # Log top issues by event count
            sorted_issues = sorted(
                issue_event_counts.items(), key=lambda x: x[1], reverse=True
            )
            LOGGER.info("Top issues by event count:")
            for issue_id, count in sorted_issues[:10]:  # Show top 10
                LOGGER.info(f"Issue {issue_id}: {count} events")

            # Add metadata to state
            self.state["metadata"] = {
                "last_sync": singer.utils.strftime(extraction_time),
                "sync_type": "full",
                "streams_synced": ["issues", "events"],
                "projects_processed": [p.get("slug") for p in self.projects]
                if self.projects
                else [],
                "issue_event_counts": issue_event_counts,
            }

    async def sync_projects(self, schema, stream):
        """Sync projects."""
        with singer.metrics.job_timer(job_type=f"sync_{stream}"):
            # Fix schema format
            schema_dict = self._get_formatted_schema(schema)
            singer.write_schema(stream, schema_dict, ["id"])

            extraction_time = singer.utils.now()

            # Track processed projects for metadata
            processed_projects = []

            if self.projects:
                for project in self.projects:
                    project_id = project.get("id")
                    project_slug = project.get("slug")

                    # Write the project record
                    singer.write_record(stream, project)

                    # Track processed project
                    processed_projects.append(project_slug)

                    # Update project-specific bookmark
                    self.state = singer.write_bookmark(
                        self.state,
                        stream,
                        project_id,
                        {"start": singer.utils.strftime(extraction_time)},
                    )

            # Update global bookmark
            self.state = singer.write_bookmark(
                self.state, "projects", "start", singer.utils.strftime(extraction_time)
            )

            # Add metadata to state
            self.state["metadata"] = {
                "last_sync": singer.utils.strftime(extraction_time),
                "sync_type": "full",
                "streams_synced": ["projects"],
                "projects_processed": processed_projects,
            }

    async def sync_teams(self, schema, stream):
        """Sync teams from Sentry API."""
        with singer.metrics.job_timer(job_type=f"sync_{stream}"):
            # Fix schema format
            schema_dict = self._get_formatted_schema(schema)
            singer.write_schema(stream, schema_dict, ["id"])

            # Get teams data from the client
            LOGGER.info("Fetching teams data")
            extraction_time = singer.utils.now()

            # Track processed teams for metadata
            processed_teams = []

            try:
                teams = await asyncio.get_event_loop().run_in_executor(
                    None, self.client.teams, self.state
                )

                if teams:
                    LOGGER.info(f"Found {len(teams)} teams to sync")
                    # Process each team
                    for team in teams:
                        # Get the previous state for this team
                        team_id = team.get("id")
                        previous_state = singer.get_bookmark(
                            self.state, stream, team_id, {}
                        )
                        previous_member_count = previous_state.get("memberCount")
                        current_member_count = team.get("memberCount")

                        # Only sync if memberCount has changed or this is the first sync
                        if (
                            previous_member_count is None
                            or previous_member_count != current_member_count
                        ):
                            # Process the record to add text_content and handle ID conversion
                            processed_team = self.process_record(stream, team)
                            singer.write_record(stream, processed_team)
                            singer.metrics.record_counter(stream).increment()

                            # Update the state with current memberCount
                            self.state = singer.write_bookmark(
                                self.state,
                                stream,
                                team_id,
                                {"memberCount": current_member_count},
                            )

                            # Track processed team
                            processed_teams.append(team_id)
                        else:
                            LOGGER.debug(
                                f"Skipping team {team_id} - memberCount unchanged"
                            )
                else:
                    LOGGER.warning("No teams found to sync")

                # Update state with extraction time
                self.state = singer.write_bookmark(
                    self.state, stream, "start", singer.utils.strftime(extraction_time)
                )

                # Add metadata to state
                self.state["metadata"] = {
                    "last_sync": singer.utils.strftime(extraction_time),
                    "sync_type": "full",
                    "streams_synced": ["teams"],
                    "teams_processed": len(processed_teams),
                }
            except Exception as e:
                LOGGER.error(f"Error syncing teams: {e}")

    async def sync_users(self, schema, stream):
        """Sync users from Sentry API."""
        with singer.metrics.job_timer(job_type=f"sync_{stream}"):
            # Fix schema format
            schema_dict = self._get_formatted_schema(schema)
            singer.write_schema(stream, schema_dict, ["id"])

            # Get users data from the client
            LOGGER.info("Fetching users data")
            extraction_time = singer.utils.now()

            # Track processed users for metadata
            processed_users = []

            try:
                # Get the start bookmark for users
                start_bookmark = singer.get_bookmark(self.state, stream, "start")
                if start_bookmark:
                    LOGGER.info(f"Starting user sync from bookmark: {start_bookmark}")
                else:
                    LOGGER.info("No start bookmark found, performing full sync")

                users = await asyncio.get_event_loop().run_in_executor(
                    None, self.client.users, self.state
                )

                if users:
                    LOGGER.info(f"Found {len(users)} users to sync")
                    # Process each user
                    for user in users:
                        # Get the previous state for this user
                        user_id = user.get("id")
                        previous_state = singer.get_bookmark(
                            self.state, stream, user_id, {}
                        )

                        # Check if relevant fields have changed
                        current_role = user.get("role")
                        current_projects = sorted(user.get("projects", []))
                        current_flags = user.get("flags", {})
                        current_date_created = user.get("dateCreated")

                        previous_role = previous_state.get("role")
                        previous_projects = sorted(previous_state.get("projects", []))
                        previous_flags = previous_state.get("flags", {})
                        previous_date_created = previous_state.get("dateCreated")

                        # Only sync if any of the tracked fields have changed or it's a new user
                        if (
                            previous_role != current_role
                            or previous_projects != current_projects
                            or previous_flags != current_flags
                            or not previous_date_created
                            or (
                                start_bookmark and current_date_created > start_bookmark
                            )
                        ):
                            singer.write_record(stream, user)
                            singer.metrics.record_counter(stream).increment()

                            # Update the state with current values
                            self.state = singer.write_bookmark(
                                self.state,
                                stream,
                                user_id,
                                {
                                    "role": current_role,
                                    "projects": current_projects,
                                    "flags": current_flags,
                                    "dateCreated": current_date_created,
                                },
                            )

                            # Track processed user
                            processed_users.append(user_id)
                        else:
                            LOGGER.debug(
                                f"Skipping user {user_id} - no relevant changes"
                            )
                else:
                    LOGGER.warning("No users found to sync")

                # Update state with extraction time
                self.state = singer.write_bookmark(
                    self.state, stream, "start", singer.utils.strftime(extraction_time)
                )

                # Add metadata to state
                self.state["metadata"] = {
                    "last_sync": singer.utils.strftime(extraction_time),
                    "sync_type": "full",
                    "streams_synced": ["users"],
                    "users_processed": len(processed_users),
                }
            except Exception as e:
                LOGGER.error(f"Error syncing users: {e}")

    async def sync_release(self, schema, stream):
        """Sync release data from Sentry API."""
        LOGGER.info(f"Syncing {stream}")

        # Fix schema format
        schema_dict = self._get_formatted_schema(schema)
        singer.write_schema(stream, schema_dict, ["version"])
        extraction_time = singer.utils.now()

        # Track processed releases for metadata
        processed_releases = []
        filtered_count = 0

        # Get the global start_date to use as fallback filter
        global_start_date = singer.get_bookmark(self.state, stream, "start", None)
        if not global_start_date and "bookmarks" in self.state:
            # Try to get start_date from any stream as fallback
            for stream_name, bookmarks in self.state.get("bookmarks", {}).items():
                if "start" in bookmarks:
                    global_start_date = bookmarks["start"]
                    break

        # If still no global date, check config
        if (
            not global_start_date
            and hasattr(self, "_config")
            and "start_date" in self._config
        ):
            global_start_date = self._config["start_date"]
            LOGGER.info(
                f"Using config start_date as fallback filter: {global_start_date}"
            )

        if global_start_date:
            LOGGER.info(
                f"Using global start date as fallback filter: {global_start_date}"
            )

        # Use existing projects property
        if self.projects:
            for project in self.projects:
                project_id = project.get("id")
                project_slug = project.get("slug")

                LOGGER.info(f"Fetching releases for project {project_slug}")

                # Get project-specific bookmark
                project_bookmark = singer.get_bookmark(
                    self.state, stream, project_id, {}
                ).get("start")

                if project_bookmark:
                    LOGGER.info(
                        f"Starting release sync for project {project_slug} from bookmark: {project_bookmark}"
                    )
                    filter_date = project_bookmark
                elif global_start_date:
                    LOGGER.info(
                        f"No project bookmark found for {project_slug}, using global start date: {global_start_date}"
                    )
                    filter_date = global_start_date
                else:
                    LOGGER.info(
                        f"No project bookmark or global start date found for {project_slug}, performing full sync"
                    )
                    filter_date = None

                # Use the existing releases method in the client
                releases = await asyncio.get_event_loop().run_in_executor(
                    None, self.client.releases, project_id, self.state
                )

                if releases:
                    # Track the original count for logging purposes
                    original_count = len(releases)

                    # Client-side filtering based on dateCreated
                    if filter_date:
                        # Filter releases by date
                        filtered_releases = []
                        for release in releases:
                            if release.get("dateCreated"):
                                release_date = parser.parse(release.get("dateCreated"))
                                if release_date >= parser.parse(filter_date):
                                    filtered_releases.append(release)
                            else:
                                # Include releases without dateCreated for safety
                                filtered_releases.append(release)

                        filtered_count += original_count - len(filtered_releases)
                        LOGGER.info(
                            f"Filtered out {original_count - len(filtered_releases)} releases older than {filter_date}"
                        )
                        releases = filtered_releases

                    LOGGER.info(
                        f"Processing {len(releases)} releases for project {project_slug}"
                    )

                    for release in releases:
                        # Add project context if not present
                        if "project_id" not in release:
                            release["project_id"] = project_id
                        if "project_slug" not in release:
                            release["project_slug"] = project_slug

                        release_version = release.get("version")
                        release_date = release.get("dateCreated")

                        # Check if we need to sync this release
                        release_bookmark = singer.get_bookmark(
                            self.state, stream, f"{project_id}:{release_version}", {}
                        ).get("start")

                        should_sync = (
                            not release_bookmark  # New release
                            or (
                                project_bookmark and release_date > project_bookmark
                            )  # Updated since last sync
                            or (
                                release_bookmark and release_date > release_bookmark
                            )  # Updated since last sync for this release
                        )

                        if should_sync:
                            # Write the record
                            processed_release = self.process_record(stream, release)
                            singer.write_record(stream, processed_release)
                            singer.metrics.record_counter(stream).increment()

                            # Track processed release
                            if release_version:
                                processed_releases.append(
                                    f"{project_slug}:{release_version}"
                                )

                            # Update release-specific bookmark
                            self.state = singer.write_bookmark(
                                self.state,
                                stream,
                                f"{project_id}:{release_version}",
                                {"start": singer.utils.strftime(extraction_time)},
                            )
                        else:
                            LOGGER.debug(
                                f"Skipping release {release_version} for project {project_slug} - no changes"
                            )

                    # Update project-specific bookmark
                    self.state = singer.write_bookmark(
                        self.state,
                        stream,
                        project_id,
                        {"start": singer.utils.strftime(extraction_time)},
                    )

            # Update state with extraction time
            self.state = singer.write_bookmark(
                self.state, stream, "start", singer.utils.strftime(extraction_time)
            )

            # Add metadata to state
            self.state["metadata"] = {
                "last_sync": singer.utils.strftime(extraction_time),
                "sync_type": "full",
                "streams_synced": ["release"],
                "projects_processed": [p.get("slug") for p in self.projects]
                if self.projects
                else [],
                "releases_processed": len(processed_releases),
                "releases_filtered": filtered_count,
            }
        else:
            LOGGER.warning("No projects found for fetching releases")

    async def sync_project_detail(self, schema, stream):
        """Sync detailed project information from Sentry API."""
        with singer.metrics.job_timer(job_type=f"sync_{stream}"):
            # Fix schema format - ensure it's not nested under 'type'
            schema_dict = schema.to_dict()
            if (
                "type" in schema_dict
                and isinstance(schema_dict["type"], dict)
                and "properties" in schema_dict["type"]
            ):
                # Schema is incorrectly nested under 'type', so extract it
                schema_dict = schema_dict["type"]

            # Now write the properly formatted schema
            singer.write_schema(stream, schema_dict, ["id"])

            # Initialize an empty list to guarantee we always have something iterable
            projects_to_process = []

            try:
                # First check if we already have projects
                if not self.projects:
                    LOGGER.info("Fetching projects for project detail sync")
                    self.projects = self.client.projects()

                # Only assign if we got a valid result
                if self.projects:
                    projects_to_process = self.projects
                    LOGGER.info(f"Found {len(projects_to_process)} projects to process")
                else:
                    LOGGER.warning("No projects found to process")
            except Exception as e:
                LOGGER.error(f"Error while preparing projects for sync: {e}")

            # Process each project - this will be skipped if projects_to_process is empty
            for project in projects_to_process:
                try:
                    project_id = project.get("id")
                    project_slug = project.get("slug")
                    org_slug = project.get("organization", {}).get(
                        "slug", self.client._organization
                    )

                    LOGGER.info(f"Syncing details for project {project_slug}")
                    LOGGER.debug(
                        f"Project details - ID: {project_id}, slug: {project_slug}, org: {org_slug}"
                    )

                    # Try to get detailed info
                    try:
                        detail_url = f"/projects/{org_slug}/{project_slug}/"
                        LOGGER.debug(
                            f"Fetching project details from: {self.client._base_url + detail_url}"
                        )
                        response = await asyncio.get_event_loop().run_in_executor(
                            None, self.client._get, detail_url
                        )

                        if response and response.status_code == 200:
                            project_detail = response.json()
                            project_detail["organization_slug"] = org_slug

                            # Write record and increment counter
                            singer.write_record(stream, project_detail)
                            singer.metrics.record_counter(stream).increment()
                            LOGGER.info(
                                f"Successfully synced project detail for {project_slug}"
                            )
                        else:
                            LOGGER.warning(
                                f"Failed to get details for project {project_slug}, falling back to basic project info"
                            )
                            # Fallback to basic project info
                            project["organization_slug"] = org_slug
                            singer.write_record(stream, project)
                            singer.metrics.record_counter(stream).increment()
                    except Exception as e:
                        LOGGER.error(
                            f"Error fetching details for project {project_slug}: {e}"
                        )
                        LOGGER.debug(f"Exception details: {str(e)}")
                        # Still try to use the basic project info
                        project["organization_slug"] = org_slug
                        singer.write_record(stream, project)
                        singer.metrics.record_counter(stream).increment()

                except Exception as e:
                    LOGGER.error(f"Error processing project: {e}")
                    LOGGER.debug(f"Exception details: {str(e)}")
                    continue

            # If we didn't process any projects, log a clear message
            if not projects_to_process:
                LOGGER.warning("No projects were processed during project_detail sync")

    def _get_formatted_schema(self, schema):
        """Ensure schema is properly formatted for Singer protocol."""
        schema_dict = schema.to_dict()
        if (
            "type" in schema_dict
            and isinstance(schema_dict["type"], dict)
            and "properties" in schema_dict["type"]
        ):
            # Schema is incorrectly nested under 'type', so extract it
            schema_dict = schema_dict["type"]
        return schema_dict

    def process_record(self, stream_name, record):
        """Add text_content field to record for embedding/search."""
        # Only process if text_content isn't already set
        if "text_content" not in record or not record["text_content"]:
            if stream_name == "teams":
                record["text_content"] = (
                    f"Team {record.get('name', '')} ({record.get('slug', '')})"
                )
            elif stream_name == "users":
                user = record.get("user", {})
                record["text_content"] = (
                    f"User {record.get('name', '')} - {user.get('email', record.get('email', ''))} - {record.get('orgRole', '')}"
                )
            elif stream_name == "projects" or stream_name == "project_detail":
                record["text_content"] = (
                    f"Project {record.get('name', '')} ({record.get('slug', '')}) - Platform: {record.get('platform', '')}"
                )
            elif stream_name == "release":
                record["text_content"] = (
                    f"Release {record.get('version', '')} - {record.get('shortVersion', '')} - Project: {record.get('project_slug', '')}"
                )
            elif stream_name == "issues":
                record["text_content"] = (
                    f"Issue {record.get('title', '')} - {record.get('culprit', '')} - {record.get('level', '')}"
                )
            elif stream_name == "events":
                record["text_content"] = (
                    f"Event {record.get('eventID', '')} - {record.get('title', '')} - Project: {record.get('project', {}).get('name', '')}"
                )
            else:
                # Generic fallback
                record["text_content"] = (
                    f"{stream_name.capitalize()} - {record.get('id', 'unknown')}"
                )

        # Convert integer IDs to strings if needed
        if "id" in record and isinstance(record["id"], int):
            record["id"] = str(record["id"])

        return record

    def get_bookmark(self, stream, key=None, default=None):
        """Get bookmark with validation."""
        if key:
            bookmark = singer.get_bookmark(self.state, stream, key, default)
        else:
            bookmark = singer.get_bookmark(self.state, stream, default)

        # Validate bookmark format
        if bookmark and isinstance(bookmark, dict):
            if "start" in bookmark:
                try:
                    # Validate date-time format
                    singer.utils.strptime(bookmark["start"])
                except ValueError:
                    LOGGER.warning(
                        f"Invalid date-time format in bookmark for {stream}:{key}"
                    )
                    return default
        return bookmark

    def write_bookmark(self, stream, key, value, metadata=None):
        """Write bookmark with metadata."""
        if not isinstance(value, dict):
            value = {"start": value}

        # Add metadata if provided
        if metadata:
            value["metadata"] = metadata

        # Validate date-time format
        if "start" in value:
            try:
                singer.utils.strptime(value["start"])
            except ValueError:
                LOGGER.error(f"Invalid date-time format for bookmark {stream}:{key}")
                return self.state

        if key:
            self.state = singer.write_bookmark(self.state, stream, key, value)
        else:
            self.state = singer.write_bookmark(self.state, stream, value)
        return self.state


def format_state_summary(state):
    """Format state data into a readable summary."""
    summary = {
        "Sync Summary": {
            "Last Sync": state.get("metadata", {}).get("last_sync"),
            "Sync Type": state.get("metadata", {}).get("sync_type"),
            "Streams Synced": state.get("metadata", {}).get("streams_synced", []),
            "Projects Processed": len(
                state.get("metadata", {}).get("projects_processed", [])
            ),
            "Total Events Processed": state.get("metadata", {}).get(
                "events_processed", 0
            ),
            "Total Issues Processed": state.get("metadata", {}).get(
                "issues_processed", 0
            ),
        },
        "Bookmarks": {
            stream: {"start": bookmark.get("start", "Not set")}
            for stream, bookmark in state.get("bookmarks", {}).items()
        },
    }

    # Add issue event counts if available
    issue_counts = state.get("metadata", {}).get("issue_event_counts", {})
    if issue_counts:
        top_issues = sorted(issue_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        summary["Top Issues by Event Count"] = {
            issue_id: count for issue_id, count in top_issues
        }

    return summary


def main():
    """Main function to run the tap."""
    # Import utils at the top of the file
    import utils

    args = utils.parse_args()

    # Initialize state with enhanced bookmark structure
    state = args.state or {}
    if not state:
        state = {
            "bookmarks": {
                "issues": {"start": args.start_date},
                "events": {"start": args.start_date},
                "teams": {"start": args.start_date},
                "users": {"start": args.start_date},
                "release": {"start": args.start_date},
                "projects": {"start": args.start_date},
            },
            "metadata": {
                "last_sync": None,
                "sync_type": "initial",
                "streams_synced": [],
                "projects_processed": [],
                "events_processed": 0,
                "issues_processed": 0,
                "issue_event_counts": {},
            },
        }

    # Initialize client with state
    client = SentryClient(
        args.api_key,
        args.organization,
        state=state,
        projects=args.projects,
        fetch_event_details=args.fetch_event_details,
    )

    # Run sync
    try:
        client.sync_all()
    except Exception as e:
        LOGGER.error(f"Error during sync: {str(e)}")
        raise

    # Format and print state summary instead of writing raw state
    state_summary = format_state_summary(state)
    LOGGER.info("=== Final State Summary ===")
    LOGGER.info(json.dumps(state_summary, indent=2))

    # Still write the state file for persistence
    singer.write_state(state)


if __name__ == "__main__":
    main()
