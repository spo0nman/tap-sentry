import asyncio
import urllib
import singer
import requests
from singer.bookmarks import get_bookmark

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
    ):
        self._base_url = url
        self._auth = auth
        self._session = None
        self._organization = organization
        LOGGER.debug(
            f"Initialized SentryClient with base URL: {url} and organization: {organization}"
        )

    @property
    def session(self):
        if not self._session:
            self._session = requests.Session()
            self._session.auth = self._auth
            self._session.headers.update({"Accept": "application/json"})

        return self._session

    def _get(self, path, params=None):
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

            # Keep the original debug message for backward compatibility with tests
            LOGGER.debug(f"Fetching projects from: {full_url}")

            # And keep our new info message for better production logging
            LOGGER.info(f"Fetching projects from URL: {full_url}")

            try:
                LOGGER.debug(f"Making request with headers: {self.session.headers}")
                projects = self._get(url_path)
                result = projects.json()
                LOGGER.info(
                    f"Projects API response status: {projects.status_code}, found {len(result)} projects"
                )
                return result
            except Exception as e:
                LOGGER.error(f"Error making request to {full_url}: {str(e)}")
                return None
        except Exception as e:
            LOGGER.debug(f"Error fetching projects: {str(e)}")
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
                        LOGGER.info(
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
            LOGGER.info(f"Making request to issues endpoint: {self._base_url + query}")
            LOGGER.debug(f"With parameters: {params}")

            response = self._get(query, params=params)
            LOGGER.info(f"Issues API response status: {response.status_code}")
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
                LOGGER.info(f"Found {len(new_issues)} additional issues on next page")
                issues += new_issues
            LOGGER.info(f"Total issues found: {len(issues)}")
            return issues

        except Exception as e:
            LOGGER.error(f"Error fetching issues: {str(e)}")
            LOGGER.debug(f"Exception details: {str(e)}")
            return None

    def event_detail(self, organization_slug, project_slug, event_id):
        """Fetch detailed information for a specific event.

        Args:
            organization_slug: The organization slug
            project_slug: The project slug
            event_id: The specific event ID to fetch details for

        Returns:
            Detailed event data or None if there's an error
        """
        try:
            # Construct the URL for event detail endpoint
            path = SENTRY_API_ENDPOINTS["event_detail"].format(
                organization_slug=organization_slug,
                project_slug=project_slug,
                event_id=event_id,
            )
            LOGGER.info(f"Fetching event detail from: {self._base_url + path}")

            response = self._get(path)
            if response.status_code == 200:
                LOGGER.info(f"Successfully fetched detailed event data for {event_id}")
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
            LOGGER.info(f"Starting events fetch with bookmark: {bookmark}")

            # Use project slug instead of project ID
            project_slug = None
            projects_list = self.projects()
            if projects_list:
                for project in projects_list:
                    if project["id"] == project_id:
                        project_slug = project["slug"]
                        LOGGER.info(
                            f"Found project slug '{project_slug}' for project ID {project_id}"
                        )
                        break

            if not project_slug:
                LOGGER.error(
                    f"Could not find project slug for project ID: {project_id}"
                )
                return None

            # Use the correct URL structure from SENTRY_API_ENDPOINTS
            query = SENTRY_API_ENDPOINTS["project_events"].format(
                organization_slug=self._organization, project_slug=project_slug
            )

            # Add query parameters
            params = {}
            if bookmark:
                params["start"] = bookmark
                params["utc"] = "true"
                params["end"] = singer.utils.strftime(singer.utils.now())

            LOGGER.info(f"Making request to events endpoint: {self._base_url + query}")
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
                LOGGER.info(f"Fetching page {page_count} of events from: {url}")
                response = self.session.get(url)
                new_events = response.json()
                events += new_events
                LOGGER.info(f"Added {len(new_events)} events from page {page_count}")

            LOGGER.info(f"Total events fetched: {len(events)}")
            return events

        except Exception as e:
            LOGGER.error(f"Error fetching events: {str(e)}")
            return None

    def issue_events(self, organization_slug, issue_id, state):
        """Fetch events for a specific issue.

        Args:
            organization_slug: The organization slug
            issue_id: The issue ID to fetch events for
            state: The current state for bookmarks

        Returns:
            List of events for the issue or None if there's an error
        """
        try:
            bookmark = get_bookmark(state, "issue_events", "start")
            LOGGER.info(
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
                    LOGGER.info(
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

            # Now construct the URL for issue events using the correct endpoint
            query = SENTRY_API_ENDPOINTS["issue_events"].format(
                organization_slug=organization_slug, issue_id=issue_id
            )

            # Add query parameters
            params = {}
            if bookmark:
                params["start"] = bookmark
                params["utc"] = "true"
                params["end"] = singer.utils.strftime(singer.utils.now())

            LOGGER.info(
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
                LOGGER.info(
                    f"Fetching page {page_count} of events for issue {issue_id} from: {url}"
                )
                response = self.session.get(url)
                new_events = response.json()
                events += new_events
                LOGGER.info(f"Added {len(new_events)} events from page {page_count}")

            LOGGER.info(f"Total events fetched for issue {issue_id}: {len(events)}")
            return events

        except Exception as e:
            LOGGER.error(f"Error fetching events for issue {issue_id}: {str(e)}")
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
            query = (
                f"/organizations/{self._organization}/releases/?project={project_id}"
            )
            if bookmark:
                query += (
                    "&start="
                    + urllib.parse.quote(bookmark)
                    + "&utc=true"
                    + "&end="
                    + urllib.parse.quote(singer.utils.strftime(singer.utils.now()))
                )
            LOGGER.debug(f"Fetching releases from: {self._base_url + query}")
            response = self._get(query)
            releases = response.json()
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
                releases += response.json()
            return releases
        except Exception as e:
            LOGGER.debug(f"Error fetching releases: {str(e)}")
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

                for project in self.projects:
                    project_id = project.get("id")
                    project_slug = project.get("slug")
                    LOGGER.info(f"Processing project {project_slug} (ID: {project_id})")

                    try:
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
                                        f"Fetching events for issue {issue_id} in project {project_slug}"
                                    )
                                    issue_events = await loop.run_in_executor(
                                        None,
                                        self.client.issue_events,
                                        self.client._organization,
                                        issue_id,
                                        self.state,
                                    )

                                    if issue_events:
                                        LOGGER.info(
                                            f"Found {len(issue_events)} events for issue {issue_id}"
                                        )
                                        for event in issue_events:
                                            event_id = event.get("id") or event.get(
                                                "eventID"
                                            )
                                            LOGGER.info(
                                                f"Processing event {event_id} from issue {issue_id}"
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
                                                    LOGGER.info(
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
                                                        LOGGER.info(
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
                                    else:
                                        LOGGER.warning(
                                            f"No events found for issue {issue_id} in project {project_slug}"
                                        )
                        else:
                            LOGGER.warning(
                                f"No issues found for project {project_slug}"
                            )
                    except Exception as e:
                        LOGGER.error(
                            f"Error processing project {project_slug}: {str(e)}"
                        )
                        LOGGER.debug(f"Full exception details: {str(e)}", exc_info=True)

            # Update state for both issues and events
            self.state = singer.write_bookmark(
                self.state, "issues", "start", singer.utils.strftime(extraction_time)
            )
            self.state = singer.write_bookmark(
                self.state, "events", "start", singer.utils.strftime(extraction_time)
            )

    async def sync_projects(self, schema, stream):
        """Sync projects."""
        with singer.metrics.job_timer(job_type=f"sync_{stream}"):
            # Fix schema format
            schema_dict = self._get_formatted_schema(schema)
            singer.write_schema(stream, schema_dict, ["id"])

            extraction_time = singer.utils.now()
            if self.projects:
                for project in self.projects:
                    singer.write_record(stream, project)

            self.state = singer.write_bookmark(
                self.state, "projects", "start", singer.utils.strftime(extraction_time)
            )

    async def sync_events(self, schema, stream):
        """Sync events from Sentry API by fetching them through issues."""
        LOGGER.info("Starting sync_events method")
        with singer.metrics.job_timer(job_type=f"sync_{stream}"):
            # Fix schema format
            schema_dict = self._get_formatted_schema(schema)
            singer.write_schema(stream, schema_dict, ["eventID"])

            extraction_time = singer.utils.now()
            if self.projects:
                LOGGER.info(f"Found {len(self.projects)} projects to process")
                # Create a new event loop for async operations
                loop = asyncio.get_event_loop()

                projects_to_process = self.projects
                for project in projects_to_process:
                    project_id = project.get("id")
                    project_slug = project.get("slug")
                    LOGGER.info(f"Processing project {project_slug} (ID: {project_id})")

                    try:
                        # First, get all issues for this project
                        LOGGER.info(
                            f"Fetching issues for project {project_slug} with state: {self.state}"
                        )
                        issues = await loop.run_in_executor(
                            None, self.client.issues, project_id, self.state
                        )

                        if issues:
                            LOGGER.info(
                                f"Found {len(issues)} issues for project {project_slug}"
                            )
                            for issue in issues:
                                issue_id = issue.get("id")
                                if not issue_id:
                                    LOGGER.warning(
                                        f"Skipping issue with no ID: {issue}"
                                    )
                                    continue

                                LOGGER.info(
                                    f"Fetching events for issue {issue_id} in project {project_slug}"
                                )
                                issue_events = await loop.run_in_executor(
                                    None,
                                    self.client.issue_events,
                                    self.client._organization,
                                    issue_id,
                                    self.state,
                                )

                                if issue_events:
                                    LOGGER.info(
                                        f"Found {len(issue_events)} events for issue {issue_id}"
                                    )
                                    for event in issue_events:
                                        event_id = event.get("id") or event.get(
                                            "eventID"
                                        )
                                        LOGGER.info(
                                            f"Processing event {event_id} from issue {issue_id}"
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
                                                LOGGER.info(
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
                                                    LOGGER.info(
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
                                        singer.write_record(stream, event)
                                else:
                                    LOGGER.warning(
                                        f"No events found for issue {issue_id} in project {project_slug}"
                                    )
                        else:
                            LOGGER.warning(
                                f"No issues found for project {project_slug}"
                            )
                    except Exception as e:
                        LOGGER.error(
                            f"Error processing project {project_slug}: {str(e)}"
                        )
                        LOGGER.debug(f"Full exception details: {str(e)}", exc_info=True)

                self.state = singer.write_bookmark(
                    self.state,
                    "events",
                    "start",
                    singer.utils.strftime(extraction_time),
                )
                LOGGER.info("Updated state bookmark for events stream")
            else:
                LOGGER.warning("No projects found to sync events from")

    async def sync_teams(self, schema, stream):
        """Sync teams from Sentry API."""
        with singer.metrics.job_timer(job_type=f"sync_{stream}"):
            # Fix schema format
            schema_dict = self._get_formatted_schema(schema)
            singer.write_schema(stream, schema_dict, ["id"])

            # Get teams data from the client
            LOGGER.info("Fetching teams data")
            extraction_time = singer.utils.now()

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

            try:
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

                        previous_role = previous_state.get("role")
                        previous_projects = sorted(previous_state.get("projects", []))
                        previous_flags = previous_state.get("flags", {})

                        # Only sync if any of the tracked fields have changed
                        if (
                            previous_role != current_role
                            or previous_projects != current_projects
                            or previous_flags != current_flags
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
                                },
                            )
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
            except Exception as e:
                LOGGER.error(f"Error syncing users: {e}")

    async def sync_release(self, schema, stream):
        """Sync release data from Sentry API."""
        LOGGER.info(f"Syncing {stream}")

        # Fix schema format
        schema_dict = self._get_formatted_schema(schema)
        singer.write_schema(stream, schema_dict, ["version"])
        extraction_time = singer.utils.now()

        # Use existing projects property
        if self.projects:
            for project in self.projects:
                project_id = project.get("id")
                project_slug = project.get("slug")

                LOGGER.info(f"Fetching releases for project {project_slug}")

                # Use the existing releases method in the client
                releases = await asyncio.get_event_loop().run_in_executor(
                    None, self.client.releases, project_id, self.state
                )

                if releases:
                    for release in releases:
                        # Add project context if not present
                        if "project_id" not in release:
                            release["project_id"] = project_id
                        if "project_slug" not in release:
                            release["project_slug"] = project_slug

                        # Write the record
                        processed_release = self.process_record(stream, release)
                        singer.write_record(stream, processed_release)
                        singer.metrics.record_counter(stream).increment()

            # Update state with extraction time
            self.state = singer.write_bookmark(
                self.state, stream, "start", singer.utils.strftime(extraction_time)
            )
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
