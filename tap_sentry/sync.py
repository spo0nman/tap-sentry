import os
import json
import asyncio
import urllib
from pathlib import Path
from itertools import repeat

from singer import Schema
from urllib.parse import urljoin

import pytz
import singer
import requests
import pendulum
from singer.bookmarks import write_bookmark, get_bookmark

LOGGER = singer.get_logger()

class SentryAuthentication(requests.auth.AuthBase):
    def __init__(self, api_token: str):
        self.api_token = api_token

    def __call__(self, req):
        req.headers.update({"Authorization": " Bearer " + self.api_token})

        return req


class SentryClient:
    def __init__(self, auth: SentryAuthentication, url="https://sentry.io/api/0/", organization="split-software"):
        self._base_url = url
        self._auth = auth
        self._session = None
        self._organization = organization
        LOGGER.debug(f"Initialized SentryClient with base URL: {url} and organization: {organization}")

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
        response = self.session.get(url, params=params)
        LOGGER.debug(f"Response status: {response.status_code}")
        response.raise_for_status()

        return response

    def projects(self):
        try:
            url_path = f"/organizations/{self._organization}/projects/"
            full_url = self._base_url + url_path
            
            # Keep the original debug message for backward compatibility with tests
            LOGGER.debug(f"Fetching projects from: {full_url}")
            
            # And keep our new info message for better production logging
            LOGGER.info(f"Fetching projects from URL: {full_url}")
            
            try:
                LOGGER.debug(f"Making request with headers: {self.session.headers}")
                projects = self._get(url_path)
                result = projects.json()
                LOGGER.info(f"Projects API response status: {projects.status_code}, found {len(result)} projects")
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
            query = f"/organizations/{self._organization}/issues/?project={project_id}"
            if bookmark:
                query += "&start=" + urllib.parse.quote(bookmark) + "&utc=true" + '&end=' + urllib.parse.quote(singer.utils.strftime(singer.utils.now()))
            LOGGER.debug(f"Fetching issues from: {self._base_url + query}")
            response = self._get(query)
            issues = response.json()
            url= response.url
            LOGGER.debug(f"Initial issues response URL: {url}")
            while (response.links is not None and response.links.__len__() >0  and response.links['next']['results'] == 'true'):
                url = response.links['next']['url']
                LOGGER.debug(f"Fetching next page of issues from: {url}")
                response = self.session.get(url)
                issues += response.json()
            return issues

        except Exception as e:
            LOGGER.debug(f"Error fetching issues: {str(e)}")
            return None

    def events(self, project_id, state):
        try:
            bookmark = get_bookmark(state, "events", "start")
            query = f"/organizations/{self._organization}/events/?project={project_id}"
            if bookmark:
                query += "&start=" + urllib.parse.quote(bookmark) + "&utc=true" + '&end=' + urllib.parse.quote(singer.utils.strftime(singer.utils.now()))
            LOGGER.debug(f"Fetching events from: {self._base_url + query}")
            response = self._get(query)
            events = response.json()
            url= response.url
            LOGGER.debug(f"Initial events response URL: {url}")
            while (response.links is not None and response.links.__len__() >0  and response.links['next']['results'] == 'true'):
                url = response.links['next']['url']
                LOGGER.debug(f"Fetching next page of events from: {url}")
                response = self.session.get(url)
                events += response.json()
            return events
        except Exception as e:
            LOGGER.debug(f"Error fetching events: {str(e)}")
            return None

    def teams(self, state):
        try:
            url_path = f"/organizations/{self._organization}/teams/"
            LOGGER.debug(f"Fetching teams from: {self._base_url + url_path}")
            response = self._get(url_path)
            teams = response.json()
            extraction_time = singer.utils.now()
            while (response.links is not None and response.links.__len__() >0  and  response.links['next']['results'] == 'true'):
                url = response.links['next']['url']
                LOGGER.debug(f"Fetching next page of teams from: {url}")
                response = self.session.get(url)
                teams += response.json()
            return teams
        except Exception as e:
            LOGGER.debug(f"Error fetching teams: {str(e)}")
            return None

    def users(self, state):
        try:
            url_path = f"/organizations/{self._organization}/users/"
            LOGGER.debug(f"Fetching users from: {self._base_url + url_path}")
            response = self._get(url_path)
            users = response.json()
            return users
        except Exception as e:
            LOGGER.debug(f"Error fetching users: {str(e)}")
            return None

    def releases(self, project_id, state):
        try:
            bookmark = get_bookmark(state, "releases", "start")
            query = f"/organizations/{self._organization}/releases/?project={project_id}"
            if bookmark:
                query += "&start=" + urllib.parse.quote(bookmark) + "&utc=true" + '&end=' + urllib.parse.quote(singer.utils.strftime(singer.utils.now()))
            LOGGER.debug(f"Fetching releases from: {self._base_url + query}")
            response = self._get(query)
            releases = response.json()
            url = response.url
            LOGGER.debug(f"Initial releases response URL: {url}")
            while (response.links is not None and response.links.__len__() > 0 and response.links['next']['results'] == 'true'):
                url = response.links['next']['url']
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
                    self.logger.error(f"Error fetching data from {url}: {response.status}")
                    return None
        except Exception as e:
            self.logger.error(f"Exception fetching data from {url}: {str(e)}")
            return None


class SentrySync:
    def __init__(self, client: SentryClient, state={}):
        self._client = client
        self._state = state
        self.projects = self.client.projects()

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
        """Sync issues from Sentry API."""
        with singer.metrics.job_timer(job_type=f"sync_{stream}"):
            # Fix schema format
            schema_dict = self._get_formatted_schema(schema)
            singer.write_schema(stream, schema_dict, ["id"])
            
            extraction_time = singer.utils.now()
            if self.projects:
                for project in self.projects:
                    issues = await asyncio.get_event_loop().run_in_executor(None, self.client.issues, project['id'], self.state)
                    if (issues):
                        for issue in issues:
                            singer.write_record(stream, issue)

            self.state = singer.write_bookmark(self.state, 'issues', 'start', singer.utils.strftime(extraction_time))

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

            self.state = singer.write_bookmark(self.state, 'projects', 'start', singer.utils.strftime(extraction_time))

    async def sync_events(self, schema, stream):
        """Sync events from Sentry API."""
        with singer.metrics.job_timer(job_type=f"sync_{stream}"):
            # Fix schema format
            schema_dict = self._get_formatted_schema(schema)
            singer.write_schema(stream, schema_dict, ["eventID"])
            
            extraction_time = singer.utils.now()
            if self.projects:
                # Create a new event loop for async operations
                loop = asyncio.get_event_loop()
                
                projects_to_process = self.projects
                for project in projects_to_process:
                    LOGGER.info(f"Syncing events for project {project.get('slug', project.get('id'))}")
                    try:
                        events = await loop.run_in_executor(None, self.client.events, project['id'], self.state)
                        if events:
                            for event in events:
                                singer.write_record(stream, event)
                    except Exception as e:
                        LOGGER.error(f"Error syncing events for project {project.get('slug', project.get('id'))}: {e}")
                
                self.state = singer.write_bookmark(self.state, 'events', 'start', singer.utils.strftime(extraction_time))

    async def sync_users(self, schema, stream):
        """Sync users from Sentry API."""
        with singer.metrics.job_timer(job_type=f"sync_{stream}"):
            # Fix schema format
            schema_dict = self._get_formatted_schema(schema)
            singer.write_schema(stream, schema_dict, ["id"])
            
            # Get users data from the client
            LOGGER.info(f"Fetching users data")
            try:
                users = await asyncio.get_event_loop().run_in_executor(
                    None, self.client.users, self.state
                )
                
                if users:
                    LOGGER.info(f"Found {len(users)} users to sync")
                    # Process each user
                    for user in users:
                        singer.write_record(stream, user)
                        singer.metrics.record_counter(stream).increment()
                else:
                    LOGGER.warning("No users found to sync")
            except Exception as e:
                LOGGER.error(f"Error syncing users: {e}")

    async def sync_teams(self, schema, stream):
        """Sync teams from Sentry API."""
        with singer.metrics.job_timer(job_type=f"sync_{stream}"):
            # Fix schema format
            schema_dict = self._get_formatted_schema(schema)
            singer.write_schema(stream, schema_dict, ["id"])
            
            # Get teams data from the client
            LOGGER.info(f"Fetching teams data")
            try:
                teams = await asyncio.get_event_loop().run_in_executor(
                    None, self.client.teams, self.state
                )
                
                if teams:
                    LOGGER.info(f"Found {len(teams)} teams to sync")
                    # Process each team
                    for team in teams:
                        # Process the record to add text_content and handle ID conversion
                        processed_team = self.process_record(stream, team)
                        singer.write_record(stream, processed_team)
                        singer.metrics.record_counter(stream).increment()
                else:
                    LOGGER.warning("No teams found to sync")
            except Exception as e:
                LOGGER.error(f"Error syncing teams: {e}")

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
                        if 'project_id' not in release:
                            release['project_id'] = project_id
                        if 'project_slug' not in release:
                            release['project_slug'] = project_slug
                        
                        # Write the record
                        processed_release = self.process_record(stream, release)
                        singer.write_record(stream, processed_release)
                        singer.metrics.record_counter(stream).increment()
            
            # Update state with extraction time
            self.state = singer.write_bookmark(self.state, stream, 'start', singer.utils.strftime(extraction_time))
        else:
            LOGGER.warning(f"No projects found for fetching releases")

    async def sync_project_detail(self, schema, stream):
        """Sync detailed project information from Sentry API."""
        with singer.metrics.job_timer(job_type=f"sync_{stream}"):
            # Fix schema format - ensure it's not nested under 'type'
            schema_dict = schema.to_dict()
            if 'type' in schema_dict and isinstance(schema_dict['type'], dict) and 'properties' in schema_dict['type']:
                # Schema is incorrectly nested under 'type', so extract it
                schema_dict = schema_dict['type']
            
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
                    org_slug = project.get("organization", {}).get("slug", self.client._organization)
                    
                    LOGGER.info(f"Syncing details for project {project_slug}")
                    LOGGER.debug(f"Project details - ID: {project_id}, slug: {project_slug}, org: {org_slug}")
                    
                    # Try to get detailed info
                    try:
                        detail_url = f"/projects/{org_slug}/{project_slug}/"
                        LOGGER.debug(f"Fetching project details from: {self.client._base_url + detail_url}")
                        response = await asyncio.get_event_loop().run_in_executor(
                            None, self.client._get, detail_url
                        )
                        
                        if response and response.status_code == 200:
                            project_detail = response.json()
                            project_detail['organization_slug'] = org_slug
                            
                            # Write record and increment counter
                            singer.write_record(stream, project_detail)
                            singer.metrics.record_counter(stream).increment()
                            LOGGER.info(f"Successfully synced project detail for {project_slug}")
                        else:
                            LOGGER.warning(f"Failed to get details for project {project_slug}, falling back to basic project info")
                            # Fallback to basic project info
                            project['organization_slug'] = org_slug
                            singer.write_record(stream, project)
                            singer.metrics.record_counter(stream).increment()
                    except Exception as e:
                        LOGGER.error(f"Error fetching details for project {project_slug}: {e}")
                        LOGGER.debug(f"Exception details: {str(e)}")
                        # Still try to use the basic project info
                        project['organization_slug'] = org_slug
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
        if 'type' in schema_dict and isinstance(schema_dict['type'], dict) and 'properties' in schema_dict['type']:
            # Schema is incorrectly nested under 'type', so extract it
            schema_dict = schema_dict['type']
        return schema_dict

    def process_record(self, stream_name, record):
        """Add text_content field to record for embedding/search."""
        # Only process if text_content isn't already set
        if 'text_content' not in record or not record['text_content']:
            if stream_name == "teams":
                record["text_content"] = f"Team {record.get('name', '')} ({record.get('slug', '')})"
            elif stream_name == "users":
                user = record.get("user", {})
                record["text_content"] = f"User {record.get('name', '')} - {user.get('email', record.get('email', ''))} - {record.get('orgRole', '')}"
            elif stream_name == "projects" or stream_name == "project_detail":
                record["text_content"] = f"Project {record.get('name', '')} ({record.get('slug', '')}) - Platform: {record.get('platform', '')}"
            elif stream_name == "release":
                record["text_content"] = f"Release {record.get('version', '')} - {record.get('shortVersion', '')} - Project: {record.get('project_slug', '')}"
            elif stream_name == "issues":
                record["text_content"] = f"Issue {record.get('title', '')} - {record.get('culprit', '')} - {record.get('level', '')}"
            elif stream_name == "events":
                record["text_content"] = f"Event {record.get('eventID', '')} - {record.get('title', '')} - Project: {record.get('project', {}).get('name', '')}"
            else:
                # Generic fallback
                record["text_content"] = f"{stream_name.capitalize()} - {record.get('id', 'unknown')}"
        
        # Convert integer IDs to strings if needed
        if 'id' in record and isinstance(record['id'], int):
            record['id'] = str(record['id'])
        
        return record