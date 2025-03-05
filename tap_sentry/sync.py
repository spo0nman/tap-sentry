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

import aiohttp
from datetime import datetime, timedelta
from dateutil import parser
from typing import Dict, List, Set, Optional, Any
import backoff
from . import utils
import logging

LOGGER = singer.get_logger()

class SentryAuthentication(requests.auth.AuthBase):
    def __init__(self, api_token: str):
        self.api_token = api_token

    def __call__(self, req):
        req.headers.update({"Authorization": " Bearer " + self.api_token})

        return req


class SentryClient:
    def __init__(self, auth_token, org_slug, base_url='https://sentry.io/api/0'):
        self.auth_token = auth_token
        self.org_slug = org_slug
        self.base_url = base_url  # This accepts custom base_url
        self.session = None

    async def _create_session(self):
        if self.session is None:
            LOGGER.debug(f"Creating new session with base_url: {self.base_url}")
            self.session = aiohttp.ClientSession(headers={
                'Authorization': f'Bearer {self.auth_token}',
                'Content-Type': 'application/json'
            })
            LOGGER.debug(f"Session headers: {self.session.headers}")

    def _get(self, path, params=None):
        # Create a URL by joining base_url and path
        url = self.base_url + path
        LOGGER.debug(f"Making GET request to: {url}")
        LOGGER.debug(f"Request parameters: {params}")
        
        try:
            response = self.session.get(url, params=params)
            LOGGER.debug(f"Response status code: {response.status_code}")
            
            # Log response headers
            LOGGER.debug(f"Response headers: {dict(response.headers)}")
            
            # Try to log response body (truncated if too large)
            try:
                response_text = response.text
                if len(response_text) > 1000:
                    LOGGER.debug(f"Response body (truncated): {response_text[:1000]}...")
                else:
                    LOGGER.debug(f"Response body: {response_text}")
            except Exception as e:
                LOGGER.debug(f"Could not log response body: {str(e)}")
            
            # Raise for any HTTP errors
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            LOGGER.error(f"HTTP request failed: {str(e)}")
            raise

    async def projects(self):
        await self._create_session()
        url = f"{self.base_url}/organizations/{self.org_slug}/projects/"
        LOGGER.debug(f"Requesting projects from URL: {url}")
        
        try:
            response = await self.session.get(url)
            status = response.status
            LOGGER.debug(f"Projects API response status: {status}")
            
            if status != 200:
                error_text = await response.text()
                LOGGER.error(f"Failed to fetch projects: Status {status}, Response: {error_text}")
                return []
            
            response_json = await response.json()
            LOGGER.debug(f"Projects API returned {len(response_json)} projects")
            for project in response_json:
                LOGGER.debug(f"Project: {project.get('slug')} (ID: {project.get('id')})")
            return response_json
        except Exception as e:
            LOGGER.error(f"Exception while fetching projects: {str(e)}")
            return []

    async def issues(self, project_slug, start_date=None, end_date=None):
        await self._create_session()
        url = f"{self.base_url}/projects/{self.org_slug}/{project_slug}/issues/"
        
        params = {}
        if start_date:
            params['start'] = start_date.isoformat() if isinstance(start_date, datetime) else start_date
        if end_date:
            params['end'] = end_date.isoformat() if isinstance(end_date, datetime) else end_date
        
        LOGGER.debug(f"Requesting issues from URL: {url}")
        LOGGER.debug(f"Request parameters: {params}")
        
        try:
            response = await self.session.get(url, params=params)
            status = response.status
            LOGGER.debug(f"Issues API response status for {project_slug}: {status}")
            
            if status != 200:
                error_text = await response.text()
                LOGGER.error(f"Failed to fetch issues for {project_slug}: Status {status}, Response: {error_text}")
                return []
            
            response_json = await response.json()
            LOGGER.debug(f"Issues API for {project_slug} returned {len(response_json)} issues")
            return response_json
        except Exception as e:
            LOGGER.error(f"Exception while fetching issues for {project_slug}: {str(e)}")
            return []

    def events(self, project_id, state):
        try:
            bookmark = get_bookmark(state, "events", "start")
            query = f"/organizations/{self.org_slug}/events/?project={project_id}"
            if bookmark:
                query += "&start=" + urllib.parse.quote(bookmark) + "&utc=true" + '&end=' + urllib.parse.quote(singer.utils.strftime(singer.utils.now()))
            response = self._get(query)
            events = response.json()
            url= response.url
            while (response.links is not None and response.links.__len__() >0  and response.links['next']['results'] == 'true'):
                url = response.links['next']['url']
                response = self.session.get(url)
                events += response.json()
            return events
        except:
            return None

    def teams(self, state):
        try:
            response = self._get(f"/organizations/{self.org_slug}/teams/")
            teams = response.json()
            extraction_time = singer.utils.now()
            while (response.links is not None and response.links.__len__() >0  and  response.links['next']['results'] == 'true'):
                url = response.links['next']['url']
                response = self.session.get(url)
                teams += response.json()
            return teams
        except:
            return None

    def users(self, state):
        try:
            response = self._get(f"/organizations/{self.org_slug}/users/")
            users = response.json()
            return users
        except:
            return None

    def releases(self, project_id, state):
        try:
            bookmark = get_bookmark(state, "releases", "start")
            query = f"/organizations/{self.org_slug}/releases/?project={project_id}"
            if bookmark:
                query += "&start=" + urllib.parse.quote(bookmark) + "&utc=true" + '&end=' + urllib.parse.quote(singer.utils.strftime(singer.utils.now()))
            response = self._get(query)
            releases = response.json()
            url = response.url
            while (response.links is not None and response.links.__len__() > 0 and response.links['next']['results'] == 'true'):
                url = response.links['next']['url']
                response = self.session.get(url)
                releases += response.json()
            return releases
        except:
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

    async def sync_issues(self, schema, stream_name):
        """Sync issues data from Sentry."""
        LOGGER.debug(f"Starting sync for issues stream")
        with singer.metrics.job_timer(job_type="sync_issues"):
            # Write schema
            singer.write_schema(stream_name, schema.to_dict(), ["id"])
            
            # Get current time for bookmarking
            extraction_time = singer.utils.now()
            
            # Prepare start/end dates
            bookmark_date = get_bookmark(self.state, stream_name, "start")
            LOGGER.debug(f"Using bookmark date for issues: {bookmark_date}")
            
            # Check if we have any projects
            if not self.projects:
                try:
                    LOGGER.debug("No projects loaded yet, fetching projects")
                    self.projects = await self.client.projects()
                    LOGGER.debug(f"Found {len(self.projects)} projects")
                except Exception as e:
                    LOGGER.error(f"Error fetching projects: {str(e)}")
                    self.projects = []
            
            # Process each project
            if self.projects:
                for project in self.projects:
                    project_id = project.get("id")
                    project_slug = project.get("slug")
                    LOGGER.debug(f"Processing issues for project: {project_slug} (ID: {project_id})")
                    
                    try:
                        # Fetch issues for this project
                        issues = await self.client.issues(project_slug, bookmark_date)
                        
                        if issues:
                            LOGGER.debug(f"Found {len(issues)} issues for project {project_slug}")
                            for issue in issues:
                                # Add project context if not present
                                if 'project_id' not in issue and project_id:
                                    issue['project_id'] = project_id
                                if 'project_slug' not in issue and project_slug:
                                    issue['project_slug'] = project_slug
                                
                                # Write the record
                                singer.write_record(stream_name, issue)
                                singer.metrics.record_counter(stream_name).increment()
                        else:
                            LOGGER.debug(f"No issues found for project {project_slug}")
                    except Exception as e:
                        LOGGER.error(f"Error processing issues for project {project_slug}: {str(e)}")
                        continue
            else:
                LOGGER.warning("No projects found, skipping issues sync")
            
            # Update state with extraction time
            self.state = singer.write_bookmark(self.state, stream_name, 'start', singer.utils.strftime(extraction_time))
            singer.write_state(self.state)
            LOGGER.debug(f"Updated state for issues stream: {self.state}")

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
                        singer.write_record(stream, team)
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
                        singer.write_record(stream, release)
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
                    org_slug = project.get("organization", {}).get("slug", "split-software")
                    
                    LOGGER.info(f"Syncing details for project {project_slug}")
                    
                    # Try to get detailed info
                    try:
                        detail_url = f"/projects/{org_slug}/{project_slug}/"
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
                        # Still try to use the basic project info
                        project['organization_slug'] = org_slug
                        singer.write_record(stream, project)
                        singer.metrics.record_counter(stream).increment()
                        
                except Exception as e:
                    LOGGER.error(f"Error processing project: {e}")
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