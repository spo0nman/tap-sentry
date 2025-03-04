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
    def __init__(self, auth: SentryAuthentication, url="https://sentry.io/api/0/"):
        self._base_url = url
        self._auth = auth
        self._session = None

    @property
    def session(self):
        if not self._session:
            self._session = requests.Session()
            self._session.auth = self._auth
            self._session.headers.update({"Accept": "application/json"})

        return self._session

    def _get(self, path, params=None):
        #url = urljoin(self._base_url, path)
        url = self._base_url + path
        response = self.session.get(url, params=params)
        response.raise_for_status()

        return response

    def projects(self):
        try:
            projects = self._get(f"/organizations/split-software/projects/")
            return projects.json()
        except:
            return None

    def issues(self, project_id, state):
        try:
            bookmark = get_bookmark(state, "issues", "start")
            query = f"/organizations/split-software/issues/?project={project_id}"
            if bookmark:
                query += "&start=" + urllib.parse.quote(bookmark) + "&utc=true" + '&end=' + urllib.parse.quote(singer.utils.strftime(singer.utils.now()))
            response = self._get(query)
            issues = response.json()
            url= response.url
            while (response.links is not None and response.links.__len__() >0  and response.links['next']['results'] == 'true'):
                url = response.links['next']['url']
                response = self.session.get(url)
                issues += response.json()
            return issues

        except:
            return None

    def events(self, project_id, state):
        try:
            bookmark = get_bookmark(state, "events", "start")
            query = f"/organizations/split-software/events/?project={project_id}"
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
            response = self._get(f"/organizations/split-software/teams/")
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
            response = self._get(f"/organizations/split-software/users/")
            users = response.json()
            return users
        except:
            return None

    def releases(self, project_id, state):
        try:
            bookmark = get_bookmark(state, "releases", "start")
            query = f"/organizations/split-software/releases/?project={project_id}"
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

    async def sync_issues(self, schema, period=None):
        """Issues per project."""
        stream = "issues"
        loop = asyncio.get_event_loop()

        singer.write_schema(stream, schema.to_dict(), ["id"])
        extraction_time = singer.utils.now()
        if self.projects:
            for project in self.projects:
                issues = await loop.run_in_executor(None, self.client.issues, project['id'], self.state)
                if (issues):
                    for issue in issues:
                        singer.write_record(stream, issue)

        self.state = singer.write_bookmark(self.state, 'issues', 'start', singer.utils.strftime(extraction_time))

    async def sync_projects(self, schema):
        """Issues per project."""
        stream = "projects"
        loop = asyncio.get_event_loop()
        singer.write_schema('projects', schema.to_dict(), ["id"])
        if self.projects:
            for project in self.projects:
                singer.write_record(stream, project)


    async  def sync_events(self, schema, period=None):
        """Events per project."""
        stream = "events"
        loop = asyncio.get_event_loop()

        singer.write_schema(stream, schema.to_dict(), ["eventID"])
        extraction_time = singer.utils.now()
        if self.projects:
            for project in self.projects:
                events = await loop.run_in_executor(None, self.client.events, project['id'], self.state)
                if events:
                    for event in events:
                        singer.write_record(stream, event)
            self.state = singer.write_bookmark(self.state, 'events', 'start', singer.utils.strftime(extraction_time))

    async def sync_users(self, schema):
        "Users in the organization."
        stream = "users"
        loop = asyncio.get_event_loop()
        singer.write_schema(stream, schema.to_dict(), ["id"])
        users = await loop.run_in_executor(None, self.client.users, self.state)
        if users:
            for user in users:
                singer.write_record(stream, user)
        #extraction_time = singer.utils.now()
        #self.state = singer.write_bookmark(self.state, 'users', 'dateCreated', singer.utils.strftime(extraction_time))

    async def sync_teams(self, schema):
        "Teams in the organization."
        stream = "teams"
        loop = asyncio.get_event_loop()
        singer.write_schema(stream, schema.to_dict(), ["id"])
        teams = await loop.run_in_executor(None, self.client.teams, self.state)
        if teams:
            for team in teams:
                singer.write_record(stream, team)
        #extraction_time = singer.utils.now()
        #self.state = singer.write_bookmark(self.state, 'teams', 'dateCreated', singer.utils.strftime(extraction_time))

    async def sync_release(self, schema, stream="release"):
        """Sync release data from Sentry API."""
        LOGGER.info(f"Syncing {stream}")
        
        singer.write_schema(stream, schema.to_dict(), ["version"])
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
                        
            # Update state with extraction time
            self.state = singer.write_bookmark(self.state, stream, 'start', singer.utils.strftime(extraction_time))

    async def sync_project_detail(self, schema, stream):
        """Sync detailed project information from Sentry API."""
        with singer.metrics.job_timer(job_type=f"sync_{stream}"):
            singer.write_schema(stream, schema.to_dict(), ["id"])
            
            # More robust handling of None projects
            projects_to_process = []
            
            # Try to get projects if self.projects is None
            if not self.projects:
                LOGGER.warning("No projects found to sync details for")
                try:
                    self.projects = self.client.projects()
                except Exception as e:
                    LOGGER.error(f"Error fetching projects: {e}")
            
            # Always use a list (even if empty) to avoid None iteration errors
            if self.projects:
                projects_to_process = self.projects
            else:
                LOGGER.warning("No projects available to process")
            
            # Now iterate over the list (which might be empty, but never None)
            for project in projects_to_process:
                project_id = project.get("id")
                project_slug = project.get("slug")
                org_slug = project.get("organization", {}).get("slug", "split-software")
                
                # Get more detailed project information
                project_detail = None
                try:
                    # Use the client to get project details if needed
                    detail_url = f"/projects/{org_slug}/{project_slug}/"
                    project_detail = await asyncio.get_event_loop().run_in_executor(
                        None, self.client._get, detail_url
                    )
                    if project_detail and project_detail.status_code == 200:
                        project_detail = project_detail.json()
                    else:
                        # Fallback to using the project data we already have
                        project_detail = project
                except Exception as e:
                    LOGGER.error(f"Error getting project details: {e}")
                    # Fallback to using the project data we already have
                    project_detail = project
                
                # Add organization context
                if project_detail:
                    project_detail['organization_slug'] = org_slug
                    
                    # Write the project detail record
                    singer.write_record(stream, project_detail)
                    singer.metrics.record_counter(stream).increment()