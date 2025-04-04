import asyncio
import aiohttp
from datetime import datetime, timedelta
import os
from typing import Dict, List, Tuple
import json
import random
from tabulate import tabulate

# Global dictionary of Sentry API endpoints
SENTRY_API_ENDPOINTS = {
    "projects": "/organizations/{organization_slug}/projects/",
    "project_issues": "/projects/{organization_slug}/{project_slug}/issues/",
    "project_stats": "/projects/{organization_slug}/{project_slug}/stats/",
    "issue_detail": "/organizations/{organization_slug}/issues/{issue_id}/",
    "issue_events": "/organizations/{organization_slug}/issues/{issue_id}/events/",
    "project_events": "/projects/{organization_slug}/{project_slug}/events/",
    "event_detail": "/projects/{organization_slug}/{project_slug}/events/{event_id}/",
    "teams": "/organizations/{organization_slug}/teams/",
    "users": "/organizations/{organization_slug}/users/",
    "releases": "/organizations/{organization_slug}/releases/",
}


class SentryDataAnalyzer:
    def __init__(self, api_token: str, organization_slug: str):
        self.api_token = api_token
        self.organization_slug = organization_slug
        self.base_url = "https://sentry.io/api/0"
        self.headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
        }
        self.session = None
        self.semaphore = asyncio.Semaphore(
            20
        )  # Increased from 10 to 20 concurrent requests

    def _get_endpoint(self, endpoint_name: str, **kwargs) -> str:
        """Get the full API endpoint URL with parameters"""
        endpoint = SENTRY_API_ENDPOINTS[endpoint_name]
        return f"{self.base_url}{endpoint.format(organization_slug=self.organization_slug, **kwargs)}"

    async def _make_request(self, url: str, params: Dict = None) -> Dict:
        """Make an async HTTP request with rate limiting"""
        max_retries = 3
        retry_delay = 1  # seconds

        for attempt in range(max_retries):
            try:
                async with self.semaphore:  # This ensures we don't exceed 20 concurrent requests
                    async with self.session.get(
                        url, params=params, headers=self.headers
                    ) as response:
                        if response.status == 429:  # Too Many Requests
                            if attempt < max_retries - 1:
                                retry_after = int(
                                    response.headers.get("Retry-After", retry_delay)
                                )
                                print(
                                    f"Rate limited, waiting {retry_after} seconds before retry..."
                                )
                                await asyncio.sleep(retry_after)
                                continue
                        response.raise_for_status()
                        return await response.json()
            except aiohttp.ClientError:
                if attempt < max_retries - 1:
                    print(f"Request failed, retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                    continue
                raise
        raise Exception(f"Failed after {max_retries} retries")

    async def get_projects(self) -> List[Dict]:
        """Get all projects asynchronously"""
        url = self._get_endpoint("projects")
        return await self._make_request(url)

    async def get_project_issues(
        self, project_slug: str, days: int
    ) -> Tuple[int, List[str], str]:
        """Get issues count and IDs for a single project"""
        stats_period = "14d" if days > 14 else "24h"
        params = {"statsPeriod": stats_period, "status": "unresolved"}
        url = self._get_endpoint("project_issues", project_slug=project_slug)
        try:
            issues = await self._make_request(url, params)
            issue_ids = [issue["id"] for issue in issues]
            print(f"Found {len(issues)} issues for project {project_slug}")
            return len(issues), issue_ids, project_slug
        except Exception as e:
            print(f"Error fetching issues for project {project_slug}: {str(e)}")
            return 0, [], project_slug

    async def get_random_issue_details(
        self, project_slug: str, issue_id: str
    ) -> Tuple[int, Dict]:
        """Get details of a random issue and calculate its size"""
        url = self._get_endpoint("issue_detail", issue_id=issue_id)
        try:
            issue_details = await self._make_request(url)
            issue_size = len(json.dumps(issue_details).encode("utf-8"))
            return issue_size, issue_details
        except Exception as e:
            print(f"Error fetching issue details for {issue_id}: {str(e)}")
            return 0, {}

    async def get_project_events(
        self, project_slug: str, start_date: datetime, end_date: datetime
    ) -> Tuple[int, List[str], str]:
        """Get events count and IDs for a single project on a specific day"""
        params = {
            "resolution": "1d",
            "start": start_date.strftime("%Y-%m-%dT%H:%M:%S"),
            "end": end_date.strftime("%Y-%m-%dT%H:%M:%S"),
        }
        url = self._get_endpoint("project_stats", project_slug=project_slug)
        try:
            stats = await self._make_request(url, params)
            day_events = sum(count for _, count in stats)

            # Only fetch event IDs if we have events
            event_ids = []
            if day_events > 0:
                events_url = self._get_endpoint(
                    "project_events", project_slug=project_slug
                )
                events = await self._make_request(events_url, params)
                # Filter out events without eventID
                event_ids = [event["eventID"] for event in events if "eventID" in event]

            print(
                f"Found {day_events} events for project {project_slug} on {start_date.strftime('%Y-%m-%d')}"
            )
            return day_events, event_ids, project_slug
        except Exception as e:
            print(f"Error fetching stats for project {project_slug}: {str(e)}")
            return 0, [], project_slug

    async def get_random_event_details(
        self, project_slug: str, event_id: str
    ) -> Tuple[int, Dict]:
        """Get details of a random event and calculate its size"""
        # The event detail endpoint is different from other endpoints
        url = f"{self.base_url}/projects/{self.organization_slug}/{project_slug}/events/{event_id}/"
        try:
            event_details = await self._make_request(url)
            event_size = len(json.dumps(event_details).encode("utf-8"))
            return event_size, event_details
        except aiohttp.ClientError as e:
            print(f"Error fetching event details for {event_id}: {str(e)}")
            return 0, {}
        except Exception as e:
            print(f"Unexpected error fetching event details for {event_id}: {str(e)}")
            return 0, {}

    def format_size(self, size_bytes: int) -> str:
        """Format size in bytes to human readable format"""
        for unit in ["B", "KB", "MB", "GB"]:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} TB"

    async def get_issues_count(self, days: int = 5) -> Dict[str, int]:
        """Get count of issues for the last N days using parallel requests"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        # Get all projects
        projects = await self.get_projects()

        # Create tasks for all projects
        tasks = [self.get_project_issues(project["slug"], days) for project in projects]

        # Run all tasks concurrently with better batching
        results = []
        batch_size = 10  # Reduced batch size to avoid rate limiting
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i : i + batch_size]
            batch_results = await asyncio.gather(*batch, return_exceptions=True)
            for result in batch_results:
                if isinstance(result, Exception):
                    print(f"Error in batch: {str(result)}")
                    results.append((0, [], ""))
                else:
                    results.append(result)
            # Add a small delay between batches to avoid rate limiting
            if i + batch_size < len(tasks):
                await asyncio.sleep(1)

        total_issues = sum(count for count, _, _ in results)
        all_issue_ids = [
            issue_id for _, issue_ids, _ in results for issue_id in issue_ids
        ]

        # Get random issue details in parallel
        issue_tasks = []
        if all_issue_ids:
            for _ in range(3):  # Try 3 random issues
                random_issue_id = random.choice(all_issue_ids)
                random_project = random.choice(projects)
                issue_tasks.append(
                    self.get_random_issue_details(
                        random_project["slug"], random_issue_id
                    )
                )

        # Run issue detail tasks in parallel
        issue_results = await asyncio.gather(*issue_tasks, return_exceptions=True)
        avg_issue_size = 0
        for result in issue_results:
            if isinstance(result, Exception):
                continue
            issue_size, _ = result
            if issue_size > 0:
                avg_issue_size = issue_size
                break

        return {
            "total_issues": total_issues,
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "avg_issue_size": avg_issue_size,
        }

    async def get_events_count(self, days: int = 5) -> List[Dict]:
        """Get events count per day for the last N days using parallel requests"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        # Get all projects
        projects = await self.get_projects()

        daily_counts = []
        current_date = start_date

        while current_date <= end_date:
            next_date = current_date + timedelta(days=1)

            # Create tasks for all projects for this day
            tasks = [
                self.get_project_events(project["slug"], current_date, next_date)
                for project in projects
            ]

            # Run all tasks concurrently with better batching
            results = []
            batch_size = 20  # Process 20 projects at a time
            for i in range(0, len(tasks), batch_size):
                batch = tasks[i : i + batch_size]
                batch_results = await asyncio.gather(*batch)
                results.extend(batch_results)

            total_events = sum(count for count, _, _ in results)

            # Get event details only from projects that have events
            avg_event_size = 0
            projects_with_events = [
                (count, event_ids, project_slug)
                for count, event_ids, project_slug in results
                if count > 0 and event_ids
            ]

            if projects_with_events:
                # Try up to 3 random events from projects that have events
                event_tasks = []
                for _ in range(3):
                    count, event_ids, project_slug = random.choice(projects_with_events)
                    if event_ids:  # Double check we have event IDs
                        random_event_id = random.choice(event_ids)
                        event_tasks.append(
                            self.get_random_event_details(project_slug, random_event_id)
                        )

                # Run event detail tasks in parallel
                event_results = await asyncio.gather(*event_tasks)
                for event_size, _ in event_results:
                    if event_size > 0:
                        avg_event_size = event_size
                        break

            # Calculate sampled size (20% of events)
            sampled_events = int(total_events * 0.2)
            sampled_size = sampled_events * avg_event_size if avg_event_size > 0 else 0

            daily_counts.append(
                {
                    "date": current_date.strftime("%Y-%m-%d"),
                    "count": total_events,
                    "avg_event_size": avg_event_size,
                    "total_size": total_events * avg_event_size
                    if avg_event_size > 0
                    else 0,
                    "sampled_size": sampled_size,
                }
            )

            current_date = next_date

        return daily_counts

    def calculate_daily_increase(self, daily_counts: List[Dict]) -> List[Dict]:
        """Calculate daily increase in events"""
        result = []
        for i in range(1, len(daily_counts)):
            increase = daily_counts[i]["count"] - daily_counts[i - 1]["count"]
            result.append(
                {
                    "date": daily_counts[i]["date"],
                    "increase": increase,
                    "percentage_increase": (
                        increase / daily_counts[i - 1]["count"] * 100
                    )
                    if daily_counts[i - 1]["count"] > 0
                    else 0,
                }
            )
        return result

    async def __aenter__(self):
        """Async context manager entry"""
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()


async def main():
    # Get API token and organization slug from environment variables
    api_token = os.getenv("SENTRY_API_TOKEN")
    organization_slug = os.getenv("SENTRY_ORGANIZATION_SLUG")

    if not all([api_token, organization_slug]):
        print("Please set the following environment variables:")
        print("- SENTRY_API_TOKEN")
        print("- SENTRY_ORGANIZATION_SLUG")
        return

    async with SentryDataAnalyzer(api_token, organization_slug) as analyzer:
        # Get issues count
        issues_data = await analyzer.get_issues_count()
        print("\nIssues Analysis:")
        print(f"Total issues in the last 5 days: {issues_data['total_issues']}")
        print(f"Period: {issues_data['start_date']} to {issues_data['end_date']}")
        print(
            f"Average issue size: {analyzer.format_size(issues_data['avg_issue_size'])}"
        )

        # Get events count
        events_data = await analyzer.get_events_count()
        print("\nEvents Analysis:")

        # Create summary table
        table_data = []
        total_events = 0
        total_size = 0
        total_sampled_size = 0

        for day in events_data:
            total_events += day["count"]
            total_size += day["total_size"]
            total_sampled_size += day["sampled_size"]
            table_data.append(
                [
                    day["date"],
                    day["count"],
                    analyzer.format_size(day["avg_event_size"]),
                    analyzer.format_size(day["total_size"]),
                    analyzer.format_size(day["sampled_size"]),
                ]
            )

        # Add total row
        table_data.append(
            [
                "TOTAL",
                total_events,
                analyzer.format_size(
                    sum(day["avg_event_size"] for day in events_data) / len(events_data)
                    if events_data
                    else 0
                ),
                analyzer.format_size(total_size),
                analyzer.format_size(total_sampled_size),
            ]
        )

        print("\nDaily Events Summary:")
        print(
            tabulate(
                table_data,
                headers=[
                    "Date",
                    "Events",
                    "Avg Event Size",
                    "Total Size",
                    "Sampled Size (20%)",
                ],
                tablefmt="grid",
            )
        )

        # Calculate daily increase
        daily_increase = analyzer.calculate_daily_increase(events_data)
        print("\nDaily Increase Analysis:")
        for day in daily_increase:
            print(
                f"{day['date']}: {day['increase']} more events ({day['percentage_increase']:.2f}% increase)"
            )


if __name__ == "__main__":
    asyncio.run(main())
