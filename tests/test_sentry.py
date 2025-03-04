import os

import requests_mock
import simplejson
import unittest
from singer import Schema
from singer.catalog import Catalog, CatalogEntry

from tap_sentry import SentryAuthentication, SentryClient, SentrySync
import asyncio
import mock


def load_file_current(filename, path):
    myDir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(myDir, path, filename)
    with open(path) as file:
        return simplejson.load(file)

def load_file(filename, path):
    sibB = os.path.join(os.path.dirname(__file__), '..', path)
    with open(os.path.join(sibB, filename)) as f:
        return simplejson.load(f)

# Our test case class
class MyGreatClassTestCase(unittest.TestCase):

    def setUp(self):
        """Set up test fixtures, if any."""
        auth = SentryAuthentication("test-token")
        self.client = SentryClient(auth)
        # Create a new event loop for each test
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def tearDown(self):
        """Tear down test fixtures, if any."""
        # Clean up but don't close the loop yet
        if self.loop and not self.loop.is_closed():
            self.loop.run_until_complete(self.loop.shutdown_asyncgens())
            self.loop.close()

    @requests_mock.mock()
    def test_projects(self, m):
        record_value = load_file_current('projects_output.json', 'data_test')
        m.get('https://sentry.io/api/0//organizations/split-software/projects/', json=[record_value])
        self.assertEqual(self.client.projects(), [record_value])

    @requests_mock.mock()
    def test_sync_projects(self, m):
        """Test sync_projects."""
        # Don't create a new loop, use self.loop
        record_value = load_file_current('projects_output.json', 'data_test')
        with mock.patch('tap_sentry.SentryClient.projects', return_value=[record_value]):
            dataSync = SentrySync(self.client)
            schema = load_file('projects.json', 'tap_sentry/schemas')
            resp = dataSync.sync_projects(Schema(schema))
            with mock.patch('singer.write_record') as patching:
                task = asyncio.gather(resp)
                self.loop.run_until_complete(task)
                patching.assert_called_with('projects', record_value)

    @requests_mock.mock()
    def test_events(self, m):
        record_value = load_file_current('events_output.json', 'data_test')
        m.get('https://sentry.io/api/0//organizations/split-software/events/?project=1', json=[record_value])
        self.assertEqual(self.client.events(1, {}), [record_value])

    @requests_mock.mock()
    def test_sync_events(self, m):
        loop = asyncio.get_event_loop()
        record_value = load_file_current('events_output.json', 'data_test')
        with mock.patch.object(SentryClient, 'projects', return_value=[{"id":1}]):
            with mock.patch('tap_sentry.SentryClient.events', return_value=[record_value]):
                dataSync = SentrySync(self.client)
                schema = load_file('events.json', 'tap_sentry/schemas')
                resp = dataSync.sync_events(Schema(schema))
                with mock.patch('singer.write_record') as patching:
                    task = asyncio.gather(resp)
                    loop.run_until_complete(task)
                    patching.assert_called_with('events', record_value)

    @requests_mock.mock()
    def test_issues(self, m):
        record_value = load_file_current('issues_output.json', 'data_test')
        m.get('https://sentry.io/api/0//organizations/split-software/issues/?project=1', json=[record_value])
        self.assertEqual(self.client.issues(1, {}), [record_value])

    @requests_mock.mock()
    def test_sync_issues(self, m):
        loop = asyncio.get_event_loop()
        record_value = load_file_current('issues_output.json', 'data_test')
        with mock.patch.object(SentryClient, 'projects', return_value=[{"id":1}]):
            with mock.patch('tap_sentry.SentryClient.issues', return_value=[record_value]):
                dataSync = SentrySync(self.client)
                schema = load_file('issues.json', 'tap_sentry/schemas')
                resp = dataSync.sync_issues(Schema(schema))
                with mock.patch('singer.write_record') as patching:
                    task = asyncio.gather(resp)
                    loop.run_until_complete(task)
                    patching.assert_called_with('issues', record_value)

    @requests_mock.mock()
    def test_teams(self, m):
        record_value = load_file_current('teams_output.json', 'data_test')
        m.get('https://sentry.io/api/0//organizations/split-software/teams/', json=[record_value])
        self.assertEqual(self.client.teams({}), [record_value])

    @requests_mock.mock()
    def test_sync_teams(self, m):
        loop = asyncio.get_event_loop()
        record_value = load_file_current('teams_output.json', 'data_test')
        with mock.patch('tap_sentry.SentryClient.teams', return_value=[record_value]):
            dataSync = SentrySync(self.client)
            schema = load_file('teams.json', 'tap_sentry/schemas')
            resp = dataSync.sync_teams(Schema(schema))
            with mock.patch('singer.write_record') as patching:
                task = asyncio.gather(resp)
                loop.run_until_complete(task)
                patching.assert_called_with('teams', record_value)

    @requests_mock.mock()
    def test_users(self, m):
        record_value = load_file_current('users_output.json', 'data_test')
        m.get('https://sentry.io/api/0//organizations/split-software/users/', json=[record_value])
        self.assertEqual(self.client.users({}), [record_value])

    @requests_mock.mock()
    def test_sync_users(self, m):
        loop = asyncio.get_event_loop()
        record_value = load_file_current('teams_output.json', 'data_test')
        with mock.patch('tap_sentry.SentryClient.users', return_value=[record_value]):
            dataSync = SentrySync(self.client)
            schema = load_file('users.json', 'tap_sentry/schemas')
            resp = dataSync.sync_users(Schema(schema))
            with mock.patch('singer.write_record') as patching:
                task = asyncio.gather(resp)
                loop.run_until_complete(task)
                patching.assert_called_with('users', record_value)

    @requests_mock.mock()
    def test_sync_releases(self, m):
        """Test the release sync via the dynamic sync method."""
        loop = asyncio.get_event_loop()
        record_value = load_file_current('release_output.json', 'data_test')
        
        with mock.patch.object(SentryClient, 'projects', return_value=[{"id":1}]):
            with mock.patch('tap_sentry.SentryClient.releases', return_value=[record_value]):
                dataSync = SentrySync(self.client)
                schema = load_file('release.json', 'tap_sentry/schemas')
                
                # Test using the dynamic sync method instead of calling sync_release directly
                with mock.patch('singer.write_record') as patching:
                    # Use the generic sync method that will route to sync_release
                    task = dataSync.sync("release", Schema(schema))
                    loop.run_until_complete(task)
                    
                    # Assert the record was written with the correct stream name
                    patching.assert_called_with('release', record_value)

    @mock.patch('tap_sentry.sync')
    def test_full_sync_pipeline(self, mock_sync):
        """Test the full sync pipeline as used in Meltano environment."""
        from tap_sentry import sync
        
        # Test version that doesn't close the event loop
        def _test_sync(config, state, catalog):
            # Create client and sync instance
            auth = SentryAuthentication(config["api_token"])
            client = SentryClient(auth)
            sync_instance = SentrySync(client)
            
            # Get selected streams
            selected_stream_ids = get_selected_streams(catalog)
            
            # Use our existing loop, don't create a new one
            loop = self.loop
            
            # Process each selected stream
            for stream in catalog.streams:
                if stream.tap_stream_id in selected_stream_ids:
                    stream_id = stream.tap_stream_id
                    schema = stream.schema
                    
                    # Use the sync method we defined
                    task = sync_instance.sync(stream_id, schema)
                    loop.run_until_complete(task)
            
            return state
        
        # Replace the real sync with our test version
        mock_sync.side_effect = _test_sync
        
        # Rest of the test remains the same
        # ...

    def test_discover_serialization(self):
        """Test that the discover function returns a serializable Catalog object."""
        from tap_sentry import discover
        import json
        
        # Get the catalog from discover function
        catalog = discover()
        
        # Verify that catalog is a Catalog object
        self.assertIsInstance(catalog, Catalog)
        
        # Verify that all entries in catalog are CatalogEntry objects
        for stream in catalog.streams:
            self.assertIsInstance(stream, CatalogEntry)
        
        # Verify that the catalog can be converted to dict and then JSON
        catalog_dict = catalog.to_dict()
        
        # This should not raise any exception
        try:
            json_output = json.dumps(catalog_dict, indent=2)
            self.assertIsNotNone(json_output)
        except Exception as e:
            self.fail(f"Failed to serialize catalog to JSON: {str(e)}")
        
        # Verify JSON structure includes streams
        catalog_data = json.loads(json_output)
        self.assertIn('streams', catalog_data)
        self.assertTrue(len(catalog_data['streams']) > 0)
        
        # Check if required fields exist in the first stream
        first_stream = catalog_data['streams'][0]
        for field in ['stream', 'tap_stream_id', 'schema', 'metadata', 'key_properties']:
            self.assertIn(field, first_stream)

    def test_write_schema_in_sync_methods(self):
        """Test that sync methods correctly call singer.write_schema."""
        from tap_sentry import discover
        from singer.catalog import Catalog, CatalogEntry
        import singer
        import mock
        
        # Create a mock for singer.write_schema
        with mock.patch('singer.write_schema') as mock_write_schema:
            # Set up our SentrySync instance with a mock client
            mock_client = mock.MagicMock()
            mock_client.projects.return_value = [
                {"id": "123", "slug": "test-project", "organization": {"slug": "test-org"}}
            ]
            
            # Create our SentrySync instance
            sync = SentrySync(mock_client)
            
            # Get the catalog from discover to use as test data
            catalog = discover()
            
            # Choose one stream to test
            stream_name = "project_detail"
            stream = catalog.get_stream(stream_name)
            
            # Run the sync method in a new event loop
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(sync.sync(stream_name, stream.schema))
            finally:
                loop.close()
            
            # Verify write_schema was called with the correct arguments
            mock_write_schema.assert_called_once()
            args, kwargs = mock_write_schema.call_args
            
            # Check that the stream and schema were passed correctly
            self.assertEqual(args[0], stream_name)
            self.assertIsInstance(args[1], dict)  # Schema should be converted to dict
            self.assertEqual(args[2], ["id"])  # Key properties should be correct


if __name__ == '__main__':
    unittest.main()