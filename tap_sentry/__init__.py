#!/usr/bin/env python3
import os
import json
import singer
import asyncio
import concurrent.futures
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

from tap_sentry.sync import SentryAuthentication, SentryClient, SentrySync

REQUIRED_CONFIG_KEYS = ["start_date",
                        "api_token"]
LOGGER = singer.get_logger()

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

# Load schemas from schemas folder
def load_schemas():
    schemas = {}

    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = json.load(file)
            
    # You might want to add some debug logging here to verify project_detail is loaded
    # logger.debug(f"Loaded schemas: {list(schemas.keys())}")
    
    return schemas

def discover():
    raw_schemas = load_schemas()
    streams = []
    
    for schema_name, schema in raw_schemas.items():
        # Create metadata and add to catalog
        stream_metadata = []
        key_properties = []
        
        # Create proper CatalogEntry objects instead of dictionaries
        catalog_entry = CatalogEntry(
            stream=schema_name,
            tap_stream_id=schema_name,
            schema=Schema(schema),
            metadata=stream_metadata,
            key_properties=key_properties
        )
        streams.append(catalog_entry)
        
    return Catalog(streams)

def get_selected_streams(catalog):
    '''
    Gets selected streams.  Checks schema's 'selected' first (legacy)
    and then checks metadata (current), looking for an empty breadcrumb
    and mdata with a 'selected' entry
    '''
    selected_streams = []
    for stream in catalog.streams:
        stream_metadata = metadata.to_map(stream.metadata)
        # stream metadata will have an empty breadcrumb
        if metadata.get(stream_metadata, (), "selected"):
            selected_streams.append(stream.tap_stream_id)

    return selected_streams

def create_sync_tasks(config, state, catalog):
    auth = SentryAuthentication(config["api_token"])
    client = SentryClient(auth)
    sync = SentrySync(client, state)

    selected_stream_ids = get_selected_streams(catalog)
    sync_tasks = []
    
    for stream in catalog.streams:
        stream_id = stream.tap_stream_id
        # Only include streams that have a matching sync method
        if stream_id in selected_stream_ids and hasattr(sync, f"sync_{stream_id}"):
            sync_tasks.append(sync.sync(stream_id, stream.schema))
    
    return asyncio.gather(*sync_tasks)

def sync(config, state, catalog):
    """Sync data from tap source."""
    # Create client and sync instance
    auth = SentryAuthentication(config["api_token"])
    client = SentryClient(auth)
    sync_instance = SentrySync(client, state)
    
    # Get selected streams
    selected_stream_ids = get_selected_streams(catalog)
    LOGGER.info(f"Selected streams: {selected_stream_ids}")
    
    # Create an event loop
    loop = asyncio.get_event_loop()
    
    try:
        # Process each selected stream
        for stream in catalog.streams:
            if stream.tap_stream_id in selected_stream_ids:
                stream_id = stream.tap_stream_id
                schema = stream.schema
                
                LOGGER.info(f"Processing stream: {stream_id}")
                
                # Use the sync method we defined
                task = sync_instance.sync(stream_id, schema)
                loop.run_until_complete(task)
    finally:
        # Clean up resources
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
    
    return state

@utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args()
    
    config = args.config if args.config else {}
    state = args.state if args.state else {}
    catalog = args.catalog if args.catalog else {}
    
    # Log configuration (scrubbing sensitive info)
    safe_config = config.copy()
    if 'api_token' in safe_config:
        safe_config['api_token'] = '***REDACTED***'
    LOGGER.debug(f"Configuration: {json.dumps(safe_config, indent=2)}")
    
    # Log state
    LOGGER.debug(f"Starting state: {json.dumps(state, indent=2)}")
    
    # Extract necessary config values
    auth_token = config.get('api_token')
    org_slug = config.get('organization')
    project_slugs = config.get('project_slugs', [])
    start_date = config.get('start_date')
    base_url = config.get('base_url', 'https://sentry.io/api/0')
    
    LOGGER.debug(f"Using organization: {org_slug}")
    LOGGER.debug(f"Using projects: {project_slugs}")
    LOGGER.debug(f"Using start_date: {start_date}")
    LOGGER.debug(f"Using base_url: {base_url}")
    
    if not auth_token or not org_slug:
        LOGGER.error("API token and organization slug are required")
        return
    
    # Create the Sentry client
    LOGGER.debug(f"Initializing Sentry client")
    client = SentryClient(auth_token, org_slug, base_url)
    
    # Initialize the sync object
    LOGGER.debug(f"Initializing SentrySync")
    sync = SentrySync(client, project_slugs, start_date)
    
    # Run the sync
    LOGGER.debug(f"Starting sync operation")
    sync.sync(catalog, state)
    LOGGER.debug(f"Sync operation completed")

if __name__ == "__main__":
    main()
