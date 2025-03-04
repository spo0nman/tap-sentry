#!/usr/bin/env python3
import os
import json
import singer
import asyncio
import concurrent.futures
from singer import utils, metadata
from singer.catalog import Catalog

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
    
    # Make sure project_detail is in the schemas that are loaded
    
    for schema_name, schema in raw_schemas.items():
        # Create metadata and add to catalog
        stream_metadata = []
        key_properties = []
        
        # Create catalog entry
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata': stream_metadata,
            'key_properties': key_properties
        }
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
    # Create client and sync instance
    auth = SentryAuthentication(config["api_token"])
    client = SentryClient(auth)
    sync_instance = SentrySync(client, state)
    
    # Log available sync methods for debugging
    sync_methods = [method for method in dir(sync_instance) if method.startswith('sync_')]
    LOGGER.info(f"Available sync methods: {sync_methods}")
    
    # Log selected streams
    selected_stream_ids = get_selected_streams(catalog)
    LOGGER.info(f"Selected streams: {selected_stream_ids}")
    
    # Group streams into async and non-async categories
    async_streams = ['projects', 'issues', 'events', 'teams']
    non_async_streams = ['project_detail', 'release']
    
    # Filter selected streams
    selected_async_streams = [s for s in selected_stream_ids if s in async_streams]
    selected_non_async_streams = [s for s in selected_stream_ids if s in non_async_streams]
    
    LOGGER.info(f"Processing async streams: {selected_async_streams}")
    LOGGER.info(f"Processing non-async streams: {selected_non_async_streams}")
    
    # Run async tasks with event loop
    loop = asyncio.get_event_loop()
    try:
        tasks = []
        for stream in catalog.streams:
            if stream.tap_stream_id in selected_async_streams:
                LOGGER.info(f"Adding async stream: {stream.tap_stream_id}")
                tasks.append(sync_instance.sync(stream.tap_stream_id, stream.schema))
        
        if tasks:
            loop.run_until_complete(asyncio.gather(*tasks))
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()

    # Handle non-async streams separately
    for stream in catalog.streams:
        if stream.tap_stream_id in selected_non_async_streams:
            LOGGER.info(f"Processing non-async stream: {stream.tap_stream_id}")
            method_name = f"sync_{stream.tap_stream_id}"
            if hasattr(sync_instance, method_name):
                method = getattr(sync_instance, method_name)
                LOGGER.info(f"Calling {method_name}")
                method(stream.schema, stream.tap_stream_id)
            else:
                LOGGER.warning(f"Method {method_name} not found in SentrySync class")

@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        print(json.dumps(catalog, indent=2))
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()

        config = args.config
        state ={
            "bookmarks": {
               "issues": {"start": config["start_date"]},
                "events": {"start": config["start_date"]}
            }
        }
        state.update(args.state)

        sync(config, state, catalog)

if __name__ == "__main__":
    main()
