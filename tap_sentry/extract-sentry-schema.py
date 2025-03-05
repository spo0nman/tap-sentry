#!/usr/bin/env python
import json
import os
import requests
import sys
import difflib
from typing import Dict, Any, List

# URL for the Sentry API schema
SENTRY_SCHEMA_URL = "https://raw.githubusercontent.com/getsentry/sentry-api-schema/main/openapi-derefed.json"

# Map Sentry API paths to our schema files with more specific endpoint patterns
SCHEMA_MAPPING = {
    # Projects detail - needs project-specific object structure
    "/api/0/projects/{organization_slug}/{project_slug}/": {
        "file": "project_detail.json",
        "schema_ref": "#/components/schemas/Project",
    },
    # Events - look for event object structure
    "/api/0/organizations/{organization_slug}/events/": {
        "file": "events.json",
        "schema_ref": "#/components/schemas/Event",
    },
    # Issues - look for issue object structure
    "/api/0/organizations/{organization_slug}/issues/": {
        "file": "issues.json",
        "schema_ref": "#/components/schemas/GroupEvent",
    },
    # Projects list - needs project object structure
    "/api/0/organizations/{organization_slug}/projects/": {
        "file": "projects.json",
        "schema_ref": "#/components/schemas/Project",
    },
    # Releases - needs release object structure
    "/api/0/organizations/{organization_slug}/releases/": {
        "file": "release.json",
        "schema_ref": "#/components/schemas/Release",
    },
    # Teams - needs team object structure
    "/api/0/organizations/{organization_slug}/teams/": {
        "file": "teams.json",
        "schema_ref": "#/components/schemas/Team",
    },
    # Users - needs user object structure
    "/api/0/organizations/{organization_slug}/members/": {
        "file": "users.json",
        "schema_ref": "#/components/schemas/OrganizationMember",
    },
}


def download_schema() -> Dict[str, Any]:
    """Download the Sentry API schema from GitHub."""
    print(f"Downloading schema from {SENTRY_SCHEMA_URL}...")
    response = requests.get(SENTRY_SCHEMA_URL)
    response.raise_for_status()
    return response.json()


def load_schema() -> Dict[str, Any]:
    """Load the Sentry API schema from local file."""
    schema_path = "tap_sentry/schemas/sentry-raw-schema.json"
    if os.path.exists(schema_path):
        print(f"Loading schema from {schema_path}...")
        with open(schema_path, "r") as f:
            return json.load(f)
    else:
        schema = download_schema()
        # Save downloaded schema for future reference
        with open(schema_path, "w") as f:
            json.dump(schema, f, indent=2)
        return schema


def convert_type_to_singer(openapi_type: str, nullable: bool = True) -> List[str]:
    """Convert OpenAPI type to Singer schema type with nullable handling."""
    type_mapping = {
        "string": "string",
        "integer": "integer",
        "number": "number",
        "boolean": "boolean",
        "object": "object",
        "array": "array",
    }

    singer_type = type_mapping.get(openapi_type, "string")

    # Handle nullable types in Singer format (null by default)
    if nullable:
        return [singer_type, "null"]
    else:
        return [singer_type]


def process_property(prop_name: str, prop_schema: Dict[str, Any]) -> Dict[str, Any]:
    """Process a single property from OpenAPI schema to Singer format."""
    singer_property = {}

    # Handle basic types
    if "type" in prop_schema:
        nullable = prop_schema.get("nullable", False)
        singer_property["type"] = convert_type_to_singer(prop_schema["type"], nullable)

        # Handle format for dates, etc.
        if "format" in prop_schema:
            if prop_schema["format"] in ["date-time", "date"]:
                singer_property["format"] = prop_schema["format"]

    # Handle enum
    if "enum" in prop_schema:
        singer_property["enum"] = prop_schema["enum"]

    # Handle nested objects
    if prop_schema.get("type") == "object" and "properties" in prop_schema:
        singer_property["properties"] = {}
        for nested_name, nested_schema in prop_schema["properties"].items():
            singer_property["properties"][nested_name] = process_property(
                nested_name, nested_schema
            )

    # Handle arrays
    if prop_schema.get("type") == "array" and "items" in prop_schema:
        singer_property["items"] = process_property("items", prop_schema["items"])

    # Handle anyOf and oneOf (convert to Singer's anyOf pattern)
    if "anyOf" in prop_schema:
        if all("type" in schema for schema in prop_schema["anyOf"]):
            # If all schemas have types, collect them
            types = []
            for schema in prop_schema["anyOf"]:
                processed = process_property(prop_name, schema)
                if "type" in processed:
                    if isinstance(processed["type"], list):
                        types.extend(processed["type"])
                    else:
                        types.append(processed["type"])

            # Deduplicate types
            singer_property["type"] = list(set(types))
        else:
            # Handle complex anyOf (like in metadata for issues)
            singer_property["anyOf"] = [
                process_property(f"{prop_name}_option_{i}", schema)
                for i, schema in enumerate(prop_schema["anyOf"])
            ]

    return singer_property


def find_schema_component(
    api_schema: Dict[str, Any], schema_ref: str
) -> Dict[str, Any]:
    """Find a schema component by reference."""
    # Parse the reference path
    if schema_ref.startswith("#/components/schemas/"):
        component_name = schema_ref.split("/")[-1]
        if component_name in api_schema.get("components", {}).get("schemas", {}):
            return api_schema["components"]["schemas"][component_name]

    # If not found or invalid reference, return empty dict
    print(f"Warning: Schema reference '{schema_ref}' not found")
    return {}


def extract_component_schema(
    api_schema: Dict[str, Any], schema_ref: str, path_pattern: str = ""
) -> Dict[str, Any]:
    """Extract schema from a component reference."""
    # Create a base Singer schema
    singer_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "additionalProperties": True,
        "properties": {},
    }

    # First try to get schema from component reference
    if schema_ref:
        component_schema = find_schema_component(api_schema, schema_ref)
        if component_schema and "properties" in component_schema:
            print(f"Found schema component: {schema_ref}")
            for prop_name, prop_schema in component_schema["properties"].items():
                singer_schema["properties"][prop_name] = process_property(
                    prop_name, prop_schema
                )

    # If component schema didn't work, try to find from path responses
    if not singer_schema["properties"]:
        print(f"Looking for schema in path responses: {path_pattern}")
        # Look for matching paths in the API schema
        for path, path_data in api_schema.get("paths", {}).items():
            # Check if this path matches our pattern
            if path_pattern in path or path in path_pattern:
                print(f"Found matching path: {path}")

                # Look for GET method with 200 response
                if "get" in path_data and "responses" in path_data["get"]:
                    if "200" in path_data["get"]["responses"]:
                        response = path_data["get"]["responses"]["200"]

                        # Extract schema from response content
                        if (
                            "content" in response
                            and "application/json" in response["content"]
                        ):
                            schema = response["content"]["application/json"].get(
                                "schema", {}
                            )

                            # Handle both array and object responses
                            if schema.get("type") == "array" and "items" in schema:
                                item_schema = schema["items"]
                                if "properties" in item_schema:
                                    for prop_name, prop_schema in item_schema[
                                        "properties"
                                    ].items():
                                        singer_schema["properties"][prop_name] = (
                                            process_property(prop_name, prop_schema)
                                        )
                            elif "properties" in schema:
                                for prop_name, prop_schema in schema[
                                    "properties"
                                ].items():
                                    singer_schema["properties"][prop_name] = (
                                        process_property(prop_name, prop_schema)
                                    )

    # Add text_content field for search/embedding
    singer_schema["properties"]["text_content"] = {
        "type": ["string", "null"],
        "description": "Combined text content for search and embedding",
    }

    return singer_schema


def get_diff(original_content: str, new_content: str, file_path: str) -> str:
    """Generate a colorized diff between original and new content."""
    original_lines = original_content.splitlines()
    new_lines = new_content.splitlines()

    diff = difflib.unified_diff(
        original_lines,
        new_lines,
        fromfile=f"a/{file_path}",
        tofile=f"b/{file_path}",
        lineterm="",
    )

    return "\n".join(diff)


def main():
    """Main function to extract schemas from Sentry API schema."""
    # Parse arguments
    preview_only = "--preview" in sys.argv
    specific_file = None
    for arg in sys.argv[1:]:
        if not arg.startswith("--") and arg in [
            mapping["file"] for mapping in SCHEMA_MAPPING.values()
        ]:
            specific_file = arg

    # Load schema
    try:
        api_schema = load_schema()
        print(f"Successfully loaded schema ({len(json.dumps(api_schema))} bytes)")
    except Exception as e:
        print(f"Error loading schema: {e}")
        return

    # Create schemas directory if it doesn't exist
    os.makedirs("tap_sentry/schemas", exist_ok=True)

    # Process each schema mapping
    for path_pattern, mapping in SCHEMA_MAPPING.items():
        schema_file = mapping["file"]
        schema_ref = mapping.get("schema_ref", "")

        # Skip if a specific file was requested and this isn't it
        if specific_file and schema_file != specific_file:
            continue

        print(f"\nProcessing schema for {path_pattern} -> {schema_file}")

        # Extract schema for this path and reference
        singer_schema = extract_component_schema(api_schema, schema_ref, path_pattern)

        # Check if we got any properties
        if not singer_schema["properties"]:
            print(f"Warning: No properties found for {path_pattern}")
            continue

        # Read the existing schema file if it exists
        output_path = os.path.join("tap_sentry/schemas", schema_file)
        existing_content = ""
        if os.path.exists(output_path):
            with open(output_path, "r") as f:
                existing_content = f.read()

        # Format the new schema content
        new_content = json.dumps(singer_schema, indent=2)

        # Generate diff
        if existing_content:
            diff = get_diff(existing_content, new_content, output_path)

            if diff:
                print(f"Changes to {schema_file}:")
                print(diff)
            else:
                print(f"No changes to {schema_file}")
        else:
            print(f"New file: {schema_file}")
            print(new_content[:500] + "..." if len(new_content) > 500 else new_content)

        # Save to schema file if not in preview mode
        if not preview_only:
            with open(output_path, "w") as f:
                f.write(new_content)
            print(
                f"Saved schema to {output_path} with {len(singer_schema['properties'])} properties"
            )

    if preview_only:
        print("\nPreview only mode - no files were modified")
    else:
        print("\nSchema extraction complete!")


if __name__ == "__main__":
    main()
