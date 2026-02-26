import requests
import json
import time

def _infer_bq_type(value):
    """Helper function to map Python types to BigQuery types."""
    if isinstance(value, bool):
        return "BOOLEAN"
    elif isinstance(value, int):
        return "INTEGER"
    elif isinstance(value, float):
        return "FLOAT"
    elif isinstance(value, dict):
        return "RECORD"
    elif isinstance(value, list):
        return "REPEATED" # Special marker handled in the recursive function
    else:
        return "STRING"

def _build_schema_field(key, value):
    """Recursively builds a BigQuery schema field object."""
    mode = "NULLABLE"
    field_type = _infer_bq_type(value)
    field_schema = {"name": key}

    if field_type == "REPEATED":
        mode = "REPEATED"
        # If the list is not empty, peek at the first element to determine the type
        if value and len(value) > 0:
            first_element = value[0]
            # Recursively handle lists of records
            if isinstance(first_element, dict):
                field_type = "RECORD"
                field_schema["fields"] = [_build_schema_field(k, v) for k, v in first_element.items()]
            else:
                field_type = _infer_bq_type(first_element)
        else:
            # Default to STRING if list is empty or type cannot be inferred
            field_type = "STRING"
    
    elif field_type == "RECORD":
        # Recursively handle nested records
        field_schema["fields"] = [_build_schema_field(k, v) for k, v in value.items()]

    field_schema["type"] = field_type
    field_schema["mode"] = mode
    return field_schema

def generate_and_save_schema(record, filename):
    """Generates a BigQuery schema from a single record and saves it."""
    schema = []
    for key, value in record.items():
        schema.append(_build_schema_field(key, value))
    
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(schema, f, indent=2)
        print(f"BigQuery schema successfully generated and saved to '{filename}'")
    except IOError as e:
        print(f"Error saving schema to file: {e}")

def fetch_all_clients_to_jsonl(output_filename="datacite_clients.jsonl", schema_filename="datacite_clients_schema.json"):
    """
    Fetches all client data from the DataCite API, saves it to a JSONL file,
    and generates a BigQuery schema file based on the first record found.
    """
    # The initial URL for the DataCite clients API endpoint
    api_url = "https://api.datacite.org/clients"
    
    client_count = 0
    page_count = 0
    schema_generated = False # Flag to ensure we only generate schema once

    print(f"Starting data fetch. Output: '{output_filename}', Schema: '{schema_filename}'")

    # Clear the output file or create it if it doesn't exist
    with open(output_filename, 'w') as f:
        pass 

    try:
        while api_url:
            page_count += 1
            print(f"Fetching page {page_count}: {api_url}")
            
            try:
                response = requests.get(api_url, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                clients_on_page = data.get('data', [])

                # --- Schema Generation Block ---
                if not schema_generated and clients_on_page:
                    # Use the first client record to infer the schema
                    first_client = clients_on_page[0]
                    generate_and_save_schema(first_client, schema_filename)
                    schema_generated = True
                # -------------------------------

                # Append data to file
                with open(output_filename, 'a', encoding='utf-8') as f:
                    for client in clients_on_page:
                        json_line = json.dumps(client)
                        f.write(json_line + '\n')
                        client_count += 1
                
                # Get next page link
                api_url = data.get('links', {}).get('next')
                time.sleep(0.1)

            except requests.exceptions.RequestException as e:
                print(f"An error occurred during the request: {e}")
                print("Stopping the script.")
                break
                
    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Exiting.")
        
    finally:
        print("-" * 30)
        print("Fetching complete.")
        print(f"Total pages fetched: {page_count}")
        print(f"Total clients written to file: {client_count}")
        print(f"Data saved in '{output_filename}'")
        if schema_generated:
            print(f"Schema saved in '{schema_filename}'")

if __name__ == "__main__":
    # You can adjust the paths here as needed
    fetch_all_clients_to_jsonl(
        output_filename='./datacite_clients.jsonl',
        schema_filename='./datacite_clients_schema.json'
    )
