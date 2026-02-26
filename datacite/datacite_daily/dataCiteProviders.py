import requests
import json
import time

def download_all_providers(api_url):
    """
    Downloads information about all DataCite providers from the API, handling pagination.

    Args:
        api_url (str): The initial URL of the DataCite providers API endpoint.

    Returns:
        list: A list of dictionaries, where each dictionary represents a provider.
              Returns None if the initial request fails.
    """
    all_providers = []
    current_url = api_url
    page_count = 1

    while current_url:
        try:
            print(f"Fetching page {page_count}: {current_url}")
            # Send an HTTP GET request to the specified URL.
            # The timeout is set to 10 seconds to prevent the program from hanging indefinitely.
            response = requests.get(current_url, timeout=10)

            # Raise an exception for bad status codes (4xx or 5xx).
            response.raise_for_status()

            # Parse the JSON response from the API.
            data = response.json()
            
            # The actual provider data is inside the 'data' key.
            providers_on_page = data.get('data', [])
            if providers_on_page:
                all_providers.extend(providers_on_page)
            
            # Get the URL for the next page of results.
            # If 'next' key doesn't exist, it will be None and the loop will terminate.
            current_url = data.get('links', {}).get('next')
            page_count += 1
            
            # A small delay to be polite to the API server.
            time.sleep(0.1)

        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
            return None # Exit if there's an error
        except requests.exceptions.ConnectionError as conn_err:
            print(f"Connection error occurred: {conn_err}")
            return None
        except requests.exceptions.Timeout as timeout_err:
            print(f"Timeout error occurred: {timeout_err}")
            return None
        except requests.exceptions.RequestException as err:
            print(f"An unexpected error occurred: {err}")
            return None
    
    print(f"\nSuccessfully downloaded data for {len(all_providers)} providers across {page_count - 1} pages.")
    return all_providers

def save_data_to_jsonl(data, filename="datacite_providers.jsonl"):
    """
    Saves the provider data to a local JSONL file.
    Each line in the file is a separate JSON object.

    Args:
        data (list): The list of provider data to be saved.
        filename (str): The name of the file to save the data to.
    """
    if data:
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                for item in data:
                    # Convert each dictionary to a compact JSON string
                    # and write it to a new line in the file.
                    f.write(json.dumps(item, ensure_ascii=False) + '\n')
            print(f"Provider data successfully saved to {filename}")
        except IOError as e:
            print(f"Error saving data to file: {e}")

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

def generate_bigquery_schema(data):
    """
    Generates a Google BigQuery schema based on the first record in the data list.
    
    Args:
        data (list): List of dictionaries (records).
        
    Returns:
        list: A list of dictionaries representing the BigQuery schema.
    """
    if not data:
        return []

    # We infer the schema from the first record. 
    # Note: In production, you might want to merge schemas from multiple records 
    # to handle optional fields that might be missing in the first record.
    sample_record = data[0]
    schema = []

    for key, value in sample_record.items():
        schema.append(_build_schema_field(key, value))

    return schema

def save_schema_to_json(schema, filename="datacite_providers_schema.json"):
    """
    Saves the generated BigQuery schema to a JSON file.
    
    Args:
        schema (list): The BigQuery schema list.
        filename (str): The output filename.
    """
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(schema, f, indent=2)
        print(f"BigQuery schema successfully saved to {filename}")
    except IOError as e:
        print(f"Error saving schema to file: {e}")

# --- Example Usage ---
if __name__ == "__main__":
    # This is the official DataCite API endpoint for providers.
    DATACITE_API_URL = "https://api.datacite.org/providers"
    OUTPUT_DATA_FILE = "datacite_providers.jsonl"
    OUTPUT_SCHEMA_FILE = "datacite_providers_schema.json"

    print(f"Attempting to download all provider information from: {DATACITE_API_URL}")
    
    # 1. Download Data
    providers = download_all_providers(DATACITE_API_URL)

    if providers:
        # 2. Save Data to JSONL
        save_data_to_jsonl(providers, OUTPUT_DATA_FILE)

        if len(providers) > 0:
            print("\n--- Sample Data Info ---")
            first_provider_name = providers[0].get('attributes', {}).get('name')
            print(f"First provider's name: {first_provider_name}")
            print("------------------------\n")

            # 3. Generate Schema
            print("Generating BigQuery schema...")
            bq_schema = generate_bigquery_schema(providers)
            
            # 4. Save Schema to JSON
            save_schema_to_json(bq_schema, OUTPUT_SCHEMA_FILE)
