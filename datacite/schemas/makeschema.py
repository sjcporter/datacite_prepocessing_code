import os
import json
import argparse
from collections import defaultdict
import datetime

# --- Type Mapping: Python types to BigQuery types ---
# This dictionary helps convert the inferred Python types into the correct
# BigQuery data type strings.
TYPE_MAPPING = {
    str: 'STRING',
    int: 'INTEGER',
    float: 'FLOAT',
    bool: 'BOOLEAN',
    bytes: 'BYTES',
    datetime.datetime: 'TIMESTAMP',
}

def infer_type(value):
    """
    Infers the Python type of a single value.
    It has special handling to detect if a string is in ISO 8601 format,
    in which case it infers it as a datetime object.
    """
    if isinstance(value, str):
        try:
            # Attempt to parse the string as a timestamp. This is a common case.
            datetime.datetime.fromisoformat(value.replace('Z', '+00:00'))
            return datetime.datetime
        except (ValueError, TypeError):
            # If it fails, it's just a regular string.
            return str
    return type(value)

def infer_schema_from_object(json_obj):
    """
    Recursively infers a schema from a single JSON object (Python dictionary).

    Args:
        json_obj (dict): The JSON object to analyze.

    Returns:
        dict: A dictionary representing the schema of the object.
              e.g., {'name': <class 'str'>, 'age': <class 'int'>}
    """
    schema = {}
    for key, value in json_obj.items():
        if value is None:
            # We can't infer a type from None, so we skip it.
            # The field might appear with a type in another record.
            continue
        
        value_type = infer_type(value)

        if value_type == dict:
            # If the value is a dictionary, recurse into it.
            schema[key] = infer_schema_from_object(value)
        elif value_type == list:
            # If it's a list, we need to determine the type of its elements.
            # We assume the list is not empty and all elements have the same type.
            if value:
                element_type = infer_schema_from_object(value[0]) if isinstance(value[0], dict) else infer_type(value[0])
                schema[key] = [element_type]
            # If the list is empty, we can't infer the type, so we skip it.
        else:
            schema[key] = value_type
    return schema

def merge_schemas(master_schema, new_schema):
    """
    Merges a newly inferred schema into the master schema.

    Args:
        master_schema (dict): The consolidated schema found so far.
        new_schema (dict): The schema from a new JSON object.

    Returns:
        dict: The updated master schema.
    """
    for key, new_type in new_schema.items():
        if key not in master_schema:
            # If the key is new, add it to the master schema.
            master_schema[key] = new_type
            continue

        master_type = master_schema[key]

        # --- Type Conflict Resolution ---
        if master_type == new_type:
            # No conflict, nothing to do.
            continue

        # Handle nested objects (RECORDs)
        if isinstance(master_type, dict) and isinstance(new_type, dict):
            master_schema[key] = merge_schemas(master_type, new_type)
        
        # Handle arrays (REPEATED fields)
        elif isinstance(master_type, list) and isinstance(new_type, list):
            # For lists, we merge the schema of their elements.
            # This handles cases where nested objects in a list gain more fields.
            if master_type and new_type:
                # We assume one element type per list for simplicity
                merged_element_type = merge_schemas(master_type[0], new_type[0]) if isinstance(master_type[0], dict) else new_type[0]
                master_schema[key] = [merged_element_type]

        # Handle type promotion (e.g., INTEGER -> FLOAT)
        elif {master_type, new_type} == {int, float}:
            master_schema[key] = float # Promote to FLOAT as it's more general

        # Default conflict resolution: promote to STRING
        else:
            # If types are fundamentally different (e.g., str and int),
            # we default to STRING as it can hold any value.
            # A warning is printed to alert the user.
            print(f"Warning: Type conflict for field '{key}'. Was {master_type.__name__}, now {new_type.__name__}. Defaulting to STRING.")
            master_schema[key] = str

    return master_schema


def convert_to_bq_schema(schema_dict):
    """
    Converts the final inferred Python-type schema into BigQuery's JSON format.

    Args:
        schema_dict (dict): The master schema with Python types.

    Returns:
        list: A list of dictionaries formatted for BigQuery.
    """
    bq_schema = []
    # Sort keys for a consistent schema output order
    for field_name in sorted(schema_dict.keys()):
        field_type = schema_dict[field_name]
        bq_field = {'name': field_name, 'mode': 'NULLABLE'}

        if isinstance(field_type, dict):
            # This is a nested object, a RECORD in BigQuery.
            bq_field['type'] = 'RECORD'
            bq_field['fields'] = convert_to_bq_schema(field_type)
        elif isinstance(field_type, list):
            # This is a REPEATED field.
            bq_field['mode'] = 'REPEATED'
            if not field_type:
                # List was always empty, default to STRING
                element_type = str
            else:
                element_type = field_type[0]

            if isinstance(element_type, dict):
                # An array of objects
                bq_field['type'] = 'RECORD'
                bq_field['fields'] = convert_to_bq_schema(element_type)
            else:
                bq_field['type'] = TYPE_MAPPING.get(element_type, 'STRING')
        else:
            # A standard, primitive type.
            bq_field['type'] = TYPE_MAPPING.get(field_type, 'STRING')
        
        bq_schema.append(bq_field)
    return bq_schema

def main():
    """Main function to orchestrate the schema generation process."""
    parser = argparse.ArgumentParser(
        description="Inspects JSONL files in directories and generates a BigQuery schema."
    )
    parser.add_argument(
        'directories',
        metavar='DIR',
        type=str,
        nargs='+',
        help='One or more directories to scan for .jsonl files.'
    )
    parser.add_argument(
        '-o', '--output',
        type=str,
        default='bigquery_schema.json',
        help='Path to save the output BigQuery schema JSON file. (default: bigquery_schema.json)'
    )
    parser.add_argument(
        '-s', '--sample',
        type=int,
        default=None,
        help='Sample the first N rows of each file for faster processing.'
    )

    args = parser.parse_args()

    master_schema = {}
    file_count = 0

    print("Starting schema inference...")
    for directory in args.directories:
        print(f"Scanning directory: {directory}")
        for root, _, files in os.walk(directory):
            for file in files:
                if file.endswith('.jsonl'):
                    file_path = os.path.join(root, file)
                    file_count += 1
                    print(f"  -> Processing file: {file_path}")
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            for i, line in enumerate(f):
                                if args.sample and i >= args.sample:
                                    print(f"      (sampled first {args.sample} rows)")
                                    break
                                if not line.strip():
                                    continue
                                try:
                                    json_obj = json.loads(line)
                                    obj_schema = infer_schema_from_object(json_obj)
                                    master_schema = merge_schemas(master_schema, obj_schema)
                                except json.JSONDecodeError:
                                    print(f"Warning: Could not decode JSON on a line in {file_path}. Skipping line.")
                    except Exception as e:
                        print(f"Error processing file {file_path}: {e}")

    if not master_schema:
        print("No valid JSONL data found. Could not generate a schema.")
        return

    print(f"\nProcessed {file_count} files.")
    print("Converting inferred schema to BigQuery format...")
    bigquery_schema = convert_to_bq_schema(master_schema)

    print(f"Saving schema to {args.output}...")
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(bigquery_schema, f, indent=2)

    print("\nSchema generation complete!")
    print(f"You can now use '{args.output}' to create your BigQuery table.")
    print("Example bq command:")
    print(f"bq mk --table your_project:your_dataset.your_table {args.output}")


if __name__ == '__main__':
    main()

