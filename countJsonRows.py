#!/usr/bin/env python3

import json
import sys
from pathlib import Path

def count_json_rows(file_path: str) -> int:
    """Count the number of items in a JSON list file."""
    with open(file_path, 'r') as f:
        data = json.load(f)
        
    if not isinstance(data, list):
        raise ValueError("JSON root element must be a list")
        
    return len(data)

def main():
    if len(sys.argv) != 2:
        print("Usage: countJsonRows.py <json_file>")
        sys.exit(1)
        
    file_path = sys.argv[1]
    if not Path(file_path).exists():
        print(f"Error: File '{file_path}' not found")
        sys.exit(1)
        
    try:
        count = count_json_rows(file_path)
        print(f"Number of items: {count}")
    except json.JSONDecodeError:
        print("Error: Invalid JSON file")
        sys.exit(1)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
