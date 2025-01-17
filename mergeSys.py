import json
from typing import List, Dict
import os

def read_json_file(filename: str) -> str:
    """Read and return the contents of a JSON file"""
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            return json.load(f)
    return ""

def add_plot_to_scenes(scenes: List[List[Dict]]) -> List[List[Dict]]:
    """Add plot information from referenced files to each scene list"""
    modified_scenes = []
    
    for scene_list in scenes:
        if not scene_list:  # Skip empty lists
            continue
            
        # Get the filename and list_index from the first scene in the list
        filename = scene_list[0].get('filename', '')
        list_index = scene_list[0].get('list_index', 0)
        
        # Read the plot from the referenced file
        plot_content = read_json_file(filename)
        
        # Create a new plot entry
        plot_entry = {
            "scene_number": 0,  # Use 0 for the plot entry
            "content": plot_content,
            "action": "",
            "filename": filename,
            "list_index": list_index,  # Use the same list_index as the scenes
            "altActions": ["", "", ""]
        }
        
        # Add plot as first item in the list
        modified_scene_list = [plot_entry] + scene_list
        modified_scenes.append(modified_scene_list)
    
    return modified_scenes

def main():
    # Read input JSON from stdin
    try:
        input_data = json.load(sys.stdin)
        modified_data = add_plot_to_scenes(input_data)
        print(json.dumps(modified_data, indent=2))
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    import sys
    main()
