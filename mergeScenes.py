import json
from collections import defaultdict
from typing import List, Dict

def merge_scenes(scenes: List[Dict]) -> List[List[Dict]]:
    """
    Merge scenes with the same scene number into lists while preserving order.
    
    Args:
        scenes: List of scene dictionaries
        
    Returns:
        List of lists where each sublist contains scenes with the same scene number
    """
    # Create a dictionary to store scenes grouped by scene number
    scene_groups = defaultdict(list)
    
    # Group scenes by scene number while maintaining order
    for scene in scenes:
        scene_number = scene['scene_number']
        scene_groups[scene_number].append(scene)
    
    # Convert the grouped dictionary to a list of lists, sorted by scene number
    merged_scenes = [scene_groups[number] for number in sorted(scene_groups.keys())]
    
    return merged_scenes

def main():
    # Read input file
    with open('scenes.json', 'r') as f:
        scenes = json.load(f)
    
    # Merge scenes
    merged_scenes = merge_scenes(scenes)
    
    # Write output to a new file
    with open('merged_scenes.json', 'w') as f:
        json.dump(merged_scenes, f, indent=2)

if __name__ == '__main__':
    main()
