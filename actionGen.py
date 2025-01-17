import json
import string
from typing import List, Dict

def transform_scene(scene_list: List[Dict]) -> List[Dict]:
    """Transform a scene into the new format with system/assistant roles."""
    if not scene_list:
        return []
    
    transformed = []
    
    # First message becomes system
    transformed.append({
        "role": "system",
        "content": scene_list[0]["content"]
    })
    
    # Process subsequent messages
    for i, item in enumerate(scene_list[1:], 1):
        content = item["content"]
        
        # For the first assistant message, add actions as a menu
        if i == 1:
            actions = [item["action"]] + item["altActions"]
            action_menu = "\n\nDo you:"
            for idx, action in enumerate(actions):
                action_menu += f"\n{string.ascii_lowercase[idx]}) {action}"
            content += action_menu
        else:
            # For subsequent messages, prepend with the chosen action
            content = f'You {item["action"]}\n\n{content}'
        
        transformed.append({
            "role": "assistant",
            "content": content
        })
    
    return transformed

def process_scenes(input_file: str, output_file: str):
    """Process all scenes from input file and write to output file."""
    try:
        with open(input_file, 'r') as f:
            scenes = json.load(f)
        
        # Group scenes by scene_number
        scene_groups = {}
        for item in scenes:
            scene_num = item["scene_number"]
            if scene_num not in scene_groups:
                scene_groups[scene_num] = []
            scene_groups[scene_num].append(item)
        
        # Transform each scene group
        transformed_scenes = []
        for scene_num in sorted(scene_groups.keys()):
            transformed = transform_scene(scene_groups[scene_num])
            transformed_scenes.append(transformed)
        
        # Write the transformed scenes to output file
        with open(output_file, 'w') as f:
            json.dump(transformed_scenes, f, indent=2)
            
    except Exception as e:
        print(f"Error processing file: {e}")

def main():
    input_file = "input.json"
    output_file = "transformed_scenes.json"
    process_scenes(input_file, output_file)

if __name__ == "__main__":
    main()
