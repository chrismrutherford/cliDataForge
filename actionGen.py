import json
import string
import random
from typing import List, Dict

def find_substring_index(lst, substring):
    for index, item in enumerate(lst):
        if substring in str(item):
            return index
    return -1  # Return -1 if no match is found

def transform_scene(scene_list: List[Dict]) -> List[Dict]:
    """Transform a scene into the new format with system/assistant roles."""
    if not scene_list:
        return []
    
    transformed = []
    
    # First message becomes system
    transformed.append({
        "role": "system",
        "content": scene_list[0]["content"],
        "action": None  # System message has no action
    })
    
    # Process subsequent messages
    prev_chosen_pos = None
    prev_chosen_action = None
    actions = []  # Initialize actions list
    for i, item in enumerate(scene_list[1:], 1):
        content = item["content"]
        chosen_pos = None
        prev_action = ""
        
        # For the first assistant message, add actions as a menu
        if i > 0:
            # Get actions
            chosen_action = item["action"]
            other_actions = item["altActions"]
            
            # In hidden, get all 4 and use chosen as e
            # TO DO 

            # Get alternative actions (up to 3)
            alt_actions = other_actions[:3]
            
            # Create full list with chosen action first, followed by alternatives
            actions = [chosen_action] + alt_actions

            random.shuffle(actions)

            letteredActions=[]
            # Format actions with letters
            for idx, action in enumerate(actions):
                letter = string.ascii_lowercase[idx]
                letteredActions.append({"letter":letter,"action":action})

            for index, item in enumerate(lst):
                if substring in item["action"]:
                    break
                    prev_action = f'n{acton["letter"]})  {action["action"]}'
                    print("index", index)

            # Create menu with actions
            action_menu = "\n\nDo you:"
            for idx, action in enumerate(letteredActions):  
                action_menu += f'n{acton["letter"]})  {action["action"]}'

            content += action_menu

        # For messages after the first assistant message, prefix with previous letter and action
        if i > 1 and prev_action is not None:
            prefixed_content = f"{prev_action}\n\n{content}"
        else:
            prefixed_content = content
            
        transformed.append({
            "role": "assistant",
            "content": prefixed_content,
            "action": actions[0] if i > 0 else None
        })

    return transformed

def process_scenes(input_file: str, output_file: str):
    """Process all scenes from input file and write to output file."""
    try:
        with open(input_file, 'r') as f:
            scenes = json.load(f)
        
        # Transform each scene
        transformed_scenes = []
        for scene in scenes:
            transformed = transform_scene(scene)
            transformed_scenes.append(transformed)
        
        # Write the transformed scenes to output file
        with open(output_file, 'w') as f:
            json.dump(transformed_scenes, f, indent=2)
            
    except Exception as e:
        print(f"Error processing file: {e}")

def main():
    try:
        input_file = "a.j"
        output_file = "transformed_scenes.json"
            
        process_scenes(input_file, output_file)
        print(f"Successfully processed scenes from '{input_file}' to '{output_file}'")
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()
