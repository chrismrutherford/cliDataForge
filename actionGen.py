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
        prev_actions = actions  # Store previous actions for reference
        
        # For the first assistant message, add actions as a menu
        if i > 0:
            # Get actions
            chosen_action = item["action"]
            other_actions = item["altActions"]
            
            # Create full list of actions including chosen and alternatives
            actions = [chosen_action] + other_actions[:3]  # Take chosen action plus up to 3 alternatives
            
            # Ensure exactly 4 options
            while len(actions) < 4:
                actions.append(f"Do nothing {len(actions) + 1}")
                
            # Randomize the order
            random.shuffle(actions)

            for idx, action in enumerate(actions[:4]):  # Only show first 4 options
                letter = string.ascii_lowercase[idx]
                actions[idx]= f"\n{letter}) {action}"

            action_index = find_substring_index(actions,chosen_action)

            chosen_action = actions[action_index]

            # Create menu with visible actions (only a-d)
            action_menu = "\n\nDo you:"
            for idx, action in enumerate(actions[:4]):  # Only show first 4 options
                action_menu += f"\n{action}"
            content += action_menu

        # For messages after the first assistant message, prefix with previous letter and action
        if i > 1 and prev_chosen_action is not None:
            prefixed_content = f"{prev_chosen_action}\n\n{content}"
        else:
            prefixed_content = content
            
        prev_chosen_action = chosen_action if i > 0 else None
            
        transformed.append({
            "role": "assistant",
            "content": prefixed_content,
            "action": actions[0] if i > 0 else None
        })

        if i > 0:
            # Use the chosen action letter
            action_index = find_substring_index(actions, chosen_action.strip())
            if action_index >= 0:
                letter = string.ascii_lowercase[action_index]
                user_content = letter
            else:
                user_content = "a"  # fallback
            transformed.append({
                "role": "user",
                "content": user_content,
                "action": actions[0]  # Always use the first action for debugging
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
