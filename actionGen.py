import json
import string
import random
from typing import List, Dict

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
    prev_action = None
    actions = []  # Initialize actions list
    for i, item in enumerate(scene_list[1:]):
        content = item["content"]
        chosen_pos = None

        print("i",i)
        
        # Get actions
        chosen_action = item["action"]
        other_actions = item["altActions"]
        
        # In hidden, get all 4 and use chosen as e
        # TO DO 

        # Determine if we should use 'e' option (10% chance and exactly 4 alt actions)
        use_e_option = random.random() < 0.1 and len(other_actions) == 4
        
        if use_e_option:
            # Use all 4 alternative actions, chosen action will be 'e'
            actions = other_actions[:4]
            random.shuffle(actions)
        else:
            # Get alternative actions (up to 3)
            alt_actions = other_actions[:3]
            # Create full list with chosen action first, followed by alternatives
            actions = [chosen_action] + alt_actions
            random.shuffle(actions)

        letteredActions = []
        # Format actions with letters
        for idx, action in enumerate(actions):
            letter = string.ascii_lowercase[idx]
            letteredActions.append({"letter":letter, "action":action})
            
        if use_e_option:
            # Add chosen action as option 'e'
            letteredActions.append({"letter":"e", "action":chosen_action})
            chosen_pos = len(letteredActions) - 1
        else:
            chosen_pos = None
            for index, item in enumerate(letteredActions):
                if chosen_action in item["action"]:
                    chosen_pos = index
                    break
        print("chosen",chosen_pos)
        if(chosen_pos == None):
            exit(-1)

        # Create menu with actions
        action_menu = "\n\nDo you:"
        for idx, action in enumerate(letteredActions):  
            action_menu += f'\n{action["letter"]}) {action["action"]}'

        content += action_menu

        # For messages after the first assistant message, prefix with previous letter and action
        if i > 0 and prev_action != None :
            prefixed_content = f'{prev_action}\n\n{content}'
        else:
            prefixed_content = content
            
        transformed.append({
            "role": "assistant",
            "content": prefixed_content,
            "action": chosen_action
        })

        transformed.append({
            "role": "user",
            "content": f'{letteredActions[chosen_pos]["letter"]}) {letteredActions[chosen_pos]["action"]}',
            "action": chosen_action
        })

        prev_action = f'{letteredActions[chosen_pos]["letter"]}) {letteredActions[chosen_pos]["action"]}'

    return transformed

def process_scenes(input_file: str, output_file: str):
    """Process all scenes from input file and write to output file."""
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
            

def main():
    input_file = "a.j"
    output_file = "transformed_scenes.json"
            
    process_scenes(input_file, output_file)
    print(f"Successfully processed scenes from '{input_file}' to '{output_file}'")
        

if __name__ == "__main__":
    main()
