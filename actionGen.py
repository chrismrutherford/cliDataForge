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

        # Determine if we should include a hidden option 'e'
        include_hidden_e = len(other_actions) >= 4 or random.random() < 0.1
        
        # Get alternative actions (up to 3, or 4 if we're including hidden e)
        alt_actions = other_actions[:4] if include_hidden_e else other_actions[:3]
        # Create full list with chosen action first, followed by alternatives
        actions = [chosen_action] + alt_actions
        random.shuffle(actions)
        
        # If including hidden e, ensure chosen action is also in position 4
        if include_hidden_e:
            actions = actions[:4]  # Trim to first 4 actions

        letteredActions = []
        # Format actions with letters
        for idx, action in enumerate(actions):
            letter = string.ascii_lowercase[idx]
            letteredActions.append({"letter":letter, "action":action})
            
        chosen_pos = None
        for index, item in enumerate(letteredActions):
            if chosen_action in item["action"]:
                chosen_pos = index
                break
        print("chosen",chosen_pos)
        if(chosen_pos == None):
            exit(-1)

        # Create menu with actions (excluding hidden e)
        action_menu = "\n\nDo you:"
        for idx, action in enumerate(letteredActions):
            if idx < 4:  # Only show a-d options
                action_menu += f'\n{action["letter"]}) {action["action"]}'
                
        # If we have hidden e, add chosen action as hidden option
        if include_hidden_e:
            letteredActions.append({"letter": "e", "action": chosen_action})

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
