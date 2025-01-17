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
        "content": scene_list[0]["content"]
    })
    
    # Process subsequent messages
    for i, item in enumerate(scene_list[1:], 1):
        content = item["content"]
        
        # For the first assistant message, add actions as a menu
        if i > 0:
            # Get all actions and shuffle them
            chosen_action = item["action"]
            all_actions = [chosen_action] + item["altActions"]
            other_actions = item["altActions"]
            random.shuffle(other_actions)
            
            # Randomly select position for chosen action
            chosen_pos = random.randint(0, len(all_actions) - 1)
            
            # Build shuffled action list with chosen action at random position
            actions = other_actions.copy()
            actions.insert(chosen_pos, chosen_action)
            
            # Create menu with shuffled actions
            action_menu = "\n\nDo you:"
            for idx, action in enumerate(actions):
                letter = string.ascii_lowercase[idx]
                action_menu += f"\n{letter}) {action}"
            content += action_menu
        #else:
        #    # For subsequent messages, prepend with the chosen action
        #    content = f'You {item["action"]}\n\n{content}'
        transformed.append({
            "role": "assistant",
            "content": content
        })

        if i > 0:
            # 90% chance for just the letter, 10% chance for letter + action
            letter = string.ascii_lowercase[chosen_pos]
            user_content = letter if random.random() < 0.9 else f"{letter}) {chosen_action}"
            transformed.append({
                "role": "user",
                "content": user_content
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
    input_file = "a.j"
    output_file = "transformed_scenes.json"
    process_scenes(input_file, output_file)

if __name__ == "__main__":
    main()
