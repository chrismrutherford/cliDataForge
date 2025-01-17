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
    prev_chosen_pos = None
    for i, item in enumerate(scene_list[1:], 1):
        content = item["content"]
        chosen_pos = None
        
        # For the first assistant message, add actions as a menu
        if i > 0:
            # Get actions
            chosen_action = item["action"]
            other_actions = item["altActions"]
            
            # Check if we have 5 or more actions total
            has_hidden_option = len(other_actions) >= 4  # 4 alt actions + 1 chosen = 5 total
            
            # 10% chance to use hidden option if available
            use_hidden = has_hidden_option and random.random() < 0.1
            
            if use_hidden:
                # Use first alternative action as hidden option
                chosen_pos = 4  # Position 'e'
                actions = [chosen_action] + other_actions[:3]  # Show only a-d options
                hidden_action = other_actions[0]  # First alt action becomes hidden option
                actions.append(hidden_action)  # Add to actions list but won't be displayed
            else:
                # Normal visible a-d options
                other_actions = other_actions[:3]  # Take only up to 3 alternative actions
                random.shuffle(other_actions)
                chosen_pos = random.randint(0, 3)
                actions = other_actions.copy()
                actions.insert(chosen_pos, chosen_action)
                
                # Ensure exactly 4 visible options
                while len(actions) < 4:
                    actions.append(f"Do nothing {len(actions) + 1}")
            
            # Create menu with visible actions (only a-d)
            action_menu = "\n\nDo you:"
            for idx, action in enumerate(actions[:4]):  # Only show first 4 options
                letter = string.ascii_lowercase[idx]
                action_menu += f"\n{letter}) {action}"
            content += action_menu
        #else:
        #    # For subsequent messages, prepend with the chosen action
        #    content = f'You {item["action"]}\n\n{content}'
        # For messages after the first assistant message, prefix with previous letter and action
        if i > 1 and prev_chosen_pos is not None:
            prev_letter = string.ascii_lowercase[prev_chosen_pos]
            prev_action = scene_list[i-1]["action"]
            prefixed_content = f"{prev_letter}) {prev_action}\n\n{content}"
        else:
            prefixed_content = content
            
        prev_chosen_pos = chosen_pos
            
        transformed.append({
            "role": "assistant", 
            "content": prefixed_content
        })

        if i > 0:
            # For option 'e', always show full action. Otherwise 90/10 chance
            letter = string.ascii_lowercase[chosen_pos]
            chosen_action = actions[chosen_pos]
            if chosen_pos == 4:  # Option 'e'
                user_content = f"{letter}) {chosen_action}"
            else:
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
