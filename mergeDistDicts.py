import json
import sys

def merge_lists(dict_list, alt_actions_list):
    """
    Merge a list of dictionaries with a list of lists.
    Each list from alt_actions_list will be added as 'altActions' to the corresponding dict.
    """
    if len(dict_list) != len(alt_actions_list):
        raise ValueError("Lists must be of equal length")
        
    merged = []
    for dict_item, alt_actions in zip(dict_list, alt_actions_list):
        new_dict = dict_item.copy()
        new_dict['altActions'] = alt_actions
        merged.append(new_dict)
    
    return merged

def main():
    if len(sys.argv) != 3:
        print("Usage: python mergeDistDicts.py <dict_list.json> <alt_actions.json>")
        sys.exit(1)
        
    try:
        # Read the JSON files
        with open(sys.argv[1], 'r') as f:
            dict_list = json.load(f)
        with open(sys.argv[2], 'r') as f:
            alt_actions_list = json.load(f)
            
        # Merge the lists
        result = merge_lists(dict_list, alt_actions_list)
        
        # Output the merged result
        print(json.dumps(result, indent=2))
        
    except FileNotFoundError as e:
        print(f"Error: Could not find file - {e}")
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON format - {e}")
    except ValueError as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
