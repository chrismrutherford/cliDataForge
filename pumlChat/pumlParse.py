import json
import re
import argparse
import sys

def parse_conversation(conversations):
    all_conversations = []
    
    for conv in conversations:
        if not isinstance(conv, str) or not conv.strip():  # Skip empty conversations
            continue
            
        exchanges = re.split(r'\[U\]:', conv)[1:]  # Split into turns, remove empty first element
        current_conversation = []
        
        for exchange in exchanges:
            parts = re.split(r'\[A\]:', exchange)
            
            if len(parts) == 2:
                user_text = parts[0].strip()
                assistant_text = parts[1].strip()
                
                current_conversation.extend([
                    {
                        "from": "human",
                        "value": user_text.rstrip('\\n')
                    },
                    {
                        "from": "gpt",
                        "value": assistant_text.rstrip('\\n')
                    }
                ])
        
        if current_conversation:  # Only add non-empty conversations
            all_conversations.append({"conversations": current_conversation})
    
    return all_conversations

def process_files(input_files, output_file):
    all_results = []
    
    for input_file in input_files:
        try:
            with open(input_file, 'r') as file:
                conversations = json.load(file)
                if not isinstance(conversations, list):
                    print(f"Error: Input JSON in '{input_file}' must be a list of strings")
                    continue
                result = parse_conversation(conversations)
                all_results.extend(result)
                print(f"Successfully processed '{input_file}'")
        except FileNotFoundError:
            print(f"Error: Input file '{input_file}' not found.")
            continue
        except json.JSONDecodeError:
            print(f"Error: Invalid JSON in input file '{input_file}'.")
            continue
        except IOError:
            print(f"Error: Unable to read input file '{input_file}'.")
            continue

    if not all_results:
        print("Error: No valid conversations were processed.")
        sys.exit(1)

    try:
        with open(output_file, 'w') as file:
            json.dump(all_results, file, indent=2)
        print(f"All processed conversations saved to '{output_file}'")
    except IOError:
        print(f"Error: Unable to write to output file '{output_file}'.")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Parse conversations from JSON files and output structured data.")
    parser.add_argument("input", nargs='+', help="Input JSON file paths")
    parser.add_argument("-o", "--output", default="output.json", help="Output JSON file path (default: output.json)")
    
    args = parser.parse_args()
    
    process_files(args.input, args.output)

if __name__ == "__main__":
    main()
