import json
import re
import argparse
import sys
import csv

def parse_single_conversation(conv_text, puml_content):
    """Parse a single conversation string and prepend puml content"""
    if not isinstance(conv_text, str) or not conv_text.strip():
        return None
        
    # Start with puml content and system understood
    conversation = [
        {
            "from": "human",
            "value": puml_content.strip()
        },
        {
            "from": "gpt", 
            "value": "system understood"
        }
    ]
    
    # Parse the existing conversation
    exchanges = re.split(r'\[U\]:', conv_text)[1:]  # Split into turns, remove empty first element
    
    for exchange in exchanges:
        parts = re.split(r'\[A\]:', exchange)
        
        if len(parts) == 2:
            user_text = parts[0].strip()
            assistant_text = parts[1].strip()
            
            conversation.extend([
                {
                    "from": "human",
                    "value": user_text.rstrip('\\n')
                },
                {
                    "from": "gpt",
                    "value": assistant_text.rstrip('\\n')
                }
            ])
    
    return conversation

def parse_csv_conversations(csv_file):
    """Parse conversations from CSV file with puml prefixes"""
    all_conversations = []
    conversation_columns = ["chat", "ff1", "ff2", "ff3", "ff4", "ff5", "ff6", "ff7", "ff8", "ff9", "ff10"]
    
    with open(csv_file, 'r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        
        for row in reader:
            puml_content = row.get('puml', '')
            
            # Process each conversation column for this row
            for col in conversation_columns:
                conv_text = row.get(col, '')
                if conv_text and conv_text.strip():
                    conversation = parse_single_conversation(conv_text, puml_content)
                    if conversation:
                        all_conversations.append({"conversations": conversation})
    
    return all_conversations

def process_files(input_files, output_file):
    all_results = []
    
    for input_file in input_files:
        try:
            result = parse_csv_conversations(input_file)
            all_results.extend(result)
            print(f"Successfully processed '{input_file}' - found {len(result)} conversations")
        except FileNotFoundError:
            print(f"Error: Input file '{input_file}' not found.")
            continue
        except Exception as e:
            print(f"Error processing '{input_file}': {e}")
            continue

    if not all_results:
        print("Error: No valid conversations were processed.")
        sys.exit(1)

    try:
        with open(output_file, 'w') as file:
            json.dump(all_results, file, indent=2)
        print(f"All processed conversations saved to '{output_file}' - total: {len(all_results)} conversations")
    except IOError:
        print(f"Error: Unable to write to output file '{output_file}'.")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Parse conversations from CSV files with puml prefixes and output structured data.")
    parser.add_argument("input", nargs='+', help="Input CSV file paths")
    parser.add_argument("-o", "--output", default="output.json", help="Output JSON file path (default: output.json)")
    
    args = parser.parse_args()
    
    process_files(args.input, args.output)

if __name__ == "__main__":
    main()
