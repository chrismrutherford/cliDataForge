import json
import argparse

def convert_to_sharegpt(input_file: str, output_file: str):
    """Convert conversations from role/content format to ShareGPT format."""
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    all_conversations = []
    
    for conversation in data:
        current_conversation = []
        for message in conversation:
            role = message.get("role")
            content = message.get("content", "").rstrip('\\n')
            
            if role == "user":
                from_role = "human"
            elif role == "gpt" or role == "assistant":
                from_role = "gpt"
            else:
                continue  # Skip system messages or unknown roles
                
            current_conversation.append({
                "from": from_role,
                "value": content
            })

        if current_conversation:  # Only add non-empty conversations
            all_conversations.append({"conversations": current_conversation})

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_conversations, f, indent=2, ensure_ascii=False)

def main():
    parser = argparse.ArgumentParser(description='Convert conversations to ShareGPT format')
    parser.add_argument('input_file', help='Input JSON file path')
    parser.add_argument('output_file', help='Output JSON file path')
    args = parser.parse_args()

    convert_to_sharegpt(args.input_file, args.output_file)

if __name__ == "__main__":
    main()

