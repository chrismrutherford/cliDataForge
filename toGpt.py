
            if len(parts) == 2:
                user_text =
                assistant_text = 

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

