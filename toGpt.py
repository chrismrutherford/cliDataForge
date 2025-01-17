
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

