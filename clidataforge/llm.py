from openai import OpenAI
from typing import List, Dict, Any, Optional
import os
import time
from dotenv import load_dotenv

class LLMClient:
    """Wrapper for LLM interactions using OpenRouter"""
    
    def __init__(self, api_key: str = None, app_name: str = "cliDataForge", site_url: str = None, base_url: str = None):
        load_dotenv()
        self.api_key = api_key or os.getenv("LLAMAFLOW_API_KEY")
        if not self.api_key:
            raise ValueError("API key not found")
            
        self.client = OpenAI(
            base_url=base_url or os.getenv("OPENAI_BASE_URL", "https://openrouter.ai/api/v1"),
            api_key=self.api_key
        )
        
        self.extra_headers = {
            "X-Title": app_name
        }
        if site_url:
            self.extra_headers["HTTP-Referer"] = site_url
            
    def build_messages(self, prompt: str, system_prompt: str, 
                      previous_response: Optional[str] = None) -> List[Dict[str, str]]:
        """Build message list for chat completion"""
        messages = [{"role": "system", "content": system_prompt}]
        
        if previous_response:
            messages.append({
                "role": "user",
                "content": previous_response
            })
            
        if prompt:
            messages.append({
                "role": "user",
                "content": prompt
            })
            
        return messages

    def complete(self, messages: List[Dict[str, str]], 
                model: str = "meta-llama/llama-3.3-70b-instruct",
                max_retries: int = 3) -> str:
        """Send a completion request to the LLM with retry logic"""
        for attempt in range(max_retries):
            try:
                completion = self.client.chat.completions.create(
                    extra_headers=self.extra_headers,
                    model=model,
                    messages=messages,
                    temperature=1,
                    max_tokens=2048,
                )
                
                if completion and completion.choices:
                    return completion.choices[0].message.content
                    
            except Exception as e:
                print(f"Error in completion (attempt {attempt + 1}/{max_retries}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(1)
                    
        return f"Error: Maximum retries ({max_retries}) exceeded"
