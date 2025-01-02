# Copyright (C) 2024 Christopher Rutherford
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from openai import OpenAI
from typing import List, Dict, Any, Optional
import os
import time
from dotenv import load_dotenv

class LLMClient:
    """Wrapper for LLM interactions using OpenRouter"""
    
    def __init__(self, api_key: str = None, app_name: str = "cliDataForge", site_url: str = None, base_url: str = None):
        load_dotenv()
        self.api_key = api_key or os.getenv("CLI_DF_API_KEY")
        if not self.api_key:
            raise ValueError("API key not found")
            
        self.client = OpenAI(
            base_url=base_url or os.getenv("CLI_DF_BASE_URL", "https://api.deepseek.com"),
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
                model: str = None,
                max_retries: int = 3) -> str:
        """Send a completion request to the LLM with retry logic"""
        # Use environment variable if model not specified
        model = model or os.getenv("CLI_DF_MODEL", "deepseek-chat")
        """Send a completion request to the LLM with retry logic"""
        for attempt in range(max_retries):
            try:
                completion = self.client.chat.completions.create(
                    extra_headers=self.extra_headers,
                    model="deepseek-chat",
                    messages=messages,
                    temperature=1.2,
                    max_tokens=8192,
                )
                
                if completion and completion.choices:
                    return completion.choices[0].message.content
                    
            except Exception as e:
                print(f"Error in completion (attempt {attempt + 1}/{max_retries}): {str(e)}")
                print(f"messages", messages)
                if("Content Exists Risk" in str(e)):
                    return "400"
                if attempt < max_retries - 1:
                    time.sleep(1)
                    
        return f"Error: Maximum retries ({max_retries}) exceeded"
