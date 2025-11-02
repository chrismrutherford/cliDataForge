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
            raise ValueError("CLI_DF_API_KEY environment variable must be set")
            
        base_url = base_url or os.getenv("CLI_DF_BASE_URL")
        if not base_url:
            raise ValueError("CLI_DF_BASE_URL environment variable must be set")
            
        self.client = OpenAI(
            base_url=base_url,
            api_key=self.api_key,
            timeout=300.0  # 300 second timeout

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
                max_retries: int = 3) -> str:
        """Send a completion request to the LLM with retry logic"""
        model = os.getenv("CLI_DF_MODEL")
        if not model:
            raise ValueError("CLI_DF_MODEL environment variable must be set")
        for attempt in range(max_retries):
            try:
                completion = self.client.chat.completions.create(
                    extra_headers=self.extra_headers,
                    model=model,
                    messages=messages,
                    timeout=300.0,  # 300 second timeout for individual requests
                    max_tokens=16384,
                    reasoning_effort="high"
                )
                
                if completion and completion.choices:
                    return completion.choices[0].message.content
                    
            except Exception as e:
                print(f"Error in completion (attempt {attempt + 1}/{max_retries}): {str(e)}")
                print(f"messages", messages)
                if("Content Exists Risk" in str(e)):
                    return "400"
                if attempt < max_retries - 1:
                    # Exponential backoff: wait longer between each retry
                    time.sleep(2 ** attempt)  # 1s, 2s, 4s between retries
                    
        return f"Error: Maximum retries ({max_retries}) exceeded"
