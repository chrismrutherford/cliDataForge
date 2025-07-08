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

from .llamacpp.LlamaCppApi import LlamaCppApi
from transformers import AutoTokenizer
from typing import List, Dict, Any, Optional
import os
import time
from dotenv import load_dotenv

class LLMClient:
    """Wrapper for LLM interactions using LlamaCpp API"""
    
    def __init__(self, api_key: str = None, app_name: str = "cliDataForge", site_url: str = None, base_url: str = None):
        load_dotenv()
        self.api_key = api_key or os.getenv("CLI_DF_API_KEY")
        
        self.base_url = base_url or os.getenv("CLI_DF_BASE_URL", "http://192.168.1.158:8080")
        print(f"LLMClient using base_url: {self.base_url}")  # Debug output
        print(f"LLMClient using api_key: {self.api_key}")  # Debug output
        self.client = LlamaCppApi(base_url=self.base_url, api_key=self.api_key)
        
        # Initialize tokenizer
        model_name = "Qwen/Qwen3-235B-A22B"
        self.tokenizer = AutoTokenizer.from_pretrained(model_name, trust_remote_code=True)
        
        self.app_name = app_name
        self.site_url = site_url
            
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
        # Model parameter is kept for compatibility but not used with LlamaCpp
        model = model or os.getenv("CLI_DF_MODEL", "llama-chat")
        
        for attempt in range(max_retries):
            try:
                # Apply chat template to create a single text prompt
                prompt_text = self.tokenizer.apply_chat_template(
                    messages,
                    tokenize=False,
                    add_generation_prompt=True,
                    enable_thinking=False
                )
                
                # Use LlamaCppApi for completion
                response = self.client.post_completion(prompt_text, {
                    "n_predict": 8192,  # equivalent to max_tokens
                    "temperature": 1.2,
                    "stop": ["<|im_end|>", "</s>", "<|end|>", "<|eot_id|>", self.tokenizer.eos_token]
                })
                
                if response and response.status_code == 200:
                    content = response.json().get("content", "")
                    if content:
                        return content
                    else:
                        raise Exception("Empty response content")
                else:
                    raise Exception(f"API request failed with status: {response.status_code if response else 'No response'}")
                    
            except Exception as e:
                print(f"Error in completion (attempt {attempt + 1}/{max_retries}): {str(e)}")
                print(f"Base URL being used: {self.base_url}")
                print(f"Messages: {messages}")
                if "Content Exists Risk" in str(e):
                    return "400"
                if attempt < max_retries - 1:
                    # Exponential backoff: wait longer between each retry
                    time.sleep(2 ** attempt)  # 1s, 2s, 4s between retries
                    
        return f"Error: Maximum retries ({max_retries}) exceeded"
