import json
import argparse
import traceback
import time
import os
import sqlite3
from datetime import datetime
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import fcntl
from typing import List
from LlamaCppApi import LlamaCppApi
from transformers import AutoTokenizer
import textract
import subprocess
import tempfile



NUM_THREADS=1

def setup_database():
    """Create a database to track processed files"""
    conn = sqlite3.connect('processed_files.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS processed_files
                 (filepath TEXT PRIMARY KEY, 
                  status TEXT,
                  timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)''')
    conn.commit()
    conn.close()

def is_file_processed(filepath: str) -> bool:
    """Check if file has already been processed"""
    conn = sqlite3.connect('processed_files.db')
    c = conn.cursor()
    c.execute('SELECT status FROM processed_files WHERE filepath = ?', (filepath,))
    result = c.fetchone()
    conn.close()
    return result is not None and result[0] == 'completed'

def mark_file_status(filepath: str, status: str):
    """Mark file as processed in database"""
    conn = sqlite3.connect('processed_files.db')
    c = conn.cursor()
    c.execute('''INSERT OR REPLACE INTO processed_files (filepath, status) 
                 VALUES (?, ?)''', (filepath, status))
    conn.commit()
    conn.close()

def get_file_lock(filepath: str) -> bool:
    """Try to acquire a lock for processing a file"""
    lock_file = f"{filepath}.lock"
    try:
        fd = os.open(lock_file, os.O_CREAT | os.O_WRONLY)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except (IOError, OSError):
        return False

def release_file_lock(filepath: str):
    """Release the lock file"""
    lock_file = f"{filepath}.lock"
    try:
        os.remove(lock_file)
    except OSError:
        pass

def get_supported_files(directory: str) -> List[str]:
    """Get list of supported files (.pdf, .txt, and .log) from directory tree"""
    supported_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.lower().endswith(('.pdf', '.txt', '.log', '.epub', '.mobi')):
                supported_files.append(os.path.join(root, file))
    return supported_files

parser = argparse.ArgumentParser(description='Process text/PDF files with QA system')
parser.add_argument("-s", "--source", type=str, required=True, help="Source directory path")
parser.add_argument("-u", "--base-url", type=str, default="http://192.168.1.158:8080", help="LlamaCpp API base URL")
parser.add_argument("-k", "--api-key-env", type=str, default="OPENAI_API_KEY", help="Environment variable name containing the API key (not used with LlamaCpp)")
parser.add_argument("-m", "--model", type=str, default="gpt-3.5-turbo", help="Model to use for completion (e.g. gpt-3.5-turbo, gpt-4)")
parser.add_argument("-p", "--prompt-template", type=str, required=True, help="Path to the prompt template file")
args = parser.parse_args()

if not os.path.exists(args.source):
    print(f"Error: Source directory '{args.source}' does not exist")
    exit(1)


import re

class FileReader:
    def __init__(self, filename):
        self.filename = filename
        self.text = self._read_file()
        self.words = self.text.split()  # split into words
        self.index = 0
        self.N = 1000  # default value for N
        self.M = 50   # default value for M
        
    def _read_file(self):
        """Read content from file using textract or calibre for mobi"""
        try:
            if self.filename.lower().endswith('.mobi'):
                # Create a temporary file for the converted text
                with tempfile.NamedTemporaryFile(suffix='.txt', delete=False) as tmp_file:
                    tmp_path = tmp_file.name
                
                # Convert mobi to txt using calibre's ebook-convert
                subprocess.run(['ebook-convert', self.filename, tmp_path], 
                             check=True, capture_output=True)
                
                # Read the converted text file
                with open(tmp_path, 'r', encoding='utf-8') as f:
                    text = f.read()
                
                # Clean up temporary file
                os.unlink(tmp_path)
            else:
                # Use textract for other file types
                text = textract.process(self.filename).decode('utf-8')
            print(f"File contents of {self.filename}:", text)
            return text
        except Exception as e:
            print(f"Error extracting text from {self.filename}: {str(e)}")
            return ""

    def set_block_size(self, N, M):
        self.N = N
        self.M = M

    def next_block(self):
        if self.index >= len(self.words):
            return None
        words_in_block = []
        count = 0
        #while count < self.N and self.index >= len(self.words):
        while count < self.N and self.index < len(self.words):
            word = self.words[self.index]
            words_in_block.append(word)
            self.index += 1
            count += 1
            #if '.' in word or (self.index < len(self.words) and self.words[self.index-1].endswith('\n')):
            #    break
        print("progress", self.index/len(self.words)*100)
        overlap_words = []
        for i in range(1, min(self.M+1, len(words_in_block))):
            overlap_words.append(words_in_block[-i])
        words_in_block.extend(overlap_words)
        return ' '.join(words_in_block)

class TextCompletionAPI:
    def __init__(self, base_url, api_key_env="OPENAI_API_KEY"):
        self.client = LlamaCppApi(base_url=base_url)
        model_name = "Qwen/Qwen3-235B-A22B"
        self.tokenizer = AutoTokenizer.from_pretrained(model_name, trust_remote_code=True)
        
    def complete_text(self, messages, options):
        """
        Sends messages to the LlamaCpp API and returns the predicted completion.
        
        Parameters:
            messages (list): List of message dictionaries with role and content
            options (dict): A dictionary of options for controlling the completion
                          generation.
        
        Returns:
            dict: The API response content
        """
        # Apply chat template to create a single text prompt
        prompt_text = self.tokenizer.apply_chat_template(
            messages,
            tokenize=False,
            add_generation_prompt=True,
            enable_thinking=False
        )
        
        # Use LlamaCppApi instead of OpenAI
        response = self.client.post_completion(prompt_text, {
            "n_predict": options.get("max_tokens", 4096),
            "stop": ["<|im_end|>", "</s>", "<|end|>", "<|eot_id|>", self.tokenizer.eos_token]
        })
        
        if response and response.status_code == 200:
            return {"content": response.json().get("content", "")}
        else:
            raise Exception(f"API request failed: {response}")

def genPrompt(data, system_prompt):
    """Generate system and user messages"""
    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": data}
    ]


class ProcessingStatus:
    def __init__(self):
        self.active_files = {}  # filepath -> start_time
        self.completed_files = set()
        self.failed_files = set()
        self.total_requests = 0
        self.lock = threading.Lock()

    def start_file(self, filepath):
        with self.lock:
            self.active_files[filepath] = time.time()

    def complete_file(self, filepath, success=True):
        with self.lock:
            if filepath in self.active_files:
                del self.active_files[filepath]
            if success:
                self.completed_files.add(filepath)
            else:
                self.failed_files.add(filepath)

    def increment_requests(self):
        with self.lock:
            self.total_requests += 1

    def get_status(self):
        with self.lock:
            return {
                'active_files': len(self.active_files),
                'completed_files': len(self.completed_files),
                'failed_files': len(self.failed_files),
                'total_requests': self.total_requests,
                'files_in_progress': list(self.active_files.keys())
            }

# Global status tracker
processing_status = ProcessingStatus()

def process_blocks_parallel(blocks, api, options, system_prompt):
    """Process multiple blocks in parallel and return results in order"""
    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = []
        for block in blocks:
            future = executor.submit(process_single_block, block, api, options, system_prompt)
            futures.append(future)
        
        results = []
        for future in futures:
            results.append(future.result())
        return results

def process_single_block(block, api, options, system_prompt):
    """Process a single block with retries"""
    for i in range(10):
        try:
            words = block.split()
            options["n_predict"] = len(words)
            messages = genPrompt(block, system_prompt)
            processing_status.increment_requests()
            completion = api.complete_text(messages, options)
            text = completion["content"]
            if len(text) > 50:
                return {"prompt": block, "response": text}
            print(f"Short response (length={len(text)}), retrying...")
        except Exception as e:
            print(f"Error processing block: {str(e)}")
            traceback.print_exc()
            time.sleep(1)
    return {"prompt": block, "response": ""}

def process_file(filepath: str, api_base_url: str, api_key_env: str, model: str, prompt_template: str):
    """Process a single file"""
    print(f"Processing file: {filepath}")
    processing_status.start_file(filepath)
    
    if is_file_processed(filepath):
        print(f"File already processed: {filepath}")
        return

    try:
        mark_file_status(filepath, 'processing')
        dest_path = filepath + '.QA'
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)

        api = TextCompletionAPI(api_base_url, api_key_env)
        options = {
            "max_tokens": 4096
        }

        # Load the system prompt
        with open(prompt_template) as f:
            system_prompt = f.read().strip()
            
        reader = FileReader(filepath)
        reader.set_block_size(1024, 100)  # Increased from 100,10 to 1000,100 for larger chunks

        with open(dest_path, 'w') as file:
            while True:
                # Collect up to 4 blocks to process in parallel (reduced due to larger chunks)
                blocks = []
                for _ in range(NUM_THREADS):
                    block = reader.next_block()
                    if block is None:
                        break
                    blocks.append(block)
                
                if not blocks:
                    break

                start_time = time.time()
                results = process_blocks_parallel(blocks, api, options, system_prompt)
                end_time = time.time()
                
                # Write results in order
                for result in results:
                    data = {"chunk": result}
                    print ("data", data)
                    file.write(json.dumps(data) + "\n")
                
                elapsed = int(end_time - start_time)
                total_bytes = sum(len(block) for block in blocks)
                if elapsed > 0 and total_bytes > 1024:
                    KBps = (total_bytes/1024)/elapsed
                    print(f"Processed {len(blocks)} blocks - total bytes {total_bytes}, time {elapsed}s, KBps {KBps}")

        mark_file_status(filepath, 'completed')
        processing_status.complete_file(filepath, success=True)
        print(f"Successfully completed processing: {filepath}")

    except Exception as e:
        print(f"Error processing {filepath}: {str(e)}")
        mark_file_status(filepath, 'failed')
        processing_status.complete_file(filepath, success=False)
    finally:
        release_file_lock(filepath)

def log_status():
    """Periodically log processing status"""
    while True:
        status = processing_status.get_status()
        active_files = ", ".join(f.split('/')[-1] for f in status['files_in_progress'])
        print(f"Status [{datetime.now().strftime('%H:%M:%S')}] Active: {status['active_files']}, Completed: {status['completed_files']}, Failed: {status['failed_files']}, Requests: {status['total_requests']}, Processing: [{active_files}]", flush=True)
        time.sleep(10)

def main():
    setup_database()
    
    # Start status logging in background thread
    status_thread = threading.Thread(target=log_status, daemon=True)
    status_thread.start()
    
    # Get list of all supported files
    files_to_process = get_supported_files(args.source)
    print(f"Found {len(files_to_process)} files to process:")
    for f in files_to_process:
        print(f"  {f}")
    
    # Process files sequentially
    for filepath in files_to_process:
        try:
            process_file(filepath, args.base_url, args.api_key_env, args.model, args.prompt_template)
        except Exception as e:
            print(f"Failed to process {filepath}: {str(e)}")

if __name__ == "__main__":
    main()

