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

from datetime import datetime
import time
import os
from typing import List, Tuple, Optional
from .llm import LLMClient
from .db import DatabaseHandler

class PipelineExecutor:
    """Executes the LLM pipeline with database integration"""
    
    def __init__(self, llm_client: LLMClient, db_handler: DatabaseHandler, stages: str):
        self.llm = llm_client
        self.db = db_handler
        self.model = os.getenv("CLI_DF_MODEL")
        if not self.model:
            raise ValueError("CLI_DF_MODEL environment variable must be set")
        # Parse stages into source:destination pairs
        # Source can be multiple columns concatenated with +
        self.stages = []
        for stage in stages.split(','):
            source_part, dest = stage.strip().split(':')
            # Handle potential multiple source columns (src1+src2)
            source = source_part.strip()
            self.stages.append((source, dest.strip()))
            
        print(f"\nInitializing pipeline with stages: {self.stages}")
        
        # Validate columns exist and are spelled correctly
        self.validate_pipeline_columns()
        
    def process_stage(self, chunk_index: int, source_col: str, dest_col: str,
                     prompt: Optional[str], previous_response: Optional[str]) -> str:
        """Process a single stage of the pipeline
        
        For the first stage, source_col can be multiple columns concatenated with +
        """
        system_prompt = self.db.get_system_prompt(dest_col)
        if not system_prompt:
            print(f"\nFATAL ERROR: No system prompt found for destination column '{dest_col}'")
            print("\nTo fix this, run the following command to add a system prompt:")
            print(f"python -m clidataforge add-prompt {dest_col} <path-to-prompt-file>")
            print("\nOr create a prompt file and add it with:")
            print(f"echo 'Your prompt text here' > prompt.txt")
            print(f"python -m clidataforge add-prompt {dest_col} prompt.txt")
            raise ValueError(f"Missing system prompt for '{dest_col}'")
            
        messages = self.llm.build_messages(prompt, system_prompt, previous_response)
        response = self.llm.complete(messages)
        
        if "Error:" in response:
            raise ValueError(f"LLM error in {dest_col}: {response}")
            
        self.db.update_pipeline_result(chunk_index, dest_col, response)
        return response
        
    def execute_pipeline(self, chunk_index: int, initial_prompt: str) -> List[str]:
        """Execute the full pipeline for a single chunk"""
        cycle_start = datetime.now()
        responses = []
        previous_response = None
        
        print(f"\nProcessing chunk {chunk_index} at {cycle_start.strftime('%H:%M:%S')}")
        
        for source_col, dest_col in self.stages:
            print(f"\nProcessing {source_col} -> {dest_col}")
            try:
                # For first stage, use initial prompt. For subsequent stages, use previous response
                current_prompt = initial_prompt if len(responses) == 0 else None
                response = self.process_stage(chunk_index, source_col, dest_col, current_prompt, previous_response)
                
                print(f"Response from {dest_col} length: {len(str(response))} chars")
                responses.append(response)
                previous_response = response
                
            except Exception as e:
                error_msg = f"Error in {dest_col}: {str(e)}"
                print(error_msg)
                return responses
                
        elapsed = (datetime.now() - cycle_start).total_seconds()
        print(f"Pipeline processing complete (total time: {elapsed:.1f}s)")
        return responses
    def validate_pipeline_columns(self):
        """Validate pipeline columns and create any missing destination columns"""
        # Get list of actual columns from database
        actual_columns = self.db.get_column_names()
        
        # First validate all source columns exist
        for source, _ in self.stages:
            # Handle potential multiple source columns (src1+src2)
            source_cols = source.split('+')
            for src_col in source_cols:
                if src_col not in actual_columns:
                    closest = self.find_closest_match(src_col, actual_columns)
                    suggestion = f" Did you mean '{closest}'?" if closest else ""
                    raise ValueError(f"Source column '{src_col}' does not exist.{suggestion}")
        
        # Then ensure all destination columns exist
        self.db.validate_columns(self.stages)
                
    def find_closest_match(self, target: str, options: List[str]) -> Optional[str]:
        """Find the closest matching column name using simple string matching"""
        if not options:
            return None
            
        target = target.lower()
        # First try exact match
        for opt in options:
            if opt.lower() == target:
                return opt
                
        # Then try contains
        contains_matches = [opt for opt in options if target in opt.lower() or opt.lower() in target]
        if contains_matches:
            return contains_matches[0]
            
        # Finally try prefix/suffix matching
        for opt in options:
            opt_lower = opt.lower()
            if opt_lower.startswith(target) or opt_lower.endswith(target):
                return opt
                
        return None
