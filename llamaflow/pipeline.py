from datetime import datetime
import time
from typing import List, Tuple, Optional
from .llm import LLMClient
from .db import DatabaseHandler

class PipelineExecutor:
    """Executes the LLM pipeline with database integration"""
    
    def __init__(self, llm_client: LLMClient, db_handler: DatabaseHandler, stages: str, model: str = "meta-llama/llama-3.3-70b-instruct"):
        self.llm = llm_client
        self.db = db_handler
        self.model = model
        # Parse stages into source:destination pairs
        self.stages = []
        for stage in stages.split(','):
            source, dest = stage.strip().split(':')
            self.stages.append((source.strip(), dest.strip()))
            
        # Validate columns exist and are spelled correctly
        self.validate_pipeline_columns()
        
    def process_stage(self, chunk_index: int, source_col: str, dest_col: str,
                     prompt: Optional[str], previous_response: Optional[str]) -> str:
        """Process a single stage of the pipeline"""
        system_prompt = self.db.get_system_prompt(dest_col)
        if not system_prompt:
            print(f"\nFATAL ERROR: No system prompt found for destination column {dest_col}")
            print("Please ensure system prompts are configured in the system table before running the pipeline.")
            exit(1)
            
        messages = self.llm.build_messages(prompt, system_prompt, previous_response)
        response = self.llm.complete(messages, model=self.model)
        
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
                current_prompt = initial_prompt if source_col == 'chunk' else None
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
        
        for source, dest in self.stages:
            # Skip validation for 'chunk' as it's a special source column
            if source != 'chunk' and source not in actual_columns:
                closest = self.find_closest_match(source, actual_columns)
                suggestion = f" Did you mean '{closest}'?" if closest else ""
                raise ValueError(f"Source column '{source}' does not exist.{suggestion}")
            
            # Create destination column if it doesn't exist
            if dest not in actual_columns:
                self.db.validate_columns(self.stages)
                # Refresh column list after creation
                actual_columns = self.db.get_column_names()
                
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
