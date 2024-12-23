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
            
        # Validate and create columns if needed
        self.db.validate_columns(self.stages)
        
    def process_stage(self, chunk_index: int, source_col: str, dest_col: str,
                     prompt: Optional[str], previous_response: Optional[str]) -> str:
        """Process a single stage of the pipeline"""
        system_prompt = self.db.get_system_prompt(dest_col)
        if not system_prompt:
            raise ValueError(f"No system prompt found for destination column {dest_col}")
            
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
                
        # Ensure minimum cycle time
        elapsed = (datetime.now() - cycle_start).total_seconds()
        if elapsed < 5:
            wait_time = max(0, 5 - elapsed)
            if wait_time > 0:
                print(f"Waiting {wait_time:.1f} seconds to complete minimum cycle time...")
                time.sleep(wait_time)
                
        print(f"Pipeline processing complete (total time: {elapsed:.1f}s)")
        return responses
