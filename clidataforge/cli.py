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

import click
from concurrent.futures import ThreadPoolExecutor
from .llm import LLMClient
from .db import DatabaseHandler
from .pipeline import PipelineExecutor
import os

@click.group()
def cli():
    """cliDataForge - Pipeline processing with LLMs"""
    pass

@cli.command(name='process-all')
@click.option('--api-key', envvar='LLAMAFLOW_API_KEY', help='API key for LLM service')
@click.option('--base-url', envvar='OPENAI_BASE_URL', help='Base URL for OpenAI-compatible API service')
@click.option('--model', default='meta-llama/llama-3.3-70b-instruct', help='Model to use')
@click.option('--threads', default=1, type=int, help='Number of parallel threads (default: 1)')
@click.option('--stages', required=True, help='Comma-separated list of source:destination column pairs (e.g. chunk:summary,summary:analysis)')
@click.option('--sys-table', default='llamaFlowSystem', show_default=True, help='Name of system prompts table')
@click.option('--data-table', default='llamaFlowData', show_default=True, help='Name of data processing table')
def process_all(api_key: str, base_url: str, model: str, threads: int, stages: str, sys_table: str, data_table: str):
    """Process all unprocessed chunks in the database"""
    try:
        # Parse stages
        stage_pairs = [(s.split(':')[0].strip(), s.split(':')[1].strip()) 
                      for s in stages.split(',')]
        
        llm = LLMClient(api_key=api_key, base_url=base_url)
        db = DatabaseHandler(sys_table=sys_table, data_table=data_table, 
                           pipeline_stages=stage_pairs)
        pipeline = PipelineExecutor(llm, db, stages, model=model)
        
        import time
        from datetime import datetime
        
        def process_chunk_wrapper(chunk_data):
            chunk_index, prompt = chunk_data
            start_time = datetime.now()
            try:
                responses = pipeline.execute_pipeline(chunk_index, prompt)
                duration = (datetime.now() - start_time).total_seconds()
                bar.update(1)
                return True
            except Exception as e:
                duration = (datetime.now() - start_time).total_seconds()
                click.echo(f"Error processing chunk {chunk_index} after {duration:.1f}s: {str(e)}", err=True)
                return False

        total_start = datetime.now()
        # Get total count and already processed count
        total_count = db.get_total_count()
        processed_count = db.get_processed_count()
        remaining_count = total_count - processed_count
        
        print(f"\nProcessing remaining {remaining_count} chunks ({processed_count}/{total_count} already processed)...")
        with click.progressbar(length=total_count,
                             label='Progress',
                             show_pos=True,
                             item_show_func=lambda x: f"{x}/{total_count} processed" if x else None) as bar:
            total_chunks = processed_count
            total_successful = processed_count
            # Update progress bar to show already processed items
            bar.update(processed_count)

            # Create a single executor for all batches
            with ThreadPoolExecutor(max_workers=threads) as executor:
                while total_chunks < total_count:
                    batch_start = datetime.now()
                    chunks = db.get_unprocessed_chunks(limit=threads * 2)  # Get more chunks to keep threads busy
                    if not chunks:
                        break

                    # Submit all chunks to thread pool at once
                    futures = [executor.submit(process_chunk_wrapper, chunk) for chunk in chunks]
                    
                    # Process results as they complete
                    for future in futures:
                        result = future.result()
                        total_chunks += 1
                        if result:
                            total_successful += 1
                

        total_duration = (datetime.now() - total_start).total_seconds()
        click.echo("\nProcessing complete!")
        click.echo(f"Total chunks processed: {total_chunks}")
        click.echo(f"Total successful: {total_successful}")
        click.echo(f"Total time: {total_duration:.1f}s")
        if total_chunks > 0:
            click.echo(f"Average time per chunk: {total_duration/total_chunks:.1f}s")
            
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
    finally:
        if 'db' in locals():
            db.disconnect()

@cli.command(name='process-chunk')
@click.option('--api-key', envvar='LLAMAFLOW_API_KEY', help='API key for LLM service')
@click.option('--base-url', envvar='OPENAI_BASE_URL', help='Base URL for OpenAI-compatible API service')
@click.option('--model', default='meta-llama/llama-3.3-70b-instruct', help='Model to use')
@click.option('--stages', required=True, help='Comma-separated list of source:destination column pairs (e.g. chunk:summary,summary:analysis)')
@click.option('--sys-table', default='llamaFlowSystem', show_default=True, help='Name of system prompts table')
@click.option('--data-table', default='llamaFlowData', show_default=True, help='Name of data processing table')
def process_chunk(api_key: str, base_url: str, model: str, stages: str, sys_table: str, data_table: str):
    """Process a single unprocessed chunk through the pipeline"""
    try:
        # Parse stages
        stage_pairs = [(s.split(':')[0].strip(), s.split(':')[1].strip()) 
                      for s in stages.split(',')]
        
        llm = LLMClient(api_key=api_key, base_url=base_url)
        db = DatabaseHandler(sys_table=sys_table, data_table=data_table, pipeline_stages=stage_pairs)
        pipeline = PipelineExecutor(llm, db, stages, model=model)
        
        chunks = db.get_unprocessed_chunks(limit=1)
        if not chunks:
            click.echo("No unprocessed chunks found")
            return
            
        chunk_index, chunk_text = chunks[0]
        responses = pipeline.execute_pipeline(chunk_index, chunk_text)
        for i, response in enumerate(responses, 1):
            click.echo(f"\nStage {i} Response:")
            click.echo("-" * 40)
            click.echo(response)
            click.echo("-" * 40)
            
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
    finally:
        if 'db' in locals():
            db.disconnect()

@cli.command(name='insert-data')
@click.argument('json_file', type=click.Path(exists=True))
@click.option('--column', required=True, help='Column name to insert data into')
@click.option('--data-table', default='llamaFlowData', show_default=True, help='Name of data processing table')
def insert_data(json_file: str, column: str, data_table: str):
    """Insert JSON list of text chunks into specified column"""
    try:
        import json
        with open(json_file, 'r') as f:
            chunks = json.load(f)
            
        if not isinstance(chunks, list):
            click.echo("Error: JSON file must contain a list of text chunks", err=True)
            return
            
        db = DatabaseHandler(data_table=data_table)
        inserted = db.insert_chunks(chunks, column)
        click.echo(f"Successfully inserted {inserted} chunks into column '{column}'")
                    
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
    finally:
        if 'db' in locals():
            db.disconnect()

@cli.command(name='clear-column')
@click.option('--column', required=True, help='Column to clear')
@click.option('--data-table', default='llamaFlowData', show_default=True, help='Name of data processing table')
def clear_column(column: str, data_table: str):
    """Clear all values in specified column (set to NULL)"""
    try:
        db = DatabaseHandler(data_table=data_table)
        try:
            rows_affected = db.clear_column(column)
            if rows_affected > 0:
                click.echo(f"Successfully cleared {rows_affected} rows in column '{column}'")
            else:
                click.echo(f"No data was cleared from column '{column}'")
        except ValueError as ve:
            click.echo(f"Error: {str(ve)}", err=True)
            
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
    finally:
        if 'db' in locals():
            db.disconnect()

@cli.command(name='list-columns')
@click.option('--data-table', default='llamaFlowData', show_default=True, help='Name of data processing table')
def list_columns(data_table: str):
    """List all columns in the data table"""
    db = None
    try:
        # Connect directly without initialization to avoid table creation
        import psycopg2
        from dotenv import load_dotenv
        load_dotenv()
        
        # Debug connection parameters
        dbname = os.getenv('DB_NAME', 'llmdata')
        dbuser = os.getenv('DB_USER', 'postgres')
        dbhost = os.getenv('DB_HOST', 'localhost')
        dbport = os.getenv('DB_PORT', '5432')
        
        click.echo(f"\nConnecting to database:")
        click.echo(f"Database: {dbname}")
        click.echo(f"User: {dbuser}")
        click.echo(f"Host: {dbhost}")
        click.echo(f"Port: {dbport}")
        
        conn = psycopg2.connect(
            dbname=dbname,
            user=dbuser,
            password=os.getenv('DB_PASSWORD'),
            host=dbhost,
            port=dbport
        )
        cursor = conn.cursor()
        
        # First check if table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            )
        """, (data_table,))
        
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            click.echo(f"\nTable '{data_table}' does not exist!")
            return
            
        # Query existing columns with schema info
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = %s
            ORDER BY ordinal_position
        """, (data_table,))
        
        columns = cursor.fetchall()
        
        if columns:
            click.echo("\nAvailable columns:")
            for col in columns:
                col_name, col_type, max_length = col
                type_info = f"{col_type}"
                if max_length:
                    type_info += f"({max_length})"
                click.echo(f"- {col_name} ({type_info})")
        else:
            click.echo(f"\nNo columns found in table '{data_table}'")
            
        cursor.close()
        conn.close()
            
    except Exception as e:
        click.echo(f"Error listing columns: {str(e)}", err=True)

@cli.command(name='save-column')
@click.argument('output_file', type=click.Path())
@click.option('--column', required=True, help='Column to save')
@click.option('--data-table', default='llamaFlowData', show_default=True, help='Name of data processing table')
def save_column(output_file: str, column: str, data_table: str):
    """Save contents of specified column to a JSON file"""
    try:
        db = DatabaseHandler(data_table=data_table)
        try:
            contents = db.get_column_contents(column)
            if not contents:
                click.echo(f"No data found in column '{column}'")
                return
                
            import json
            with open(output_file, 'w') as f:
                json.dump(contents, f, indent=2)
                
            click.echo(f"Successfully saved {len(contents)} entries from column '{column}' to {output_file}")
                
        except ValueError as ve:
            click.echo(f"Error: {str(ve)}", err=True)
            
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
    finally:
        if 'db' in locals():
            db.disconnect()

@cli.command(name='show-prompt')
@click.argument('stage')
@click.option('--sys-table', default='llamaFlowSystem', show_default=True, help='Name of system prompts table')
def show_prompt(stage: str, sys_table: str):
    """Show the system prompt for a specific processing stage"""
    try:
        db = DatabaseHandler(sys_table=sys_table)
        try:
            prompt = db.get_system_prompt(stage)
            if prompt:
                click.echo(f"\nSystem prompt for stage '{stage}':")
                click.echo("-" * 40)
                click.echo(prompt)
                click.echo("-" * 40)
            else:
                click.echo(f"No system prompt found for stage '{stage}'")
                
        except ValueError as ve:
            click.echo(f"Error: {str(ve)}", err=True)
            
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
    finally:
        if 'db' in locals():
            db.disconnect()

@cli.command(name='add-prompt')
@click.argument('stage')
@click.argument('prompt_file', type=click.Path(exists=True))
@click.option('--sys-table', default='llamaFlowSystem', show_default=True, help='Name of system prompts table')
def add_prompt(stage: str, prompt_file: str, sys_table: str):
    """Add or update system prompt for a processing stage from a file"""
    try:
        with open(prompt_file, 'r') as f:
            prompt_text = f.read().strip()
            
        if not prompt_text:
            click.echo("Error: Prompt file is empty", err=True)
            return
            
        db = DatabaseHandler(sys_table=sys_table)
        try:
            db.set_system_prompt(stage, prompt_text)
            click.echo(f"Successfully added/updated prompt for stage '{stage}'")
                
        except ValueError as ve:
            click.echo(f"Error: {str(ve)}", err=True)
            
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
    finally:
        if 'db' in locals():
            db.disconnect()

@cli.command(name='delete-prompt')
@click.argument('stage')
@click.option('--sys-table', default='llamaFlowSystem', show_default=True, help='Name of system prompts table')
def delete_prompt(stage: str, sys_table: str):
    """Delete the system prompt for a specific processing stage"""
    try:
        db = DatabaseHandler(sys_table=sys_table)
        try:
            db.delete_system_prompt(stage)
            click.echo(f"Successfully deleted prompt for stage '{stage}'")
                
        except ValueError as ve:
            click.echo(f"Error: {str(ve)}", err=True)
            
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
    finally:
        if 'db' in locals():
            db.disconnect()

@cli.command(name='list-prompts')
@click.option('--sys-table', default='llamaFlowSystem', show_default=True, help='Name of system prompts table')
def list_prompts(sys_table: str):
    """List all available system prompts and their stages"""
    try:
        db = DatabaseHandler(sys_table=sys_table)
        try:
            prompts = db.get_all_prompts()
            if prompts:
                click.echo("\nAvailable system prompts:")
                click.echo("-" * 25)
                for stage, _ in sorted(prompts):
                    click.echo(f"| {stage:<20} |")
                click.echo("-" * 25)
            else:
                click.echo("No system prompts found")
                
        except ValueError as ve:
            click.echo(f"Error: {str(ve)}", err=True)
            
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
    finally:
        if 'db' in locals():
            db.disconnect()

@cli.command(name='delete-column')
@click.option('--column', required=True, help='Column to delete')
@click.option('--data-table', default='llamaFlowData', show_default=True, help='Name of data processing table')
def delete_column(column: str, data_table: str):
    """Completely remove specified column from the table"""
    try:
        db = DatabaseHandler(data_table=data_table)
        try:
            db.delete_column(column)
            click.echo(f"Successfully deleted column '{column}'")
                
        except ValueError as ve:
            click.echo(f"Error: {str(ve)}", err=True)
            
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
    finally:
        if 'db' in locals():
            db.disconnect()

if __name__ == '__main__':
    cli()
