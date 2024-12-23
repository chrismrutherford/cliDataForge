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

import os
import psycopg2
from typing import Dict, Optional, List, Tuple
from dotenv import load_dotenv

class DatabaseHandler:
    """Handler for database operations"""
    
    def __init__(self, sys_table: str = 'cliDataForgeSystem', data_table: str = 'cliDataForgeData', pipeline_stages=None):
        self.sys_table = sys_table
        self.data_table = data_table
        self.pipeline_stages = pipeline_stages or []
        load_dotenv()
        self.conn = None
        self.cursor = None
        self.initialize_database()
        
    def initialize_database(self):
        """Initialize database tables if they don't exist"""
        self.connect()
        try:
            # Check if tables exist
            self.cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                AND table_name = %s
            """, (self.sys_table,))
            
            # Create system prompts table if it doesn't exist
            if not self.cursor.fetchone():
                self.cursor.execute(f'''
                    CREATE TABLE "{self.sys_table}" (
                        stage VARCHAR(50) PRIMARY KEY,
                        prompt TEXT NOT NULL
                    )
                ''')
                
            self.cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                AND table_name = %s
            """, (self.data_table,))
            
            # Create data processing table if it doesn't exist
            if not self.cursor.fetchone():
                self.cursor.execute(f'''
                    CREATE TABLE "{self.data_table}" (
                        index SERIAL PRIMARY KEY,
                        chunk TEXT
                    )
                ''')
                
            self.conn.commit()
        except Exception as e:
            print(f"Error initializing database: {e}")
            self.conn.rollback()
        
    def connect(self):
        """Connect to the database using environment variables"""
        try:
            if not self.conn or self.conn.closed:
                self.conn = psycopg2.connect(
                    dbname=os.getenv('DB_NAME', 'llmdata'),
                    user=os.getenv('DB_USER', 'postgres'),
                    password=os.getenv('DB_PASSWORD'),
                    host=os.getenv('DB_HOST', 'localhost'),
                    port=os.getenv('DB_PORT', '5432')
                )
                self.cursor = self.conn.cursor()
        except psycopg2.Error as e:
            raise Exception(f"Database connection failed: {str(e)}")
            
    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cursor = None
            
    def validate_columns(self, stages):
        """Validate that all required columns exist in llamaFlowData"""
        self.connect()
        try:
            # Get existing columns with exact names
            self.cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'public'
                AND table_name = %s
            """, (self.data_table,))
            existing_columns = {row[0] for row in self.cursor.fetchall()}
            
            # Create table if it doesn't exist
            if not existing_columns:
                self.cursor.execute(f'''
                    CREATE TABLE "{self.data_table}" (
                        index SERIAL PRIMARY KEY,
                        chunk TEXT
                    )
                ''')
                self.conn.commit()
                existing_columns = {'index', 'chunk'}

            # Check each destination column
            for _, dest_col in stages:
                if dest_col not in existing_columns:
                    try:
                        # Add column if it doesn't exist
                        self.cursor.execute(f'ALTER TABLE "{self.data_table}" ADD COLUMN "{dest_col}" TEXT')
                        self.conn.commit()
                        existing_columns.add(dest_col)
                    except psycopg2.Error as e:
                        # If column already exists, just continue
                        if 'already exists' in str(e):
                            self.conn.rollback()
                            existing_columns.add(dest_col)
                            continue
                        # For other errors, rollback and raise
                        self.conn.rollback()
                        raise e
                    
            self.conn.commit()
        except Exception as e:
            print(f"Error validating columns: {e}")
            self.conn.rollback()
            
    def get_all_prompts(self) -> List[Tuple[str, str]]:
        """Get all system prompts as (stage, prompt) tuples"""
        self.connect()
        try:
            self.cursor.execute(f'SELECT stage, prompt FROM "{self.sys_table}" ORDER BY stage')
            return self.cursor.fetchall()
        except Exception as e:
            print(f"Error fetching system prompts: {e}")
            return []
            
    def get_system_prompt(self, stage: str) -> Optional[str]:
        """Get system prompt for a specific stage"""
        self.connect()
        try:
            self.cursor.execute(
                f'SELECT prompt FROM "{self.sys_table}" WHERE stage = %s',
                (stage,)
            )
            result = self.cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            print(f"Error fetching system prompt: {e}")
            return None
            
    def update_pipeline_result(self, index: int, column: str, result: str):
        """Update pipeline result for a specific column"""
        self.connect()
        try:
            # Update the result for the specified column
            self.cursor.execute(
                f'UPDATE "{self.data_table}" SET "{column}" = %s WHERE index = %s',
                (result, index)
            )
                
            self.conn.commit()
        except Exception as e:
            print(f"Error updating pipeline result: {e}")
            self.conn.rollback()
            
    def insert_chunks(self, chunks: List[str], column: str) -> int:
        """Insert chunks into specified column if empty"""
        self.connect()
        try:
            # Add column if it doesn't exist
            self.cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s AND column_name = %s
            """, (self.data_table, column))
            
            result = self.cursor.fetchone()
            if not result:
                self.cursor.execute(f"""
                    ALTER TABLE "{self.data_table}" 
                    ADD COLUMN "{column}" TEXT
                """)
                self.conn.commit()
                
            # Use the actual column name from the database
            actual_column = column
            
            # Check if column is empty
            self.cursor.execute(f"""
                SELECT COUNT(*) FROM "{self.data_table}" 
                WHERE "{column}" IS NOT NULL
            """)
            if self.cursor.fetchone()[0] > 0:
                raise ValueError(f"Column '{column}' already contains data")
            
            # Insert chunks
            for chunk in chunks:
                self.cursor.execute(f"""
                    INSERT INTO "{self.data_table}" ("{column}")
                    VALUES (%s)
                """, (chunk,))
                
            self.conn.commit()
            return len(chunks)
            
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Error inserting chunks: {str(e)}")
            
    def clear_column(self, column: str) -> int:
        """Set all values in specified column to NULL"""
        self.connect()
        try:
            # Verify column exists
            self.cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s AND column_name = %s
            """, (self.data_table, column))
            
            result = self.cursor.fetchone()
            if not result:
                raise ValueError(f"Column '{column}' does not exist in table '{self.data_table}'")
                
            # Use the actual column name from the database
            actual_column = result[0]
            
            # First verify the column exists and has data
            self.cursor.execute(f"""
                SELECT COUNT(*) 
                FROM "{self.data_table}" 
                WHERE "{actual_column}" IS NOT NULL
            """)
            existing_rows = self.cursor.fetchone()[0]
            
            if existing_rows == 0:
                raise ValueError(f"Column '{column}' exists but contains no data to clear")
            
            # Clear the column using actual case-sensitive name
            self.cursor.execute(f"""
                UPDATE "{self.data_table}" 
                SET "{actual_column}" = NULL
            """)
            
            rows_affected = self.cursor.rowcount
            self.conn.commit()
            return rows_affected
            
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Error clearing column: {str(e)}")

    def get_unprocessed_chunks(self, limit: int = 1) -> List[Tuple[int, str]]:
        """Get unprocessed chunks from the database"""
        self.connect()
        try:
            # First validate/create all needed columns
            if self.pipeline_stages:
                self.validate_columns(self.pipeline_stages)

            # Get the destination columns from the pipeline stages
            columns = [stage[1] for stage in self.pipeline_stages]
            
            if not columns:
                return []
                
            # Build dynamic query to find rows where ANY target column is NULL
            column_checks = []
            for col in columns:
                # Verify column exists before adding to query
                self.cursor.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = 'public'
                    AND table_name = %s
                    AND column_name = %s
                """, (self.data_table, col))
                if self.cursor.fetchone():
                    column_checks.append(f'(d."{col}" IS NULL)')
            
            if not column_checks:
                return []
                
            # Get source column from first pipeline stage
            source_col = self.pipeline_stages[0][0] if self.pipeline_stages else columns[0]
            
            # First check if the table exists
            self.cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                )
            """, (self.data_table,))
            table_exists = self.cursor.fetchone()[0]
            print(f"\nTable '{self.data_table}' exists: {table_exists}")

            if not table_exists:
                print(f"Table '{self.data_table}' does not exist!")
                return []

            # Check if source column exists
            self.cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.columns 
                    WHERE table_schema = 'public' 
                    AND table_name = %s 
                    AND column_name = %s
                )
            """, (self.data_table, source_col))
            column_exists = self.cursor.fetchone()[0]
            print(f"Source column '{source_col}' exists: {column_exists}")

            if not column_exists:
                print(f"Source column '{source_col}' does not exist!")
                return []

            # Verify source column exists and has data
            self.cursor.execute(f"""
                SELECT COUNT(*) FROM "{self.data_table}" 
                WHERE "{source_col}" IS NOT NULL
            """)
            source_count = self.cursor.fetchone()[0]
            print(f"Rows with data in source column '{source_col}': {source_count}")
            
            print("\nConstructing query...")
            # Build query to get rows where source exists and ANY destination is NULL
            null_conditions = ' OR '.join([f'd."{col}" IS NULL' for col in columns])
            query = f'''
                -- Query to find unprocessed chunks
                SELECT d.index, d."{source_col}"
                FROM "{self.data_table}" d
                WHERE d."{source_col}" IS NOT NULL
                AND ({null_conditions})
                ORDER BY d.index
                LIMIT %s
            '''
            
            # Execute and get results
            self.cursor.execute(query, (limit,))
            results = self.cursor.fetchall()
            if len(results) == 0:
                print("\nChecking data in relevant columns:")
                # Check source column data
                self.cursor.execute(f'SELECT COUNT(*) FROM "{self.data_table}" WHERE "{source_col}" IS NOT NULL')
                source_count = self.cursor.fetchone()[0]
                print(f"- Rows with data in source column '{source_col}': {source_count}")
                
                # Check target column nulls
                self.cursor.execute(f'SELECT COUNT(*) FROM "{self.data_table}" WHERE "{columns[0]}" IS NULL')
                null_count = self.cursor.fetchone()[0]
                print(f"- Rows with NULL in target column '{columns[0]}': {null_count}")
            
            return results
        except Exception as e:
            print(f"Error fetching unprocessed chunks: {e}")
            return []
    def get_column_names(self) -> List[str]:
        """Get list of column names from the data table"""
        self.connect()
        try:
            self.cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s
                ORDER BY ordinal_position
            """, (self.data_table,))
            return [row[0] for row in self.cursor.fetchall()]
        except Exception as e:
            print(f"Error getting column names: {e}")
            return []
            
    def delete_column(self, column: str) -> bool:
        """Delete a column from the data table"""
        self.connect()
        try:
            # Verify column exists
            self.cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s AND column_name = %s
            """, (self.data_table, column))
            
            if not self.cursor.fetchone():
                raise ValueError(f"Column '{column}' does not exist in table '{self.data_table}'")
            
            # Don't allow deletion of essential columns
            if column.lower() == 'index':
                raise ValueError(f"Cannot delete essential column '{column}'")
            
            # Drop the column
            self.cursor.execute(f"""
                ALTER TABLE "{self.data_table}"
                DROP COLUMN "{column}"
            """)
            
            self.conn.commit()
            return True
            
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Error deleting column: {str(e)}")
            
    def delete_system_prompt(self, stage: str) -> bool:
        """Delete a system prompt from the system table"""
        self.connect()
        try:
            # Verify prompt exists
            self.cursor.execute(f"""
                SELECT stage FROM "{self.sys_table}"
                WHERE stage = %s
            """, (stage,))
            
            if not self.cursor.fetchone():
                raise ValueError(f"No prompt found for stage '{stage}'")
            
            # Delete the prompt
            self.cursor.execute(f"""
                DELETE FROM "{self.sys_table}"
                WHERE stage = %s
            """, (stage,))
            
            self.conn.commit()
            return True
            
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Error deleting prompt: {str(e)}")
            
    def set_system_prompt(self, stage: str, prompt: str) -> bool:
        """Add or update a system prompt in the system table"""
        self.connect()
        try:
            # Insert or update the prompt
            self.cursor.execute(f"""
                INSERT INTO "{self.sys_table}" (stage, prompt)
                VALUES (%s, %s)
                ON CONFLICT (stage) 
                DO UPDATE SET prompt = EXCLUDED.prompt
            """, (stage, prompt))
            
            self.conn.commit()
            return True
            
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Error setting prompt: {str(e)}")
            
    def get_total_count(self) -> int:
        """Get total count of chunks in the database"""
        self.connect()
        try:
            source_col = self.pipeline_stages[0][0]
            query = f'''
                SELECT COUNT(*)
                FROM "{self.data_table}"
                WHERE "{source_col}" IS NOT NULL
            '''
            self.cursor.execute(query)
            return self.cursor.fetchone()[0]
        except Exception as e:
            print(f"Error getting total count: {e}")
            return 0
            
    def get_processed_count(self) -> int:
        """Get count of already processed chunks"""
        self.connect()
        try:
            # Get the destination columns from the pipeline stages
            columns = [stage[1] for stage in self.pipeline_stages]
            if not columns:
                return 0
                
            # Build query to count rows where all destinations are NOT NULL
            source_col = self.pipeline_stages[0][0]
            not_null_conditions = ' AND '.join([f'd."{col}" IS NOT NULL' for col in columns])
            query = f'''
                SELECT COUNT(*)
                FROM "{self.data_table}" d
                WHERE d."{source_col}" IS NOT NULL
                AND ({not_null_conditions})
            '''
            
            self.cursor.execute(query)
            return self.cursor.fetchone()[0]
            
        except Exception as e:
            print(f"Error getting processed count: {e}")
            return 0

    def get_column_contents(self, column: str) -> List[str]:
        """Get contents of specified column as a list"""
        self.connect()
        try:
            # Verify column exists
            self.cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s AND column_name = %s
            """, (self.data_table, column))
            
            if not self.cursor.fetchone():
                raise ValueError(f"Column '{column}' does not exist in table '{self.data_table}'")
            
            # Fetch non-null values from the column
            self.cursor.execute(f"""
                SELECT "{column}"
                FROM "{self.data_table}"
                WHERE "{column}" IS NOT NULL
                ORDER BY index
            """)
            
            return [row[0] for row in self.cursor.fetchall()]
            
        except Exception as e:
            print(f"Error fetching column contents: {e}")
            return []
