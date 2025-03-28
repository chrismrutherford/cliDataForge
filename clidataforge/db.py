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
from psycopg2 import pool
from typing import Dict, Optional, List, Tuple, Any
from functools import lru_cache
from dotenv import load_dotenv
import threading

class DatabaseHandler:
    """Handler for database operations"""
    
    _pool = None
    _pool_lock = threading.Lock()
    
    def __init__(self, sys_table: str = 'cliDataForgeSystem', data_table: str = None, pipeline_stages=None, require_data_table: bool = True):
        self.sys_table = sys_table
        self.pipeline_stages = pipeline_stages or []
        self.conn = None
        self.cursor = None
        self._prompt_cache: Dict[str, str] = {}
        load_dotenv()
        
        # Store connection parameters
        self.db_params = {
            'dbname': os.getenv('DB_NAME', 'llmdata'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD'),
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432')
        }
        
        # Initialize the connection pool if not already done
        with self._pool_lock:
            if self._pool is None:
                self._pool = pool.ThreadedConnectionPool(
                    minconn=1,
                    maxconn=20,  # Adjust based on your needs
                    **self.db_params
                )
        
        print("\nDatabaseHandler initialization:")
        print(f"- System table: {sys_table}")
        print(f"- Data table: {data_table}")
        
        # Initialize system table and maintain the connection
        self.connect()
        self.initialize_system_table(self.cursor)
        
        self.data_table = data_table
        
        # Only validate data table if required
        if require_data_table and not self.data_table and sys_table != 'llamaFlowSystem':
            print("ERROR: No data table specified")
            raise ValueError("No data table specified")
        
    def initialize_system_table(self, cursor=None):
        """Initialize just the system table if it doesn't exist"""
        print("\nInitializing system table:")
        print(f"- Using provided cursor: {cursor is not None}")
        
        if cursor:
            self.cursor = cursor
            self.conn = self.cursor.connection
        else:
            print("- Creating new connection")
            self.conn = psycopg2.connect(**self.db_params)
            self.cursor = self.conn.cursor()
        try:
            # Check if system table exists
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
                
            self.conn.commit()
            print("- System table initialized successfully")
        except Exception as e:
            print(f"Error initializing system table: {e}")
            if self.conn and not cursor:  # Only rollback our own connection
                print("- Rolling back transaction")
                self.conn.rollback()
            raise
        
    def _with_connection(self, operation):
        """Execute an operation with a fresh connection"""
        conn = None
        cursor = None
        try:
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()
            result = operation(cursor)
            conn.commit()
            return result
        except Exception as e:
            if conn:
                conn.rollback()
            raise e
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
            
    def connect(self):
        """Get a connection from the pool"""
        if not self.conn:
            self.conn = self._pool.getconn()
            self.cursor = self.conn.cursor()
            
    def disconnect(self):
        """Return connection to the pool"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self._pool.putconn(self.conn)
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
            
    def get_all_prompts(self, table_name: str) -> List[Tuple[str, str]]:
        """Get all system prompts as (stage, prompt) tuples for a specific table"""
        if not table_name:
            raise ValueError("Table name is required for getting prompts")
            
        self.connect()
        try:
            # Get prompts for specific table
            self.cursor.execute(f'SELECT stage, prompt FROM "{self.sys_table}" WHERE stage LIKE %s ORDER BY stage', 
                              (f"{table_name}:%",))
            # Remove table prefix from stage names
            return [(stage.split(':', 1)[1], prompt) for stage, prompt in self.cursor.fetchall()]
        except Exception as e:
            print(f"Error fetching system prompts: {e}")
            return []
            
    def get_system_prompt(self, stage: str) -> Optional[str]:
        """Get system prompt for a specific stage"""
        # Check cache first
        cache_key = f"{self.data_table}:{stage}" if self.data_table else stage
        if cache_key in self._prompt_cache:
            return self._prompt_cache[cache_key]

        with self._pool_lock:
            self.connect()
            try:
                query = f'''
                    SELECT prompt FROM "{self.sys_table}"
                    WHERE stage = %s
                '''
                stage_key = f"{self.data_table}:{stage}" if self.data_table else stage
                print(f"\nExecuting system prompt query:")
                print(f"{query.strip()}")
                self.cursor.execute(query, (stage_key,))
                result = self.cursor.fetchone()
                prompt = result[0] if result else None
                if prompt:
                    self._prompt_cache[cache_key] = prompt
                return prompt
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
        """Insert chunks into specified column"""
        self.connect()
        try:
            # Print connection details
            print(f"\nConnecting to database:")
            print(f"Database: {os.getenv('DB_NAME', 'llmdata')}")
            print(f"User: {os.getenv('DB_USER', 'postgres')}")
            print(f"Host: {os.getenv('DB_HOST', 'localhost')}")
            print(f"Port: {os.getenv('DB_PORT', '5432')}")
            print(f"Table: {self.data_table}")

            # Debug print chunks
            print(f"\nInserting {len(chunks)} chunks into column '{column}'")
            print(f"First chunk preview: {chunks[0][:100]}...")
            
            # Create table with column if needed
            self.cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS "{self.data_table}" (
                    index SERIAL PRIMARY KEY,
                    "{column}" TEXT
                )
            """)
            self.conn.commit()

            # Get count of existing rows
            self.cursor.execute(f"""
                SELECT COUNT(*) FROM "{self.data_table}"
            """)
            existing_rows = self.cursor.fetchone()[0]

            # Clean data by removing null bytes
            cleaned_chunks = [chunk.replace('\x00', '') if chunk else chunk for chunk in chunks]
            
            if existing_rows == 0:
                # If table is empty, do regular inserts
                for chunk in cleaned_chunks:
                    self.cursor.execute(f"""
                        INSERT INTO "{self.data_table}" ("{column}") 
                        VALUES (%s)
                    """, (chunk,))
                    self.conn.commit()
            else:
                # Update existing rows in order
                for i, chunk in enumerate(cleaned_chunks, 1):
                    if i <= existing_rows:
                        self.cursor.execute(f"""
                            UPDATE "{self.data_table}" 
                            SET "{column}" = %s 
                            WHERE index = %s
                        """, (chunk, i))
                        self.conn.commit()
                    else:
                        # If we have more chunks than rows, insert the remainder
                        self.cursor.execute(f"""
                            INSERT INTO "{self.data_table}" ("{column}") 
                            VALUES (%s)
                        """, (chunk,))
                        self.conn.commit()
            
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
            # Get source column(s) - handle potential concatenation with +
            source_col = self.pipeline_stages[0][0]
            source_cols = source_col.split('+')
            
            # Build the SELECT part for potentially multiple source columns
            if len(source_cols) == 1:
                # Simple case - just one source column
                select_part = f'"{source_cols[0]}"'
            else:
                # Multiple source columns to concatenate with newlines
                concat_parts = []
                for col in source_cols:
                    concat_parts.append(f'"{col}"')
                select_part = " || '\n\n\n\n' || ".join(concat_parts)
            
            # Simple query to find first NULL in destination column
            query = f'''
                SELECT index, {select_part}
                FROM "{self.data_table}"
                WHERE "{self.pipeline_stages[-1][1]}" IS NULL
                ORDER BY index ASC
                LIMIT %s
            '''
            print(f"\nQuery to execute:")
            print(f"{query % limit}")  # Show the actual query with limit value
            print(f"Pipeline stages: {self.pipeline_stages}")
            
            # Execute and get results
            self.cursor.execute(query, (limit,))
            results = self.cursor.fetchall()
            print(f"Found {len(results)} unprocessed chunks")
            
            if results:
                print("\nFirst result:")
                print(f"Index: {results[0][0]}")
                print(f"Content: {results[0][1][:100]}...")  # First 100 chars
            
            return results
        except Exception as e:
            print(f"Error fetching unprocessed chunks: {e}")
            return []
    def get_column_names(self) -> List[str]:
        """Get list of column names from the data table"""
        self.connect()
        try:
            query = """
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s
                ORDER BY ordinal_position
            """
            print(f"\nExecuting get_column_names query:")
            print(f"Query: {query}")
            print(f"Parameters: table_name = '{self.data_table}'")
            
            self.cursor.execute(query, (self.data_table,))
            columns = [row[0] for row in self.cursor.fetchall()]
            print(f"Found columns: {columns}")
            return columns
        except Exception as e:
            print(f"Error getting column names: {e}")
            return []
            
    def create_column(self, column: str) -> bool:
        """Create a new TEXT column in the data table"""
        self.connect()
        try:
            # Verify column doesn't exist
            self.cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s AND column_name = %s
            """, (self.data_table, column))
            
            if self.cursor.fetchone():
                raise ValueError(f"Column '{column}' already exists in table '{self.data_table}'")
            
            # Create the column
            self.cursor.execute(f"""
                ALTER TABLE "{self.data_table}"
                ADD COLUMN "{column}" TEXT
            """)
            
            self.conn.commit()
            return True
            
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Error creating column: {str(e)}")

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
            
    def set_system_prompt(self, stage: str, prompt: str, table_name: str = None) -> bool:
        """Add or update a system prompt in the system table"""
        with self._pool_lock:
            self.connect()
            try:
                # Always store both prefixed and unprefixed versions
                stages_to_set = [stage]  # Unprefixed version
                if table_name:
                    stages_to_set.append(f"{table_name}:{stage}")  # Prefixed version
                
                for stage_name in stages_to_set:
                    self.cursor.execute(f"""
                        INSERT INTO "{self.sys_table}" (stage, prompt)
                        VALUES (%s, %s)
                        ON CONFLICT (stage) 
                        DO UPDATE SET prompt = EXCLUDED.prompt
                    """, (stage_name, prompt))
                    # Update cache
                    self._prompt_cache[stage_name] = prompt
                
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
            # Get the destination column (last stage)
            dest_col = self.pipeline_stages[-1][1]
            if not dest_col:
                return 0
                
            # Get count of rows where destination column is NOT NULL
            query = f'''
                SELECT COUNT(*)
                FROM "{self.data_table}" d
                WHERE d."{dest_col}" IS NOT NULL
            '''
            
            print(f"\nExecuting query to count processed rows:")
            print(query)
            
            self.cursor.execute(query)
            processed_count = self.cursor.fetchone()[0]
            
            print(f"Found {processed_count} processed rows")
            
            # Debug query to show first few rows
            debug_query = f'''
                SELECT index, "{dest_col}" 
                FROM "{self.data_table}"
                ORDER BY index
                LIMIT 5
            '''
            print("\nFirst 5 rows in table:")
            self.cursor.execute(debug_query)
            for row in self.cursor.fetchall():
                print(f"Index {row[0]}: {row[1] and 'Processed' or 'NULL'}")
                
            return processed_count
            
        except Exception as e:
            print(f"Error getting processed count: {e}")
            return 0

    def list_tables(self) -> List[str]:
        """Get list of all tables in the database"""
        self.connect()
        try:
            self.cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name
            """)
            return [row[0] for row in self.cursor.fetchall()]
        except Exception as e:
            print(f"Error listing tables: {e}")
            return []

    def create_table(self, table_name: str, columns: List[Tuple[str, str]]) -> bool:
        """Create a new table with specified columns"""
        self.connect()
        try:
            # Check if table already exists
            self.cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                )
            """, (table_name,))
            
            if self.cursor.fetchone()[0]:
                raise ValueError(f"Table '{table_name}' already exists")
            
            # Build CREATE TABLE statement
            column_defs = ['index SERIAL PRIMARY KEY']  # Always include index as primary key
            for col_name, col_type in columns:
                if col_name.lower() != 'index':  # Skip if column is named index
                    column_defs.append(f'"{col_name}" {col_type.upper()}')
            
            create_stmt = f"""
                CREATE TABLE "{table_name}" (
                    {', '.join(column_defs)}
                )
            """
            
            self.cursor.execute(create_stmt)
            self.conn.commit()
            return True
            
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Error creating table: {str(e)}")


    def get_column_contents(self, column: str) -> List[str]:
        """Get contents of specified column as a list"""
        self.connect()
        try:
            # Directly try to fetch values from the column
            print(f"\nExecuting get_column_contents query for column '{column}'")
            query = f"""
                SELECT "{column}"
                FROM "{self.data_table}"
                WHERE "{column}" IS NOT NULL
                ORDER BY index
            """
            print(f"Query: {query}")
            print(f"Table: {self.data_table}")
            
            self.cursor.execute(query)
            results = [row[0] for row in self.cursor.fetchall()]
            print(f"Found {len(results)} rows")
            return results
            
        except Exception as e:
            print(f"Error fetching column contents: {e}")
            return []
