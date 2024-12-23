import os
import psycopg2
from typing import Dict, Optional, List, Tuple
from dotenv import load_dotenv

class DatabaseHandler:
    """Handler for database operations"""
    
    def __init__(self, sys_table: str = 'llamaFlowSystem', data_table: str = 'llamaFlowData', pipeline_stages=None):
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
                        chunk TEXT NOT NULL
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
            # Get existing columns
            self.cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s
            """, (self.data_table.lower(),))
            existing_columns = {row[0] for row in self.cursor.fetchall()}
            
            # Check each destination column
            for _, dest_col in stages:
                try:
                    if dest_col.lower() not in {col.lower() for col in existing_columns}:
                        # Add column if it doesn't exist
                        self.cursor.execute(f'ALTER TABLE "{self.data_table}" ADD COLUMN "{dest_col}" TEXT')
                        self.conn.commit()
                except psycopg2.Error as e:
                    if 'already exists' not in str(e):
                        self.conn.rollback()
                        raise e
                    self.conn.rollback()
                    
            self.conn.commit()
        except Exception as e:
            print(f"Error validating columns: {e}")
            self.conn.rollback()
            
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
            # Get the destination columns from the pipeline stages
            columns = [stage[1] for stage in self.pipeline_stages]
            
            if not columns:
                return []
                
            # Build dynamic query to find rows where ANY target column is NULL
            column_checks = ' OR '.join(f'(d."{col}" IS NULL)' for col in columns)
            
            query = f'''
                SELECT DISTINCT d.index, d.chunk
                FROM "{self.data_table}" d
                WHERE d.chunk IS NOT NULL
                AND ({column_checks})
                ORDER BY d.index
                LIMIT %s
            '''
            
            self.cursor.execute(query, (limit,))
            return self.cursor.fetchall()
        except Exception as e:
            print(f"Error fetching unprocessed chunks: {e}")
            return []
