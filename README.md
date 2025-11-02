# cliDataForge CLI Documentation

cliDataForge provides a command-line interface for managing and executing LLM processing pipelines with PostgreSQL integration.

## Setup

### Environment Variables (Required)

Before using cliDataForge, you must set the following environment variables:

**Database Configuration:**
- `DB_NAME`: Database name (default: 'llmdata')
- `DB_USER`: Database user (default: 'postgres') 
- `DB_PASSWORD`: Database password (required)
- `DB_HOST`: Database host (default: 'localhost')
- `DB_PORT`: Database port (default: '5432')

**LLM API Configuration:**
- `CLI_DF_API_KEY`: API key for LLM service (required)
- `CLI_DF_BASE_URL`: Base URL for OpenAI-compatible API (required)
- `CLI_DF_MODEL`: Model name to use for completions (required)

### Quick Setup

Use the interactive setup command to create a `.env` file:

```bash
python -m clidataforge setup
```

Then load the environment variables:
```bash
source .env
```

Or add the variables to your shell profile for persistence.

## Pipeline Stages

Pipeline stages are specified as comma-separated pairs of `source:destination` columns:

- `chunk:summary`: Generate summary from input chunk
- `summary:analysis`: Generate analysis from summary
- `analysis:conclusion`: Generate conclusion from analysis
- `src1+src2:combined`: Concatenate multiple source columns into one destination

Example pipeline specifications:
```bash
# Basic linear pipeline
--stages "chunk:summary,summary:analysis,analysis:conclusion"

# Using multiple source columns
--stages "title+content:summary,summary:analysis"
```

Each stage's processing is guided by system prompts stored in the system prompts table.
The pipeline automatically manages dependencies between stages and ensures data consistency.

### Multi-Source Columns

You can combine multiple source columns using the `+` operator:
- Source columns are concatenated with newlines between them
- All specified source columns must exist in the table
- This is particularly useful for combining related data (e.g., title and content)

## Architecture

![cliDataForge Architecture](cliDataForgeArchitecture.png)

## Global Options

These options are available for most commands:

- `--sys-table`: Name of system prompts table (default: 'cliDataForgeSystem')

Note: Most commands require a table name as their first argument. This is a required positional argument, not an option.

## Commands

### save-column

Save contents of specified column to a JSON file.

```bash
python -m clidataforge save-column TABLE_NAME OUTPUT_FILE --column COLUMN [options]
```

Arguments:
- `OUTPUT_FILE`: Path to save the JSON file
- `--column`: Required. Column to save

### show-prompt

Show the system prompt for a specific processing stage.

```bash
python -m clidataforge show-prompt TABLE_NAME STAGE [options]
```

Arguments:
- `STAGE`: The processing stage name

### add-prompt

Add or update system prompt for a processing stage from a file.

```bash
python -m clidataforge add-prompt TABLE_NAME STAGE PROMPT [options]

Options:
- `--from-file`: Treat prompt argument as a file path
```

Arguments:
- `STAGE`: The processing stage name
- `PROMPT_FILE`: Path to file containing the prompt

### delete-prompt

Delete the system prompt for a specific processing stage.

```bash
python -m clidataforge delete-prompt TABLE_NAME STAGE [options]
```

Arguments:
- `STAGE`: The processing stage name

### delete-column

Delete a column from the data table.

```bash
python -m clidataforge delete-column TABLE_NAME --column COLUMN [options]
```

Options:
- `--column`: Required. Column to delete

### process-all

Process all unprocessed chunks in parallel through the pipeline.

```bash
python -m clidataforge process-all TABLE_NAME --stages "source:dest[,source:dest...]" [options]
```

Options:
- `--threads`: Number of parallel threads (default: 1)
- `--stages`: Required. Comma-separated list of source:destination pairs
- `--sys-table`: Name of system prompts table (default: 'cliDataForgeSystem')

Examples:
```bash
# Basic pipeline
python -m clidataforge process-all my_table --stages "chunk:summary,summary:analysis,analysis:conclusion" --threads 4

# Using multiple source columns
python -m clidataforge process-all my_table --stages "title+content:summary,summary:analysis" --threads 2
```

### process-chunk

Process a single unprocessed chunk through the pipeline.

```bash
python -m clidataforge process-chunk TABLE_NAME --stages "source:dest[,source:dest...]" [options]
```

Options:
- `--stages`: Required. Comma-separated list of source:destination pairs
- `--sys-table`: Name of system prompts table (default: 'cliDataForgeSystem')

Examples:
```bash
# Basic pipeline
python -m clidataforge process-chunk my_table --stages "chunk:summary,summary:analysis"

# Using multiple source columns
python -m clidataforge process-chunk my_table --stages "title+body:summary"
```

### insert-data

Insert JSON list of text chunks into specified column.

```bash
python -m clidataforge insert-data TABLE_NAME JSON_FILE --column COLUMN
```

Arguments:
- `TABLE_NAME`: Name of the table to insert data into
- `JSON_FILE`: Path to JSON file containing list of text chunks
- `--column`: Required. Column name to insert data into

### clear-column

Clear all values in specified column (set to NULL).

```bash
python -m clidataforge clear-column TABLE_NAME --column COLUMN
```

Arguments:
- `TABLE_NAME`: Name of the table containing the column
- `--column`: Required. Column to clear

### list-columns

List all columns in the specified table with their types.

```bash
python -m clidataforge list-columns TABLE_NAME
```

Arguments:
- `TABLE_NAME`: Name of the table to list columns for

Shows detailed column information including data types and maximum lengths.

### list-tables

List all tables in the database.

```bash
python -m clidataforge list-tables
```

### create-table

Create a new table with specified columns.

```bash
python -m clidataforge create-table TABLE_NAME COLUMN:TYPE [COLUMN:TYPE...]
```

Arguments:
- `TABLE_NAME`: Name of the table to create
- `COLUMN:TYPE`: Column definitions in name:type format

Valid types: serial, text, varchar, int, float, boolean, timestamp

Example:
```bash
python -m clidataforge create-table mytable id:serial title:text content:text
```

### list-prompts

List all available system prompts for the specified table.

```bash
python -m clidataforge list-prompts TABLE_NAME [--sys-table SYS_TABLE]
```

Arguments:
- `TABLE_NAME`: Name of the table to list prompts for

### create-column

Create a new TEXT column in the table.

```bash
python -m clidataforge create-column TABLE_NAME --column COLUMN
```

Arguments:
- `TABLE_NAME`: Name of the table to add column to
- `--column`: Required. Column name to create

### setup

Interactive setup to create a .env file with required environment variables.

```bash
python -m clidataforge setup [--force]
```

Options:
- `--force`: Overwrite existing .env file

## Advanced Features

### Column Name Validation

The system validates column names and provides helpful suggestions if you mistype a column name:
```
Error: Source column 'titl' does not exist. Did you mean 'title'?
```

### Table Management

- Create tables with custom columns
- List all tables in the database
- View detailed column information
- Add, delete, or clear columns

### Prompt Management

- Store system prompts per table and stage
- View, add, update, or delete prompts
- Table-specific prompts with `table_name:stage` format

## Pipeline Features

- Multi-stage processing with configurable source/destination columns
- Multi-source column support (concatenate columns with `+`)
- Parallel processing with configurable thread count
- Automatic column creation and validation
- Progress tracking and error handling
- Retry logic for LLM API calls (3 attempts)
- Transaction management for database operations
- System prompt management (add/update/delete/show)
- Column management (create/delete/clear/save)
- Detailed column information display
- JSON data import/export capabilities
- Column name validation with suggestions
- Table-specific system prompts
