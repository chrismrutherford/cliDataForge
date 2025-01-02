# cliDataForge CLI Documentation

cliDataForge provides a command-line interface for managing and executing LLM processing pipelines with PostgreSQL integration.


## Pipeline Stages

Pipeline stages are specified as comma-separated pairs of `source:destination` columns:

- `chunk:summary`: Generate summary from input chunk
- `summary:analysis`: Generate analysis from summary
- `analysis:conclusion`: Generate conclusion from analysis

Example pipeline specification:
```bash
--stages "chunk:summary,summary:analysis,analysis:conclusion"
```

Each stage's processing is guided by system prompts stored in the system prompts table.
The pipeline automatically manages dependencies between stages and ensures data consistency.


## Architecture

![cliDataForge Architecture](cliDataForgeArchitecture.png)

## Global Options

These options are available for most commands:

- `--sys-table`: Name of system prompts table (default: 'cliDataForgeSystem')
- `--model`: Model to use (default: 'deepseek-chat')
- `--base-url`: Base URL for OpenAI-compatible API (default: 'https://api.deepseek.com')

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
python -m clidataforge process-all --stages "source:dest[,source:dest...]" [options]
```

Options:
- `--api-key`: API key for LLM service (env: CLIDATAFORGE_API_KEY)
- `--base-url`: Base URL for OpenAI-compatible API (env: OPENAI_BASE_URL)
- `--model`: Model to use (default: 'meta-llama/llama-3.3-70b-instruct')
- `--threads`: Number of parallel threads (default: 1)
- `--stages`: Required. Comma-separated list of source:destination pairs

Example:
```bash
python -m clidataforge process-all --stages "chunk:summary,summary:analysis,analysis:conclusion" --threads 4
```

### process-chunk

Process a single unprocessed chunk through the pipeline.

```bash
python -m clidataforge process-chunk --stages "source:dest[,source:dest...]" [options]
```

Options:
- `--api-key`: API key for LLM service (env: LLAMAFLOW_API_KEY)
- `--base-url`: Base URL for OpenAI-compatible API (env: OPENAI_BASE_URL)
- `--model`: Model to use (default: 'meta-llama/llama-3.3-70b-instruct')
- `--stages`: Required. Comma-separated list of source:destination pairs

### insert-data

Insert JSON list of text chunks into specified column.

```bash
python -m clidataforge insert-data JSON_FILE --column COLUMN [options]
```

Arguments:
- `JSON_FILE`: Path to JSON file containing list of text chunks
- `--column`: Required. Column name to insert data into

### clear-column

Clear all values in specified column (set to NULL).

```bash
python -m clidataforge clear-column --column COLUMN [options]
```

Options:
- `--column`: Required. Column to clear

### list-columns

List all columns in the data table with their types.

```bash
python -m clidataforge list-columns [options]
```

Shows detailed column information including data types and maximum lengths.

## Environment Variables

The following environment variables can be used:

- `CLI_DF_API_KEY`: API key for LLM service
- `CLI_DF_BASE_URL`: Base URL for OpenAI-compatible API service (default: 'https://api.deepseek.com')
- `CLI_DF_MODEL`: Model to use for completions (default: 'deepseek-chat')
- `DB_NAME`: Database name (default: 'llmdata')
- `DB_USER`: Database user (default: 'postgres')
- `DB_PASSWORD`: Database password
- `DB_HOST`: Database host (default: 'localhost')
- `DB_PORT`: Database port (default: '5432')

## Pipeline Features

- Multi-stage processing with configurable source/destination columns
- Parallel processing with configurable thread count
- Automatic column creation and validation
- Progress tracking and error handling
- Retry logic for LLM API calls (3 attempts)
- Transaction management for database operations
- System prompt management (add/update/delete/show)
- Column management (create/delete/clear/save)
- Detailed column information display
- JSON data import/export capabilities
