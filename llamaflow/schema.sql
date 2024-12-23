-- System prompts table
CREATE TABLE IF NOT EXISTS "llamaFlowSystem" (
    stage VARCHAR(50) PRIMARY KEY,
    prompt TEXT NOT NULL
);

-- Data processing table
CREATE TABLE IF NOT EXISTS "llamaFlowData" (
    index SERIAL PRIMARY KEY,
    chunk TEXT NOT NULL,
    summary TEXT,
    analysis TEXT,
    conclusion TEXT
);
