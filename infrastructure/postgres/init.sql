CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY,
    prompt TEXT NOT NULL,

    status VARCHAR(20) NOT NULL
        CHECK (status IN ('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED')),

    result TEXT,
    error TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);