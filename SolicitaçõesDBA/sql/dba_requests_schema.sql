-- Execute este script conectado ao banco 'dba_requests' com um usuário com permissão (ex.: postgres ou dba_app)
-- Cria o esquema (tabelas), índices e dados mínimos.

-- =========================
-- Tabelas
-- =========================
CREATE TABLE IF NOT EXISTS users (
    id BIGSERIAL PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    salt TEXT NOT NULL,
    is_admin BOOLEAN NOT NULL DEFAULT FALSE,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TEXT NOT NULL,
    last_login_at TEXT
);

CREATE TABLE IF NOT EXISTS groups (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS user_groups (
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    group_id BIGINT NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    PRIMARY KEY (user_id, group_id)
);

CREATE TABLE IF NOT EXISTS dba_requests (
    id BIGSERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    cliente TEXT,
    sistema TEXT,
    prioridade TEXT NOT NULL DEFAULT 'Média',
    loja_parada BOOLEAN NOT NULL DEFAULT FALSE,
    status TEXT NOT NULL DEFAULT 'aberta',
    created_by BIGINT REFERENCES users(id) ON DELETE SET NULL,
    created_at TEXT NOT NULL,
    taken_by BIGINT REFERENCES users(id) ON DELETE SET NULL,
    taken_at TEXT,
    closed_at TEXT,
    last_update TEXT,
    history TEXT
);

CREATE TABLE IF NOT EXISTS dba_request_messages (
    id BIGSERIAL PRIMARY KEY,
    request_id BIGINT NOT NULL REFERENCES dba_requests(id) ON DELETE CASCADE,
    sender_id  BIGINT NOT NULL REFERENCES users(id) ON DELETE SET NULL,
    message TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dba_request_files (
    id BIGSERIAL PRIMARY KEY,
    request_id BIGINT NOT NULL REFERENCES dba_requests(id) ON DELETE CASCADE,
    filename TEXT NOT NULL,
    mime TEXT NOT NULL,
    size_bytes BIGINT NOT NULL,
    blob BYTEA NOT NULL,
    uploaded_by BIGINT NOT NULL REFERENCES users(id) ON DELETE SET NULL,
    created_at TEXT NOT NULL
);

-- =========================
-- Índices
-- =========================
CREATE INDEX IF NOT EXISTS idx_users_active ON users(active);
CREATE INDEX IF NOT EXISTS idx_requests_status ON dba_requests(status);
CREATE INDEX IF NOT EXISTS idx_requests_created_at ON dba_requests(created_at);
CREATE INDEX IF NOT EXISTS idx_requests_taken_by ON dba_requests(taken_by);
CREATE INDEX IF NOT EXISTS idx_msg_req ON dba_request_messages(request_id);
CREATE INDEX IF NOT EXISTS idx_msg_created_at ON dba_request_messages(created_at);
CREATE INDEX IF NOT EXISTS idx_file_req ON dba_request_files(request_id);
CREATE INDEX IF NOT EXISTS idx_file_created_at ON dba_request_files(created_at);
CREATE INDEX IF NOT EXISTS idx_msg_req_id ON dba_request_messages(request_id, id);

-- =========================
-- Dados mínimos
-- =========================
INSERT INTO groups(name) VALUES ('SUPORTE') ON CONFLICT (name) DO NOTHING;
INSERT INTO groups(name) VALUES ('DBA') ON CONFLICT (name) DO NOTHING;
