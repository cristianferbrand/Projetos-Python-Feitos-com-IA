# -*- coding: utf-8 -*-
"""
App de Solicitações para Setor de DBA (Flet) — (Abas + Detalhe + Chat + Anexos)
pip install flet "psycopg[binary]" tzdata psycopg_pool
"""
from __future__ import annotations

import psycopg
from psycopg_pool import ConnectionPool
from psycopg.rows import dict_row
from psycopg import sql
import os
import json
import csv
import hashlib
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Any, Dict, Tuple, Callable
from zoneinfo import ZoneInfo
import base64
import mimetypes
import platform, subprocess, webbrowser

import flet as ft

# ---------------------------------------------------------------------------
# Compat cores/ícones (Flet 0.22+ / 0.21-) + helper op()
# ---------------------------------------------------------------------------
try:
    C = ft.Colors
except AttributeError:
    C = ft.colors
try:
    I = ft.Icons
except AttributeError:
    I = ft.icons

def _resolve_with_opacity():
    try:
        return ft.colors.with_opacity
    except Exception:
        try:
            return ft.Colors.with_opacity  # type: ignore[attr-defined]
        except Exception:
            return lambda alpha, color: color

with_opacity = _resolve_with_opacity()

def op(alpha: float, color: str) -> str:
    try:
        return with_opacity(alpha, color)
    except Exception:
        return color

def ftype_from_name(name: str) -> str:
    m, _ = mimetypes.guess_type(name)
    return m or "application/octet-stream"

# ---------------------------------------------------------------------------
# Paletas de alto contraste (Dark/Light)
# ---------------------------------------------------------------------------
PALETTE_DARK = {
    "bg": C.BLACK,
    "card": op(0.08, C.WHITE),
    "text": C.WHITE,
    "muted": C.GREY_300,
    "primary": C.TEAL_400 if hasattr(C, "TEAL_400") else C.TEAL,
    "primary_text": C.WHITE,
    "success": C.GREEN_400 if hasattr(C, "GREEN_400") else C.GREEN,
    "warning": C.AMBER_400 if hasattr(C, "AMBER_400") else C.AMBER,
    "danger": C.RED_400 if hasattr(C, "RED_400") else C.RED,
}

PALETTE_LIGHT = {
    "bg": C.WHITE,
    "card": C.GREY_100,
    "text": C.BLACK,
    "muted": C.GREY_700,
    "primary": C.TEAL_700 if hasattr(C, "TEAL_700") else C.TEAL,
    "primary_text": C.WHITE,
    "success": C.GREEN_700 if hasattr(C, "GREEN_700") else C.GREEN,
    "warning": C.AMBER_800 if hasattr(C, "AMBER_800") else C.AMBER,
    "danger": C.RED_700 if hasattr(C, "RED_700") else C.RED,
}

# ---------------------------------------------------------------------------
# Constantes/Paths
# ---------------------------------------------------------------------------
TZ = ZoneInfo("America/Sao_Paulo")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
EXPORT_DIR = os.path.join(BASE_DIR, "export")
CONFIG_DIR = os.path.join(BASE_DIR, "config")
DB_PATH = os.path.join(DATA_DIR, "dba_requests.db")  # reaproveita o mesmo DB
SETTINGS_PATH = os.path.join(CONFIG_DIR, "settings.json")

for d in (DATA_DIR, EXPORT_DIR, CONFIG_DIR):
    os.makedirs(d, exist_ok=True)

SQLITE_DEFAULT_TIMEOUT = 30.0
SQLITE_BUSY_TIMEOUT_MS = 10_000
CHAT_POLL_INTERVAL_S = float(os.getenv("CHAT_POLL_INTERVAL_S", "2.0"))

# ---------------------------------------------------------------------------
# Settings helpers
# ---------------------------------------------------------------------------
def load_settings() -> Dict[str, Any]:
    try:
        with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_settings(data: Dict[str, Any]) -> None:
    try:
        with open(SETTINGS_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

# ---------------------------------------------------------------------------
# Esquema: usuários, grupos, associação, solicitações DBA, mensagens e arquivos

# --- [SCHEMA SQL (module-level)] ---
PG_SCHEMA = """
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
    sender_id BIGINT NOT NULL REFERENCES users(id) ON DELETE SET NULL,
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
CREATE TABLE IF NOT EXISTS dba_request_reads (
    request_id BIGINT NOT NULL REFERENCES dba_requests(id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    last_seen_msg_id BIGINT NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (request_id, user_id)
);

"""

# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------
@dataclass
class User:
    id: int
    username: str
    is_admin: bool
    active: bool
    created_at: datetime
    last_login_at: Optional[datetime]

@dataclass
class Group:
    id: int
    name: str

@dataclass
class Request:
    id: int
    title: str
    description: str
    cliente: str
    sistema: str
    prioridade: str
    loja_parada: bool
    status: str
    created_by: int
    created_at: datetime
    taken_by: Optional[int]
    taken_at: Optional[datetime]
    closed_at: Optional[datetime]
    last_update: datetime
    history: str

@dataclass
class Message:
    id: int
    request_id: int
    sender_id: int
    message: str
    created_at: datetime

@dataclass
class Attachment:
    id: int
    request_id: int
    filename: str
    mime: str
    size_bytes: int
    blob: bytes
    uploaded_by: int
    created_at: datetime

# ---------------------------------------------------------------------------
# Segurança
# ---------------------------------------------------------------------------
import secrets as _secrets

def _gen_salt(n: int = 16) -> bytes:
    return _secrets.token_bytes(n)

def _hash_password(password: str, salt: bytes, rounds: int = 200_000) -> str:
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, rounds, dklen=32)
    return dk.hex()

# ---------------------------------------------------------------------------
# Banco de dados
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# PostgreSQL connection (pool) helpers + settings.json bootstrap

# ---------------------------------------------------------------------------
# URL helpers: percent-encode seguro para user/password (evita erro com '@')

# --- DB auto-create helper ---------------------------------------------------
def _maybe_create_db(url: str, settings: dict) -> None:
    """Cria o database alvo se não existir.
    - Usa o mesmo host/porta do DSN alvo.
    - Tenta com a própria credencial (se tiver CREATEDB).
    - Se houver settings['DB_ADMIN_URL'] ou env DATABASE_ADMIN_URL, tenta com admin.
    """
    try:
        from urllib.parse import urlparse, urlunparse
        import os as _os_autodb

        safe = ensure_percent_encoded_dsn(url)
        parsed = urlparse(safe)
        dbname = (parsed.path or '').lstrip('/') or 'postgres'
        if dbname == 'postgres':
            return

        # Descobrir admin_url
        admin_url = (
            settings.get('DB_ADMIN_URL')
            or _os_autodb.getenv('DATABASE_ADMIN_URL')
        )

        if not admin_url:
            # usa o mesmo usuário/host/porta, mas banco 'postgres'
            admin_url = urlunparse((parsed.scheme, parsed.netloc, '/postgres', parsed.params, parsed.query, parsed.fragment))

        import psycopg
        from psycopg import sql

        # Conecta no admin
        with psycopg.connect(admin_url) as con:
            con.autocommit = True
            with con.cursor() as cur:
                cur.execute("SELECT 1 FROM pg_database WHERE datname=%s", (dbname,))
                exists = cur.fetchone() is not None
                if not exists:
                    owner = parsed.username or 'postgres'
                    try:
                        cur.execute(sql.SQL("CREATE DATABASE {} OWNER {}" ).format(sql.Identifier(dbname), sql.Identifier(owner)))
                        # ok, criado
                    except Exception as ce:
                        # Sem permissão? Loga dica
                        print(f"[db-init] Falha ao criar banco '{dbname}': {ce}")
                        print("[db-init] Dicas: conceda CREATEDB ao usuário OU informe DB_ADMIN_URL/DATABASE_ADMIN_URL com um superuser.")
    except Exception as e:
        print(f"[db-init] Aviso: rotina de auto-criação falhou: {e}")

# --- Debug helpers -----------------------------------------------------------
import os as _os_dbg
from urllib.parse import urlsplit as _urlsplit_dbg, unquote as _unquote_dbg

def _mask_dsn_for_log(dsn: str) -> str:
    try:
        p = _urlsplit_dbg(dsn if "://" in dsn else f"postgresql://{dsn}")
        user = p.username or ""
        pwd = p.password or ""
        host = p.hostname or ""
        port = f":{p.port}" if p.port else ""
        path = p.path or ""
        auth = f"{user}:***@" if user or pwd else ""
        return f"{p.scheme}://{auth}{host}{port}{path}"
    except Exception:
        # fallback: do not leak anything
        return "<dsn hidden>"

# ---------------------------------------------------------------------------
from urllib.parse import urlsplit, urlunsplit, quote, unquote, urlencode

def _encode_auth(user: str | None, password: str | None) -> tuple[str, str]:
    """Percent-encode seguro para user/password (evita double-encoding)."""
    u = quote(unquote(user or ""), safe="")
    p = quote(unquote(password or ""), safe="")
    return u, p

def ensure_percent_encoded_dsn(dsn: str, default_scheme: str = "postgresql") -> str:
    """Garante credenciais percent-encoded sem alterar host/porta/path/query."""
    if not dsn:
        return dsn
    candidate = dsn if "://" in dsn else f"{default_scheme}://{dsn}"
    parts = urlsplit(candidate)
    user = parts.username or ""
    pwd = parts.password or ""
    enc_user, enc_pwd = _encode_auth(user, pwd)
    host = parts.hostname or ""
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    auth = ""
    if user or pwd:
        if user and pwd:
            auth = f"{enc_user}:{enc_pwd}@"
        elif user and not pwd:
            auth = f"{enc_user}@"
        else:
            auth = f":{enc_pwd}@"
    netloc = f"{auth}{host}"
    if parts.port:
        netloc += f":{parts.port}"
    safe_parts = (parts.scheme or default_scheme, netloc, parts.path or "", parts.query or "", parts.fragment or "")
    return urlunsplit(safe_parts)

def build_pg_dsn(user: str, password: str, host: str = "127.0.0.1", port: int = 5432,
                 dbname: str = "postgres", query_params: dict | None = None) -> str:
    enc_user, enc_pwd = _encode_auth(user, password)
    host_fmt = host
    if ":" in host and not host.startswith("["):
        host_fmt = f"[{host}]"
    base = f"postgresql://{enc_user}:{enc_pwd}@{host_fmt}:{port}/{dbname}"
    if query_params:
        return f"{base}?{urlencode(query_params)}"
    return base

def resolve_database_url(settings: dict, default_url: str) -> str:
    import os
    raw = (settings.get("DATABASE_URL") or os.getenv("DATABASE_URL") or default_url)
    return ensure_percent_encoded_dsn(raw)

# ---------------------------------------------------------------------------
_POOL = None
_POOL_URL = None

def ensure_settings_defaults() -> dict:
    s = load_settings()
    default_dsn = "postgresql://dba_app:suporte@123@127.0.0.1:5432/dba_requests"
    changed = False
    if not s.get("DATABASE_URL"):
        s["DATABASE_URL"] = default_dsn
        changed = True
    if changed:
        save_settings(s)
    return s

def _read_database_url_from_settings() -> str:
    s = load_settings()
    return s.get("DATABASE_URL") or os.getenv("DATABASE_URL") or "postgresql://dba_app:suporte@123@127.0.0.1:5432/dba_requests"

def get_pool():
    ensure_database_exists_on_startup()
    global _POOL, _POOL_URL
    s = ensure_settings_defaults()
    # DSN robusto: percent-encode das credenciais
    url = resolve_database_url(s, default_url="postgresql://dba_app:suporte@123@127.0.0.1:5432/dba_requests")

    # Log controlado (sem expor senha)
    if _os_dbg.getenv("DB_DEBUG") == "1" or s.get("DEBUG_DB") is True:
        try:
            print(f"[db] Using DSN: {_mask_dsn_for_log(url)}")
        except Exception:
            print("[db] Using DSN (masked)")

    if _POOL is None or _POOL_URL != url:
        # Tenta criar pool com DSN de URL; se falhar por parse/autenticação,
        # faz fallback para conninfo mapping (sem URL)
        try:
            if _POOL is not None:
                _POOL.close()
        except Exception:
            pass

        try:
            _POOL = ConnectionPool(url, min_size=1, max_size=10)
            _POOL_URL = url
        except Exception as e_url:
            # Fallback: construir conninfo mapping a partir da URL
            try:
                p = _urlsplit_dbg(url if "://" in url else f"postgresql://{url}")
                conninfo = {
                    "user": _unquote_dbg(p.username or ""),
                    "password": _unquote_dbg(p.password or ""),
                    "host": p.hostname or "127.0.0.1",
                    "port": p.port or 5432,
                    "dbname": (p.path or "").lstrip("/") or "postgres",
                }
                if _os_dbg.getenv("DB_DEBUG") == "1" or s.get("DEBUG_DB") is True:
                    print(f"[db] Fallback conninfo: user={conninfo['user']}, host={conninfo['host']}, port={conninfo['port']}, db={conninfo['dbname']}")
                _POOL = ConnectionPool(conninfo=conninfo, min_size=1, max_size=10)
                _POOL_URL = url  # mantemos para comparação futura
            except Exception as e_map:
                # Re-levanta a exceção original (mais fiel) adicionando contexto do fallback
                raise RuntimeError(f"Falha ao criar pool por URL ({e_url!r}) e por mapping ({e_map!r}). Verifique senha/pg_hba/instância.") from e_map
    return _POOL

def ensure_database_exists_on_startup() -> None:
    """Se o database do DATABASE_URL não existir, tenta criar antes da conexão do pool."""
    try:
        s = ensure_settings_defaults()
        url = resolve_database_url(s, default_url="postgresql://dba_app:suporte@123@127.0.0.1:5432/dba_requests")
        _maybe_create_db(url, s)
    except Exception as e:
        print(f"[init] Aviso: ensure_database_exists_on_startup falhou: {e}")

class DB:
    def __init__(self, path: str):
        # path ignorado em PostgreSQL
        self._ensure_schema()
        self._migrate_indices()
        self._bootstrap_default_groups()

    def connect(self):
        # Pool dinâmico baseado no settings.json
        return get_pool().connection()

    def _executescript(self, con, script: str):
        with con.cursor() as cur:
            for stmt in script.split(";"):
                if stmt.strip():
                    cur.execute(stmt + ";")
        con.commit()

    def _ensure_schema(self):
        with self.connect() as con:
            self._executescript(con, PG_SCHEMA)

    def _migrate_indices(self):
        stmts = [
            "CREATE INDEX IF NOT EXISTS idx_users_active ON users(active);",
            "CREATE INDEX IF NOT EXISTS idx_requests_status ON dba_requests(status);",
            "CREATE INDEX IF NOT EXISTS idx_requests_created_at ON dba_requests(created_at);",
            "CREATE INDEX IF NOT EXISTS idx_requests_taken_by ON dba_requests(taken_by);",
            "CREATE INDEX IF NOT EXISTS idx_msg_req ON dba_request_messages(request_id);",
            "CREATE INDEX IF NOT EXISTS idx_msg_created_at ON dba_request_messages(created_at);",
            "CREATE INDEX IF NOT EXISTS idx_file_req ON dba_request_files(request_id);",
            "CREATE INDEX IF NOT EXISTS idx_file_created_at ON dba_request_files(created_at);",
            "CREATE INDEX IF NOT EXISTS idx_msg_req_id ON dba_request_messages(request_id, id);"
        
            "CREATE INDEX IF NOT EXISTS idx_reads_user ON dba_request_reads(user_id);",
            "CREATE INDEX IF NOT EXISTS idx_reads_req ON dba_request_reads(request_id);",
            "CREATE INDEX IF NOT EXISTS idx_msg_req_sender_id ON dba_request_messages(request_id, sender_id, id);",]
        with self.connect() as con:
            with con.cursor() as cur:
                for s in stmts:
                    cur.execute(s)
            con.commit()

    def _bootstrap_default_groups(self):
        with self.connect() as con:
            with con.cursor() as cur:
                cur.execute("INSERT INTO groups(name) VALUES (%s) ON CONFLICT (name) DO NOTHING", ("SUPORTE",))
                cur.execute("INSERT INTO groups(name) VALUES (%s) ON CONFLICT (name) DO NOTHING", ("DBA",))
            con.commit()

    # ---------- Users ----------
    def users_count(self) -> int:
        with self.connect() as con:
            with con.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM users")
                return int(cur.fetchone()[0])

    def active_admin_count(self) -> int:
        with self.connect() as con:
            with con.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM users WHERE is_admin=TRUE AND active=TRUE")
                return int(cur.fetchone()[0])

    def list_users(self) -> List[User]:
        with self.connect() as con:
            with con.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT * FROM users ORDER BY username ASC")
                return [self._row_to_user(r) for r in cur.fetchall()]

    def get_user_by_username(self, username: str) -> Optional[User]:
        with self.connect() as con:
            with con.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT * FROM users WHERE username=%s", (username.strip(),))
                row = cur.fetchone()
                return self._row_to_user(row) if row else None

    def create_user(self, username: str, password: str, is_admin: bool = False) -> int:
        salt = _gen_salt()
        ph = _hash_password(password, salt)
        now = datetime.now(TZ).isoformat()
        with self.connect() as con:
            with con.cursor() as cur:
                cur.execute("""
                    INSERT INTO users (username, password_hash, salt, is_admin, active, created_at)
                    VALUES (%s, %s, %s, %s, TRUE, %s)
                    RETURNING id
                """, (username.strip(), ph, salt.hex(), bool(is_admin), now))
                return int(cur.fetchone()[0])

    def set_user_password(self, user_id_or_name: int | str, new_password: str):
        salt = _gen_salt()
        ph = _hash_password(new_password, salt)
        with self.connect() as con:
            with con.cursor() as cur:
                if isinstance(user_id_or_name, int):
                    cur.execute("UPDATE users SET password_hash=%s, salt=%s WHERE id=%s", (ph, salt.hex(), user_id_or_name))
                else:
                    cur.execute("UPDATE users SET password_hash=%s, salt=%s WHERE username=%s", (ph, salt.hex(), user_id_or_name))
            con.commit()

    def verify_login(self, username: str, password: str) -> Optional[User]:
        with self.connect() as con:
            with con.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT * FROM users WHERE username=%s AND active=TRUE", (username.strip(),))
                row = cur.fetchone()
                if not row:
                    return None
                salt = bytes.fromhex(row["salt"]) if row["salt"] else b""
                ph = _hash_password(password, salt)
                if ph != row["password_hash"]:
                    return None
            with con.cursor() as cur2:
                cur2.execute("UPDATE users SET last_login_at=%s WHERE id=%s", (datetime.now(TZ).isoformat(), row["id"]))
                con.commit()
            return self._row_to_user(row)

    def set_user_admin(self, user_id: int, make_admin: bool) -> Tuple[bool, str]:
        with self.connect() as con:
            with con.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT is_admin, active FROM users WHERE id=%s", (user_id,))
                row = cur.fetchone()
                if not row:
                    return False, "Usuário não encontrado."
                if not make_admin and bool(row["is_admin"]) and bool(row["active"]) and self.active_admin_count() <= 1:
                    return False, "Não é possível remover a permissão do último administrador ativo."
            with con.cursor() as cur2:
                cur2.execute("UPDATE users SET is_admin=%s WHERE id=%s", (bool(make_admin), user_id))
                con.commit()
            return True, "Permissão atualizada."

    def set_user_active(self, user_id: int, make_active: bool) -> Tuple[bool, str]:
        with self.connect() as con:
            with con.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT is_admin, active FROM users WHERE id=%s", (user_id,))
                row = cur.fetchone()
                if not row:
                    return False, "Usuário não encontrado."
                if not make_active and bool(row["is_admin"]) and bool(row["active"]) and self.active_admin_count() <= 1:
                    return False, "Não é possível desativar o último administrador ativo."
            with con.cursor() as cur2:
                cur2.execute("UPDATE users SET active=%s WHERE id=%s", (bool(make_active), user_id))
                con.commit()
            return True, "Status atualizado."

    def delete_user(self, user_id: int) -> Tuple[bool, str]:
        with self.connect() as con:
            with con.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT is_admin, active FROM users WHERE id=%s", (user_id,))
                row = cur.fetchone()
                if not row:
                    return False, "Usuário não encontrado."
                if bool(row["is_admin"]) and bool(row["active"]) and self.active_admin_count() <= 1:
                    return False, "Não é possível excluir o último administrador ativo."
            with con.cursor() as cur2:
                cur2.execute("DELETE FROM users WHERE id=%s", (user_id,))
                con.commit()
            return True, "Usuário excluído."

    def _row_to_user(self, row: dict) -> User:
        return User(
            id=row["id"],
            username=row["username"],
            is_admin=bool(row["is_admin"]),
            active=bool(row["active"]),
            created_at=datetime.fromisoformat(row["created_at"]),
            last_login_at=datetime.fromisoformat(row["last_login_at"]) if row.get("last_login_at") else None,
        )

    # ---------- Groups ----------
    def ensure_group(self, name: str) -> int:
        with self.connect() as con:
            with con.cursor() as cur:
                cur.execute("INSERT INTO groups(name) VALUES (%s) ON CONFLICT (name) DO NOTHING", (name.strip(),))
                cur.execute("SELECT id FROM groups WHERE name=%s", (name.strip(),))
                return int(cur.fetchone()[0])

    def list_groups(self) -> List[Group]:
        with self.connect() as con:
            with con.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT * FROM groups ORDER BY name")
                return [Group(id=r["id"], name=r["name"]) for r in cur.fetchall()]

    def set_user_group(self, user_id: int, group_name: str, in_group: bool) -> None:
        gid = self.ensure_group(group_name)
        with self.connect() as con:
            with con.cursor() as cur:
                if in_group:
                    cur.execute("INSERT INTO user_groups(user_id, group_id) VALUES (%s,%s) ON CONFLICT (user_id, group_id) DO NOTHING", (user_id, gid))
                else:
                    cur.execute("DELETE FROM user_groups WHERE user_id=%s AND group_id=%s", (user_id, gid))
            con.commit()

    def user_in_group(self, user_id: int, group_name: str) -> bool:
        with self.connect() as con:
            with con.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM user_groups ug JOIN groups g ON g.id=ug.group_id WHERE ug.user_id=%s AND UPPER(TRIM(g.name))=UPPER(TRIM(%s))",
                    (user_id, group_name),
                )
                return bool(cur.fetchone())

    # ---------- Requests ----------
    def create_request(self, *, title: str, description: str, cliente: str, sistema: str,
                       prioridade: str, loja_parada: bool, created_by: int) -> int:
        now = datetime.now(TZ).isoformat()
        hist = [{"ts": now, "evt": "aberta", "by": created_by}]
        with self.connect() as con:
            with con.cursor() as cur:
                cur.execute("""
                    INSERT INTO dba_requests
                        (title, description, cliente, sistema, prioridade, loja_parada,
                         status, created_by, created_at, last_update, history)
                    VALUES (%s,%s,%s,%s,%s,%s,'aberta',%s,%s,%s,%s)
                    RETURNING id
                """, (title.strip(), (description or "").strip(), (cliente or "").strip(), (sistema or "").strip(),
                      (prioridade.strip() or "Média"), bool(loja_parada), created_by, now, now, json.dumps(hist, ensure_ascii=False)))
                return int(cur.fetchone()[0])

    def list_requests(self, *, status: Optional[str] = None, only_mine_opened_by: Optional[int] = None,
                      only_mine_taken_by: Optional[int] = None, limit: int = 500) -> List[Request]:
        q = "SELECT * FROM dba_requests WHERE 1=1"
        params = []
        if status:
            q += " AND status=%s"
            params.append(status)
        if only_mine_opened_by:
            q += " AND created_by=%s"
            params.append(only_mine_opened_by)
        if only_mine_taken_by:
            q += " AND taken_by=%s"
            params.append(only_mine_taken_by)
        q += " ORDER BY created_at DESC LIMIT %s"
        params.append(limit)
        with self.connect() as con:
            with con.cursor(row_factory=dict_row) as cur:
                cur.execute(q, tuple(params))
                return [self._row_to_request(r) for r in cur.fetchall()]

    def get_request(self, rid: int) -> Optional[Request]:
        with self.connect() as con:
            with con.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT * FROM dba_requests WHERE id=%s", (rid,))
                row = cur.fetchone()
                return self._row_to_request(row) if row else None

    def take_request(self, rid: int, user_id: int):
        now = datetime.now(TZ).isoformat()
        with self.connect() as con:
            with con.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT history FROM dba_requests WHERE id=%s", (rid,))
                row = cur.fetchone()
                hist = json.loads(row["history"] or "[]") if row else []
                hist.append({"ts": now, "evt": "em_atendimento", "by": user_id})
            with con.cursor() as cur2:
                cur2.execute(
                    "UPDATE dba_requests SET status='em_atendimento', taken_by=%s, taken_at=%s, last_update=%s, history=%s WHERE id=%s",
                    (user_id, now, now, json.dumps(hist, ensure_ascii=False), rid),
                )
                con.commit()

    def release_request(self, rid: int, user_id: int):
        now = datetime.now(TZ).isoformat()
        with self.connect() as con:
            with con.cursor() as cur:
                cur.execute("SELECT history FROM dba_requests WHERE id=%s", (rid,))
                row = cur.fetchone()
                hist = json.loads(row[0] or "[]") if row else []
                hist.append({"ts": now, "evt": "aberta", "by": user_id, "obs": "liberada"})
            with con.cursor() as cur2:
                cur2.execute(
                    "UPDATE dba_requests SET status='aberta', taken_by=NULL, taken_at=NULL, last_update=%s, history=%s WHERE id=%s",
                    (now, json.dumps(hist, ensure_ascii=False), rid),
                )
                con.commit()

    def resolve_request(self, rid: int, user_id: int):
        now = datetime.now(TZ).isoformat()
        with self.connect() as con:
            with con.cursor() as cur:
                cur.execute("SELECT history FROM dba_requests WHERE id=%s", (rid,))
                row = cur.fetchone()
                hist = json.loads(row[0] or "[]") if row else []
                hist.append({"ts": now, "evt": "resolvida", "by": user_id})
            with con.cursor() as cur2:
                cur2.execute(
                    "UPDATE dba_requests SET status='resolvida', closed_at=%s, last_update=%s, history=%s WHERE id=%s",
                    (now, now, json.dumps(hist, ensure_ascii=False), rid),
                )
                con.commit()

    def cancel_request(self, rid: int, user_id: int):
        with self.connect() as con:
            with con.cursor() as cur:
                cur.execute("SELECT is_admin FROM users WHERE id=%s", (user_id,))
                row = cur.fetchone()
                if not row or (not bool(row[0])):
                    raise PermissionError("Ação restrita ao administrador.")
        now = datetime.now(TZ).isoformat()
        with self.connect() as con:
            with con.cursor() as cur1:
                cur1.execute("SELECT history FROM dba_requests WHERE id=%s", (rid,))
                row = cur1.fetchone()
                hist = json.loads(row[0] or "[]") if row else []
                hist.append({"ts": now, "evt": "cancelada", "by": user_id})
            with con.cursor() as cur2:
                cur2.execute(
                    "UPDATE dba_requests SET status='cancelada', closed_at=%s, last_update=%s, history=%s WHERE id=%s",
                    (now, now, json.dumps(hist, ensure_ascii=False), rid),
                )
                con.commit()

    # ---------- Chat & Files ----------
    def add_message(self, request_id: int, sender_id: int, message: str) -> int:
        now = datetime.now(TZ).isoformat()
        with self.connect() as con:
            with con.cursor() as cur:
                cur.execute(
                    "INSERT INTO dba_request_messages(request_id, sender_id, message, created_at) VALUES (%s,%s,%s,%s) RETURNING id",
                    (request_id, sender_id, message.strip(), now),
                )
                return int(cur.fetchone()[0])

    def list_messages(self, request_id: int, limit: int = 200) -> List[Message]:
        with self.connect() as con:
            with con.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    "SELECT * FROM dba_request_messages WHERE request_id=%s ORDER BY created_at ASC LIMIT %s",
                    (request_id, limit),
                )
                def _ts(x): return datetime.fromisoformat(x) if x else None
                return [Message(id=r["id"], request_id=r["request_id"], sender_id=r["sender_id"], message=r["message"], created_at=_ts(r["created_at"])) for r in cur.fetchall()]

    def last_message_id(self, request_id: int) -> int:
        with self.connect() as con:
            with con.cursor() as cur:
                cur.execute("SELECT MAX(id) FROM dba_request_messages WHERE request_id=%s", (request_id,))
                row = cur.fetchone()
                return int(row[0] or 0)

    def list_messages_since(self, request_id: int, last_id: int, limit: int = 500) -> List[Message]:
        with self.connect() as con:
            with con.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    "SELECT * FROM dba_request_messages WHERE request_id=%s AND id>%s ORDER BY id ASC LIMIT %s",
                    (request_id, last_id, limit),
                )
                def _ts(x): return datetime.fromisoformat(x) if x else None
                return [Message(id=r["id"], request_id=r["request_id"], sender_id=r["sender_id"], message=r["message"], created_at=_ts(r["created_at"])) for r in cur.fetchall()]

    # ---------- NL (não lidas): leituras e contagem ----------

    def get_last_seen(self, request_id: int, user_id: int) -> int:
        with self.connect() as con:
            with con.cursor() as cur:
                cur.execute(
                    "SELECT last_seen_msg_id FROM dba_request_reads WHERE request_id=%s AND user_id=%s",
                    (request_id, user_id),
                )
                row = cur.fetchone()
                return int(row[0]) if row and row[0] is not None else 0

    def set_last_seen(self, request_id: int, user_id: int, last_seen_msg_id: int) -> None:
        now = datetime.now(TZ).isoformat()
        with self.connect() as con:
            with con.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dba_request_reads (request_id, user_id, last_seen_msg_id, updated_at)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (request_id, user_id)
                    DO UPDATE SET last_seen_msg_id = EXCLUDED.last_seen_msg_id,
                                  updated_at = EXCLUDED.updated_at
                    """,
                    (request_id, user_id, int(last_seen_msg_id), now),
                )
            con.commit()

    def get_unread_count(self, request_id: int, me_id: int, other_id: int) -> int:
        """
        Conta quantas mensagens do 'other_id' estão com id > last_seen(me_id) no request.
        """
        if not other_id or other_id == me_id:
            return 0
        last_seen = self.get_last_seen(request_id, me_id)
        with self.connect() as con:
            with con.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*) FROM dba_request_messages
                     WHERE request_id=%s
                       AND sender_id=%s
                       AND id > %s
                    """,
                    (request_id, other_id, last_seen),
                )
                row = cur.fetchone()
                return int(row[0] or 0)

    def get_message_dict(self, msg_id: int) -> Dict[str, Any]:
        with self.connect() as con:
            with con.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT id, request_id, sender_id, message, created_at FROM dba_request_messages WHERE id=%s", (msg_id,))
                r = cur.fetchone()
                return dict(r) if r else {}

    def add_file(self, request_id: int, filename: str, mime: str, data: bytes, uploaded_by: int) -> int:
        now = datetime.now(TZ).isoformat()
        with self.connect() as con:
            with con.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dba_request_files(request_id, filename, mime, size_bytes, blob, uploaded_by, created_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    RETURNING id
                    """,
                    (request_id, filename, mime, len(data), data, uploaded_by, now),
                )
                return int(cur.fetchone()[0])

    def list_files(self, request_id: int) -> List[Attachment]:
        with self.connect() as con:
            with con.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT * FROM dba_request_files WHERE request_id=%s ORDER BY created_at ASC", (request_id,))
                def _ts(x): return datetime.fromisoformat(x) if x else None
                return [
                    Attachment(
                        id=r["id"], request_id=r["request_id"], filename=r["filename"], mime=r["mime"],
                        size_bytes=r["size_bytes"], blob=(r["blob"] if r["blob"] is not None else b""),
                        uploaded_by=r["uploaded_by"], created_at=_ts(r["created_at"])
                    ) for r in cur.fetchall()
                ]

    def get_file(self, file_id: int) -> Optional[Attachment]:
        with self.connect() as con:
            with con.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT * FROM dba_request_files WHERE id=%s", (file_id,))
                r = cur.fetchone()
                if not r:
                    return None
                def _ts(x): return datetime.fromisoformat(x) if x else None
                return Attachment(
                    id=r["id"], request_id=r["request_id"], filename=r["filename"], mime=r["mime"],
                    size_bytes=r["size_bytes"], blob=(r["blob"] if r["blob"] is not None else b""),
                    uploaded_by=r["uploaded_by"], created_at=_ts(r["created_at"])
                )

    def delete_file(self, file_id: int) -> None:
        with self.connect() as con:
            with con.cursor() as cur:
                cur.execute("DELETE FROM dba_request_files WHERE id=%s", (file_id,))
            con.commit()

    def _row_to_request(self, r: dict) -> Request:
        def _ts(x): return datetime.fromisoformat(x) if x else None
        return Request(
            id=r["id"], title=r["title"], description=r.get("description") or "", cliente=r.get("cliente") or "",
            sistema=r.get("sistema") or "", prioridade=r.get("prioridade") or "Média",
            loja_parada=bool(r["loja_parada"]), status=r["status"],
            created_by=r["created_by"] if r.get("created_by") is not None else 0,
            created_at=_ts(r["created_at"]), taken_by=r.get("taken_by"), taken_at=_ts(r.get("taken_at")),
            closed_at=_ts(r.get("closed_at")), last_update=_ts(r.get("last_update")) or datetime.now(TZ),
            history=r.get("history") or "[]",
        )

# ---------------------------------------------------------------------------
# Views: Login e App
# ---------------------------------------------------------------------------

class LoginView:
    def __init__(self, page: ft.Page, db: DB, on_success: Callable[[User], None]):
        self.page = page
        self.db = db
        self.on_success = on_success
        self._build()

    def _toast(self, msg: str):
        self.page.snack_bar = ft.SnackBar(ft.Text(msg))
        self.page.snack_bar.open = True
        self.page.update()

    def _build(self):
        self.page.title = "Login - Solicitações DBA"
        settings = load_settings()
        theme_mode = settings.get("theme_mode", "dark")
        self.page.theme_mode = ft.ThemeMode.DARK if theme_mode == "dark" else ft.ThemeMode.LIGHT
        self.palette = PALETTE_DARK if theme_mode == "dark" else PALETTE_LIGHT
        self.page.theme = ft.Theme(color_scheme_seed=C.TEAL, use_material3=True)
        self.page.padding = 0

        settings = load_settings()
        last_user = settings.get("last_username", "")
        self.in_user = ft.TextField(label="Usuário", value=last_user, prefix_icon=I.PERSON, expand=True)
        self.in_pwd = ft.TextField(label="Senha", password=True, can_reveal_password=True, prefix_icon=I.LOCK, expand=True)
        self.remember = ft.Checkbox(label="Lembrar usuário", value=bool(last_user))

        def do_login(e):
            if not self.in_user.value or not self.in_pwd.value:
                self._toast("Informe usuário e senha.")
                return
            u = self.db.verify_login(self.in_user.value, self.in_pwd.value)
            if not u:
                self._toast("Usuário e/ou senha inválidos ou inativo.")
                return
            s = load_settings()
            if self.remember.value:
                s["last_username"] = self.in_user.value.strip()
            else:
                s.pop("last_username", None)
            save_settings(s)
            # limpa a tela de login antes de entrar no app
            self.page.controls.clear()
            self.page.update()
            self.on_success(u)

        # Painel para criar o 1º admin
        self.adm_user = ft.TextField(label="Usuário (admin)", width=220)
        self.adm_p1 = ft.TextField(label="Senha", password=True, can_reveal_password=True, width=220)
        self.adm_p2 = ft.TextField(label="Confirmar senha", password=True, can_reveal_password=True, width=220)

        def do_create_admin(e):
            if self.db.users_count() > 0:
                self._toast("Já existe usuário. Peça a um admin.")
                return
            if not self.adm_user.value or not self.adm_p1.value or not self.adm_p2.value:
                self._toast("Preencha usuário e senha.")
                return
            if self.adm_p1.value != self.adm_p2.value:
                self._toast("Senhas não conferem.")
                return
            self.db.create_user(self.adm_user.value, self.adm_p1.value, is_admin=True)
            self._toast("Administrador criado. Faça login.")

        create_admin_panel = ft.Container(
            visible=(self.db.users_count() == 0),
            bgcolor=C.GREY_900, border_radius=12, padding=12,
            content=ft.Column([
                ft.Text("Criar Administrador (primeiro acesso)", size=14, weight=ft.FontWeight.BOLD),
                ft.Row([self.adm_user, self.adm_p1, self.adm_p2], spacing=10, wrap=True),
                ft.Row([
                    ft.FilledButton("Criar", icon=I.CHECK, on_click=do_create_admin),
                ], alignment=ft.MainAxisAlignment.END),
            ], spacing=8)
        )

        login_btn = ft.FilledButton("Entrar", icon=I.CHEVRON_RIGHT, on_click=do_login, expand=True)

        card = ft.Card(
            elevation=12,
            content=ft.Container(
                width=560,
                padding=24,
                border_radius=ft.border_radius.all(18),
                content=ft.Column([
                    ft.Text("Solicitações DBA", size=22, weight=ft.FontWeight.W_700, text_align=ft.TextAlign.CENTER),
                    ft.Text("Acesse para abrir solicitações ou atender a fila", size=12, color=C.GREY_500, text_align=ft.TextAlign.CENTER),
                    ft.Divider(),
                    self.in_user,
                    self.in_pwd,
                    self.remember,
                    login_btn,
                    ft.Divider(),
                    create_admin_panel,
                ], spacing=12, tight=True)
            )
        )

        backdrop = ft.Container(expand=True, gradient=ft.LinearGradient(begin=ft.alignment.top_left, end=ft.alignment.bottom_right, colors=[C.BLUE_GREY_900, C.BLACK]))
        self.page.controls.clear()
        self.page.add(ft.Stack(controls=[backdrop, ft.Container(content=card, alignment=ft.alignment.center, expand=True, padding=20)]))
        self.page.update()

# ---------------------------------------------------------------------------
# App principal (com Abas)
# ---------------------------------------------------------------------------
class DBAApp:

    # Helper: normaliza opções do Dropdown de Status (garante 'todos' único e remove duplicatas)
    def _normalize_status_dropdown(self, dd):
        
        try:
            if dd is None:
                return
            import re as _re

            def _to_text(obj):
                # 1) try direct attributes
                for attr in ("text", "value", "key", "label", "name"):
                    try:
                        v = getattr(obj, attr, None)
                        if v is not None and str(v).strip():
                            return str(v).strip()
                    except Exception:
                        pass
                # 2) If it's a plain string
                if isinstance(obj, str):
                    s = obj.strip()
                else:
                    s = str(obj).strip()

                s_low = s.lower()
                # 3) Try to parse Flet reprs like: dropdownoption {'key': 'aberta'}
                m = _re.search(r"'key'\s*:\s*'([^']+)'", s)
                if m:
                    return m.group(1).strip()
                # 4) Try to parse Option(text='...') / value='...'
                m = _re.search(r"(?:text|value)\s*=\s*'([^']+)'", s)
                if m:
                    return m.group(1).strip()
                # 5) If looks like a serialized Option and we couldn't extract, ignore
                if s_low.startswith("dropdownoption") or s_low.startswith("option("):
                    return ""
                # 6) Otherwise, use as-is
                return s

            # Current value (keep if possible)
            cur = getattr(dd, "value", None)

            # Collect candidates from existing options
            texts = []
            try:
                for opt in list(dd.options or []):
                    try:
                        t = _to_text(opt)
                        if isinstance(t, str):
                            texts.append(t)
                    except Exception:
                        pass
            except Exception:
                pass

            # Add defaults and current value
            defaults = ["aberta", "em_atendimento", "resolvida", "cancelada"]
            texts.extend(defaults)
            if cur:
                texts.append(str(cur).strip())

            # Dedupe (case-insensitive) and drop 'todos'
            seen = set()
            rest = []
            for t in texts:
                key = (t or "").strip().lower()
                if not key or key == "todos":
                    continue
                if key not in seen:
                    seen.add(key)
                    rest.append(t.strip())

            # Rebuild dropdown: 'todos' + unique rest
            dd.options = [ft.dropdown.Option("todos")] + [ft.dropdown.Option(t) for t in rest]

            # Keep value if still available; else fallback to 'todos'
            if cur:
                cur_key = str(cur).strip().lower()
                if cur_key == "todos":
                    dd.value = "todos"
                else:
                    match = next((t for t in rest if t.lower() == cur_key), None)
                    dd.value = match if match else "todos"
            else:
                if not getattr(dd, "value", None):
                    dd.value = "todos"

            try:
                dd.update()
            except Exception:
                pass
        except Exception:
            pass

            def _opt_text(_o):
                try:
                    _t = getattr(_o, "text", None)
                    if _t is not None and str(_t).strip():
                        return str(_t).strip()
                except Exception:
                    pass
                try:
                    _t = getattr(_o, "value", None)
                    if _t is not None and str(_t).strip():
                        return str(_t).strip()
                except Exception:
                    pass
                _s = str(_o)
                m = _re.search(r"[Oo]ption\(\s*(?:text=)?[\"']([^\"']+)[\"']", _s)
                if m:
                    return m.group(1).strip()
                return _s.strip()

            defaults = ["aberta", "em_atendimento", "resolvida", "cancelada"]
            cur = getattr(dd, "value", None)

            texts = []
            try:
                for _o in list(dd.options or []):
                    try:
                        texts.append(_opt_text(_o))
                    except Exception:
                        pass
            except Exception:
                pass

            texts.extend(defaults)
            if cur:
                texts.append(str(cur).strip())

            seen = set()
            rest = []
            for _t in texts:
                _key = (str(_t) or "").strip().lower()
                if not _key or _key == "todos":
                    continue
                if _key not in seen:
                    seen.add(_key)
                    rest.append(str(_t).strip())

            dd.options = [ft.dropdown.Option("todos")] + [ft.dropdown.Option(t) for t in rest]

            if cur:
                ckey = str(cur).strip().lower()
                if ckey == "todos":
                    dd.value = "todos"
                else:
                    _match = next((t for t in rest if t.strip().lower() == ckey), None)
                    dd.value = _match if _match else "todos"
            else:
                if not getattr(dd, "value", None):
                    dd.value = "todos"

            try:
                dd.update()
            except Exception:
                pass
        except Exception:
            pass

    # --- Helper: normaliza valor do filtro de status ("Todos" -> None) ---
    def _normalize_status(self, status):
        try:
            s = (status or "").strip().lower()
        except Exception:
            s = str(status).strip().lower() if status is not None else ""
        if s in ("todos", "todas", "__all__", "all", "*", ""):
            return None
        return status
    def _build_minhas_table(self) -> ft.DataTable:
            return ft.DataTable(
                columns=[
                    ft.DataColumn(ft.Text("ID")),
                    ft.DataColumn(ft.Text("Criado em")),
                    ft.DataColumn(ft.Text("Cliente")),
                    ft.DataColumn(ft.Row([ft.Icon(I.MARK_CHAT_UNREAD, size=16), ft.Text("Chat (não lidas)")], spacing=6)),

                    ft.DataColumn(ft.Text("Sistema")),
                    ft.DataColumn(ft.Text("Prioridade")),
                    ft.DataColumn(ft.Text("Status")),
                    ft.DataColumn(ft.Text("Responsável")),
                    ft.DataColumn(ft.Text("Ações")),
                ],
                rows=[],
                column_spacing=14,
                data_row_min_height=44,
            )

        # --- Helpers: Coluna NL (não lidas) ---
    def _make_nl_badge(self, cnt: int):
            label = ft.Text(str(cnt), size=12, weight=ft.FontWeight.W_600)
            pill = ft.Container(
                content=ft.Row([ft.Icon(I.MARK_CHAT_UNREAD, size=14), label], spacing=4,
                               vertical_alignment=ft.CrossAxisAlignment.CENTER),
                padding=ft.padding.symmetric(horizontal=8, vertical=4),
                border_radius=999,
                bgcolor=op(0.12, C.RED_500),
                tooltip=f"{cnt} não lida(s)",
                visible=cnt > 0,
            )
            hyphen = ft.Text("-", color=C.GREY_600, visible=(cnt <= 0))
            return pill, label, hyphen

    def _build_nl_cell(self, rid: int, table: str) -> ft.Control:
            cnt = int(self._unread.get(rid, 0))
            pill, label, hyphen = self._make_nl_badge(cnt)
            wrapper = ft.Stack([hyphen, pill], height=28, width=60)
            self._unread_cells.setdefault(table, {})
            self._unread_cells[table][rid] = {"pill": pill, "label": label, "hyphen": hyphen}
            return wrapper

    def _apply_nl_value(self, rid: int) -> None:
            cnt = int(self._unread.get(rid, 0))
            for table in ("fila", "minhas"):
                ref = self._unread_cells.get(table, {}).get(rid)
                if not ref:
                    continue
                ref["label"].value = str(cnt)
                ref["pill"].visible = cnt > 0
                ref["pill"].tooltip = f"{cnt} não lida(s)"
                ref["hyphen"].visible = cnt <= 0
                for ctrl in (ref["label"], ref["pill"], ref["hyphen"]):
                    try:
                        ctrl.update()
                    except Exception:
                        pass


    def _load_minhas_rows(self) -> None:
            start_gen = getattr(self, "_tab_gen", 0)
            _in_chat = str(getattr(self.page, "route", "") or "").startswith("/chat/")
            if getattr(self, "current_tab", None) != 3 and not _in_chat:
                return
            # PATCH: União (A ∪ B): abertas por mim + atribuídas a mim; dedupe por id; ordena desc por created_at
            try:
                self.table_minhas.rows.clear()
            except Exception:
                pass

            status = self._normalize_status(getattr(self, "filter_my_status", None).value if getattr(self, "filter_my_status", None) else None)
            try:
                users = {u.id: u.username for u in self.db.list_users()}
            except Exception:
                users = {}

            try:
                a = self.db.list_requests(status=status, only_mine_opened_by=self.user.id)
            except Exception:
                a = []
            try:
                b = self.db.list_requests(status=status, only_mine_taken_by=self.user.id)
            except Exception:
                b = []

            seen, reqs = set(), []
            for r in list(a) + list(b):
                if getattr(r, 'id', None) is not None and r.id not in seen:
                    seen.add(r.id)
                    reqs.append(r)

            try:
                reqs.sort(key=lambda r: getattr(r, 'created_at', None), reverse=True)
            except Exception:
                pass

            if not hasattr(self, '_unread'):
                self._unread = {}
            for r in reqs:
                try:
                    if getattr(r, 'taken_by', None) == self.user.id:
                        other_id = getattr(r, 'created_by', 0)
                    elif getattr(r, 'created_by', None) == self.user.id and getattr(r, 'taken_by', None):
                        other_id = r.taken_by
                    else:
                        other_id = 0
                    self._unread[r.id] = (self.db.get_unread_count(r.id, self.user.id, other_id)
                                          if other_id and other_id != self.user.id else 0)
                except Exception:
                    self._unread[r.id] = 0

            import flet as ft
            Icons = getattr(ft, 'Icons', None)

            for r in reqs:
                try:
                    created_text = getattr(r, 'created_at', None)
                    if created_text and hasattr(created_text, 'astimezone') and 'TZ' in globals():
                        created_text = created_text.astimezone(TZ).strftime('%d/%m/%Y %H:%M:%S')
                    else:
                        created_text = str(getattr(r, 'created_at', ''))

                    cliente = getattr(r, 'cliente', None) or '-'
                    sistema = getattr(r, 'sistema', None) or '-'
                    prioridade = getattr(r, 'prioridade', None)
                    status_val = getattr(r, 'status', None)
                    status_chip = self._status_chip(status_val) if hasattr(self, '_status_chip') else ft.Text(str(status_val))
                    prioridade_chip = self._prioridade_chip(prioridade) if hasattr(self, '_prioridade_chip') else ft.Text(str(prioridade))
                    users_map = users if isinstance(users, dict) else {}
                    resp_txt = users_map.get(getattr(r, 'taken_by', None), '-') if getattr(r, 'taken_by', None) else '-'

                    nl_cell = self._build_nl_cell(r.id, 'minhas') if hasattr(self, '_build_nl_cell') else ft.Text(str(self._unread.get(r.id, 0)))

                    id_btn = ft.TextButton(str(r.id), on_click=lambda _=None, rid=r.id: self._open_detail_dialog(rid)) if hasattr(self, '_open_detail_dialog') else ft.Text(str(r.id))
                    try:
                        vis_icon = Icons.VISIBILITY if Icons and hasattr(Icons, 'VISIBILITY') else 'visibility'
                        chat_icon = Icons.CHAT if Icons and hasattr(Icons, 'CHAT') else 'chat'
                        actions = ft.Row([
                            ft.IconButton(vis_icon, tooltip='Visualizar', on_click=lambda _=None, rid=r.id: self._open_detail_dialog(rid) if hasattr(self, '_open_detail_dialog') else None),
                            ft.IconButton(chat_icon, tooltip='Chat', on_click=lambda _=None, rid=r.id: self._open_chat_dialog(rid) if hasattr(self, '_open_chat_dialog') else None),
                        ], spacing=4)
                    except Exception:
                        actions = ft.Row([])

                    row = ft.DataRow(cells=[
                        ft.DataCell(id_btn),
                        ft.DataCell(ft.Text(created_text)),
                        ft.DataCell(ft.Text(cliente)),
                        ft.DataCell(nl_cell),
                        ft.DataCell(ft.Text(sistema)),
                        ft.DataCell(prioridade_chip),
                        ft.DataCell(status_chip),
                        ft.DataCell(ft.Text(resp_txt)),
                        ft.DataCell(actions),
                    ])
                    self.table_minhas.rows.append(row)
                except Exception:
                    continue

            try:
                self._update_minhas_badge()
            except Exception:
                pass
            try:
                if hasattr(self, 'page') and hasattr(self.page, 'update'):
                    # Guard final contra renderização atrasada
                    if start_gen != getattr(self, "_tab_gen", 0) or (getattr(self, "current_tab", None) != 3 and not _in_chat):
                        return
                    self.page.update()
            except Exception:
                pass
            except Exception:
                pass
    def _export_my_filtered(self, status: str):
            # Normaliza "Todos" -> None
            status_norm = self._normalize_status(status)
            # PATCH: Exporta A ∪ B (abri + peguei) deduplicado e ordenado desc por created_at
            import os, csv
            from datetime import datetime
            try:
                a = self.db.list_requests(status=status, only_mine_opened_by=self.user.id)
            except Exception:
                a = []
            try:
                b = self.db.list_requests(status=status, only_mine_taken_by=self.user.id)
            except Exception:
                b = []
            seen, rows = set(), []
            for r in list(a) + list(b):
                if getattr(r, 'id', None) is not None and r.id not in seen:
                    seen.add(r.id); rows.append(r)
            try:
                rows.sort(key=lambda r: getattr(r, 'created_at', None), reverse=True)
            except Exception:
                pass
            name = 'minhas_aberturas_e_atribuicoes'
            TZ = globals().get('TZ', None)
            label = "todos" if status_norm is None else str(status_norm)
            fn = f"{name}_{label}_{datetime.now(TZ).strftime('%Y%m%d_%H%M%S') if TZ else datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            EXPORT_DIR = globals().get('EXPORT_DIR', os.getcwd())
            path = os.path.join(EXPORT_DIR, fn)
            try:
                users = {u.id: u.username for u in self.db.list_users()}
            except Exception:
                users = {}
            with open(path, 'w', newline='', encoding='utf-8') as f:
                w = csv.writer(f, delimiter=';')
                w.writerow(['ID', 'Criado em', 'Cliente', 'Sistema', 'Prioridade', 'Status', 'Responsável'])
                for r in rows:
                    try:
                        created_text = getattr(r, 'created_at', None)
                        if created_text and hasattr(created_text, 'astimezone') and TZ:
                            created_text = created_text.astimezone(TZ).strftime('%d/%m/%Y %H:%M:%S')
                        else:
                            created_text = str(getattr(r, 'created_at', ''))
                        w.writerow([
                            getattr(r, 'id', ''),
                            created_text,
                            getattr(r, 'cliente', None) or '-',
                            getattr(r, 'sistema', None) or '-',
                            getattr(r, 'prioridade', None),
                            getattr(r, 'status', None),
                            users.get(getattr(r, 'taken_by', None), '-') if getattr(r, 'taken_by', None) else '-',
                        ])
                    except Exception:
                        continue
            if hasattr(self, '_toast'):
                self._toast(f'Exportado: {path}')
            return path
    def _minhas_tab(self) -> ft.Control:
            # # Normaliza Dropdown de Status (Minhas)
            # self._normalize_status_dropdown(self.filter_my_status)
            def on_status_change(_e):
                self._load_minhas_rows()
            self.filter_my_status.on_change = on_status_change

            toolbar = ft.Row(
                [
                    ft.Text("Status:"),
                    self.filter_my_status,
                    ft.OutlinedButton(
                        "Atualizar",
                        icon=I.REFRESH,
                        on_click=lambda _: self._load_minhas_rows(),
                    ),
                    ft.Text("Próximo auto:"),
                    self._minhas_timer_lbl,
                    ft.Container(expand=True),
                    ft.OutlinedButton(
                        "Exportar minhas",
                        icon=I.DOWNLOAD,
                        on_click=lambda _: self._export_my_filtered(self.filter_my_status.value),
                    ),
                ],
                spacing=10,
            )

            self._load_minhas_rows()

            return ft.Column(
                [
                    ft.Card(content=ft.Container(padding=12, content=toolbar)),
                    ft.Card(content=ft.Container(padding=8, content=self.table_minhas)),
                ],
                spacing=12,
            )

    def __init__(self, page: ft.Page, db: DB, user: User, on_logout: Callable[[], None]):
            self.page = page
            self.db = db
            self.user = user
            self._tab_gen = 0  # token de geração para invalidar carregamentos antigos
            self.on_logout = on_logout
            # Fallback: garante atributo existente
            if not hasattr(self, "_chat_view_realtime"):
                self._chat_view_realtime = self._chat_view

            # --- Estado do Chat em tempo real ---
            self._chat_running = False
            self._chat_task = None
            self._chat_current_id = None
            self._chat_last_id = 0
            self._chat_seen_ids = set()
            self._chat_msgs_view = None
            self._chat_users_map = {}
            try:
                self._pubsub = self.page.pubsub
                self._pubsub.subscribe(self._on_pubsub_event)
            except Exception:
                self._pubsub = None
            # --- Estado de não lidas por solicitação (NL) ---
            self._unread: dict[int, int] = {}  # {request_id: contagem de mensagens não lidas}
            self._unread_cells: dict[str, dict[int, dict[str, ft.Control]]] = {
                "fila": {},    # {rid: {"pill":..., "label":..., "hyphen":...}}
                "minhas": {},
            }

            # Filtro exclusivo da aba Minhas (igual ao da Fila)
            self.filter_my_status = ft.Dropdown(

                label="Status",

                options=[

                    ft.dropdown.Option("aberta"),

                    ft.dropdown.Option("em_atendimento"),

                    ft.dropdown.Option("resolvida"),

                    ft.dropdown.Option("cancelada"),

                ],

                value="em_atendimento",

            )

            self._normalize_status_dropdown(self.filter_my_status)


            # Tabela persistente para a aba Minhas
            self.table_minhas = self._build_minhas_table()

            self.page.title = f"Solicitações DBA — {user.username}"
            settings = load_settings()
            theme_mode = settings.get("theme_mode", "dark")
            self.page.theme_mode = ft.ThemeMode.DARK if theme_mode == "dark" else ft.ThemeMode.LIGHT
            self.palette = PALETTE_DARK if theme_mode == "dark" else PALETTE_LIGHT
            self.page.theme = ft.Theme(color_scheme_seed=C.TEAL, use_material3=True, visual_density=ft.VisualDensity.COMPACT)
            self.page.bgcolor = self.palette["bg"]
            self.page.scroll = ft.ScrollMode.AUTO
            self.page.padding = 0
            # limpa qualquer UI anterior (ex.: tela de login) antes de montar o app
            self.page.controls.clear()

            # Singleton de FilePicker para Chat (evita múltiplas instâncias em overlay)
            self._fp_chat = None
            # Estado
            self.current_tab = getattr(self, 'current_tab', 0)  # 0=Home,1=Abrir,2=Fila,3=Minhas,4=Admin
            self.filter_status = ft.Dropdown(

                label="Status",

                options=[

                    ft.dropdown.Option("aberta"),

                    ft.dropdown.Option("em_atendimento"),

                    ft.dropdown.Option("resolvida"),

                    ft.dropdown.Option("cancelada"),

                ],

                value="aberta",

            )

            self._normalize_status_dropdown(self.filter_status)


            # Inputs reutilizados (Abertura)
            self.f_title = ft.TextField(label="Assunto/Título *", expand=True)
            self.f_cliente = ft.TextField(label="Cliente *", expand=True)
            self.f_sistema = ft.TextField(label="Sistema/Módulo", expand=True)
            self.f_prioridade = ft.Dropdown(label="Prioridade", options=[
                ft.dropdown.Option("Baixa"), ft.dropdown.Option("Média"), ft.dropdown.Option("Alta"), ft.dropdown.Option("Crítica"),
            ], value="Média")
            self.f_loja_parada = ft.Checkbox(label="Loja Parada?")
            self.f_desc = ft.TextField(label="Descrição", multiline=True, min_lines=3, max_lines=6, expand=True)

            # Tabelas
            self.table_fila = self._build_fila_table()

            # Abas
            self.tabs = ft.Tabs(
                selected_index=self.current_tab,
                scrollable=False,
                tabs=[
                    ft.Tab(text="Home", icon=I.HOME),
                    ft.Tab(text="Abrir", icon=I.SUPPORT_AGENT),
                    ft.Tab(text="Fila", icon=I.VIEW_LIST),
                    ft.Tab(text="Minhas", icon=I.TASK),
                    ft.Tab(text="Admin", icon=I.ADMIN_PANEL_SETTINGS, visible=self.user.is_admin),
                ],
                on_change=self._on_tab_change,
                indicator_color=C.TEAL,
            )

            # Estado do contador Fila e timer
            self._fila_badge_count = 0
            self._fila_timer_on = False
            self._fila_next_refresh_s = 30
            self._fila_timer_lbl = ft.Text('', size=12, opacity=0.85)
            self._fila_suppress_next_reload = False  # suprimir PubSub de volta
            try:
                self._fila_tab_ref = self.tabs.tabs[2]
            except Exception:
                self._fila_tab_ref = None
            try:
                self._update_fila_badge()
            except Exception:
                pass

            # Estado do contador Minhas e timer
            self._minhas_badge_count = 0
            self._minhas_timer_on = False
            self._minhas_next_refresh_s = 30
            self._minhas_timer_lbl = ft.Text('', size=12, opacity=0.85)
            self._minhas_suppress_next_reload = False  # reservado para ações locais da aba Minhas
            try:
                self._minhas_tab_ref = self.tabs.tabs[3]  # 0=Home,1=Abrir,2=Fila,3=Minhas,4=Admin
            except Exception:
                self._minhas_tab_ref = None
            try:
                self._update_minhas_badge()
            except Exception:
                pass
            self._apply_tab_permissions()

            # Conteúdo dinâmico abaixo das abas
            self.content_area = ft.Container(padding=16, expand=True)

            # Top bar com ações rápidas (logout)
            self._theme_switch = ft.Switch(label="Tema escuro", value=(theme_mode == "dark"), on_change=lambda e: self._on_toggle_theme(e.control.value))
            self.top_actions = ft.Row([
                ft.Container(expand=True),
                self._theme_switch,
                ft.IconButton(I.SWITCH_ACCOUNT, tooltip="Trocar usuário", on_click=self._logout),
            ], alignment=ft.MainAxisAlignment.END)

            # Inicializa
        
            self._render_tab_content()
            # Inicializa overlay (usado pelo Detalhe e preview de imagens)
            self._init_overlay()
            # Routing: View stack com navegação roteada
            self._init_routing()
            self._build_main_view()
            # Garante rota inicial
            self.page.go(self.page.route or "/")

            # ---------------- Routing (View stack) ----------------
        # --- Chips coloridos (dark/light aware) ---
        # --- Chips coloridos (compat: Flet sem ChipStyle) ---
    def _chip(self, text: str, icon: str, bg) -> ft.Control:
            fg = C.BLACK
            return ft.Container(
                bgcolor=bg,
                padding=ft.padding.symmetric(horizontal=10, vertical=6),
                border_radius=999,
                content=ft.Row(
                    [
                        ft.Icon(icon, size=14, color=fg),
                        ft.Text(text, color=fg, size=11, weight=ft.FontWeight.W_600),
                    ],
                    spacing=6,
                    vertical_alignment=ft.CrossAxisAlignment.CENTER,
                ),
            )

    def _neutral_chip_bg(self):
            # Vivid fallback (sem neutro/branco)
            return C.INDIGO_400

    def _status_chip(self, status: str) -> ft.Control:
            s = (status or "").lower()
            m = {
                "aberta": C.TEAL_400,
                "em_atendimento": C.AMBER_400,
                "resolvida": C.GREEN_400,
                "cancelada": C.RED_400,
            }
            return self._chip(status, I.LABEL, m.get(s, self._neutral_chip_bg()))

    def _prioridade_chip(self, prioridade: str) -> ft.Control:
            p = (prioridade or "").lower()
            if p in ("crítica", "critica"):
                bg = C.RED_500
            elif p == "alta":
                bg = C.ORANGE_400
            elif p in ("média", "media"):
                bg = C.BLUE_400
            else:
                bg = C.PURPLE_400  # baixa ou desconhecido → vivo
            return self._chip(prioridade, I.PRIORITY_HIGH, bg)

    def _loja_chip(self, parada: bool) -> ft.Control:
            return self._chip(
                "Loja parada" if parada else "Loja OK",
                I.FACTORY,
                C.RED_500 if parada else C.GREEN_400,
            )

    def _init_routing(self) -> None:
            self.page.on_route_change = self._on_route_change
            self.page.on_view_pop = self._on_view_pop
    
    def _build_main_view(self) -> None:
            # Sempre reconstruir a view base "/"
            main_column = ft.Column(
                [self.tabs, ft.Divider(height=1), self.top_actions, self.content_area],
                spacing=0, expand=True
            )
            main_view = ft.View(
                route="/",
                controls=[main_column],
                padding=0,
                bgcolor=self.palette["bg"],
            )
            self.page.views.clear()
            self.page.views.append(main_view)
            self.page.update()
    
    def _on_view_pop(self, e: ft.ViewPopEvent) -> None:
            try:
                self.page.views.pop()
            except Exception:
                pass
            self.page.go(self.page.views[-1].route if self.page.views else "/")
    def _on_route_change(self, e: ft.RouteChangeEvent) -> None:
            route = (self.page.route or "/").strip()
            # Evita reconstruções repetidas na mesma rota (pode ocorrer em alguns eventos de janela)
            if getattr(self, "_last_route", None) == route:
                return
            self._last_route = route
            # Recria a base
            self._build_main_view()
            # Rota de chat
            if route.startswith("/chat/"):
                try:
                    rid = int(route.split("/chat/", 1)[1].split("/")[0])
                except Exception:
                    self._toast("Rota de chat inválida.")
                    self.page.update()
                    return
                # sempre parar watcher anterior antes de montar o novo
                self._stop_chat_live()
                chat_view = getattr(self, "_chat_view_realtime", self._chat_view)(rid)
                if chat_view:
                    self.page.views.append(chat_view)
                    # inicia watcher (polling) para este chat
                    self._start_chat_live(rid)
            self.page.update()    
    def _chat_view(self, rid: int) -> Optional[ft.View]:

            r = self.db.get_request(rid)
            if not r:
                self._toast("Solicitação não encontrada.")
                return None

            users = {u.id: u.username for u in self.db.list_users()}
            solicitante = users.get(r.created_by, "-")
            responsavel = users.get(r.taken_by, "-") if r.taken_by else "-"

            def can_chat(u: User, req: Request) -> bool:
                return (u.is_admin or u.id == req.created_by or (req.taken_by and u.id == req.taken_by))

            # AppBar
            appbar = ft.AppBar(
                leading=ft.IconButton(I.ARROW_BACK, on_click=lambda _: self.page.go("/")),
                title=ft.Text(f"Chat — Solicitação #{r.id}"),
                bgcolor=self.palette["card"],
                center_title=False,
            )

            # Chips de status
            chips = ft.Row(controls=[
                ft.Chip(label=ft.Text(f"{r.id}"), leading=ft.Icon(I.TAG)),
                self._status_chip(r.status),
                self._prioridade_chip(r.prioridade),
                self._loja_chip(r.loja_parada),
                ft.Container(expand=True),
                ft.Text(f"Solicitante: {solicitante}", size=12, color=C.GREY_500),
                ft.Text(" • ", size=12, color=C.GREY_600),
                ft.Text(f"Responsável: {responsavel}", size=12, color=C.GREY_500),
            ], spacing=6)

            # Histórico
            msgs_view = ft.ListView(expand=True, spacing=6, auto_scroll=True, padding=10)

            self._chat_msgs_view = msgs_view
            self._chat_users_map = users
        # Helpers para abrir/baixar anexos usando métodos já existentes na classe
            def _open_file(att_id: int, fname: str, mime: str):
                att = self.db.get_file(att_id)
                if not att:
                    self._toast("Arquivo não encontrado")
                    return
                if str(mime).startswith("image/") and att.blob:
                    self._open_image_preview(fname, att.blob)
                else:
                    self._save_and_open_file(fname, att.blob)

            def _save_file(att_id: int, fname: str):
                att = self.db.get_file(att_id)
                if not att:
                    self._toast("Arquivo não encontrado")
                    return
                self._save_and_open_file(fname, att.blob)

            # Carrega histórico com suporte a miniaturas por MENSAGEM via tag [anexo:<id>] filename
            def load_msgs() -> None:
                msgs_view.controls.clear()
                msgs = self.db.list_messages(r.id)

                # largura útil da bolha
                try:
                    win_w = int(self.page.window_width or 1080)
                except Exception:
                    win_w = 1080
                dlg_w_local = max(700, min(980, win_w - 80))
                max_bubble_w = int(dlg_w_local * 0.75)

                for m in msgs:
                    is_me = (m.sender_id == self.user.id)
                    who = users.get(m.sender_id, "-")
                    when = m.created_at.astimezone(TZ).strftime("%d/%m/%Y %H:%M")
                    bubble_bg = self.palette["primary"] if is_me else self.palette["card"]
                    text_color = self.palette.get("primary_text", C.WHITE) if is_me else self.palette["text"]
                    align = ft.MainAxisAlignment.END if is_me else ft.MainAxisAlignment.START
                    br = ft.border_radius.BorderRadius(16, 16, 4 if is_me else 16, 16 if is_me else 4)

                    msg_text = (getattr(m, "message", "") or "").strip()

                    # 1) Se a mensagem for uma TAG de anexo "[anexo:<id>] filename", renderizamos MINIATURA
                    mtag = None
                    if msg_text.startswith("[anexo:") and "]" in msg_text:
                        try:
                            tag, rest = msg_text.split("]", 1)
                            att_id = int(tag.split(":", 1)[1].strip())
                            fname = rest.strip() or f"arquivo_{att_id}"
                            att = self.db.get_file(att_id)
                            if att is not None:
                                # thumb
                                if str(att.mime).startswith("image/") and att.blob:
                                    b64 = base64.b64encode(att.blob).decode("ascii")
                                    thumb = ft.Image(src_base64=b64, width=200, height=140, fit=ft.ImageFit.COVER, border_radius=10)
                                else:
                                    thumb = ft.Icon(I.INSERT_DRIVE_FILE, size=48)
                                # bloco de anexo
                            
                                tile = ft.Container(
                                    width=max_bubble_w,
                                    bgcolor=bubble_bg,
                                    padding=10,
                                    border_radius=br,
                                    content=ft.Column(
                                        [
                                            ft.Row(
                                                [ft.Text("Anexo", size=12, color=op(0.8, text_color))],
                                                alignment=ft.MainAxisAlignment.START,
                                            ),
                                            ft.Container(content=thumb, alignment=ft.alignment.center),
                                            ft.Text(
                                                fname,
                                                size=12,
                                                color=text_color,
                                                max_lines=2,
                                                overflow=ft.TextOverflow.ELLIPSIS,
                                            ),
                                            ft.Row(
                                                [
                                                    ft.TextButton(
                                                        "Abrir",
                                                        icon=I.OPEN_IN_NEW,
                                                        on_click=lambda _=None, _id=att_id, _fn=fname, _mm=att.mime: _open_file(_id, _fn, _mm),
                                                    ),
                                                    ft.TextButton(
                                                        "Baixar",
                                                        icon=I.DOWNLOAD,
                                                        on_click=lambda _=None, _id=att_id, _fn=fname: _save_file(_id, _fn),
                                                    ),
                                                ],
                                                alignment=ft.MainAxisAlignment.CENTER,
                                                spacing=8,
                                            ),
                                        ],
                                        spacing=6,
                                    ),
                                )
                                msgs_view.controls.append(ft.Row([tile], alignment=align))
                                # meta linha
                                meta = ft.Text(f"{'Você' if is_me else who} • {when}", size=10, color=op(0.7, text_color))
                                msgs_view.controls.append(ft.Row([ft.Container(width=max_bubble_w, content=meta)], alignment=align))
                                mtag = True
                        except Exception:
                            mtag = None

                    # 2) Se houver texto normal (ou falha na tag), renderiza bolha de texto
                    if not mtag and msg_text:
                        body_txt = ft.Text(msg_text, color=text_color, selectable=True)
                        meta_row = ft.Row([ft.Text(f"{'Você' if is_me else who} • {when}", size=11, color=op(0.75, text_color))], alignment=ft.MainAxisAlignment.END)
                        bubble = ft.Container(
                            bgcolor=bubble_bg, padding=10, border_radius=br, width=max_bubble_w,
                            content=ft.Column([body_txt, meta_row], spacing=6),
                        )
                        msgs_view.controls.append(ft.Row([bubble], alignment=align))

                if getattr(msgs_view, "page", None) is not None:
                    msgs_view.update()

                self._chat_seen_ids = {m.id for m in msgs}
                self._chat_last_id = max([m.id for m in msgs], default=0)
            # Input + enviar
            inp_msg = ft.TextField(
                label="Mensagem",
                hint_text="Digite e pressione Enter para enviar",
                expand=True,
                multiline=False,
                autofocus=True,
                on_submit=lambda _: send_msg(),
            )
            def send_msg() -> None:
                if not can_chat(self.user, r):
                    self._toast("Você não tem permissão para enviar mensagens neste chat.")
                    return
                txt = (inp_msg.value or "").strip()
                if not txt:
                    return
                self.db.add_message(r.id, self.user.id, txt)
                inp_msg.value = ""
                # -- garantir foco de volta no campo de mensagem --
                try:
                    self.page.set_focus(inp_msg)
                except Exception:
                    try:
                        inp_msg.focus()
                    except Exception:
                        try:
                            self.page.focus(inp_msg)
                        except Exception:
                            pass
                # --------------------------------------------------
                inp_msg.update()
                load_msgs()
                # NL: notificar outras sessões/abas (texto)
                try:
                    if getattr(self, "_pubsub", None):
                        row = self.db.get_message_dict(self.db.last_message_id(r.id))
                        self._pubsub.send_all({"type": "chat_new", "chat_id": r.id, "message": row})
                except Exception:
                    pass

            send_btn = ft.FilledButton("Enviar", icon=I.SEND, on_click=lambda _: send_msg())

            # FilePicker: agora envia o ARQUIVO e cria automaticamente uma mensagem de TAG [anexo:<id>] filename
            if getattr(self, '_fp_chat', None) is None:
                self._fp_chat = ft.FilePicker()
                try:
                    self.page.overlay.append(self._fp_chat)
                except Exception:
                    pass
            fp = self._fp_chat
            def on_pick_result(e: ft.FilePickerResultEvent):
                if not e.files:
                    return
                total = 0
                for f in e.files:
                    try:
                        with open(f.path, "rb") as fh:
                            data = fh.read()
                    except Exception:
                        self._toast(f"Não foi possível ler: {f.name}")
                        continue
                    total += len(data)
                    if total > 20 * 1024 * 1024:
                        self._toast("Limite total de 20MB por envio excedido.")
                        break
                    mime = ftype_from_name(f.name)
                    file_id = self.db.add_file(r.id, f.name, mime, data, self.user.id)
                    mid = self.db.add_message(r.id, self.user.id, f"[anexo:{file_id}] {f.name}")
                    row = self.db.get_message_dict(mid)
                    try:
                        if getattr(self, "_pubsub", None):
                            self._pubsub.send_all({"type": "chat_new", "chat_id": r.id, "message": row})
                    except Exception:
                        pass
                    self._append_msg_row(row)
                    try:
                        self._chat_last_id = max(self._chat_last_id, int(row.get("id", 0)))
                        self._chat_seen_ids.add(int(row.get("id", 0)))
                    except Exception:
                        pass
                self.page.update()
            fp.on_result = on_pick_result
            attach_btn = ft.OutlinedButton("Anexar", icon=I.ATTACH_FILE, on_click=lambda _: fp.pick_files(allow_multiple=True))

            can_write = can_chat(self.user, r)
            input_row = ft.Row(
                [attach_btn, inp_msg, send_btn],
                alignment=ft.MainAxisAlignment.START,
                vertical_alignment=ft.CrossAxisAlignment.CENTER,
            )
            if not can_write:
                inp_msg.disabled = True
                send_btn.disabled = True
                input_row.controls.insert(0, ft.Icon(I.LOCK))
                input_row.controls.insert(1, ft.Text("Somente leitura", color=C.GREY_500))

            body = ft.Column(
                controls=[
                    ft.Container(padding=10, content=chips),
                    ft.Divider(height=1),
                    ft.Container(expand=True, content=msgs_view),
                    ft.Divider(height=1),
                    ft.Container(padding=10, content=input_row),
                ],
                expand=True,
                spacing=0,
            )
            view = ft.View(
                route=f"/chat/{rid}",
                appbar=appbar,
                controls=[body],
                padding=0,
                bgcolor=self.palette["bg"],
            )
            load_msgs()

            try:
                # Ao abrir o chat, zera contador NL e atualiza as tabelas
                self._unread[rid] = 0
                self._apply_nl_value(rid)
            except Exception:
                pass

            # NL: persistir leitura ao abrir (marca último visto = último id do chat)
            try:
                last_id = self.db.last_message_id(rid)
                self.db.set_last_seen(rid, self.user.id, last_id)
            except Exception:
                pass

            return view

        # ---------------- Chat Live: PubSub + Polling (canonical) ----------------
    def _append_msg_row(self, row: Dict[str, Any]) -> None:
            if not self._chat_msgs_view or not row:
                return
            try:
                msg_id = int(row.get("id", 0))
                if msg_id in self._chat_seen_ids:
                    return
                if self._chat_current_id and int(row.get("request_id", 0)) != self._chat_current_id:
                    return
                try:
                    win_w = int(self.page.window_width or 1080)
                except Exception:
                    win_w = 1080
                dlg_w_local = max(700, min(980, win_w - 80))
                max_bubble_w = int(dlg_w_local * 0.75)

                sender_id = int(row.get("sender_id", 0))
                who = self._chat_users_map.get(sender_id, "-")
                is_me = (sender_id == self.user.id)
                bubble_bg = self.palette["primary"] if is_me else self.palette["card"]
                text_color = self.palette.get("primary_text", C.WHITE) if is_me else self.palette["text"]
                align = ft.MainAxisAlignment.END if is_me else ft.MainAxisAlignment.START
                br = ft.border_radius.BorderRadius(16, 16, 4 if is_me else 16, 16 if is_me else 4)
                msg_text = (row.get("message") or "").strip()
                when_dt = datetime.fromisoformat(row.get("created_at")) if row.get("created_at") else datetime.now(TZ)
                when = when_dt.astimezone(TZ).strftime("%d/%m/%Y %H:%M")

                mtag = None
                if msg_text.startswith("[anexo:") and "]" in msg_text:
                    try:
                        tag, rest = msg_text.split("]", 1)
                        att_id = int(tag.split(":", 1)[1].strip())
                        fname = rest.strip() or f"arquivo_{att_id}"
                        att = self.db.get_file(att_id)
                        if att is not None:
                            # thumbnail or icon
                            if str(att.mime).startswith("image/") and att.blob:
                                b64 = base64.b64encode(att.blob).decode("ascii")
                                thumb = ft.Image(src_base64=b64, width=200, height=140, fit=ft.ImageFit.COVER, border_radius=10)
                            else:
                                thumb = ft.Icon(I.INSERT_DRIVE_FILE, size=48)
                            tile = ft.Container(
                                width=max_bubble_w,
                                bgcolor=bubble_bg,
                                padding=10,
                                border_radius=br,
                                content=ft.Column(
                                    [
                                        ft.Row([ft.Text("Anexo", size=12, color=op(0.8, text_color))],
                                               alignment=ft.MainAxisAlignment.START),
                                        ft.Container(content=thumb, alignment=ft.alignment.center),
                                        ft.Text(fname, size=12, color=text_color, max_lines=2, overflow=ft.TextOverflow.ELLIPSIS),
                                        ft.Row(
                                            [
                                                ft.TextButton("Abrir", icon=I.OPEN_IN_NEW,
                                                              on_click=lambda _=None, _id=att_id, _fn=fname, _mm=att.mime: self._open_file_inline(_id, _fn, _mm)),
                                                ft.TextButton("Baixar", icon=I.DOWNLOAD,
                                                              on_click=lambda _=None, _id=att_id, _fn=fname: self._save_file_inline(_id, _fn)),
                                            ],
                                            alignment=ft.MainAxisAlignment.CENTER,
                                            spacing=8,
                                        ),
                                    ],
                                    spacing=6,
                                ),
                            )
                            self._chat_msgs_view.controls.append(ft.Row([tile], alignment=align))
                            meta = ft.Text(f"{'Você' if is_me else who} • {when}", size=10, color=op(0.7, text_color))
                            self._chat_msgs_view.controls.append(ft.Row([ft.Container(width=max_bubble_w, content=meta)], alignment=align))
                            mtag = True
                    except Exception:
                        mtag = None

                if not mtag and msg_text:
                    body_txt = ft.Text(msg_text, color=text_color, selectable=True)
                    meta_row = ft.Row([ft.Text(f"{'Você' if is_me else who} • {when}", size=11, color=op(0.75, text_color))],
                                      alignment=ft.MainAxisAlignment.END)
                    bubble = ft.Container(bgcolor=bubble_bg, padding=10, border_radius=br, width=max_bubble_w,
                                          content=ft.Column([body_txt, meta_row], spacing=6))
                    self._chat_msgs_view.controls.append(ft.Row([bubble], alignment=align))

                if getattr(self._chat_msgs_view, "page", None) is not None:
                    self._chat_msgs_view.update()
                self._chat_seen_ids.add(msg_id)
            except Exception:
                pass

    def _open_file_inline(self, att_id: int, fname: str, mime: str):
            att = self.db.get_file(att_id)
            if not att:
                self._toast("Arquivo não encontrado")
                return
            if str(mime).startswith("image/") and att.blob:
                self._open_image_preview(fname, att.blob)
            else:
                self._save_and_open_file(fname, att.blob)

    def _save_file_inline(self, att_id: int, fname: str):
            att = self.db.get_file(att_id)
            if not att:
                self._toast("Arquivo não encontrado")
                return
            self._save_and_open_file(fname, att.blob)

    def _start_chat_live(self, request_id: int) -> None:
            self._chat_current_id = request_id
            try:
                self._chat_last_id = self.db.last_message_id(request_id) or self._chat_last_id
            except Exception:
                pass
            self._chat_running = True
            try:
                if self._chat_task and not getattr(self._chat_task, "done", lambda: True)():
                    self._chat_task.cancel()
            except Exception:
                pass
            try:
                self._chat_task = self.page.run_task(self._poll_new_messages)
            except Exception:
                self._chat_task = None

    def _stop_chat_live(self) -> None:
            self._chat_running = False
            try:
                if self._chat_task and not getattr(self._chat_task, "done", lambda: True)():
                    self._chat_task.cancel()
            except Exception:
                pass
            self._chat_task = None
            self._chat_current_id = None
            self._chat_msgs_view = None
            self._chat_seen_ids.clear()

    async def _poll_new_messages(self):
            import asyncio
            while self._chat_running and self._chat_current_id:
                try:
                    rid = self._chat_current_id
                    if not rid:
                        break
                    new_msgs = self.db.list_messages_since(rid, self._chat_last_id, limit=500)
                    for m in new_msgs:
                        row = {
                            "id": m.id,
                            "request_id": m.request_id,
                            "sender_id": m.sender_id,
                            "message": m.message,
                            "created_at": m.created_at.isoformat() if isinstance(m.created_at, datetime) else str(m.created_at),
                        }
                        if m.id not in self._chat_seen_ids:
                            self._append_msg_row(row)
                            self._chat_last_id = max(self._chat_last_id, m.id)
                            self._chat_seen_ids.add(m.id)
                            # _NL_POLL_PUBSUB_: notificar PubSub se vier de outro usuário
                            try:
                                if getattr(self, "_pubsub", None) and int(row.get("sender_id", 0)) != self.user.id:
                                    self._pubsub.send_all({"type": "chat_new", "chat_id": rid, "message": row})
                            except Exception:
                                pass
                except Exception:
                    pass
                await asyncio.sleep(CHAT_POLL_INTERVAL_S)

    def _on_pubsub_event(self, payload: Any):
            # ---- PubSub: fila/solicitações ----
            try:
                if isinstance(payload, dict) and payload.get("type") in (
                    "queue_changed", "request_changed", "status_changed",
                    "take", "release", "resolve", "cancel", "fila_changed"
                ):
                    try:
                        self._update_fila_badge()
                        try:
                            self._update_minhas_badge()
                        except Exception:
                            pass
                    except Exception:
                        pass
                    if getattr(self, "_fila_suppress_next_reload", False):
                        self._fila_suppress_next_reload = False
                        return
                    if getattr(self, "current_tab", None) == 2:
                        try:
                            self._reload_open_list()
                        except Exception:
                            pass
                    elif getattr(self, "current_tab", None) == 3:
                        try:
                            self._load_minhas_rows()
                        except Exception:
                            pass
                    return
            except Exception:
                pass
            try:
                if not isinstance(payload, dict):
                    return
            
                evt_type = payload.get("type")
                if evt_type not in ("chat_new", "chat", "message_new", "chat_message"):
                    return
                rid_payload_raw = (
                    payload.get("chat_id")
                    or payload.get("request_id")
                    or payload.get("rid")
                    or payload.get("id_solicitacao")
                )
                if not rid_payload_raw and isinstance(payload.get("message"), dict):
                    rid_payload_raw = (
                        payload["message"].get("chat_id")
                        or payload["message"].get("request_id")
                    )
                rid_payload = int(rid_payload_raw or 0)
                row = payload.get("message") or {}
                msg_id = int(row.get("id", 0) or 0)
                sender_id_raw = (
                    row.get("sender_id")
                    or row.get("from_user_id")
                    or row.get("from_id")
                    or payload.get("sender_id")
                )
                try:
                    sender_id = int(sender_id_raw or 0)
                except Exception:
                    sender_id = 0
                # Se o chat aberto é o mesmo, renderiza mensagem normalmente
                if self._chat_current_id is not None and rid_payload == self._chat_current_id:
                    if msg_id and msg_id <= self._chat_last_id:
                        return
                    if msg_id in self._chat_seen_ids:
                        return
                    self._append_msg_row(row)
                    self._chat_last_id = max(self._chat_last_id, msg_id)
                    self._chat_seen_ids.add(msg_id)
                    return
                # Caso contrário, atualizar contador NL das tabelas (se não for minha mensagem)
                try:
                    if rid_payload and (sender_id == 0 or sender_id != self.user.id):
                        self._unread[rid_payload] = self._unread.get(rid_payload, 0) + 1
                        self._apply_nl_value(rid_payload)
                except Exception:
                    pass
                return

            except Exception:
                pass
        # ---------------- Fila: badge + timer + reload helpers ----------------
    def _update_fila_badge(self) -> None:
            try:
                cnt = len(self.db.list_requests(status="aberta"))
            except Exception:
                cnt = 0
            self._fila_badge_count = int(cnt)
            try:
                if self._fila_tab_ref is not None:
                    self._fila_tab_ref.text = "Fila" if cnt <= 0 else f"Fila ({cnt})"
                if hasattr(self.page, "update"):
                    self.page.update()
            except Exception:
                pass

    async def _fila_timer_loop(self):
            import asyncio
            self._fila_next_refresh_s = 30
            while self._fila_timer_on:
                try:
                    try:
                        self._fila_timer_lbl.value = f"Próximo auto: {self._fila_next_refresh_s}s"
                        self._fila_timer_lbl.update()
                    except Exception:
                        pass
                    if self._fila_next_refresh_s <= 0:
                        try:
                            self._load_fila_rows()
                        except Exception:
                            pass
                        self._fila_next_refresh_s = 30
                    await asyncio.sleep(1)
                    self._fila_next_refresh_s -= 1
                except Exception:
                    break

    def _start_fila_timer(self):
            if not self._fila_timer_on:
                self._fila_timer_on = True
                try:
                    self.page.run_task(self._fila_timer_loop)
                except Exception:
                    self._fila_timer_on = False

    def _stop_fila_timer(self):
            self._fila_timer_on = False
            try:
                self._fila_timer_lbl.value = ""
                self._fila_timer_lbl.update()
            except Exception:
                pass

    def _reload_open_list(self):
            try:
                if getattr(self, "filter_status", None) is not None:
                    if str(getattr(self.filter_status, "value", "")).lower() != "aberta":
                        self.filter_status.value = "aberta"
                        if hasattr(self.filter_status, "update"):
                            self.filter_status.update()
            except Exception:
                pass
            try:
                self._load_fila_rows()
            except Exception:
                pass

    
        # ---------------- Minhas: badge + timer + reload helpers ----------------
    def _update_minhas_badge(self) -> None:
            # PATCH: Badge = |A ∪ B| (status='aberta')
            try:
                a = self.db.list_requests(status='aberta', only_mine_opened_by=self.user.id)
            except Exception:
                a = []
            try:
                b = self.db.list_requests(status='aberta', only_mine_taken_by=self.user.id)
            except Exception:
                b = []
            try:
                cnt = len({r.id for r in list(a) + list(b) if getattr(r, 'id', None) is not None})
            except Exception:
                cnt = 0
            self._minhas_badge_count = int(cnt)
            try:
                if getattr(self, '_minhas_tab_ref', None) is not None:
                    self._minhas_tab_ref.text = 'Minhas' if cnt <= 0 else f'Minhas ({cnt})'
            except Exception:
                pass
            try:
                if hasattr(self, 'page') and hasattr(self.page, 'update'):
                    self.page.update()
            except Exception:
                pass
    def _start_minhas_timer(self):
            if not self._minhas_timer_on:
                self._minhas_timer_on = True
                try:
                    self.page.run_task(self._minhas_timer_loop)
                except Exception:
                    self._minhas_timer_on = False

    async def _minhas_timer_loop(self):
            import asyncio
            self._minhas_next_refresh_s = 30
            while self._minhas_timer_on:
                try:
                    try:
                        self._minhas_timer_lbl.value = f"Próximo auto: {self._minhas_next_refresh_s}s"
                        self._minhas_timer_lbl.update()
                    except Exception:
                        pass
                    if self._minhas_next_refresh_s <= 0:
                        try:
                            self._load_minhas_rows()
                        except Exception:
                            pass
                        self._minhas_next_refresh_s = 30
                    await asyncio.sleep(1)
                    self._minhas_next_refresh_s -= 1
                except Exception:
                    break


    def _stop_minhas_timer(self):
            self._minhas_timer_on = False
            try:
                self._minhas_timer_lbl.value = ""
                self._minhas_timer_lbl.update()
            except Exception:
                pass

    # ---------------- Helpers UI ----------------
    def _on_toggle_theme(self, dark_on: bool):
            s = load_settings()
            s['theme_mode'] = 'dark' if dark_on else 'light'
            save_settings(s)
            self._apply_theme(dark_on)

    def _apply_theme(self, dark_on: bool):
            # Atualiza theme_mode, palette e cores chave
            self.page.theme_mode = ft.ThemeMode.DARK if dark_on else ft.ThemeMode.LIGHT
            self.palette = PALETTE_DARK if dark_on else PALETTE_LIGHT
            # Recria o Theme para garantir consistência em M3
            self.page.theme = ft.Theme(color_scheme_seed=C.TEAL, use_material3=True, visual_density=ft.VisualDensity.COMPACT)
            # Atualiza bg geral
            self.page.bgcolor = self.palette.get('bg', None)
            # Atualiza áreas conhecidas
            try:
                if getattr(self, 'content_area', None) is not None:
                    self.content_area.bgcolor = self.palette.get('bg', None)
            except Exception:
                pass
            # Atualiza overlay
            try:
                if getattr(self, '_overlay_bg', None) is not None:
                    self._overlay_bg.bgcolor = op(0.55, C.BLACK)
                if getattr(self, '_overlay_card', None) is not None:
                    self._overlay_card.bgcolor = self.palette.get('bg', None)
            except Exception:
                pass
            # Reconstroi view raiz para aplicar bgcolor e heranças
            try:
                self._build_main_view()
            except Exception:
                pass
            # Rerenderiza o conteúdo atual da aba
            try:
                self._render_tab_content()
            except Exception:
                pass
            self.page.update()

    def _init_overlay(self):
            # fundo escuro clicável para fechar
            self._overlay_bg = ft.Container(
                visible=False,
                expand=True,
                bgcolor=op(0.55, C.BLACK),
                on_click=lambda _: self._close_overlay(),
            )
            # área central onde entra o card/flutuante
            self._overlay_card = ft.Container(
                visible=False,
                alignment=ft.alignment.center,
                bgcolor=self.palette["bg"],
                border_radius=16,
                padding=8,
                content=None,
            )
            if getattr(self.page, "overlay", None) is None:
                self.page.overlay = []  # type: ignore
            self.page.overlay.append(self._overlay_bg)
            self.page.overlay.append(self._overlay_card)

    def _open_overlay(self, content: ft.Control) -> None:
            self._overlay_card.content = content
            self._overlay_bg.visible = True
            self._overlay_card.visible = True
            self.page.update()

    def _close_overlay(self) -> None:
            self._overlay_bg.visible = False
            self._overlay_card.visible = False
            self._overlay_card.content = None
            self.page.update()

    def _toast(self, msg: str):
            self.page.snack_bar = ft.SnackBar(ft.Text(msg))
            self.page.snack_bar.open = True
            self.page.update()

    def _username(self, uid: Optional[int]) -> str:
            if not uid:
                return "-"
            u = next((u for u in self.db.list_users() if u.id == uid), None)
            return u.username if u else "-"

    def _user_is(self, group_name: str) -> bool:
            return self.db.user_in_group(self.user.id, group_name)
        # --- Helpers de permissão e feedback ---
    def _can_open(self) -> bool:
            # Pode ver/usar a aba "Abrir"
            return self.user.is_admin or self._user_is("SUPORTE")

    def _can_queue(self) -> bool:
            # Pode ver/usar a aba "Fila"
            return self.user.is_admin or self._user_is("DBA")

    def _access_denied(self, action_desc: str = "acessar este conteúdo") -> ft.Control:
            return ft.Card(
                content=ft.Container(
                    padding=16,
                    content=ft.Column(
                        [
                            ft.Row([ft.Icon(I.LOCK), ft.Text("Acesso negado", size=18, weight=ft.FontWeight.BOLD)]),
                            ft.Text(f"Você não tem permissão para {action_desc}."),
                            ft.Row(
                                [ft.FilledButton("Voltar ao início", icon=I.HOME, on_click=lambda _: self._goto_tab(0))],
                                alignment=ft.MainAxisAlignment.END,
                            ),
                        ],
                        spacing=10,
                    ),
                )
            )

    def _apply_tab_permissions(self) -> None:
            """
            Mantém TODAS as abas visíveis.
            O bloqueio de acesso é feito via guards no _render_tab_content()
            e early-returns dentro de cada aba.
            """
            try:
                # Índices: 0=Home, 1=Abrir, 2=Fila, 3=Minhas, 4=Admin
                if hasattr(self, "tabs") and self.tabs and getattr(self.tabs, "tabs", None):
                    if len(self.tabs.tabs) >= 3:
                        self.tabs.tabs[1].visible = True   # Abrir sempre visível
                        self.tabs.tabs[2].visible = True   # Fila sempre visível
                    # Admin já é controlada no momento da criação (visible=self.user.is_admin)
                    self.tabs.update()
            except Exception:
                pass

    def _save_and_open_file(self, filename: str, data: bytes) -> None:
            out = os.path.join(EXPORT_DIR, filename)
            try:
                with open(out, "wb") as w:
                    w.write(data)
                self._toast(f"Salvo em: {out}")
                try:
                    if hasattr(os, "startfile"):
                        os.startfile(out)  # Windows
                    elif platform.system() == "Darwin":
                        subprocess.Popen(["open", out])  # macOS
                    else:
                        subprocess.Popen(["xdg-open", out])  # Linux
                except Exception:
                    webbrowser.open(f"file://{out}")
            except Exception as ex:
                self._toast(f"Erro ao salvar/abrir: {ex}")

    def _open_image_preview(self, title: str, blob: bytes) -> None:
            b64 = base64.b64encode(blob).decode("ascii")
            image = ft.Image(src_base64=b64, width=980, height=680, fit=ft.ImageFit.CONTAIN)
            content = ft.Container(
                width=1000,
                border_radius=18,
                bgcolor=self.palette["bg"],
                clip_behavior=ft.ClipBehavior.NONE,
                padding=12,
                content=ft.Column([
                    ft.Row([
                        ft.Text(title, size=16, weight=ft.FontWeight.BOLD),
                        ft.Container(expand=True),
                        ft.IconButton(I.CLOSE, on_click=lambda _: self._close_overlay())
                    ]),
                    ft.Container(height=700, content=image, alignment=ft.alignment.center),
                    ft.Row([
                        ft.Container(expand=True),
                        ft.OutlinedButton("Fechar", icon=I.CLOSE, on_click=lambda _: self._close_overlay())
                    ], alignment=ft.MainAxisAlignment.END)
                ], spacing=8)
            )
            self._open_overlay(content)

        # ---------------- Tabelas/Builders ----------------
    def _build_fila_table(self) -> ft.DataTable:
            return ft.DataTable(columns=[
                ft.DataColumn(ft.Text("ID")),
                ft.DataColumn(ft.Text("Criado em")),
                ft.DataColumn(ft.Text("Solicitante")),
                ft.DataColumn(ft.Text("Cliente")),
                ft.DataColumn(ft.Row([ft.Icon(I.MARK_CHAT_UNREAD, size=16), ft.Text("Chat (não lidas)")], spacing=6)),

                ft.DataColumn(ft.Text("Sistema")),
                ft.DataColumn(ft.Text("Prioridade")),
                ft.DataColumn(ft.Text("Status")),
                ft.DataColumn(ft.Text("Responsável")),
                ft.DataColumn(ft.Text("Ações")),
            ], rows=[], column_spacing=14, data_row_min_height=44)

    def _load_fila_rows(self):
            start_gen = getattr(self, "_tab_gen", 0)
            _in_chat = str(getattr(self.page, "route", "") or "").startswith("/chat/")
            if getattr(self, "current_tab", None) != 2 and not _in_chat:
                return
            self.table_fila.rows.clear()
            reqs = self.db.list_requests(status=self._normalize_status(getattr(self, "filter_status", None).value if getattr(self, "filter_status", None) else None))
            users = {u.id: u.username for u in self.db.list_users()}
            for r in reqs:
                def _pegar(_):
                    self.db.take_request(r.id, self.user.id)
                    self._fila_suppress_next_reload = True
                    try:
                        (self._pubsub and self._pubsub.send_all({"type":"queue_changed"}))
                    except Exception:
                        pass
                    self._reload_open_list()
                def _liberar(_):
                    self.db.release_request(r.id, self.user.id)
                    self._fila_suppress_next_reload = True
                    try:
                        (self._pubsub and self._pubsub.send_all({"type":"queue_changed"}))
                    except Exception:
                        pass
                    self._reload_open_list()
                def _resolver(_):
                    self.db.resolve_request(r.id, self.user.id)
                    self._fila_suppress_next_reload = True
                    try:
                        (self._pubsub and self._pubsub.send_all({"type":"queue_changed"}))
                    except Exception:
                        pass
                    self._reload_open_list()
                def _cancelar(_):
                    self.db.cancel_request(r.id, self.user.id)
                    self._fila_suppress_next_reload = True
                    try:
                        (self._pubsub and self._pubsub.send_all({"type":"queue_changed"}))
                    except Exception:
                        pass
                    self._reload_open_list()

                actions: List[ft.Control] = []
                actions.append(ft.IconButton(I.VISIBILITY, tooltip="Visualizar", on_click=lambda _=None, rid=r.id: self._open_detail_dialog(rid)))
                actions.append(ft.IconButton(I.CHAT, tooltip="Chat", on_click=lambda _=None, rid=r.id: self._open_chat_dialog(rid)))
                if r.status == "aberta":
                    actions.append(ft.IconButton(I.PLAY_ARROW, tooltip="Pegar", on_click=_pegar))
                    if self.user.is_admin:
                        actions.append(ft.IconButton(I.CANCEL, tooltip="Cancelar", on_click=_cancelar))
                elif r.status == "em_atendimento":
                    actions.append(ft.IconButton(I.CHECK, tooltip="Resolver", on_click=_resolver))
                    actions.append(ft.IconButton(I.REMOVE_CIRCLE, tooltip="Liberar", on_click=_liberar))
                elif r.status in ("resolvida", "cancelada"):
                    actions.append(ft.IconButton(I.DOWNLOAD, tooltip="Exportar CSV", on_click=lambda _=None, rid=r.id: self._export_one(rid)))

                id_btn = ft.TextButton(str(r.id), on_click=lambda _=None, rid=r.id: self._open_detail_dialog(rid))

            
                # _NL_PRELOAD_FILA_: pré-carga de 'não lidas' para a Fila
                try:
                    if self._user_is("DBA"):
                        other_id = r.created_by
                    else:
                        other_id = r.taken_by or 0
                    self._unread[r.id] = (
                        self.db.get_unread_count(r.id, self.user.id, other_id)
                        if other_id and other_id != self.user.id else 0
                    )
                except Exception:
                    self._unread[r.id] = 0

                self.table_fila.rows.append(ft.DataRow(cells=[
                    ft.DataCell(id_btn),
                    ft.DataCell(ft.Text(r.created_at.astimezone(TZ).strftime("%d/%m/%Y %H:%M:%S"))),
                    ft.DataCell(ft.Text(users.get(r.created_by, "-"))),
                    ft.DataCell(ft.Text(r.cliente or "-")),
                    ft.DataCell(self._build_nl_cell(r.id, "fila")),

                    ft.DataCell(ft.Text(r.sistema or "-")),
                    ft.DataCell(self._prioridade_chip(r.prioridade)),
                    ft.DataCell(self._status_chip(r.status)),
                    ft.DataCell(ft.Text(users.get(r.taken_by, "-") if r.taken_by else "-")),
                    ft.DataCell(ft.Row(actions, spacing=4)),
                ]))
            self._update_fila_badge()
            # Guard final contra renderização atrasada
            if start_gen != getattr(self, "_tab_gen", 0) or (getattr(self, "current_tab", None) != 2 and not _in_chat):
                return
            # Guard final contra renderização atrasada
            if start_gen != getattr(self, "_tab_gen", 0) or (getattr(self, "current_tab", None) != 2 and not _in_chat):
                return
            self.page.update()

        # ---------------- Abas ----------------

    def _render_tab_content(self) -> None:

            """Renderiza conteúdo conforme aba selecionada.
            0=Home, 1=Abrir (perm), 2=Fila (perm), 3=Minhas, 4=Admin (admin).
            """
            try:
                start_gen = getattr(self, "_tab_gen", 0)
                tab = getattr(self, "current_tab", None)
                if tab is None and hasattr(self, "tabs"):
                    tab = self.tabs.selected_index

                # Monta a view localmente sem aplicar ainda
                view = None
                requested_tab = tab

                if tab == 1:
                    view = (self._abrir_tab() if self._can_open() else self._access_denied("abrir solicitações"))
                elif tab == 2:
                    view = (self._fila_tab() if self._can_queue() else self._access_denied("ver a fila"))
                elif tab == 3:
                    view = self._minhas_tab()
                elif tab == 4 and getattr(self.user, "is_admin", False):
                    view = self._admin_tab()
                else:
                    # 0 (Home) ou qualquer índice não mapeado
                    view = self._home_tab()

                # Antes de aplicar a view, confirma se a geração e a aba não mudaram
                if start_gen != getattr(self, "_tab_gen", 0) or getattr(self, "current_tab", None) != requested_tab:
                    return

                self.content_area.content = view
                try:
                    self.page.update()
                except Exception:
                    pass
            except Exception:
                # Fallback robusto para não quebrar a UI
                try:
                    self.content_area.content = self._home_tab()
                    self.page.update()
                except Exception:
                    # último recurso: noop
                    pass
    def _home_tab(self) -> ft.Control:
            """Home básica: pode ser substituída por um painel resumido futuro."""
            # Pequenos KPIs/atalhos (placeholders) + instrução
            return ft.Column([
                ft.Row([
                    ft.Text("Bem-vindo(a) ao Solicitações DBA", size=20, weight=ft.FontWeight.W_700),
                ]),
                ft.Text("Use as abas acima para navegar: Abrir, Fila, Minhas e Admin.", size=13, color=C.GREY_600),
                ft.Divider(),
                ft.ResponsiveRow([
                    ft.Container(
                        content=ft.Column([
                            ft.Row([ft.Icon(I.SUPPORT_AGENT), ft.Text("Abrir Solicitação")], spacing=8),
                            ft.Text("Usuários do grupo SUPORTE podem abrir solicitações para o DBA.", size=12, color=C.GREY_600),
                        ], spacing=4),
                        padding=16, border=ft.border.all(1, C.GREY_300), border_radius=12, col={"sm":12,"md":6,"lg":3}
                    ),
                    ft.Container(
                        content=ft.Column([
                            ft.Row([ft.Icon(I.VIEW_LIST), ft.Text("Fila")], spacing=8),
                            ft.Text("Usuários do grupo DBA podem pegar, liberar e resolver solicitações.", size=12, color=C.GREY_600),
                        ], spacing=4),
                        padding=16, border=ft.border.all(1, C.GREY_300), border_radius=12, col={"sm":12,"md":6,"lg":3}
                    ),
                    ft.Container(
                        content=ft.Column([
                            ft.Row([ft.Icon(I.TASK), ft.Text("Minhas")], spacing=8),
                            ft.Text("Veja suas solicitações e status.", size=12, color=C.GREY_600),
                        ], spacing=4),
                        padding=16, border=ft.border.all(1, C.GREY_300), border_radius=12, col={"sm":12,"md":6,"lg":3}
                    ),
                    ft.Container(
                        visible=getattr(self.user, "is_admin", False),
                        content=ft.Column([
                            ft.Row([ft.Icon(I.ADMIN_PANEL_SETTINGS), ft.Text("Admin")], spacing=8),
                            ft.Text("Ferramentas administrativas.", size=12, color=C.GREY_600),
                        ], spacing=4),
                        padding=16, border=ft.border.all(1, C.GREY_300), border_radius=12, col={"sm":12,"md":6,"lg":3}
                    ),
                ], spacing=12, run_spacing=12),
            ], spacing=8)

    def _on_tab_change(self, e: ft.ControlEvent):
            self._tab_gen = getattr(self, "_tab_gen", 0) + 1
            # Atualiza aba selecionada e renderiza conteúdo UMA vez
            self.current_tab = self.tabs.selected_index
            self._render_tab_content()
        
            # Badge da fila
            try:
                self._update_fila_badge()
            except Exception:
                pass
        
            # Timer da Fila
            try:
                if self.current_tab == 2:
                    self._start_fila_timer()
                else:
                    self._stop_fila_timer()
            except Exception:
                pass
        
            # Badge de Minhas
            try:
                self._update_minhas_badge()
            except Exception:
                pass
        
            # Timer de Minhas
            try:
                if self.current_tab == 3:
                    self._start_minhas_timer()
                else:
                    self._stop_minhas_timer()
            except Exception:
                pass
    def _goto_tab(self, idx: int):
            self.tabs.selected_index = idx
            self._on_tab_change(None)

    def _abrir_tab(self) -> ft.Control:
            if not self._can_open():
                return self._access_denied("abrir solicitações")
            new_files = []  # (name, mime, data)
            fp = ft.FilePicker()
            try:
                self.page.overlay.append(fp)
            except Exception:
                pass
            anexos_preview = ft.Column([], spacing=8)
            def refresh_previews():
                tiles = []
                for name, mime, data in new_files:
                    if str(mime).startswith("image/"):
                        import base64
                        b64 = base64.b64encode(data).decode("ascii")
                        img = ft.Image(src_base64=b64, width=120, height=90, fit=ft.ImageFit.COVER, border_radius=8)
                    else:
                        img = ft.Icon(I.INSERT_DRIVE_FILE, size=40)
                    cap = ft.Text(name, size=11, max_lines=1, overflow=ft.TextOverflow.ELLIPSIS)
                    def remove_one(_=None, fname=name):
                        for i, (n, _, __) in enumerate(list(new_files)):
                            if n == fname:
                                new_files.pop(i); break
                        refresh_previews(); self.page.update()
                    tiles.append(ft.Container(width=150, padding=6, content=ft.Column([img, cap, ft.TextButton("Remover", icon=I.DELETE, on_click=remove_one)], spacing=4, horizontal_alignment=ft.CrossAxisAlignment.CENTER)))
                anexos_preview.controls.clear()
                if tiles:
                    anexos_preview.controls.append(ft.Text("Anexos (pré-envio)", size=16, weight=ft.FontWeight.BOLD))
                    anexos_preview.controls.append(ft.Row(tiles, wrap=True, spacing=8))
            def on_pick_result(e: ft.FilePickerResultEvent):
                if not e.files:
                    return
                for f in e.files:
                    try:
                        with open(f.path, "rb") as fh:
                            data = fh.read()
                    except Exception:
                        self._toast(f"Não foi possível ler: {f.name}")
                        continue
                    if len(data) > 10 * 1024 * 1024:
                        self._toast(f"{f.name}: acima de 10MB")
                        continue
                    mime = ftype_from_name(f.name)
                    new_files.append((f.name, mime, data))
                refresh_previews(); self.page.update()
            fp.on_result = on_pick_result
            def pick_files(_=None):
                fp.pick_files(allow_multiple=True)
            def clear_attachments(_=None):
                new_files.clear(); refresh_previews(); self.page.update()
            def do_submit(_):
                if not self.f_title.value or not self.f_cliente.value:
                    self._toast("Informe Título e Cliente."); return
                rid = self.db.create_request(
                    title=self.f_title.value, description=self.f_desc.value or "", cliente=self.f_cliente.value or "",
                    sistema=self.f_sistema.value or "", prioridade=self.f_prioridade.value or "Média",
                    loja_parada=bool(self.f_loja_parada.value), created_by=self.user.id,
                )
                for name, mime, data in new_files:
                    self.db.add_file(rid, name, mime, data, self.user.id)
                clear_attachments()
                self._toast(f"Solicitação #{rid} aberta!")
                self.f_title.value = self.f_cliente.value = self.f_sistema.value = ""
                self.f_desc.value = ""
                self.f_prioridade.value = "Média"
                self.f_loja_parada.value = False
                self.page.update()
            form = ft.Card(content=ft.Container(padding=16, content=ft.Column([
                ft.Text("Abrir Solicitação para DBA", size=18, weight=ft.FontWeight.BOLD),
                ft.Row([self.f_title], spacing=10),
                ft.Row([self.f_cliente, self.f_sistema], spacing=10),
                ft.Row([self.f_prioridade, self.f_loja_parada], spacing=10),
                self.f_desc,
                ft.Divider(),
                ft.Row([
                    ft.OutlinedButton("Anexar arquivo(s)", icon=I.ATTACH_FILE, on_click=pick_files),
                    ft.TextButton("Limpar anexos", icon=I.CLEAR_ALL, on_click=clear_attachments),
                ], alignment=ft.MainAxisAlignment.START, spacing=8),
                ft.Card(content=ft.Container(padding=12, content=anexos_preview)),
                ft.Row([
                    ft.FilledButton("Enviar", icon=I.SEND, on_click=do_submit),
                    ft.OutlinedButton("Limpar", icon=I.CLEAR, on_click=lambda _: self._clear_form()),
                ], alignment=ft.MainAxisAlignment.END),
            ], spacing=10)))
            refresh_previews()
            return ft.Column([form], scroll=ft.ScrollMode.ADAPTIVE)

    def _clear_form(self):
            self.f_title.value = self.f_cliente.value = self.f_sistema.value = ""
            self.f_desc.value = ""
            self.f_prioridade.value = "Média"
            self.f_loja_parada.value = False
            self.page.update()

    def _fila_tab(self) -> ft.Control:

            # # Normaliza Dropdown de Status (Fila)
            # self._normalize_status_dropdown(self.filter_status)
            def on_status_change(e):
                self._load_fila_rows()

            self.filter_status.on_change = on_status_change

            btn_refresh = ft.OutlinedButton(
                "Atualizar",
                icon=I.REFRESH,
                on_click=lambda _: self._load_fila_rows(),
            )

            toolbar = ft.Row([
                ft.Text("Status:"),
                self.filter_status,
                btn_refresh,
                ft.Text("Próximo auto:"),
                self._fila_timer_lbl,
                ft.Container(expand=True),
                ft.OutlinedButton(
                    "Exportar Fila",
                    icon=I.DOWNLOAD,
                    on_click=lambda _: self._export_queue(self.filter_status.value),
                ),
            ], spacing=10)

            self._load_fila_rows()

            return ft.Column([
                ft.Card(content=ft.Container(padding=12, content=toolbar)),
                ft.Card(content=ft.Container(padding=8, content=self.table_fila)),
            ], spacing=12)
    def _admin_tab(self) -> ft.Control:
            users = self.db.list_users()
            grupos = ["SUPORTE", "DBA"]

            table = ft.DataTable(columns=[
                ft.DataColumn(ft.Text("ID")),
                ft.DataColumn(ft.Text("Usuário")),
                ft.DataColumn(ft.Text("Admin")),
                ft.DataColumn(ft.Text("Ativo")),
                ft.DataColumn(ft.Text("Grupos")),
                ft.DataColumn(ft.Text("Ações")),
            ], rows=[], column_spacing=14, data_row_min_height=48)

            def refresh():
                start_gen = getattr(self, "_tab_gen", 0)
                if getattr(self, "current_tab", None) != 4:
                    return
                table.rows.clear()
                for u in self.db.list_users():
                    chk_sup = ft.Checkbox(value=self.db.user_in_group(u.id, "SUPORTE"))
                    chk_dba = ft.Checkbox(value=self.db.user_in_group(u.id, "DBA"))

                    def bind_toggle(uid=u.id, chk=chk_sup, name="SUPORTE"):
                        def _(_e=None):
                            self.db.set_user_group(uid, name, bool(chk.value))
                        return _
                    chk_sup.on_change = bind_toggle(u.id, chk_sup, "SUPORTE")
                    chk_dba.on_change = bind_toggle(u.id, chk_dba, "DBA")

                    def toggle_admin(_e=None, uid=u.id, cur=bool(u.is_admin)):
                        self.db.set_user_admin(uid, not cur)
                        refresh()
                    def toggle_active(_e=None, uid=u.id, cur=bool(u.active)):
                        self.db.set_user_active(uid, not cur)
                        refresh()

                    table.rows.append(ft.DataRow(cells=[
                        ft.DataCell(ft.Text(str(u.id))),
                        ft.DataCell(ft.Text(u.username)),
                        ft.DataCell(ft.Text("Sim" if u.is_admin else "Não")),
                        ft.DataCell(ft.Text("Sim" if u.active else "Não")),
                        ft.DataCell(ft.Row([ft.Text("SUPORTE"), chk_sup, ft.Text("DBA"), chk_dba], spacing=6)),
                        ft.DataCell(ft.Row([
                            ft.IconButton(I.VERIFIED_USER if u.is_admin else I.SHIELD, tooltip="Alternar admin", on_click=toggle_admin),
                            ft.IconButton(I.TOGGLE_ON if u.active else I.TOGGLE_OFF, tooltip="Ativar/Desativar", on_click=toggle_active),
                        ], spacing=4)),
                    ]))
                # Guard final no refresh da aba Admin
                if start_gen != getattr(self, "_tab_gen", 0) or getattr(self, "current_tab", None) != 4:
                    return
                # Guard final no refresh da aba Admin
                if start_gen != getattr(self, "_tab_gen", 0) or getattr(self, "current_tab", None) != 4:
                    return
                self.page.update()

            refresh()

            # Criar usuário
            nu_user = ft.TextField(label="Usuário", width=220)
            nu_p1 = ft.TextField(label="Senha", password=True, can_reveal_password=True, width=220)
            nu_p2 = ft.TextField(label="Confirmar senha", password=True, can_reveal_password=True, width=220)
            nu_is_admin = ft.Checkbox(label="Administrador")

            def create_user(_):
                if not nu_user.value or not nu_p1.value or not nu_p2.value:
                    self._toast("Informe usuário e senha.")
                    return
                if nu_p1.value != nu_p2.value:
                    self._toast("Senhas não conferem.")
                    return
                self.db.create_user(nu_user.value, nu_p1.value, is_admin=bool(nu_is_admin.value))
                nu_user.value = nu_p1.value = nu_p2.value = ""
                nu_is_admin.value = False
                refresh()

            return ft.Column([
                ft.Text("Administração de Usuários e Grupos", size=18, weight=ft.FontWeight.BOLD),
                ft.Card(content=ft.Container(padding=12, content=table)),
                ft.Divider(),
                ft.Text("Novo usuário", size=14, weight=ft.FontWeight.BOLD),
                ft.Row([nu_user, nu_p1, nu_p2, nu_is_admin, ft.FilledButton("Criar", icon=I.PERSON_ADD, on_click=create_user)], spacing=10, wrap=True),
            ], spacing=12)

        # ---------------- Detalhe (popup) + Anexos ----------------
    def _open_detail_dialog(self, rid: int) -> None:
            # Tamanhos responsivos do diálogo e da área de anexos (sem transparência)
            try:
                win_w = int(self.page.window_width or 1080)
                win_h = int(self.page.window_height or 720)
            except Exception:
                win_w, win_h = 1080, 720
            dlg_w = max(700, min(980, win_w - 80))
            anexos_h = max(240, min(520, int(win_h * 0.55)))
            r = self.db.get_request(rid)
            if not r:
                self._toast("Solicitação não encontrada.")
                return

            users = {u.id: u.username for u in self.db.list_users()}
            solicitante = users.get(r.created_by, "-")
            responsavel = users.get(r.taken_by, "-") if r.taken_by else "-"

            chips = ft.Row(controls=[
                ft.Chip(label=ft.Text(f"#{r.id}"), leading=ft.Icon(I.TAG)),
                self._status_chip(r.status),
                self._prioridade_chip(r.prioridade),
                self._loja_chip(r.loja_parada),
            ], spacing=6)

            info = ft.Column(controls=[
                ft.Text(r.title, size=18, weight=ft.FontWeight.BOLD),
                ft.Text(r.description or "(sem descrição)", selectable=True),
                ft.Divider(),
                ft.Row(controls=[ft.Text(f"Cliente: {r.cliente or '-'}"), ft.Text(f"Sistema: {r.sistema or '-'}")], wrap=True, spacing=20),
                ft.Row(controls=[ft.Text(f"Solicitante: {solicitante}"), ft.Text(f"Responsável: {responsavel}")], wrap=True, spacing=20),
                ft.Row(controls=[
                    ft.Text(f"Criada: {r.created_at.astimezone(TZ).strftime('%d/%m/%Y %H:%M:%S')}") ,
                    ft.Text(f"Última atualização: {r.last_update.astimezone(TZ).strftime('%d/%m/%Y %H:%M:%S')}") ,
                ], wrap=True, spacing=20),
            ], spacing=6)

            # --- Anexos ---
            anexos_container = ft.Container()
            fp = ft.FilePicker()
            self.page.overlay.append(fp)

            def open_file(att_id: int, fname: str, mime: str):
                att = self.db.get_file(att_id)
                if not att:
                    self._toast("Arquivo não encontrado")
                    return
                if mime.startswith("image/") and att.blob:
                    self._open_image_preview(fname, att.blob)
                else:
                    self._save_and_open_file(fname, att.blob)

            def export_file(att_id: int, fname: str):
                att = self.db.get_file(att_id)
                if not att:
                    self._toast("Arquivo não encontrado")
                    return
                self._save_and_open_file(fname, att.blob)

            def load_files():
                files = self.db.list_files(r.id)
                # Tabela de anexos com ações claras
                def _hsize(n: int) -> str:
                    units = ["B","KB","MB","GB","TB"]
                    i = 0
                    f = float(max(n, 0))
                    while f >= 1024 and i < len(units)-1:
                        f /= 1024.0
                        i += 1
                    return f"{f:.1f} {units[i]}"
                # helper: extrai extensão amigável
                def _ext_of(filename: str, mime: str | None) -> str:
                    import os
                    ext = os.path.splitext(filename or "")[1].lower()
                    if ext:
                        return ext
                    mm = (mime or "").lower()
                    mapping = {
                        "image/jpeg": ".jpg",
                        "image/jpg": ".jpg",
                        "image/png": ".png",
                        "image/gif": ".gif",
                        "application/pdf": ".pdf",
                        "text/plain": ".txt",
                        "text/csv": ".csv",
                        "application/zip": ".zip",
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
                        "application/vnd.ms-excel": ".xls",
                        "application/msword": ".doc",
                        "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
                    }
                    return mapping.get(mm, (("." + mm.split("/")[-1]) if "/" in mm else (ext or "-")))

                rows = []
                for f in files:
                    nome_cell = ft.TextButton(f.filename, icon=I.INSERT_DRIVE_FILE,
                                              on_click=lambda _=None, fid=f.id, fn=f.filename, mm=f.mime: open_file(fid, fn, mm))
                    tipo = ft.Text(_ext_of(f.filename, getattr(f, 'mime', None)), size=12)
                    tam = ft.Text(_hsize(getattr(f, 'size_bytes', len(f.blob) if getattr(f, 'blob', b'') else 0)), size=12)
                    up_by = ft.Text(users.get(getattr(f, 'uploaded_by', None), str(getattr(f, 'uploaded_by', '-'))), size=12)
                    up_at = ft.Text((f.created_at.astimezone(TZ).strftime('%d/%m/%Y %H:%M') if getattr(f, 'created_at', None) else '-'), size=12)
                    actions = ft.Row([
                        ft.IconButton(I.OPEN_IN_NEW, tooltip="Abrir/Visualizar",
                                      on_click=lambda _=None, fid=f.id, fn=f.filename, mm=f.mime: open_file(fid, fn, mm)),
                        ft.IconButton(I.DOWNLOAD, tooltip="Exportar/Salvar",
                                      on_click=lambda _=None, fid=f.id, fn=f.filename: export_file(fid, fn)),
                    ], spacing=4)
                    rows.append(ft.DataRow(cells=[
                        ft.DataCell(nome_cell),
                        ft.DataCell(tipo),
                        ft.DataCell(tam),
                        ft.DataCell(up_by),
                        ft.DataCell(up_at),
                        ft.DataCell(actions),
                    ]))
                table = ft.DataTable(
                    columns=[
                        ft.DataColumn(ft.Text("Nome")),
                        ft.DataColumn(ft.Text("Extensão")),
                        ft.DataColumn(ft.Text("Tamanho")),
                        ft.DataColumn(ft.Text("Usuário")),
                        ft.DataColumn(ft.Text("Enviado em")),
                        ft.DataColumn(ft.Text("Ações")),
                    ],
                    rows=rows,
                    heading_row_height=38,
                    data_row_max_height=44,
                    column_spacing=16,
                    divider_thickness=0.5,
                )
                anexos_container.content = ft.Column([
                    ft.Text("Anexos", size=16, weight=ft.FontWeight.BOLD),
                    ft.Container(content=table, expand=True),
                ], spacing=8)

            def on_pick_result(e: ft.FilePickerResultEvent):
                if not e.files:
                    return
                for f in e.files:
                    try:
                        with open(f.path, "rb") as fh:
                            data = fh.read()
                    except Exception:
                        self._toast(f"Não foi possível ler: {f.name}")
                        continue
                    mime = ftype_from_name(f.name)
                    if len(data) > 10 * 1024 * 1024:
                        self._toast(f"{f.name}: acima de 10MB")
                        continue
                    self.db.add_file(r.id, f.name, mime, data, self.user.id)
                load_files()
                self.page.update()

            fp.on_result = on_pick_result
            anexar_btn = ft.OutlinedButton("Anexar arquivo", icon=I.ATTACH_FILE, on_click=lambda _: fp.pick_files(allow_multiple=True))
            load_files()

            # Ações
            def _do_take(_=None):
                self.db.take_request(r.id, self.user.id); self._close_overlay(); self._post_action_refresh()
            def _do_release(_=None):
                self.db.release_request(r.id, self.user.id); self._close_overlay(); self._post_action_refresh()
            def _do_resolve(_=None):
                self.db.resolve_request(r.id, self.user.id); self._close_overlay(); self._post_action_refresh()
            def _do_cancel(_=None):
                self.db.cancel_request(r.id, self.user.id); self._close_overlay(); self._post_action_refresh()
            def _open_chat(_=None):
                self._close_overlay(); self._open_chat_dialog(r.id)

            btns: List[ft.Control] = [
                ft.OutlinedButton("Exportar CSV", icon=I.DOWNLOAD, on_click=lambda _: self._export_one(r.id)),
                ft.FilledButton("Abrir Chat", icon=I.CHAT, on_click=_open_chat),
                ft.OutlinedButton("Fechar", icon=I.CLOSE, on_click=lambda _: self._close_overlay()),
            ]
            if r.status == "aberta":
                btns += [ft.FilledButton("Pegar", icon=I.PLAY_ARROW, on_click=_do_take)]
                if self.user.is_admin:
                    btns.append(ft.OutlinedButton("Cancelar", icon=I.CANCEL, on_click=_do_cancel))
            elif r.status == "em_atendimento":
                btns += [ft.FilledButton("Resolver", icon=I.CHECK, on_click=_do_resolve), ft.OutlinedButton("Liberar", icon=I.REMOVE_CIRCLE, on_click=_do_release)]

            content = ft.Container(
                width=900,
                border_radius=18,
                bgcolor=self.palette["bg"],
                padding=0,
                content=ft.Column(width=dlg_w, scroll=ft.ScrollMode.ADAPTIVE, controls=[
                    ft.Container(padding=14, bgcolor=self.palette["card"], border_radius=ft.border_radius.only(top_left=16, top_right=16), content=chips),
                    ft.Container(padding=12, content=ft.Card(content=ft.Container(padding=12, content=info))),
                    ft.Container(padding=12, content=ft.Row([anexar_btn], alignment=ft.MainAxisAlignment.START)),
                    ft.Container(padding=12, content=ft.Card(content=ft.Container(padding=12, height=anexos_h, content=ft.Column([anexos_container], expand=True, scroll=ft.ScrollMode.AUTO)))),
                    ft.Container(padding=12, content=ft.Row(controls=btns, alignment=ft.MainAxisAlignment.END)),
                ], spacing=0)
            )
            self._open_overlay(content)

        # ---------------- Chat (popup) + Anexos ----------------
    def _open_chat_dialog(self, rid: int) -> None:
            # Redireciona para View de chat roteada (mesmo comportamento do routed_fix2)
            try:
                self.page.go(f"/chat/{rid}")
            except Exception:
                self._toast("Não foi possível abrir o chat.")
                self.page.update()

    def _post_action_refresh(self) -> None:
            if self.current_tab == 2:  # Fila
                self._load_fila_rows()
            elif self.current_tab == 3:  # Minhas
                self.content_area.content = self._minhas_tab()
                self.page.update()

        # ---------------- Exportações ----------------
    def _export_queue(self, status: str):
            status_norm = self._normalize_status(status)
            rows = self.db.list_requests(status=status_norm)
            label = "todos" if status_norm is None else str(status_norm)
            fn = f"fila_{label}_{datetime.now(TZ).strftime('%Y%m%d_%H%M%S')}.csv"
            path = os.path.join(EXPORT_DIR, fn)
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f, delimiter=";")
                w.writerow(["ID", "Criado em", "Solicitante", "Cliente", "Sistema", "Prioridade", "Status", "Responsável"])
                users = {u.id: u.username for u in self.db.list_users()}
                for r in rows:
                    w.writerow([
                        r.id,
                        r.created_at.astimezone(TZ).strftime("%d/%m/%Y %H:%M:%S"),
                        users.get(r.created_by, "-"),
                        r.cliente or "-", r.sistema or "-", r.prioridade, r.status,
                        users.get(r.taken_by, "-") if r.taken_by else "-",
                    ])
            self._toast(f"Exportado: {path}")

    def _export_my(self):
            if self._user_is("DBA"):
                rows = self.db.list_requests(only_mine_taken_by=self.user.id)
                name = "minhas_atribuicoes"
            else:
                rows = self.db.list_requests(only_mine_opened_by=self.user.id)
                name = "minhas_aberturas"
            fn = f"{name}_{datetime.now(TZ).strftime('%Y%m%d_%H%M%S')}.csv"
            path = os.path.join(EXPORT_DIR, fn)
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f, delimiter=";")
                w.writerow(["ID", "Criado em", "Cliente", "Sistema", "Prioridade", "Status", "Responsável"])
                users = {u.id: u.username for u in self.db.list_users()}
                for r in rows:
                    w.writerow([
                        r.id,
                        r.created_at.astimezone(TZ).strftime("%d/%m/%Y %H:%M:%S"),
                        r.cliente or "-", r.sistema or "-", r.prioridade, r.status,
                        users.get(r.taken_by, "-") if r.taken_by else "-",
                    ])
            self._toast(f"Exportado: {path}")

    def _export_one(self, rid: int):
            r = self.db.get_request(rid)
            if not r:
                self._toast("Solicitação não encontrada.")
                return
            fn = f"solicitacao_{rid}.csv"
            path = os.path.join(EXPORT_DIR, fn)
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f, delimiter=";")
                w.writerow(["Campo", "Valor"])
                users = {u.id: u.username for u in self.db.list_users()}
                fields = [
                    ("ID", r.id), ("Criado em", r.created_at.astimezone(TZ).strftime("%d/%m/%Y %H:%M:%S")),
                    ("Solicitante", users.get(r.created_by, "-")), ("Cliente", r.cliente or "-"),
                    ("Sistema", r.sistema or "-"), ("Prioridade", r.prioridade), ("Status", r.status),
                    ("Responsável", users.get(r.taken_by, "-") if r.taken_by else "-"),
                ]
                for k, v in fields:
                    w.writerow([k, v])
            self._toast(f"Exportado: {path}")

        # ---------------- Sessão ----------------
    def _logout(self, e=None):
            """Encerra a sessão atual e volta para a tela de login."""
            try:
                # Fecha overlays específicos se existirem
                if hasattr(self, "_overlay_bg"):
                    self._overlay_bg.visible = False
                if hasattr(self, "_overlay_card"):
                    self._overlay_card.visible = False
                    self._overlay_card.content = None
    
                # Limpa overlay da página (dialogs/overlays globais)
                if getattr(self.page, "overlay", None) is not None:
                    try:
                        self.page.overlay.clear()
                    except Exception:
                        pass
    
                # Remove handlers e pilha de views
                try:
                    self.page.on_route_change = None
                    self.page.on_view_pop = None
                except Exception:
                    pass
    
                if getattr(self.page, "views", None) is not None:
                    try:
                        self.page.views.clear()
                    except Exception:
                        pass
    
                # Limpa controles, reseta rota e atualiza
                try:
                    self.page.controls.clear()
                except Exception:
                    pass
                try:
                    self.page.route = "/"
                except Exception:
                    pass
                try:
                    self.page.update()
                except Exception:
                    pass
            except Exception:
                # Nunca deixa erro impedir o retorno ao login
                pass
    
            # Callback para exibir login novamente
            try:
                self.on_logout()
            except Exception:
                pass

        # ====== Função principal do Flet (corrige Pylance: target=main) ======
def main(page):
    db = DB(DB_PATH)

    def after_login(user: User):
        DBAApp(page, db, user, on_logout=show_login)

    def show_login():
        LoginView(page, db, on_success=after_login)

    show_login()

# ====== INÍCIO: bloco de inicialização ======
if __name__ == "__main__":
    import argparse
    import flet as ft
    from pathlib import Path

    # garante BASE_DIR se não existir no módulo
    try:
        BASE_DIR
    except NameError:
        BASE_DIR = Path(__file__).resolve().parent

    parser = argparse.ArgumentParser(description="Solicitações DBA - Desktop/Web")
    parser.add_argument("--web", action="store_true", help="Inicia como servidor web (acesso via navegador)")
    parser.add_argument("--host", default="0.0.0.0", help="Host para o servidor web (padrão: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8551, help="Porta do servidor web (padrão: 8551)")
    parser.add_argument("--view", default="BROWSER", choices=["BROWSER", "NONE"],
                        help="BROWSER abre o navegador local (útil em dev); NONE só inicia o servidor")
    args = parser.parse_args()

    if args.web:
        # Modo SERVIDOR WEB: usuários acessam por http://host:port
        # view=None evita abrir navegador no servidor (use BROWSER em desenvolvimento)
        app_view = None if args.view == "NONE" else ft.AppView.WEB_BROWSER

        ft.app(
            target=main,
            view=app_view,
            host=args.host,
            port=args.port,
        )
    else:
        # Modo DESKTOP (janela nativa)
        ft.app(
            target=main,
            view=ft.AppView.FLET_APP,  # janela nativa
        )
# ====== FIM: bloco de inicialização ======