#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Monitor Cloud Backup (Acronis) — HOS Sistemas
Versão SQLAlchemy (DB plugável): SQLite / PostgreSQL / MySQL

Recursos:
- Cache/persistência em banco (SQLAlchemy Core) — TTL por tenant + ETag/Last-Modified
- UI Flet: KPIs, busca, filtros, chips de severidade, ordenação, paginação, export CSV
- Tema claro/escuro e switch para habilitar Debug no console
- Descoberta de tenant raiz via /api/2/clients/{CLIENT_ID} (token client_credentials)
- Blindagem de FK: insere/atualiza tenant antes do alerta; se sem tenant_id -> grava NULL

Dependências:
  pip install flet requests pandas sqlalchemy

Config via ENV:
- BASE_URL, CLIENT_ID, CLIENT_SECRET
- DATABASE_URL (ex.: sqlite:///C:/pasta/acronis_database.db | postgresql+psycopg2://user:pass@host/db | mysql+pymysql://user:pass@host/db)
  * Se não definido, usa SQLite na pasta do app: ./cache/acronis_database.db
- CACHE_TTL_SECONDS (padrão: 300)
- CACHE_MODE = cache-first | network-first (padrão: cache-first)
- PAGE_SIZE (padrão: 50)
- MAX_ROWS (padrão: 10000)
- LOG_LEVEL = DEBUG | INFO | WARNING | ERROR (padrão: INFO)
"""
import os
import sys
import json
import time
import asyncio
from pathlib import Path
import logging
import platform
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler
import subprocess

# =============================
# Util: base dir (script vs exe)
# =============================
def app_base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    try:
        return Path(__file__).resolve().parent
    except NameError:
        return Path.cwd()

BASE_DIR = app_base_dir()

# =============================
# Helpers de diretório/config
# =============================
def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

CONFIG_DIR = (BASE_DIR / "config")
CONFIG_PATH = CONFIG_DIR / "settings.json"
ensure_dir(CONFIG_DIR)

# ===== DIAGNOSTIC LOGGING =====
DIAG_ENV = os.getenv("DIAG_LOG", "0") == "1"
DIAG_DIR = os.getenv("DIAG_LOG_PATH", "logs")  # manter nome "logs"
DIAG_ROTATION = os.getenv("DIAG_ROTATION", "daily")  # daily|size
DIAG_BACKUP = int(os.getenv("DIAG_BACKUP", "7"))     # days/files
DIAG_MAXMB  = int(os.getenv("DIAG_MAXMB", "5"))
DIAG_LOG_RAW = os.getenv("DIAG_LOG_RAW", "0") == "1"
DIAG_JSON_INDENT = int(os.getenv("DIAG_JSON_INDENT", "0"))
DIAG_BODY_MAXKB = int(os.getenv("DIAG_BODY_MAXKB", "0"))

diag_logger = logging.getLogger("diag")
diag_logger.setLevel(logging.INFO)
diag_logger.propagate = False
_diag_handler = None
diag_console_handler = None
diag_logger.addHandler(logging.NullHandler())

def _redact(v: str, keep: int = 4) -> str:
    if not v:
        return ""
    s = str(v)
    if len(s) <= keep*2:
        return "*" * len(s)
    return f"{s[:keep]}***{s[-keep:]}"

def start_diagnostic_logger():
    """Liga logger em BASE_DIR/logs com rotação configurável."""
    from datetime import datetime as _dt
    global _diag_handler
    if _diag_handler:
        return
    log_dir = (BASE_DIR / DIAG_DIR)
    ensure_dir(log_dir)
    logfile = log_dir / f"monitor_{_dt.now().strftime('%Y-%m-%d')}.log"
    if DIAG_ROTATION.lower() == "size":
        _diag_handler = RotatingFileHandler(logfile, maxBytes=DIAG_MAXMB*1024*1024, backupCount=DIAG_BACKUP, encoding="utf-8")
    else:
        _diag_handler = TimedRotatingFileHandler(logfile, when="midnight", backupCount=DIAG_BACKUP, encoding="utf-8")
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    _diag_handler.setFormatter(fmt)
    diag_logger.addHandler(_diag_handler)
    diag_logger.propagate = False
    diag_logger.info("=== DIAG mode ON ===")
    diag_logger.info(f"[diag] path={logfile}")

def diag_current_log_path():
    from datetime import datetime as _dt
    log_dir = (BASE_DIR / DIAG_DIR)
    ensure_dir(log_dir)
    return log_dir / f"monitor_{_dt.now().strftime('%Y-%m-%d')}.log"

def attach_diag_console():
    global diag_console_handler
    if diag_console_handler:
        return
    diag_console_handler = logging.StreamHandler()
    diag_console_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    diag_logger.addHandler(diag_console_handler)

def detach_diag_console():
    global diag_console_handler
    if not diag_console_handler:
        return
    diag_logger.removeHandler(diag_console_handler)
    diag_console_handler = None

def stop_diagnostic_logger():
    global _diag_handler
    if not _diag_handler:
        return
    diag_logger.info("=== DIAG mode OFF ===")
    diag_logger.removeHandler(_diag_handler)
    _diag_handler.close()
    _diag_handler = None

def _redact_json(obj):
    if DIAG_LOG_RAW:
        return obj
    SENSITIVE = {"access_token", "refresh_token", "client_secret", "authorization", "authorization_bearer"}
    try:
        if isinstance(obj, dict):
            out = {}
            for k, v in obj.items():
                if str(k).lower() in SENSITIVE:
                    out[k] = _redact(v)
                else:
                    out[k] = _redact_json(v)
            return out
        elif isinstance(obj, list):
            return [_redact_json(x) for x in obj]
    except Exception:
        pass
    return obj

def _truncate_body(text: str) -> str:
    if not text or DIAG_BODY_MAXKB <= 0:
        return text
    limit = DIAG_BODY_MAXKB * 1024
    try:
        b = text.encode('utf-8', 'ignore')
        if len(b) <= limit:
            return text
        return b[:limit].decode('utf-8', 'ignore') + "...<truncated>"
    except Exception:
        return text

def diag_dump_json(prefix: str, data):
    try:
        import json as _json
        if isinstance(data, (dict, list)):
            red = _redact_json(data)
            if DIAG_JSON_INDENT > 0:
                diag_logger.info(f"[json] {prefix}:\n" + _json.dumps(red, ensure_ascii=False, indent=DIAG_JSON_INDENT))
            else:
                diag_logger.info(f"[json] {prefix}: " + _json.dumps(red, ensure_ascii=False, separators=(",", ":")))
        else:
            diag_logger.info(f"[json] {prefix}: {data}")
    except Exception as ex:
        diag_logger.info(f"[json] {prefix}: <error {ex}>")

# ===== /DIAGNOSTIC LOGGING =====

from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Tuple

import requests
import pandas as pd
import flet as ft

# Timezone helpers
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
    HAVE_ZONEINFO = True
except Exception:
    HAVE_ZONEINFO = False

try:
    import pytz  # fallback
    HAVE_PYTZ = True
except Exception:
    HAVE_PYTZ = False

TZ_NAME = "America/Sao_Paulo"

def _get_brt_tz():
    if HAVE_ZONEINFO:
        try:
            return ZoneInfo(TZ_NAME)
        except Exception:
            pass
    if HAVE_PYTZ:
        try:
            return pytz.timezone(TZ_NAME)
        except Exception:
            pass
    return None

BRT = _get_brt_tz()

def fmt_brt(iso_str: str) -> str:
    if not iso_str:
        return ""
    s = str(iso_str).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = None
    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z",
                    "%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d %H:%M:%S"):
            try:
                dt = datetime.strptime(s, fmt)
                break
            except Exception:
                pass
    if dt is None:
        return iso_str
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    if BRT is not None:
        try:
            dt_brt = dt.astimezone(BRT)
            return dt_brt.strftime("%d/%m/%Y %H:%M:%S")
        except Exception:
            pass
    return dt.astimezone().strftime("%d/%m/%Y %H:%M:%S")

def agora_brt_str() -> str:
    if BRT is None:
        return datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    return datetime.now(BRT).strftime("%d/%m/%Y %H:%M:%S")

# SQLAlchemy
from sqlalchemy import (
    create_engine, MetaData, Table, Column, String, Text, Integer,
    ForeignKey, Index, select, func, text
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
try:
    from sqlalchemy.dialects.postgresql import insert as pg_insert
except Exception:
    pg_insert = None
try:
    from sqlalchemy.dialects.mysql import insert as my_insert
except Exception:
    my_insert = None

# --- Logging no console -------------------------------------------------------
LOG_LEVEL = (os.getenv("LOG_LEVEL") or "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("hos.acronis.monitor")
# ------------------------------------------------------------------------------

# =============================
# CREDENCIAIS / CONFIG (defaults)
# =============================
BASE_URL = (os.getenv("BASE_URL") or "https://backupcloud.fsassistencia.com.br").rstrip("/")
CLIENT_ID = os.getenv("CLIENT_ID") or "8857d247-853a-408d-9189-861c4d3efdfe"
CLIENT_SECRET = os.getenv("CLIENT_SECRET") or "mwh4sfypidsarqnk6ixpucy5dizuzv64br6olsc4dmrw476rg6fq"

DEFAULT_LIMIT = int(os.getenv("ALERTS_LIMIT", "200"))
TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "30"))
VERIFY_SSL = (os.getenv("VERIFY_SSL", "true").lower() != "false")

RETRY_TOTAL = int(os.getenv("HTTP_RETRY_TOTAL", "3"))
RETRY_BACKOFF = float(os.getenv("HTTP_RETRY_BACKOFF", "0.8"))
RETRY_STATUS = (429, 500, 502, 503, 504)

# DB config
DEFAULT_DB_PATH = (BASE_DIR / "data" / "acronis_database.db").resolve()
ensure_dir(DEFAULT_DB_PATH.parent)
DATABASE_URL = os.getenv("DATABASE_URL") or f"sqlite:///{DEFAULT_DB_PATH}"

CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "300"))
CACHE_MODE = (os.getenv("CACHE_MODE") or "cache-first").lower()
PAGE_SIZE_DEFAULT = int(os.getenv("PAGE_SIZE", "50"))
MAX_ROWS = int(os.getenv("MAX_ROWS", "10000"))

tenant_name_cache: Dict[str, str] = {}

# =============================
# Persistência de Config (JSON)
# =============================
DEFAULT_SETTINGS = {
    "BASE_URL": BASE_URL,            # mantém default atual
    "CLIENT_ID": CLIENT_ID,          # mantém default atual
    "CLIENT_SECRET": CLIENT_SECRET,  # mantém default atual
    "DATABASE_URL": "",              # se vazio, usa SQLite em DEFAULT_DB_PATH
    "CACHE_TTL_SECONDS": 300,
    "CACHE_MODE": "cache-first",     # ou "network-first"
    "VERIFY_SSL": True,
    "PAGE_SIZE": 50,
    "LOG_TO_FILE": False,
    # Novo: persistir limite por requisição (Qtd)
    "ALERTS_LIMIT": DEFAULT_LIMIT
}

def load_settings() -> dict:
    try:
        if CONFIG_PATH.exists():
            data = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
            out = dict(DEFAULT_SETTINGS)
            out.update({k: data.get(k, out.get(k)) for k in DEFAULT_SETTINGS.keys()})
            return out
    except Exception:
        pass
    return dict(DEFAULT_SETTINGS)

def save_settings(data: dict):
    ensure_dir(CONFIG_DIR)
    CONFIG_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

# =============================
# Sessão HTTP com retries/backoff
# =============================
def build_session() -> requests.Session:
    logger.debug("Construindo sessão HTTP com retries/backoff")
    s = requests.Session()
    try:
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        retry = Retry(
            total=RETRY_TOTAL, read=RETRY_TOTAL, connect=RETRY_TOTAL,
            backoff_factor=RETRY_BACKOFF, status_forcelist=RETRY_STATUS,
            allowed_methods=frozenset(["GET","POST","PUT","PATCH","DELETE","HEAD","OPTIONS"]),
            raise_on_status=False,
        )
        s.mount("https://", HTTPAdapter(max_retries=retry))
        s.mount("http://", HTTPAdapter(max_retries=retry))
    except Exception:
        pass
    return s

session = build_session()

class ApiError(Exception):
    pass

def base_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}

def get_token() -> str:
    logger.debug("Solicitando token OAuth2 (client_credentials)")
    url = f"{BASE_URL}/api/2/idp/token"
    auth = (CLIENT_ID, CLIENT_SECRET)
    data = {"grant_type": "client_credentials"}
    diag_dump_json("request token form", data)
    r = session.post(url, auth=auth, data=data, timeout=TIMEOUT, verify=VERIFY_SSL)
    if r.status_code != 200:
        diag_logger.info(f"[net][token] status={r.status_code} body=" + _truncate_body(r.text))
        logger.error(f"Erro ao obter token: status={r.status_code} body={r.text[:200]}")
        raise ApiError(f"Erro ao obter token ({r.status_code}).")
    diag_logger.info("[net][token] 200 OK")
    diag_dump_json("token response", (r.json() if r.text else {}))
    token = r.json().get("access_token", "")
    logger.info("Token obtido com sucesso")
    return token

def get_client_tenant_id(token: str, client_id: str) -> str:
    url = f"{BASE_URL}/api/2/clients/{client_id}"
    diag_logger.info(f"[net][GET] url={url}")
    r = session.get(url, headers=base_headers(token), timeout=TIMEOUT, verify=VERIFY_SSL)
    diag_logger.info(f"[net][GET] status={r.status_code}")
    try:
        diag_dump_json("GET response", (r.json() if r.text else {}))
    except Exception:
        diag_logger.info("[net][GET] non-JSON body: " + _truncate_body(r.text))
    if r.status_code != 200:
        raise ApiError(f"Erro em /clients/{client_id} ({r.status_code}): {r.text[:200]}")
    js = r.json() if r.text else {}
    tid = js.get("tenant_id") or js.get("tenantId")
    if not tid:
        data = js.get("data") or {}
        tid = data.get("tenant_id") or data.get("tenantId")
    if not tid:
        raise ApiError("Resposta de /clients não contém tenant_id")
    logger.info("/clients/{client_id} OK; tenant raiz: " + str(tid))
    return tid

def list_subtenants(token: str, parent_id: str, limit: int = 500) -> List[dict]:
    logger.debug(f"Listando subtenants de {parent_id} limit={limit}")
    if not parent_id:
        return []
    url = f"{BASE_URL}/api/2/tenants"
    params = {"parent_id": parent_id, "limit": limit}
    diag_logger.info(f"[net][GET] url={url} params={params}")
    r = session.get(url, headers=base_headers(token), params=params, timeout=TIMEOUT, verify=VERIFY_SSL)
    diag_logger.info(f"[net][GET] status={r.status_code}")
    try:
        diag_dump_json("GET response", (r.json() if r.text else {}))
    except Exception:
        diag_logger.info("[net][GET] non-JSON body: " + _truncate_body(r.text))
    if r.status_code != 200:
        logger.error(f"Erro ao listar subtenants: status={r.status_code} body={r.text[:200]}")
        return []
    js = r.json() if r.text else {}
    return js.get("items") or js.get("tenants") or []

def headers_with_tenant(token: str, tenant_id: str) -> dict:
    h = base_headers(token)
    if tenant_id:
        h["X-Apigw-Tenant-Id"] = tenant_id
    return h

def fetch_tenant_name(token: str, tenant_id: str) -> str:
    if not tenant_id:
        return ""
    if tenant_id in tenant_name_cache:
        return tenant_name_cache[tenant_id]
    url = f"{BASE_URL}/api/2/tenants/{tenant_id}"
    diag_logger.info(f"[net][GET] url={url}")
    r = session.get(url, headers=base_headers(token), timeout=TIMEOUT, verify=VERIFY_SSL)
    diag_logger.info(f"[net][GET] status={r.status_code}")
    try:
        diag_dump_json("GET response", (r.json() if r.text else {}))
    except Exception:
        diag_logger.info("[net][GET] non-JSON body: " + _truncate_body(r.text))
    if r.status_code == 200:
        j = r.json() if r.text else {}
        diag_dump_json("GET response", j)
        name = (
            (j.get("name") if isinstance(j, dict) else None)
            or (j.get("displayName") if isinstance(j, dict) else None)
            or ((j.get("tenant") or {}).get("name") if isinstance(j, dict) else None)
            or ""
        )
        if name:
            tenant_name_cache[tenant_id] = name
            return name
    tenant_name_cache[tenant_id] = tenant_id
    return tenant_id

def fetch_workload_name(token: str, workload_id: str) -> str:
    if not workload_id:
        return ""
    url = f"{BASE_URL}/api/workload_management/v5/workloads/{workload_id}"
    diag_logger.info(f"[net][GET] url={url}")
    r = session.get(url, headers=base_headers(token), timeout=TIMEOUT, verify=VERIFY_SSL)
    diag_logger.info(f"[net][GET] status={r.status_code}")
    try:
        diag_dump_json("GET response", (r.json() if r.text else {}))
    except Exception:
        diag_logger.info("[net][GET] non-JSON body: " + _truncate_body(r.text))
    if r.status_code == 200:
        _ = r.json() if r.text else {}
    return ""

def fetch_plan_name_by_ids(token: str, policy_id: str = "", plan_id: str = "") -> str:
    try:
        if policy_id:
            url_p = f"{BASE_URL}/api/resource_management/v2/policies/{policy_id}"
            diag_logger.info(f"[net][GET] url={url_p}")
            r = session.get(url_p, headers=base_headers(token), timeout=TIMEOUT, verify=VERIFY_SSL)
            diag_logger.info(f"[net][GET] status={r.status_code}")
            try:
                diag_dump_json("GET response", (r.json() if r.text else {}))
            except Exception:
                diag_logger.info("[net][GET] non-JSON body: " + _truncate_body(r.text))
        if plan_id:
            url_pl = f"{BASE_URL}/api/resource_management/v2/plans/{plan_id}"
            diag_logger.info(f"[net][GET] url={url_pl}")
            r = session.get(url_pl, headers=base_headers(token), timeout=TIMEOUT, verify=VERIFY_SSL)
            diag_logger.info(f"[net][GET] status={r.status_code}")
            try:
                diag_dump_json("GET response", (r.json() if r.text else {}))
            except Exception:
                diag_logger.info("[net][GET] non-JSON body: " + _truncate_body(r.text))
    except Exception:
        pass
    return ""

def normalize_alert(token: str, a: dict) -> dict:
    det = a.get("details") or {}
    tenant = a.get("tenant") or {}
    ctx = det.get("context") or {}

    severity = a.get("severity", "")
    alert_type = a.get("type", "")
    message = det.get("message") or det.get("reason") or det.get("description") or ""
    received = a.get("receivedAt") or a.get("createdAt") or ""
    created = a.get("createdAt") or ""

    if alert_type == "NoBackupForXDays":
        days = det.get("daysPassed")
        if days is not None:
            message = f"NoBackupForXDays - {days} dias"

    client = (
        tenant.get("TenantName") or tenant.get("tenantName") or
        tenant.get("name") or tenant.get("display_name") or ""
    )
    if not client:
        tid = tenant.get("id") or tenant.get("uuid") or ""
        client = fetch_tenant_name(token, tid) if tid else ""

    client_uuid = tenant.get("uuid")
    if client_uuid:
        try:
            client2 = fetch_tenant_name(token, client_uuid)
            if client2:
                client = client2
        except Exception:
            pass

    workload_name = det.get("machineName") or det.get("resourceName") or ctx.get("resource_name") or ""
    if not workload_name:
        workload_id = det.get("workloadId") or det.get("resourceId") or ctx.get("resource_id") or ctx.get("workload_id") or ""
        if workload_id:
            try:
                workload_name = fetch_workload_name(token, workload_id)
            except Exception:
                workload_name = ""

    plan_name = det.get("protectionPlanName") or det.get("planName") or ctx.get("plan_name") or ""
    if not plan_name:
        policy_id = det.get("policyId") or ctx.get("policy_id") or ""
        plan_id = det.get("planId") or ctx.get("plan_id") or det.get("protectionPlanId") or ""
        try:
            plan_name = fetch_plan_name_by_ids(token, policy_id, plan_id)
        except Exception:
            plan_name = ""

    return {
        "id": a.get("id") or "",
        "tenant_id": tenant.get("id") or tenant.get("uuid") or "",
        "tenant_name": client or "",
        "severity": severity or "",
        "alert_type": alert_type or "",
        "message": message or "",
        "workload_name": workload_name or "",
        "plan_name": plan_name or "",
        "received_at": received or "",
        "created_at": created or "",
        "_raw": a,
    }

# =============================
# Camada de Dados (SQLAlchemy)
# =============================
class Database:
    def __init__(self, url: str):
        self.url = url
        self.engine: Engine = create_engine(url, future=True)
        if self.engine.dialect.name == "sqlite":
            from sqlalchemy import event
            @event.listens_for(self.engine, "connect")
            def set_sqlite_pragma(dbapi_connection, connection_record):
                cur = dbapi_connection.cursor()
                cur.execute("PRAGMA journal_mode=WAL;")
                cur.execute("PRAGMA synchronous=NORMAL;")
                cur.execute("PRAGMA foreign_keys=ON;")
                cur.close()

        self.meta = MetaData()

        self.tenants = Table(
            "tenants", self.meta,
            Column("id", String, primary_key=True),
            Column("crm", String),
            Column("name", String)
        )
        self.alerts = Table(
            "alerts", self.meta,
            Column("id", String, primary_key=True),
            Column("tenant_id", String, ForeignKey("tenants.id"), nullable=True),
            Column("crm", String),
            Column("tenant_name", String),
            Column("severity", String),
            Column("alert_type", String),
            Column("message", Text),
            Column("workload_name", String),
            Column("plan_name", String),
            Column("received_at", String),
            Column("created_at", String),
            Column("json_raw", Text),
            Column("first_seen_at", String),
            Column("last_seen_at", String),
        )
        self.fetch_log = Table(
            "fetch_log", self.meta,
            Column("tenant_id", String, primary_key=True),
            Column("endpoint", String, primary_key=True),
            Column("fetched_at", String),
        )
        self.http_tags = Table(
            "http_tags", self.meta,
            Column("tenant_id", String, primary_key=True),
            Column("endpoint", String, primary_key=True),
            Column("etag", String),
            Column("last_modified", String),
        )

        Index("idx_alerts_tenant", self.alerts.c.tenant_id)
        Index("idx_alerts_received", self.alerts.c.received_at)
        Index("idx_alerts_severity", self.alerts.c.severity)
        Index("idx_alerts_type", self.alerts.c.alert_type)

        self.meta.create_all(self.engine)

    def _upsert(self, table: Table, values: dict, conflict_cols: List[str], update_cols: Optional[List[str]] = None):
        dname = self.engine.dialect.name
        if dname == "postgresql" and pg_insert is not None:
            stmt = pg_insert(table).values(**values)
            if update_cols:
                stmt = stmt.on_conflict_do_update(
                    index_elements=[table.c[c] for c in conflict_cols],
                    set_={c: stmt.excluded.c[c] if hasattr(stmt.excluded, c) else values[c] for c in update_cols if c in values}
                )
            else:
                stmt = stmt.on_conflict_do_nothing(index_elements=[table.c[c] for c in conflict_cols])
        elif dname == "mysql" and my_insert is not None:
            stmt = my_insert(table).values(**values)
            if update_cols:
                stmt = stmt.on_duplicate_key_update({c: values[c] for c in update_cols if c in values})
        else:
            try:
                stmt = sqlite_insert(table).values(**values)
                if update_cols:
                    stmt = stmt.on_conflict_do_update(
                        index_elements=[table.c[c] for c in conflict_cols],
                        set_={c: values[c] for c in update_cols if c in values}
                    )
                else:
                    stmt = stmt.on_conflict_do_nothing(index_elements=[table.c[c] for c in conflict_cols])
            except Exception:
                with self.engine.begin() as conn:
                    where = [table.c[c] == values[c] for c in conflict_cols if c in values]
                    upd = table.update().where(*where).values(**values)
                    res = conn.execute(upd)
                    if res.rowcount == 0:
                        ins = table.insert().values(**values)
                        conn.execute(ins)
                    return
        with self.engine.begin() as conn:
            conn.execute(stmt)

    def upsert_tenant(self, tid: str, name: str):
        self._upsert(self.tenants, {"id": tid, "name": name or tid}, conflict_cols=["id"], update_cols=["name"])

    def upsert_alert(self, a: dict):
        now_iso = datetime.utcnow().isoformat()
        values = {
            "id": a.get("id"),
            "tenant_id": a.get("tenant_id"),
            "tenant_name": a.get("tenant_name"),
            "severity": a.get("severity"),
            "alert_type": a.get("alert_type"),
            "message": a.get("message"),
            "workload_name": a.get("workload_name"),
            "plan_name": a.get("plan_name"),
            "received_at": a.get("received_at"),
            "created_at": a.get("created_at"),
            "json_raw": json.dumps(a.get("_raw", {}), ensure_ascii=False),
            "first_seen_at": now_iso,
            "last_seen_at": now_iso,
        }
        self._upsert(self.alerts, values, conflict_cols=["id"], update_cols=[
            "tenant_id","tenant_name","severity","alert_type","message","workload_name",
            "plan_name","received_at","created_at","json_raw","last_seen_at"
        ])

    def mark_fetch(self, tenant_id: str, endpoint: str):
        self._upsert(self.fetch_log, {
            "tenant_id": tenant_id, "endpoint": endpoint, "fetched_at": datetime.utcnow().isoformat()
        }, conflict_cols=["tenant_id","endpoint"], update_cols=["fetched_at"])

    def get_last_fetch(self, tenant_id: str, endpoint: str) -> Optional[datetime]:
        with self.engine.connect() as conn:
            res = conn.execute(
                select(self.fetch_log.c.fetched_at).where(
                    self.fetch_log.c.tenant_id == tenant_id, self.fetch_log.c.endpoint == endpoint
                )
            ).first()
            if res and res[0]:
                try:
                    return datetime.fromisoformat(res[0])
                except Exception:
                    return None
        return None

    def get_http_tags(self, tenant_id: str, endpoint: str) -> Tuple[Optional[str], Optional[str]]:
        with self.engine.connect() as conn:
            res = conn.execute(
                select(self.http_tags.c.etag, self.http_tags.c.last_modified).where(
                    self.http_tags.c.tenant_id == tenant_id, self.http_tags.c.endpoint == endpoint
                )
            ).first()
            return (res[0], res[1]) if res else (None, None)

    def set_http_tags(self, tenant_id: str, endpoint: str, etag: Optional[str], last_modified: Optional[str]):
        self._upsert(self.http_tags, {
            "tenant_id": tenant_id, "endpoint": endpoint, "etag": etag, "last_modified": last_modified
        }, conflict_cols=["tenant_id","endpoint"], update_cols=["etag","last_modified"])

    def query_alerts(self, where: str = "", params: dict = {}, order_by: str = "received_at DESC", limit: Optional[int] = None, offset: int = 0) -> List[dict]:
        base_sql = "SELECT id, tenant_id, tenant_name, severity, alert_type, message, workload_name, plan_name, received_at FROM alerts"
        if where:
            base_sql += f" WHERE {where}"
        if order_by:
            base_sql += f" ORDER BY {order_by}"
        if limit is not None:
            base_sql += f" LIMIT {int(limit)} OFFSET {int(offset)}"
        with self.engine.connect() as conn:
            res = conn.execute(text(base_sql), params or {}).all()
        items = []
        for row in res:
            d = dict(row._mapping)
            items.append({
                "Severidade": d.get("severity") or "",
                "Tipo do alerta": d.get("alert_type") or "",
                "Mensagem": d.get("message") or "",
                "Carga de trabalho": d.get("workload_name") or "",
                "Cliente": d.get("tenant_name") or "",
                "Data e hora": fmt_brt(d.get("received_at")),
                "Plano": d.get("plan_name") or "",
                "_id": d.get("id"),
                "_tenant_id": d.get("tenant_id"),
            })
        return items

    def count_by(self, col: str, where: str = "", params: dict = {}) -> List[Tuple[str, int]]:
        base_sql = f"SELECT {col} as k, COUNT(*) as c FROM alerts"
        if where:
            base_sql += f" WHERE {where}"
        base_sql += " GROUP BY k ORDER BY c DESC"
        with self.engine.connect() as conn:
            res = conn.execute(text(base_sql), params or {}).all()
        return [(row._mapping["k"] or "—", int(row._mapping["c"])) for row in res]

    def total(self, where: str = "", params: dict = {}) -> int:
        base_sql = "SELECT COUNT(*) as c FROM alerts"
        if where:
            base_sql += f" WHERE {where}"
        with self.engine.connect() as conn:
            res = conn.execute(text(base_sql), params or {}).first()
        return int(res[0]) if res and res[0] is not None else 0

# =============================
# Network: Alerts + paginação
# =============================
def fetch_alerts_network(token: str, tenant_id: str, limit: int = DEFAULT_LIMIT, etag: Optional[str] = None, last_modified: Optional[str] = None) -> Tuple[List[dict], Optional[str], Optional[str]]:
    logger.debug(f"Buscando alerts para tenant={tenant_id} limit={limit} etag={etag} last_modified={last_modified}")
    endpoint = "/api/alert_manager/v1/alerts"
    url = f"{BASE_URL}{endpoint}"
    params = {"limit": limit}

    headers = headers_with_tenant(token, tenant_id)
    if etag:
        headers["If-None-Match"] = etag
    if last_modified:
        headers["If-Modified-Since"] = last_modified

    all_items: List[dict] = []
    seen = set()
    page_count = 0
    next_url = url
    next_params = dict(params)

    while True:
        diag_logger.info(f"[net][alerts] GET {next_url} params={next_params} headers={{k: (v if k not in ('Authorization',) else _redact(v)) for k,v in headers.items()}}")
        r = session.get(next_url, headers=headers, params=next_params, timeout=TIMEOUT, verify=VERIFY_SSL)
        diag_logger.info(f"[net][alerts] status={r.status_code}")
        try:
            diag_dump_json(f"alerts page {page_count+1} raw", (r.json() if r.text else {}))
        except Exception:
            diag_logger.info("[net][alerts] non-JSON body: " + _truncate_body(r.text))
        if r.status_code == 304:
            logger.info(f"tenant={tenant_id} sem alterações (304 Not Modified)")
            return [], r.headers.get("ETag"), r.headers.get("Last-Modified")
        if r.status_code != 200:
            logger.error(f"Erro ao listar alerts tenant={tenant_id}: status={r.status_code} body={r.text[:200]}")
            raise ApiError(f"Erro ao listar alertas (tenant {tenant_id}, {r.status_code}).")

        js = r.json() if r.text else {}
        items = js.get("items") if isinstance(js, dict) else js
        items = items or []
        logger.debug(f"Página de alerts recebida: {len(items)} itens")
        for it in items:
            key = it.get("id") or json.dumps(it, sort_keys=True)
            if key not in seen:
                seen.add(key)
                all_items.append(it)

        next_token = None
        if isinstance(js, dict):
            next_token = js.get("next") or js.get("next_token") or js.get("offset") or js.get("continuation")

        page_count += 1
        logger.debug(f"page_count={page_count} next_token={next_token}")
        if not next_token or page_count >= 50:
            break

        if "next" in js and isinstance(js["next"], str) and js["next"].startswith("http"):
            next_url = js["next"]; next_params = {}
        else:
            next_params["offset"] = next_token

        headers.pop("If-None-Match", None)
        headers.pop("If-Modified-Since", None)

    return all_items, r.headers.get("ETag"), r.headers.get("Last-Modified")

# =============================
# CSV Helper
# =============================
def build_csv(rows: List[dict], sep: str = ";") -> bytes:
    import pandas as _pd
    df = _pd.DataFrame(rows, columns=[
        "Severidade", "Tipo do alerta", "Mensagem",
        "Carga de trabalho", "Cliente", "Data e hora", "Plano"
    ])
    return df.to_csv(index=False, sep=sep).encode("utf-8-sig")

# =============================
# UI Helpers
# =============================
def severity_chip(sev: str, dark: bool) -> ft.Container:
    sev = (sev or "").lower()
    if dark:
        bg_map = {
            "critical": ft.Colors.RED_900,
            "warning": ft.Colors.AMBER_800,
            "error": ft.Colors.DEEP_ORANGE_900,
        }
        txt_color = ft.Colors.WHITE
    else:
        bg_map = {
            "critical": ft.Colors.RED_100,
            "warning": ft.Colors.AMBER_100,
            "error": ft.Colors.DEEP_ORANGE_100,
        }
        txt_color = ft.Colors.BLACK
    txt = sev.capitalize() if sev else "—"
    bg = bg_map.get(sev, ft.Colors.GREY_800 if dark else ft.Colors.GREY_100)
    return ft.Container(
        content=ft.Text(txt, size=12, weight=ft.FontWeight.BOLD, color=txt_color),
        bgcolor=bg,
        padding=ft.padding.symmetric(6, 8),
        border_radius=12
    )

def severity_icon_color(sev: str, dark: bool):
    sev = (sev or "").lower()
    if dark:
        bg_map = {
            "critical": ft.Colors.RED_900,
            "warning": ft.Colors.AMBER_800,
            "error": ft.Colors.DEEP_ORANGE_900,
        }
    else:
        bg_map = {
            "critical": ft.Colors.RED_100,
            "warning": ft.Colors.AMBER_100,
            "error": ft.Colors.DEEP_ORANGE_100,
        }
    return bg_map.get(sev, ft.Colors.GREY_700 if dark else ft.Colors.GREY_400)

def kpi_card(title: str, value: str, icon: str, color=None):
    value_txt = ft.Text(value, size=18, weight=ft.FontWeight.BOLD)
    icon_ctrl = ft.Icon(icon, size=28, color=color)
    card = ft.Card(
        content=ft.Container(
            padding=16,
            content=ft.Row([
                icon_ctrl,
                ft.Column([ft.Text(title, size=12), value_txt], spacing=2),
            ], alignment=ft.MainAxisAlignment.START, vertical_alignment=ft.CrossAxisAlignment.CENTER)
        )
    )
    return card, value_txt, icon_ctrl

# =============================
# App
# =============================

def recolor_kpi_icons():
    try:
        pass
    except Exception:
        pass

def main(page: ft.Page):
    page.title = "Painel de Alertas — Cloud Backup (HOS) v1.0 | Desenvolvido por Cristianfer Brand"
    page.window_width = 1300
    page.window_height = 850
    page.horizontal_alignment = "stretch"
    page.vertical_alignment = "start"
    page.theme_mode = ft.ThemeMode.DARK

    # === Helpers push/pop view (como no exemplo) ===
    def push_view(view: ft.View):
        page.views.append(view)
        page.update()

    def pop_view():
        try:
            if len(page.views) > 1:
                page.views.pop()
                page.update()
        except Exception:
            pass

    # === Carregar configurações (JSON) e aplicar defaults/overrides ===
    settings = load_settings()

    # Aplicar em variáveis globais para afetar chamadas subsequentes
    global BASE_URL, CLIENT_ID, CLIENT_SECRET, DATABASE_URL, CACHE_TTL_SECONDS, CACHE_MODE, VERIFY_SSL, PAGE_SIZE_DEFAULT, DEFAULT_LIMIT
    if (settings.get("BASE_URL") or "").strip():
        BASE_URL = str(settings["BASE_URL"]).rstrip("/")
    if (settings.get("CLIENT_ID") or "").strip():
        CLIENT_ID = settings["CLIENT_ID"]
    if (settings.get("CLIENT_SECRET") or "").strip():
        CLIENT_SECRET = settings["CLIENT_SECRET"]
    if (settings.get("DATABASE_URL") or "").strip():
        DATABASE_URL = settings["DATABASE_URL"]
    CACHE_TTL_SECONDS = int(settings.get("CACHE_TTL_SECONDS", CACHE_TTL_SECONDS))
    CACHE_MODE = str(settings.get("CACHE_MODE", CACHE_MODE)).lower()
    VERIFY_SSL = bool(settings.get("VERIFY_SSL", VERIFY_SSL))
    PAGE_SIZE_DEFAULT = int(settings.get("PAGE_SIZE", PAGE_SIZE_DEFAULT))
    # Novo: aplicar ALERTS_LIMIT das configurações
    DEFAULT_LIMIT = int(settings.get("ALERTS_LIMIT", DEFAULT_LIMIT))

    # Liga logger em arquivo se solicitado (governado por "Gerar Log")
    if bool(settings.get("LOG_TO_FILE", False)):
        try:
            start_diagnostic_logger()
        except Exception:
            pass

    # DB init
    db = Database(DATABASE_URL)
    logger.info(f"DB conectado: {DATABASE_URL}")

    # Topbar / Estado
    last_status = ft.Text("Inicializando...", size=12)
    last_auto_lbl = ft.Text("Última auto-atualização: —", size=12)
    theme_switch = ft.Switch(label="Tema escuro", value=True)

    def on_theme_change(e):
        page.theme_mode = ft.ThemeMode.DARK if theme_switch.value else ft.ThemeMode.LIGHT
        fill_table_from_db()
        recolor_kpi_icons()
        page.update()
    theme_switch.on_change = on_theme_change

    refresh_btn = ft.ElevatedButton("Atualizar", icon=ft.Icons.REFRESH)
    export_btn = ft.ElevatedButton("Exportar CSV", icon=ft.Icons.DOWNLOAD, disabled=True)
    limit_slider = ft.Slider(min=50, max=1000, value=DEFAULT_LIMIT, divisions=19, label="{value}", width=260)
    ttl_slider = ft.Slider(min=60, max=3600, value=CACHE_TTL_SECONDS, divisions=59, label="TTL {value}s", width=260)
    def _on_ttl_change(e):
        try:
            if auto_task["running"]:
                set_status(build_auto_status())
        except Exception:
            pass
    ttl_slider.on_change = _on_ttl_change
    auto_switch = ft.Switch(label="Auto", value=False, tooltip="Ativa atualização automática (respeita TTL & ETag)")
    auto_interval = ft.TextField(label="Intervalo (s)", value="300", width=140,
                                 input_filter=ft.NumbersOnlyInputFilter(),
                                 tooltip="Intervalo entre atualizações automáticas. Mínimo 5s.")

    # Restaurar preferências (auto)
    try:
        saved_auto = page.client_storage.get("auto_on")
        if isinstance(saved_auto, bool):
            auto_switch.value = saved_auto
        saved_int = page.client_storage.get("auto_interval")
        if saved_int:
            auto_interval.value = str(_clamp_interval(str(saved_int)))
    except Exception:
        pass

    search_tf = ft.TextField(label="Buscar (mensagem, cliente, workload, plano)", width=480, prefix_icon=ft.Icons.SEARCH)
    tipo_alerta_dd = ft.Dropdown(label="Tipo do alerta", width=320, options=[ft.dropdown.Option("Todos")], value="Todos")
    order_dd = ft.Dropdown(label="Ordenar por", width=220, options=[
        ft.dropdown.Option("Data desc"),
        ft.dropdown.Option("Data asc"),
        ft.dropdown.Option("Severidade"),
        ft.dropdown.Option("Cliente"),
        ft.dropdown.Option("Tipo do alerta"),
    ], value="Data desc")
    page_size_dd = ft.Dropdown(
        label="Por página",
        width=140,
        options=[ft.dropdown.Option(str(n)) for n in (25, 50, 100, 200, 400, 600, 800)],
        value=str(PAGE_SIZE_DEFAULT)
    )
    kpi_total, kpi_total_val, _ = kpi_card("Total (filtro)", "0", ft.Icons.LIST_ALT)
    kpi_clients, kpi_clients_val, _ = kpi_card("Clientes", "0", ft.Icons.APARTMENT)
    kpi_crit, kpi_crit_val, kpi_crit_icon = kpi_card("Critical", "0", ft.Icons.ERROR, color=severity_icon_color("critical", page.theme_mode == ft.ThemeMode.DARK))
    kpi_warn, kpi_warn_val, kpi_warn_icon = kpi_card("Warning", "0", ft.Icons.WARNING, color=severity_icon_color("warning", page.theme_mode == ft.ThemeMode.DARK))
    kpi_err, kpi_err_val, kpi_err_icon = kpi_card("Error", "0", ft.Icons.REPORT, color=severity_icon_color("error", page.theme_mode == ft.ThemeMode.DARK))
    kpi_other, kpi_other_val, kpi_other_icon = kpi_card("Outros", "0", ft.Icons.MORE_HORIZ, color=severity_icon_color("other", page.theme_mode == ft.ThemeMode.DARK))

    columns = [
        ft.DataColumn(ft.Text("Severidade")),
        ft.DataColumn(ft.Text("Tipo do alerta")),
        ft.DataColumn(ft.Text("Mensagem")),
        ft.DataColumn(ft.Text("Carga de trabalho")),
        ft.DataColumn(ft.Text("Cliente")),
        ft.DataColumn(ft.Text("Data e hora")),
        ft.DataColumn(ft.Text("Plano")),
    ]
    table = ft.DataTable(columns=columns, rows=[], column_spacing=14, data_row_max_height=88, heading_text_style=ft.TextStyle(weight=ft.FontWeight.BOLD))

    page_lbl = ft.Text("Página 1")
    prev_btn = ft.IconButton(ft.Icons.CHEVRON_LEFT)
    next_btn = ft.IconButton(ft.Icons.CHEVRON_RIGHT)

    progress = ft.ProgressBar(width=260, visible=False)

    # Garante diretório de logs
    try:
        (BASE_DIR / DIAG_DIR).resolve().mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

    state = {
        "token": "",
        "me_tid": "",
        "subs": [],
        "order": "received_at DESC",
        "page_size": PAGE_SIZE_DEFAULT,
        "page_index": 0,
        "where": "",
        "params": (),
        "cached_rows_total": 0,
        "cache_mode": CACHE_MODE,
    }

    def set_status(msg: str):
        last_status.value = msg
        page.update()

    def calc_next_network_eta(ttl_s: int) -> int:
        try:
            tenants = []
            if state.get("me_tid"):
                tenants.append(state["me_tid"])
            tenants = [t for t in dict.fromkeys(tenants) if t]
            if not tenants:
                return 0
            from datetime import datetime as _dt
            remain_list = []
            for tid in tenants:
                last = db.get_last_fetch(tid, "/api/alert_manager/v1/alerts")
                if not last:
                    remain_list.append(0)
                else:
                    elapsed = (_dt.utcnow() - last).total_seconds()
                    remain = max(0, int(ttl_s - elapsed))
                    remain_list.append(remain)
            return min(remain_list) if remain_list else 0
        except Exception:
            return 0

    def build_auto_status() -> str:
        try:
            iv = _clamp_interval(auto_interval.value or "300")
        except Exception:
            iv = 300
        try:
            ttl = int(ttl_slider.value or CACHE_TTL_SECONDS)
        except Exception:
            ttl = CACHE_TTL_SECONDS
        eta = calc_next_network_eta(ttl)
        def _fmt_eta(v:int)->str:
            if v <= 0:
                return "agora"
            h = v // 3600; rem = v % 3600; m = rem // 60; s = rem % 60
            if h > 0:
                return f"{h:d}:{m:02d}:{s:02d}"
            return f"{m:02d}:{s:02d}"
        eta_txt = _fmt_eta(eta)
        return f"Auto ON — intervalo {iv}s · próxima chamada de rede em {eta_txt} (TTL restante)"

    def set_progress(on: bool):
        progress.visible = on
        page.update()

    def apply_filters_build_where() -> Tuple[str, dict]:
        wheres = []
        params = {}

        tipo_sel = (tipo_alerta_dd.value or "Todos").strip()
        if tipo_sel != "Todos":
            wheres.append("alert_type = :tipo")
            params["tipo"] = tipo_sel

        q = (search_tf.value or "").strip()
        if q:
            like = f"%{q.lower()}%"
            wheres.append("(lower(message) LIKE :q OR lower(tenant_name) LIKE :q OR lower(workload_name) LIKE :q OR lower(plan_name) LIKE :q)")
            params["q"] = like

        where = " AND ".join(wheres)
        return where, params

    def rebuild_tipos_options():
        with db.engine.connect() as conn:
            res = conn.execute(text("SELECT DISTINCT alert_type FROM alerts ORDER BY alert_type")).all()
        tipos = ["Todos"]
        tipos.extend([row[0] for row in res if row[0]])
        tipo_alerta_dd.options = [ft.dropdown.Option(t) for t in tipos]
        if tipo_alerta_dd.value not in tipos:
            tipo_alerta_dd.value = "Todos"

    def update_kpis(where: str, params: Tuple):
        total = db.total(where, params)
        by_client = db.count_by("tenant_name", where, params)
        by_sev = db.count_by("lower(severity)", where, params)
        sev_map = {k: v for k, v in by_sev}

        kpi_total_val.value = str(total)
        kpi_clients_val.value = str(len(by_client))
        kpi_crit_val.value = str(sev_map.get("critical", 0))
        kpi_warn_val.value = str(sev_map.get("warning", 0))
        kpi_err_val.value = str(sev_map.get("error", 0))
        outros = sum(v for k, v in sev_map.items() if k not in {"critical", "warning", "error"})
        kpi_other_val.value = str(outros)
        page.update()

    def order_to_sql(order_label: str) -> str:
        mapping = {
            "Data desc": "received_at DESC",
            "Data asc": "received_at ASC",
            "Severidade": "severity ASC, received_at DESC",
            "Cliente": "tenant_name ASC, received_at DESC",
            "Tipo do alerta": "alert_type ASC, received_at DESC",
        }
        return mapping.get(order_label, "received_at DESC")

    def fill_table_from_db():
        where, params = apply_filters_build_where()
        state["where"], state["params"] = where, params
        state["page_size"] = int(page_size_dd.value or PAGE_SIZE_DEFAULT)
        state["order"] = order_to_sql(order_dd.value or "Data desc")

        total = db.total(where, params)
        state["cached_rows_total"] = total

        max_page = max(0, (total - 1) // state["page_size"])
        if state["page_index"] > max_page:
            state["page_index"] = max_page

        offset = state["page_index"] * state["page_size"]
        rows = db.query_alerts(where, params, state["order"], limit=state["page_size"], offset=offset)

        table.rows = []
        for r in rows:
            table.rows.append(ft.DataRow(cells=[
                ft.DataCell(severity_chip(r.get("Severidade",""), page.theme_mode == ft.ThemeMode.DARK)),
                ft.DataCell(ft.Text(r.get("Tipo do alerta",""))),
                ft.DataCell(ft.Text(r.get("Mensagem",""), selectable=True)),
                ft.DataCell(ft.Text(r.get("Carga de trabalho",""))),
                ft.DataCell(ft.Text(r.get("Cliente",""))),
                ft.DataCell(ft.Text(r.get("Data e hora",""))),
                ft.DataCell(ft.Text(r.get("Plano",""))),
            ]))
        page.update()

        update_kpis(where, params)

        if total == 0:
            page_lbl.value = "Sem resultados"
        else:
            page_lbl.value = f"Página {state['page_index']+1} de {max_page+1} — {total} registros"
        export_btn.disabled = (total == 0)
        page.update()

    def on_any_filter_change(e):
        state["page_index"] = 0
        fill_table_from_db()
    search_tf.on_submit = on_any_filter_change
    tipo_alerta_dd.on_change = on_any_filter_change
    order_dd.on_change = on_any_filter_change
    page_size_dd.on_change = on_any_filter_change

    def on_prev(e):
        if state["page_index"] > 0:
            state["page_index"] -= 1
            fill_table_from_db()

    def on_next(e):
        max_page = max(0, (state["cached_rows_total"] - 1) // state["page_size"])
        if state["page_index"] < max_page:
            state["page_index"] += 1
            fill_table_from_db()

    prev_btn.on_click = on_prev
    next_btn.on_click = on_next

    def export_current_view(e):
        where, params = state["where"], state["params"]
        order = state["order"]

        out_dir = (BASE_DIR / "export"); out_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = out_dir / f"alerts_export_{ts}.csv"

        with open(csv_path, "wb") as f:
            header = ("Severidade;Tipo do alerta;Mensagem;Carga de trabalho;Cliente;Data e hora;Plano\n").encode("utf-8-sig")
            f.write(header)

        batch = 2000; offset = 0
        while True:
            part = db.query_alerts(where, params, order, limit=batch, offset=offset)
            if not part:
                break
            csv_bytes = build_csv(part, sep=";")
            lines = csv_bytes.decode("utf-8-sig").splitlines(True)
            if offset > 0 and lines:
                lines = lines[1:]
            with open(csv_path, "ab") as f:
                f.write("".join(lines).encode("utf-8-sig"))
            offset += batch
            if offset >= MAX_ROWS:
                break

        page.snack_bar = ft.SnackBar(ft.Text(f"CSV salvo: {csv_path}"), open=True)
        page.update()

    export_btn.on_click = export_current_view

    # "Gerar Log" (agora somente dentro de Configurações)
    logs_switch = ft.Switch(label="Gerar Log", value=bool(settings.get("LOG_TO_FILE", False)),
                            tooltip="Gravar diagnósticos em arquivo ./logs")

    def on_logs_switch(e):
        try:
            if logs_switch.value:
                start_diagnostic_logger()
                pth = diag_current_log_path()
                page.snack_bar = ft.SnackBar(ft.Text(f"Logs em arquivo: {pth}"), open=True)
                settings["LOG_TO_FILE"] = True
                save_settings(settings)
            else:
                stop_diagnostic_logger()
                page.snack_bar = ft.SnackBar(ft.Text("Logs em arquivo: desligado"), open=True)
                settings["LOG_TO_FILE"] = False
                save_settings(settings)
        except Exception as ex:
            page.snack_bar = ft.SnackBar(ft.Text(f"Erro ao alternar logs: {ex}"), open=True)
        page.update()
    logs_switch.on_change = on_logs_switch

    # ======================
    # Fluxo de atualização
    # ======================
    def should_refresh(tenant_id: str, endpoint: str, ttl_seconds: int) -> bool:
        last = db.get_last_fetch(tenant_id, endpoint)
        if not last:
            return True
        return (datetime.utcnow() - last) > timedelta(seconds=ttl_seconds)

    def upsert_many_alerts(token: str, tenant_id: str, raw_alerts: List[dict]):
        tname = fetch_tenant_name(token, tenant_id)
        db.upsert_tenant(tenant_id, tname)

        for a in raw_alerts:
            na = normalize_alert(token, a)

            na_tid = (na.get("tenant_id") or tenant_id or "").strip()
            na_tname = (na.get("tenant_name") or tname or "").strip()

            if na_tid:
                na["tenant_id"] = na_tid
                na["tenant_name"] = na_tname or na_tid
                try:
                    db.upsert_tenant(na_tid, na["tenant_name"])
                except Exception as ex:
                    logger.debug(f"upsert_tenant falhou para tenant={na_tid}: {ex}")
            else:
                na["tenant_id"] = None
                na["tenant_name"] = na_tname or ""

            try:
                db.upsert_alert(na)
            except IntegrityError as ex:
                logger.debug(f"FK ao inserir alert id={na.get('id')} tenant_id={na.get('tenant_id')} -> tentando resolver: {ex}")
                if na.get("tenant_id"):
                    try:
                        db.upsert_tenant(na["tenant_id"], na.get("tenant_name") or na["tenant_id"])
                        db.upsert_alert(na)
                    except Exception as ex2:
                        logger.debug(f"Reinserção falhou para alert id={na.get('id')}: {ex2}")
                        raise
                else:
                    raise

    def fetch_and_cache_for_tenant(token: str, tenant_id: str, limit: int):
        endpoint = "/api/alert_manager/v1/alerts"
        etag, last_mod = db.get_http_tags(tenant_id, endpoint)
        items, new_etag, new_lastmod = fetch_alerts_network(token, tenant_id, limit=limit, etag=etag, last_modified=last_mod)
        if items:
            upsert_many_alerts(token, tenant_id, items)
        if (new_etag is not None) or (new_lastmod is not None):
            try:
                diag_logger.info(f"[tags] set {tenant_id} {endpoint} etag={_redact(new_etag)!r} last_mod={_redact(new_lastmod,6)!r}")
            except Exception:
                pass
            db.set_http_tags(tenant_id, endpoint, new_etag, new_lastmod)
        try:
            diag_logger.info(f"[fetch_log] set {tenant_id} {endpoint} now")
        except Exception:
            pass
        db.mark_fetch(tenant_id, endpoint)

    def initial_auth():
        set_progress(True)
        try:
            token = get_token()
            my_tid = get_client_tenant_id(token, CLIENT_ID)
            subs = list_subtenants(token, my_tid) if my_tid else []
            state["token"] = token
            state["me_tid"] = my_tid
            state["subs"] = [t.get("id") or t.get("uuid") for t in subs if (t.get("id") or t.get("uuid"))]
            set_status(f"Autenticado. Tenant raiz: {my_tid}. Subtenants: {len(state['subs'])}")
        except Exception as e:
            set_status(f"Erro de autenticação/listagem: {e}")
        finally:
            set_progress(False)

    def refresh_data(e=None):
        set_progress(True)
        token = state.get("token") or ""
        if not token:
            set_status("Não autenticado. Tentando autenticar...")
            initial_auth()
            token = state.get("token") or ""
            if not token:
                set_progress(False)
                return

        tenants = [state["me_tid"]] if state["me_tid"] else []
        tenants = [t for t in dict.fromkeys(tenants) if t]

        limit = int(limit_slider.value or DEFAULT_LIMIT)
        ttl = int(ttl_slider.value or CACHE_TTL_SECONDS)

        fetched_any = False
        for tid in tenants or [state["me_tid"]]:
            try:
                need = should_refresh(tid, "/api/alert_manager/v1/alerts", ttl)
                if state["cache_mode"] == "network-first" or need:
                    fetch_and_cache_for_tenant(token, tid, limit)
                    fetched_any = True
            except Exception as ex_tenant:
                set_status(f"Falha ao atualizar tenant {tid}: {ex_tenant}")

        rebuild_tipos_options()
        fill_table_from_db()
        set_status(f"Atualizado em {datetime.now().strftime('%d/%m/%Y %H:%M:%S')} — {'network' if fetched_any else 'cache'}")
        set_progress(False)

    refresh_btn.on_click = refresh_data

    def _clamp_interval(val: str) -> int:
        try:
            v = int(val)
        except Exception:
            v = 300
        if v < 5:
            v = 5
        if v > 3600:
            v = 3600
        return v

    auto_task = {"running": False}
    auto_eta_task = {"running": False}

    async def auto_loop():
        try:
            diag_logger.info(f"[auto] loop start interval={auto_interval.value or '300'}")
        except Exception:
            pass
        while auto_task["running"]:
            try:
                interval = max(5, int(auto_interval.value or 300))
            except Exception:
                interval = 300
            await asyncio.sleep(interval)
            if not auto_task["running"]:
                break
            refresh_data(None)
            last_auto_lbl.value = "Última auto-atualização: " + datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            try:
                if auto_task["running"]:
                    set_status(build_auto_status())
            except Exception:
                pass
            try:
                diag_logger.info(f"[auto] tick done at {last_auto_lbl.value}")
            except Exception:
                pass

    async def eta_countdown_loop():
        try:
            while auto_task["running"] and auto_eta_task["running"]:
                try:
                    set_status(build_auto_status())
                except Exception:
                    pass
                await asyncio.sleep(1)
        except Exception:
            pass

    def on_auto_toggle(e):
        iv = _clamp_interval(auto_interval.value or "300")
        auto_interval.value = str(iv)
        try:
            page.client_storage.set("auto_on", bool(auto_switch.value))
            page.client_storage.set("auto_interval", iv)
        except Exception:
            pass
        if auto_switch.value and not auto_task["running"]:
            auto_task["running"] = True
            auto_switch.value = True
            page.run_task(auto_loop)
            auto_eta_task["running"] = True
            page.run_task(eta_countdown_loop)
            set_status(build_auto_status())
            try:
                diag_logger.info(f"[auto] ON interval={iv}")
            except Exception:
                pass
        elif not auto_switch.value and auto_task["running"]:
            auto_task["running"] = False
            auto_eta_task["running"] = False
            auto_switch.value = False
            set_status("Auto OFF")
            try:
                diag_logger.info("[auto] OFF")
            except Exception:
                pass
        page.update()

    auto_switch.on_change = on_auto_toggle

    def on_interval_change(e):
        iv = _clamp_interval(auto_interval.value or "300")
        if str(iv) != (auto_interval.value or "").strip():
            auto_interval.value = str(iv)
            page.snack_bar = ft.SnackBar(ft.Text(f"Intervalo ajustado para {iv}s (mín. 5, máx. 3600)"), open=True)
        try: page.client_storage.set("auto_interval", iv)
        except Exception: pass
        try:
            if auto_task["running"]:
                set_status(build_auto_status())
        except Exception: pass
        page.update()

    auto_interval.on_blur = on_interval_change

    def on_disconnect(e):
        auto_task["running"] = False
    page.on_disconnect = on_disconnect

    # ============= View de Configurações (ABA/TELA) =============
    def open_settings_view(_e=None):
        # campos principais
        base_url_tf = ft.TextField(label="BASE_URL", value=str(BASE_URL), width=520)
        client_id_tf = ft.TextField(label="CLIENT_ID", value=str(CLIENT_ID), width=520)
        client_secret_tf = ft.TextField(
            label="CLIENT_SECRET",
            value=str(settings.get("CLIENT_SECRET", CLIENT_SECRET)),
            password=True,
            can_reveal_password=True,
            width=520
        )
        database_url_tf = ft.TextField(
            label="DATABASE_URL",
            value=str(settings.get("DATABASE_URL", "")),
            hint_text="se vazio, usar SQLite local",
            width=520
        )
        cache_mode_dd2 = ft.Dropdown(
            label="CACHE_MODE",
            options=[ft.dropdown.Option("cache-first"), ft.dropdown.Option("network-first")],
            value=str(settings.get("CACHE_MODE", CACHE_MODE)).lower(),
            width=220
        )
        verify_ssl_sw = ft.Switch(label="VERIFY_SSL", value=bool(settings.get("VERIFY_SSL", VERIFY_SSL)))
        page_size_dd2 = ft.Dropdown(
            label="PAGE_SIZE",
            options=[ft.dropdown.Option(str(n)) for n in (25, 50, 100, 200, 400, 600, 800)],
            value=str(settings.get("PAGE_SIZE", PAGE_SIZE_DEFAULT)),
            width=220
        )

        info_txt = ft.Text(
            "Credenciais/URL/DB/SSL terão efeito total após reabrir ou usar 'Atualizar'. "
            "TTL, Qtd, Auto/Intervalo e logs aplicam imediatamente.",
            size=12, color=ft.Colors.GREY_600
        )

        # === BLOCO: Opções movidas da página principal ===
        qtd_row = ft.Row(
            [ft.Text("Qtd. (por requisição à API)"), limit_slider],
            spacing=8
        )
        ttl_row = ft.Row(
            [ft.Text("TTL (segundos)"), ttl_slider],
            spacing=8
        )
        auto_row = ft.Row(
            [auto_switch, auto_interval],
            spacing=8
        )
        logs_row = ft.Row(
            [logs_switch],
            spacing=8
        )

        def on_cancel(_ev=None):
            pop_view()

        def on_save(_ev=None):
            global CACHE_TTL_SECONDS, CACHE_MODE, VERIFY_SSL, PAGE_SIZE_DEFAULT, BASE_URL, CLIENT_ID, CLIENT_SECRET, DATABASE_URL, DEFAULT_LIMIT
            # validações e coleta (TTL via slider, Qtd via slider)
            try:
                ttl_val = int((ttl_slider.value or 300))
            except Exception:
                ttl_val = 300
            ttl_val = max(60, min(3600, ttl_val))

            mode_val = (cache_mode_dd2.value or "cache-first").strip().lower()
            if mode_val not in ("cache-first", "network-first"):
                mode_val = "cache-first"

            try:
                pg_val = int((page_size_dd2.value or "50").strip())
            except Exception:
                pg_val = 50
            if pg_val not in (25, 50, 100, 200, 400, 600, 800):
                pg_val = 50

            try:
                new_limit = int(limit_slider.value or DEFAULT_LIMIT)
            except Exception:
                new_limit = DEFAULT_LIMIT
            if new_limit < 50: new_limit = 50
            if new_limit > 1000: new_limit = 1000

            new_settings = dict(DEFAULT_SETTINGS)
            new_settings.update({
                "BASE_URL": (base_url_tf.value or "").strip() or BASE_URL,
                "CLIENT_ID": (client_id_tf.value or "").strip() or CLIENT_ID,
                "CLIENT_SECRET": (client_secret_tf.value or "").strip() or CLIENT_SECRET,
                "DATABASE_URL": (database_url_tf.value or "").strip(),
                "CACHE_TTL_SECONDS": ttl_val,
                "CACHE_MODE": mode_val,
                "VERIFY_SSL": bool(verify_ssl_sw.value),
                "PAGE_SIZE": pg_val,
                # Persiste a preferência do "Gerar Log"
                "LOG_TO_FILE": bool(logs_switch.value),
                # Novo: persistir Qtd (limite por requisição)
                "ALERTS_LIMIT": new_limit,
            })

            # persiste
            try:
                save_settings(new_settings)
                page.snack_bar = ft.SnackBar(ft.Text("Configurações salvas em ./config/settings.json"), open=True)
            except Exception as ex:
                page.snack_bar = ft.SnackBar(ft.Text(f"Erro ao salvar configurações: {ex}"), open=True)

            # aplica hot
            try:
                settings.clear()
                settings.update(new_settings)
            except Exception:
                pass

            CACHE_TTL_SECONDS = int(new_settings["CACHE_TTL_SECONDS"])
            CACHE_MODE = str(new_settings["CACHE_MODE"]).lower()
            VERIFY_SSL = bool(new_settings["VERIFY_SSL"])
            PAGE_SIZE_DEFAULT = int(new_settings["PAGE_SIZE"])
            DEFAULT_LIMIT = int(new_settings["ALERTS_LIMIT"])

            BASE_URL = str(new_settings["BASE_URL"]).rstrip("/")
            CLIENT_ID = str(new_settings["CLIENT_ID"])
            CLIENT_SECRET = str(new_settings["CLIENT_SECRET"])
            if (new_settings.get("DATABASE_URL") or "").strip():
                DATABASE_URL = new_settings["DATABASE_URL"]

            try:
                page_size_dd.value = str(PAGE_SIZE_DEFAULT)
            except Exception:
                pass
            try:
                state["cache_mode"] = CACHE_MODE
            except Exception:
                pass
            try:
                fill_table_from_db()
            except Exception:
                pass

            # aplicar logs conforme "Gerar Log"
            try:
                if bool(new_settings["LOG_TO_FILE"]):
                    start_diagnostic_logger()
                    pth = diag_current_log_path()
                    page.snack_bar = ft.SnackBar(ft.Text(f"Logs em arquivo: {pth}"), open=True)
                else:
                    stop_diagnostic_logger()
                    page.snack_bar = ft.SnackBar(ft.Text("Logs em arquivo: desligado"), open=True)
            except Exception:
                pass

            # aviso de credenciais/URL/DB/SSL
            try:
                page.snack_bar = ft.SnackBar(
                    ft.Text("Algumas alterações (BASE_URL/credenciais/DB/SSL) terão efeito total após reabrir ou clicar em Atualizar."),
                    open=True
                )
            except Exception:
                pass

            pop_view()

        view = ft.View(
            route="/settings",
            appbar=ft.AppBar(
                title=ft.Text("Configurações"),
                center_title=False,
                leading=ft.IconButton(ft.Icons.ARROW_BACK, on_click=lambda e: on_cancel())
            ),
            controls=[
                ft.Container(
                    content=ft.Column(
                        controls=[
                            base_url_tf,
                            client_id_tf,
                            client_secret_tf,
                            database_url_tf,
                            ft.Row([cache_mode_dd2, page_size_dd2, verify_ssl_sw], wrap=True, spacing=10),
                            ft.Divider(),
                            ft.Text("Preferências de atualização e diagnóstico", weight=ft.FontWeight.BOLD),
                            qtd_row,
                            ttl_row,
                            auto_row,
                            logs_row,
                            ft.Divider(),
                            info_txt,
                            ft.Row(
                                [ft.OutlinedButton("Cancelar", on_click=on_cancel),
                                 ft.FilledButton("Salvar", icon=ft.Icons.SAVE, on_click=on_save)],
                                alignment=ft.MainAxisAlignment.END
                            ),
                        ],
                        tight=True,
                        scroll=ft.ScrollMode.AUTO,
                        spacing=8,
                    ),
                    padding=20,
                    expand=True
                )
            ]
        )
        push_view(view)

    # ===== Layout principal =====
    page.add(
        ft.Column([
            ft.Row(
                [
                    ft.Text(
                        "Seja bem-vindo ao Painel de Alertas Cloud Backup (HOS)!",
                        size=20,
                        weight=ft.FontWeight.BOLD
                    ),

                    # Flex spacer para empurrar os controles da direita
                    ft.Container(expand=1),

                    # Ícone Configurações + Theme na MESMA LINHA (removido 'Gerar Log' da barra principal)
                    ft.Row(
                        [
                            ft.IconButton(
                                icon=ft.Icons.SETTINGS,
                                tooltip="Configurações",
                                on_click=lambda e: open_settings_view()
                            ),
                            theme_switch,
                        ],
                        spacing=8,
                        alignment=ft.MainAxisAlignment.END,
                        vertical_alignment=ft.CrossAxisAlignment.CENTER,
                    ),
                ],
                alignment=ft.MainAxisAlignment.START,
                vertical_alignment=ft.CrossAxisAlignment.CENTER,
            ),

            ft.Row(
                [last_status, ft.Container(width=16), last_auto_lbl],
                alignment=ft.MainAxisAlignment.SPACE_BETWEEN
            ),

            # Removido: Qtd / TTL / Auto / Intervalo da página principal
            # (agora todos dentro de Configurações)

            ft.Row(
                [
                    search_tf,
                    tipo_alerta_dd,
                    order_dd,
                    page_size_dd,
                    refresh_btn,
                    export_btn,
                ],
                spacing=8
            ),

            ft.Row(
                [
                    ft.Container(content=kpi_total, expand=1),
                    ft.Container(content=kpi_clients, expand=1),
                    ft.Container(content=kpi_crit, expand=1),
                    ft.Container(content=kpi_warn, expand=1),
                    ft.Container(content=kpi_err, expand=1),
                    ft.Container(content=kpi_other, expand=1),
                ],
                spacing=12
            ),

            progress,
            table,

            ft.Row(
                [prev_btn, page_lbl, next_btn],
                alignment=ft.MainAxisAlignment.CENTER
            ),
        ],
        expand=True,
        scroll=ft.ScrollMode.AUTO)
    )

    # Bootstrap Auto
    if auto_switch.value:
        auto_task["running"] = True
        page.run_task(auto_loop)
        auto_eta_task["running"] = True
        page.run_task(eta_countdown_loop)
        try:
            set_status(build_auto_status())
        except Exception:
            pass

    # Bootstrap dados
    initial_auth()
    rebuild_tipos_options()
    fill_table_from_db()
    set_status("Pronto. Ajuste os filtros e clique em Atualizar quando desejar.")

if __name__ == "__main__":
    ft.app(target=main)
