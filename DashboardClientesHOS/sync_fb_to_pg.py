#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Backend Firebird → PostgreSQL
------------------------------------------------------
- Firebird sempre em WIN1252 (fixo)
- PostgreSQL sempre em UTF8 (fixo, lc_messages=C)
- Mantém suporte a fbclient_path / variáveis de ambiente para localizar fbclient.dll

Uso:
  python sync_fb_to_pg.py --once
  python sync_fb_to_pg.py --watch --interval 1800
"""

from __future__ import annotations

import os
import sys
import time
import json
import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, Tuple

import pandas as pd
import psycopg2
from psycopg2.extras import DictCursor, execute_values

try:
    import fdb  # Firebird driver
except Exception:
    fdb = None

# --- Paths ---
BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = BASE_DIR / "config"
LOG_DIR = BASE_DIR / "logs"
CONFIG_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_CFG_PATH = CONFIG_DIR / "backend_sync_settings.json"
LOG_PATH = LOG_DIR / "backend_sync.log"

# --- Defaults ---
DEFAULT_SETTINGS: Dict[str, Any] = {
    "firebird": {
        "dsn": "127.0.0.1/3050:crm_hos",
        "user": "SYSDBA",
        "password": "masterkey",
        # Fixo conforme solicitado
        "charset": "WIN1252",
        # Caminho completo do fbclient.dll (opcional, mas recomendado no Windows)
        "fbclient_path": ""
    },
    "postgres": {
        "host": "127.0.0.1",
        "port": 5432,
        "dbname": "mapacliente",
        "user": "postgres",
        "password": "postgres"
    },
    "schema": "public"
}

# --- Logging ---
def setup_logging(level: str = "INFO") -> None:
    lvl = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=lvl,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOG_PATH, encoding="utf-8"),
            logging.StreamHandler(sys.stdout)
        ]
    )
    logging.info("Log iniciado. Nível: %s", level.upper())
    logging.info("Base dir: %s", BASE_DIR)

# --- Config ---
def load_settings(path: Optional[str] = None) -> Tuple[Dict[str, Any], Path]:
    cfg_path = Path(path) if path else DEFAULT_CFG_PATH
    if not cfg_path.exists():
        cfg_path.write_text(json.dumps(DEFAULT_SETTINGS, ensure_ascii=False, indent=2), encoding="utf-8")
        return dict(DEFAULT_SETTINGS), cfg_path
    try:
        data = json.loads(cfg_path.read_text(encoding="utf-8"))
    except Exception:
        data = {}
    out = dict(DEFAULT_SETTINGS)
    out["firebird"] = {**DEFAULT_SETTINGS["firebird"], **(data.get("firebird") or {})}
    out["postgres"] = {**DEFAULT_SETTINGS["postgres"], **(data.get("postgres") or {})}
    out["schema"] = data.get("schema", out["schema"])
    # Forçar charset conforme pedido
    out["firebird"]["charset"] = "WIN1252"
    return out, cfg_path

# --- Localização/Carregamento de fbclient ---
def _dll_name_candidates():
    if os.name == "nt":
        return ("fbclient.dll",)
    elif sys.platform == "darwin":
        return ("libfbclient.dylib",)
    else:
        return ("libfbclient.so", "libfbclient.so.2", "libfbclient.so.3")

def _try_load_fbclient(fbcfg: Dict[str, Any]) -> None:
    if fdb is None:
        raise RuntimeError("Driver Firebird ausente. Instale com: pip install fdb")

    # 1) Caminho explícito (config/env)
    explicit = (fbcfg.get("fbclient_path") or "").strip()
    if not explicit:
        explicit = os.environ.get("FIREBIRD_CLIENT") or os.environ.get("FBCLIENT") or ""
    if explicit:
        p = Path(explicit)
        if p.exists():
            if os.name == "nt":
                try: os.add_dll_directory(str(p.parent))
                except Exception as e: logging.debug("add_dll_directory: %s", e)
            fdb.load_api(str(p))
            logging.info("[FB] fbclient carregado: %s", p)
            return
        else:
            logging.warning("[FB] Caminho fbclient não existe: %s", explicit)

    # 2) Diretório FIREBIRD
    fb_dir = (os.environ.get("FIREBIRD") or "").strip()
    for name in _dll_name_candidates():
        if fb_dir:
            p = Path(fb_dir) / name
            if p.exists():
                if os.name == "nt":
                    try: os.add_dll_directory(str(p.parent))
                    except Exception as e: logging.debug("add_dll_directory: %s", e)
                fdb.load_api(str(p))
                logging.info("[FB] fbclient via FIREBIRD: %s", p)
                return

    # 3) Locais comuns
    if os.name == "nt":
        candidates = [
            r"C:\Program Files\Firebird\Firebird_4_0\fbclient.dll",
            r"C:\Program Files\Firebird\Firebird_3_0\fbclient.dll",
            r"C:\Program Files\Firebird\Firebird_2_5\bin\fbclient.dll",
            r"C:\Program Files (x86)\Firebird\Firebird_4_0\fbclient.dll",
            r"C:\Program Files (x86)\Firebird\Firebird_3_0\fbclient.dll",
            r"C:\Program Files (x86)\Firebird\Firebird_2_5\bin\fbclient.dll",
        ]
        for c in candidates:
            p = Path(c)
            if p.exists():
                try: os.add_dll_directory(str(p.parent))
                except Exception as e: logging.debug("add_dll_directory: %s", e)
                fdb.load_api(str(p))
                logging.info("[FB] fbclient local padrão: %s", p)
                return
    else:
        unix_dirs = ["/usr/lib", "/usr/local/lib", "/opt/firebird/lib", "/opt/firebird"]
        for d in unix_dirs:
            for n in _dll_name_candidates():
                p = Path(d) / n
                if p.exists():
                    fdb.load_api(str(p))
                    logging.info("[FB] fbclient local padrão: %s", p)
                    return

    logging.warning("[FB] fbclient não localizado automaticamente. Informe 'firebird.fbclient_path' no JSON "
                    "ou variável FIREBIRD_CLIENT com o caminho completo.")

# --- PostgreSQL UTF8 fixo ---
def pg_connect(settings: Dict[str, Any]):
    pg = settings.get("postgres") or {}
    params = dict(
        host=pg.get("host", "127.0.0.1"),
        port=int(pg.get("port", 5432)),
        dbname=pg.get("dbname", "mapacliente"),
        user=pg.get("user", "postgres"),
        password=pg.get("password", "postgres"),
        cursor_factory=DictCursor,
    )
    os.environ["PGCLIENTENCODING"] = "UTF8"
    opt = "-c lc_messages=C -c client_encoding=UTF8"
    con = psycopg2.connect(**params, connect_timeout=8, options=opt)
    try:
        con.set_client_encoding("UTF8")
    except Exception:
        pass
    con.autocommit = False
    with con.cursor() as cur:
        cur.execute("SELECT current_setting('server_encoding'), current_setting('client_encoding'), current_setting('lc_messages')")
        se, ce, lm = cur.fetchone()
    logging.info("[PG] conectado (server=%s, client=%s, lc_messages=%s)", se, ce, lm)
    return con

# --- Schema/Views no PG ---
def ensure_schema_pg(conn, schema: str = "public") -> None:
    cur = conn.cursor()
    cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS "{schema}"."sync_runs" (
        snapshot_id SERIAL PRIMARY KEY,
        snapshot_at TIMESTAMP NOT NULL,
        rows_hos INTEGER,
        rows_mod INTEGER,
        rows_rep INTEGER,
        ok INTEGER DEFAULT 1,
        message TEXT
    );
    """)
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS "{schema}"."clientes_hos_hist" (
        snapshot_id INTEGER REFERENCES "{schema}"."sync_runs"(snapshot_id),
        "CRM" TEXT, "FANTASIA" TEXT, "CNPJ" TEXT, "TELEFONE" TEXT, "CIDADE" TEXT,
        "ENDERECO" TEXT, "BAIRRO" TEXT, "CEP" TEXT, "CODIGO_IBGE" TEXT,
        "ESTADO" TEXT, "ATIVO" TEXT, "STATUS" TEXT, "DIAS_LIBERADOS" INTEGER, "MODULOS" TEXT,
        "NOME_REP" TEXT
    );
    """)
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS "{schema}"."clientes_modulos_hist" (
        snapshot_id INTEGER REFERENCES "{schema}"."sync_runs"(snapshot_id),
        "MODULO" TEXT, "QUANTIDADE" INTEGER
    );
    """)
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS "{schema}"."clientes_rep_hist" (
        snapshot_id INTEGER REFERENCES "{schema}"."sync_runs"(snapshot_id),
        "COD_REP" TEXT, "NOME_REP" TEXT, "QTD_CLIENTES" INTEGER
    );
    """)
    cur.execute(f'CREATE INDEX IF NOT EXISTS idx_hos_ibge  ON "{schema}"."clientes_hos_hist" ("CODIGO_IBGE");')
    cur.execute(f'CREATE INDEX IF NOT EXISTS idx_hos_uf    ON "{schema}"."clientes_hos_hist" ("ESTADO");')
    cur.execute(f'CREATE INDEX IF NOT EXISTS idx_mod_nome  ON "{schema}"."clientes_modulos_hist" ("MODULO");')
    cur.execute(f'CREATE INDEX IF NOT EXISTS idx_rep_nome  ON "{schema}"."clientes_rep_hist" ("NOME_REP");')
    conn.commit()
    cur.close()

def create_latest_views_pg(conn, schema: str = "public") -> None:
    cur = conn.cursor()
    for v in ("clientes_hos_latest", "clientes_modulos_latest", "clientes_rep_latest"):
        cur.execute(f'DROP VIEW IF EXISTS "{schema}"."{v}";')
    cur.execute(f"""
    CREATE VIEW "{schema}"."clientes_hos_latest" AS
    SELECT * FROM "{schema}"."clientes_hos_hist" h
    WHERE h.snapshot_id = (SELECT MAX(snapshot_id) FROM "{schema}"."sync_runs" WHERE ok=1);
    """)
    cur.execute(f"""
    CREATE VIEW "{schema}"."clientes_modulos_latest" AS
    SELECT * FROM "{schema}"."clientes_modulos_hist" m
    WHERE m.snapshot_id = (SELECT MAX(snapshot_id) FROM "{schema}"."sync_runs" WHERE ok=1);
    """)
    cur.execute(f"""
    CREATE VIEW "{schema}"."clientes_rep_latest" AS
    SELECT * FROM "{schema}"."clientes_rep_hist" r
    WHERE r.snapshot_id = (SELECT MAX(snapshot_id) FROM "{schema}"."sync_runs" WHERE ok=1);
    """)
    conn.commit()
    cur.close()

# --- Firebird WIN1252 fixo ---
def fb_connect(fbcfg: Dict[str, Any]):
    if fdb is None:
        raise RuntimeError("Driver Firebird ausente. Instale com: pip install fdb")
    _try_load_fbclient(fbcfg)
    dsn = fbcfg.get("dsn", "127.0.0.1/3050:crm_hos")
    user = fbcfg.get("user", "SYSDBA")
    password = fbcfg.get("password", "masterkey")
    charset = "WIN1252"  # fixo
    con = fdb.connect(dsn=dsn, user=user, password=password, charset=charset)
    logging.info("[FB] conectado (charset=%s)", charset)
    return con

# --- Coleta e gravação ---
def sync_once(settings: Dict[str, Any]) -> int:
    schema = settings.get("schema", "public")
    pg_con = pg_connect(settings)
    try:
        ensure_schema_pg(pg_con, schema=schema)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows_hos = rows_mod = rows_rep = 0

        fb_con = fb_connect(settings["firebird"])
        try:
            cur = fb_con.cursor()
            cur.execute("""
                SELECT
                    c.CODIGO as CRM,
                    c.FANTASIA,
                    c.CNPJ,
                    COALESCE(NULLIF(TRIM(c.FONE),''), '') as FONE,
                    COALESCE(NULLIF(TRIM(c.FONE2),''), '') as FONE2,
                    cd.NOME as CIDADE,
                    c.ENDERECO,
                    c.BAIRRO,
                    c.CEP,
                    cd.CODIGO_IBGE,
                    c.ESTADO,
                    c.ATIVO,
                    c.STATUS,
                    cw.QTD_DIAS_HAB as DIAS_LIBERADOS,
                    r.CODIGO as COD_REP,
                    r.NOME as NOME_REP
                FROM CLIENTES c
                JOIN CLIENTES_WEB cw ON cw.COD_CLIENTE = c.CODIGO
                JOIN CIDADES cd ON cd.CODIGO = c.CIDADE
                LEFT JOIN REPRESENTANTES r ON r.CODIGO = c.REPRESENTANTE
                WHERE c.STATUS = 'CLIENTE'
            """)
            cols = [d[0] for d in cur.description]
            df_cli = pd.DataFrame(cur.fetchall(), columns=cols)

            cur.execute("""
                SELECT
                    cp.CODIGO_CLIENTE as CRM,
                    p.DESCRICAO as MODULO,
                    COALESCE(cp.NUM_ESTACOES, 1) as NUM_ESTACOES
                FROM CLIENTE_PRODUTOS cp
                JOIN PRODUTOS p ON p.CODIGO = cp.CODIGO_PRODUTO
            """)
            cols = [d[0] for d in cur.description]
            df_mod_det = pd.DataFrame(cur.fetchall(), columns=cols)
        finally:
            try: fb_con.close()
            except Exception: pass

        if not df_mod_det.empty:
            df_mod_det["MOD_TXT"] = df_mod_det["MODULO"].astype(str) + ": " + df_mod_det["NUM_ESTACOES"].astype(str)
            df_mod_list = df_mod_det.groupby("CRM")["MOD_TXT"].apply(lambda s: ", ".join(sorted(set(s)))).reset_index(name="MODULOS")
        else:
            df_mod_list = pd.DataFrame(columns=["CRM","MODULOS"])

        if not df_cli.empty:
            df_cli = df_cli.merge(df_mod_list, on="CRM", how="left")
            df_cli["MODULOS"] = df_cli["MODULOS"].fillna("Nenhum módulo")
            df_cli["TELEFONE"] = df_cli["FONE"].fillna("").astype(str).str.strip()
            has_fone2 = df_cli["FONE2"].fillna("").astype(str).str.strip() != ""
            df_cli.loc[has_fone2, "TELEFONE"] = (df_cli["TELEFONE"] + " / " + df_cli["FONE2"].fillna("").astype(str).str.strip()).str.strip(" / ")
            df_cli["CODIGO_IBGE"] = df_cli["CODIGO_IBGE"].astype(str).str.replace(r"\\D", "", regex=True).str.zfill(7)
        else:
            df_cli = pd.DataFrame(columns=["CRM","FANTASIA","CNPJ","TELEFONE","CIDADE","ENDERECO","BAIRRO","CEP","CODIGO_IBGE","ESTADO","ATIVO","STATUS","DIAS_LIBERADOS","MODULOS","NOME_REP"])

        if not df_cli.empty:
            df_rep = (df_cli.groupby(["COD_REP","NOME_REP"]).size().reset_index(name="QTD_CLIENTES").sort_values("QTD_CLIENTES", ascending=False))
        else:
            df_rep = pd.DataFrame(columns=["COD_REP","NOME_REP","QTD_CLIENTES"])

        if not df_mod_det.empty and not df_cli.empty:
            ativos_crm = set(df_cli["CRM"].astype(str))
            df_mod_det_active = df_mod_det[df_mod_det["CRM"].astype(str).isin(ativos_crm)].copy()
            df_mod = (df_mod_det_active.groupby("MODULO").size().reset_index(name="QUANTIDADE").sort_values("QUANTIDADE", ascending=False))
        else:
            df_mod = pd.DataFrame(columns=["MODULO","QUANTIDADE"])

        keep_cols = ["CRM","FANTASIA","CNPJ","TELEFONE","CIDADE","ENDERECO","BAIRRO","CEP","CODIGO_IBGE","ESTADO","ATIVO","STATUS","DIAS_LIBERADOS","MODULOS","NOME_REP"]
        df_hos = df_cli[keep_cols].copy() if not df_cli.empty else pd.DataFrame(columns=keep_cols)
        rows_hos, rows_mod, rows_rep = len(df_hos), len(df_mod), len(df_rep)

        cur_pg = pg_con.cursor()
        cur_pg.execute(
            f'INSERT INTO "{schema}"."sync_runs" (snapshot_at, rows_hos, rows_mod, rows_rep, ok, message) VALUES (%s,%s,%s,%s,%s,%s) RETURNING snapshot_id;',
            (now, rows_hos, rows_mod, rows_rep, 1, None)
        )
        snapshot_id = cur_pg.fetchone()[0]

        if rows_hos:
            cols = keep_cols
            tuples = [tuple(r) for r in df_hos[cols].itertuples(index=False, name=None)]
            cols_sql = ", ".join([f'"{c}"' for c in cols])
            sql = f'INSERT INTO "{schema}"."clientes_hos_hist" (snapshot_id, {cols_sql}) VALUES %s'
            execute_values(cur_pg, sql, [(snapshot_id,)+t for t in tuples], page_size=500)

        if rows_mod:
            cols = ["MODULO","QUANTIDADE"]
            tuples = [tuple(r) for r in df_mod[cols].itertuples(index=False, name=None)]
            cols_sql = ", ".join([f'"{c}"' for c in cols])
            sql = f'INSERT INTO "{schema}"."clientes_modulos_hist" (snapshot_id, {cols_sql}) VALUES %s'
            execute_values(cur_pg, sql, [(snapshot_id,)+t for t in tuples], page_size=500)

        if rows_rep:
            cols = ["COD_REP","NOME_REP","QTD_CLIENTES"]
            tuples = [tuple(r) for r in df_rep[cols].itertuples(index=False, name=None)]
            cols_sql = ", ".join([f'"{c}"' for c in cols])
            sql = f'INSERT INTO "{schema}"."clientes_rep_hist" (snapshot_id, {cols_sql}) VALUES %s'
            execute_values(cur_pg, sql, [(snapshot_id,)+t for t in tuples], page_size=500)

        pg_con.commit()
        create_latest_views_pg(pg_con, schema=schema)
        logging.info("Snapshot %s gravado. h=%s, m=%s, r=%s", snapshot_id, rows_hos, rows_mod, rows_rep)
        return int(snapshot_id)
    except Exception as e:
        logging.exception("Falha no sync: %s", e)
        try:
            cur = pg_con.cursor()
            cur.execute(
                f'INSERT INTO "{settings.get("schema","public")}"."sync_runs" (snapshot_at, rows_hos, rows_mod, rows_rep, ok, message) VALUES (%s,%s,%s,%s,%s,%s)',
                (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 0, 0, 0, 0, str(e)[:2000])
            )
            pg_con.commit()
        except Exception:
            pass
        raise
    finally:
        try: pg_con.close()
        except Exception: pass

# --- CLI ---
def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Backend de sincronização Firebird → PostgreSQL")
    p.add_argument("--config", help="Caminho do JSON de configuração (default: ./config/backend_sync_settings.json)")
    p.add_argument("--schema", help="Override do schema (default: public)")
    p.add_argument("--once", action="store_true", help="Executa uma sincronização e sai")
    p.add_argument("--watch", action="store_true", help="Loop infinito executando sincronizações")
    p.add_argument("--interval", type=int, default=1800, help="Intervalo em segundos no modo --watch (default: 1800)")
    p.add_argument("--log-level", default="INFO", help="Nível de log (DEBUG, INFO, WARNING, ERROR)")
    return p.parse_args(argv)

def main(argv=None):
    args = parse_args(argv)
    setup_logging(args.log_level)
    settings, cfg_path = load_settings(args.config)
    if args.schema:
        settings["schema"] = args.schema
    logging.info("Config carregada de: %s", cfg_path)
    logging.info("Schema: %s", settings.get("schema"))
    if args.once and args.watch:
        logging.error("Use apenas um dos modos: --once OU --watch.")
        return 2
    rc = 0
    if args.once or not args.watch:
        try:
            sid = sync_once(settings)
            logging.info("Concluído (snapshot_id=%s).", sid)
        except Exception as e:
            logging.error("Falha no sync (modo once): %s", e)
            rc = 1
    else:
        logging.info("Entrando no modo watch. Intervalo=%s s", args.interval)
        while True:
            try:
                sid = sync_once(settings)
                logging.info("Concluído (snapshot_id=%s). Aguardando %s s...", sid, args.interval)
            except Exception as e:
                logging.error("Falha no sync (modo watch): %s", e)
            try:
                time.sleep(max(10, int(args.interval)))
            except KeyboardInterrupt:
                logging.info("Encerrado por sinal (Ctrl+C).")
                break
    return rc

if __name__ == "__main__":
    raise SystemExit(main())
