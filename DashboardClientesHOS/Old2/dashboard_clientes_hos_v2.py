# -*- coding: utf-8 -*-
"""
MapaCliente.py — App único FULL SQLite + Sync (Firebird→SQLite) com layout clássico,
Configurações em BottomSheet (rápida), aparência do mapa e ações de atualização dentro de Configurações,
tema Dark compatível (fallback) e otimizações de desempenho (debounce + cache por snapshot + PRAGMAs SQLite).

Requisitos:
    pip install flet folium pandas fdb

Defaults de configuração (gerados automaticamente se o arquivo não existir):
{
  "firebird": {
    "dsn": "192.168.1.9/3050:crm_hos",
    "user": "SYSDBA",
    "password": "had63rg@"
  },
  "sqlite_db": "data/mapa_clientes.db",
  "auto_sync_enabled": false,
  "auto_sync_interval_s": 1800,
  "mapbox_token": "",
  "theme_dark": true,
  "logs_to_file": false,
  "map_style": "carto-positron",
  "heatmap_enabled": false,
  "heat_radius": 20
}

Rode com o modo web e abra no navegador: 
python "DashboardClientesHOS.py" --web --host 127.0.0.1 --port 8558 --view BROWSER
"""
from __future__ import annotations

import ssl
import urllib.request

import os
import sys
import json
import math
import sqlite3
from datetime import datetime
from threading import Event, Thread, Timer
from pathlib import Path as _Path
from typing import Optional, Dict, List, Any

import pandas as pd
import flet as ft



# Forward declaration to satisfy static analyzers
uf_legend_wrap: ft.Row | None = None
# --- Global patch to avoid 'Image must have either "src" or "src_base64" specified.' ---
try:
    if not getattr(ft, "_image_patched", False):
        _orig_Image = ft.Image
        def _SafeImage(*args, **kwargs):
            src = kwargs.get("src")
            src_b64 = kwargs.get("src_base64")
            if (src is None or str(src).strip() == "") and (src_b64 is None or str(src_b64).strip() == ""):
                kwargs["src_base64"] = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMB/at1ZogAAAAASUVORK5CYII="
            return _orig_Image(*args, **kwargs)
        ft.Image = _SafeImage
        ft._image_patched = True
except Exception:
    pass


# --- Global patch to avoid 'Image must have either "src" or "src_base64" specified.' ---
try:
    import flet as ft  # alias just to be safe if 'ft' isn't bound yet
except Exception:
    _ft_patch_ref = None
try:
    if _ft_patch_ref is None:
        pass
    else:
        if not getattr(_ft_patch_ref, "_image_patched", False):
            _orig_Image = _ft_patch_ref.Image
            def _SafeImage(*args, **kwargs):
                src = kwargs.get("src")
                src_b64 = kwargs.get("src_base64")
                if (src is None or str(src).strip() == "") and (src_b64 is None or str(src_b64).strip() == ""):
                    kwargs["src_base64"] = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMB/at1ZogAAAAASUVORK5CYII="
                return _orig_Image(*args, **kwargs)
            _ft_patch_ref.Image = _SafeImage
            _ft_patch_ref._image_patched = True
except Exception:
    # If patch fails for any reason, silently ignore.
    pass

import urllib.parse as ul

from concurrent.futures import ThreadPoolExecutor
import threading
from pathlib import Path
import functools
import http.server
import socketserver
import socket

# =============================
# Base dir + settings JSON e data dir (autogerados)
# =============================
def app_base_dir() -> _Path:
    if getattr(sys, "frozen", False):  # exe/onefile
        return _Path(sys.executable).resolve().parent
    try:
        return _Path(__file__).resolve().parent
    except NameError:
        return _Path.cwd()

BASE_DIR = app_base_dir()
CONFIG_DIR = BASE_DIR / "config"
SETTINGS_PATH = CONFIG_DIR / "mapacliente_settings.json"
CONFIG_DIR.mkdir(parents=True, exist_ok=True)

DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
MAP_DIR = BASE_DIR / "mapa"
MAP_DIR.mkdir(parents=True, exist_ok=True)
MAP_HTML_PATH = MAP_DIR / "mapa_folium_tmp.html"
GEO_DIR = BASE_DIR / "geo"
GEO_DIR.mkdir(parents=True, exist_ok=True)

# --- HTTP local para servir o mapa (Desktop) ---
MAP_HTTPD = None
MAP_HTTPD_PORT = None
_MAP_HTTPD_LOCK = threading.Lock()

class _SilentTCPServer(socketserver.TCPServer):
    allow_reuse_address = True

def ensure_map_http_server():
    """Sobe um servidor HTTP local servindo BASE_DIR/mapa; retorna a porta efetiva."""
    global MAP_HTTPD, MAP_HTTPD_PORT
    if MAP_HTTPD is not None and MAP_HTTPD_PORT:
        return MAP_HTTPD_PORT
    with _MAP_HTTPD_LOCK:
        if MAP_HTTPD is not None and MAP_HTTPD_PORT:
            return MAP_HTTPD_PORT
        try:
            handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(MAP_DIR))
        except TypeError:
            # Python muito antigo (sem 'directory'): mudar cwd como fallback
            import os
            cwd = os.getcwd()
            os.chdir(str(MAP_DIR))
            handler = http.server.SimpleHTTPRequestHandler

        for port_try in (8765, 8766, 0):
            try:
                httpd = _SilentTCPServer(("127.0.0.1", port_try), handler)
                MAP_HTTPD = httpd
                MAP_HTTPD_PORT = httpd.server_address[1]
                th = threading.Thread(target=httpd.serve_forever, name="MapHTTPServer", daemon=True)
                th.start()
                break
            except OSError:
                continue
        return MAP_HTTPD_PORT
DEFAULT_SETTINGS = {
    "firebird": {
        "dsn": "192.168.1.9/3050:crm_hos",
        "user": "SYSDBA",
        "password": "had63rg@"
    },
    "sqlite_db": "data/mapa_clientes.db",
    "auto_sync_enabled": False,
    "auto_sync_interval_s": 1800,
    "mapbox_token": "",
    "theme_dark": True,
    "logs_to_file": False,
    "map_style": "carto-positron",
    "heatmap_enabled": False,
    "heat_radius": 20
}

def ensure_sqlite_path(path_str: str) -> str:
    """
    Normaliza caminho do SQLite: se for relativo, resolve em BASE_DIR/data; garante criação do diretório pai.
    Se vier vazio, usa "data/mapa_clientes.db".
    """
    if not path_str:
        path_str = "data/mapa_clientes.db"
    p = _Path(path_str)
    if not p.is_absolute():
        p = BASE_DIR / p
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    return str(p)

def load_settings() -> dict:
    """
    Lê as configurações. Se o arquivo não existir, cria com DEFAULT_SETTINGS e retorna.
    Faz deep-merge com defaults quando existir (para preencher chaves novas).
    Se o arquivo estiver inválido, recria com defaults.
    """
    try:
        if SETTINGS_PATH.exists():
            data = json.loads(SETTINGS_PATH.read_text(encoding="utf-8"))
            out = dict(DEFAULT_SETTINGS)
            fb = dict(DEFAULT_SETTINGS["firebird"])
            fb.update((data.get("firebird") or {}))
            out["firebird"] = fb
            for k in DEFAULT_SETTINGS.keys():
                if k == "firebird":
                    continue
                out[k] = data.get(k, out.get(k))
            # normaliza sqlite_db se vier vazio
            out["sqlite_db"] = data.get("sqlite_db", out.get("sqlite_db", "data/mapa_clientes.db"))
            return out
        else:
            SETTINGS_PATH.write_text(json.dumps(DEFAULT_SETTINGS, ensure_ascii=False, indent=2), encoding="utf-8")
            return dict(DEFAULT_SETTINGS)
    except Exception:
        # arquivo corrompido → recria com defaults
        SETTINGS_PATH.write_text(json.dumps(DEFAULT_SETTINGS, ensure_ascii=False, indent=2), encoding="utf-8")
        return dict(DEFAULT_SETTINGS)

def save_settings(data: dict):
    """
    Persiste normalizando com DEFAULT_SETTINGS (garante todas as chaves).
    """
    ns = dict(DEFAULT_SETTINGS)
    fb = dict(DEFAULT_SETTINGS["firebird"])
    if isinstance(data.get("firebird"), dict):
        fb.update(data["firebird"])
    ns["firebird"] = fb
    for k in DEFAULT_SETTINGS.keys():
        if k == "firebird":
            continue
        if k in data:
            ns[k] = data[k]
    SETTINGS_PATH.write_text(json.dumps(ns, ensure_ascii=False, indent=2), encoding="utf-8")

def load_config_embedded() -> dict:
    """
    Carrega cfg a partir de load_settings(), com overrides por ENV (opcional).
    """
    s = load_settings()
    cfg = {
        "firebird": {
            "dsn": s["firebird"]["dsn"],
            "user": s["firebird"]["user"],
            "password": s["firebird"]["password"],
        },
        "sqlite_db": ensure_sqlite_path(s.get("sqlite_db", "data/mapa_clientes.db")),
        "auto_sync_enabled": bool(s.get("auto_sync_enabled", False)),
        "auto_sync_interval_s": int(s.get("auto_sync_interval_s", 1800)),
        "theme_dark": bool(s.get("theme_dark", True)),
        "logs_to_file": bool(s.get("logs_to_file", False)),
        "map_style": s.get("map_style", "carto-positron"),
        "heatmap_enabled": bool(s.get("heatmap_enabled", False)),
        "heat_radius": int(s.get("heat_radius", 20)),
    }
    # Overrides por ENV (se quiser)
    env_map = {
        "FB_DSN": ("firebird","dsn"),
        "FB_USER": ("firebird","user"),
        "FB_PASSWORD": ("firebird","password"),
        "MAPA_SQLITE_DB": ("sqlite_db"),
        "MAPBOX_TOKEN": ("mapbox_token"),
    }
    for env_k, path in env_map.items():
        v = os.getenv(env_k)
        if v:
            if len(path) == 2:
                cfg[path[0]][path[1]] = v
            else:
                cfg[path[0]] = v
    # Normaliza sqlite_db após overrides
    cfg["sqlite_db"] = ensure_sqlite_path(cfg.get("sqlite_db"))
    return cfg

# =============================
# Tema compatível com versões antigas do Flet (sem SURFACE/SURFACE_VARIANT)
# =============================
def theme_tokens(page: "ft.Page"):
    dark = (getattr(page, "theme_mode", None) == ft.ThemeMode.DARK)
    surface = ft.Colors.GREY_900 if dark else ft.Colors.WHITE
    surface_variant = ft.Colors.GREY_800 if dark else ft.Colors.GREY_100
    on_surface = ft.Colors.WHITE if dark else ft.Colors.BLACK
    return {"surface": surface, "surface_variant": surface_variant, "on_surface": on_surface}

# =============================
# SQLite: tuning, schema, views e util
# =============================
def tune_sqlite(con: sqlite3.Connection) -> None:
    """PRAGMAs amigáveis para leitura e snapshots (WAL, cache, mmap)."""
    try:
        cur = con.cursor()
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("PRAGMA synchronous=NORMAL;")
        cur.execute("PRAGMA temp_store=MEMORY;")
        cur.execute("PRAGMA cache_size=-200000;")  # ~200MB
        cur.execute("PRAGMA mmap_size=134217728;")  # 128MB
        con.commit()
    except Exception:
        pass

def ensure_schema(con: sqlite3.Connection) -> None:
    tune_sqlite(con)
    cur = con.cursor()
    # runs
    cur.execute("""
    CREATE TABLE IF NOT EXISTS sync_runs (
        snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
        snapshot_at TEXT NOT NULL,
        rows_hos INTEGER, rows_mod INTEGER, rows_rep INTEGER,
        ok INTEGER DEFAULT 1,
        message TEXT
    );
    """)
    # histórico (inclui NOME_REP)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS clientes_hos_hist (
        snapshot_id INTEGER, CRM TEXT, FANTASIA TEXT, CNPJ TEXT, TELEFONE TEXT, CIDADE TEXT,
        ENDERECO TEXT, BAIRRO TEXT, CEP TEXT, CODIGO_IBGE TEXT,
        ESTADO TEXT, ATIVO TEXT, STATUS TEXT, DIAS_LIBERADOS INTEGER, MODULOS TEXT,
        NOME_REP TEXT
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS clientes_modulos_hist (
        snapshot_id INTEGER, MODULO TEXT, QUANTIDADE INTEGER
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS clientes_rep_hist (
        snapshot_id INTEGER, COD_REP TEXT, NOME_REP TEXT, QTD_CLIENTES INTEGER
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS municipios (
        municipio TEXT, uf TEXT, lon REAL, lat REAL, name TEXT
    );
    """)
    # índices
    cur.execute("CREATE INDEX IF NOT EXISTS idx_hos_ibge  ON clientes_hos_hist(CODIGO_IBGE);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_hos_uf    ON clientes_hos_hist(ESTADO);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mod_nome  ON clientes_modulos_hist(MODULO);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_rep_nome  ON clientes_rep_hist(NOME_REP);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mun_ibge  ON municipios(municipio);")
    con.commit()

def create_latest_views(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    cur.execute("DROP VIEW IF EXISTS clientes_hos_latest;")
    cur.execute("DROP VIEW IF EXISTS clientes_modulos_latest;")
    cur.execute("DROP VIEW IF EXISTS clientes_rep_latest;")
    cur.execute("""
    CREATE VIEW clientes_hos_latest AS
    SELECT * FROM clientes_hos_hist h
    WHERE h.snapshot_id = (SELECT MAX(snapshot_id) FROM sync_runs WHERE ok=1);
    """)
    cur.execute("""
    CREATE VIEW clientes_modulos_latest AS
    SELECT * FROM clientes_modulos_hist m
    WHERE m.snapshot_id = (SELECT MAX(snapshot_id) FROM sync_runs WHERE ok=1);
    """)
    cur.execute("""
    CREATE VIEW clientes_rep_latest AS
    SELECT * FROM clientes_rep_hist r
    WHERE r.snapshot_id = (SELECT MAX(snapshot_id) FROM sync_runs WHERE ok=1);
    """)
    con.commit()

def seed_municipios_to_db(con: sqlite3.Connection, csv_path: str) -> int:
    if not os.path.exists(csv_path):
        return 0
    try:
        df = pd.read_csv(csv_path, sep=",", encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_csv(csv_path, sep=",", encoding="latin-1")
    if "municipio" in df.columns:
        df["municipio"] = df["municipio"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(7)
    cols = ["municipio","uf","lon","lat","name"]
    df = df[[c for c in cols if c in df.columns]]
    df.to_sql("municipios", con, if_exists="replace", index=False)
    con.execute("CREATE INDEX IF NOT EXISTS idx_mun_ibge  ON municipios(municipio);")
    con.commit()
    return len(df)

# =============================
# Firebird → snapshot (rodada única)
# =============================
def run_sync_once(cfg: dict) -> int:
    try:
        import fdb
    except Exception as e:
        raise RuntimeError("Driver Firebird ausente. Instale com: pip install fdb") from e

    db_path = cfg["sqlite_db"]
    (_Path(db_path).parent).mkdir(parents=True, exist_ok=True)

    con_sql = sqlite3.connect(db_path)
    ensure_schema(con_sql)

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ok = 1
    message = ""
    snapshot_id = None
    rows_hos = rows_mod = rows_rep = 0

    con_fb = None
    try:
        con_fb = fdb.connect(dsn=cfg["firebird"]["dsn"], user=cfg["firebird"]["user"], password=cfg["firebird"]["password"])
        cur = con_fb.cursor()

        # Base de clientes ativos (ajuste os nomes das tabelas/colunas conforme seu schema)
        cur.execute("""
            SELECT c.CODIGO as CRM,
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

        # Módulos por cliente (para agregar)
        cur.execute("""
            SELECT cp.CODIGO_CLIENTE as CRM, p.DESCRICAO as MODULO, COALESCE(cp.NUM_ESTACOES, 1) as NUM_ESTACOES
            FROM CLIENTE_PRODUTOS cp
            JOIN PRODUTOS p ON p.CODIGO = cp.CODIGO_PRODUTO
        """)
        cols = [d[0] for d in cur.description]
        df_mod_det = pd.DataFrame(cur.fetchall(), columns=cols)

        # Agrega lista de módulos "MOD: NUM" por cliente
        if not df_mod_det.empty:
            df_mod_det["MOD_TXT"] = df_mod_det["MODULO"].astype(str) + ": " + df_mod_det["NUM_ESTACOES"].astype(str)
            df_mod_list = df_mod_det.groupby("CRM")["MOD_TXT"].apply(lambda s: ", ".join(sorted(set(s)))).reset_index(name="MODULOS")
        else:
            df_mod_list = pd.DataFrame(columns=["CRM","MODULOS"])

        # Join com clientes
        if not df_cli.empty:
            df_cli = df_cli.merge(df_mod_list, on="CRM", how="left")
            df_cli["MODULOS"] = df_cli["MODULOS"].fillna("Nenhum módulo")
            df_cli["TELEFONE"] = (df_cli["FONE"].fillna("").astype(str).str.strip())
            df_cli.loc[df_cli["FONE2"].fillna("").astype(str).str.strip()!="", "TELEFONE"] = \
                (df_cli["TELEFONE"] + " / " + df_cli["FONE2"].fillna("").astype(str).str.strip()).str.strip(" / ")
            df_cli["CODIGO_IBGE"] = df_cli["CODIGO_IBGE"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(7)
        else:
            df_cli["MODULOS"] = []
            df_cli["TELEFONE"] = ""
            df_cli["CODIGO_IBGE"] = ""

        # Representantes (contagem)
        if not df_cli.empty:
            df_rep = (df_cli.groupby(["COD_REP","NOME_REP"]).size()
                      .reset_index(name="QTD_CLIENTES")
                      .sort_values("QTD_CLIENTES", ascending=False))
        else:
            df_rep = pd.DataFrame(columns=["COD_REP","NOME_REP","QTD_CLIENTES"])

        # Módulos (contagem) — considera apenas clientes ativos
        if not df_mod_det.empty and not df_cli.empty:
            ativos_crm = set(df_cli["CRM"].astype(str))
            df_mod_det_active = df_mod_det[df_mod_det["CRM"].astype(str).isin(ativos_crm)].copy()
            df_mod = (df_mod_det_active.groupby("MODULO").size()
                      .reset_index(name="QUANTIDADE")
                      .sort_values("QUANTIDADE", ascending=False))
        else:
            df_mod = pd.DataFrame(columns=["MODULO","QUANTIDADE"])

        # Prepara df_hos para gravar
        keep_cols = ["CRM","FANTASIA","CNPJ","TELEFONE","CIDADE","ENDERECO","BAIRRO","CEP",
                     "CODIGO_IBGE","ESTADO","ATIVO","STATUS","DIAS_LIBERADOS","MODULOS","NOME_REP"]
        df_hos = df_cli[keep_cols].copy() if not df_cli.empty else pd.DataFrame(columns=keep_cols)

        rows_hos, rows_mod, rows_rep = len(df_hos), len(df_mod), len(df_rep)

        # Grava snapshot
        cur_sql = con_sql.cursor()
        cur_sql.execute(
            "INSERT INTO sync_runs(snapshot_at, rows_hos, rows_mod, rows_rep, ok, message) VALUES (?,?,?,?,?,?)",
            (now, rows_hos, rows_mod, rows_rep, ok, message)
        )
        snapshot_id = cur_sql.lastrowid

        df_hos.assign(snapshot_id=snapshot_id).to_sql("clientes_hos_hist", con_sql, if_exists="append", index=False)
        df_mod.assign(snapshot_id=snapshot_id).to_sql("clientes_modulos_hist", con_sql, if_exists="append", index=False)
        df_rep.assign(snapshot_id=snapshot_id).to_sql("clientes_rep_hist", con_sql, if_exists="append", index=False)

        con_sql.commit()

    except Exception as e:
        ok = 0
        message = str(e)
        con_sql.execute(
            "INSERT INTO sync_runs(snapshot_at, rows_hos, rows_mod, rows_rep, ok, message) VALUES (?,?,?,?,?,?)",
            (now, 0, 0, 0, ok, message)
        )
        con_sql.commit()
        raise
    finally:
        try:
            if con_fb:
                con_fb.close()
        except Exception:
            pass
        create_latest_views(con_sql)
        con_sql.close()

    print(f"✅ Snapshot {snapshot_id} em {now} | HOS={rows_hos} MOD={rows_mod} REP={rows_rep} → {db_path}")
    return snapshot_id

def read_sync_runs(db_path: str, limit: int = 30) -> pd.DataFrame:
    con = sqlite3.connect(db_path)
    try:
        tune_sqlite(con)
        df = pd.read_sql_query(
            "SELECT snapshot_id, snapshot_at, rows_hos, rows_mod, rows_rep, ok, COALESCE(message,'') as message FROM sync_runs ORDER BY snapshot_id DESC LIMIT ?",
            con, params=(limit)
        )
    finally:
        con.close()
    return df

def get_latest_ok_snapshot_id(db_path: str) -> int:
    if not os.path.exists(db_path):
        return 0
    con = sqlite3.connect(db_path)
    try:
        cur = con.cursor()
        cur.execute("SELECT MAX(snapshot_id) FROM sync_runs WHERE ok=1")
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    finally:
        con.close()

# cache de municípios (evita reler a tabela em toda atualização)
MUNICIPIOS_CACHE = {"db": None, "df": None}
def load_municipios_once(db_path: str) -> pd.DataFrame:
    if not os.path.exists(db_path):
        return pd.DataFrame(columns=["municipio","uf","lon","lat","name"])
    if MUNICIPIOS_CACHE["db"] == db_path and isinstance(MUNICIPIOS := MUNICIPIOS_CACHE["df"], pd.DataFrame):
        return MUNICIPIOS
    con = sqlite3.connect(db_path)
    try:
        tune_sqlite(con)
        df = pd.read_sql_query("SELECT * FROM municipios", con)
    finally:
        con.close()
    if "municipio" in df.columns:
        df["municipio"] = df["municipio"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(7)
    MUNICIPIOS_CACHE["db"] = db_path
    MUNICIPIOS_CACHE["df"] = df
    return df

def load_from_sqlite(db_path: str, snapshot_id: Optional[int] = None) -> Dict[str, Any]:
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"SQLite não encontrado: {db_path}")
    con = sqlite3.connect(db_path)
    try:
        tune_sqlite(con)
        if snapshot_id is None:
            try:
                clientes_hos_df = pd.read_sql_query("SELECT * FROM clientes_hos_latest", con)
                clientes_mod_df = pd.read_sql_query("SELECT * FROM clientes_modulos_latest", con)
                clientes_rep_df = pd.read_sql_query("SELECT * FROM clientes_rep_latest", con)
            except Exception:
                clientes_hos_df = pd.read_sql_query("SELECT * FROM clientes_hos_hist WHERE snapshot_id=(SELECT MAX(snapshot_id) FROM sync_runs WHERE ok=1)", con)
                clientes_mod_df = pd.read_sql_query("SELECT * FROM clientes_modulos_hist WHERE snapshot_id=(SELECT MAX(snapshot_id) FROM sync_runs WHERE ok=1)", con)
                clientes_rep_df = pd.read_sql_query("SELECT * FROM clientes_rep_hist WHERE snapshot_id=(SELECT MAX(snapshot_id) FROM sync_runs WHERE ok=1)", con)
        else:
            clientes_hos_df = pd.read_sql_query("SELECT * FROM clientes_hos_hist WHERE snapshot_id=?", con, params=(snapshot_id))
            clientes_mod_df = pd.read_sql_query("SELECT * FROM clientes_modulos_hist WHERE snapshot_id=?", con, params=(snapshot_id))
            clientes_rep_df = pd.read_sql_query("SELECT * FROM clientes_rep_hist WHERE snapshot_id=?", con, params=(snapshot_id))
    finally:
        con.close()

    if "CODIGO_IBGE" in clientes_hos_df.columns:
        clientes_hos_df["CODIGO_IBGE"] = clientes_hos_df["CODIGO_IBGE"].astype(str).str.replace(r"\D","", regex=True).str.zfill(7)

    municipios_df = load_municipios_once(db_path)  # cacheado
    if not municipios_df.empty:
        mun_key = municipios_df[["municipio","uf","lon","lat","name"]].rename(columns={"municipio":"CODIGO_IBGE"})
    else:
        mun_key = pd.DataFrame(columns=["CODIGO_IBGE","uf","lon","lat","name"])
    merged_df = pd.merge(clientes_hos_df, mun_key, on="CODIGO_IBGE", how="left").sort_values(by="CIDADE", na_position="last")

    df_totalizado_estado = clientes_hos_df.groupby("ESTADO").size().reset_index(name="Clientes") if not clientes_hos_df.empty else pd.DataFrame(columns=["ESTADO","Clientes"])
    df_totalizado_cidade = clientes_hos_df.groupby(["CIDADE","ESTADO"]).size().reset_index(name="Clientes").rename(columns={"ESTADO":"UF"}) if not clientes_hos_df.empty else pd.DataFrame(columns=["CIDADE","UF","Clientes"])

    return {
        "clientes_hos_df": clientes_hos_df,
        "clientes_mod_df": clientes_mod_df,
        "clientes_rep_df": clientes_rep_df,
        "municipios_df": municipios_df,
        "merged_df": merged_df,
        "df_totalizado_estado": df_totalizado_estado,
        "df_totalizado_cidade": df_totalizado_cidade,
    }


# =============================
# UI helpers (mapa Folium)
# =============================
# Dependências: folium
try:
    import folium  # type: ignore[reportMissingImports]
    from folium import plugins as _folium_plugins  # type: ignore[reportMissingImports]
except Exception:
    folium = None
    _folium_plugins = None


def _ensure_geojson_estados() -> str | None:
    try:
        gj = GEO_DIR / "br_estados.geojson"
        if gj.exists() and gj.stat().st_size > 1024:
            return str(gj)
        ctx = ssl.create_default_context()
        urls = [
            "https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson",
        ]
        for u in urls:
            try:
                with urllib.request.urlopen(u, context=ctx, timeout=15) as resp:
                    data = resp.read()
                    if data and len(data) > 1024:
                        gj.write_bytes(data)
                        return str(gj)
            except Exception:
                continue
        return None
    except Exception as e:
        print("[MAP][ERROR] _ensure_geojson_estados:", e, file=sys.stderr)
        return None

def _infer_geojson_uf_key(geojson_obj: dict) -> str:
    try:
        feats = geojson_obj.get("features") or []
        for f in feats[:5]:
            props = (f or {}).get("properties") or {}
            for key in ["sigla", "UF", "uf", "Sigla", "state_code", "abbr", "name", "nome", "id"]:
                if key in props:
                    return key
    except Exception:
        pass
    return "name"
def make_color_map(estados: List[str]) -> Dict[str, str]:
    # Paleta local (sem plotly)
    base = [
        "#1f77b4","#ff7f0e","#2ca02c","#d62728","#9467bd","#8c564b","#e377c2","#7f7f7f","#bcbd22","#17becf",
        "#393b79","#637939","#8c6d31","#843c39","#7b4173","#3182bd","#e6550d","#31a354","#756bb1","#636363",
        "#9c9ede","#e7ba52","#74c476","#9e9ac8","#bdbdbd","#6b6ecf","#b5cf6b","#cedb9c","#e7969c","#a55194"
    ]
    if not estados:
        return {}
    palette = (base * ((len(estados) // len(base)) + 1))[: len(estados)]
    return {estado: palette[i] for i, estado in enumerate(estados)}

def _map_style_to_folium(style: str, theme_mode: str | None = None) -> str:
    s = (style or "").lower().strip()
    if s in ("carto-positron","positron"): return "CartoDB positron"
    if s in ("carto-darkmatter","dark","darkmatter","dark-matter"): return "CartoDB dark_matter"
    if s in ("open-street-map","openstreetmap","osm"): return "OpenStreetMap"
    # fallback por tema
    if (theme_mode or "").lower() == "dark": return "CartoDB dark_matter"
    return "CartoDB positron"

def criar_mapa(
    merged_df: pd.DataFrame,
    df_totalizado_estado: pd.DataFrame,
    color_map: Dict[str, str],
    estado_selecionado: Optional[str] = None,
    altura: int = 600,
    legend_mode: str = "wide",
    map_style: str = "carto-positron",
    heatmap: bool = False,
    heat_radius: int = 20,
    view_mode: str = "pontos",
) -> str:
    if folium is None:
        return "<div style='padding:12px;color:#b00'>Folium não instalado. pip install folium</div>"
    df = merged_df.copy() if isinstance(merged_df, pd.DataFrame) else pd.DataFrame(columns=["lat","lon","ESTADO","CIDADE","FANTASIA"])
    if estado_selecionado:
        try: df = df[df["ESTADO"] == estado_selecionado]
        except Exception: pass
    try: df = df[df["lat"].notna() & df["lon"].notna()]
    except Exception: pass

    center_lat = float(df["lat"].mean()) if len(df) else -14.2350
    center_lon = float(df["lon"].mean()) if len(df) else -51.9253
    tiles = _map_style_to_folium(map_style)
    m = folium.Map(location=[center_lat, center_lon], tiles=tiles, zoom_start=4, control_scale=True)

    vm = (view_mode or "pontos").strip().lower()
    if vm.startswith("cidade"): vm = "cidades"
    elif vm.startswith("estado"): vm = "estados"
    elif vm.startswith("choro") or vm.startswith("choropl"): vm = "choropleth"
    else: vm = "pontos"

    try:
        if vm == "pontos":
            if heatmap and _folium_plugins is not None and not df.empty:
                heat_data = []
                for _, r in df.iterrows():
                    try: heat_data.append([float(r["lat"]), float(r["lon"])])
                    except Exception: continue
                if heat_data:
                    _folium_plugins.HeatMap(heat_data, radius=int(heat_radius or 20), blur=18, min_opacity=0.3).add_to(m)
            else:
                cluster = _folium_plugins.MarkerCluster().add_to(m) if _folium_plugins else m
                for _, r in df.iterrows():
                    try: lat = float(r["lat"]); lon = float(r["lon"])
                    except Exception: continue
                    nome = str(r.get("FANTASIA","")); uf = str(r.get("ESTADO","")); cid = str(r.get("CIDADE",""))
                    popup = folium.Popup(f"<b>{nome}</b><br>UF: {uf}<br>Município: {cid}", max_width=300)
                    folium.Marker([lat, lon], popup=popup).add_to(cluster)

        elif vm == "cidades":
            if not df.empty:
                df_city = (df.groupby(["CIDADE","ESTADO","lat","lon"]).size().reset_index(name="Clientes").sort_values("Clientes", ascending=False))
                for _, r in df_city.iterrows():
                    try: lat = float(r["lat"]); lon = float(r["lon"])
                    except Exception: continue
                    uf = str(r["ESTADO"]); cid = str(r["CIDADE"]); qtd = int(r["Clientes"])
                    radius = max(6, min(36, 6 + 3 * (qtd ** 0.5)))
                    color = color_map.get(uf, "#3388ff")
                    popup = folium.Popup(f"<b>{cid}/{uf}</b><br>Clientes: {qtd}", max_width=300)
                    folium.CircleMarker([lat, lon], radius=radius, color=color, weight=2, fill=True, fill_opacity=0.55, popup=popup).add_to(m)

        elif vm == "estados":
            if not df.empty:
                df_uf = (df.groupby(["ESTADO"]).agg(Clientes=("ESTADO","size"), lat=("lat","mean"), lon=("lon","mean")).reset_index().sort_values("Clientes", ascending=False))
                for _, r in df_uf.iterrows():
                    try: lat = float(r["lat"]); lon = float(r["lon"])
                    except Exception: continue
                    uf = str(r["ESTADO"]); qtd = int(r["Clientes"])
                    radius = max(8, min(40, 8 + 3.5 * (qtd ** 0.5)))
                    color = color_map.get(uf, "#7f7f7f")
                    popup = folium.Popup(f"<b>UF: {uf}</b><br>Clientes: {qtd}", max_width=280)
                    folium.CircleMarker([lat, lon], radius=radius, color=color, weight=2, fill=True, fill_opacity=0.50, popup=popup).add_to(m)

        elif vm == "choropleth":
            df_uf = (df.groupby(["ESTADO"]).size().reset_index(name="Clientes") if not df.empty else pd.DataFrame(columns=["ESTADO","Clientes"]))
            gj_path = _ensure_geojson_estados()
            if gj_path and os.path.exists(gj_path):
                import json as _json
                with open(gj_path, "r", encoding="utf-8") as fh: geoj = _json.load(fh)
                uf_key = _infer_geojson_uf_key(geoj)
                ch = folium.Choropleth(geo_data=geoj, data=df_uf, columns=["ESTADO","Clientes"], key_on=f"feature.properties.{uf_key}", fill_opacity=0.75, line_opacity=0.6, legend_name="Clientes por UF")
                ch.add_to(m)
                try:
                    folium.GeoJson(geoj, name="UF", tooltip=folium.GeoJsonTooltip(fields=[uf_key], aliases=["UF:"])).add_to(m)
                except Exception: pass
        else:
            cluster = _folium_plugins.MarkerCluster().add_to(m) if _folium_plugins else m
            for _, r in df.iterrows():
                try: lat = float(r["lat"]); lon = float(r["lon"])
                except Exception: continue
                nome = str(r.get("FANTASIA","")); uf = str(r.get("ESTADO","")); cid = str(r.get("CIDADE",""))
                popup = folium.Popup(f"<b>{nome}</b><br>UF: {uf}<br>Município: {cid}", max_width=300)
                folium.Marker([lat, lon], popup=popup).add_to(cluster)

    except Exception as e:
        print("[MAP][ERROR] Folium build:", e, file=sys.stderr)

    try: return m.get_root().render()
    except Exception as e:
        print("[MAP][ERROR] Folium render:", e, file=sys.stderr)
        return "<div style='padding:12px;color:#b00'>Falha ao renderizar mapa Folium.</div>"


def log_map_error(ctx: str, e: Exception):
    try:
        print(f"[MAP][ERROR] {ctx}: {e}", file=sys.stderr)
    except Exception:
        pass

def embed_folium_map(page, html_str: str, *, altura: int | None = None, height: int | None = None):
    """
    Persiste o mapa em BASE_DIR/mapa/mapa_folium_tmp.html e exibe no app:
      - Desktop (Windows/Linux/macOS): WebView apontando para http://127.0.0.1:<PORT>/mapa_folium_tmp.html
        (servido por um HTTP local dedicado e estável).
      - Web: Iframe(src=...) apontando para o mesmo HTTP local quando possível (ambiente local),
        com fallback para srcdoc quando o servidor não puder ser iniciado.
    """
    h = altura or height or 600

    # 1) Persistir arquivo local (auditoria e para abrir externo)
    file_uri = None
    try:
        MAP_HTML_PATH.write_text(html_str, encoding="utf-8")
        file_uri = MAP_HTML_PATH.as_uri()
    except Exception as e:
        log_map_error("embed_folium_map(write_base_dir)", e)

    # Detectar plataforma
    try:
        import flet as ft
    except Exception as e:
        log_map_error("embed_folium_map(import_flet)", e)
        raise

    platform = str(getattr(page, "platform", "")).lower()

    # 2) Tentar servidor HTTP local para servir a pasta do mapa
    port = None
    try:
        port = ensure_map_http_server()
    except Exception as e:
        log_map_error("embed_folium_map(ensure_map_http_server)", e)

    http_url = f"http://127.0.0.1:{port}/mapa_folium_tmp.html" if port else None

    # 3) Web → preferir Iframe(srcdoc=...) para funcionar em dispositivos remotos e evitar dependência de 127.0.0.1
    if platform in ("web", "fuchsia"):
        if hasattr(ft, "Iframe"):
            return ft.Iframe(srcdoc=html_str, width=getattr(page, "width", None) or 1200, height=h, expand=True)

    # 4) Desktop → usar WebView(URL http://127.0.0.1:<PORT>/mapa_folium_tmp.html)
    if hasattr(ft, "WebView") and http_url:
        try:
            return ft.WebView(url=http_url, height=h, expand=True)
        except Exception as e:
            log_map_error("embed_folium_map(WebView_http_url)", e)

    # 5) Desktop fallback → WebView(content=html_str) (alguns runtimes bloqueiam JS externo)
    if hasattr(ft, "WebView"):
        try:
            return ft.WebView(content=html_str, height=h, expand=True)
        except Exception as e:
            log_map_error("embed_folium_map(WebView_content)", e)

    # 6) Fallback final: botão para abrir no navegador + dica runtime
    def _open_in_browser(_):
        try:
            if file_uri:
                page.launch_url(file_uri if not http_url else http_url)
                if hasattr(ft, "SnackBar"):
                    page.snack_bar = ft.SnackBar(ft.Text("Abrindo mapa no navegador padrão..."), open=True)
                    page.update()
        except Exception as ex:
            log_map_error("embed_folium_map(launch_url)", ex)

    hint_runtime = (
        "Dica: No Windows, certifique-se de ter o 'Microsoft Edge WebView2 Runtime' instalado "
        "para que o WebView funcione embutido. Esta tela usará HTTP local para carregar o mapa."
    )

    return ft.Container(
        content=ft.Column(
            [
                ft.Text("Não foi possível embutir o mapa neste ambiente."),
                ft.Text(hint_runtime),
                ft.ElevatedButton("Abrir mapa no navegador", on_click=_open_in_browser),
            ],
            alignment=getattr(ft.MainAxisAlignment, "CENTER", None),
            horizontal_alignment=getattr(ft.CrossAxisAlignment, "CENTER", None),
            spacing=10,
        ),
        height=h,
        expand=True,
        padding=8,
        border_radius=12,
    )
class RepeatingJob:
    def __init__(self, job, interval_seconds: int = 10):
        self._job = job
        self._interval = max(1, int(interval_seconds))
        self._stop = Event()
        self._thread: Optional[Thread] = None

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()

    def alive(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    def update_interval(self, seconds: int):
        self._interval = max(1, int(seconds))

    def _run(self):
        while not self._stop.is_set():
            self._stop.wait(self._interval)
            if self._stop.is_set():
                break
            try:
                self._job()
            except Exception:
                pass

class Debouncer:
    def __init__(self, wait_seconds: float = 0.25):
        self.wait = float(wait_seconds)
        self._t: Optional[Timer] = None

    def call(self, fn):
        try:
            if self._t is not None:
                self._t.cancel()
        except Exception:
            pass
        self._t = Timer(self.wait, fn)
        try:
            self._t.daemon = True
        except Exception:
            pass
        self._t.start()

def safe_call_ui(page: ft.Page, fn) -> None:
    try:
        page.call_from_thread(fn)
    except Exception:
        try: fn()
        except Exception: pass

# =============================
# App Flet
# =============================
def main(page: ft.Page):

    # --- Suppress benign asyncio CancelledError from Flet broker on shutdown ---
    try:
        import asyncio as _asyncio
        def _ignore_cancelled(loop, context):
            exc = context.get("exception")
            if isinstance(exc, _asyncio.CancelledError):
                return  # ignore noise on normal shutdown/disconnect
            # fallback to default behavior
            loop.default_exception_handler(context)
        try:
            _loop = _asyncio.get_event_loop_policy().get_event_loop()
            _loop.set_exception_handler(_ignore_cancelled)
        except Exception:
            pass
    except Exception:
        pass
    
    # --- Patch: impedir erro "Image must have either 'src' or 'src_base64' specified." ---
    def _patch_flet_image():
        # Evita múltiplos patches
        if getattr(ft, "_image_patched", False):
            return
        _orig_Image = ft.Image
        def _Image(*args, **kwargs):
            # Se nenhum src ou src_base64 foi passado (ou vier vazio), injeta um PNG 1x1 transparente
            src = kwargs.get("src")
            src_b64 = kwargs.get("src_base64")
            if (src is None or str(src).strip() == "") and (src_b64 is None or str(src_b64).strip() == ""):
                kwargs["src_base64"] = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMB/at1ZogAAAAASUVORK5CYII="
            return _orig_Image(*args, **kwargs)
        ft.Image = _Image
        ft._image_patched = True

    _patch_flet_image()
# Tema inicial (Material 3 + Dark/Light)
    s_theme = load_settings()
    # Pré-declarações para evitar warnings do Pylance
    auto_sync = None  # type: ignore
    eta_ticker = None  # type: ignore

    page.theme = ft.Theme(use_material3=True, color_scheme_seed=ft.Colors.BLUE)
    page.theme_mode = ft.ThemeMode.DARK if s_theme.get("theme_dark", True) else ft.ThemeMode.LIGHT
    t = theme_tokens(page)

    # [UF LEGEND V10] chips abaixo do mapa
    uf_legend_wrap: ft.Row | None = ft.Row(spacing=12, alignment=ft.MainAxisAlignment.SPACE_BETWEEN, expand=True)

    def update_uf_legend():
        nonlocal clientes_hos_df, df_totalizado_estado, color_map, uf_legend_wrap
        try:
            import pandas as pd
            ufs_all = ["AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS","MG","PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC","SP","SE","TO"]

            # Fonte de dados para a legenda
            df = df_totalizado_estado
            if df is None or not isinstance(df, pd.DataFrame) or df.empty:
                if clientes_hos_df is not None and isinstance(clientes_hos_df, pd.DataFrame) and not clientes_hos_df.empty:
                    uf_col = "ESTADO" if "ESTADO" in clientes_hos_df.columns else ("UF" if "UF" in clientes_hos_df.columns else None)
                    if uf_col:
                        df = clientes_hos_df.groupby(uf_col).size().reset_index(name="Clientes").rename(columns={uf_col: "ESTADO"})
                    else:
                        df = pd.DataFrame(columns=["ESTADO","Clientes"])
                else:
                    df = pd.DataFrame(columns=["ESTADO","Clientes"])
                df_totalizado_estado = df

            # Coluna de valor
            val_col = "Clientes" if "Clientes" in df.columns else next((c for c in ["QTD_CLIENTES","QTD"] if c in df.columns), None)
            if val_col is None:
                df = df.copy()
                df["Clientes"] = 0
                val_col = "Clientes"

            # Totais por UF (27 garantidas)
            counts = {uf: 0 for uf in ufs_all}
            for r in df.itertuples(index=False):
                uf = str(getattr(r, "ESTADO", "")).upper()
                qtd = getattr(r, val_col, 0)
                try:
                    qtd = int(qtd)
                except Exception:
                    try:
                        qtd = int(float(qtd))
                    except Exception:
                        qtd = 0
                if uf in counts:
                    counts[uf] += qtd

            # Garantir color_map (usa make_color_map se existir; senão fallback local)
            def _fallback_cmap(ufs):
                pal = [
                    ft.Colors.AMBER, ft.Colors.BLUE, ft.Colors.CYAN, ft.Colors.DEEP_ORANGE, ft.Colors.DEEP_PURPLE,
                    ft.Colors.GREEN, ft.Colors.INDIGO, ft.Colors.LIGHT_BLUE, ft.Colors.LIGHT_GREEN, ft.Colors.LIME,
                    ft.Colors.ORANGE, ft.Colors.PINK, ft.Colors.PURPLE, ft.Colors.RED, ft.Colors.TEAL, ft.Colors.BLUE_GREY
                ]
                return {u: pal[i % len(pal)] for i, u in enumerate(sorted(ufs))}
            if color_map is None or not isinstance(color_map, dict) or not color_map:
                base_ufs = [k for k, v in counts.items() if v > 0] or list(counts.keys())
                try:
                    cm = make_color_map(base_ufs)
                except Exception:
                    cm = _fallback_cmap(base_ufs)
                color_map = cm
            else:
                cm = color_map

            ordered = sorted(counts.items(), key=lambda x: (-x[1], x[0]))

            # === NOVO LAYOUT: 4 colunas ===
            col_count = 4
            per_col = (len(ordered) + col_count - 1) // col_count  # ceil
            columns_controls = []
            for i in range(col_count):
                chunk = ordered[i*per_col:(i+1)*per_col]
                col_rows = []
                for uf, qtd in chunk:
                    color = cm.get(uf, ft.Colors.GREY_400)
                    col_rows.append(
                        ft.Row(
                            [
                                ft.Container(width=10, height=10, border_radius=2, bgcolor=color),
                                ft.Text(f"{uf}: {qtd}", size=18, color=t['on_surface']),
                            ],
                            spacing=8,
                            vertical_alignment=ft.CrossAxisAlignment.CENTER,
                        )
                    )
                columns_controls.append(ft.Column(col_rows, spacing=4, tight=True))

            if uf_legend_wrap:
                uf_legend_wrap.controls = columns_controls
                # Garantir que o Row base mantenha exatamente 4 colunas lado a lado (quebra só se a tela for muito estreita)
                try:
                    uf_legend_wrap.alignment = ft.MainAxisAlignment.SPACE_BETWEEN
                    uf_legend_wrap.wrap = False
                except Exception:
                    pass
                uf_legend_wrap.update()


        except Exception as e:
            # print('update_uf_legend error:', e)  # opcional para debug
            pass



    def _on_disconnect(e):
        try:
            syncing["on"] = False
            set_syncing_ui(False)
        except Exception:
            pass
        try:
            if auto_sync and auto_sync.alive():
                auto_sync.stop()
        except Exception:
            pass
    try:
        page.on_disconnect = _on_disconnect
    except Exception:
        pass

    # Estado geral
    cfg = load_config_embedded()

    # === Async loader: executa I/O pesado fora da UI e aplica depois ===
    _io_executor = ThreadPoolExecutor(max_workers=1)

    def refresh_data_async():
        nonlocal state
        """Carrega snapshot em background e aplica na UI ao concluir."""
        # calcula snapshot desejado (replica a lógica do refresh_data)
        try:
            if snapshot_in_use is None:
                sid_wanted = get_latest_ok_snapshot_id(sqlite_db)
            else:
                sid_wanted = int(snapshot_in_use)
        except Exception:
            sid_wanted = None

        # se não houver snapshot, apenas cai no refresh padrão (que mostrará erro/aviso)
        if not sid_wanted:
            safe_call_ui(page, refresh_data)
            return

        # geração para controlar respostas vencidas
        state["load_gen"] = state.get("load_gen", 0) + 1
        my_gen = state["load_gen"]

        # [PATCH] não mostrar banner ao recarregar dados
        def _io_job():
            # roda fora da UI
            try:
                data = load_from_sqlite(sqlite_db, snapshot_id=sid_wanted)
            except Exception as e:
                data = e
            # volta para a UI
            def _apply():
                # ignora se já houve outra geração depois desta
                if my_gen != state.get("load_gen", 0):
                    return
                # erro?
                if isinstance(data, Exception):
                    page.snack_bar = ft.SnackBar(ft.Text(f"Falha ao carregar snapshot: {data}"), open=True)
                    try:
                        set_syncing_ui(False)
                        # [PATCH] banner update omitido
                    except Exception:
                        pass
                    return
                # guarda payload para o refresh_data usar sem I/O
                state["preloaded_sid"] = sid_wanted
                state["preloaded_data"] = data
                # agora roda o refresh_data normal (aplica UI) - ele vai usar o payload em memória
                safe_call_ui(page, refresh_data)
            page.call_from_thread(_apply)

        _io_executor.submit(_io_job)
    # state já inicializado acima
    resize_deb = Debouncer(0.25)

    sqlite_db = cfg["sqlite_db"]
    MAPBOX_TOKEN = cfg.get("mapbox_token", os.getenv("MAPBOX_TOKEN", ""))

    snapshot_in_use: Optional[int] = None  # None = latest
    syncing = {"on": False}

    # UI topo e indicadores
    last_update_text = ft.Text("Última atualização: —", size=12, color=t['on_surface'])
    # [BEGIN PATCH] Texto de contagem regressiva para Auto Sync
    next_sync_text = ft.Text('', size=12, color=t['on_surface'])
    next_sync_state = {'eta': 0}  # segundos restantes
    # [END PATCH]

    sync_banner = ft.Container(
        visible=False, bgcolor=t['surface_variant'], border_radius=8, padding=8,
        content=ft.Row([ft.ProgressRing(), ft.Text("Sincronizando...", color=t['on_surface'])], spacing=10)
    )

    # Controles de dados/mapa
    is_web = False

    mapa_container = ft.Container(content=ft.Text('Carregando mapa...'), expand=True, height=480)
    ddl_estado = ft.Dropdown(label="UF", options=[ft.dropdown.Option("")] + [ft.dropdown.Option(uf) for uf in
                  ["AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS","MG","PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC","SP","SE","TO"]], value="")
    
    ddl_visao = ft.Dropdown(
    label="Visão",
    options=[
        ft.dropdown.Option("Pontos"),
        ft.dropdown.Option("Cidades"),
        ft.dropdown.Option("Estados"),
        ft.dropdown.Option("Choropleth UF"),
    ],
    value="Cidades",
    )
    ddl_visao.on_change = lambda e: resize_deb.call(lambda: safe_call_ui(page, refresh_map_only))
    ddl_estado.on_change = lambda e: resize_deb.call(lambda: safe_call_ui(page, refresh_map_only))
    btn_reset = ft.OutlinedButton("Resetar Mapa", icon=ft.Icons.CENTER_FOCUS_WEAK, on_click=lambda e: (setattr(ddl_estado, "value", ""), ddl_estado.update(), resize_deb.call(lambda: safe_call_ui(page, refresh_map_only))))

    # Aparência do Mapa (estado baseado nas configurações)
    map_style_val = {"value": cfg.get("map_style", "carto-positron")}
    heat_state = {"on": bool(cfg.get("heatmap_enabled", False)), "radius": int(cfg.get("heat_radius", 20))}

    # Tabelas
    table_rep = ft.DataTable(columns=[ft.DataColumn(ft.Text("Representante")), ft.DataColumn(ft.Text("Quantidade"))], rows=[],
                             column_spacing=14, heading_row_color=t['surface_variant'])
    table_sem = ft.DataTable(columns=[ft.DataColumn(ft.Text("Estado"))], rows=[], heading_row_color=t['surface_variant'], column_spacing=18)
    table_mod = ft.DataTable(columns=[ft.DataColumn(ft.Text("Módulo")), ft.DataColumn(ft.Text("Quantidade"))], rows=[],
                             column_spacing=18, heading_row_color=t['surface_variant'])
    table_cid_sample = ft.DataTable(columns=[ft.DataColumn(ft.Text("UF")), ft.DataColumn(ft.Text("Cidade")), ft.DataColumn(ft.Text("Quantidade"))],
                                    rows=[], column_spacing=18, heading_row_color=t['surface_variant'])

    # Painéis / Containers
    def make_card(title: str, child: ft.Control, h: int) -> ft.Container:
        return ft.Container(
            content=ft.Column([ft.Text(title, size=14, weight=ft.FontWeight.BOLD, color=t['on_surface']),
                               ft.Container(ft.Column([child], scroll=ft.ScrollMode.AUTO, expand=True), height=h)],
                              tight=True, spacing=10),
            bgcolor=t['surface'], border_radius=12, padding=8
        )

    rep_card     = make_card("Clientes por Representante", table_rep, 300)
    sem_hos_card = make_card("Estados sem HOS", table_sem, 150)
    mod_card     = make_card("Módulos Utilizados", table_mod, 230)
    cidades_card = make_card("Clientes por Cidade", table_cid_sample, 230)

    # Header topo (Total + botão Configurações à direita)
    total_container = ft.Container(
        height=110, bgcolor=t['surface'], padding=8, border_radius=12,
        shadow=ft.BoxShadow(blur_radius=10, spread_radius=1, color=ft.Colors.with_opacity(0.08, ft.Colors.BLACK)))
    # criado depois, quando dados carregarem (para popular o total)

    # -------- Configurações em BottomSheet (rápido) --------
    settings_overlay = ft.Container(visible=False)
    # [BEGIN PATCH] Estado de bloqueio do mapa enquanto Configurações estiver aberta
    settings_state = {'map_backup': None}
    # [END PATCH]

    

    def open_settings_sheet(_e=None):
        state["settings_open"] = True
        # [BEGIN PATCH] Bloquear interações do mapa enquanto Configurações estiver aberta
        try:
            if settings_state.get('map_backup') is None:
                settings_state['map_backup'] = mapa_container.content
            blocker = ft.Container(
                expand=True,
                height=mapa_container.height or 600,
                bgcolor=ft.Colors.BLACK,
                opacity=0.01,
            )
            mapa_container.content = blocker
            mapa_container.update()
        except Exception as _ex:
            pass
        # [END PATCH]

        s = load_settings()

        # [Memoization] Se já construímos a UI uma vez, apenas atualize os valores e mostre.
        try:
            _refs = getattr(settings_overlay, "data", None)
        except Exception:
            _refs = None
        if _refs:
            fb = s.get("firebird", {}) or {}
            _refs["dsn_tf"].value = str(fb.get("dsn",""))
            _refs["user_tf"].value = str(fb.get("user",""))
            _refs["pass_tf"].value = str(fb.get("password",""))
            _refs["sqlite_tf"].value = str(s.get("sqlite_db",""))
            _refs["auto_sw"].value = bool(s.get("auto_sync_enabled", False))
            _refs["auto_iv"].value = str(int(s.get("auto_sync_interval_s", 1800)))
            _refs["theme_sw"].value = bool(s.get("theme_dark", True))
            _refs["logs_sw"].value = bool(s.get("logs_to_file", False))
            _refs["est_dd"].value = s.get("map_style","carto-positron")
            _refs["ht_sw"].value = bool(s.get("heatmap_enabled", False))
            _refs["ht_sl"].value = float(s.get("heat_radius", 20))

        settings_overlay.visible = True
        settings_overlay.update()

    

        dsn_tf = ft.TextField(label="Firebird DSN", value=str(s.get("firebird",{}).get("dsn","")), width=520)
        user_tf = ft.TextField(label="Firebird USER", value=str(s.get("firebird",{}).get("user","")), width=260)
        pass_tf = ft.TextField(label="Firebird PASSWORD", value=str(s.get("firebird",{}).get("password","")), password=True, can_reveal_password=True, width=260)
        sqlite_tf = ft.TextField(label="SQLite DB", value=str(s.get("sqlite_db","")), width=520)
        auto_sw = ft.Switch(label="Auto Sync", value=bool(s.get("auto_sync_enabled", False)))
        auto_iv = ft.TextField(label="Intervalo (s)", value=str(int(s.get("auto_sync_interval_s",1800))), width=160, input_filter=ft.NumbersOnlyInputFilter())

        # [PATCH] Handlers de Auto Sync: toggle e mudança de intervalo com contador
        def on_auto_sw_change(e):
            try:
                try:
                    iv = max(60, min(3600, int(auto_iv.value or str(cfg.get('auto_sync_interval_s', 1800)))))
                except Exception:
                    iv = int(cfg.get('auto_sync_interval_s', 1800))

                auto_sync.update_interval(iv)

                if auto_sw.value:
                    # ligar auto sync + contador
                    if not auto_sync.alive():
                        auto_sync.start()
                    next_sync_state['eta'] = iv
                    safe_call_ui(page, update_next_sync_text)
                    if not eta_ticker.alive():
                        eta_ticker.start()
                else:
                    # desligar auto sync + limpar contador
                    if auto_sync.alive():
                        auto_sync.stop()
                    next_sync_state['eta'] = 0
                    safe_call_ui(page, update_next_sync_text)
                    eta_ticker.stop()
            except Exception:
                pass

        auto_sw.on_change = on_auto_sw_change

        def on_auto_iv_change(e):
            try:
                iv = max(60, min(3600, int(auto_iv.value or '1800')))
            except Exception:
                iv = 1800
            auto_sync.update_interval(iv)
            if auto_sw.value:
                next_sync_state['eta'] = iv
                safe_call_ui(page, update_next_sync_text)
                if not eta_ticker.alive():
                    eta_ticker.start()

        auto_iv.on_change = on_auto_iv_change
        theme_sw = ft.Switch(label="Tema escuro", value=bool(s.get("theme_dark", True)))
        logs_sw  = ft.Switch(label="Gerar Log", value=bool(s.get("logs_to_file", False)))
        info_txt = ft.Text("Mudanças em Auto/Intervalo e Tema aplicam na hora. Outras podem exigir reabrir o app.", size=12, color=t['on_surface'])

        # Aparência do Mapa
        est_dd = ft.Dropdown(label="Estilo do mapa", value=s.get("map_style","carto-positron"),
                             options=[ft.dropdown.Option(v) for v in ["carto-positron","carto-darkmatter","open-street-map"]], width=260)
        ht_sw  = ft.Switch(label="Heatmap", value=bool(s.get("heatmap_enabled", False)))
        ht_sl  = ft.Slider(min=5, max=50, divisions=9, value=float(s.get("heat_radius",20)), label="{value}", width=220)

        # Ações de atualização
        # [Memo] Guardar refs para reuso futuro
        refs = {}
        refs.update({
            'dsn_tf': dsn_tf,
            'user_tf': user_tf,
            'pass_tf': pass_tf,
            'sqlite_tf': sqlite_tf,
                        'auto_sw': auto_sw,
            'auto_iv': auto_iv,
            'theme_sw': theme_sw,
            'logs_sw': logs_sw,
            'est_dd': est_dd,
            'ht_sw': ht_sw,
            'ht_sl': ht_sl,
        })

        row_actions = ft.Row([
            ft.FilledButton("Sincronizar agora", icon=ft.Icons.CLOUD_SYNC, on_click=lambda e: sync_now()),
            ft.OutlinedButton("Seed Municípios (CSV)", icon=ft.Icons.FILE_UPLOAD, on_click=lambda e: seed_picker.pick_files(allow_multiple=False)),
            ft.OutlinedButton("Saúde da Sync", icon=ft.Icons.HEALTH_AND_SAFETY, on_click=lambda e: open_sync_dialog()),
        ], spacing=4, wrap=True)

        def close_sheet():
            settings_overlay.visible = False
            settings_overlay.update()
            state["settings_open"] = False
            # [BEGIN PATCH] Restaurar mapa original e opcionalmente recarregar
            try:
                if settings_state.get('map_backup') is not None:
                    mapa_container.content = settings_state['map_backup']
                    settings_state['map_backup'] = None
                    mapa_container.update()
                try:
                    # Atualiza apenas o mapa, se a função existir
                    refresh_map_only()
                except Exception:
                    pass
            except Exception:
                pass
            # [END PATCH]


        def on_save(_ev=None):
            ns = load_settings()
            fb = ns.get("firebird", {}) or {}
            fb["dsn"] = (dsn_tf.value or "").strip()
            fb["user"] = (user_tf.value or "").strip()
            fb["password"] = (pass_tf.value or "").strip()
            ns["firebird"] = fb
            ns["sqlite_db"] = (sqlite_tf.value or "").strip() or ns.get("sqlite_db")
            try:
                ns["auto_sync_interval_s"] = max(60, min(3600, int(auto_iv.value or "1800")))
            except Exception:
                ns["auto_sync_interval_s"] = 1800
            ns["auto_sync_enabled"] = bool(auto_sw.value)
            ns["theme_dark"] = bool(theme_sw.value)
            ns["logs_to_file"] = bool(logs_sw.value)

            # Aparência
            ns["map_style"] = est_dd.value or "carto-positron"
            ns["heatmap_enabled"] = bool(ht_sw.value)
            ns["heat_radius"] = int(ht_sl.value)

            try:
                save_settings(ns)
                page.snack_bar = ft.SnackBar(ft.Text("Configurações salvas."), open=True)
            except Exception as ex:
                page.snack_bar = ft.SnackBar(ft.Text(f"Erro ao salvar: {ex}"), open=True)
            page.update()

            # Aplicação "hot"
            try:
                nonlocal cfg, sqlite_db, MAPBOX_TOKEN, map_style_val, heat_state
                cfg = load_config_embedded()
                sqlite_db = cfg.get("sqlite_db")
                MAPBOX_TOKEN = ""
                page.theme_mode = ft.ThemeMode.DARK if ns.get("theme_dark", True) else ft.ThemeMode.LIGHT

                # Auto sync
                try:
                    new_iv = int(ns.get("auto_sync_interval_s", 1800))
                except Exception:
                    new_iv = 1800
                auto_sync.update_interval(new_iv)
                if ns.get("auto_sync_enabled", False) and not auto_sync.alive():
                    auto_sync.start()
                    next_sync_state['eta'] = int(ns.get('auto_sync_interval_s', 1800))
                    safe_call_ui(page, update_next_sync_text)
                    (eta_ticker.start() if not eta_ticker.alive() else None)
                elif not ns.get("auto_sync_enabled", False) and auto_sync.alive():
                    auto_sync.stop()
                    next_sync_state['eta'] = 0
                    safe_call_ui(page, update_next_sync_text)
                    eta_ticker.stop()

                # Aparência do mapa
                map_style_val["value"] = cfg.get("map_style", "carto-positron")
                heat_state["on"] = bool(cfg.get("heatmap_enabled", False))
                heat_state["radius"] = int(cfg.get("heat_radius", 20))

                state["sid_loaded"] = None  # força recarga
                
            except Exception:
                pass

            close_sheet()
            # Após fechar, aplica recarga pesada (fora da abertura/fechamento do overlay)
            try:
                refresh_data_async()
            except Exception:
                pass

        settings_overlay.content = ft.Container(
            bgcolor=ft.Colors.with_opacity(0.40, ft.Colors.BLACK),
            alignment=ft.alignment.center,
            content=ft.Container(
            bgcolor=t['surface'],
            content=ft.Column([
                ft.Text("Configurações", size=20, weight=ft.FontWeight.BOLD, color=t['on_surface']),
                ft.Divider(),

                ft.Text("Banco de Dados", weight=ft.FontWeight.BOLD, color=t['on_surface']),
                dsn_tf, ft.Row([user_tf, pass_tf], spacing=10),
                sqlite_tf,
                ft.Divider(),

                ft.Text("Mapa", weight=ft.FontWeight.BOLD, color=t['on_surface']),
                ft.Divider(),

                ft.Text("Aparência do Mapa", weight=ft.FontWeight.BOLD, color=t['on_surface']),
                ft.Row([est_dd], spacing=10),
                ft.Row([ht_sw, ht_sl], spacing=10),
                ft.Divider(),

                ft.Text("Atualização (SQLite + Sync)", weight=ft.FontWeight.BOLD, color=t['on_surface']),
                row_actions,
                ft.Divider(),

                ft.Text("Preferências", weight=ft.FontWeight.BOLD, color=t['on_surface']),
                ft.Row([auto_sw, auto_iv, theme_sw, logs_sw], spacing=10),
                ft.Divider(), info_txt,
                ft.Row([ft.OutlinedButton("Fechar", on_click=lambda e: close_sheet()),
                        ft.FilledButton("Salvar", icon=ft.Icons.SAVE, on_click=on_save)],
                       alignment=ft.MainAxisAlignment.END),
            ], spacing=10, scroll=ft.ScrollMode.AUTO, height=520, width=900),
            padding=20)
        )
        settings_overlay.data = refs
        settings_overlay.visible = True
        settings_overlay.visible = True
        settings_overlay.update()
    # Botão Configurações (top-right)
    btn_settings = ft.IconButton(icon=ft.Icons.SETTINGS, tooltip="Configurações", on_click=open_settings_sheet)

    # Saúde da Sync dialog
    sync_dialog = ft.AlertDialog(modal=True)
    sync_table = ft.DataTable(columns=[
        ft.DataColumn(ft.Text("ID")), ft.DataColumn(ft.Text("Data/Hora")),
        ft.DataColumn(ft.Text("HOS")), ft.DataColumn(ft.Text("MOD")), ft.DataColumn(ft.Text("REP")),
        ft.DataColumn(ft.Text("OK")), ft.DataColumn(ft.Text("Msg")),
    ], rows=[], heading_row_color=t['surface_variant'])
    ddl_snapshot = ft.Dropdown(width=360, options=[ft.dropdown.Option("latest")], value="latest")

    def open_sync_dialog(e=None):
        if not sqlite_db or not os.path.exists(sqlite_db):
            page.snack_bar = ft.SnackBar(ft.Text("Banco SQLite não encontrado."), open=True); return
        df_runs = read_sync_runs(sqlite_db, limit=30)
        rows = []; opts = [ft.dropdown.Option("latest")]
        for _, r in df_runs.iterrows():
            rows.append(ft.DataRow(cells=[
                ft.DataCell(ft.Text(str(r["snapshot_id"]))),
                ft.DataCell(ft.Text(str(r["snapshot_at"]))),
                ft.DataCell(ft.Text(str(r["rows_hos"]))),
                ft.DataCell(ft.Text(str(r["rows_mod"]))),
                ft.DataCell(ft.Text(str(r["rows_rep"]))),
                ft.DataCell(ft.Text("✔" if int(r["ok"])==1 else "❌")),
                ft.DataCell(ft.Text((str(r["message"])[:40] + ("..." if len(str(r["message"]))>40 else "")))),
            ]))
            label = f'{int(r["snapshot_id"])} — {str(r["snapshot_at"])}'
            opts.append(ft.dropdown.Option(label))
        sync_table.rows = rows
        ddl_snapshot.options = opts; ddl_snapshot.value = "latest"

        sync_dialog.title = ft.Text("Saúde da Sync")
        sync_dialog.content = ft.Column(
            [
                ft.Text("Últimos snapshots"),
                sync_table,
                ft.Row([ft.Text("Snapshot:"), ddl_snapshot], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
            ],
            scroll=ft.ScrollMode.AUTO, width=900, height=520
        )
        sync_dialog.actions = [
            ft.TextButton("Usar Latest", on_click=lambda e: apply_snapshot(None)),
            ft.FilledButton("Aplicar Selecionado", on_click=lambda e: apply_snapshot(ddl_snapshot.value)),
            ft.TextButton("Fechar", on_click=lambda e: close_sync_dialog()),
        ]

    def close_sync_dialog():
        sync_dialog.open = False; page.update()

    def apply_snapshot(val):
        nonlocal snapshot_in_use
        if val is None or val == "latest":
            snapshot_in_use = None
        else:
            try:
                sid = int(str(val).split("—")[0].strip())
                snapshot_in_use = sid
            except Exception:
                snapshot_in_use = None
        state["sid_loaded"] = None
        refresh_data()
        page.snack_bar = ft.SnackBar(ft.Text(f"Snapshot em uso: {'latest' if snapshot_in_use is None else snapshot_in_use}"), open=True)
        close_sync_dialog()

    # Seed municípios
    def on_seed_result(e: ft.FilePickerResultEvent):
        try:
            if not e.files:
                return
            csv_path = e.files[0].path
            # garante que o diretório do SQLite existe
            _Path(sqlite_db).parent.mkdir(parents=True, exist_ok=True)
            con = sqlite3.connect(sqlite_db)
            ensure_schema(con)
            n = seed_municipios_to_db(con, csv_path)
            con.close()
            # Invalida cache de municípios para refletir o seed imediatamente
            try:
                MUNICIPIOS_CACHE["db"] = None
                MUNICIPIOS_CACHE["df"] = None
            except Exception:
                pass
            page.snack_bar = ft.SnackBar(ft.Text(f"Seed municípios: {n} linhas → {sqlite_db}"), open=True)
            state["sid_loaded"] = None
            refresh_data()
        except Exception as ex:
            page.snack_bar = ft.SnackBar(ft.Text(f"Falha no seed: {ex}"), open=True)
    seed_picker = ft.FilePicker(on_result=on_seed_result)
    page.overlay.append(seed_picker)
    page.overlay.append(settings_overlay)

    # Helpers de status
    def update_last_update_label():
        try:
            dbp = sqlite_db
            ts = "—"
            if dbp and os.path.exists(dbp):
                con = sqlite3.connect(dbp)
                try:
                    cur = con.cursor()
                    cur.execute("SELECT snapshot_at FROM sync_runs WHERE ok=1 ORDER BY snapshot_id DESC LIMIT 1")
                    row = cur.fetchone()
                    if row and row[0]:
                        ts = str(row[0])
                finally:
                    con.close()
            last_update_text.value = f"Última atualização: {ts}"
            last_update_text.update()
        except Exception:
            last_update_text.value = "Última atualização: —"; last_update_text.update()

    def set_syncing_ui(active: bool):
        try:
            sync_banner.visible = bool(active); # [PATCH] banner update omitido
        except Exception:
            pass

    # Carregamento de dados e mapa

    # [BEGIN PATCH] ETA do Auto Sync (contador regressivo + texto)
    def _fmt_eta(seconds: int) -> str:
        try:
            seconds = max(0, int(seconds))
            m, s = divmod(seconds, 60)
            h, m = divmod(m, 60)
            if h > 0:
                return f"{h:02d}:{m:02d}:{s:02d}"
            return f"{m:02d}:{s:02d}"
        except Exception:
            return ""

    def update_next_sync_text():
        try:
            eta = int(next_sync_state.get('eta', 0))
            # Texto
            next_sync_text.value = f"Próxima sincronização em: {_fmt_eta(eta)}" if eta > 0 else ''
            # Cor: normal, <=10s (âmbar), <=3s (vermelho)
            try:
                if eta <= 0:
                    next_sync_text.color = t['on_surface']
                elif eta <= 3:
                    next_sync_text.color = ft.Colors.RED_700
                elif eta <= 10:
                    next_sync_text.color = ft.Colors.AMBER_800
                else:
                    next_sync_text.color = t['on_surface']
            except Exception:
                pass
            next_sync_text.update()
        except Exception:
            pass


    eta_ticker = RepeatingJob(
        job=lambda: (
            next_sync_state.__setitem__('eta', max(0, int(next_sync_state.get('eta', 0)) - 1)),
            safe_call_ui(page, update_next_sync_text)
        ),
        interval_seconds=1
    )

    # === DB Watchdog: recarrega o mapa quando o arquivo SQLite mudar ===
    _db_sig = {"val": None}
    def _check_db_change():
        import os
        try:
            st = os.stat(sqlite_db)
            cur = (st.st_size, int(st.st_mtime))
        except Exception:
            cur = None
        if cur and _db_sig.get("val") != cur:
            _db_sig["val"] = cur
            try:
                # invalida cache e atualiza UI
                MUNICIPIOS_CACHE["db"] = None
                MUNICIPIOS_CACHE["df"] = None
            except Exception:
                pass
            safe_call_ui(page, refresh_data)

    db_watchdog = RepeatingJob(job=_check_db_change, interval_seconds=3)
    try:
        db_watchdog.start()
    except Exception:
        pass

    clientes_hos_df = clientes_mod_df = clientes_rep_df = None
    merged_df = None
    df_totalizado_estado = df_totalizado_cidade = None
    color_map = None
    state = {}  # estado interno (preload, sid_loaded, settings, etc.)
    def legend_mode_by_width() -> str:
        w = float(getattr(page, "width", 1200) or 1200)
        return "wide" if w >= 900 else "narrow"

    def mapa_height() -> int:
        w = float(getattr(page, "width", 1200) or 1200)
        if w < 600: return 420
        if w < 900: return 520
        if w < 1200: return 580
        return 640

    def set_map(html: str):
        try:
            mapa_container.content = embed_folium_map(page, html, altura=mapa_height())
            mapa_container.update()
        except Exception as e:
            page.snack_bar = ft.SnackBar(ft.Text(f"Mapa indisponível: {e}"), open=True)

    def refresh_map_only():

        try:
            uf_atual = None if ddl_estado.value in (None, "", "Todos") else ddl_estado.value
        except Exception:
            uf_atual = None

        try:
            vm = str(ddl_visao.value or "Pontos").lower()
            if vm.startswith("cidade"): vm = "cidades"
            elif vm.startswith("estado"): vm = "estados"
            elif vm.startswith("choro") or vm.startswith("choropl"): vm = "choropleth"
            else: vm = "pontos"
        except Exception:
            vm = "pontos"

        try:
            html = criar_mapa(
                merged_df=merged_df if merged_df is not None else pd.DataFrame(columns=["lat","lon","ESTADO","FANTASIA","CIDADE"]),
                df_totalizado_estado=df_totalizado_estado if df_totalizado_estado is not None else pd.DataFrame(columns=["ESTADO","Clientes"]),
                color_map=color_map or make_color_map([]),
                estado_selecionado=uf_atual,
                altura=mapa_height(),
                legend_mode=legend_mode_by_width(),
                map_style=map_style_val.get("value","carto-positron"),
                heatmap=(bool(heat_state.get("on", False)) and vm == "pontos"),
                heat_radius=int(heat_state.get("radius", 20)),
                view_mode=vm,
            )
            set_map(html)
            try:
                update_uf_legend()
            except Exception:
                pass
            update_uf_legend()  # [UF LEGEND CALL] Atualiza legenda após render do mapa
        except Exception:
            pass

    def refresh_data(_=None):
        nonlocal clientes_hos_df, clientes_mod_df, clientes_rep_df, merged_df, df_totalizado_estado, df_totalizado_cidade, color_map
        # Se houver payload pré-carregado (I/O feito em background), consome aqui
        _pre_sid = state.get("preloaded_sid")
        _pre_data = state.get("preloaded_data")
        if _pre_sid is not None and _pre_data is not None:
            try:
                data = _pre_data
                sid_wanted = _pre_sid
                # limpa para próximos ciclos
                state.pop("preloaded_sid", None)
                state.pop("preloaded_data", None)
            except Exception:
                data = None
        else:
            data = None  # segue para caminho normal
        # [PATCH] não mostrar banner em refresh_data()
        nonlocal clientes_hos_df, clientes_mod_df, clientes_rep_df, merged_df, df_totalizado_estado, df_totalizado_cidade, color_map, total_container
        # Cache por snapshot: evita recarregar quando nada mudou
        try:
            if snapshot_in_use is None:
                sid_wanted = get_latest_ok_snapshot_id(sqlite_db)
            else:
                sid_wanted = int(snapshot_in_use)
        except Exception:
            sid_wanted = None
        if state.get("sid_loaded") is not None and sid_wanted is not None and state["sid_loaded"] == sid_wanted:
            refresh_map_only()
            update_last_update_label()
        try:
            update_uf_legend()
        except Exception:
            pass
        

        try:
            data = data or load_from_sqlite(sqlite_db, snapshot_id=snapshot_in_use)
            clientes_hos_df = data["clientes_hos_df"]
            clientes_mod_df = data["clientes_mod_df"]
            clientes_rep_df = data["clientes_rep_df"]
            merged_df       = data["merged_df"]
            df_totalizado_estado = data["df_totalizado_estado"]
            df_totalizado_cidade = data["df_totalizado_cidade"]

            estados = sorted(merged_df["ESTADO"].dropna().unique().tolist()) if merged_df is not None else []
            color_map = make_color_map(estados)

            total_clientes_new = int(df_totalizado_estado["Clientes"].sum()) if df_totalizado_estado is not None and not df_totalizado_estado.empty else 0
            total_container.content = ft.Row(alignment=ft.MainAxisAlignment.SPACE_BETWEEN, vertical_alignment=ft.CrossAxisAlignment.CENTER, controls=[
                ft.Row(spacing=12, vertical_alignment=ft.CrossAxisAlignment.CENTER, controls=[
                    ft.Container(content=ft.Icon(ft.Icons.STORE_MALL_DIRECTORY, size=32, color=ft.Colors.BLUE_600),
                                 bgcolor=ft.Colors.with_opacity(0.15, ft.Colors.BLUE_200), padding=10, border_radius=10),
                    ft.Column(spacing=2, tight=True, controls=[
                        ft.Text("Total de Clientes", size=14, color=t['on_surface']),
                        ft.Text(f"{total_clientes_new:,}".replace(",", "."), size=36, weight=ft.FontWeight.BOLD, selectable=False, color=t['on_surface']),
                    ]),
                ]),
                ft.Container(content=ft.Text("visão geral", size=12, color=t['on_surface']), padding=8),
            ])
            total_container.update()

            # Tabelas
            if clientes_rep_df is not None and not clientes_rep_df.empty:
                table_rep.rows = [ft.DataRow(cells=[ft.DataCell(ft.Text(str(r["NOME_REP"]), color=t['on_surface'])),
                                                    ft.DataCell(ft.Text(str(r["QTD_CLIENTES"]), color=t['on_surface']))]) 
                                  for _, r in clientes_rep_df.sort_values("QTD_CLIENTES", ascending=False).iterrows()]
                table_rep.update()
            else:
                table_rep.rows = []; table_rep.update()

            todos_estados_const = ["AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS","MG","PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC","SP","SE","TO"]
            estados_sem = []
            if df_totalizado_estado is not None and not df_totalizado_estado.empty:
                estados_sem = sorted(list(set(todos_estados_const) - set(df_totalizado_estado["ESTADO"])))
            else:
                estados_sem = todos_estados_const
            table_sem.rows = [ft.DataRow(cells=[ft.DataCell(ft.Text(uf, color=t['on_surface']))]) for uf in estados_sem]; table_sem.update()

            if clientes_mod_df is not None and not clientes_mod_df.empty:
                table_mod.rows = [ft.DataRow(cells=[ft.DataCell(ft.Text(str(r["MODULO"]), color=t['on_surface'])), ft.DataCell(ft.Text(str(r["QUANTIDADE"]), color=t['on_surface']))])
                                  for _, r in clientes_mod_df.sort_values("QUANTIDADE", ascending=False).iterrows()]
                table_mod.update()
            else:
                table_mod.rows = []; table_mod.update()

            if df_totalizado_cidade is not None and not df_totalizado_cidade.empty:
                df_cid_sorted = df_totalizado_cidade.sort_values("Clientes", ascending=False)
                table_cid_sample.rows = [ft.DataRow(cells=[ft.DataCell(ft.Text(str(r["UF"]), color=t['on_surface'])),
                                                           ft.DataCell(ft.Text(str(r["CIDADE"]), color=t['on_surface'])),
                                                           ft.DataCell(ft.Text(str(r["Clientes"]), color=t['on_surface'])) ])
                                         for _, r in df_cid_sorted.head(120).iterrows()]
                table_cid_sample.update()
            else:
                table_cid_sample.rows = []; table_cid_sample.update()

            refresh_map_only()
            update_last_update_label()
            state["sid_loaded"] = sid_wanted

        except Exception as ex:
            page.snack_bar = ft.SnackBar(ft.Text(f"Falha ao atualizar: {ex}"), open=True)

    # Sync
        try:
            set_syncing_ui(False)
            # [PATCH] banner update omitido
        except Exception:
            pass

    def _start_sync_thread(auto: bool = False):
        # [BEGIN PATCH] Controle de UI para sync MANUAL/AUTO e ETA
        try:
            set_syncing_ui(True)  # mostrar banner apenas quando sincronizando
        except Exception:
            pass
        try:
            if auto:
                new_iv = int(cfg.get('auto_sync_interval_s', 1800))
                next_sync_state['eta'] = new_iv
                if not eta_ticker.alive():
                    eta_ticker.start()
                safe_call_ui(page, update_next_sync_text)
        except Exception:
            pass
        # [END PATCH]
        if syncing["on"]:
            return
        syncing["on"] = True
        set_syncing_ui(True)

        def _worker():
            nonlocal cfg, sqlite_db, snapshot_in_use
            try:
                sid = run_sync_once(cfg)
                snapshot_in_use = None  # latest
                def _after_ok():
                    state["sid_loaded"] = None
                    refresh_data()
                    page.snack_bar = ft.SnackBar(ft.Text(f"{'Auto ' if auto else ''}Sync concluída. Snapshot {sid} aplicado (latest)."), open=True)
                safe_call_ui(page, _after_ok)
            except Exception as ex:
                safe_call_ui(page, (lambda msg=str(ex): (page.__setattr__('snack_bar', ft.SnackBar(ft.Text(f"Erro na sync: {msg}"), open=True)) or None)))
            finally:
                syncing["on"] = False
                safe_call_ui(page, lambda: set_syncing_ui(False))

        Thread(target=_worker, daemon=True).start()

    def sync_now(_=None):
        # [PATCH] Resetar contador ao clicar em Sincronizar agora
        try:
            if auto_sync and auto_sync.alive():
                new_iv = int(cfg.get('auto_sync_interval_s', 1800))
                next_sync_state['eta'] = new_iv
                if not eta_ticker.alive():
                    eta_ticker.start()
                safe_call_ui(page, update_next_sync_text)
        except Exception:
            pass
        _start_sync_thread(auto=False)

    # Auto Sync
    auto_sync = RepeatingJob(job=lambda: _start_sync_thread(auto=True), interval_seconds=int(cfg.get("auto_sync_interval_s", 1800)))
    if cfg.get("auto_sync_enabled", False):
        auto_sync.start()

        # [PATCH] Iniciar contador na carga do app se o Auto Sync já estiver habilitado
        try:
            next_sync_state['eta'] = int(cfg.get('auto_sync_interval_s', 1800))
            safe_call_ui(page, update_next_sync_text)
            if not eta_ticker.alive():
                eta_ticker.start()
        except Exception:
            pass


    # Layout principal
    nav_card = ft.Container(content=ft.Column([ft.Text("Navegação", size=14, weight=ft.FontWeight.BOLD, color=t['on_surface']),
                                               ft.Row([ddl_estado, ddl_visao, btn_reset], spacing=10, wrap=True)], tight=True, spacing=10),
                            bgcolor=t['surface'], border_radius=12, padding=8)
    mapa_stack = mapa_container
    col_esq  = ft.Column([total_container, rep_card, sem_hos_card], spacing=8, expand=True)
    col_meio = ft.Column([
        ft.Container(bgcolor=t['surface'], border_radius=12, padding=8,
                     content=ft.Column([ft.Row([ft.Text("Mapa de Clientes", size=14, weight=ft.FontWeight.BOLD, color=t['on_surface'])],
                                               alignment=ft.MainAxisAlignment.START),
                                        mapa_stack], spacing=4)),
        ft.Container(
        content=ft.Column([
            ft.Text('Legenda', size=14, weight=ft.FontWeight.BOLD, color=t['on_surface']),
            uf_legend_wrap
        ], spacing=2, tight=True),
        bgcolor=t['surface'], border_radius=12, padding=8
    )], spacing=8, expand=True)
    col_dir  = ft.Column([mod_card, cidades_card, nav_card], spacing=8, expand=True)

    page.add(ft.Column([
        ft.Row([btn_settings], alignment=ft.MainAxisAlignment.END),

        ft.Row([last_update_text, next_sync_text], alignment=ft.MainAxisAlignment.START),
        sync_banner,
        ft.ResponsiveRow(controls=[
            ft.Container(col_esq,  col={"xs":12,"md":12,"lg":3}),
            ft.Container(col_meio, col={"xs":12,"md":12,"lg":7}),
            ft.Container(col_dir,  col={"xs":12,"md":12,"lg":2}),
        ], spacing=12, run_spacing=12)
    ], expand=True, spacing=12))

    page.on_resize = lambda e: (None if state.get('settings_open') else resize_deb.call(lambda: safe_call_ui(page, refresh_map_only)))

    # Primeira render
    def _has_ok_snapshot(db_path: str) -> bool:
        try:
            if not db_path or not os.path.exists(db_path):
                return False
            con = sqlite3.connect(db_path)
            try:
                cur = con.cursor()
                cur.execute("SELECT COUNT(1) FROM sync_runs WHERE ok=1")
                cnt = cur.fetchone()[0] or 0
                return cnt > 0
            finally:
                con.close()
        except Exception:
            return False

    try:
        if not _has_ok_snapshot(sqlite_db):
            update_last_update_label()
            _start_sync_thread(auto=False)
        else:
            refresh_data()
        # aplica auto sync das configurações (já feito acima, mas confirma interval)
        s = load_settings()
        auto_sync.update_interval(int(s.get("auto_sync_interval_s", 1800)))
        if s.get("auto_sync_enabled", False) and not auto_sync.alive():
            auto_sync.start()
    except Exception as ex:
        page.add(ft.Container(bgcolor=t['surface_variant'], border_radius=8, padding=16,
                              content=ft.Column([ft.Text("Erro ao renderizar a interface", size=20, weight=ft.FontWeight.BOLD, color=t['on_surface']),
                                                 ft.Text(str(ex), color=t['on_surface'], selectable=True),
                                                 ft.Text("Verifique as configurações e rode uma sincronização.", color=t['on_surface'])])))

# ====== INÍCIO: bloco de inicialização ======
if __name__ == "__main__":
    import argparse
    import flet as ft

    parser = argparse.ArgumentParser(description="Monitor Cloud Backup (HOS) - Desktop/Web")
    parser.add_argument("--web", action="store_true", help="Inicia como servidor web (acesso via navegador)")
    parser.add_argument("--host", default="0.0.0.0", help="Host para o servidor web (padrão: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8551, help="Porta do servidor web (padrão: 8550)")
    parser.add_argument("--view", default="BROWSER",
                        choices=["BROWSER", "NONE"],
                        help="BROWSER abre o navegador local (útil em dev); NONE só inicia o servidor")
    args = parser.parse_args()

    if args.web:
        # Modo SERVIDOR WEB: usuários acessam por http://host:port
        # view=None evita abrir navegador no servidor; use BROWSER em dev.
        app_view = None if args.view == "NONE" else ft.AppView.WEB_BROWSER
        ft.app(
            target=main,
            view=app_view,
            host=args.host,
            port=args.port,
            assets_dir=str((BASE_DIR / "assets")),
        )
    else:
        # Modo DESKTOP (janela nativa)
        ft.app(
            target=main,
            view=ft.AppView.FLET_APP,  # janela nativa
            assets_dir=str((BASE_DIR / "assets")),
        )
# ====== FIM: bloco de inicialização ======