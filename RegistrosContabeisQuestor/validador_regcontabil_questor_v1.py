
# auditor_movimentos_flet.py
# -*- coding: utf-8 -*-
"""
App Flet: Importa TXT -> SQLite (schema genérico 'movimentos') e exibe KPIs/relatórios.
- Sem uso de 'fato_vendas'. Tudo baseado em 'movimentos' (+ views opcionais).
- UI com filtros por período e por TIPO (VENDA/PAGAMENTO/RECEBIMENTO/CPAGAR/CRECEBER/AJUSTE/TRANSF/...).
- Importação performática (WAL, transações grandes, executemany).
"""

import os
import re
import csv
import sys
import json
import time
import sqlite3
from datetime import datetime
from pathlib import Path

import flet as ft

# --------------------------
# BASE_DIR e estrutura de pastas
# --------------------------
def get_base_dir() -> Path:
    if getattr(sys, 'frozen', False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent

BASE_DIR = get_base_dir()
(BASE_DIR / "config").mkdir(parents=True, exist_ok=True)
(BASE_DIR / "logs").mkdir(parents=True, exist_ok=True)
(BASE_DIR / "export").mkdir(parents=True, exist_ok=True)
(BASE_DIR / "cache").mkdir(parents=True, exist_ok=True)

DB_PATH = str(BASE_DIR / "cache" / "audithos_movimentos.db")
CONF_PATH = str(BASE_DIR / "config" / "settings.json")

# --------------------------
# Settings
# --------------------------
DEFAULT_CONF = {
    "batch_size": 100000,
    "delimiter": ",",
    "encoding": "utf-8",
    "page_size": 200,
}

def load_settings():
    if os.path.exists(CONF_PATH):
        try:
            with open(CONF_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
                DEFAULT_CONF.update(data or {})
        except Exception:
            pass

def save_settings():
    try:
        with open(CONF_PATH, "w", encoding="utf-8") as f:
            json.dump(DEFAULT_CONF, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

load_settings()

# --------------------------
# Regex auxiliares (ajuste conforme seu padrão)
# --------------------------
_re_cliente = re.compile(r"Cliente:\s*(.+?)\s+Cupom:", re.I)
_re_cupom   = re.compile(r"Cupom:\s*([A-Za-z0-9\-_/]+)", re.I)
_re_serie   = re.compile(r"Serie:\s*([A-Za-z0-9\-_/]+)", re.I)

def extrair_campos_historico(hist: str):
    if not isinstance(hist, str):
        return None, None, None
    mc = _re_cliente.search(hist)
    cliente = mc.group(1).strip() if mc else None
    mcp = _re_cupom.search(hist)
    cupom = mcp.group(1).strip() if mcp else None
    ms = _re_serie.search(hist)
    serie = ms.group(1).strip() if ms else None
    return cliente, cupom, serie

# --------------------------
# Normalização
# --------------------------
def to_date_ddmmyyyy(s: str):
    s = (s or "").strip()
    if re.fullmatch(r"\d{8}", s):
        try:
            return datetime.strptime(s, "%d%m%Y").date().isoformat()
        except Exception:
            return None
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except Exception:
            pass
    return None

def to_float(s):
    if s is None:
        return 0.0
    st = str(s).strip()
    try:
        return float(st)
    except Exception:
        pass
    st_try = st.replace(".", "").replace(",", ".")
    try:
        return float(st_try)
    except Exception:
        return 0.0

def define_dc(conta_debito: str, conta_credito: str, valor: float):
    cd = (conta_debito or "").strip()
    cc = (conta_credito or "").strip()
    if cd and cd != "0" and (not cc or cc == "0"):
        return "D", valor, 0.0
    if cc and cc != "0" and (not cd or cd == "0"):
        return "C", 0.0, valor
    return None, 0.0, 0.0

# --------------------------
# Esquema genérico
# --------------------------
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS movimentos (
  id               INTEGER PRIMARY KEY,
  origem           TEXT,
  tipo             TEXT NOT NULL,        -- VENDA | PAGAMENTO | RECEBIMENTO | CPAGAR | CRECEBER | AJUSTE | TRANSF | ...
  data             TEXT NOT NULL,        -- YYYY-MM-DD
  empresa          TEXT,
  documento_id     TEXT,
  documento_tipo   TEXT,
  parcela          TEXT,
  entidade_id      TEXT,
  entidade_nome    TEXT,
  conta            TEXT,
  conta_nome       TEXT,
  centro_custo     TEXT,
  projeto          TEXT,
  historico        TEXT,
  dc               TEXT CHECK(dc IN ('D','C')),
  valor_debito     REAL DEFAULT 0,
  valor_credito    REAL DEFAULT 0,
  valor            REAL DEFAULT 0,
  moeda            TEXT,
  extra_json       TEXT,
  source_file      TEXT,
  line_no          INTEGER,
  hash_linha       TEXT,
  criado_em        TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS ix_mov_data           ON movimentos(data);
CREATE INDEX IF NOT EXISTS ix_mov_tipo           ON movimentos(tipo);
CREATE INDEX IF NOT EXISTS ix_mov_docid          ON movimentos(documento_id);
CREATE INDEX IF NOT EXISTS ix_mov_empresa        ON movimentos(empresa);
CREATE INDEX IF NOT EXISTS ix_mov_dc             ON movimentos(dc);
CREATE INDEX IF NOT EXISTS ix_mov_conta          ON movimentos(conta);
CREATE INDEX IF NOT EXISTS ix_mov_entidade       ON movimentos(entidade_id);
CREATE INDEX IF NOT EXISTS ix_mov_data_docid     ON movimentos(data, documento_id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_mov_hash    ON movimentos(hash_linha);
"""

# Views úteis (opcionais)
VIEWS_SQL = """
CREATE VIEW IF NOT EXISTS vw_vendas AS
SELECT
  data, empresa, documento_id AS cupom_id,
  json_extract(extra_json, '$.cupom')  AS cupom,
  json_extract(extra_json, '$.serie')  AS serie,
  entidade_nome AS cliente,
  conta, conta_nome, dc,
  valor_debito, valor_credito, valor, historico
FROM movimentos
WHERE tipo = 'VENDA';

CREATE VIEW IF NOT EXISTS vw_pagamentos AS
SELECT
  data, empresa, documento_id, entidade_nome AS fornecedor,
  conta, conta_nome, dc,
  valor_debito, valor_credito, valor, historico
FROM movimentos
WHERE tipo = 'PAGAMENTO';

CREATE VIEW IF NOT EXISTS vw_recebimentos AS
SELECT
  data, empresa, documento_id, entidade_nome AS cliente,
  conta, conta_nome, dc,
  valor_debito, valor_credito, valor, historico
FROM movimentos
WHERE tipo = 'RECEBIMENTO';
"""

def init_db(db_path: str):
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=OFF")
    con.execute("PRAGMA temp_store=MEMORY")
    con.execute("PRAGMA cache_size=-200000")
    con.executescript(SCHEMA_SQL)
    con.executescript(VIEWS_SQL)
    return con

def ensure_schema(db_path: str):
    con = sqlite3.connect(db_path)
    try:
        con.executescript(SCHEMA_SQL)
        con.executescript(VIEWS_SQL)
    finally:
        con.close()

def finalize_import(con: sqlite3.Connection):
    try:
        con.execute("PRAGMA synchronous=NORMAL")
        con.execute("ANALYZE;")
        con.execute("PRAGMA optimize;")
    except Exception:
        pass

# --------------------------
# Helpers SQL
# --------------------------
def sql_one(db_path: str, sql: str, params=None):
    con = sqlite3.connect(db_path)
    try:
        cur = con.cursor()
        cur.execute(sql, params or [])
        row = cur.fetchone()
        return row
    except sqlite3.OperationalError as ex:
        if "no such table: movimentos" in str(ex):
            ensure_schema(db_path)
            cur = con.cursor()
            cur.execute(sql, params or [])
            row = cur.fetchone()
            return row
        raise
    finally:
        con.close()

def sql_all(db_path: str, sql: str, params=None):
    con = sqlite3.connect(db_path)
    try:
        cur = con.cursor()
        cur.execute(sql, params or [])
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description] if cur.description else []
        return cols, rows
    except sqlite3.OperationalError as ex:
        if "no such table: movimentos" in str(ex):
            ensure_schema(db_path)
            cur = con.cursor()
            cur.execute(sql, params or [])
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description] if cur.description else []
            return cols, rows
        raise
    finally:
        con.close()

# --------------------------
# Ingestão TXT -> movimentos
# Layout default esperado (mínimo 9 colunas):
# tipo,empresa,data(DDMMAAAA),conta_debito,conta_credito,valor,filler,historico,documento_id
# --------------------------
import hashlib

def hash_linha(source_file, line_no, row):
    base = f"{source_file}|{line_no}|{'|'.join(map(str,row))}"
    return hashlib.sha1(base.encode("utf-8", errors="ignore")).hexdigest()

def bulk_load_txt_to_movimentos(
    txt_path: str, db_path: str, sep=None, encoding=None, batch=None,
    tipo_padrao="VENDA", documento_tipo_padrao="CUPOM", on_progress=None
):
    sep = sep or DEFAULT_CONF["delimiter"]
    encoding = encoding or DEFAULT_CONF["encoding"]
    batch = int(batch or DEFAULT_CONF["batch_size"])

    con = init_db(db_path)
    cur = con.cursor()

    total_lines = 0
    inserted = 0
    started = time.time()

    with open(txt_path, "r", encoding=encoding, newline="") as f:
        reader = csv.reader(f, delimiter=sep, quotechar='"')
        buf = []
        con.execute("BEGIN")
        for i, row in enumerate(reader, start=1):
            total_lines += 1
            if not row or len(row) < 9:
                continue
            _tipo, empresa, data_raw, cde, ccr, valor, _filler, historico, documento_id = row[:9]
            data = to_date_ddmmyyyy(data_raw)
            v = to_float(valor)
            dc, deb, cred = define_dc(cde, ccr, v)

            cliente, cupom, serie = extrair_campos_historico(historico)
            extra = {"cupom": cupom, "serie": serie}
            extra_json = json.dumps(extra, ensure_ascii=False)

            h = hash_linha(txt_path, i, row)
            buf.append((
                "CSV",                         # origem
                (_tipo or tipo_padrao) or "VENDA",
                data or "",
                empresa or "",
                documento_id or "",
                documento_tipo_padrao,
                None,                          # parcela
                None,                          # entidade_id
                cliente,                       # entidade_nome
                (cde or ccr or ""),            # conta (prioridade cde/ccr)
                None,                          # conta_nome (mapear depois)
                None,                          # centro_custo
                None,                          # projeto
                historico or "",
                dc, deb, cred, deb - cred,
                "BRL",
                extra_json,
                str(txt_path),
                i,
                h
            ))
            if len(buf) >= batch:
                cur.executemany("""
                    INSERT OR IGNORE INTO movimentos
                    (origem,tipo,data,empresa,documento_id,documento_tipo,parcela,entidade_id,entidade_nome,
                     conta,conta_nome,centro_custo,projeto,historico,dc,valor_debito,valor_credito,valor,moeda,
                     extra_json,source_file,line_no,hash_linha)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, buf)
                con.commit()
                inserted += len(buf)
                if on_progress:
                    on_progress(inserted, total_lines, time.time() - started)
                buf.clear()

        if buf:
            cur.executemany("""
                INSERT OR IGNORE INTO movimentos
                (origem,tipo,data,empresa,documento_id,documento_tipo,parcela,entidade_id,entidade_nome,
                 conta,conta_nome,centro_custo,projeto,historico,dc,valor_debito,valor_credito,valor,moeda,
                 extra_json,source_file,line_no,hash_linha)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, buf)
            con.commit()
            inserted += len(buf)
            buf.clear()

    finalize_import(con)
    con.close()
    return inserted, total_lines, time.time() - started

def truncate_movimentos(db_path: str):
    con = sqlite3.connect(db_path)
    with con:
        con.execute("DELETE FROM movimentos;")
        con.execute("VACUUM;")
    con.close()

# --------------------------
# Consultas (genéricas)
# --------------------------
def get_kpis(db_path: str, data_ini=None, data_fim=None, tipo=None):
    qs = "SELECT COALESCE(SUM(valor_debito),0), COALESCE(SUM(valor_credito),0), COALESCE(SUM(valor_debito-valor_credito),0) FROM movimentos WHERE 1=1"
    params=[]
    if data_ini:
        qs += " AND data >= ?"; params.append(data_ini)
    if data_fim:
        qs += " AND data <= ?"; params.append(data_fim)
    if tipo and tipo != "(Todos)":
        qs += " AND tipo = ?"; params.append(tipo)
    return sql_one(db_path, qs, params) or (0,0,0)

def query_preview(db_path: str, data_ini=None, data_fim=None, tipo=None, limit=200, offset=0):
    sql = (
        "SELECT data,empresa,tipo,documento_id,documento_tipo,entidade_nome,conta,dc,"
        "valor_debito,valor_credito,valor,historico "
        "FROM movimentos WHERE 1=1"
    )
    params=[]
    if data_ini:
        sql += " AND data >= ?"; params.append(data_ini)
    if data_fim:
        sql += " AND data <= ?"; params.append(data_fim)
    if tipo and tipo != "(Todos)":
        sql += " AND tipo = ?"; params.append(tipo)
    sql += " ORDER BY data, empresa, documento_id, tipo, dc DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    return sql_all(db_path, sql, params)

def query_por_dia(db_path: str, data_ini=None, data_fim=None, tipo=None):
    sql = (
        "SELECT data,"
        "ROUND(SUM(valor_debito),2) AS debitos,"
        "ROUND(SUM(valor_credito),2) AS creditos,"
        "ROUND(SUM(valor_debito-valor_credito),2) AS diferenca "
        "FROM movimentos WHERE 1=1"
    )
    params=[]
    if data_ini:
        sql += " AND data >= ?"; params.append(data_ini)
    if data_fim:
        sql += " AND data <= ?"; params.append(data_fim)
    if tipo and tipo != "(Todos)":
        sql += " AND tipo = ?"; params.append(tipo)
    sql += " GROUP BY data ORDER BY data"
    return sql_all(db_path, sql, params)

def query_por_documento_vendas(db_path: str, data_ini=None, data_fim=None):
    sql = (
        "SELECT data, documento_id,"
        "json_extract(extra_json, '$.cupom') AS cupom,"
        "json_extract(extra_json, '$.serie') AS serie,"
        "entidade_nome AS cliente,"
        "ROUND(SUM(valor_debito),2) AS debitos,"
        "ROUND(SUM(valor_credito),2) AS creditos,"
        "COUNT(*) AS movimentos,"
        "ROUND(SUM(valor_debito-valor_credito),2) AS diferenca "
        "FROM movimentos WHERE tipo='VENDA'"
    )
    params=[]
    if data_ini:
        sql += " AND data >= ?"; params.append(data_ini)
    if data_fim:
        sql += " AND data <= ?"; params.append(data_fim)
    sql += " GROUP BY data, documento_id, cupom, serie, cliente ORDER BY data, documento_id"
    return sql_all(db_path, sql, params)

def query_por_conta(db_path: str, data_ini=None, data_fim=None, tipo=None):
    sql = (
        "SELECT conta, COALESCE(conta_nome,'') AS conta_nome,"
        "ROUND(SUM(valor_debito),2) AS debitos,"
        "ROUND(SUM(valor_credito),2) AS creditos,"
        "ROUND(SUM(valor),2) AS saldo,"
        "COUNT(*) AS movimentos "
        "FROM movimentos WHERE 1=1"
    )
    params=[]
    if data_ini:
        sql += " AND data >= ?"; params.append(data_ini)
    if data_fim:
        sql += " AND data <= ?"; params.append(data_fim)
    if tipo and tipo != "(Todos)":
        sql += " AND tipo = ?"; params.append(tipo)
    sql += " GROUP BY conta, conta_nome ORDER BY conta"
    return sql_all(db_path, sql, params)

def query_por_entidade(db_path: str, data_ini=None, data_fim=None, tipo=None):
    sql = (
        "SELECT COALESCE(entidade_nome,'(sem entidade)') AS entidade,"
        "ROUND(SUM(valor_debito),2) AS debitos,"
        "ROUND(SUM(valor_credito),2) AS creditos,"
        "ROUND(SUM(valor),2) AS saldo,"
        "COUNT(DISTINCT documento_id) AS documentos "
        "FROM movimentos WHERE 1=1"
    )
    params=[]
    if data_ini:
        sql += " AND data >= ?"; params.append(data_ini)
    if data_fim:
        sql += " AND data <= ?"; params.append(data_fim)
    if tipo and tipo != "(Todos)":
        sql += " AND tipo = ?"; params.append(tipo)
    sql += " GROUP BY entidade ORDER BY entidade"
    return sql_all(db_path, sql, params)

def query_duplicidades(db_path: str, data_ini=None, data_fim=None, tipo=None, limit=5000):
    sql = """
WITH d AS (
  SELECT data, conta, valor, documento_id, dc, COUNT(*) AS qtd
  FROM movimentos
  WHERE 1=1
    /*__FILTROS__*/
  GROUP BY data, conta, valor, documento_id, dc
  HAVING COUNT(*) > 1
)
SELECT m.*
FROM movimentos m
JOIN d ON d.data=m.data AND d.conta=m.conta AND d.valor=m.valor
       AND d.documento_id=m.documento_id AND d.dc=m.dc
LIMIT ?
""".strip()

    filtros = []
    params = []
    if data_ini:
        filtros.append("AND data >= ?"); params.append(data_ini)
    if data_fim:
        filtros.append("AND data <= ?"); params.append(data_fim)
    if tipo and tipo != "(Todos)":
        filtros.append("AND tipo = ?"); params.append(tipo)

    sql = sql.replace("/*__FILTROS__*/", ("\n    " + "\n    ".join(filtros)) if filtros else "")
    params.append(limit)
    return sql_all(db_path, sql, params)

# --------------------------
# Exportação CSV
# --------------------------
def export_to_csv(cols, rows, out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=";", quotechar='"')
        if cols:
            w.writerow(cols)
        for r in rows:
            w.writerow(r)

# --------------------------
# UI com Flet
# --------------------------

try:
    C = ft.Colors  # versões novas
except AttributeError:
    C = ft.colors  # fallback

try:
    I = ft.Icons
except AttributeError:
    I = ft.icons

def run_app():
    def main(page: ft.Page):
        page.title = "AuditHOS - Movimentos (Genérico)"
        page.horizontal_alignment = ft.CrossAxisAlignment.STRETCH
        page.vertical_alignment = ft.MainAxisAlignment.START
        page.theme_mode = ft.ThemeMode.DARK
        page.window_min_width = 1100
        page.window_min_height = 700

        ensure_schema(DB_PATH)

        # Controles
        txt_path = ft.TextField(label="Arquivo TXT (delimitado)", expand=True, read_only=True)
        btn_pick = ft.ElevatedButton("Selecionar TXT", icon=I.UPLOAD_FILE)
        btn_import = ft.FilledButton("Importar", icon=I.DOWNLOAD)
        btn_truncate = ft.OutlinedButton("Limpar Base", icon=I.DELETE)

        sep_dd = ft.Dropdown(
            label="Delimitador",
            options=[ft.dropdown.Option(","), ft.dropdown.Option(";"), ft.dropdown.Option("|"), ft.dropdown.Option("\t")],
            value=str(DEFAULT_CONF["delimiter"]),
            width=120,
        )
        enc_dd = ft.Dropdown(
            label="Encoding",
            options=[ft.dropdown.Option("utf-8"), ft.dropdown.Option("latin-1"), ft.dropdown.Option("cp1252")],
            value=str(DEFAULT_CONF["encoding"]),
            width=140,
        )
        batch_tf = ft.TextField(label="Batch", value=str(DEFAULT_CONF["batch_size"]), width=120)
        page_sz_tf = ft.TextField(label="Page size", value=str(DEFAULT_CONF["page_size"]), width=120)

        # Filtros
        data_ini = ft.TextField(label="Data início (YYYY-MM-DD)", width=180)
        data_fim = ft.TextField(label="Data fim (YYYY-MM-DD)", width=180)
        tipo_dd = ft.Dropdown(
            label="Tipo",
            options=[ft.dropdown.Option("(Todos)"), ft.dropdown.Option("VENDA"), ft.dropdown.Option("PAGAMENTO"),
                     ft.dropdown.Option("RECEBIMENTO"), ft.dropdown.Option("CPAGAR"), ft.dropdown.Option("CRECEBER"),
                     ft.dropdown.Option("AJUSTE"), ft.dropdown.Option("TRANSF")],
            value="(Todos)",
            width=180,
        )
        doc_tipo_dd = ft.Dropdown(
            label="Documento Tipo (para importação)",
            options=[ft.dropdown.Option("CUPOM"), ft.dropdown.Option("NFE"), ft.dropdown.Option("BOLETO"), ft.dropdown.Option("NFSE"), ft.dropdown.Option("OUTRO")],
            value="CUPOM",
            width=180,
        )

        prog = ft.ProgressBar(width=400, visible=False)
        prog_txt = ft.Text("", size=12)

        # Tabelas com coluna placeholder (Flet exige ao menos 1)
        table_preview = ft.DataTable(columns=[ft.DataColumn(ft.Text("Carregando..."))], rows=[], heading_row_color=C.with_opacity(0.08, C.BLUE) if hasattr(C, "with_opacity") else None)
        table_dia = ft.DataTable(columns=[ft.DataColumn(ft.Text("Carregando..."))], rows=[], heading_row_color=C.with_opacity(0.08, C.BLUE) if hasattr(C, "with_opacity") else None)
        table_doc_vendas = ft.DataTable(columns=[ft.DataColumn(ft.Text("Carregando..."))], rows=[], heading_row_color=C.with_opacity(0.08, C.BLUE) if hasattr(C, "with_opacity") else None)
        table_conta = ft.DataTable(columns=[ft.DataColumn(ft.Text("Carregando..."))], rows=[], heading_row_color=C.with_opacity(0.08, C.BLUE) if hasattr(C, "with_opacity") else None)
        table_entidade = ft.DataTable(columns=[ft.DataColumn(ft.Text("Carregando..."))], rows=[], heading_row_color=C.with_opacity(0.08, C.BLUE) if hasattr(C, "with_opacity") else None)
        table_dups = ft.DataTable(columns=[ft.DataColumn(ft.Text("Carregando..."))], rows=[], heading_row_color=C.with_opacity(0.08, C.RED) if hasattr(C, "with_opacity") else None)

        kpi_deb = ft.Text("0,00", size=24, weight=ft.FontWeight.BOLD, color=C.GREEN if hasattr(C, "GREEN") else None)
        kpi_cred = ft.Text("0,00", size=24, weight=ft.FontWeight.BOLD, color=C.AMBER if hasattr(C, "AMBER") else None)
        kpi_diff = ft.Text("0,00", size=24, weight=ft.FontWeight.BOLD, color=C.CYAN if hasattr(C, "CYAN") else None)

        offset = 0

        def save_conf_from_ui():
            try:
                DEFAULT_CONF["delimiter"] = sep_dd.value
                DEFAULT_CONF["encoding"] = enc_dd.value
                DEFAULT_CONF["batch_size"] = int(batch_tf.value or "100000")
                DEFAULT_CONF["page_size"] = int(page_sz_tf.value or "200")
                save_settings()
            except Exception:
                pass

        def fmt_money(v):
            try:
                return f"{float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            except Exception:
                return str(v)

        def set_table(dt: ft.DataTable, cols, rows, max_rows=1000):
            dt.columns = [ft.DataColumn(ft.Text(c)) for c in cols]
            out_rows = []
            for r in rows[:max_rows]:
                cells = [ft.DataCell(ft.Text("" if v is None else str(v))) for v in r]
                out_rows.append(ft.DataRow(cells=cells))
            dt.rows = out_rows

        def refresh_kpis(_=None):
            save_conf_from_ui()
            td, tc, df = get_kpis(DB_PATH, data_ini.value or None, data_fim.value or None, tipo_dd.value)
            kpi_deb.value = fmt_money(td)
            kpi_cred.value = fmt_money(tc)
            kpi_diff.value = fmt_money(df)
            page.update()

        def run_query_preview(_=None):
            nonlocal offset
            save_conf_from_ui()
            limit = int(DEFAULT_CONF["page_size"])
            cols, rows = query_preview(DB_PATH, data_ini.value or None, data_fim.value or None, tipo_dd.value, limit=limit, offset=offset)
            set_table(table_preview, cols, rows)
            page.update()

        def next_page(_=None):
            nonlocal offset
            offset += int(DEFAULT_CONF["page_size"])
            run_query_preview()

        def prev_page(_=None):
            nonlocal offset
            offset = max(0, offset - int(DEFAULT_CONF["page_size"]))
            run_query_preview()

        def run_query_por_dia(_=None):
            cols, rows = query_por_dia(DB_PATH, data_ini.value or None, data_fim.value or None, tipo_dd.value)
            set_table(table_dia, cols, rows)
            page.update()

        def run_query_por_documento_vendas(_=None):
            cols, rows = query_por_documento_vendas(DB_PATH, data_ini.value or None, data_fim.value or None)
            set_table(table_doc_vendas, cols, rows)
            page.update()

        def run_query_por_conta(_=None):
            cols, rows = query_por_conta(DB_PATH, data_ini.value or None, data_fim.value or None, tipo_dd.value)
            set_table(table_conta, cols, rows)
            page.update()

        def run_query_por_entidade(_=None):
            cols, rows = query_por_entidade(DB_PATH, data_ini.value or None, data_fim.value or None, tipo_dd.value)
            set_table(table_entidade, cols, rows)
            page.update()

        def run_query_dups(_=None):
            cols, rows = query_duplicidades(DB_PATH, data_ini.value or None, data_fim.value or None, tipo_dd.value, limit=5000)
            set_table(table_dups, cols, rows)
            page.update()

        def export_current(dt: ft.DataTable, default_name: str):
            if not dt.columns:
                page.snack_bar = ft.SnackBar(ft.Text("Nada para exportar"), open=True)
                page.update()
                return
            cols = [c.label.value for c in dt.columns]
            rows = [[cell.content.value for cell in dr.cells] for dr in dt.rows]
            out_path = BASE_DIR / "export" / f"{default_name}.csv"
            export_to_csv(cols, rows, out_path)
            page.snack_bar = ft.SnackBar(ft.Text(f"Exportado: {out_path}"), open=True)
            page.update()

        file_picker = ft.FilePicker(on_result=lambda e: setattr(txt_path, "value", (e.files[0].path if e.files else "")) or page.update())
        page.overlay.append(file_picker)

        def on_pick(_):
            file_picker.pick_files(allow_multiple=False)

        def on_truncate(_):
            try:
                truncate_movimentos(DB_PATH)
                ensure_schema(DB_PATH)
                page.snack_bar = ft.SnackBar(ft.Text("Base limpa com sucesso."), open=True)
                refresh_kpis()
            except Exception as ex:
                page.snack_bar = ft.SnackBar(ft.Text(f"Erro ao limpar base: {ex}"), open=True)
            page.update()

        def on_import(_):
            save_conf_from_ui()
            if not txt_path.value:
                page.snack_bar = ft.SnackBar(ft.Text("Selecione um arquivo TXT."), open=True)
                page.update()
                return
            prog.visible = True
            prog.value = None
            prog_txt.value = "Importando..."
            page.update()

            def _progress(inserted, total_lines, elapsed):
                prog.value = None
                prog_txt.value = f"Inseridos: {inserted:,} / Lidos: {total_lines:,} | {elapsed:.1f}s"
                page.update()

            try:
                ins, tot, sec = bulk_load_txt_to_movimentos(
                    txt_path.value, DB_PATH,
                    sep=sep_dd.value,
                    encoding=enc_dd.value,
                    batch=int(batch_tf.value or "100000"),
                    tipo_padrao=tipo_dd.value if tipo_dd.value != "(Todos)" else "VENDA",
                    documento_tipo_padrao=doc_tipo_dd.value,
                    on_progress=_progress
                )
                prog.visible = False
                prog_txt.value = f"Concluído: {ins:,}/{tot:,} linhas em {sec:.1f}s"
                page.snack_bar = ft.SnackBar(ft.Text("Importação finalizada."), open=True)
                refresh_kpis()
                run_query_preview()
                run_query_por_dia()
                run_query_por_documento_vendas()
                run_query_por_conta()
                run_query_por_entidade()
                run_query_dups()
            except Exception as ex:
                prog.visible = False
                prog_txt.value = f"Erro: {ex}"
                page.snack_bar = ft.SnackBar(ft.Text(f"Erro na importação: {ex}"), open=True)
            page.update()

        btn_pick.on_click = on_pick
        btn_import.on_click = on_import
        btn_truncate.on_click = on_truncate

        toolbar = ft.Row(
            controls=[
                txt_path, btn_pick,
                sep_dd, enc_dd, batch_tf, page_sz_tf,
                tipo_dd, doc_tipo_dd,
                btn_import, btn_truncate
            ],
            alignment=ft.MainAxisAlignment.START,
            vertical_alignment=ft.CrossAxisAlignment.CENTER,
            spacing=10,
        )

        filtros = ft.Row(controls=[data_ini, data_fim,
                                   ft.ElevatedButton("Aplicar Filtros", icon=I.FILTER_ALT, on_click=lambda e: (refresh_kpis(), run_query_preview(), run_query_por_dia(), run_query_por_documento_vendas(), run_query_por_conta(), run_query_por_entidade(), run_query_dups()))],
                         spacing=10)

        kpis_row = ft.Row([
            ft.Card(content=ft.Container(content=ft.Column([ft.Text("Débitos"), kpi_deb], spacing=4), padding=12), elevation=2),
            ft.Card(content=ft.Container(content=ft.Column([ft.Text("Créditos"), kpi_cred], spacing=4), padding=12), elevation=2),
            ft.Card(content=ft.Container(content=ft.Column([ft.Text("Diferença (D-C)"), kpi_diff], spacing=4), padding=12), elevation=2),
        ], spacing=16)

        tab_preview = ft.Tab(
            text="Preview",
            content=ft.Column([
                ft.Row([ft.OutlinedButton("<< Página", on_click=prev_page),
                        ft.OutlinedButton("Página >>", on_click=next_page),
                        ft.FilledButton("Exportar CSV", icon=I.SAVE, on_click=lambda e: export_current(table_preview, "preview"))],
                       spacing=10),
                table_preview
            ], expand=True)
        )
        tab_dia = ft.Tab(text="Por Dia", content=ft.Column([
            ft.Row([ft.FilledButton("Exportar CSV", icon=I.SAVE, on_click=lambda e: export_current(table_dia, "por_dia"))]),
            table_dia
        ], expand=True))
        tab_doc_vendas = ft.Tab(text="Vendas por Documento", content=ft.Column([
            ft.Row([ft.FilledButton("Exportar CSV", icon=I.SAVE, on_click=lambda e: export_current(table_doc_vendas, "vendas_por_documento"))]),
            table_doc_vendas
        ], expand=True))
        tab_conta = ft.Tab(text="Por Conta", content=ft.Column([
            ft.Row([ft.FilledButton("Exportar CSV", icon=I.SAVE, on_click=lambda e: export_current(table_conta, "por_conta"))]),
            table_conta
        ], expand=True))
        tab_entidade = ft.Tab(text="Por Entidade", content=ft.Column([
            ft.Row([ft.FilledButton("Exportar CSV", icon=I.SAVE, on_click=lambda e: export_current(table_entidade, "por_entidade"))]),
            table_entidade
        ], expand=True))
        tab_dups = ft.Tab(text="Duplicidades", content=ft.Column([
            ft.Row([ft.FilledButton("Exportar CSV", icon=I.SAVE, on_click=lambda e: export_current(table_dups, "duplicidades"))]),
            table_dups
        ], expand=True))

        tabs = ft.Tabs(tabs=[tab_preview, tab_dia, tab_doc_vendas, tab_conta, tab_entidade, tab_dups], expand=True)

        page.add(
            ft.AppBar(title=ft.Text("AuditHOS – Movimentos (Genérico)"), center_title=False),
            ft.Container(content=toolbar, padding=10),
            ft.Container(content=ft.Row([prog, prog_txt], spacing=10), padding=10),
            ft.Container(content=filtros, padding=10),
            ft.Container(content=kpis_row, padding=10),
            ft.Container(content=tabs, padding=10, expand=True),
        )

        # Primeira renderização (DB pode estar vazio, mas schema existe)
        refresh_kpis()
        run_query_preview()
        run_query_por_dia()
        run_query_por_documento_vendas()
        run_query_por_conta()
        run_query_por_entidade()
        run_query_dups()

    ft.app(target=main)

if __name__ == "__main__":
    run_app()
