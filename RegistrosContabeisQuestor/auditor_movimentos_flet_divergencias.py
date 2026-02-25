
# auditor_movimentos_flet_divergencias.py
# -*- coding: utf-8 -*-
"""
App Flet para importar TXT genérico -> SQLite (tabela 'movimentos')
e exibir abas: Preview, Por Conta, Por Entidade, Divergência D x C.
- Sem uso de 'fato_vendas'
- Sem paginação; com barras de rolagem via ListView
- Executa consultas automaticamente após terminar a importação
"""
import os, re, csv, sys, json, time, sqlite3, hashlib
from datetime import datetime
from pathlib import Path
import flet as ft

# ------------------------- Util e Config -------------------------

def get_base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent

BASE_DIR = get_base_dir()
for sub in ("config", "logs", "export", "cache"):
    (BASE_DIR / sub).mkdir(parents=True, exist_ok=True)

DB_PATH = str(BASE_DIR / "cache" / "audithos_movimentos.db")
CONF_PATH = str(BASE_DIR / "config" / "settings.json")
DEFAULT_CONF = {"batch_size": 100000, "delimiter": ",", "encoding": "utf-8"}

def load_settings():
    if os.path.exists(CONF_PATH):
        try:
            data = json.load(open(CONF_PATH, "r", encoding="utf-8"))
            if isinstance(data, dict):
                DEFAULT_CONF.update(data)
        except Exception:
            pass

def save_settings():
    try:
        with open(CONF_PATH, "w", encoding="utf-8") as f:
            json.dump(DEFAULT_CONF, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

load_settings()

# ------------------------- Parse Helpers -------------------------

_re_cliente = re.compile(r"Cliente:\s*(.+?)\s+(?:Cupom:|$)", re.I)
_re_cupom   = re.compile(r"Cupom:\s*([A-Za-z0-9\-_/]+)", re.I)
_re_serie   = re.compile(r"Serie:\s*([A-Za-z0-9\-_/]+)", re.I)

def extrair_campos_historico(hist: str):
    if not isinstance(hist, str):
        return None, None, None
    m1 = _re_cliente.search(hist)
    m2 = _re_cupom.search(hist)
    m3 = _re_serie.search(hist)
    cliente = m1.group(1).strip() if m1 else None
    cupom = m2.group(1).strip() if m2 else None
    serie = m3.group(1).strip() if m3 else None
    return cliente, cupom, serie

def to_date_ddmmyyyy(s: str):
    s = (s or "").strip()
    # 31012025
    if re.fullmatch(r"\d{8}", s):
        try: return datetime.strptime(s, "%d%m%Y").date().isoformat()
        except Exception: pass
    # 31/01/2025
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try: return datetime.strptime(s, fmt).date().isoformat()
        except Exception: pass
    return None

def to_float(s):
    if s is None: return 0.0
    st = str(s).strip()
    try: return float(st)
    except Exception: pass
    st2 = st.replace(".", "").replace(",", ".")
    try: return float(st2)
    except Exception: return 0.0

def define_dc(conta_debito: str, conta_credito: str, valor: float):
    cd = (conta_debito or "").strip()
    cc = (conta_credito or "").strip()
    if cd and cd != "0" and (not cc or cc == "0"):
        return "D", valor, 0.0
    if cc and cc != "0" and (not cd or cd == "0"):
        return "C", 0.0, valor
    return None, 0.0, 0.0

def hash_linha(source_file, line_no, row):
    base = f"{source_file}|{line_no}|{'|'.join(map(str,row))}"
    return hashlib.sha1(base.encode("utf-8", errors="ignore")).hexdigest()

# ------------------------- SQLite Schema -------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS movimentos (
  id               INTEGER PRIMARY KEY,
  origem           TEXT,
  tipo             TEXT NOT NULL,
  data             TEXT NOT NULL,
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

def ensure_schema(db_path: str):
    con = sqlite3.connect(db_path)
    try:
        con.executescript(SCHEMA_SQL)
    finally:
        con.close()

def init_db(db_path: str):
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=OFF")
    con.execute("PRAGMA temp_store=MEMORY")
    con.execute("PRAGMA cache_size=-200000")
    con.executescript(SCHEMA_SQL)
    return con

def finalize_import(con: sqlite3.Connection):
    try:
        con.execute("PRAGMA synchronous=NORMAL")
        con.execute("ANALYZE;")
        con.execute("PRAGMA optimize;")
    except Exception:
        pass

def sql_one(db_path: str, sql: str, params=None):
    con = sqlite3.connect(db_path)
    try:
        cur = con.cursor()
        cur.execute(sql, params or [])
        return cur.fetchone()
    except sqlite3.OperationalError as ex:
        if "no such table: movimentos" in str(ex):
            ensure_schema(db_path)
            cur = con.cursor()
            cur.execute(sql, params or [])
            return cur.fetchone()
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

# ------------------------- Importação TXT -------------------------

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
    start = time.time()

    with open(txt_path, "r", encoding=encoding, newline="") as f:
        reader = csv.reader(f, delimiter=sep, quotechar='"')
        buf = []
        con.execute("BEGIN")
        for i, row in enumerate(reader, start=1):
            total_lines += 1
            # Layout mínimo esperado: 9 colunas
            # tipo, empresa, data, conta_debito, conta_credito, valor, (filler), historico, documento_id
            if not row or len(row) < 9:
                continue

            _tipo, empresa, data_raw, cde, ccr, valor, _filler, historico, documento_id = row[:9]
            data = to_date_ddmmyyyy(data_raw)
            v = to_float(valor)
            dc, deb, cred = define_dc(cde, ccr, v)
            cliente, cupom, serie = extrair_campos_historico(historico)
            extra_json = json.dumps({"cupom": cupom, "serie": serie}, ensure_ascii=False)

            buf.append((
                "CSV",
                (_tipo or tipo_padrao) or "VENDA",
                data or "",
                empresa or "",
                documento_id or "",
                documento_tipo_padrao,
                None,     # parcela
                None,     # entidade_id
                cliente,  # entidade_nome
                (cde or ccr or ""),
                None, None, None,        # conta_nome, centro_custo, projeto
                historico or "",
                dc, deb, cred, deb - cred,
                "BRL",
                extra_json,
                str(txt_path),
                i,
                hash_linha(txt_path, i, row)
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
                    on_progress(inserted, total_lines, time.time() - start)
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
    return inserted, total_lines, time.time() - start

def truncate_movimentos(db_path: str):
    con = sqlite3.connect(db_path)
    with con:
        con.execute("DELETE FROM movimentos;")
        con.execute("VACUUM;")
    con.close()

# ------------------------- Consultas -------------------------

def get_kpis(db_path: str, data_ini=None, data_fim=None, tipo=None):
    qs = "SELECT COALESCE(SUM(valor_debito),0), COALESCE(SUM(valor_credito),0), COALESCE(SUM(valor_debito-valor_credito),0) FROM movimentos WHERE 1=1"
    params=[]
    if data_ini: qs += " AND data >= ?"; params.append(data_ini)
    if data_fim: qs += " AND data <= ?"; params.append(data_fim)
    if tipo and tipo != "(Todos)": qs += " AND tipo = ?"; params.append(tipo)
    return sql_one(db_path, qs, params) or (0,0,0)

def query_preview(db_path: str, data_ini=None, data_fim=None, tipo=None):
    sql = (
        "SELECT data,empresa,tipo,documento_id,documento_tipo,entidade_nome,conta,dc,"
        "ROUND(valor_debito,2) AS valor_debito,"
        "ROUND(valor_credito,2) AS valor_credito,"
        "ROUND(valor,2) AS saldo,"
        "historico "
        "FROM movimentos WHERE 1=1"
    )
    params=[]
    if data_ini: sql += " AND data >= ?"; params.append(data_ini)
    if data_fim: sql += " AND data <= ?"; params.append(data_fim)
    if tipo and tipo != "(Todos)": sql += " AND tipo = ?"; params.append(tipo)
    sql += " ORDER BY data, empresa, documento_id, tipo, dc DESC"
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
    if data_ini: sql += " AND data >= ?"; params.append(data_ini)
    if data_fim: sql += " AND data <= ?"; params.append(data_fim)
    if tipo and tipo != "(Todos)": sql += " AND tipo = ?"; params.append(tipo)
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
    if data_ini: sql += " AND data >= ?"; params.append(data_ini)
    if data_fim: sql += " AND data <= ?"; params.append(data_fim)
    if tipo and tipo != "(Todos)": sql += " AND tipo = ?"; params.append(tipo)
    sql += " GROUP BY entidade ORDER BY entidade"
    return sql_all(db_path, sql, params)

def query_divergencias(db_path: str, data_ini=None, data_fim=None, tipo=None, threshold=0.0):
    sql = (
        "SELECT data, empresa, documento_id, "
        "ROUND(SUM(valor_debito),2) AS debitos, "
        "ROUND(SUM(valor_credito),2) AS creditos, "
        "ROUND(SUM(valor_debito-valor_credito),2) AS diferenca, "
        "COUNT(*) AS movimentos "
        "FROM movimentos WHERE 1=1"
    )
    params=[]
    if data_ini: sql += " AND data >= ?"; params.append(data_ini)
    if data_fim: sql += " AND data <= ?"; params.append(data_fim)
    if tipo and tipo != "(Todos)": sql += " AND tipo = ?"; params.append(tipo)
    sql += " GROUP BY data, empresa, documento_id"
    sql += " HAVING ABS(SUM(valor_debito) - SUM(valor_credito)) > ?"
    params.append(float(threshold))
    sql += " ORDER BY data, empresa, documento_id"
    return sql_all(db_path, sql, params)

def export_to_csv(cols, rows, out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=";", quotechar='"')
        if cols: w.writerow(cols)
        for r in rows: w.writerow(r)

# ------------------------- UI (Flet) -------------------------

def run_app():
    def main(page: ft.Page):
        page.title = "AuditHOS – Movimentos (Genérico)"
        page.theme_mode = ft.ThemeMode.DARK
        page.window_min_width = 1100
        page.window_min_height = 700
        page.scroll = ft.ScrollMode.AUTO
        page.horizontal_alignment = ft.CrossAxisAlignment.STRETCH
        page.vertical_alignment = ft.MainAxisAlignment.START

        ensure_schema(DB_PATH)

        # Controles topo
        txt_path = ft.TextField(label="Arquivo TXT (delimitado)", expand=True, read_only=True)
        btn_pick = ft.ElevatedButton("Selecionar TXT", icon=ft.Icons.UPLOAD_FILE)
        btn_import = ft.FilledButton("Importar", icon=ft.Icons.DOWNLOAD)
        btn_truncate = ft.OutlinedButton("Limpar Base", icon=ft.Icons.DELETE)

        sep_dd = ft.Dropdown(label="Delimitador", width=120,
                             options=[ft.dropdown.Option(","), ft.dropdown.Option(";"), ft.dropdown.Option("|"), ft.dropdown.Option("\t")],
                             value=str(DEFAULT_CONF["delimiter"]))
        enc_dd = ft.Dropdown(label="Encoding", width=140,
                             options=[ft.dropdown.Option("utf-8"), ft.dropdown.Option("latin-1"), ft.dropdown.Option("cp1252")],
                             value=str(DEFAULT_CONF["encoding"]))
        batch_tf = ft.TextField(label="Batch", width=120, value=str(DEFAULT_CONF["batch_size"]))

        data_ini = ft.TextField(label="Data início (YYYY-MM-DD)", width=180)
        data_fim = ft.TextField(label="Data fim (YYYY-MM-DD)", width=180)
        tipo_dd = ft.Dropdown(label="Tipo", width=180,
                              options=[ft.dropdown.Option("(Todos)"), ft.dropdown.Option("VENDA"), ft.dropdown.Option("PAGAMENTO"),
                                       ft.dropdown.Option("RECEBIMENTO"), ft.dropdown.Option("CPAGAR"), ft.dropdown.Option("CRECEBER"),
                                       ft.dropdown.Option("AJUSTE"), ft.dropdown.Option("TRANSF")],
                              value="(Todos)")
        doc_tipo_dd = ft.Dropdown(label="Documento Tipo (importação)", width=160,
                                  options=[ft.dropdown.Option("CUPOM"), ft.dropdown.Option("NFE"),
                                           ft.dropdown.Option("BOLETO"), ft.dropdown.Option("NFSE"),
                                           ft.dropdown.Option("OUTRO")],
                                  value="CUPOM")

        prog = ft.ProgressBar(width=400, visible=False)
        prog_txt = ft.Text("", size=12)

        # Tabelas (mantém ao menos 1 coluna para evitar Assert de Flet)
        table_preview  = ft.DataTable(columns=[ft.DataColumn(ft.Text("..."))], rows=[])
        table_conta    = ft.DataTable(columns=[ft.DataColumn(ft.Text("..."))], rows=[])
        table_entidade = ft.DataTable(columns=[ft.DataColumn(ft.Text("..."))], rows=[])
        table_div      = ft.DataTable(columns=[ft.DataColumn(ft.Text("..."))], rows=[])

        # KPIs
        kpi_deb  = ft.Text("0,00", size=24, weight=ft.FontWeight.BOLD, color=ft.Colors.GREEN)
        kpi_cred = ft.Text("0,00", size=24, weight=ft.FontWeight.BOLD, color=ft.Colors.AMBER)
        kpi_diff = ft.Text("0,00", size=24, weight=ft.FontWeight.BOLD, color=ft.Colors.CYAN)

        # Helpers
        def save_conf_from_ui():
            try:
                DEFAULT_CONF["delimiter"] = sep_dd.value
                DEFAULT_CONF["encoding"] = enc_dd.value
                DEFAULT_CONF["batch_size"] = int(batch_tf.value or "100000")
                save_settings()
            except Exception:
                pass

        def fmt_money(v):
            try: return f"{float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            except Exception: return str(v)

        def set_table(dt: ft.DataTable, cols, rows, max_rows=2_000_000):
            # garante pelo menos uma coluna
            dt.columns = [ft.DataColumn(ft.Text(c)) for c in (cols or ["(vazio)"])]
            out_rows = []
            for r in rows[:max_rows]:
                cells = [ft.DataCell(ft.Text("" if v is None else str(v))) for v in r]
                out_rows.append(ft.DataRow(cells=cells))
            dt.rows = out_rows

        # Callbacks de refresh
        def refresh_kpis(_=None):
            td, tc, df = get_kpis(DB_PATH, data_ini.value or None, data_fim.value or None, tipo_dd.value)
            kpi_deb.value  = fmt_money(td)
            kpi_cred.value = fmt_money(tc)
            kpi_diff.value = fmt_money(df)
            page.update()

        def run_query_preview(_=None):
            cols, rows = query_preview(DB_PATH, data_ini.value or None, data_fim.value or None, tipo_dd.value)
            set_table(table_preview, cols, rows); page.update()

        def run_query_por_conta(_=None):
            cols, rows = query_por_conta(DB_PATH, data_ini.value or None, data_fim.value or None, tipo_dd.value)
            set_table(table_conta, cols, rows); page.update()

        def run_query_por_entidade(_=None):
            cols, rows = query_por_entidade(DB_PATH, data_ini.value or None, data_fim.value or None, tipo_dd.value)
            set_table(table_entidade, cols, rows); page.update()

        def run_query_divergencias(_=None):
            cols, rows = query_divergencias(DB_PATH, data_ini.value or None, data_fim.value or None, tipo_dd.value, threshold=0.0)
            set_table(table_div, cols, rows); page.update()

        def export_current(dt: ft.DataTable, default_name: str):
            if not dt.columns:
                page.snack_bar = ft.SnackBar(ft.Text("Nada para exportar"), open=True)
                page.update(); return
            cols = [c.label.value for c in dt.columns]
            rows = [[cell.content.value for cell in dr.cells] for dr in dt.rows]
            out_path = BASE_DIR / "export" / f"{default_name}.csv"
            export_to_csv(cols, rows, out_path)
            page.snack_bar = ft.SnackBar(ft.Text(f"Exportado: {out_path}"), open=True); page.update()

        # FilePicker
        file_picker = ft.FilePicker(on_result=lambda e: setattr(txt_path, "value", (e.files[0].path if e.files else "")) or page.update())
        page.overlay.append(file_picker)
        btn_pick.on_click = lambda e: file_picker.pick_files(allow_multiple=False)

        # Ações
        def on_truncate(_):
            try:
                truncate_movimentos(DB_PATH); ensure_schema(DB_PATH)
                page.snack_bar = ft.SnackBar(ft.Text("Base limpa."), open=True)
                refresh_kpis()
                # também limpa tabelas visuais
                for dt in (table_preview, table_conta, table_entidade, table_div):
                    set_table(dt, ["(vazio)"], [])
            except Exception as ex:
                page.snack_bar = ft.SnackBar(ft.Text(f"Erro ao limpar base: {ex}"), open=True)
            page.update()

        def on_import(_):
            save_conf_from_ui()
            if not txt_path.value:
                page.snack_bar = ft.SnackBar(ft.Text("Selecione um arquivo TXT."), open=True); page.update(); return
            ensure_schema(DB_PATH)

            prog.visible = True; prog.value = None; prog_txt.value = "Importando..."; page.update()

            def _progress(inserted, total_lines, elapsed):
                prog.value = None
                prog_txt.value = f"Inseridos: {inserted:,} / Lidos: {total_lines:,} | {elapsed:.1f}s"
                page.update()

            try:
                ins, tot, sec = bulk_load_txt_to_movimentos(
                    txt_path.value, DB_PATH,
                    sep=sep_dd.value, encoding=enc_dd.value,
                    batch=int(batch_tf.value or "100000"),
                    tipo_padrao=tipo_dd.value if tipo_dd.value != "(Todos)" else "VENDA",
                    documento_tipo_padrao=doc_tipo_dd.value,
                    on_progress=_progress
                )
                prog.visible = False
                prog_txt.value = f"Concluído: {ins:,}/{tot:,} linhas em {sec:.1f}s"

                # >>> Executa TODAS as consultas automaticamente, sem exigir filtros <<<
                refresh_kpis()
                run_query_preview()
                run_query_por_conta()
                run_query_por_entidade()
                run_query_divergencias()

                page.snack_bar = ft.SnackBar(ft.Text("Importação finalizada e consultas atualizadas."), open=True)
            except Exception as ex:
                prog.visible = False; prog_txt.value = f"Erro: {ex}"
                page.snack_bar = ft.SnackBar(ft.Text(f"Erro na importação: {ex}"), open=True)
            page.update()

        btn_import.on_click = on_import
        btn_truncate.on_click = on_truncate

        toolbar = ft.Row(
            controls=[
                txt_path, btn_pick,
                sep_dd, enc_dd, batch_tf,
                tipo_dd, doc_tipo_dd,
                btn_import, btn_truncate
            ],
            spacing=10, alignment=ft.MainAxisAlignment.START,
            vertical_alignment=ft.CrossAxisAlignment.CENTER
        )

        filtros = ft.Row(
            controls=[
                data_ini, data_fim,
                ft.ElevatedButton(
                    "Aplicar Filtros", icon=ft.Icons.FILTER_ALT,
                    on_click=lambda e: (
                        refresh_kpis(),
                        run_query_preview(),
                        run_query_por_conta(),
                        run_query_por_entidade(),
                        run_query_divergencias()
                    )
                )
            ],
            spacing=10
        )

        kpis_row = ft.Row([
            ft.Card(content=ft.Container(content=ft.Column([ft.Text("Débitos"), kpi_deb], spacing=4), padding=12), elevation=2),
            ft.Card(content=ft.Container(content=ft.Column([ft.Text("Créditos"), kpi_cred], spacing=4), padding=12), elevation=2),
            ft.Card(content=ft.Container(content=ft.Column([ft.Text("Diferença (D-C)"), kpi_diff], spacing=4), padding=12), elevation=2),
        ], spacing=16)

        # Abas (cada uma com ListView para rolagem)
        tab_preview = ft.Tab(
            text="Preview",
            content=ft.Column([
                ft.Row([ft.FilledButton("Exportar CSV", icon=ft.Icons.SAVE, on_click=lambda e: export_current(table_preview, "preview"))]),
                ft.ListView(expand=True, controls=[table_preview])
            ], expand=True)
        )
        tab_conta = ft.Tab(
            text="Por Conta",
            content=ft.Column([
                ft.Row([ft.FilledButton("Exportar CSV", icon=ft.Icons.SAVE, on_click=lambda e: export_current(table_conta, "por_conta"))]),
                ft.ListView(expand=True, controls=[table_conta])
            ], expand=True)
        )
        tab_entidade = ft.Tab(
            text="Por Entidade",
            content=ft.Column([
                ft.Row([ft.FilledButton("Exportar CSV", icon=ft.Icons.SAVE, on_click=lambda e: export_current(table_entidade, "por_entidade"))]),
                ft.ListView(expand=True, controls=[table_entidade])
            ], expand=True)
        )
        tab_div = ft.Tab(
            text="Divergência D x C",
            content=ft.Column([
                ft.Row([ft.FilledButton("Exportar CSV", icon=ft.Icons.SAVE, on_click=lambda e: export_current(table_div, "divergencias"))]),
                ft.ListView(expand=True, controls=[table_div])
            ], expand=True)
        )

        tabs = ft.Tabs(tabs=[tab_preview, tab_conta, tab_entidade, tab_div], expand=True)

        page.add(
            ft.AppBar(title=ft.Text("AuditHOS – Movimentos (Genérico)"), center_title=False),
            ft.Container(content=toolbar, padding=10),
            ft.Container(content=ft.Row([prog, prog_txt], spacing=10), padding=10),
            ft.Container(content=filtros, padding=10),
            ft.Container(content=kpis_row, padding=10),
            ft.Container(content=tabs, padding=10, expand=True),
        )

        # Primeira renderização (sem filtros)
        refresh_kpis()
        run_query_preview()
        run_query_por_conta()
        run_query_por_entidade()
        run_query_divergencias()

    ft.app(target=main)

if __name__ == "__main__":
    run_app()
