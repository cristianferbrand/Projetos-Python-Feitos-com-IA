
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Email Sender (DB) — HOS / Monitor Cloud Backup
- Lê os alertas diretamente do SQLite (tabela alerts) usado pelo Monitor.
- UI leve em Flet, com seleção por checkbox e envio em massa.
- Usa ft.Colors e ft.Icons (atenção ao case).

Dependências:
  pip install flet

Obs.: Este app NÃO altera nada no banco — apenas lê da tabela "alerts".
"""

import os
import sys
import json
import sqlite3
import time
from pathlib import Path
from datetime import datetime

import flet as ft
import smtplib
from email.message import EmailMessage

# ====== Paths e config compartilhados com o Monitor ======
def app_base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    try:
        return Path(__file__).resolve().parent
    except NameError:
        return Path.cwd()

BASE_DIR = app_base_dir()

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

CONFIG_DIR = (BASE_DIR / "config")
CONFIG_PATH = CONFIG_DIR / "settings_email.json"
ensure_dir(CONFIG_DIR)

# Mesmo default do Monitor
DEFAULT_DB_PATH = (BASE_DIR / "cache" / "acronis_cache.db").resolve()
ensure_dir(DEFAULT_DB_PATH.parent)

# ====== Settings ======
DEFAULT_SETTINGS = {
    # SMTP / E-mail
    "DATABASE_URL": "",  # se vazio, usa SQLite local    
    "SMTP_HOST": "",
    "SMTP_PORT": 587,
    "SMTP_USE_TLS": True,
    "SMTP_USE_SSL": False,
    "SMTP_USERNAME": "",
    "SMTP_PASSWORD": "",
    "SMTP_FROM": "",
    "DEFAULT_MAIL_TO": "",
    "DEFAULT_MAIL_CC": "",
    "DEFAULT_MAIL_BCC": "",
    # Templating
    "SUBJECT_TEMPLATE": "{Cliente} - {Tipo do alerta}",
    "BODY_TEMPLATE": (
        "Alerta: {Tipo do alerta}\n"
        "Severidade: {Severidade}\n"
        "Cliente: {Cliente}\n"
        "Carga de trabalho: {Carga de trabalho}\n"
        "Plano: {Plano}\n"
        "Data/hora: {Data e hora}\n\n"
        "Mensagem:\n{Mensagem}"
    ),
    "RATE_LIMIT_MS": 200,
}

def load_settings() -> dict:
    try:
        if CONFIG_PATH.exists():
            data = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
            out = dict(DEFAULT_SETTINGS)
            # mantém apenas chaves conhecidas; não quebra se arquivo tiver extras
            for k in out.keys():
                if k in data:
                    out[k] = data[k]
            # preserva DATABASE_URL se existir no arquivo (mesmo vazio)
            if "DATABASE_URL" in data:
                out["DATABASE_URL"] = data["DATABASE_URL"]
            return out
    except Exception:
        pass
    return dict(DEFAULT_SETTINGS)

def save_settings(data: dict):
    # mescla não-destrutiva com arquivo existente
    try:
        current = {}
        if CONFIG_PATH.exists():
            current = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        merged = dict(current)
        for k, v in data.items():
            merged[k] = v
        ensure_dir(CONFIG_DIR)
        CONFIG_PATH.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as ex:
        print("Erro ao salvar settings:", ex)

# ====== Helpers ======
def parse_email_list(s: str) -> list[str]:
    s = (s or "").strip()
    if not s:
        return []
    parts = [x.strip() for x in s.replace(";", ",").split(",")]
    return [p for p in parts if p]

def db_path_from_settings(settings: dict) -> Path:
    url = (settings.get("DATABASE_URL") or "").strip()
    if not url:
        return DEFAULT_DB_PATH
    # Aceita apenas SQLite
    if url.startswith("sqlite:///"):
        return Path(url.replace("sqlite:///", "", 1)).resolve()
    # Caso remoto/driver diferente, por ora não suportamos neste app
    # (poderia ser estendido para SQLAlchemy)
    return DEFAULT_DB_PATH

def fmt_brt(iso_str: str) -> str:
    if not iso_str:
        return ""
    s = str(iso_str).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    from datetime import datetime, timezone
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
    try:
        # BRT
        from zoneinfo import ZoneInfo
        dt_brt = dt.astimezone(ZoneInfo("America/Sao_Paulo"))
        return dt_brt.strftime("%d/%m/%Y %H:%M:%S")
    except Exception:
        return dt.astimezone().strftime("%d/%m/%Y %H:%M:%S")

# ====== Email ======
def send_email_smtp(settings: dict, to_list: list[str], subject: str, body: str, cc_list=None, bcc_list=None):
    cc_list = cc_list or []
    bcc_list = bcc_list or []
    host = settings.get("SMTP_HOST", "").strip()
    port = int(settings.get("SMTP_PORT", 587) or 587)
    use_tls = bool(settings.get("SMTP_USE_TLS", True))
    use_ssl = bool(settings.get("SMTP_USE_SSL", False))
    username = (settings.get("SMTP_USERNAME") or "").strip()
    password = settings.get("SMTP_PASSWORD", "")
    from_addr = (settings.get("SMTP_FROM") or username or "").strip()

    if not host or not from_addr or not to_list:
        raise ValueError("Configuração SMTP incompleta: verifique HOST, FROM e destinatário(s).")

    msg = EmailMessage()
    msg["From"] = from_addr
    msg["To"] = ", ".join(to_list)
    if cc_list:
        msg["Cc"] = ", ".join(cc_list)
    msg["Subject"] = subject
    msg.set_content(body)

    all_rcpts = list(to_list) + list(cc_list) + list(bcc_list)

    if use_ssl:
        with smtplib.SMTP_SSL(host, port) as s:
            if username:
                s.login(username, password)
            s.send_message(msg, from_addr=from_addr, to_addrs=all_rcpts)
    else:
        with smtplib.SMTP(host, port) as s:
            s.ehlo()
            if use_tls:
                s.starttls()
                s.ehlo()
            if username:
                s.login(username, password)
            s.send_message(msg, from_addr=from_addr, to_addrs=all_rcpts)

def build_subject(settings: dict, row: dict) -> str:
    tpl = settings.get("SUBJECT_TEMPLATE", DEFAULT_SETTINGS["SUBJECT_TEMPLATE"])
    try:
        return tpl.format(**row)
    except Exception:
        return f"{row.get('Cliente','')} - {row.get('Tipo do alerta','')}"

def build_body(settings: dict, row: dict) -> str:
    tpl = settings.get("BODY_TEMPLATE", DEFAULT_SETTINGS["BODY_TEMPLATE"])
    try:
        return tpl.format(**row)
    except Exception:
        return (
            f"Alerta: {row.get('Tipo do alerta','')}\n"
            f"Severidade: {row.get('Severidade','')}\n"
            f"Cliente: {row.get('Cliente','')}\n"
            f"Carga de trabalho: {row.get('Carga de trabalho','')}\n"
            f"Plano: {row.get('Plano','')}\n"
            f"Data/hora: {row.get('Data e hora','')}\n\n"
            f"Mensagem:\n{row.get('Mensagem','')}"
        )

# ====== Database (somente leitura / SQLite) ======
ALERTS_SELECT = """
SELECT
    id,
    tenant_id,
    tenant_name,
    severity,
    alert_type,
    message,
    workload_name,
    plan_name,
    received_at
FROM alerts
"""

def load_rows_from_db(db_file: Path, where_clause: str = "", params: tuple = (), order_by: str = "received_at DESC", limit: int | None = None, offset: int = 0) -> list[dict]:
    if not db_file.exists():
        return []
    sql = ALERTS_SELECT
    if where_clause:
        sql += " WHERE " + where_clause
    if order_by:
        sql += " ORDER BY " + order_by
    if limit is not None:
        sql += f" LIMIT {int(limit)} OFFSET {int(offset)}"
    rows: list[dict] = []
    with sqlite3.connect(str(db_file)) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(sql, params)
        for r in cur.fetchall():
            rows.append({
                "Severidade": r["severity"] or "",
                "Tipo do alerta": r["alert_type"] or "",
                "Mensagem": r["message"] or "",
                "Carga de trabalho": r["workload_name"] or "",
                "Cliente": r["tenant_name"] or "",
                "Data e hora": fmt_brt(r["received_at"]),
                "Plano": r["plan_name"] or "",
                "_id": r["id"],
                "_tenant_id": r["tenant_id"],
            })
    return rows

# ====== UI Helpers (chips de severidade iguais ao Monitor) ======
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

# ====== UI (Flet) ======
APP_TITLE = "Seja Muito Bem-vindo ao E-mail Sender (DB) do Monitor Cloud Backup HOS!"

def main(page: ft.Page):
    page.title = "E-mail Sender — Cloud Backup HOS (DB) v1.0"
    page.window_width = 1180
    page.window_height = 760
    page.horizontal_alignment = "stretch"
    page.vertical_alignment = "start"
    page.theme_mode = ft.ThemeMode.LIGHT  # inicia leve
    theme_switch = ft.Switch(label="Tema escuro", value=False)

    settings = load_settings()
    DB_FILE = db_path_from_settings(settings)

    status_txt = ft.Text("Pronto", size=12, color=ft.Colors.GREY_600)
    total_label = ft.Text("0 linha(s)")

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
        options=[ft.dropdown.Option(str(n)) for n in (25, 50, 200, 400, 600, 1000)],
        value="100"
    )

    progress = ft.ProgressBar(width=260, visible=False)

    # Tabela
    columns = [
        ft.DataColumn(ft.Text("Sel.")),
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

    # Estado
    state = {
        "page_index": 0,
        "page_size": 100,
        "where": "",
        "params": (),
        "order": "received_at DESC",
        "cached_total": 0,
        "rows": [],
        "selected": set(),  # índices da página
        "dry_run": False,
    }

    # ===== Settings (view estilo Monitor) =====
    def open_settings_view(_e=None):
        tf_host = ft.TextField(label="SMTP_HOST", value=str(settings.get("SMTP_HOST","")), width=360)
        tf_port = ft.TextField(label="SMTP_PORT", value=str(settings.get("SMTP_PORT",587)), width=140, keyboard_type=ft.KeyboardType.NUMBER)
        sw_tls  = ft.Switch(label="SMTP_USE_TLS", value=bool(settings.get("SMTP_USE_TLS", True)))
        sw_ssl  = ft.Switch(label="SMTP_USE_SSL", value=bool(settings.get("SMTP_USE_SSL", False)))
        tf_user = ft.TextField(label="SMTP_USERNAME", value=str(settings.get("SMTP_USERNAME","")), width=360)
        tf_pwd  = ft.TextField(label="SMTP_PASSWORD", value=str(settings.get("SMTP_PASSWORD","")), password=True, can_reveal_password=True, width=360)
        tf_from = ft.TextField(label="SMTP_FROM", value=str(settings.get("SMTP_FROM","")), width=360)
        tf_to   = ft.TextField(label="DEFAULT_MAIL_TO", value=str(settings.get("DEFAULT_MAIL_TO","")), width=360, tooltip="Separar por vírgula ou ;")
        tf_cc   = ft.TextField(label="DEFAULT_MAIL_CC", value=str(settings.get("DEFAULT_MAIL_CC","")), width=360)
        tf_bcc  = ft.TextField(label="DEFAULT_MAIL_BCC", value=str(settings.get("DEFAULT_MAIL_BCC","")), width=360)
        tf_subj = ft.TextField(label="SUBJECT_TEMPLATE", value=str(settings.get("SUBJECT_TEMPLATE", DEFAULT_SETTINGS["SUBJECT_TEMPLATE"])), width=520)
        tf_body = ft.TextField(label="BODY_TEMPLATE", value=str(settings.get("BODY_TEMPLATE", DEFAULT_SETTINGS["BODY_TEMPLATE"])), multiline=True, min_lines=6, max_lines=10, width=520)
        tf_rate = ft.TextField(label="RATE_LIMIT_MS", value=str(settings.get("RATE_LIMIT_MS", 200)), width=180, keyboard_type=ft.KeyboardType.NUMBER)

        # Exibir/editar DATABASE_URL também, para refletir a origem dos dados
        tf_db   = ft.TextField(label="DATABASE_URL (somente sqlite:///)", value=str(settings.get("DATABASE_URL","")), hint_text="vazio = usar ./cache/acronis_cache.db", width=520)

        def on_cancel(_ev=None):
            page.views.pop()
            page.update()

        def on_save(_ev=None):
            try:
                new = dict(settings)
                new.update({
                    "SMTP_HOST": tf_host.value.strip(),
                    "SMTP_PORT": int(tf_port.value.strip() or "587"),
                    "SMTP_USE_TLS": bool(sw_tls.value),
                    "SMTP_USE_SSL": bool(sw_ssl.value),
                    "SMTP_USERNAME": tf_user.value.strip(),
                    "SMTP_PASSWORD": tf_pwd.value,
                    "SMTP_FROM": tf_from.value.strip(),
                    "DEFAULT_MAIL_TO": tf_to.value.strip(),
                    "DEFAULT_MAIL_CC": tf_cc.value.strip(),
                    "DEFAULT_MAIL_BCC": tf_bcc.value.strip(),
                    "SUBJECT_TEMPLATE": tf_subj.value,
                    "BODY_TEMPLATE": tf_body.value,
                    "RATE_LIMIT_MS": int(tf_rate.value.strip() or "200"),
                    "DATABASE_URL": tf_db.value.strip(),
                })
                save_settings(new)
                settings.clear(); settings.update(new)
                nonlocal DB_FILE
                DB_FILE = db_path_from_settings(settings)
                page.snack_bar = ft.SnackBar(ft.Text("Configurações salvas."), open=True)
                on_cancel()
            except Exception as ex:
                page.snack_bar = ft.SnackBar(ft.Text(f"Erro ao salvar: {ex}"), open=True)

        def do_test(_e=None):
            try:
                to_list = parse_email_list(tf_to.value)
                if not to_list:
                    page.snack_bar = ft.SnackBar(ft.Text("Informe ao menos um destinatário (To)."), open=True); return
                demo = {"Cliente":"Teste","Tipo do alerta":"SMTP","Severidade":"info","Carga de trabalho":"-","Plano":"-","Data e hora": datetime.now().strftime("%d/%m/%Y %H:%M:%S"), "Mensagem":"E-mail de teste."}
                tmp = dict(settings)
                tmp.update({
                    "SMTP_HOST": tf_host.value.strip(),
                    "SMTP_PORT": int(tf_port.value.strip() or "587"),
                    "SMTP_USE_TLS": bool(sw_tls.value),
                    "SMTP_USE_SSL": bool(sw_ssl.value),
                    "SMTP_USERNAME": tf_user.value.strip(),
                    "SMTP_PASSWORD": tf_pwd.value,
                    "SMTP_FROM": tf_from.value.strip(),
                })
                send_email_smtp(tmp, to_list, tf_subj.value.format(**demo), tf_body.value.format(**demo), parse_email_list(tf_cc.value), parse_email_list(tf_bcc.value))
                page.snack_bar = ft.SnackBar(ft.Text("E-mail de teste enviado!"), open=True)
            except Exception as ex:
                page.snack_bar = ft.SnackBar(ft.Text(f"Falha ao enviar teste: {ex}"), open=True)

        view = ft.View(
            route="/settings",
            appbar=ft.AppBar(
                title=ft.Text("Configurações (SMTP / Banco)"),
                center_title=False,
                leading=ft.IconButton(ft.Icons.ARROW_BACK, on_click=lambda e: on_cancel())
            ),
            controls=[
                ft.Container(
                    content=ft.Column(
                        controls=[
                            ft.Text("Origem dos dados", weight=ft.FontWeight.BOLD),
                            tf_db,
                            ft.Divider(),
                            ft.Text("SMTP / E-mail", weight=ft.FontWeight.BOLD),
                            ft.Row([tf_host, tf_port], wrap=True, spacing=10),
                            ft.Row([sw_tls, sw_ssl], wrap=True, spacing=10),
                            ft.Row([tf_user, tf_pwd], wrap=True, spacing=10),
                            ft.Row([tf_from], wrap=True, spacing=10),
                            ft.Row([tf_to, tf_cc, tf_bcc], wrap=True, spacing=10),
                            ft.Row([tf_subj, tf_rate], wrap=True, spacing=10),
                            tf_body,
                            ft.Row([
                                ft.OutlinedButton("Testar envio", icon=ft.Icons.SEND, on_click=do_test),
                                ft.FilledButton("Salvar", icon=ft.Icons.SAVE, on_click=on_save),
                            ], alignment=ft.MainAxisAlignment.END, spacing=10),
                        ],
                        tight=True, scroll=ft.ScrollMode.AUTO, spacing=8,
                    ),
                    padding=20, expand=True
                )
            ]
        )
        page.views.append(view)
        page.update()

    # ===== Filtros e paginação =====
    def apply_filters_build_where() -> tuple[str, tuple]:
        wheres = []
        params = []
        tipo_sel = (tipo_alerta_dd.value or "Todos").strip()
        if tipo_sel != "Todos":
            wheres.append("alert_type = ?")
            params.append(tipo_sel)

        q = (search_tf.value or "").strip().lower()
        if q:
            wheres.append("(lower(message) LIKE ? OR lower(tenant_name) LIKE ? OR lower(workload_name) LIKE ? OR lower(plan_name) LIKE ?)")
            like = f"%{q}%"
            params.extend([like, like, like, like])

        where = " AND ".join(wheres)
        return where, tuple(params)

    def order_to_sql(order_label: str) -> str:
        mapping = {
            "Data desc": "received_at DESC",
            "Data asc": "received_at ASC",
            "Severidade": "severity ASC, received_at DESC",
            "Cliente": "tenant_name ASC, received_at DESC",
            "Tipo do alerta": "alert_type ASC, received_at DESC",
        }
        return mapping.get(order_label, "received_at DESC")

    def rebuild_tipos_options():
        try:
            if not DB_FILE.exists():
                tipo_alerta_dd.options = [ft.dropdown.Option("Todos")]
                tipo_alerta_dd.value = "Todos"
                page.update()
                return
            with sqlite3.connect(str(DB_FILE)) as conn:
                cur = conn.execute("SELECT DISTINCT alert_type FROM alerts ORDER BY alert_type")
                tipos = ["Todos"]
                tipos.extend([row[0] for row in cur.fetchall() if row[0]])
                tipo_alerta_dd.options = [ft.dropdown.Option(t) for t in tipos]
                if tipo_alerta_dd.value not in tipos:
                    tipo_alerta_dd.value = "Todos"
                page.update()
        except Exception:
            pass

    def set_progress(on: bool):
        progress.visible = on
        page.update()

    def fill_table_from_db():
        set_progress(True)
        try:
            where, params = apply_filters_build_where()
            state["where"], state["params"] = where, params
            state["page_size"] = int(page_size_dd.value or "100")
            state["order"] = order_to_sql(order_dd.value or "Data desc")

            # total
            total = 0
            if DB_FILE.exists():
                with sqlite3.connect(str(DB_FILE)) as conn:
                    sql = "SELECT COUNT(*) FROM alerts"
                    if where:
                        sql += " WHERE " + where
                    total = int(conn.execute(sql, params).fetchone()[0])

            state["cached_total"] = total
            max_page = max(0, (total - 1) // state["page_size"])
            if state["page_index"] > max_page:
                state["page_index"] = max_page

            offset = state["page_index"] * state["page_size"]
            rows = load_rows_from_db(DB_FILE, where, params, state["order"], limit=state["page_size"], offset=offset)
            state["rows"] = rows

            table.rows = []
            for i, r in enumerate(rows):
                chk = ft.Checkbox(value=(i in state["selected"]), on_change=lambda e, ii=i: toggle_select(ii))
                table.rows.append(ft.DataRow(cells=[
                    ft.DataCell(chk),
                    ft.DataCell(severity_chip(r.get("Severidade",""), page.theme_mode == ft.ThemeMode.DARK)),
                    ft.DataCell(ft.Text(r.get("Tipo do alerta",""))),
                    ft.DataCell(ft.Text(r.get("Mensagem",""), selectable=True)),
                    ft.DataCell(ft.Text(r.get("Carga de trabalho",""))),
                    ft.DataCell(ft.Text(r.get("Cliente",""))),
                    ft.DataCell(ft.Text(r.get("Data e hora",""))),
                    ft.DataCell(ft.Text(r.get("Plano",""))),
                ]))
            total_label.value = f"{total} registro(s)"
            page_lbl.value = ("Sem resultados" if total == 0 else f"Página {state['page_index']+1} de {max_page+1} — {total} registros")
            page.update()
        finally:
            set_progress(False)

    def on_any_filter_change(e):
        state["page_index"] = 0
        fill_table_from_db()
    search_tf.on_submit = on_any_filter_change
    tipo_alerta_dd.on_change = on_any_filter_change
    order_dd.on_change = on_any_filter_change
    page_size_dd.on_change = on_any_filter_change

    def on_prev(_e=None):
        if state["page_index"] > 0:
            state["page_index"] -= 1
            fill_table_from_db()

    def on_next(_e=None):
        max_page = max(0, (state["cached_total"] - 1) // state["page_size"])
        if state["page_index"] < max_page:
            state["page_index"] += 1
            fill_table_from_db()

    prev_btn.on_click = on_prev
    next_btn.on_click = on_next

    # ===== Seleção e envio =====
    def toggle_select(i: int):
        if i in state["selected"]:
            state["selected"].remove(i)
        else:
            state["selected"].add(i)

    def select_all(_e=None):
        state["selected"] = set(range(len(state["rows"])))
        fill_table_from_db()

    def unselect_all(_e=None):
        state["selected"].clear()
        fill_table_from_db()

    dry_run_cb = ft.Checkbox(label="Dry-run (não enviar, só simular)", value=state["dry_run"], on_change=lambda e: state.update({"dry_run": bool(e.control.value)}))

    def send_selected(_e=None):
        if not state["rows"] or not state["selected"]:
            page.snack_bar = ft.SnackBar(ft.Text("Selecione ao menos uma linha."), open=True); return
        to_list = parse_email_list(settings.get("DEFAULT_MAIL_TO",""))
        if not to_list:
            page.snack_bar = ft.SnackBar(ft.Text("Defina o destinatário padrão em Configurações."), open=True); return
        cc_list = parse_email_list(settings.get("DEFAULT_MAIL_CC",""))
        bcc_list = parse_email_list(settings.get("DEFAULT_MAIL_BCC",""))
        rate_ms = int(settings.get("RATE_LIMIT_MS", 200) or 0)

        count = 0
        for idx in sorted(list(state["selected"])):
            if idx < 0 or idx >= len(state["rows"]):
                continue
            row = state["rows"][idx]
            subj = build_subject(settings, row)
            body = build_body(settings, row)
            if state["dry_run"]:
                status_txt.value = f"[SIMULA] {subj} -> {', '.join(to_list)}"
                page.update()
            else:
                try:
                    send_email_smtp(settings, to_list, subj, body, cc_list, bcc_list)
                    status_txt.value = f"[OK] {subj}"
                    page.update()
                    time.sleep(rate_ms/1000.0 if rate_ms>0 else 0)
                except Exception as ex:
                    status_txt.value = f"[ERRO] {subj} -> {ex}"
                    page.update()
            count += 1
        page.snack_bar = ft.SnackBar(ft.Text(f"Processadas {count} linha(s)."), open=True)

    # ===== Layout =====
    header = ft.Row(
        [
            ft.Text(APP_TITLE, size=20, weight=ft.FontWeight.BOLD),
            ft.Container(expand=1),
            ft.IconButton(icon=ft.Icons.SETTINGS, tooltip="Configurações", on_click=open_settings_view),
            theme_switch,
        ],
        alignment=ft.MainAxisAlignment.START,
        vertical_alignment=ft.CrossAxisAlignment.CENTER,
    )

    def on_theme_change(e):
        page.theme_mode = ft.ThemeMode.DARK if theme_switch.value else ft.ThemeMode.LIGHT
        fill_table_from_db()
        page.update()
    theme_switch.on_change = on_theme_change

    toolbar = ft.Row([
        search_tf,
        tipo_alerta_dd,
        order_dd,
        page_size_dd,
        ft.ElevatedButton("Atualizar", icon=ft.Icons.REFRESH, on_click=lambda e: [rebuild_tipos_options(), fill_table_from_db()]),
        ft.ElevatedButton("Selecionar tudo", icon=ft.Icons.CHECK_BOX, on_click=select_all),
        ft.ElevatedButton("Limpar seleção", icon=ft.Icons.CHECK_BOX_OUTLINE_BLANK, on_click=unselect_all),
        dry_run_cb,
        ft.ElevatedButton("Enviar selecionados", icon=ft.Icons.SEND, on_click=send_selected),
        ft.Container(content=total_label, padding=ft.padding.only(left=16, top=8)),
    ], spacing=8, wrap=True)

    pager = ft.Row([prev_btn, page_lbl, next_btn], alignment=ft.MainAxisAlignment.CENTER)

    page.add(
        ft.Column([
            header,
            ft.Row([status_txt], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
            toolbar,
            ft.Container(table, expand=True, padding=10, border_radius=8),
            pager,
        ], expand=True, scroll=ft.ScrollMode.AUTO)
    )

    # Bootstrap
    rebuild_tipos_options()
    fill_table_from_db()
    status_txt.value = f"Banco: {DB_FILE}"
    page.update()

if __name__ == "__main__":
    ft.app(target=main)