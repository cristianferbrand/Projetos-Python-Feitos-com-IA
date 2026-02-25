# -*- coding: utf-8 -*-
"""
Gerenciador de Tempo - Flet + SQLite
----------------------------------------------------------------------------------------
- Timer grande (sem dashboard/KPIs/datas).
- Controles: Iniciar / Pausar / Retomar / Finalizar / Descartar.
- Persistência em SQLite.
- Filtro por cliente e Exportação CSV.
- Timezone America/Sao_Paulo.
"""

from __future__ import annotations

import asyncio
import csv
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Any
from zoneinfo import ZoneInfo

import flet as ft

# ---------------------------------------------------------------------------
# Constantes e diretórios
# ---------------------------------------------------------------------------
TZ = ZoneInfo("America/Sao_Paulo")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
EXPORT_DIR = os.path.join(BASE_DIR, "export")
LOGS_DIR = os.path.join(BASE_DIR, "logs")
DB_PATH = os.path.join(DATA_DIR, "time_manager.db")

for d in (DATA_DIR, EXPORT_DIR, LOGS_DIR):
    os.makedirs(d, exist_ok=True)

# ---------------------------------------------------------------------------
# Modelo de Dados
# ---------------------------------------------------------------------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client TEXT NOT NULL,
    ticket TEXT,
    category TEXT,
    description TEXT,
    start_time TEXT NOT NULL,       -- ISO format, timezone-aware
    end_time TEXT,                  -- ISO format, timezone-aware (nullable)
    status TEXT NOT NULL,           -- running|paused|finished|discarded
    paused_at TEXT,                 -- ISO, quando entrou em pause
    pause_seconds INTEGER NOT NULL DEFAULT 0   -- total acumulado de pausas
);
CREATE INDEX IF NOT EXISTS idx_sessions_status ON sessions(status);
CREATE INDEX IF NOT EXISTS idx_sessions_start ON sessions(start_time);
"""

@dataclass
class Session:
    id: int
    client: str
    ticket: str
    category: str
    description: str
    start_time: datetime
    end_time: Optional[datetime]
    status: str
    paused_at: Optional[datetime]
    pause_seconds: int

# ---------------------------------------------------------------------------
# Camada de Persistência
# ---------------------------------------------------------------------------
class DB:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._ensure_db()

    def _ensure_db(self):
        with sqlite3.connect(self.db_path) as con:
            con.executescript(SCHEMA)

    def _row_to_session(self, row: sqlite3.Row) -> Session:
        def parse_ts(x: Optional[str]) -> Optional[datetime]:
            if x is None:
                return None
            return datetime.fromisoformat(x)  # preserva tzinfo

        return Session(
            id=row[0],
            client=row[1],
            ticket=row[2] or "",
            category=row[3] or "",
            description=row[4] or "",
            start_time=parse_ts(row[5]),
            end_time=parse_ts(row[6]),
            status=row[7],
            paused_at=parse_ts(row[8]),
            pause_seconds=row[9] or 0
        )

    # CRUD de sessões -------------------------------------------------------
    def create_session(self, client: str, ticket: str, category: str, description: str) -> int:
        now = datetime.now(TZ).isoformat()
        with sqlite3.connect(self.db_path) as con:
            cur = con.cursor()
            cur.execute(
                """
                INSERT INTO sessions (client, ticket, category, description, start_time, status)
                VALUES (?, ?, ?, ?, ?, 'running')
                """,
                (client.strip(), (ticket or "").strip(), (category or "").strip(), (description or "").strip(), now)
            )
            con.commit()
            return cur.lastrowid

    def update_status(self, session_id: int, status: str, when: Optional[datetime] = None):
        with sqlite3.connect(self.db_path) as con:
            if status == "paused":
                con.execute(
                    "UPDATE sessions SET status=?, paused_at=? WHERE id=?",
                    (status, (when or datetime.now(TZ)).isoformat(), session_id)
                )
            elif status in ("running", "finished", "discarded"):
                if status == "running":
                    con.execute(
                        "UPDATE sessions SET status=?, paused_at=NULL WHERE id=?",
                        (status, session_id)
                    )
                else:
                    con.execute(
                        "UPDATE sessions SET status=?, end_time=?, paused_at=NULL WHERE id=?",
                        (status, (when or datetime.now(TZ)).isoformat(), session_id)
                    )
            con.commit()

    def accumulate_pause(self, session_id: int, seconds: int):
        with sqlite3.connect(self.db_path) as con:
            con.execute(
                "UPDATE sessions SET pause_seconds = pause_seconds + ?, paused_at=NULL WHERE id=?",
                (int(max(0, seconds)), session_id)
            )
            con.commit()

    def get_session(self, session_id: int) -> Optional[Session]:
        with sqlite3.connect(self.db_path) as con:
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            cur.execute("SELECT * FROM sessions WHERE id=?", (session_id,))
            row = cur.fetchone()
            return self._row_to_session(row) if row else None

    def list_sessions(self,
                      client_like: Optional[str] = None,
                      limit: int = 300) -> List[Session]:
        q = "SELECT * FROM sessions WHERE 1=1"
        params: List[Any] = []
        if client_like:
            q += " AND client LIKE ?"
            params.append(f"%{client_like}%")
        q += " ORDER BY datetime(start_time) DESC LIMIT ?"
        params.append(limit)

        with sqlite3.connect(self.db_path) as con:
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            cur.execute(q, params)
            rows = cur.fetchall()
            return [self._row_to_session(r) for r in rows]

# ---------------------------------------------------------------------------
# Regras de tempo
# ---------------------------------------------------------------------------
def elapsed_seconds(s: Session, now: Optional[datetime] = None) -> int:
    if now is None:
        now = datetime.now(TZ)
    base_end = s.end_time or now
    total = int((base_end - s.start_time).total_seconds())
    paused = int(s.pause_seconds or 0)
    if s.status == "paused" and s.paused_at:
        paused += int((now - s.paused_at).total_seconds())
    return max(0, total - paused)

def fmt_hms(total_seconds: int) -> str:
    h = total_seconds // 3600
    m = (total_seconds % 3600) // 60
    s = total_seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------
class TimeManagerApp:
    def __init__(self, page: ft.Page):
        self.page = page
        self.db = DB(DB_PATH)
        self.active_session_id: Optional[int] = None

        # Loop de "ticks" usando asyncio + page.run_task (sem ft.Timer)
        self._tick_running: bool = False
        self._tick_task = None

        # --------------------- Inputs Nova Sessão ---------------------------
        self.in_client = ft.TextField(label="Cliente *", autofocus=True, expand=True)
        self.in_ticket = ft.TextField(label="Ticket/Chamado", expand=True)
        self.in_category = ft.Dropdown(
            label="Categoria",
            options=[
                ft.dropdown.Option("Implantação"),
                ft.dropdown.Option("Suporte - Fiscal"),
                ft.dropdown.Option("Suporte - Vendas"),
                ft.dropdown.Option("Treinamento"),
                ft.dropdown.Option("Manutenção/Corretivo"),
                ft.dropdown.Option("Reunião"),
            ],
            expand=True
        )
        self.in_desc = ft.TextField(label="Descrição", multiline=True, min_lines=2, max_lines=3, expand=True)

        self.bt_start = ft.ElevatedButton("Iniciar", icon=ft.Icons.PLAY_ARROW, on_click=self.start_session)
        self.bt_pause = ft.OutlinedButton("Pausar", icon=ft.Icons.PAUSE, disabled=True, on_click=self.pause_session)
        self.bt_resume = ft.OutlinedButton("Retomar", icon=ft.Icons.PLAY_ARROW, disabled=True, on_click=self.resume_session)
        self.bt_finish = ft.FilledTonalButton("Finalizar", icon=ft.Icons.STOP, disabled=True, on_click=self.finish_session)
        self.bt_discard = ft.TextButton("Descartar", icon=ft.Icons.DELETE, disabled=True, on_click=self.discard_session)

        # --------------------- Timer Grande ---------------------------------
        self.big_timer = ft.Text(
            "00:00:00",
            size=120,  # grande
            weight=ft.FontWeight.W_700,
            font_family="RobotoMono",
            color=ft.Colors.GREEN,
            text_align=ft.TextAlign.CENTER,
        )

        # Badge no AppBar
        self.timer_badge = ft.Text("00:00:00", weight=ft.FontWeight.BOLD, size=16, color=ft.Colors.GREEN)

        # --------------------- Busca & Export --------------------------------
        self.search_client = ft.TextField(label="Filtrar por cliente", on_submit=lambda e: self.refresh_tables(), expand=True)
        self.bt_export = ft.OutlinedButton("Exportar CSV", icon=ft.Icons.DOWNLOAD, on_click=self.export_csv)

        # --------------------- Tabela ----------------------------------------
        self.table_sessions = ft.DataTable(
            columns=[
                ft.DataColumn(ft.Text("Início")),
                ft.DataColumn(ft.Text("Cliente")),
                ft.DataColumn(ft.Text("Ticket")),
                ft.DataColumn(ft.Text("Categoria")),
                ft.DataColumn(ft.Text("Descrição")),
                ft.DataColumn(ft.Text("Status")),
                ft.DataColumn(ft.Text("Tempo (H:M:S)")),
                ft.DataColumn(ft.Text("Ações")),
            ],
            rows=[],
        )

        self.build_ui()
        self.refresh_tables()

    # -------------------------- UI BUILDERS --------------------------------
    def build_ui(self):
        self.page.title = "Gerenciador de Tempo - Atendimento"
        self.page.horizontal_alignment = ft.CrossAxisAlignment.STRETCH
        self.page.theme_mode = ft.ThemeMode.DARK
        self.page.appbar = ft.AppBar(
            title=ft.Text("Gerenciador de Tempo"),
            actions=[
                ft.Container(ft.Icon(ft.Icons.ACCESS_TIME), padding=ft.padding.only(right=8)),
                ft.Container(self.timer_badge, padding=ft.padding.only(right=16)),
            ]
        )

        # Card Nova Sessão
        new_session_card = ft.Card(
            content=ft.Container(
                content=ft.Column(
                    [
                        ft.Text("Nova Sessão de Atendimento", size=16, weight=ft.FontWeight.BOLD),
                        ft.Row([self.in_client, self.in_ticket], spacing=10),
                        ft.Row([self.in_category], spacing=10),
                        self.in_desc,
                        ft.Row([self.bt_start, self.bt_pause, self.bt_resume, self.bt_finish, self.bt_discard], spacing=10),
                    ],
                    tight=True,
                    spacing=10,
                ),
                padding=16,
            ),
        )

        # Card do Timer Grande
        big_timer_card = ft.Card(
            content=ft.Container(
                content=ft.Column(
                    [
                        ft.Text("Contador de Tempo", size=16, weight=ft.FontWeight.BOLD),
                        ft.Container(
                            content=self.big_timer,
                            alignment=ft.alignment.center,
                            padding=ft.padding.symmetric(vertical=10),
                        ),
                    ],
                    spacing=8,
                    horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                ),
                padding=16,
            ),
        )

        # Card Tabela de Sessões + Filtros mínimos (cliente + export)
        sessions_card = ft.Card(
            content=ft.Container(
                content=ft.Column(
                    [
                        ft.Row([
                            ft.Text("Sessões", size=14, weight=ft.FontWeight.BOLD),
                            ft.Container(expand=True),
                            self.search_client,
                            self.bt_export,
                        ], spacing=10, alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
                        self.table_sessions,
                    ],
                    spacing=12,
                ),
                padding=16,
            )
        )

        # Adiciona tudo
        self.page.add(
            ft.Container(
                content=ft.Column([new_session_card, big_timer_card, sessions_card], spacing=16),
                padding=16
            )
        )

        # Tema compacto
        self.page.theme = ft.Theme(visual_density=ft.VisualDensity.COMPACT)

    # -------------------------- TICK ENGINE (asyncio) ----------------------
    def _start_ticks(self):
        if self._tick_running:
            return
        self._tick_running = True
        # dispara loop assíncrono sem bloquear
        self._tick_task = self.page.run_task(self._tick_loop)

    def _stop_ticks(self):
        self._tick_running = False
        self._tick_task = None

    async def _tick_loop(self):
        while self._tick_running and self.active_session_id:
            self._update_timer_display()
            await asyncio.sleep(1)

    # -------------------------- EVENTOS / AÇÕES ----------------------------
    def start_session(self, e):
        client = (self.in_client.value or "").strip()
        if not client:
            self._toast("Informe o cliente (obrigatório).")
            return
        ticket = self.in_ticket.value or ""
        category = self.in_category.value or ""
        description = self.in_desc.value or ""

        sid = self.db.create_session(client, ticket, category, description)
        self.active_session_id = sid
        self._toast(f"Sessão iniciada (ID {sid}).")

        # Estado botões
        self.bt_start.disabled = True
        self.bt_pause.disabled = False
        self.bt_resume.disabled = True
        self.bt_finish.disabled = False
        self.bt_discard.disabled = False

        self.in_desc.value = ""
        self.page.update()

        # Inicia ticks
        self._start_ticks()
        self.refresh_tables()

    def pause_session(self, e):
        if not self.active_session_id:
            return
        s = self.db.get_session(self.active_session_id)
        if not s or s.status != "running":
            return
        self.db.update_status(s.id, "paused")
        self.bt_pause.disabled = True
        self.bt_resume.disabled = False
        self.page.update()
        self.refresh_tables()

    def resume_session(self, e):
        if not self.active_session_id:
            return
        s = self.db.get_session(self.active_session_id)
        if not s or s.status != "paused":
            return
        if s.paused_at:
            paused_secs = int((datetime.now(TZ) - s.paused_at).total_seconds())
            self.db.accumulate_pause(s.id, paused_secs)
        self.db.update_status(s.id, "running")
        self.bt_pause.disabled = False
        self.bt_resume.disabled = True
        self.page.update()
        self.refresh_tables()
        self._start_ticks()

    def finish_session(self, e):
        if not self.active_session_id:
            return
        s = self.db.get_session(self.active_session_id)
        if not s:
            return
        if s.status == "paused" and s.paused_at:
            paused_secs = int((datetime.now(TZ) - s.paused_at).total_seconds())
            self.db.accumulate_pause(s.id, paused_secs)
        self.db.update_status(s.id, "finished")
        self._toast("Sessão finalizada.")
        self._clear_active_session()
        self.refresh_tables()

    def discard_session(self, e):
        if not self.active_session_id:
            return
        self.db.update_status(self.active_session_id, "discarded")
        self._toast("Sessão descartada.")
        self._clear_active_session()
        self.refresh_tables()

    def _clear_active_session(self):
        self.active_session_id = None
        self.bt_start.disabled = False
        self.bt_pause.disabled = True
        self.bt_resume.disabled = True
        self.bt_finish.disabled = True
        self.bt_discard.disabled = True
        self.big_timer.value = "00:00:00"
        self.big_timer.color = ft.Colors.GREEN
        self.timer_badge.value = "00:00:00"
        self.page.update()
        self._stop_ticks()

    # -------------------------- DISPLAY UPDATE -----------------------------
    def _update_timer_display(self):
        if not self.active_session_id:
            return
        s = self.db.get_session(self.active_session_id)
        if not s:
            return
        secs = elapsed_seconds(s, now=datetime.now(TZ))
        t = fmt_hms(secs)
        self.big_timer.value = t
        self.timer_badge.value = t
        self.big_timer.color = ft.Colors.GREEN if s.status == "running" else ft.Colors.AMBER
        self.timer_badge.color = self.big_timer.color
        self.page.update()

    # -------------------------- TABELA / EXPORT ----------------------------
    def refresh_tables(self):
        client_like = self.search_client.value.strip() if self.search_client.value else None
        rows = self.db.list_sessions(client_like=client_like, limit=300)
        self.table_sessions.rows = []
        now = datetime.now(TZ)

        for s in rows:
            secs = elapsed_seconds(s, now=now)
            actions = self._actions_for_row(s)
            self.table_sessions.rows.append(
                ft.DataRow(cells=[
                    ft.DataCell(ft.Text(s.start_time.astimezone(TZ).strftime("%d/%m/%Y %H:%M:%S"))),
                    ft.DataCell(ft.Text(s.client)),
                    ft.DataCell(ft.Text(s.ticket or "-")),
                    ft.DataCell(ft.Text(s.category or "-")),
                    ft.DataCell(ft.Text(s.description or "-")),
                    ft.DataCell(ft.Text(s.status)),
                    ft.DataCell(ft.Text(fmt_hms(secs))),
                    ft.DataCell(actions),
                ])
            )
        self.page.update()

    def _actions_for_row(self, s: Session) -> ft.Row:
        items: List[ft.Control] = []

        def _resume(_):
            if s.status == "paused":
                if s.paused_at:
                    paused_secs = int((datetime.now(TZ) - s.paused_at).total_seconds())
                    self.db.accumulate_pause(s.id, paused_secs)
                self.db.update_status(s.id, "running")
                self.active_session_id = s.id
                self.bt_start.disabled = True
                self.bt_pause.disabled = False
                self.bt_resume.disabled = True
                self.bt_finish.disabled = False
                self.bt_discard.disabled = False
                self._start_ticks()
                self.refresh_tables()

        def _pause(_):
            if s.status == "running":
                self.db.update_status(s.id, "paused")
                if self.active_session_id == s.id:
                    self.bt_pause.disabled = True
                    self.bt_resume.disabled = False
                self.refresh_tables()

        def _finish(_):
            if s.status in ("running", "paused"):
                if s.status == "paused" and s.paused_at:
                    paused_secs = int((datetime.now(TZ) - s.paused_at).total_seconds())
                    self.db.accumulate_pause(s.id, paused_secs)
                self.db.update_status(s.id, "finished")
                if self.active_session_id == s.id:
                    self._clear_active_session()
                self.refresh_tables()

        def _discard(_):
            self.db.update_status(s.id, "discarded")
            if self.active_session_id == s.id:
                self._clear_active_session()
            self.refresh_tables()

        if s.status == "running":
            items.append(ft.IconButton(ft.Icons.PAUSE, icon_color=ft.Colors.AMBER, tooltip="Pausar", on_click=_pause))
            items.append(ft.IconButton(ft.Icons.STOP, icon_color=ft.Colors.RED, tooltip="Finalizar", on_click=_finish))
        elif s.status == "paused":
            items.append(ft.IconButton(ft.Icons.PLAY_ARROW, icon_color=ft.Colors.GREEN, tooltip="Retomar", on_click=_resume))
            items.append(ft.IconButton(ft.Icons.STOP, icon_color=ft.Colors.RED, tooltip="Finalizar", on_click=_finish))
        elif s.status == "finished":
            items.append(ft.IconButton(ft.Icons.DELETE, icon_color=ft.Colors.GREY_600, tooltip="Descartar", on_click=_discard))
        return ft.Row(items, spacing=4)

    def export_csv(self, e):
        client_like = self.search_client.value.strip() if self.search_client.value else None
        rows = self.db.list_sessions(client_like=client_like, limit=99999)

        fn = f"sessoes_{datetime.now(TZ).strftime('%Y%m%d_%H%M%S')}.csv"
        path = os.path.join(EXPORT_DIR, fn)
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow(["ID", "Cliente", "Ticket", "Categoria", "Descrição", "Início", "Fim", "Status", "Tempo(HH:MM:SS)", "Pausas(seg)"])
            now = datetime.now(TZ)
            for s in rows:
                w.writerow([
                    s.id,
                    s.client,
                    s.ticket,
                    s.category,
                    s.description,
                    s.start_time.astimezone(TZ).strftime("%d/%m/%Y %H:%M:%S"),
                    s.end_time.astimezone(TZ).strftime("%d/%m/%Y %H:%M:%S") if s.end_time else "",
                    s.status,
                    fmt_hms(elapsed_seconds(s, now=now)),
                    s.pause_seconds,
                ])
        self._toast(f"Exportado: {path}")

    # -------------------------- UTIL ---------------------------------------
    def _toast(self, msg: str):
        self.page.snack_bar = ft.SnackBar(ft.Text(msg))
        self.page.snack_bar.open = True
        self.page.update()

# ---------------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------------
def main(page: ft.Page):
    page.padding = 0
    page.scroll = ft.ScrollMode.AUTO
    page.theme = ft.Theme(visual_density=ft.VisualDensity.COMPACT)
    TimeManagerApp(page)

if __name__ == "__main__":
    ft.app(target=main)
