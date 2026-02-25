# -*- coding: utf-8 -*-
"""
Gerenciador de Tempo - Flet + SQLite (com Jornada, Ociosidade e Contador de Horas Trabalhadas)
-----------------------------------------------------------------------------------------------
- Jornada por atendente (login/logout) e ociosidade por inatividade (mouse/teclado).
- Contador no AppBar = horas trabalhadas (total - ocioso) desde o login (ou abertura).
- Timer grande e sessões preservados (Iniciar / Pausar / Retomar / Finalizar / Descartar).
- Persistência em SQLite e Exportação CSV.
- Timezone America/Sao_Paulo.

Recursos extras:
- Hook global opcional (pynput) para maior precisão de ociosidade:
  pip install pynput
"""

from __future__ import annotations

import asyncio
import csv
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, List, Any, Callable
from zoneinfo import ZoneInfo

import flet as ft

# ----------------------------------------------------------------------------
# Compat de Cores/Ícones (preferência do usuário) + helper de opacidade
# ----------------------------------------------------------------------------
try:
    C = ft.Colors  # Flet recente
except AttributeError:  # Flet antigo
    C = ft.colors

try:
    I = ft.Icons
except AttributeError:
    I = ft.icons

def op(alpha: float, color: str) -> str:
    """Compat: aplica opacidade em cor, suportando Flet antigo/novo."""
    try:
        # Flet novo (Colors.with_opacity(color, alpha))
        return ft.Colors.with_opacity(color, alpha)  # type: ignore[attr-defined]
    except Exception:
        try:
            # Flet antigo (colors.with_opacity(color, alpha))
            return ft.colors.with_opacity(color, alpha)  # type: ignore[attr-defined]
        except Exception:
            return color

# ----------------------------------------------------------------------------
# Constantes e diretórios
# ----------------------------------------------------------------------------
TZ = ZoneInfo("America/Sao_Paulo")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
EXPORT_DIR = os.path.join(BASE_DIR, "export")
LOGS_DIR = os.path.join(BASE_DIR, "logs")
DB_PATH = os.path.join(DATA_DIR, "time_manager.db")

for d in (DATA_DIR, EXPORT_DIR, LOGS_DIR):
    os.makedirs(d, exist_ok=True)

# Ociosidade
INACTIVITY_THRESHOLD_SECS = 300  # 5 minutos (ajuste conforme sua política)
AUTO_PAUSE_SESSION_WHEN_IDLE = False  # True => pausa sessão automaticamente ao ficar ocioso

# ----------------------------------------------------------------------------
# Modelo de Dados
# ----------------------------------------------------------------------------
SCHEMA = """
-- Sessões (como no seu app original)
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

-- Jornada (workday) por atendente
CREATE TABLE IF NOT EXISTS workdays (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent TEXT NOT NULL,
    login_time TEXT NOT NULL,       -- início da jornada
    logout_time TEXT,               -- fim da jornada
    idle_seconds INTEGER NOT NULL DEFAULT 0,  -- ociosidade acumulada
    idle_started_at TEXT,           -- quando entrou ocioso (se estiver ocioso)
    last_activity TEXT NOT NULL,    -- última atividade detectada
    status TEXT NOT NULL            -- active|idle|ended
);
CREATE INDEX IF NOT EXISTS idx_workdays_agent ON workdays(agent);
CREATE INDEX IF NOT EXISTS idx_workdays_status ON workdays(status);
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

@dataclass
class Workday:
    id: int
    agent: str
    login_time: datetime
    logout_time: Optional[datetime]
    idle_seconds: int
    idle_started_at: Optional[datetime]
    last_activity: datetime
    status: str  # active|idle|ended

# ----------------------------------------------------------------------------
# Camada de Persistência
# ----------------------------------------------------------------------------
class DB:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._ensure_db()

    def _ensure_db(self):
        with sqlite3.connect(self.db_path) as con:
            con.executescript(SCHEMA)

    # ------------------------- Helpers de parse ----------------------------
    @staticmethod
    def _parse_ts(x: Optional[str]) -> Optional[datetime]:
        if x is None:
            return None
        return datetime.fromisoformat(x)

    def _row_to_session(self, row: sqlite3.Row) -> Session:
        return Session(
            id=row[0],
            client=row[1],
            ticket=row[2] or "",
            category=row[3] or "",
            description=row[4] or "",
            start_time=DB._parse_ts(row[5]),
            end_time=DB._parse_ts(row[6]),
            status=row[7],
            paused_at=DB._parse_ts(row[8]),
            pause_seconds=row[9] or 0
        )

    def _row_to_workday(self, row: sqlite3.Row) -> Workday:
        return Workday(
            id=row[0],
            agent=row[1],
            login_time=DB._parse_ts(row[2]),
            logout_time=DB._parse_ts(row[3]),
            idle_seconds=row[4] or 0,
            idle_started_at=DB._parse_ts(row[5]),
            last_activity=DB._parse_ts(row[6]),
            status=row[7]
        )

    # ------------------------------ Workday --------------------------------
    def start_workday(self, agent: str) -> int:
        now = datetime.now(TZ).isoformat()
        with sqlite3.connect(self.db_path) as con:
            cur = con.cursor()
            cur.execute(
                """
                INSERT INTO workdays (agent, login_time, last_activity, status)
                VALUES (?, ?, ?, 'active')
                """,
                (agent.strip() or "Operador", now, now)
            )
            con.commit()
            return cur.lastrowid

    def get_active_workday(self, agent: str) -> Optional[Workday]:
        with sqlite3.connect(self.db_path) as con:
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            cur.execute(
                "SELECT * FROM workdays WHERE agent=? AND status IN ('active','idle') ORDER BY datetime(login_time) DESC LIMIT 1",
                (agent.strip() or "Operador",)
            )
            row = cur.fetchone()
            return self._row_to_workday(row) if row else None

    def get_workday(self, workday_id: int) -> Optional[Workday]:
        with sqlite3.connect(self.db_path) as con:
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            cur.execute("SELECT * FROM workdays WHERE id=?", (workday_id,))
            row = cur.fetchone()
            return self._row_to_workday(row) if row else None

    def bump_activity(self, workday_id: int, when: Optional[datetime] = None):
        """Registra atividade; se estava ocioso, computa a ociosidade e volta a active."""
        now = (when or datetime.now(TZ)).isoformat()
        with sqlite3.connect(self.db_path) as con:
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            cur.execute("SELECT idle_started_at, idle_seconds, status FROM workdays WHERE id=?", (workday_id,))
            row = cur.fetchone()
            if not row:
                return
            idle_started_at = row["idle_started_at"]
            idle_seconds = row["idle_seconds"] or 0
            status = row["status"]

            if status == "idle" and idle_started_at:
                # saiu do ocioso: acumula
                delta = int((datetime.fromisoformat(now) - datetime.fromisoformat(idle_started_at)).total_seconds())
                idle_seconds += max(0, delta)
                cur.execute(
                    "UPDATE workdays SET idle_seconds=?, idle_started_at=NULL, last_activity=?, status='active' WHERE id=?",
                    (idle_seconds, now, workday_id)
                )
            else:
                # apenas atualiza última atividade
                cur.execute("UPDATE workdays SET last_activity=? WHERE id=?", (now, workday_id))
            con.commit()

    def enter_idle(self, workday_id: int, when: Optional[datetime] = None):
        """Entra em estado ocioso (se já não estiver)."""
        now = (when or datetime.now(TZ)).isoformat()
        with sqlite3.connect(self.db_path) as con:
            con.execute(
                """
                UPDATE workdays 
                   SET status='idle', idle_started_at=COALESCE(idle_started_at, ?)
                 WHERE id=? AND status='active'
                """,
                (now, workday_id)
            )
            con.commit()

    def end_workday(self, workday_id: int, when: Optional[datetime] = None):
        """Finaliza a jornada; se estiver ocioso, acumula o trecho final e grava logout."""
        now_dt = when or datetime.now(TZ)
        with sqlite3.connect(self.db_path) as con:
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            cur.execute("SELECT idle_started_at, idle_seconds, status FROM workdays WHERE id=?", (workday_id,))
            row = cur.fetchone()
            if row:
                idle_started_at = row["idle_started_at"]
                idle_seconds = row["idle_seconds"] or 0
                status = row["status"]
                if status == "idle" and idle_started_at:
                    delta = int((now_dt - datetime.fromisoformat(idle_started_at)).total_seconds())
                    idle_seconds += max(0, delta)
                    cur.execute("UPDATE workdays SET idle_seconds=? WHERE id=?", (idle_seconds, workday_id))
            cur.execute(
                "UPDATE workdays SET status='ended', logout_time=?, idle_started_at=NULL WHERE id=?",
                (now_dt.isoformat(), workday_id)
            )
            con.commit()

    # Cálculo de tempo trabalhado (total - ocioso)
    def worked_seconds(self, wd: Workday, now: Optional[datetime] = None) -> int:
        now_dt = now or datetime.now(TZ)
        base_end = wd.logout_time or now_dt
        total = int((base_end - wd.login_time).total_seconds())
        idle = int(wd.idle_seconds or 0)
        if wd.status == "idle" and wd.idle_started_at:
            idle += int((now_dt - wd.idle_started_at).total_seconds())
        return max(0, total - idle)

    # ------------------------------ Sessions --------------------------------
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

# ----------------------------------------------------------------------------
# Regras de tempo (sessões)
# ----------------------------------------------------------------------------
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

# ----------------------------------------------------------------------------
# Hook global opcional (pynput) para atividade do usuário
# ----------------------------------------------------------------------------
class ActivityHook:
    """Liga ouvintes globais de teclado/mouse (se 'pynput' estiver instalado)."""
    def __init__(self, on_activity: Callable[[], None]):
        self.on_activity = on_activity
        self.k_listener = None
        self.m_listener = None
        try:
            from pynput import keyboard, mouse  # type: ignore
            self.k_listener = keyboard.Listener(on_press=self._kb)
            self.m_listener = mouse.Listener(on_move=self._ms, on_click=self._ms, on_scroll=self._ms)
            self.k_listener.daemon = True
            self.m_listener.daemon = True
            self.k_listener.start()
            self.m_listener.start()
        except Exception:
            # pynput não disponível: segue só com eventos do Flet
            pass

    def _kb(self, *args, **kwargs):
        try:
            self.on_activity()
        except Exception:
            pass

    def _ms(self, *args, **kwargs):
        try:
            self.on_activity()
        except Exception:
            pass

# ----------------------------------------------------------------------------
# UI
# ----------------------------------------------------------------------------
class TimeManagerApp:
    def __init__(self, page: ft.Page):
        self.page = page
        self.db = DB(DB_PATH)

        # Jornada/Agente
        self.agent_name: str = "Operador"
        self.active_workday_id: Optional[int] = None

        # Sessões
        self.active_session_id: Optional[int] = None

        # Loops assíncronos
        self._tick_running: bool = False
        self._tick_task = None
        self._wd_tick_running: bool = False
        self._wd_tick_task = None

        # Detecção de atividade (fallback Flet + opcional pynput)
        self._last_activity_bump = datetime.now(TZ)
        self.activity_hook = ActivityHook(self._activity_bump)

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

        self.bt_start = ft.ElevatedButton("Iniciar", icon=I.PLAY_ARROW, on_click=self.start_session)
        self.bt_pause = ft.OutlinedButton("Pausar", icon=I.PAUSE, disabled=True, on_click=self.pause_session)
        self.bt_resume = ft.OutlinedButton("Retomar", icon=I.PLAY_ARROW, disabled=True, on_click=self.resume_session)
        self.bt_finish = ft.FilledTonalButton("Finalizar", icon=I.STOP, disabled=True, on_click=self.finish_session)
        self.bt_discard = ft.TextButton("Descartar", icon=I.DELETE, disabled=True, on_click=self.discard_session)

        # --------------------- Timer Grande ---------------------------------
        self.big_timer = ft.Text(
            "00:00:00",
            size=120,  # grande
            weight=ft.FontWeight.W_700,
            font_family="RobotoMono",
            color=C.GREEN,
            text_align=ft.TextAlign.CENTER,
        )

        # Badge no AppBar (horas trabalhadas da jornada)
        self.timer_badge = ft.Text("00:00:00", weight=ft.FontWeight.BOLD, size=16, color=C.GREEN)

        # --------------------- Busca & Export --------------------------------
        self.search_client = ft.TextField(label="Filtrar por cliente", on_submit=lambda e: self.refresh_tables(), expand=True)
        self.bt_export = ft.OutlinedButton("Exportar CSV", icon=I.DOWNLOAD, on_click=self.export_csv)

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

        # Build UI e eventos globais
        self.build_ui()
        self.page.on_keyboard_event = lambda e: self._activity_bump()
        self.refresh_tables()
        self._ask_login_then_start()

    # -------------------------- UI BUILDERS --------------------------------
    def build_ui(self):
        self.page.title = "Gerenciador de Tempo - Atendimento"
        self.page.horizontal_alignment = ft.CrossAxisAlignment.STRETCH
        self.page.theme_mode = ft.ThemeMode.DARK

        # AppBar com ação "Encerrar dia"
        self.page.appbar = ft.AppBar(
            title=ft.Text("Gerenciador de Tempo"),
            actions=[
                ft.IconButton(I.EXIT_TO_APP, tooltip="Encerrar dia", on_click=self._end_workday_dialog),
                ft.Container(ft.Icon(I.ACCESS_TIME), padding=ft.padding.only(right=8)),
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
                        ft.Text("Contador de Tempo (Sessão ativa)", size=16, weight=ft.FontWeight.BOLD),
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

        # Root com detector de gestos para capturar cliques/arrastes (atividade)
        self.root = ft.GestureDetector(
            on_tap=lambda e: self._activity_bump(),
            on_double_tap=lambda e: self._activity_bump(),
            on_pan_update=lambda e: self._activity_bump(),
            content=ft.Container(
                content=ft.Column([new_session_card, big_timer_card, sessions_card], spacing=16),
                padding=16
            )
        )

        self.page.add(self.root)

        # Tema compacto
        self.page.theme = ft.Theme(visual_density=ft.VisualDensity.COMPACT)

    # -------------------------- LOGIN / WORKDAY ----------------------------
    def _ask_login_then_start(self):
        name_input = ft.TextField(label="Nome do atendente", value=self.agent_name, autofocus=True, expand=True)

        def _confirm(_):
            self.agent_name = (name_input.value or "Operador").strip()
            # Se já houver jornada ativa, usa; senão, cria
            wd = self.db.get_active_workday(self.agent_name)
            if wd:
                self.active_workday_id = wd.id
            else:
                self.active_workday_id = self.db.start_workday(self.agent_name)
            self._activity_bump()  # marca atividade inicial
            self._start_workday_engine()
            self.page.close(dialog)
            self._toast(f"Bem-vindo, {self.agent_name}! Jornada iniciada.")

        dialog = ft.AlertDialog(
            modal=True,
            title=ft.Text("Login do atendente"),
            content=name_input,
            actions=[
                ft.TextButton("Entrar", on_click=_confirm),
            ],
            actions_alignment=ft.MainAxisAlignment.END,
        )
        self.page.dialog = dialog
        dialog.open = True
        self.page.update()

    def _end_workday_dialog(self, e):
        if not self.active_workday_id:
            self._toast("Nenhuma jornada ativa.")
            return

        def _end(_):
            self._end_workday()
            self.page.close(dlg)

        dlg = ft.AlertDialog(
            modal=True,
            title=ft.Text("Encerrar dia"),
            content=ft.Text("Deseja encerrar a jornada atual? O contador será finalizado."),
            actions=[ft.TextButton("Cancelar", on_click=lambda _: self.page.close(dlg)),
                     ft.FilledButton("Encerrar", on_click=_end)],
            actions_alignment=ft.MainAxisAlignment.END,
        )
        self.page.dialog = dlg
        dlg.open = True
        self.page.update()

    def _end_workday(self):
        if not self.active_workday_id:
            return
        self.db.end_workday(self.active_workday_id)
        wd = self.db.get_workday(self.active_workday_id)
        if wd:
            worked = self.db.worked_seconds(wd)
            self.timer_badge.value = fmt_hms(worked)
            self.timer_badge.color = C.GREY_600
            self.page.update()
        self.active_workday_id = None
        self._stop_workday_engine()
        self._toast("Jornada encerrada.")

    # -------------------------- DETECÇÃO DE ATIVIDADE ----------------------
    def _activity_bump(self):
        """Qualquer atividade chama isso (tecla, clique, gesto, hook global...)."""
        self._last_activity_bump = datetime.now(TZ)
        if self.active_workday_id:
            self.db.bump_activity(self.active_workday_id)

    # -------------------------- WORKDAY ENGINE -----------------------------
    def _start_workday_engine(self):
        if self._wd_tick_running:
            return
        self._wd_tick_running = True
        self._wd_tick_task = self.page.run_task(self._workday_loop)

    def _stop_workday_engine(self):
        self._wd_tick_running = False
        self._wd_tick_task = None

    async def _workday_loop(self):
        """Atualiza badge e estado de ociosidade/atividade a cada 1s."""
        prev_state: Optional[str] = None
        while self._wd_tick_running:
            await asyncio.sleep(1)
            if not self.active_workday_id:
                continue

            wd = self.db.get_workday(self.active_workday_id)
            if not wd:
                continue

            now = datetime.now(TZ)
            # Regras de ociosidade
            idle_limit = wd.last_activity + timedelta(seconds=INACTIVITY_THRESHOLD_SECS)
            if wd.status == "active" and now >= idle_limit:
                # entrou em ocioso
                self.db.enter_idle(wd.id, now)
                wd = self.db.get_workday(wd.id) or wd
                # Pausar sessão automaticamente (se habilitado)
                if AUTO_PAUSE_SESSION_WHEN_IDLE and self.active_session_id:
                    s = self.db.get_session(self.active_session_id)
                    if s and s.status == "running":
                        self.db.update_status(s.id, "paused")
                        if self.active_session_id == s.id:
                            self.bt_pause.disabled = True
                            self.bt_resume.disabled = False

            # Atualiza badge (horas trabalhadas = total - ocioso)
            wd = self.db.get_workday(wd.id) or wd
            worked = self.db.worked_seconds(wd, now=now)
            self.timer_badge.value = fmt_hms(worked)
            # cor por estado
            if wd.status == "active":
                self.timer_badge.color = C.GREEN
            elif wd.status == "idle":
                self.timer_badge.color = C.AMBER
            else:
                self.timer_badge.color = C.GREY_600

            # Notificação leve de mudança de estado
            if prev_state != wd.status:
                if wd.status == "idle":
                    self._toast("Inatividade detectada: marcando como ocioso.")
                elif wd.status == "active" and prev_state is not None:
                    self._toast("Atividade retomada.")
            prev_state = wd.status

            self.page.update()

    # -------------------------- TICK (Sessões) -----------------------------
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

    # -------------------------- EVENTOS / AÇÕES (Sessões) ------------------
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
        self.big_timer.color = C.GREEN
        self.page.update()
        self._stop_ticks()

    # -------------------------- DISPLAY (Sessão) ---------------------------
    def _update_timer_display(self):
        if not self.active_session_id:
            return
        s = self.db.get_session(self.active_session_id)
        if not s:
            return
        secs = elapsed_seconds(s, now=datetime.now(TZ))
        t = fmt_hms(secs)
        self.big_timer.value = t
        # cor: verde = running, âmbar = paused
        self.big_timer.color = C.GREEN if s.status == "running" else C.AMBER
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
            items.append(ft.IconButton(I.PAUSE, icon_color=C.AMBER, tooltip="Pausar", on_click=_pause))
            items.append(ft.IconButton(I.STOP, icon_color=C.RED, tooltip="Finalizar", on_click=_finish))
        elif s.status == "paused":
            items.append(ft.IconButton(I.PLAY_ARROW, icon_color=C.GREEN, tooltip="Retomar", on_click=_resume))
            items.append(ft.IconButton(I.STOP, icon_color=C.RED, tooltip="Finalizar", on_click=_finish))
        elif s.status == "finished":
            items.append(ft.IconButton(I.DELETE, icon_color=C.GREY_600, tooltip="Descartar", on_click=_discard))
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

# ----------------------------------------------------------------------------
# ENTRYPOINT
# ----------------------------------------------------------------------------
def main(page: ft.Page):
    page.padding = 0
    page.scroll = ft.ScrollMode.AUTO
    page.theme = ft.Theme(visual_density=ft.VisualDensity.COMPACT)
    TimeManagerApp(page)

if __name__ == "__main__":
    ft.app(target=main)