# -*- coding: utf-8 -*-
"""
Temporizador de Atendimento — Multi-login (Flet)
Versão: routed_views
- "Minha Senha" abre rota /minha-senha (PageView dedicada)
- "Usuários (Admin)" abre rota /admin/usuarios (PageView dedicada)
- Sem AlertDialog: confirmações ficam inline na Home
"""

from __future__ import annotations

import asyncio
import csv
import json
import os
import secrets
import sqlite3
import hashlib
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Any, Callable, Dict, Tuple
from zoneinfo import ZoneInfo

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

# ---------------------------------------------------------------------------
# Constantes/Paths
# ---------------------------------------------------------------------------
TZ = ZoneInfo("America/Sao_Paulo")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
EXPORT_DIR = os.path.join(BASE_DIR, "export")
CONFIG_DIR = os.path.join(BASE_DIR, "config")
DB_PATH = os.path.join(DATA_DIR, "time_manager.db")
SETTINGS_PATH = os.path.join(CONFIG_DIR, "settings.json")

for d in (DATA_DIR, EXPORT_DIR, CONFIG_DIR):
    os.makedirs(d, exist_ok=True)

BADGE_ZERO_AO_ENCERRAR = True
SQLITE_BUSY_TIMEOUT_MS = 10_000
SQLITE_DEFAULT_TIMEOUT = 30.0

# ---------------------------------------------------------------------------
# Settings
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
# Schema base
# ---------------------------------------------------------------------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client TEXT NOT NULL,
    ticket TEXT,
    category TEXT,
    description TEXT,
    start_time TEXT NOT NULL,
    end_time TEXT,
    status TEXT NOT NULL,
    paused_at TEXT,
    pause_seconds INTEGER NOT NULL DEFAULT 0,
    agent TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS workdays (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    login_time TEXT NOT NULL,
    logout_time TEXT,
    idle_seconds INTEGER NOT NULL DEFAULT 0,
    idle_started_at TEXT,
    last_activity TEXT NOT NULL,
    status TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    salt TEXT NOT NULL,
    is_admin INTEGER NOT NULL DEFAULT 0,
    active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    last_login_at TEXT
);
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
    agent: str

@dataclass
class Workday:
    id: int
    agent: str
    instance_id: str
    login_time: datetime
    logout_time: Optional[datetime]
    idle_seconds: int
    idle_started_at: Optional[datetime]
    last_activity: datetime
    status: str

@dataclass
class User:
    id: int
    username: str
    is_admin: bool
    active: bool
    created_at: datetime
    last_login_at: Optional[datetime]

# ---------------------------------------------------------------------------
# Senhas
# ---------------------------------------------------------------------------
def _gen_salt(n: int = 16) -> bytes:
    return secrets.token_bytes(n)

def _hash_password(password: str, salt: bytes, rounds: int = 200_000) -> str:
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, rounds, dklen=32)
    return dk.hex()

# ---------------------------------------------------------------------------
# Banco
# ---------------------------------------------------------------------------
class DB:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._ensure_db()
        self._migrate()

    def connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.db_path, timeout=SQLITE_DEFAULT_TIMEOUT, check_same_thread=False)
        try:
            con.execute("PRAGMA journal_mode=WAL;")
        except Exception:
            pass
        try:
            con.execute("PRAGMA synchronous=NORMAL;")
        except Exception:
            pass
        try:
            con.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS};")
        except Exception:
            pass
        try:
            con.execute("PRAGMA foreign_keys=ON;")
        except Exception:
            pass
        return con

    def _ensure_db(self):
        with self.connect() as con:
            con.executescript(SCHEMA)

    def _migrate(self):
        with self.connect() as con:
            cur = con.cursor()
            def safe(sql):
                try: con.execute(sql)
                except Exception: pass
            # Índices
            safe("CREATE INDEX IF NOT EXISTS idx_sessions_status ON sessions(status);")
            safe("CREATE INDEX IF NOT EXISTS idx_sessions_start ON sessions(start_time);")
            safe("CREATE INDEX IF NOT EXISTS idx_sessions_agent ON sessions(agent);")
            safe("CREATE INDEX IF NOT EXISTS idx_workdays_agent ON workdays(agent);")
            safe("CREATE INDEX IF NOT EXISTS idx_workdays_status ON workdays(status);")
            safe("CREATE INDEX IF NOT EXISTS idx_workdays_agent_instance ON workdays(agent, instance_id, status);")
            con.commit()

    @staticmethod
    def _parse_ts(x: Optional[str]) -> Optional[datetime]:
        if x is None:
            return None
        return datetime.fromisoformat(x)

    def _row_to_session(self, row: sqlite3.Row) -> Session:
        keys = set(row.keys())
        return Session(
            id=row["id"],
            client=row["client"],
            ticket=row["ticket"] if "ticket" in keys else "",
            category=row["category"] if "category" in keys else "",
            description=row["description"] if "description" in keys else "",
            start_time=self._parse_ts(row["start_time"]),
            end_time=self._parse_ts(row["end_time"]),
            status=row["status"],
            paused_at=self._parse_ts(row["paused_at"]),
            pause_seconds=row["pause_seconds"] or 0,
            agent=row["agent"] if "agent" in keys else ""
        )

    def _row_to_workday(self, row: sqlite3.Row) -> Workday:
        keys = set(row.keys())
        return Workday(
            id=row["id"],
            agent=row["agent"],
            instance_id=row["instance_id"] if "instance_id" in keys else "legacy",
            login_time=self._parse_ts(row["login_time"]),
            logout_time=self._parse_ts(row["logout_time"]),
            idle_seconds=row["idle_seconds"] or 0,
            idle_started_at=self._parse_ts(row["idle_started_at"]),
            last_activity=self._parse_ts(row["last_activity"]),
            status=row["status"]
        )

    def _row_to_user(self, row: sqlite3.Row) -> User:
        return User(
            id=row["id"],
            username=row["username"],
            is_admin=bool(row["is_admin"]),
            active=bool(row["active"]),
            created_at=datetime.fromisoformat(row["created_at"]),
            last_login_at=datetime.fromisoformat(row["last_login_at"]) if row["last_login_at"] else None,
        )

    # ---------------- Users ----------------
    def users_count(self) -> int:
        with self.connect() as con:
            cur = con.cursor()
            cur.execute("SELECT COUNT(*) FROM users")
            return int(cur.fetchone()[0])

    def active_admin_count(self) -> int:
        with self.connect() as con:
            cur = con.cursor()
            cur.execute("SELECT COUNT(*) FROM users WHERE is_admin=1 AND active=1")
            return int(cur.fetchone()[0])

    def list_users(self) -> List[User]:
        with self.connect() as con:
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            cur.execute("SELECT * FROM users ORDER BY username ASC")
            rows = cur.fetchall()
            return [self._row_to_user(r) for r in rows]

    def get_user_by_username(self, username: str) -> Optional[User]:
        with self.connect() as con:
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            cur.execute("SELECT * FROM users WHERE username=?", (username.strip(),))
            row = cur.fetchone()
            return self._row_to_user(row) if row else None

    def create_user(self, username: str, password: str, is_admin: bool = False) -> int:
        username = username.strip()
        if not username or not password:
            raise ValueError("Usuário e senha são obrigatórios.")
        salt = _gen_salt()
        ph = _hash_password(password, salt)
        now = datetime.now(TZ).isoformat()
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(
                """
                INSERT INTO users (username, password_hash, salt, is_admin, active, created_at)
                VALUES (?, ?, ?, ?, 1, ?)
                """,
                (username, ph, salt.hex(), 1 if is_admin else 0, now)
            )
            con.commit()
            return cur.lastrowid

    def set_user_password(self, username_or_id: str | int, new_password: str) -> None:
        if not new_password:
            raise ValueError("Senha não pode ser vazia.")
        salt = _gen_salt()
        ph = _hash_password(new_password, salt)
        with self.connect() as con:
            if isinstance(username_or_id, int):
                con.execute("UPDATE users SET password_hash=?, salt=? WHERE id=?", (ph, salt.hex(), username_or_id))
            else:
                con.execute("UPDATE users SET password_hash=?, salt=? WHERE username=?", (ph, salt.hex(), username_or_id))
            con.commit()

    def set_user_admin(self, user_id: int, make_admin: bool) -> Tuple[bool, str]:
        with self.connect() as con:
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            cur.execute("SELECT is_admin, active FROM users WHERE id=?", (user_id,))
            row = cur.fetchone()
            if not row:
                return False, "Usuário não encontrado."
            is_admin_now = bool(row["is_admin"])
            is_active = bool(row["active"])
            if not make_admin and is_admin_now and is_active and self.active_admin_count() <= 1:
                return False, "Não é possível remover a permissão do último administrador ativo."
            cur.execute("UPDATE users SET is_admin=? WHERE id=?", (1 if make_admin else 0, user_id))
            con.commit()
            return True, "Permissão atualizada."

    def set_user_active(self, user_id: int, make_active: bool) -> Tuple[bool, str]:
        with self.connect() as con:
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            cur.execute("SELECT is_admin, active FROM users WHERE id=?", (user_id,))
            row = cur.fetchone()
            if not row:
                return False, "Usuário não encontrado."
            is_admin_now = bool(row["is_admin"])
            is_active_now = bool(row["active"])
            if not make_active and is_admin_now and is_active_now and self.active_admin_count() <= 1:
                return False, "Não é possível desativar o último administrador ativo."
            cur.execute("UPDATE users SET active=? WHERE id=?", (1 if make_active else 0, user_id))
            con.commit()
            return True, "Status atualizado."

    def delete_user(self, user_id: int) -> Tuple[bool, str]:
        with self.connect() as con:
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            cur.execute("SELECT is_admin, active FROM users WHERE id=?", (user_id,))
            row = cur.fetchone()
            if not row:
                return False, "Usuário não encontrado."
            is_admin_now = bool(row["is_admin"])
            is_active_now = bool(row["active"])
            if is_admin_now and is_active_now and self.active_admin_count() <= 1:
                return False, "Não é possível excluir o último administrador ativo."
            cur.execute("DELETE FROM users WHERE id=?", (user_id,))
            con.commit()
            return True, "Usuário excluído."

    def verify_login(self, username: str, password: str) -> Optional[User]:
        with self.connect() as con:
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            cur.execute("SELECT * FROM users WHERE username=? AND active=1", (username.strip(),))
            row = cur.fetchone()
            if not row:
                return None
            salt = bytes.fromhex(row["salt"])
            ph = _hash_password(password, salt)
            if ph != row["password_hash"]:
                return None
            cur.execute("UPDATE users SET last_login_at=? WHERE id=?", (datetime.now(TZ).isoformat(), row["id"]))
            con.commit()
            return self._row_to_user(row)

    # ---------------- Workday ----------------
    def start_workday(self, agent: str, instance_id: str) -> int:
        now = datetime.now(TZ).isoformat()
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(
                """INSERT INTO workdays (agent, instance_id, login_time, last_activity, status)
                   VALUES (?, ?, ?, ?, 'active')""",
                (agent.strip() or "Operador", instance_id, now, now)
            )
            con.commit()
            return cur.lastrowid

    def get_active_workday(self, agent: str, instance_id: str) -> Optional[Workday]:
        with self.connect() as con:
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            cur.execute(
                """SELECT * FROM workdays
                   WHERE agent=? AND instance_id=? AND status IN ('active','idle')
                   ORDER BY datetime(login_time) DESC LIMIT 1""",
                (agent.strip() or "Operador", instance_id)
            )
            row = cur.fetchone()
            return self._row_to_workday(row) if row else None

    def get_workday(self, workday_id: int) -> Optional[Workday]:
        with self.connect() as con:
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            cur.execute("SELECT * FROM workdays WHERE id=?", (workday_id,))
            row = cur.fetchone()
            return self._row_to_workday(row) if row else None

    def bump_activity(self, workday_id: int, when: Optional[datetime] = None):
        now = (when or datetime.now(TZ)).isoformat()
        with self.connect() as con:
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
                delta = int((datetime.fromisoformat(now) - datetime.fromisoformat(idle_started_at)).total_seconds())
                idle_seconds += max(0, delta)
                cur.execute(
                    "UPDATE workdays SET idle_seconds=?, idle_started_at=NULL, last_activity=?, status='active' WHERE id=?",
                    (idle_seconds, now, workday_id)
                )
            else:
                cur.execute("UPDATE workdays SET last_activity=? WHERE id=?", (now, workday_id))
            con.commit()

    def end_workday(self, workday_id: int, when: Optional[datetime] = None):
        now_dt = when or datetime.now(TZ)
        with self.connect() as con:
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

    def worked_seconds(self, wd: Workday, now: Optional[datetime] = None) -> int:
        now_dt = now or datetime.now(TZ)
        base_end = wd.logout_time or now_dt
        total = int((base_end - wd.login_time).total_seconds())
        idle = int(wd.idle_seconds or 0)
        if wd.status == "idle" and wd.idle_started_at:
            idle += int((now_dt - wd.idle_started_at).total_seconds())
        return max(0, total - idle)

    # ---------------- Sessions ----------------
    def create_session(self, client: str, ticket: str, category: str, description: str, agent: str) -> int:
        now = datetime.now(TZ).isoformat()
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(
                """
                INSERT INTO sessions (client, ticket, category, description, start_time, status, agent)
                VALUES (?, ?, ?, ?, ?, 'running', ?)
                """,
                (client.strip(), (ticket or "").strip(), (category or "").strip(), (description or "").strip(), now, agent.strip())
            )
            con.commit()
            return cur.lastrowid

    def update_status(self, session_id: int, status: str, when: Optional[datetime] = None):
        with self.connect() as con:
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
        with self.connect() as con:
            con.execute(
                "UPDATE sessions SET pause_seconds = pause_seconds + ?, paused_at=NULL WHERE id=?",
                (int(max(0, seconds)), session_id)
            )
            con.commit()

    def get_session(self, session_id: int) -> Optional[Session]:
        with self.connect() as con:
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            cur.execute("SELECT * FROM sessions WHERE id=?", (session_id,))
            row = cur.fetchone()
            return self._row_to_session(row) if row else None

    def list_sessions(self, client_like: Optional[str] = None, agent_exact: Optional[str] = None, limit: int = 300) -> List[Session]:
        q = "SELECT * FROM sessions WHERE 1=1"
        params: List[Any] = []
        if client_like:
            q += " AND client LIKE ?"
            params.append(f"%{client_like}%")
        if agent_exact:
            q += " AND agent = ?"
            params.append(agent_exact)
        q += " ORDER BY datetime(start_time) DESC LIMIT ?"
        params.append(limit)

        with self.connect() as con:
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            cur.execute(q, params)
            rows = cur.fetchall()
            return [self._row_to_session(r) for r in rows]

# ---------------------------------------------------------------------------
# UI: Login
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
        self.page.title = "Login - Temporizador de Atendimento"
        self.page.theme_mode = ft.ThemeMode.DARK
        self.page.theme = ft.Theme(
            color_scheme_seed=C.TEAL,
            use_material3=True,
            visual_density=ft.VisualDensity.COMPACT,
        )
        self.page.padding = 0

        settings = load_settings()
        last_user = settings.get("last_username", "")

        self.user = ft.TextField(label="Usuário", autofocus=True, expand=True, prefix_icon=I.PERSON, value=last_user)
        self.password = ft.TextField(label="Senha", password=True, can_reveal_password=True, expand=True, prefix_icon=I.LOCK)
        self.remember = ft.Checkbox(label="Lembrar usuário", value=bool(last_user))

        def do_login(e):
            if not self.user.value or not self.password.value:
                self._toast("Informe usuário e senha.")
                return
            u = self.db.verify_login(self.user.value, self.password.value)
            if not u:
                self._toast("Usuário e/ou senha inválidos ou usuário inativo.")
                return
            if self.remember.value:
                s = load_settings()
                s["last_username"] = self.user.value.strip()
                save_settings(s)
            else:
                s = load_settings()
                s.pop("last_username", None)
                save_settings(s)
            self.on_success(u)

        # Painel inline (primeiro admin)
        self.adm_user = ft.TextField(label="Usuário (admin)", width=220)
        self.adm_p1 = ft.TextField(label="Senha", password=True, can_reveal_password=True, width=220)
        self.adm_p2 = ft.TextField(label="Confirmar senha", password=True, can_reveal_password=True, width=220)

        def do_create_admin(e):
            if self.db.users_count() > 0:
                self._toast("Já existe usuário cadastrado. Peça a um administrador.")
                self._toggle_admin_panel(False)
                return
            if not self.adm_user.value or not self.adm_p1.value or not self.adm_p2.value:
                self._toast("Preencha usuário e senha.")
                return
            if self.adm_p1.value != self.adm_p2.value:
                self._toast("Senhas não conferem.")
                return
            try:
                self.db.create_user(self.adm_user.value, self.adm_p1.value, is_admin=True)
            except sqlite3.IntegrityError:
                self._toast("Usuário já existe.")
                return
            except Exception as ex:
                self._toast(f"Erro ao criar admin: {ex}")
                return
            # Auto-login após criar admin
            self._toast("Administrador criado. Entrando...")
            try:
                s = load_settings()
                s["last_username"] = self.adm_user.value.strip()
                save_settings(s)
            except Exception:
                pass
            u = self.db.verify_login(self.adm_user.value, self.adm_p1.value)
            if u:
                self.on_success(u)
                return
            else:
                self._toast("Administrador criado, mas não consegui efetuar login automático. Faça login manualmente.")
                self._toggle_admin_panel(False)

        self.create_admin_panel = ft.Container(
            visible=False,
            bgcolor=C.GREY_900,
            border_radius=12,
            padding=12,
            content=ft.Column([
                ft.Text("Criar Administrador (primeiro acesso)", size=14, weight=ft.FontWeight.BOLD),
                ft.Row([self.adm_user, self.adm_p1, self.adm_p2], wrap=True, spacing=10),
                ft.Row([
                    ft.FilledButton("Criar", icon=I.CHECK, on_click=do_create_admin),
                    ft.TextButton("Cancelar", icon=I.CLOSE, on_click=lambda e: self._toggle_admin_panel(False)),
                ], spacing=8)
            ], spacing=8)
        )

        create_admin_btn = ft.TextButton("Primeiro acesso? Criar administrador", icon=I.ADMIN_PANEL_SETTINGS,
                                         on_click=lambda e: self._toggle_admin_panel(True))
        login_btn = ft.FilledButton("Entrar", icon=I.CHEVRON_RIGHT, on_click=do_login, expand=True)
        forgot_btn = ft.TextButton("Esqueci minha senha", icon=I.HELP_OUTLINE,
                                   on_click=lambda e: self._toast("Procure o administrador para redefinir sua senha."))

        controls = [
            ft.Container(
                alignment=ft.alignment.center,
                expand=True,
                content=ft.Column(
                    [
                        ft.Row([ft.Container(width=72, height=72, border_radius=36, bgcolor=C.TEAL_900,
                                             alignment=ft.alignment.center, content=ft.Icon(I.LOCK, size=36, color=C.TEAL_200))],
                               alignment=ft.MainAxisAlignment.CENTER),
                        ft.Text("Acessar sua conta", size=22, weight=ft.FontWeight.W_700, text_align=ft.TextAlign.CENTER),
                        ft.Text("Entre para iniciar sua jornada e registrar atendimentos",
                                size=12, color=C.GREY_500, text_align=ft.TextAlign.CENTER),
                    ],
                    spacing=10, horizontal_alignment=ft.CrossAxisAlignment.CENTER
                ),
            ),
            ft.Divider(color=C.GREY_800),
            self.user,
            self.password,
            self.remember,
            login_btn,
            ft.Row([forgot_btn], alignment=ft.MainAxisAlignment.CENTER),
        ]

        if self.db.users_count() == 0:
            controls += [ft.Divider(color=C.GREY_900),
                         ft.Row([create_admin_btn], alignment=ft.MainAxisAlignment.CENTER),
                         self.create_admin_panel]

        card = ft.Card(
            elevation=12,
            content=ft.Container(width=560, padding=24, border_radius=ft.border_radius.all(18),
                                 content=ft.Column(controls, spacing=12, tight=True))
        )

        backdrop = ft.Container(expand=True, gradient=ft.LinearGradient(
            begin=ft.alignment.top_left, end=ft.alignment.bottom_right,
            colors=[C.BLUE_GREY_900, C.BLACK],
        ))

        root = ft.Stack(controls=[backdrop, ft.Container(content=card, alignment=ft.alignment.center, expand=True, padding=20)])
        self.page.controls.clear()
        self.page.add(root)
        self.page.update()

        if self.db.users_count() == 0:
            self._toggle_admin_panel(True)

    def _toggle_admin_panel(self, show: bool):
        self.create_admin_panel.visible = show
        self.page.update()

# ---------------------------------------------------------------------------
# UI: App principal
# ---------------------------------------------------------------------------
class TimeManagerApp:
    def __init__(self, page: ft.Page, db: DB, user: User, on_logout: Callable[[], None]):
        self.page = page
        self.db = db
        self.user = user
        self.on_logout = on_logout

        self.agent_name: str = user.username
        self.instance_id: str = secrets.token_hex(8)

        self.active_workday_id: Optional[int] = None
        self.active_session_id: Optional[int] = None

        self._badge_frozen: bool = False

        self._tick_running: bool = False
        self._tick_task = None
        self._wd_tick_running: bool = False
        self._wd_tick_task = None

        self._confirm_ok_cb: Optional[Callable] = None
        self._confirm_cancel_cb: Optional[Callable] = None

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

        self.big_timer = ft.Text("00:00:00", size=120, weight=ft.FontWeight.W_700,
                                 font_family="RobotoMono", color=C.GREEN, text_align=ft.TextAlign.CENTER)
        self.timer_badge = ft.Text("00:00:00", weight=ft.FontWeight.BOLD, size=16, color=C.GREEN)

        self.search_client = ft.TextField(label="Filtrar por cliente", on_submit=lambda e: self.refresh_tables(), expand=True)
        self.bt_export = ft.OutlinedButton("Exportar CSV", icon=I.DOWNLOAD, on_click=self.export_csv)

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

        # Campos reutilizados pela view de senha
        self._pwd_old = ft.TextField(label="Senha atual", password=True, can_reveal_password=True, width=220)
        self._pwd_new1 = ft.TextField(label="Nova senha", password=True, can_reveal_password=True, width=220)
        self._pwd_new2 = ft.TextField(label="Confirmar nova senha", password=True, can_reveal_password=True, width=220)

        # Painel inline: confirmações (continua inline)
        self._build_confirm_panel()

        self.build_ui()
        self.refresh_tables()
        self._start_workday_after_login()

        # Router
        self.page.on_route_change = self._on_route_change
        self.page.on_view_pop = self._on_view_pop

    # ---------- Painel de confirmação (inline) ----------
    def _build_confirm_panel(self):
        self._confirm_title = ft.Text("", size=16, weight=ft.FontWeight.BOLD)
        self._confirm_msg = ft.Text("")
        def _cancel(_):
            self._hide_confirm()
            if self._confirm_cancel_cb:
                try: self._confirm_cancel_cb()
                except Exception: pass
        def _ok(_):
            self._hide_confirm()
            if self._confirm_ok_cb:
                try: self._confirm_ok_cb()
                except Exception: pass
        self.confirm_panel = ft.Container(
            visible=False,
            padding=12,
            bgcolor=op(0.03, C.WHITE),
            border_radius=12,
            content=ft.Column(
                controls=[
                    self._confirm_title,
                    self._confirm_msg,
                    ft.Row([
                        ft.OutlinedButton("Cancelar", icon=I.CLOSE, on_click=_cancel),
                        ft.FilledButton("Confirmar", icon=I.CHECK, on_click=_ok),
                    ], alignment=ft.MainAxisAlignment.END),
                ],
                spacing=10,
            ),
        )

    def _show_confirm(self, title: str, msg: str, on_ok: Callable, on_cancel: Optional[Callable] = None):
        self._confirm_title.value = title
        self._confirm_msg.value = msg
        self._confirm_ok_cb = on_ok
        self._confirm_cancel_cb = on_cancel
        self.confirm_panel.visible = True
        self.page.update()

    def _hide_confirm(self):
        self.confirm_panel.visible = False
        self._confirm_ok_cb = None
        self._confirm_cancel_cb = None
        self.page.update()

    # ---------- AppBar (navega por rotas) ----------
    def _build_appbar(self) -> ft.AppBar:
        actions = [
            ft.IconButton(I.KEY, tooltip="Minha senha", on_click=self._go_my_password),
        ]
        if self.user.is_admin:
            actions.append(ft.IconButton(I.ADMIN_PANEL_SETTINGS, tooltip="Usuários (Admin)", on_click=self._go_users_admin))
        actions.extend([
            ft.IconButton(I.SWITCH_ACCOUNT, tooltip="Trocar usuário", on_click=self._ask_logout),
            ft.IconButton(I.EXIT_TO_APP, tooltip="Encerrar dia", on_click=self._ask_end_workday),
            ft.Container(ft.Text(f"ID:{self.instance_id[:4]}", size=11, color=C.GREY_500), padding=ft.padding.only(right=8)),
            ft.Container(ft.Icon(I.ACCESS_TIME), padding=ft.padding.only(right=8)),
            ft.Container(self.timer_badge, padding=ft.padding.only(right=16)),
        ])
        return ft.AppBar(title=ft.Text(f"Gerenciador de Tempo — {self.agent_name}"), actions=actions)

    # ---------- UI principal ----------
    def build_ui(self):
        self.page.title = f"Gerenciador de Tempo - {self.agent_name}"
        self.page.horizontal_alignment = ft.CrossAxisAlignment.STRETCH
        self.page.theme_mode = ft.ThemeMode.DARK

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
                    tight=True, spacing=10,
                ),
                padding=16,
            ),
        )

        big_timer_card = ft.Card(
            content=ft.Container(
                content=ft.Column(
                    [
                        ft.Text("Contador de Tempo (Sessão ativa)", size=16, weight=ft.FontWeight.BOLD),
                        ft.Container(content=self.big_timer, alignment=ft.alignment.center, padding=ft.padding.symmetric(vertical=10)),
                    ],
                    spacing=8, horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                ),
                padding=16,
            ),
        )

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

        # Home: não inclui painel de senha; abre por rota
        self.root = ft.Container(
            content=ft.Column(
                [
                    self.confirm_panel,
                    new_session_card,
                    big_timer_card,
                    sessions_card,
                ],
                spacing=16
            ),
            padding=16
        )

        self.page.controls.clear()
        self.page.views.clear()
        self.page.views.append(ft.View("/", controls=[self.root], appbar=self._build_appbar()))
        self.page.update()

    # ---------- Ações AppBar ----------
    def _ask_logout(self, e):
        def _do():
            self._end_workday()
            self.on_logout()
        self._show_confirm("Trocar usuário", "Deseja encerrar a jornada e voltar à tela de login?", _do)

    def _ask_end_workday(self, e):
        if not self.active_workday_id:
            self._toast("Nenhuma jornada ativa.")
            return
        def _do():
            self._end_workday()
        self._show_confirm("Encerrar dia", "Deseja encerrar a jornada atual? O contador será finalizado.", _do)

    # ---------- Navegação ----------
    def _go_users_admin(self, e):
        if not self.user.is_admin:
            self._toast("Acesso negado: somente Administrador.")
            return
        self.page.go("/admin/usuarios")

    def _go_my_password(self, e):
        self.page.go("/minha-senha")

    def _on_route_change(self, e: ft.RouteChangeEvent):
        route = e.route or "/"
        self.page.views.clear()
        self.page.views.append(ft.View("/", controls=[self.root], appbar=self._build_appbar()))

        if route.startswith("/admin/usuarios"):
            if not self.user.is_admin:
                self._toast("Acesso negado: somente Administrador.")
            else:
                self.page.views.append(self._users_admin_view())

        if route.startswith("/minha-senha"):
            self.page.views.append(self._my_password_view())

        self.page.update()

    def _on_view_pop(self, e: ft.ViewPopEvent):
        if len(self.page.views) > 1:
            self.page.views.pop()
        top = self.page.views[-1]
        self.page.go(top.route)

    # ---------- Jornada ----------
    def _start_workday_after_login(self):
        self._badge_frozen = False

        wd = self.db.get_active_workday(self.agent_name, self.instance_id)
        if wd:
            self.active_workday_id = wd.id
        else:
            self.active_workday_id = self.db.start_workday(self.agent_name, self.instance_id)
        self._start_workday_engine()
        self._toast(f"Bem-vindo, {self.agent_name}! Jornada iniciada (instância {self.instance_id}).")

    def _end_workday(self):
        try:
            if self.active_session_id:
                s = self.db.get_session(self.active_session_id)
                if s:
                    if s.status == "paused" and s.paused_at:
                        paused_secs = int((datetime.now(TZ) - s.paused_at).total_seconds())
                        self.db.accumulate_pause(s.id, paused_secs)
                    if s.status in ("running", "paused"):
                        self.db.update_status(s.id, "finished")
                self._clear_active_session()
                self.refresh_tables()
        except Exception as ex:
            self._toast(f"Obs.: não consegui finalizar a sessão ativa automaticamente ({ex}).")

        if not self.active_workday_id:
            return

        self._badge_frozen = True
        self.db.end_workday(self.active_workday_id)

        if BADGE_ZERO_AO_ENCERRAR:
            self.timer_badge.value = "00:00:00"
        else:
            wd = self.db.get_workday(self.active_workday_id)
            if wd:
                worked = self.db.worked_seconds(wd)
                self.timer_badge.value = fmt_hms(worked)

        self.timer_badge.color = C.GREY_600
        self.page.update()

        self.active_workday_id = None
        self._stop_workday_engine()
        self._toast("Jornada encerrada.")

    def _start_workday_engine(self):
        if self._wd_tick_running:
            return
        self._wd_tick_running = True
        self._wd_tick_task = self.page.run_task(self._workday_loop)

    def _stop_workday_engine(self):
        self._wd_tick_running = False
        self._wd_tick_task = None

    async def _workday_loop(self):
        while self._wd_tick_running:
            if self._badge_frozen:
                break
            await asyncio.sleep(1)
            if not self.active_workday_id:
                continue
            wd = self.db.get_workday(self.active_workday_id)
            if not wd:
                continue
            now = datetime.now(TZ)
            worked = self.db.worked_seconds(wd, now=now)
            self.timer_badge.value = fmt_hms(worked)
            self.timer_badge.color = C.GREEN
            self.page.update()

    def _start_ticks(self):
        if self._tick_running:
            return
        self._tick_running = True
        self._tick_task = self.page.run_task(self._tick_loop)

    def _stop_ticks(self):
        self._tick_running = False
        self._tick_task = None

    async def _tick_loop(self):
        while self._tick_running and self.active_session_id:
            self._update_timer_display()
            await asyncio.sleep(1)

    # ---------- Sessões ----------
    def start_session(self, e):
        client = (self.in_client.value or "").strip()
        if not client:
            self._toast("Informe o cliente (obrigatório).")
            return
        ticket = self.in_ticket.value or ""
        category = self.in_category.value or ""
        description = self.in_desc.value or ""

        sid = self.db.create_session(client, ticket, category, description, agent=self.agent_name)
        self.active_session_id = sid
        self._toast(f"Sessão iniciada (ID {sid}).")

        self.bt_start.disabled = True
        self.bt_pause.disabled = False
        self.bt_resume.disabled = True
        self.bt_finish.disabled = False
        self.bt_discard.disabled = False
        self.in_desc.value = ""
        self.page.update()

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

    def _update_timer_display(self):
        if not self.active_session_id:
            return
        s = self.db.get_session(self.active_session_id)
        if not s:
            return
        secs = elapsed_seconds(s, now=datetime.now(TZ))
        self.big_timer.value = fmt_hms(secs)
        self.big_timer.color = C.GREEN if s.status == "running" else C.AMBER
        self.page.update()

    def refresh_tables(self):
        client_like = self.search_client.value.strip() if self.search_client.value else None
        rows = self.db.list_sessions(client_like=client_like, agent_exact=self.agent_name, limit=300)
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
            items += [
                ft.IconButton(I.PAUSE, icon_color=C.AMBER, tooltip="Pausar", on_click=_pause),
                ft.IconButton(I.STOP, icon_color=C.RED, tooltip="Finalizar", on_click=_finish),
            ]
        elif s.status == "paused":
            items += [
                ft.IconButton(I.PLAY_ARROW, icon_color=C.GREEN, tooltip="Retomar", on_click=_resume),
                ft.IconButton(I.STOP, icon_color=C.RED, tooltip="Finalizar", on_click=_finish),
            ]
        elif s.status == "finished":
            items += [ft.IconButton(I.DELETE, icon_color=C.GREY_600, tooltip="Descartar", on_click=_discard)]
        return ft.Row(items, spacing=4)

    def export_csv(self, e):
        rows = self.db.list_sessions(client_like=None, agent_exact=self.agent_name, limit=99999)
        fn = f"sessoes_{datetime.now(TZ).strftime('%Y%m%d_%H%M%S')}.csv"
        path = os.path.join(EXPORT_DIR, fn)
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow(["ID", "Agente", "Cliente", "Ticket", "Categoria", "Descrição", "Início", "Fim", "Status", "Tempo(HH:MM:SS)", "Pausas(seg)"])
            now = datetime.now(TZ)
            for s in rows:
                w.writerow([
                    s.id, s.agent, s.client, s.ticket, s.category, s.description,
                    s.start_time.astimezone(TZ).strftime("%d/%m/%Y %H:%M:%S"),
                    s.end_time.astimezone(TZ).strftime("%d/%m/%Y %H:%M:%S") if s.end_time else "",
                    s.status, fmt_hms(elapsed_seconds(s, now=now)), s.pause_seconds,
                ])
        self._toast(f"Exportado: {path}")

    # ---------- Views roteadas ----------
    def _my_password_view(self) -> ft.View:
        # reset campos
        self._pwd_old.value = ""
        self._pwd_new1.value = ""
        self._pwd_new2.value = ""

        def _save(_):
            me = self.db.verify_login(self.user.username, self._pwd_old.value or "")
            if not me:
                self._toast("Senha atual incorreta.")
                return
            if not self._pwd_new1.value or self._pwd_new1.value != self._pwd_new2.value:
                self._toast("Nova senha vazia ou não confere.")
                return
            try:
                self.db.set_user_password(self.user.id, self._pwd_new1.value)
            except Exception as ex:
                self._toast(f"Erro: {ex}")
                return
            self._toast("Senha alterada com sucesso.")
            self.page.go("/")

        content = ft.Container(
            padding=16,
            content=ft.Column([
                ft.Text("Alterar minha senha", size=20, weight=ft.FontWeight.BOLD),
                ft.Row([self._pwd_old, self._pwd_new1, self._pwd_new2], wrap=True, spacing=10),
                ft.Row([
                    ft.FilledButton("Salvar", icon=I.SAVE, on_click=_save),
                    ft.OutlinedButton("Cancelar", icon=I.ARROW_BACK, on_click=lambda _: self.page.go("/")),
                ], alignment=ft.MainAxisAlignment.END),
            ], spacing=12)
        )

        return ft.View(
            "/minha-senha",
            controls=[content],
            appbar=ft.AppBar(
                title=ft.Text("Gerenciador — Minha Senha"),
                actions=[
                    ft.IconButton(I.HOME, tooltip="Home", on_click=lambda e: self.page.go("/")),
                ],
            ),
            bgcolor=op(0.01, C.BLACK),
        )

    def _users_admin_view(self) -> ft.View:
        title = ft.Text("Administração de Usuários", size=20, weight=ft.FontWeight.BOLD)

        table = ft.DataTable(
            columns=[
                ft.DataColumn(ft.Text("ID")),
                ft.DataColumn(ft.Text("Usuário")),
                ft.DataColumn(ft.Text("Admin")),
                ft.DataColumn(ft.Text("Ativo")),
                ft.DataColumn(ft.Text("Criado em")),
                ft.DataColumn(ft.Text("Último login")),
                ft.DataColumn(ft.Text("Ações")),
            ],
            rows=[],
            heading_row_color=op(0.03, C.BLACK),
            data_row_min_height=44,
            column_spacing=16,
        )

        reset_panel = ft.Container(visible=False)
        reset_user_id = {"value": None}
        reset_user_name = {"value": None}

        new_pass1 = ft.TextField(label="Nova senha", password=True, can_reveal_password=True, width=220)
        new_pass2 = ft.TextField(label="Confirmar", password=True, can_reveal_password=True, width=220)

        def close_reset_panel(_=None):
            reset_user_id["value"] = None
            reset_user_name["value"] = None
            new_pass1.value = ""
            new_pass2.value = ""
            reset_panel.visible = False
            self.page.update()

        def confirm_reset(_):
            if not reset_user_id["value"]:
                self._toast("Nenhum usuário selecionado.")
                return
            if not new_pass1.value or not new_pass2.value:
                self._toast("Informe e confirme a nova senha.")
                return
            if new_pass1.value != new_pass2.value:
                self._toast("As senhas não conferem.")
                return
            try:
                self.db.set_user_password(reset_user_id["value"], new_pass1.value)
                self._toast(f"Senha redefinida para '{reset_user_name['value']}'.")
                close_reset_panel()
            except Exception as ex:
                self._toast(f"Erro ao redefinir senha: {ex}")

        reset_panel.content = ft.Card(
            elevation=2,
            content=ft.Container(
                padding=12,
                content=ft.Column(
                    controls=[
                        ft.Text("Redefinir senha", size=16, weight=ft.FontWeight.BOLD),
                        ft.Text(lambda: f"Usuário alvo: {reset_user_name['value'] or '-'}"),
                        ft.Row([new_pass1, new_pass2], wrap=True, spacing=10),
                        ft.Row(
                            [
                                ft.FilledButton("Confirmar", icon=I.KEY, on_click=confirm_reset),
                                ft.OutlinedButton("Cancelar", icon=I.CLOSE, on_click=close_reset_panel),
                            ],
                            alignment=ft.MainAxisAlignment.END,
                        ),
                    ],
                    spacing=10,
                ),
            ),
        )

        def make_actions(u: User):
            uid = u.id
            uname = u.username
            is_admin = bool(u.is_admin)
            is_active = bool(u.active)

            def toggle_admin(_):
                try:
                    _, msg = self.db.set_user_admin(uid, not is_admin)
                    self._toast(msg)
                    refresh_table()
                except Exception as ex:
                    self._toast(f"Erro ao alternar admin: {ex}")

            def toggle_active(_):
                try:
                    _, msg = self.db.set_user_active(uid, not is_active)
                    self._toast(msg)
                    refresh_table()
                except Exception as ex:
                    self._toast(f"Erro ao alternar ativo: {ex}")

            def ask_reset(_):
                reset_user_id["value"] = uid
                reset_user_name["value"] = uname
                reset_panel.visible = True
                self.page.update()

            def delete_user(_):
                try:
                    _, msg = self.db.delete_user(uid)
                    self._toast(msg)
                    refresh_table()
                except Exception as ex:
                    self._toast(f"Erro ao excluir: {ex}")

            return ft.Row(
                controls=[
                    ft.IconButton(I.VERIFIED_USER if is_admin else I.SHIELD, tooltip="Alternar admin", on_click=toggle_admin),
                    ft.IconButton(I.TOGGLE_ON if is_active else I.TOGGLE_OFF, tooltip="Ativar/Desativar", on_click=toggle_active),
                    ft.IconButton(I.KEY, tooltip="Redefinir senha", on_click=ask_reset),
                    ft.IconButton(I.DELETE, tooltip="Excluir usuário", on_click=delete_user),
                ],
                spacing=4,
            )

        def refresh_table():
            table.rows.clear()
            try:
                users = self.db.list_users()
            except Exception as ex:
                self._toast(f"Erro ao listar usuários: {ex}")
                users = []

            for u in users:
                is_admin = "Sim" if bool(u.is_admin) else "Não"
                is_active = "Sim" if bool(u.active) else "Não"
                created = u.created_at.astimezone(TZ).strftime("%d/%m/%Y %H:%M:%S")
                last_login = u.last_login_at.astimezone(TZ).strftime("%d/%m/%Y %H:%M:%S") if u.last_login_at else "-"

                table.rows.append(
                    ft.DataRow(
                        cells=[
                            ft.DataCell(ft.Text(str(u.id))),
                            ft.DataCell(ft.Text(u.username)),
                            ft.DataCell(ft.Text(is_admin)),
                            ft.DataCell(ft.Text(is_active)),
                            ft.DataCell(ft.Text(created)),
                            ft.DataCell(ft.Text(last_login)),
                            ft.DataCell(make_actions(u)),
                        ]
                    )
                )
            self.page.update()

        refresh_table()

        nu_user = ft.TextField(label="Usuário", width=220)
        nu_p1 = ft.TextField(label="Senha", password=True, can_reveal_password=True, width=220)
        nu_p2 = ft.TextField(label="Confirmar senha", password=True, can_reveal_password=True, width=220)
        nu_is_admin = ft.Checkbox(label="Administrador?", value=False)

        def create_user(_):
            if not nu_user.value or not nu_p1.value or not nu_p2.value:
                self._toast("Informe usuário e senha.")
                return
            if nu_p1.value != nu_p2.value:
                self._toast("Senhas não conferem.")
                return
            try:
                self.db.create_user(nu_user.value, nu_p1.value, is_admin=nu_is_admin.value)
                self._toast(f"Usuário '{nu_user.value}' criado.")
                nu_user.value, nu_p1.value, nu_p2.value = "", "", ""
                nu_is_admin.value = False
                refresh_table()
            except Exception as ex:
                self._toast(f"Erro ao criar usuário: {ex}")

        create_btn = ft.FilledButton("Criar usuário", icon=I.PERSON_ADD, on_click=create_user)
        back_btn = ft.OutlinedButton("Voltar", icon=I.ARROW_BACK, on_click=lambda _: self.page.go("/"))

        content = ft.Container(
            padding=16,
            content=ft.Column(
                controls=[
                    title,
                    ft.Card(content=ft.Container(padding=12, content=table)),
                    ft.Divider(),
                    ft.Text("Redefinir senha (inline, sem diálogo)", size=14, weight=ft.FontWeight.BOLD),
                    reset_panel,
                    ft.Divider(),
                    ft.Text("Novo usuário", size=14, weight=ft.FontWeight.BOLD),
                    ft.Row([nu_user, nu_p1, nu_p2, nu_is_admin, create_btn], wrap=True, spacing=10),
                    ft.Divider(),
                    ft.Row([back_btn, ft.OutlinedButton("Atualizar", icon=I.REFRESH, on_click=lambda _: refresh_table())],
                           alignment=ft.MainAxisAlignment.END),
                ],
                spacing=12,
                scroll=ft.ScrollMode.AUTO,
            ),
        )

        return ft.View(
            "/admin/usuarios",
            controls=[content],
            appbar=ft.AppBar(
                title=ft.Text("Gerenciador — Administração de Usuários"),
                actions=[
                    ft.IconButton(I.HOME, tooltip="Home", on_click=lambda e: self.page.go("/")),
                ],
            ),
            bgcolor=op(0.01, C.BLACK),
        )

    def _toast(self, msg: str):
        self.page.snack_bar = ft.SnackBar(ft.Text(msg))
        self.page.snack_bar.open = True
        self.page.update()

# ---------------------------------------------------------------------------
# Tempo de sessão / util
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
# Main
# ---------------------------------------------------------------------------
def main(page: ft.Page):
    db = DB(DB_PATH)
    def show_login():
        LoginView(page, db, on_success=after_login)
    def after_login(user: User):
        TimeManagerApp(page, db, user, on_logout=show_login)
    show_login()

if __name__ == "__main__":
    ft.app(target=main)