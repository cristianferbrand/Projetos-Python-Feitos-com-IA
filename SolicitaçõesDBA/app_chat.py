from __future__ import annotations

import os
import sys
import base64
import sqlite3
import mimetypes
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, List, Tuple

import flet as ft

# ---------------------------------------------------------------------
# Compat core
# ---------------------------------------------------------------------
try:
    C = ft.Colors  # Flet novo
except Exception:
    C = ft.colors  # Flet legado

try:
    I = ft.Icons
except Exception:
    I = ft.icons

def op(alpha: float, color: str):
    try:
        return getattr(ft, "colors").with_opacity(color, alpha)
    except Exception:
        try:
            return color.with_opacity(alpha)
        except Exception:
            return color

TZ = datetime.now().astimezone().tzinfo or timezone.utc
DB_PATH = "dba_demo.db"


# ---------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------
@dataclass
class User:
    id: int
    username: str

@dataclass
class Request:
    id: int
    created_by: int
    taken_by: Optional[int]
    created_at: datetime

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
    message_id: Optional[int]  # V2


# ---------------------------------------------------------------------
# DB Layer
# ---------------------------------------------------------------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  username TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS dba_requests (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_by INTEGER NOT NULL,
  taken_by INTEGER,
  created_at TEXT NOT NULL,
  FOREIGN KEY (created_by) REFERENCES users(id),
  FOREIGN KEY (taken_by) REFERENCES users(id)
);
CREATE TABLE IF NOT EXISTS dba_request_messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  request_id INTEGER NOT NULL,
  sender_id INTEGER NOT NULL,
  message TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (request_id) REFERENCES dba_requests(id) ON DELETE CASCADE,
  FOREIGN KEY (sender_id) REFERENCES users(id)
);
CREATE TABLE IF NOT EXISTS dba_request_files (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  request_id INTEGER NOT NULL,
  filename TEXT NOT NULL,
  mime TEXT NOT NULL,
  size_bytes INTEGER NOT NULL,
  blob BLOB NOT NULL,
  uploaded_by INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  message_id INTEGER,
  FOREIGN KEY (request_id) REFERENCES dba_requests(id) ON DELETE CASCADE,
  FOREIGN KEY (uploaded_by) REFERENCES users(id) ON DELETE SET NULL,
  FOREIGN KEY (message_id) REFERENCES dba_request_messages(id) ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_file_msg ON dba_request_files(message_id);
"""

class DB:
    def __init__(self, path: str = DB_PATH):
        self.path = path
        self._ensure()

    def connect(self):
        return sqlite3.connect(self.path)

    def _ensure(self):
        with self.connect() as con:
            con.executescript(SCHEMA)
            cur = con.cursor()
            # Migração defensiva (caso o DB já exista sem message_id)
            cur.execute("PRAGMA table_info(dba_request_files)")
            cols = [r[1] for r in cur.fetchall()]
            if "message_id" not in cols:
                cur.execute("ALTER TABLE dba_request_files ADD COLUMN message_id INTEGER")
                con.commit()
            cur.execute("CREATE INDEX IF NOT EXISTS idx_file_msg ON dba_request_files(message_id)")
            con.commit()

    # Users
    def ensure_user(self, username: str) -> int:
        with self.connect() as con:
            cur = con.cursor()
            cur.execute("SELECT id FROM users WHERE username=?", (username,))
            r = cur.fetchone()
            if r:
                return int(r[0])
            cur.execute("INSERT INTO users(username) VALUES(?)", (username,))
            con.commit()
            return int(cur.lastrowid)

    # Requests
    def ensure_request_demo(self, created_by: int) -> int:
        with self.connect() as con:
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            cur.execute("SELECT id FROM dba_requests ORDER BY id ASC LIMIT 1")
            r = cur.fetchone()
            if r:
                return int(r["id"])
            now = datetime.now(TZ).isoformat()
            cur.execute("INSERT INTO dba_requests(created_by, taken_by, created_at) VALUES(?,?,?)",
                        (created_by, None, now))
            con.commit()
            return int(cur.lastrowid)

    def get_request(self, rid: int) -> Optional[Request]:
        with self.connect() as con:
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            cur.execute("SELECT * FROM dba_requests WHERE id=?", (rid,))
            r = cur.fetchone()
            if not r: return None
            return Request(id=r["id"], created_by=r["created_by"], taken_by=r["taken_by"],
                           created_at=datetime.fromisoformat(r["created_at"]))

    # Messages
    def add_message(self, rid: int, sender_id: int, text: str) -> int:
        now = datetime.now(TZ).isoformat()
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(
                "INSERT INTO dba_request_messages(request_id, sender_id, message, created_at) VALUES(?,?,?,?)",
                (rid, sender_id, text, now)
            )
            con.commit()
            return int(cur.lastrowid)

    def list_messages(self, rid: int) -> List[Message]:
        with self.connect() as con:
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            cur.execute(
                "SELECT * FROM dba_request_messages WHERE request_id=? ORDER BY datetime(created_at) ASC, id ASC",
                (rid,)
            )
            rows = cur.fetchall()
            return [
                Message(id=r["id"], request_id=r["request_id"], sender_id=r["sender_id"],
                        message=r["message"], created_at=datetime.fromisoformat(r["created_at"]))
                for r in rows
            ]

    # Files
    def add_file(self, request_id: int, filename: str, mime: str, data: bytes,
                 uploaded_by: int, message_id: Optional[int] = None) -> int:
        now = datetime.now(TZ).isoformat()
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(
                """INSERT INTO dba_request_files
                   (request_id, filename, mime, size_bytes, blob, uploaded_by, created_at, message_id)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (request_id, filename, mime, len(data), sqlite3.Binary(data), uploaded_by, now, message_id)
            )
            con.commit()
            return int(cur.lastrowid)

    def get_file(self, fid: int) -> Optional[Attachment]:
        with self.connect() as con:
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            cur.execute("SELECT * FROM dba_request_files WHERE id=?", (fid,))
            r = cur.fetchone()
            if not r: return None
            return Attachment(
                id=r["id"], request_id=r["request_id"], filename=r["filename"], mime=r["mime"],
                size_bytes=r["size_bytes"], blob=bytes(r["blob"]) if r["blob"] is not None else b"",
                uploaded_by=r["uploaded_by"], created_at=datetime.fromisoformat(r["created_at"]),
                message_id=r["message_id"]
            )

    def list_files_by_message(self, message_id: int) -> List[Attachment]:
        with self.connect() as con:
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            cur.execute(
                "SELECT * FROM dba_request_files WHERE message_id=? ORDER BY datetime(created_at) ASC, id ASC",
                (message_id,)
            )
            rows = cur.fetchall()
            return [
                Attachment(
                    id=r["id"], request_id=r["request_id"], filename=r["filename"], mime=r["mime"],
                    size_bytes=r["size_bytes"], blob=bytes(r["blob"]) if r["blob"] is not None else b"",
                    uploaded_by=r["uploaded_by"], created_at=datetime.fromisoformat(r["created_at"]),
                    message_id=r["message_id"]
                )
                for r in rows
            ]


# ---------------------------------------------------------------------
# App (sem UserControl)
# ---------------------------------------------------------------------
class ChatDemoApp:
    def __init__(self, page: ft.Page):
        self.page = page
        self.db = DB(DB_PATH)
        self.user_id = self.db.ensure_user("Cristianfer")
        self.rid = self.db.ensure_request_demo(self.user_id)
        self.chat_files: List[Tuple[str, str, bytes]] = []

        # UI refs
        self.fp: ft.FilePicker | None = None
        self.msgs_view: ft.ListView | None = None
        self.inp: ft.TextField | None = None
        self.pending: ft.Container | None = None

    def toast(self, msg: str):
        self.page.snack_bar = ft.SnackBar(ft.Text(msg))
        self.page.snack_bar.open = True
        self.page.update()

    def open_image_preview(self, fname: str, data: bytes):
        b64 = base64.b64encode(data).decode("ascii")
        img = ft.Image(src_base64=b64, width=800, height=600, fit=ft.ImageFit.CONTAIN)
        dlg = ft.AlertDialog(title=ft.Text(fname),
                             content=ft.Container(img, width=820, height=620),
                             actions=[ft.TextButton("Fechar", on_click=lambda e: self.page.close(dlg))])
        self.page.dialog = dlg
        dlg.open = True
        self.page.update()

    def save_and_open_file(self, fname: str, data: bytes):
        os.makedirs("downloads", exist_ok=True)
        path = os.path.abspath(os.path.join("downloads", fname))
        with open(path, "wb") as f:
            f.write(data)
        try:
            if sys.platform.startswith("win"):
                os.startfile(path)  # type: ignore
            elif sys.platform == "darwin":
                os.system(f"open '{path}'")
            else:
                os.system(f"xdg-open '{path}'")
        except Exception:
            pass
        self.toast(f"Salvo em: {path}")

    def on_pick(self, e: ft.FilePickerResultEvent):
        if not e.files: return
        total = sum(len(d) for _,_,d in self.chat_files)
        for f in e.files:
            try:
                data = open(f.path, "rb").read()
            except Exception:
                self.toast(f"Não foi possível ler: {f.name}")
                continue
            total += len(data)
            if total > 20 * 1024 * 1024:
                self.toast("Limite total de 20MB por envio excedido.")
                break
            mime = mimetypes.guess_type(f.name)[0] or "application/octet-stream"
            self.chat_files.append((f.name, mime, data))
        self.refresh_pending()

    def refresh_pending(self):
        if not self.pending: return
        if not self.chat_files:
            self.pending.content = None
            self.page.update()
            return

        tiles: List[ft.Control] = []
        for name, mime, data in self.chat_files:
            if str(mime).startswith("image/"):
                b64 = base64.b64encode(data).decode("ascii")
                img = ft.Image(src_base64=b64, width=120, height=90, fit=ft.ImageFit.COVER, border_radius=8)
            else:
                img = ft.Icon(I.INSERT_DRIVE_FILE, size=40)
            cap = ft.Text(name, size=11, max_lines=1, overflow=ft.TextOverflow.ELLIPSIS)

            def remove_one(_, fname=name):
                for i, (n, mm, dd) in enumerate(list(self.chat_files)):
                    if n == fname:
                        self.chat_files.pop(i)
                        break
                self.refresh_pending()

            tiles.append(
                ft.Container(
                    width=150, padding=6,
                    content=ft.Column([img, cap, ft.TextButton("Remover", icon=I.DELETE, on_click=remove_one)],
                                      spacing=4, horizontal_alignment=ft.CrossAxisAlignment.CENTER),
                )
            )

        self.pending.content = ft.Container(
            bgcolor=op(0.06, C.ON_SURFACE),
            border_radius=12, padding=10,
            content=ft.Column([ft.Text("Anexos (pré-envio)", size=16, weight=ft.FontWeight.BOLD),
                               ft.Row(tiles, wrap=True, spacing=8)], spacing=8),
        )
        self.page.update()

    def send_msg(self, _=None):
        if not self.inp or not self.msgs_view: return
        txt = (self.inp.value or "").strip()
        has_text = bool(txt)
        has_files = len(self.chat_files) > 0
        if not has_text and not has_files:
            return

        # cria mensagem (mesmo vazia) para ancorar anexos
        msg_id = self.db.add_message(self.rid, self.user_id, txt if has_text else "")

        # grava anexos com message_id
        if has_files:
            for name, mime, data in self.chat_files:
                self.db.add_file(self.rid, name, mime, data, self.user_id, message_id=msg_id)
            self.chat_files.clear()
            self.refresh_pending()

        self.inp.value = ""
        self.inp.update()
        self.load_msgs()

    def load_msgs(self):
        if not self.msgs_view: return
        self.msgs_view.controls.clear()
        msgs = self.db.list_messages(self.rid)
        max_w = int((self.page.window_width or 1000) * 0.75)

        for m in msgs:
            is_me = (m.sender_id == self.user_id)
            align = ft.MainAxisAlignment.END if is_me else ft.MainAxisAlignment.START
            bubble_bg = C.BLUE_600 if is_me else op(0.06, C.ON_SURFACE)
            txt_color = C.WHITE if is_me else C.WHITE
            when = m.created_at.astimezone(TZ).strftime("%d/%m/%Y %H:%M")

            if m.message:
                bubble = ft.Container(
                    bgcolor=bubble_bg, padding=10,
                    border_radius=ft.border_radius.BorderRadius(16, 16, 4 if is_me else 16, 16 if is_me else 4),
                    width=max_w,
                    content=ft.Column([ft.Text(m.message, color=txt_color, selectable=True),
                                       ft.Text(when, size=11, color=op(0.75, txt_color))], spacing=8),
                )
                self.msgs_view.controls.append(ft.Row([bubble], alignment=align))

            files = self.db.list_files_by_message(m.id)
            if files:
                tiles: List[ft.Control] = []
                for f in files:
                    if str(f.mime).startswith("image/") and f.blob:
                        b64 = base64.b64encode(f.blob).decode("ascii")
                        thumb = ft.Image(src_base64=b64, width=140, height=100, fit=ft.ImageFit.COVER, border_radius=8)
                    else:
                        thumb = ft.Icon(I.INSERT_DRIVE_FILE, size=40)

                    def do_open(_, fid=f.id, fname=f.filename, mime=f.mime):
                        att = self.db.get_file(fid)
                        if not att:
                            self.toast("Arquivo não encontrado")
                            return
                        if str(mime).startswith("image/"):
                            self.open_image_preview(fname, att.blob)
                        else:
                            self.save_and_open_file(fname, att.blob)

                    def do_save(_, fid=f.id, fname=f.filename):
                        att = self.db.get_file(fid)
                        if not att:
                            self.toast("Arquivo não encontrado")
                            return
                        self.save_and_open_file(fname, att.blob)

                    tiles.append(
                        ft.Container(
                            width=180, padding=6,
                            content=ft.Column(
                                [thumb,
                                 ft.Text(f.filename, size=11, max_lines=2, overflow=ft.TextOverflow.ELLIPSIS),
                                 ft.Row([ft.TextButton("Abrir", icon=I.OPEN_IN_NEW, on_click=do_open),
                                         ft.TextButton("Baixar", icon=I.DOWNLOAD, on_click=do_save)],
                                        spacing=6, alignment=ft.MainAxisAlignment.CENTER)],
                                spacing=6, horizontal_alignment=ft.CrossAxisAlignment.CENTER),
                        )
                    )

                wrap = ft.Container(
                    bgcolor=bubble_bg, padding=10,
                    border_radius=ft.border_radius.BorderRadius(16, 16, 4 if is_me else 16, 16 if is_me else 4),
                    width=max_w,
                    content=ft.Column([ft.Row(tiles, wrap=True, spacing=8),
                                       ft.Text(when, size=11, color=op(0.75, txt_color))], spacing=8),
                )
                self.msgs_view.controls.append(ft.Row([wrap], alignment=align))

        self.page.update()

    def mount(self):
        # Page setup
        self.page.title = "Chat — Miniaturas no histórico"
        self.page.theme_mode = ft.ThemeMode.DARK
        self.page.window_width = 1024
        self.page.window_height = 720
        self.page.update()

        # Controls
        self.fp = ft.FilePicker(on_result=self.on_pick)
        self.page.overlay.append(self.fp)

        self.msgs_view = ft.ListView(expand=True, spacing=8, auto_scroll=True)
        self.inp = ft.TextField(label="Mensagem", hint_text="Escreva sua mensagem e tecle Enter",
                                expand=True, multiline=False, on_submit=self.send_msg)
        self.pending = ft.Container()

        btn_anexar = ft.TextButton("Anexar", icon=I.ATTACH_FILE,
                                   on_click=lambda e: self.fp.pick_files(allow_multiple=True) if self.fp else None)
        btn_enviar = ft.FilledTonalButton("Enviar", icon=I.SEND, on_click=self.send_msg)

        header = ft.Row([ft.Icon(I.CHAT),
                         ft.Text(f"Solicitação #{self.rid}", size=20, weight=ft.FontWeight.BOLD)], spacing=8)

        root = ft.Column(
            [header, ft.Divider(),
             ft.Container(self.msgs_view, height=520),
             self.pending, ft.Divider(),
             ft.Row([btn_anexar, self.inp, btn_enviar], vertical_alignment=ft.CrossAxisAlignment.CENTER)],
            spacing=8, expand=True)

        self.page.add(root)
        self.load_msgs()


def main(page: ft.Page):
    app = ChatDemoApp(page)
    app.mount()

if __name__ == "__main__":
    ft.app(target=main)
