# -*- coding: utf-8 -*-
r"""
Resgate XML – v1

Atualizações desta versão:
- Home SEM seletor do .FDB (fica apenas em Configurações via engrenagem).
- Datas com DatePicker e exibição em formato BR (DD/MM/AAAA).
- Navegação roteada com View stack (Home "/" e Settings "/settings").
- Removido uso de ft.Expanded (compat com versões antigas do Flet).
- Evitada cor ON_PRIMARY/PRIMARY no AppBar para compatibilidade de tema.
- Docstring raw para não disparar SyntaxWarning com "\\sql".
- SQL fixa no código-fonte (nada de pasta \\sql externa).

Requisitos:
- Python 3.8+
- Flet
- SQLAlchemy
- python-firebird-driver (ou fdb)
"""

from __future__ import annotations

import datetime as dt
import json
import re
import threading
from pathlib import Path
from typing import Optional, Callable, Any

import flet as ft
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

APP_NAME = "Resgate XML – v1"
BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = BASE_DIR / "config"
CONFIG_DIR.mkdir(parents=True, exist_ok=True)
SETTINGS_FILE = CONFIG_DIR / "settings.json"

DEFAULT_SETTINGS = {
    "fb_host": "localhost",
    "fb_port": 3050,
    "fb_user": "sysdba",
    "fb_password": "masterkey",
    "fb_charset": "UTF8",
    "fb_driver": "firebird",  # 'firebird' (python-firebird-driver) ou 'fdb'
    "fb_database": str(BASE_DIR / "NOTAS_FISCAIS.FDB"),
    "xml_folder": str(BASE_DIR / "xml"),
}

# --- Compat: cores e ícones (preferência do usuário) ---
try:
    C = ft.Colors  # preferido
except AttributeError:  # compat
    C = ft.colors

try:
    I = ft.Icons  # preferido
except AttributeError:  # compat
    I = ft.icons


def op(alpha: float, color: Any):
    """Helper para ajustar opacidade quando disponível; caso contrário, retorna a cor original."""
    try:
        return color.with_opacity(alpha)
    except Exception:
        return color


# -------------------- Persistência de configurações --------------------

def load_settings() -> dict:
    if SETTINGS_FILE.exists():
        try:
            data = json.loads(SETTINGS_FILE.read_text(encoding="utf-8"))
            for k, v in DEFAULT_SETTINGS.items():
                data.setdefault(k, v)
            return data
        except Exception:
            pass
    return DEFAULT_SETTINGS.copy()


def save_settings(data: dict):
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    SETTINGS_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p


# -------------------- Adaptador SQLAlchemy / Firebird --------------------

class SAAdapter:
    def __init__(self):
        self.engine: Optional[Engine] = None
        self.driver: str = "firebird"

    def build_uri(
        self,
        host: str,
        port: int,
        database_path: str,
        user: str,
        password: str,
        charset: str = "UTF8",
    ) -> str:
        db_path = database_path.replace("\\\\", "/").replace("\\", "/")
        if host and str(host).strip():
            auth = f"{user}:{password}@{host}:{int(port)}"
            path_part = f"/{db_path}"
        else:
            auth = ""
            path_part = f"/{db_path}"
        uri = f"firebird+{self.driver}://{auth}{path_part}?charset={charset}"
        return uri

    def connect(
        self,
        host: str,
        port: int,
        database_path: str,
        user: str,
        password: str,
        charset: str = "UTF8",
        driver: str = "firebird",
    ):
        from pathlib import Path as _P

        self.driver = driver
        _host = (host or "").strip().lower()
        # Se local, verifique existência do arquivo .FDB
        if not _host or _host in {"localhost", "127.0.0.1"}:
            if not _P(database_path).exists():
                raise FileNotFoundError(f"Arquivo .FDB não encontrado: {database_path}")
        uri = self.build_uri(host, port, database_path, user, password, charset)
        self.engine = create_engine(uri, pool_pre_ping=True, future=True)
        # valida rapidamente
        with self.engine.connect() as conn:
            conn.execute(text("SELECT 1 FROM RDB$DATABASE"))
        return self.engine

    def close(self):
        if self.engine:
            self.engine.dispose(close=True)
            self.engine = None


# -------------------- App principal com Views (Home/Settings) --------------------

class AuditoriaNFApp:
    def __init__(self, page: ft.Page):
        self.page = page
        self.page.title = APP_NAME
        self.page.theme_mode = ft.ThemeMode.LIGHT
        self.page.padding = 0
        self.page.horizontal_alignment = ft.CrossAxisAlignment.STRETCH
        self.page.vertical_alignment = ft.MainAxisAlignment.START
        self.page.window_width = 1000
        self.page.window_height = 700
        self.page.window_min_width = 920
        self.page.window_min_height = 600

        self.settings = load_settings()
        ensure_dir(Path(self.settings.get("xml_folder", DEFAULT_SETTINGS["xml_folder"])))

        # Estado
        self.adapter = SAAdapter()
        self.status_text = ft.Text("Pronto.", selectable=False)

        today = dt.date.today()
        first_of_month = today.replace(day=1).isoformat()

        # --------- DatePickers + TextFields (formato BR) ---------
        self.dp_ini = ft.DatePicker(on_change=self._on_dp_ini_change, first_date=dt.date(2000, 1, 1), last_date=dt.date(2100, 12, 31))
        self.dp_fim = ft.DatePicker(on_change=self._on_dp_fim_change, first_date=dt.date(2000, 1, 1), last_date=dt.date(2100, 12, 31))
        self.dp_ini.value = dt.date.fromisoformat(first_of_month)
        self.dp_fim.value = today
        self.tf_ini = ft.TextField(
            label="Data inicial",
            value=dt.datetime.strptime(first_of_month, "%Y-%m-%d").strftime("%d/%m/%Y"),
            width=170,
            dense=True,
            read_only=True,
            suffix=ft.IconButton(I.CALENDAR_MONTH, tooltip="Escolher data", on_click=lambda e: self._open_dp(self.dp_ini)),
        )
        self.tf_fim = ft.TextField(
            label="Data final",
            value=today.strftime("%d/%m/%Y"),
            width=170,
            dense=True,
            read_only=True,
            suffix=ft.IconButton(I.CALENDAR_MONTH, tooltip="Escolher data", on_click=lambda e: self._open_dp(self.dp_fim)),
        )

        # --------- Tipo de nota (Home) ---------
        self.dd_tipo = ft.Dropdown(
            label="Tipo de nota",
            options=[ft.dropdown.Option("Entrada"), ft.dropdown.Option("Saída"), ft.dropdown.Option("Ambas")],
            value="Saída",
            width=160,
            dense=True,
        )

        # --------- Seletor PASTA XML (Home) ---------
        self.tf_xml_dir = ft.TextField(
            label="Pasta dos XML (saída)",
            value=self.settings.get("xml_folder", DEFAULT_SETTINGS["xml_folder"]),
            read_only=True,
            expand=True,
            dense=True,
        )
        self.btn_xml_pick = ft.FilledButton(
            "Selecionar pasta dos XML...", icon=I.FOLDER_OPEN, on_click=self._on_pick_xml_folder
        )

        self.btn_test = ft.ElevatedButton("Testar conexão", icon=I.CHECK_CIRCLE, on_click=self._on_test_conn)
        self.btn_extract = ft.ElevatedButton("Extrair XML", icon=I.DOWNLOAD, on_click=self._on_extract_xml)

        # Log (Home)
        self.lv_log = ft.ListView(height=320, auto_scroll=True, spacing=4, padding=8)
        self.log_card = ft.Card(
            content=ft.Container(
                content=ft.Column(
                    [ft.Text("Console de Log", weight=ft.FontWeight.BOLD), self.lv_log],
                    expand=True,
                    spacing=8,
                ),
                padding=12,
            )
        )

        # File pickers (page.overlay)
        self.dir_picker_db = ft.FilePicker(on_result=self._on_dir_result_db)
        self.dir_picker_xml = ft.FilePicker(on_result=self._on_dir_result_xml)
        self.file_picker_db = ft.FilePicker(on_result=self._on_file_result_db)
        self.page.overlay.extend([self.dir_picker_db, self.dir_picker_xml, self.file_picker_db, self.dp_ini, self.dp_fim])

        # Navegação roteada (View stack)
        self.page.on_route_change = self._route_change
        self.page.on_view_pop = self._view_pop
        # Inicializa a rota atual (normalmente "/")
        self.page.go(self.page.route)

    # ---------- Datas (helpers BR) ----------
    def _fmt_br(self, d: dt.date) -> str:
        try:
            if isinstance(d, dt.datetime):
                d = d.date()
            return d.strftime("%d/%m/%Y")
        except Exception:
            return ""

    def _parse_br(self, s: str) -> dt.date:
        return dt.datetime.strptime((s or "").strip(), "%d/%m/%Y").date()
    def _open_dp(self, dp):
        """Abre o DatePicker de forma compatível com versões antigas do Flet."""
        try:
            dp.pick_date()  # Flet mais novo
        except Exception:
            try:
                dp.open = True  # Flet mais antigo
                self.page.update()
            except Exception:
                pass

    def _on_dp_ini_change(self, e: ft.DatePickerChangeEvent):
        d = self.dp_ini.value
        if isinstance(d, dt.datetime):
            d = d.date()
        if isinstance(d, dt.date):
            self.tf_ini.value = self._fmt_br(d)
            try:
                self.page.update()
            except Exception:
                pass

    def _on_dp_fim_change(self, e: ft.DatePickerChangeEvent):
        d = self.dp_fim.value
        if isinstance(d, dt.datetime):
            d = d.date()
        if isinstance(d, dt.date):
            self.tf_fim.value = self._fmt_br(d)
            try:
                self.page.update()
            except Exception:
                pass

    # ---------- Helpers de UI / Logs ----------
    def _append_log(self, msg: str):
        ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # Limitador de itens para evitar crescimento infinito
        if len(self.lv_log.controls) > 2000:
            self.lv_log.controls = self.lv_log.controls[-1500:]
        self.lv_log.controls.append(ft.Text(f"{ts}  {msg}"))
        # Atualização segura (view atual)
        try:
            self.page.update()
        except Exception:
            pass

    def _safe_ui(self, fn: Callable[[], None]):
        """Chama fn no thread da UI quando possível; fallback chama direto."""
        try:
            self.page.call_from_thread(fn)
        except Exception:
            try:
                self.page.invoke_later(fn)
            except Exception:
                fn()

    # ---------- File pickers ----------
    def _on_pick_xml_folder(self, e: ft.ControlEvent):
        self.dir_picker_xml.get_directory_path()

    def _on_dir_result_db(self, e: ft.FilePickerResultEvent):
        if not e.path:
            return
        folder = Path(e.path)
        candidate = folder / "NOTAS_FISCAIS.FDB"
        chosen = None

        if candidate.exists():
            chosen = candidate
        else:
            # Aceita *.FDB (ou *.fdb) se houver exatamente um arquivo na pasta
            fdbs = list(folder.glob("*.FDB")) + list(folder.glob("*.fdb"))
            if len(fdbs) == 1:
                chosen = fdbs[0]
            elif len(fdbs) > 1:
                self.page.snack_bar = ft.SnackBar(
                    ft.Text(
                        f"Encontrei {len(fdbs)} arquivos .FDB em {folder}. Use o botão de ARQUIVO para escolher o correto."
                    ),
                    open=True,
                )
                self.page.update()
                return

        if chosen:
            self._apply_fdb_path(chosen)
        else:
            self.page.snack_bar = ft.SnackBar(ft.Text(f"Não encontrei um arquivo .FDB em: {folder}"), open=True)
            self.page.update()

    def _on_dir_result_xml(self, e: ft.FilePickerResultEvent):
        if not e.path:
            return
        folder = Path(e.path)
        ensure_dir(folder)
        s = load_settings()
        s["xml_folder"] = str(folder)
        save_settings(s)
        self.settings = s
        self.tf_xml_dir.value = str(folder)
        self._append_log(f"Pasta dos XML definida: {folder}")
        self.page.update()

    def _on_file_result_db(self, e: ft.FilePickerResultEvent):
        if not e.files:
            return
        f = e.files[0]
        p = Path(f.path or f.name)
        if p.suffix.lower() != ".fdb":
            self.page.snack_bar = ft.SnackBar(ft.Text("Selecione um arquivo .FDB válido."), open=True)
            self.page.update()
            return
        self._apply_fdb_path(p)

    # ---------- Conexão ----------
    def _connect_if_needed(self) -> bool:
        if self.adapter.engine:
            return True
        s = load_settings()
        try:
            self.adapter.connect(
                host=s["fb_host"],
                port=int(s["fb_port"]),
                database_path=s["fb_database"],
                user=s["fb_user"],
                password=s["fb_password"],
                charset=s["fb_charset"],
                driver=s["fb_driver"],
            )
            self.status_text.value = "Conectado."
            self._append_log("Conectado ao Firebird.")
            return True
        except Exception as e:
            self.status_text.value = "Erro de conexão."
            self._append_log(f"ERRO ao conectar: {e}")
            self.page.snack_bar = ft.SnackBar(ft.Text(f"Falha ao conectar: {e}"), open=True)
            self.page.update()
            return False

    def _on_test_conn(self, e: ft.ControlEvent):
        s = load_settings()
        try:
            engine = self.adapter.connect(
                host=s["fb_host"],
                port=int(s["fb_port"]),
                database_path=s["fb_database"],
                user=s["fb_user"],
                password=s["fb_password"],
                charset=s["fb_charset"],
                driver=s["fb_driver"],
            )
            if engine:
                self.adapter.close()
            self.page.snack_bar = ft.SnackBar(ft.Text("Conexão OK!"), open=True)
            self._append_log("Conexão Firebird bem-sucedida (teste).")
            self.page.update()
        except Exception as e2:
            self.page.snack_bar = ft.SnackBar(ft.Text(str(e2)), open=True)
            self._append_log(f"ERRO de conexão: {e2}")
            self.page.update()

    # ---------- Extração de XML (SQL fixa no código) ----------
    def _on_extract_xml(self, e: ft.ControlEvent):
        # Validar datas (BR)
        try:
            d_ini = self._parse_br(self.tf_ini.value.strip())
            d_fim = self._parse_br(self.tf_fim.value.strip())
            data_ini = dt.datetime.combine(d_ini, dt.time.min)
            data_fim = dt.datetime.combine(d_fim, dt.time.min) + dt.timedelta(days=1) - dt.timedelta(seconds=1)
            if data_ini > data_fim:
                raise ValueError("Data inicial maior que a final.")
        except Exception as ex:
            self.page.snack_bar = ft.SnackBar(ft.Text(f"Datas inválidas: {ex}"), open=True)
            self.page.update()
            return

        if not self._connect_if_needed():
            return

        xml_dir = ensure_dir(Path(self.tf_xml_dir.value.strip()))
        # Tipo de nota (Entrada/Saída) – mantém o dropdown? Agora só com Saída por padrão.
        # Se quiser reativar seleção, adicione um Dropdown; aqui default = Saída
        tipo_str = (self.dd_tipo.value or "").strip().lower()
        if tipo_str.startswith("e"):
            origem = "entrada"
        elif tipo_str.startswith("s"):
            origem = "saida"
        else:
            origem = "ambas"
        self._append_log(f"Iniciando extração XML: origem={origem}, período {data_ini:%Y-%m-%d} a {data_fim:%Y-%m-%d}.")

        t = threading.Thread(target=self._extract_xml_thread, args=(xml_dir, origem, data_ini, data_fim), daemon=True)
        t.start()

    def _sanitize_name(self, s: str) -> str:
        s = (s or "").strip()
        s = re.sub(r"[^0-9A-Za-z_-]+", "", s)
        return s or "sem_chave"

    def _write_xml_file(self, xml_dir: Path, chavenfe: str, payload):
        fname = self._sanitize_name(chavenfe) + ".xml"
        path = xml_dir / fname
        if isinstance(payload, (bytes, bytearray, memoryview)):
            data = bytes(payload)
            path.write_bytes(data)
        else:
            path.write_text(str(payload), encoding="utf-8", errors="replace")
        return path

    def _extract_xml_thread(self, xml_dir: Path, origem: str, dt_ini: dt.datetime, dt_fim: dt.datetime):
        try:
            total_count = 0
            total_skipped = 0
            with self.adapter.engine.connect() as conn:
                def run_query(kind: str):
                    nonlocal total_count, total_skipped
                    if kind == "entrada":
                        sql = text(
                            """
                            SELECT CHAVENFE, ARQUIVOXML
                              FROM NOTASENTRADA
                             WHERE DATADOWNLOAD BETWEEN :ini AND :fim
                            """
                        )
                    else:
                        sql = text(
                            """
                            SELECT CHAVENFE, ARQUIVOXML
                              FROM NOTASLOTENFE
                             WHERE DATAEMISSAO BETWEEN :ini AND :fim
                               AND STATUS IN (100, 150)
                            """
                        )
                    params = {"ini": dt_ini, "fim": dt_fim}
                    res = conn.execution_options(stream_results=True).execute(sql, params)
                    for chavenfe, blob in res:
                        if not blob:
                            total_skipped += 1
                            self._safe_ui(lambda c=chavenfe: self._append_log(f"Sem XML para chave: {c} (vazio)."))
                            continue
                        try:
                            p = self._write_xml_file(xml_dir, chavenfe, blob)
                            total_count += 1
                            self._safe_ui(lambda s=str(p): self._append_log(f"Salvo: {s}"))
                        except Exception as e:
                            self._safe_ui(lambda c=chavenfe, err=str(e): self._append_log(f"ERRO ao salvar {c}: {err}"))

                if origem in ("entrada", "saida"):
                    run_query(origem)
                else:  # ambas
                    self._safe_ui(lambda: self._append_log("Processando notas de ENTRADA..."))
                    run_query("entrada")
                    self._safe_ui(lambda: self._append_log("Processando notas de SAÍDA..."))
                    run_query("saida")

            self._safe_ui(lambda: self._append_log(f"Extração concluída. XMLs salvos: {total_count}. Ignorados (vazios): {total_skipped}."))
            self._safe_ui(lambda: self._set_status("Extração concluída."))
        except Exception as e:
            self._safe_ui(lambda: self._append_log(f"Falha na extração: {e}"))
            self._safe_ui(lambda: self._set_status("Erro na extração."))

    def _set_status(self, txt: str):
        self.status_text.value = txt
        try:
            self.page.update()
        except Exception:
            pass

    # ---------- Utilitário: aplicar caminho do .FDB ----------
    def _apply_fdb_path(self, p: Path):
        s = load_settings()
        s["fb_database"] = str(p)
        save_settings(s)
        self.settings = s

        if hasattr(self, "tf_db_file") and self.tf_db_file is not None:
            try:
                self.tf_db_file.value = str(p)
            except Exception:
                pass

        self._append_log(f".FDB definido: {p}")
        try:
            self.page.update()
        except Exception:
            pass

    # ==================== VIEWS & NAVEGAÇÃO ====================

    def _view_pop(self, e: ft.ViewPopEvent):
        # Remove a view do topo e volta para a rota da view remanescente
        self.page.views.pop()
        top = self.page.views[-1]
        self.page.go(top.route)

    def _build_home_body(self) -> ft.Control:
        return ft.Container(
            content=ft.Column(
                [
                    ft.ResponsiveRow(
                        controls=[
                            ft.Container(
                                content=ft.Column(
                                    [
                                        ft.Row([self.tf_ini, self.tf_fim, self.dd_tipo], alignment=ft.MainAxisAlignment.START),
                                        ft.Row([self.tf_xml_dir, self.btn_xml_pick], alignment=ft.MainAxisAlignment.START),
                                        ft.Row(
                                            [self.btn_test, self.btn_extract],
                                            alignment=ft.MainAxisAlignment.START,
                                            spacing=12,
                                        ),
                                    ],
                                    spacing=12,
                                    expand=False,
                                ),
                                padding=16,
                                border_radius=12,
                                bgcolor=op(0.04, C.GREY_100),
                            ),
                        ]
                    ),
                    self.log_card,
                    ft.Row([self.status_text], alignment=ft.MainAxisAlignment.START),
                ],
                spacing=16,
                expand=False,
            ),
            padding=16,
        )

    def _build_home_view(self) -> ft.View:
        return ft.View(
            route="/",
            controls=[self._build_home_body()],
            appbar=ft.AppBar(
                title=ft.Text(APP_NAME),
                actions=[
                    ft.IconButton(I.SETTINGS, tooltip="Configurações (credenciais)", on_click=lambda e: self.page.go("/settings")),
                ],
            ),
            padding=0,
        )

    def _collect_settings(self) -> dict:
        existing = load_settings()
        # Suporte a Dropdown antigo (dd_driver) e novo RadioGroup (rg_driver)
        driver = None
        try:
            driver = (self.dd_driver.value or "").strip()  # se existir
        except Exception:
            pass
        if not driver:
            try:
                driver = (self.rg_driver.value or "").strip()
            except Exception:
                driver = "firebird"
        return {
            "fb_host": (self.tf_host.value or "").strip(),
            "fb_port": int((self.tf_port.value or "3050").strip()),
            "fb_user": (self.tf_user.value or "").strip(),
            "fb_password": self.tf_pw.value,
            "fb_charset": (self.tf_charset.value or "UTF8").strip(),
            "fb_driver": driver or "firebird",
            "fb_database": (self.tf_db_file.value or "").strip(),
            "xml_folder": existing.get("xml_folder", DEFAULT_SETTINGS["xml_folder"]),
        }

    def _build_settings_view(self) -> ft.View:
        s = load_settings()
        self.settings = s

        # ---------------- Campos (com dicas e larguras adequadas) ----------------
        self.tf_host = ft.TextField(
            label="Servidor (host)", value=s.get("fb_host", "localhost"), dense=True,
            hint_text="ex.: localhost ou 192.168.0.10"
        )
        self.tf_port = ft.TextField(
            label="Porta", value=str(s.get("fb_port", 3050)), dense=True, width=140,
            keyboard_type=ft.KeyboardType.NUMBER,
        )
        self.tf_user = ft.TextField(
            label="Usuário", value=s.get("fb_user", "sysdba"), dense=True, width=260,
        )
        self.tf_pw = ft.TextField(
            label="Senha", value=s.get("fb_password", "masterkey"), dense=True,
            password=True, can_reveal_password=True, width=260,
        )
        self.tf_charset = ft.TextField(
            label="Charset", value=s.get("fb_charset", "UTF8"), dense=True, width=160,
        )
        # Driver agora em RadioGroup (horizontal), mais claro que Dropdown
        self.rg_driver = ft.RadioGroup(
            value=s.get("fb_driver", "firebird"),
            content=ft.Row(
                [ft.Radio(label="firebird (python-firebird-driver)", value="firebird"), ft.Radio(label="fdb", value="fdb")],
                wrap=True,
            ),
        )
        self.tf_db_file = ft.TextField(
            label="Arquivo .FDB", value=s.get("fb_database", ""),
            dense=True, expand=True, read_only=True,
            hint_text="Selecione um .FDB existente na máquina ou rede"
        )
        btn_pick_fdb = ft.IconButton(I.INSERT_DRIVE_FILE, tooltip="Selecionar .FDB...", on_click=lambda e: self._pick_fdb_file())
        # ---------------- Validação ----------------
        def _validate() -> bool:
            ok = True
            # limpar mensagens anteriores
            self.tf_port.error_text = None
            self.tf_db_file.error_text = None
            try:
                p = int((self.tf_port.value or "").strip())
                if p < 1 or p > 65535:
                    raise ValueError
            except Exception:
                self.tf_port.error_text = "Informe uma porta válida (1–65535)."
                ok = False
            from pathlib import Path as _P
            dbp = (self.tf_db_file.value or "").strip()
            if not dbp:
                self.tf_db_file.error_text = "Informe o arquivo .FDB."
                ok = False
            else:
                try:
                    if not _P(dbp).exists():
                        self.tf_db_file.error_text = "Arquivo .FDB não encontrado."
                        ok = False
                except Exception:
                    self.tf_db_file.error_text = "Caminho inválido para o .FDB."
                    ok = False
            try:
                self.page.update()
            except Exception:
                pass
            return ok

        # ---------------- Ações ----------------
        def on_save(e: ft.ControlEvent):
            if not _validate():
                self.page.snack_bar = ft.SnackBar(ft.Text("Corrija os campos destacados."), open=True)
                self.page.update()
                return
            data = self._collect_settings()
            save_settings(data)
            self.settings = data
            self.page.snack_bar = ft.SnackBar(ft.Text("Configurações salvas."), open=True)
            self.page.go("/")

        def on_test(e: ft.ControlEvent):
            if not _validate():
                self.page.snack_bar = ft.SnackBar(ft.Text("Corrija os campos destacados antes de testar."), open=True)
                self.page.update()
                return
            data = self._collect_settings()
            try:
                engine = self.adapter.connect(
                    host=data["fb_host"], port=int(data["fb_port"]), database_path=data["fb_database"],
                    user=data["fb_user"], password=data["fb_password"], charset=data["fb_charset"], driver=data["fb_driver"],
                )
                if engine:
                    self.adapter.close()
                self.page.snack_bar = ft.SnackBar(ft.Text("Conexão OK!"), open=True)
                self._append_log("Conexão Firebird bem-sucedida (teste).")
            except Exception as ex:
                self.page.snack_bar = ft.SnackBar(ft.Text(f"Falha na conexão: {ex}"), open=True)
            self.page.update()

        # ---------------- Layout (Cards + ResponsiveRow) ----------------
        card_server = ft.Card(
            content=ft.Container(
                content=ft.Column(
                    [
                        ft.Text("Servidor e autenticação", weight=ft.FontWeight.BOLD),
                        ft.ResponsiveRow(
                            controls=[
                                ft.Container(self.tf_host, col={"xs":12, "sm":6, "md":5, "lg":5}),
                                ft.Container(self.tf_port, col={"xs":6, "sm":3, "md":2, "lg":2}),
                                ft.Container(self.tf_charset, col={"xs":6, "sm":3, "md":2, "lg":2}),
                                ft.Container(ft.Column([ft.Text("Driver", size=12, color=C.GREY_700), self.rg_driver], spacing=4), col={"xs":12, "sm":12, "md":3, "lg":3}),
                            ],
                            run_spacing=12,
                        ),
                        ft.ResponsiveRow(
                            controls=[
                                ft.Container(self.tf_user, col={"xs":12, "sm":6, "md":4, "lg":4}),
                                ft.Container(self.tf_pw, col={"xs":12, "sm":6, "md":4, "lg":4}),
                            ],
                            run_spacing=12,
                        ),
                    ],
                    spacing=12,
                ),
                padding=16,
            )
        )

        row_db = ft.Row(
            [
                self.tf_db_file,
                btn_pick_fdb,
            ],
            alignment=ft.MainAxisAlignment.START,
            spacing=8,
        )

        card_db = ft.Card(
            content=ft.Container(
                content=ft.Column(
                    [
                        ft.Text("Banco de dados (.FDB)", weight=ft.FontWeight.BOLD),
                        row_db,
                        ft.Text("Dica: para caminhos locais, o host pode ficar em branco ou 'localhost'.", size=12, italic=True, color=C.GREY_700),
                    ],
                    spacing=10,
                ),
                padding=16,
            )
        )

        actions_bar = ft.Row(
            [
                ft.TextButton("Testar conexão", icon=I.CHECK_CIRCLE, on_click=on_test),
                ft.FilledButton("Salvar", icon=I.SAVE, on_click=on_save),
                ft.OutlinedButton("Fechar", icon=I.CLOSE, on_click=lambda e: self.page.go("/")),
            ],
            alignment=ft.MainAxisAlignment.END,
        )

        content = ft.Column(
            [
                ft.Text("Configurações", weight=ft.FontWeight.BOLD, size=18),
                card_server,
                card_db,
                actions_bar,
            ],
            tight=True,
            spacing=16,
            scroll=ft.ScrollMode.ADAPTIVE,
        )

        return ft.View(
            route="/settings",
            controls=[ft.Container(content, padding=16)],
            appbar=ft.AppBar(
                leading=ft.IconButton(I.ARROW_BACK, on_click=lambda e: self.page.go("/")),
                title=ft.Text("Configurações"),
            ),
            padding=0,
        )

    def _route_change(self, e: ft.RouteChangeEvent):
        # Reconstrói a pilha com a view Home e, se necessário, empilha a view de Configurações
        self.page.views.clear()
        self.page.views.append(self._build_home_view())
        if self.page.route == "/settings":
            self.page.views.append(self._build_settings_view())
        self.page.update()

    # ---------- File picker de arquivo .FDB ----------
    def _pick_fdb_file(self):
        self.file_picker_db.pick_files(allow_multiple=False, allowed_extensions=["fdb"])


# -------------------- Entry point --------------------

def main(page: ft.Page):
    AuditoriaNFApp(page)


if __name__ == "__main__":
    settings = load_settings()
    ensure_dir(Path(settings.get("xml_folder", DEFAULT_SETTINGS["xml_folder"])) )
    ft.app(target=main)
