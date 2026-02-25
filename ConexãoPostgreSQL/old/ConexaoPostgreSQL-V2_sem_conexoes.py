# -*- coding: utf-8 -*-
"""
Executor SQL Multi‑Conexões — Flet (versão simplificada: sem container de conexões)
-----------------------------------------------------------------------------------
• Configs em ./config/ (relativo ao .py): conexoes.json, usuarios.json, chave.key
• REMOVIDO: container de "Conexões" (lista + botões Adicionar/Editar/Excluir/Usuários/Testar/(De)Selecionar)
• Mantido: Usuário executor (dropdown), caixa para SQL, execução paralela, abas de resultados, log

Comportamento novo:
- Ao clicar em "Executar SQL", o comando será executado em TODAS as conexões existentes em config/conexoes.json.

Requisitos:
    pip install flet psycopg2-binary cryptography
"""

import os
import json
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any

from cryptography.fernet import Fernet
import psycopg2
import flet as ft

# ── Compat: lidar com diferenças entre ft.Colors/ft.colors e ausência de alguns tokens
def _color(name: str, fallback: str):
    return getattr(getattr(ft, "Colors", object), name, getattr(getattr(ft, "colors", object), name, fallback))

COLOR_SURFACE_VARIANT = _color("SURFACE_VARIANT", "#E7E0EC")
COLOR_PRIMARY_CONTAINER = _color("PRIMARY_CONTAINER", "#EADDFF")
COLOR_SECONDARY = _color("SECONDARY", "#625B71")
COLOR_BLACK = _color("BLACK", "#000000")

ICONS = getattr(ft, "Icons", getattr(ft, "icons", None))

# ────────────────────────── Paths de configuração ─────────────────────
BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = BASE_DIR / "config"
CONEXOES_FILE = CONFIG_DIR / "conexoes.json"
USUARIOS_FILE = CONFIG_DIR / "usuarios.json"
CHAVE_FILE = CONFIG_DIR / "chave.key"

DEFAULT_MAX_ROWS = 500
PAGE_SIZE = 100

os.makedirs(str(CONFIG_DIR), exist_ok=True)
if not os.path.exists(str(CHAVE_FILE)):
    with open(str(CHAVE_FILE), "wb") as _f:
        _f.write(Fernet.generate_key())
with open(str(CHAVE_FILE), "rb") as _f:
    fernet = Fernet(_f.read())


def carregar_json(path: Path | str, vazio):
    path = str(path)
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as _f:
            json.dump(vazio, _f, ensure_ascii=False, indent=2)
    with open(path, "r", encoding="utf-8") as _f:
        return json.load(_f)


def salvar_json(path: Path | str, data):
    path = str(path)
    with open(path, "w", encoding="utf-8") as _f:
        json.dump(data, _f, ensure_ascii=False, indent=2)


def criptografar(txt: str) -> str:
    return fernet.encrypt(txt.encode("utf-8")).decode("utf-8")


def descriptografar(txt: str) -> str:
    return fernet.decrypt(txt.encode("utf-8")).decode("utf-8")


# ────────────────────────── Camada de dados ──────────────────────────
class Repositorio:
    def __init__(self) -> None:
        self.conexoes: List[Dict[str, Any]] = carregar_json(CONEXOES_FILE, [])
        self.usuarios: List[Dict[str, str]] = carregar_json(USUARIOS_FILE, [])


# ────────────────────────── UI Helpers ───────────────────────────────
class Logger:
    def __init__(self, txt_control: ft.Text) -> None:
        self.txt = txt_control
        self._lock = threading.Lock()

    def write(self, page: ft.Page, msg: str) -> None:
        def _append() -> None:
            self.txt.value = (self.txt.value or "") + msg
            self.txt.update()
        # Se a página suportar pubsub, despacha para o thread principal
        if page is not None and hasattr(page, "pubsub") and page.pubsub is not None:
            try:
                page.pubsub.send_message(("log", msg))
                return
            except Exception:
                pass
        # fallback síncrono
        _append()


def dialog_alert(page: ft.Page, title: str, msg: str) -> None:
    dlg = ft.AlertDialog(
        modal=True,
        title=ft.Text(title),
        content=ft.Text(msg),
        actions=[ft.TextButton("OK", on_click=lambda e: (setattr(dlg, "open", False), page.update()))],
        actions_alignment=ft.MainAxisAlignment.END,
    )
    page.dialog = dlg
    dlg.open = True
    page.update()


# ────────────────────────── Resultado (Tabs + Tabelas) ───────────────

class ResultadoTabs:
    def __init__(self) -> None:
        self.tabs = ft.Tabs(
            selected_index=0,
            animation_duration=200,
            tabs=[],
            height=360,
            expand=True,
        )
        self._datasets: Dict[str, Dict[str, Any]] = {}

    def limpar(self, page: ft.Page | None) -> None:
        if page is not None and hasattr(page, "pubsub") and page.pubsub is not None:
            try:
                page.pubsub.send_message(("clear_tabs", None))
                return
            except Exception:
                pass
        self.tabs.tabs = []
        self._datasets.clear()
        self.tabs.update()

    def adicionar_tabela(self, page: ft.Page | None, titulo: str, colunas: List[str], dados: List[tuple]) -> None:
        if page is not None and hasattr(page, "pubsub") and page.pubsub is not None:
            try:
                payload = {"titulo": titulo, "colunas": list(colunas), "dados": list(dados)}
                page.pubsub.send_message(("add_table", payload))
                return
            except Exception:
                pass

        dados_safe = list(dados)  # sem limite
        self._datasets[titulo] = {"colunas": list(colunas), "dados": dados_safe}

        self.tabs.tabs.append(ft.Tab(text=titulo, content=ft.Container(ft.Text("Carregando..."))))
        self.tabs.update()

        self._render_table(titulo)

    def _render_table(self, titulo: str) -> None:
        info = self._datasets.get(titulo, {})
        colunas: List[str] = list(info.get("colunas", []))
        dados: List[tuple] = list(info.get("dados", []))

        columns = [ft.DataColumn(ft.Text(c)) for c in colunas]
        rows: List[ft.DataRow] = []
        for row in dados:
            cells = [ft.DataCell(ft.Text("" if v is None else str(v))) for v in row]
            rows.append(ft.DataRow(cells=cells))

        dt = ft.DataTable(columns=columns, rows=rows, divider_thickness=0)

        conteudo = ft.Container(
            content=ft.Column(
                controls=[dt],
                tight=True,
                scroll=ft.ScrollMode.AUTO
            ),
            expand=True,
            padding=0
        )

        for t in self.tabs.tabs:
            if t.text == titulo:
                t.content = conteudo
                break
        self.tabs.update()


class ExecutorSQL:
    def __init__(self, page: ft.Page, logger: Logger, resultados: ResultadoTabs) -> None:
        self.page = page
        self.logger = logger
        self.resultados = resultados
        self.pool = ThreadPoolExecutor(max_workers=6)

    def executar(self, conexoes: List[Dict[str, Any]], usuario: str, senha: str, sql: str) -> None:
        if not conexoes:
            dialog_alert(self.page, "Aviso", "Nenhuma conexão encontrada em config/conexoes.json.")
            return
        if not sql or not sql.strip():
            dialog_alert(self.page, "Aviso", "Digite um comando SQL.")
            return
        self.resultados.limpar(self.page)
        self.logger.write(self.page, "Executando em {} conexão(ões)...\n".format(len(conexoes)))
        for cx in conexoes:
            self.pool.submit(self._run_sql, cx, usuario, senha, sql)

    def _run_sql(self, cx: Dict[str, Any], user: str, senha: str, sql: str) -> None:
        try:
            conn = psycopg2.connect(
                host=cx["host"],
                port=int(cx["port"]) if isinstance(cx.get("port"), str) and str(cx["port"]).isdigit() else cx["port"],
                user=user,
                password=senha,
                database=cx["database"],
            )
            cur = conn.cursor()
            cur.execute(sql)
            is_select = sql.strip().lower().startswith("select")
            if is_select:
                dados = cur.fetchall()
                colunas = [desc[0] for desc in cur.description]
                self.resultados.adicionar_tabela(self.page, cx.get("nome", "?"), colunas, dados)
            conn.commit()
            cur.close()
            conn.close()
            self.logger.write(self.page, "✅ [{}] Comando executado\n".format(cx.get("nome", "?")))
        except Exception as e:
            self.logger.write(self.page, "❌ [{}] Erro: {}\n".format(cx.get("nome", "?"), e))


# ────────────────────────── App Flet ─────────────────────────────────
class AppFlet:
    def __init__(self) -> None:
        self.repo = Repositorio()
        self.page: ft.Page | None = None

        # UI principais
        self.txt_log = ft.Text(selectable=True)
        self.logger = Logger(self.txt_log)
        self.result_tabs = ResultadoTabs()
        self.executor: ExecutorSQL | None = None

        # Inputs
        self.sql_input = ft.TextField(label="Comando SQL", multiline=True, min_lines=6, max_lines=12, expand=True)
        self.usuario_combo = ft.Dropdown(label="Usuário para executar", options=[], value=None)

    def _on_pubsub(self, message):
        try:
            kind, payload = message
        except Exception:
            return
        if kind == "log":
            self.txt_log.value = (self.txt_log.value or "") + str(payload)
            self.txt_log.update()
        elif kind == "clear_tabs":
            self.result_tabs.tabs.tabs.clear()
            self.result_tabs._datasets.clear()
            self.result_tabs.tabs.update()
        elif kind == "add_table":
            data = payload or {}
            titulo = data.get("titulo", "?")
            colunas = list(data.get("colunas", []))
            dados = list(data.get("dados", []))
            dados_safe = list(dados)
            self.result_tabs.tabs.tabs.append(ft.Tab(text=titulo, content=ft.Container(ft.Text("Carregando..."))))
            self.result_tabs.tabs.update()
            self.result_tabs._render_table(titulo)

    def _refresh_usuarios_combo(self) -> None:
        self.usuario_combo.options = [ft.dropdown.Option(u["nome"]) for u in self.repo.usuarios]
        if self.repo.usuarios and (self.usuario_combo.value is None):
            self.usuario_combo.value = self.repo.usuarios[0]["nome"]
        try:
            if self.usuario_combo.page is not None:
                self.usuario_combo.update()
        except Exception:
            pass

    # Execução
    def _executar(self, _e=None) -> None:
        if not self.usuario_combo.value:
            dialog_alert(self.page, "Aviso", "Selecione um usuário executor.")
            return
        cred = next((u for u in self.repo.usuarios if u["nome"] == self.usuario_combo.value), None)
        if not cred:
            dialog_alert(self.page, "Erro", "Usuário não encontrado.")
            return
        senha = descriptografar(cred["senha"])
        # NOVO: executa em TODAS as conexões cadastradas
        selecionadas = list(self.repo.conexoes)
        self.executor.executar(selecionadas, cred["nome"], senha, self.sql_input.value)

    # Layout
    def view(self, page: ft.Page):
        self.page = page
        page.title = "Executor SQL Multi‑Conexões — Flet (simplificado)"
        page.theme_mode = ft.ThemeMode.SYSTEM
        page.window_width = 1000
        page.window_height = 750
        page.padding = 12
        page.scroll = ft.ScrollMode.AUTO

        if hasattr(page, "pubsub") and page.pubsub is not None:
            page.pubsub.subscribe(self._on_pubsub)

        self.executor = ExecutorSQL(page, self.logger, self.result_tabs)

        # ── Centro: SQL + usuário + executar
        barra_exec = ft.Row([
            self.usuario_combo,
            ft.FilledButton("Executar SQL", icon=ICONS.PLAY_ARROW, on_click=self._executar),
        ], alignment=ft.MainAxisAlignment.END)

        bloco_sql = ft.Card(
            content=ft.Container(
                content=ft.Column([
                    ft.Text("Comando SQL", weight=ft.FontWeight.BOLD),
                    self.sql_input,
                    barra_exec,
                ], tight=True),
                padding=16,
            )
        )

        # ── Log
        bloco_log = ft.Card(
            content=ft.Container(
                content=ft.Column([
                    ft.Text("Log", weight=ft.FontWeight.BOLD),
                    ft.Container(ft.Text("(Somente leitura)", size=11, color=COLOR_SECONDARY), padding=0),
                    ft.Container(self.txt_log, bgcolor=None, border_radius=8, padding=10, height=160),
                ], tight=True),
                padding=16,
            )
        )

        # ── Resultados (Tabs)
        bloco_resultados = ft.Card(
            content=ft.Container(
                content=ft.Column([
                    ft.Text("Resultados (SELECT)", weight=ft.FontWeight.BOLD),
                    self.result_tabs.tabs,
                ], tight=True),
                padding=16,
            )
        )

        # Layout sem a coluna de conexões
        page.add(
            ft.ResponsiveRow([
                ft.Column([bloco_sql, bloco_log, bloco_resultados], col={"xs": 12, "sm": 12, "md": 12, "lg": 12, "xl": 12}),
            ])
        )

        # Popular combo de usuários
        self._refresh_usuarios_combo()
        return page


def main(page: ft.Page):
    app = AppFlet()
    app.view(page)


if __name__ == "__main__":
    ft.app(target=main)
