# -*- coding: utf-8 -*-
"""
Executor SQL Multi‑Conexões — Flet (versão limpa e estável)
----------------------------------------------------------
• Configs SEMPRE em ./config/ (relativo ao .py): conexoes.json, usuarios.json, chave.key
• Lista de conexões (✓ executa) + seleção por clique (editar/excluir)
• Usuários com senha criptografada (Fernet)
• Execução paralela (ThreadPoolExecutor); SELECT → resultado em abas
• Log somente‑leitura
• Testar conexão selecionada
• ThemeMode.SYSTEM (claro/escuro automático)

Observação: atendendo pedido, usei ft.Icons e ft.Colors.
Se sua versão do Flet expõe apenas ft.icons/ft.colors, troque esses dois nomes para minúsculos.

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

COLOR_SURFACE_VARIANT = _color("SURFACE_VARIANT", "#E7E0EC")  # fallback M3 aproximado
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

DEFAULT_MAX_ROWS = 500  # segurança de UI (limite global por aba)
PAGE_SIZE = 100         # linhas por página na tabela

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

    # Conexões
    def add_conexao(self, dados: Dict[str, Any]) -> None:
        self.conexoes.append(dados)
        salvar_json(CONEXOES_FILE, self.conexoes)

    def update_conexao(self, idx: int, dados: Dict[str, Any]) -> None:
        self.conexoes[idx] = dados
        salvar_json(CONEXOES_FILE, self.conexoes)

    def del_conexao(self, idx: int) -> None:
        del self.conexoes[idx]
        salvar_json(CONEXOES_FILE, self.conexoes)

    # Usuários
    def add_usuario(self, nome: str, senha: str) -> None:
        self.usuarios.append({"nome": nome, "senha": criptografar(senha)})
        salvar_json(USUARIOS_FILE, self.usuarios)

    def del_usuario(self, idx: int) -> None:
        del self.usuarios[idx]
        salvar_json(USUARIOS_FILE, self.usuarios)


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


# ────────────────────────── Forms (Dialogs) ──────────────────────────
class ConexaoDialog:
    def __init__(self, page: ft.Page, on_save) -> None:
        self.page = page
        self.on_save = on_save
        self.nome = ft.TextField(label="Nome", dense=True)
        self.host = ft.TextField(label="Host", dense=True)
        self.port = ft.TextField(label="Porta", dense=True, value="5432")
        self.db = ft.TextField(label="Banco", dense=True)
        self.dlg = ft.AlertDialog(
            modal=True,
            title=ft.Text("Conexão"),
            content=ft.Column([self.nome, self.host, self.port, self.db], tight=True, scroll=ft.ScrollMode.AUTO),
            actions=[
                ft.TextButton("Cancelar", on_click=self._close),
                ft.FilledButton("Salvar", on_click=self._salvar),
            ],
            actions_alignment=ft.MainAxisAlignment.END,
        )

    def open(self, dados=None) -> None:
        if dados:
            self.nome.value = dados.get("nome", "")
            self.host.value = dados.get("host", "")
            self.port.value = str(dados.get("port", ""))
            self.db.value = dados.get("database", "")
        else:
            self.nome.value = ""
            self.host.value = ""
            self.db.value = ""
            self.port.value = "5432"
        self.dlg.open = True
        self.page.dialog = self.dlg
        self.page.update()

    def _close(self, _e=None) -> None:
        self.dlg.open = False
        self.page.update()

    def _salvar(self, _e=None) -> None:
        if not all([self.nome.value, self.host.value, self.port.value, self.db.value]):
            dialog_alert(self.page, "Aviso", "Preencha todos os campos.")
            return
        try:
            port = int(self.port.value)
        except ValueError:
            dialog_alert(self.page, "Aviso", "Porta inválida.")
            return
        self.on_save({"nome": self.nome.value, "host": self.host.value, "port": port, "database": self.db.value})
        self._close()


class UsuariosDialog:
    def __init__(self, page: ft.Page, repo: Repositorio, on_change) -> None:
        self.page = page
        self.repo = repo
        self.on_change = on_change
        self.listview = ft.ListView(height=240, spacing=4, padding=0)
        self.nome = ft.TextField(label="Nome", dense=True)
        self.senha = ft.TextField(label="Senha", dense=True, password=True, can_reveal_password=True)
        self.dlg = ft.AlertDialog(
            modal=True,
            title=ft.Text("Usuários"),
            content=ft.Column([
                self.listview,
                ft.Row([self.nome, self.senha], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
            ], tight=True),
            actions=[
                ft.TextButton("Fechar", on_click=self._close),
                ft.OutlinedButton("Remover selecionado", on_click=self._remover),
                ft.FilledButton("Adicionar", on_click=self._adicionar),
            ],
            actions_alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
        )
        self._refresh()

    def _refresh(self) -> None:
        self.listview.controls.clear()
        for i, u in enumerate(self.repo.usuarios):
            self.listview.controls.append(ft.Checkbox(label=u["nome"], value=False, data=i))

    def open(self) -> None:
        self._refresh()
        self.dlg.open = True
        self.page.dialog = self.dlg
        self.page.update()

    def _close(self, _e=None) -> None:
        self.dlg.open = False
        self.page.update()

    def _adicionar(self, _e=None) -> None:
        if (not self.nome.value) or (not self.senha.value):
            dialog_alert(self.page, "Aviso", "Informe nome e senha.")
            return
        self.repo.add_usuario(self.nome.value, self.senha.value)
        self.nome.value = ""
        self.senha.value = ""
        self._refresh()
        self.on_change()
        self.page.update()

    def _remover(self, _e=None) -> None:
        selecionados = [cb for cb in self.listview.controls if isinstance(cb, ft.Checkbox) and cb.value]
        if not selecionados:
            dialog_alert(self.page, "Aviso", "Selecione pelo menos um usuário para remover.")
            return
        for cb in selecionados[::-1]:
            self.repo.del_usuario(cb.data)
        self._refresh()
        self.on_change()
        self.page.update()


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
        # datasets por aba: chave = título
        self._datasets: Dict[str, Dict[str, Any]] = {}

    def limpar(self, page: ft.Page | None) -> None:
        # limpa abas e datasets; tenta via pubsub se disponível
        if page is not None and hasattr(page, "pubsub") and page.pubsub is not None:
            try:
                page.pubsub.send_message(("clear_tabs", None))
                return
            except Exception:
                pass
        # fallback direto
        self.tabs.tabs = []
        self._datasets.clear()
        self.tabs.update()

    def adicionar_tabela(self, page: ft.Page | None, titulo: str, colunas: List[str], dados: List[tuple]) -> None:
        # tenta via pubsub (thread-safe) quando disponível
        if page is not None and hasattr(page, "pubsub") and page.pubsub is not None:
            try:
                payload = {"titulo": titulo, "colunas": list(colunas), "dados": list(dados)}
                page.pubsub.send_message(("add_table", payload))
                return
            except Exception:
                pass

        # fallback direto (assume contexto de UI)
        dados_safe = list(dados)  # sem limite: mostra tudo
        self._datasets[titulo] = {"colunas": list(colunas), "dados": dados_safe}

        # cria aba com placeholder
        self.tabs.tabs.append(ft.Tab(text=titulo, content=ft.Container(ft.Text("Carregando..."))))
        self.tabs.update()

        # renderiza a tabela
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

        # coloca o conteúdo na aba correspondente
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

    def executar(self, conexoes_sel: List[Dict[str, Any]], usuario: str, senha: str, sql: str) -> None:
        if not conexoes_sel:
            dialog_alert(self.page, "Aviso", "Selecione pelo menos uma conexão.")
            return
        if not sql or not sql.strip():
            dialog_alert(self.page, "Aviso", "Digite um comando SQL.")
            return
        self.resultados.limpar(self.page)
        self.logger.write(self.page, "Executando...\n")
        for cx in conexoes_sel:
            self.pool.submit(self._run_sql, cx, usuario, senha, sql)

    def testar(self, conexao: Dict[str, Any], usuario: str, senha: str) -> None:
        try:
            conn = psycopg2.connect(
                host=conexao["host"],
                port=int(conexao["port"]) if isinstance(conexao.get("port"), str) and str(conexao["port"]).isdigit() else conexao["port"],
                user=usuario,
                password=senha,
                database=conexao["database"],
                connect_timeout=5,
            )
            conn.close()
            self.logger.write(self.page, "✅ [{}] Conexão OK\n".format(conexao.get("nome", "?")))
        except Exception as e:
            self.logger.write(self.page, "❌ [{}] Falha de conexão: {}\n".format(conexao.get("nome", "?"), e))

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

        # Estado de seleção
        self.chk_conexoes: List[ft.Checkbox] = []
        self.idx_selecionado: int | None = None

        # UI principais
        self.txt_log = ft.Text(selectable=True)
        self.logger = Logger(self.txt_log)
        self.result_tabs = ResultadoTabs()
        self.executor: ExecutorSQL | None = None

        # Inputs
        self.sql_input = ft.TextField(label="Comando SQL", multiline=True, min_lines=6, max_lines=12, expand=True)
        self.usuario_combo = ft.Dropdown(label="Usuário para executar", options=[], value=None)

    def _on_pubsub(self, message):
        """Manipula mensagens vindas de threads: (kind, payload)."""
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
            dados_safe = list(dados)  # sem limite: mostra tudo
            self.result_tabs.tabs.tabs.append(ft.Tab(text=titulo, content=ft.Container(ft.Text("Carregando..."))))
            self.result_tabs.tabs.update()
            self.result_tabs._render_table(titulo)

    # Util
    def _refresh_usuarios_combo(self) -> None:
        self.usuario_combo.options = [ft.dropdown.Option(u["nome"]) for u in self.repo.usuarios]
        if self.repo.usuarios and (self.usuario_combo.value is None):
            self.usuario_combo.value = self.repo.usuarios[0]["nome"]
        try:
            if self.usuario_combo.page is not None:
                self.usuario_combo.update()
        except Exception:
            pass

    def _refresh_conexoes_list(self, lst: ft.ListView) -> None:
        lst.controls.clear()
        self.chk_conexoes.clear()
        for i, cx in enumerate(self.repo.conexoes):
            label_text = "{} ({}:{}/{})".format(cx.get("nome", "?"), cx.get("host", "?"), cx.get("port", "?"), cx.get("database", "?"))
            cb = ft.Checkbox(label=label_text, value=True, data=i)
            row = ft.Container(
                content=ft.Row([cb], alignment=ft.MainAxisAlignment.START),
                padding=8,
                bgcolor=COLOR_SURFACE_VARIANT,
                border_radius=8,
                on_click=lambda e, idx=i: self._select_row(idx, e.control),
            )
            lst.controls.append(row)
            self.chk_conexoes.append(cb)
        try:
            if lst.page is not None:
                lst.update()
        except Exception:
            pass

    def _select_row(self, idx: int, container: ft.Container) -> None:
        parent = container.parent
        for cont in parent.controls:
            if isinstance(cont, ft.Container):
                cont.bgcolor = COLOR_SURFACE_VARIANT
        container.bgcolor = COLOR_PRIMARY_CONTAINER
        container.update()
        self.idx_selecionado = idx

    def _toggle_all(self, value: bool) -> None:
        for cb in self.chk_conexoes:
            cb.value = value
            cb.update()

    def _toggle_all_auto(self, _e=None) -> None:
        """Marca todos se houver pelo menos um desmarcado; senão desmarca todos."""
        mark_all = any((not cb.value) for cb in self.chk_conexoes)
        self._toggle_all(mark_all)

    # Ações de conexões
    def _adicionar_conexao(self, lst: ft.ListView) -> None:
        dlg = ConexaoDialog(self.page, on_save=lambda dados: (self.repo.add_conexao(dados), self._refresh_conexoes_list(lst)))
        dlg.open()

    def _editar_conexao(self, lst: ft.ListView) -> None:
        if self.idx_selecionado is None:
            dialog_alert(self.page, "Aviso", "Clique em uma conexão para editar.")
            return
        dados = self.repo.conexoes[self.idx_selecionado]
        dlg = ConexaoDialog(self.page, on_save=lambda d: (self.repo.update_conexao(self.idx_selecionado, d), self._refresh_conexoes_list(lst)))
        dlg.open(dados)

    def _excluir_conexao(self, lst: ft.ListView) -> None:
        if self.idx_selecionado is None:
            dialog_alert(self.page, "Aviso", "Clique em uma conexão para excluir.")
            return
        nome = self.repo.conexoes[self.idx_selecionado].get("nome", "?")

        confirm = ft.AlertDialog(
            modal=True,
            title=ft.Text("Confirmar exclusão"),
            content=ft.Text(f"Excluir conexão '{nome}'?"),
        )

        def cancelar(_e=None) -> None:
            confirm.open = False
            self.page.update()

        def confirmar(_e=None) -> None:
            confirm.open = False
            self.page.update()
            self.repo.del_conexao(self.idx_selecionado)
            self.idx_selecionado = None
            self._refresh_conexoes_list(lst)

        confirm.actions = [ft.TextButton("Cancelar", on_click=cancelar), ft.FilledButton("Excluir", on_click=confirmar)]
        self.page.dialog = confirm
        confirm.open = True
        self.page.update()

    def _gerenciar_usuarios(self) -> None:
        dlg = UsuariosDialog(self.page, self.repo, on_change=self._refresh_usuarios_combo)
        dlg.open()

    def _testar_conexao_sel(self, lst: ft.ListView) -> None:
        if self.idx_selecionado is None:
            dialog_alert(self.page, "Aviso", "Clique em uma conexão e depois em Testar.")
            return
        if not self.usuario_combo.value:
            dialog_alert(self.page, "Aviso", "Selecione um usuário executor para testar a conexão.")
            return
        cred = next((u for u in self.repo.usuarios if u["nome"] == self.usuario_combo.value), None)
        if not cred:
            dialog_alert(self.page, "Erro", "Usuário não encontrado.")
            return
        senha = descriptografar(cred["senha"])
        cx = self.repo.conexoes[self.idx_selecionado]
        self.executor.testar(cx, cred["nome"], senha)

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
        selecionadas: List[Dict[str, Any]] = []
        for cb in self.chk_conexoes:
            if cb.value:
                i = int(cb.data)
                selecionadas.append(self.repo.conexoes[i])
        self.executor.executar(selecionadas, cred["nome"], senha, self.sql_input.value)

    # Layout
    def view(self, page: ft.Page):
        self.page = page
        page.title = "Executor SQL Multi‑Conexões — Flet"
        page.theme_mode = ft.ThemeMode.SYSTEM
        page.window_width = 1100
        page.window_height = 800
        page.padding = 12
        page.scroll = ft.ScrollMode.AUTO

        # inscreve handler de pubsub para atualizar UI a partir de threads
        if hasattr(page, "pubsub") and page.pubsub is not None:
            page.pubsub.subscribe(self._on_pubsub)

        self.executor = ExecutorSQL(page, self.logger, self.result_tabs)

        # ── Coluna esquerda: conexões + botões
        lst_con = ft.ListView(height=250, spacing=6, padding=0, auto_scroll=False)
        barra_con_botoes = ft.Row([
            ft.ElevatedButton("Adicionar", icon=ICONS.ADD, on_click=lambda e: self._adicionar_conexao(lst_con)),
            ft.OutlinedButton("Editar", icon=ICONS.EDIT, on_click=lambda e: self._editar_conexao(lst_con)),
            ft.OutlinedButton("Excluir", icon=ICONS.DELETE, on_click=lambda e: self._excluir_conexao(lst_con)),
            ft.OutlinedButton("Usuários", icon=ICONS.PERSON, on_click=lambda e: self._gerenciar_usuarios()),
            ft.OutlinedButton("Testar", icon=ICONS.CHECK_CIRCLE, on_click=lambda e: self._testar_conexao_sel(lst_con)),
            ft.TextButton("(De)Selecionar todos", on_click=self._toggle_all_auto),
        ], wrap=True)

        bloco_conexoes = ft.Card(
            content=ft.Container(
                content=ft.Column([
                    ft.Text("Conexões (✓ executa | clique seleciona)", weight=ft.FontWeight.BOLD),
                    lst_con,
                    barra_con_botoes,
                ], tight=True),
                padding=16,
            )
        )

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

        page.add(
            ft.ResponsiveRow([
                ft.Column([bloco_conexoes], col={"xs": 12, "sm": 12, "md": 5, "lg": 4, "xl": 3}),
                ft.Column([bloco_sql, bloco_log, bloco_resultados], col={"xs": 12, "sm": 12, "md": 7, "lg": 8, "xl": 9}),
            ])
        )

        # Controles anexados → agora podemos atualizar listas/combos
        self._refresh_conexoes_list(lst_con)
        self._refresh_usuarios_combo()
        return page


def main(page: ft.Page):
    app = AppFlet()
    app.view(page)


if __name__ == "__main__":
    ft.app(target=main)