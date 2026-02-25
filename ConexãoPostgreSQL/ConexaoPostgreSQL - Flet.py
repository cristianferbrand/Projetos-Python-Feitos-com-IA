# -*- coding: utf-8 -*-
"""
Executor SQL Multi-Conexões — Flet
----------------------------------------------
Ferramenta para executar comandos SQL (SELECT, INSERT, UPDATE, DELETE, etc.) em múltiplas conexões PostgreSQL simultaneamente.
Interface gráfica com Flet (Flutter para Python).

Arquivos (relativos ao .py):
./config/conexoes.json   -> [{"nome": "...","host":"...","port":5432,"database":"..."}]
./config/usuarios.json   -> [{"nome": "...","senha": "<FERNET>"}]
./config/chave.key       -> chave de criptografia (gerado automático)
Requisitos:
    pip install flet psycopg2-binary cryptography
"""

import os
import json
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any, Optional

import flet as ft

# ====== Criptografia (Fernet) ======
try:
    from cryptography.fernet import Fernet
    _HAS_FERNET = True
except Exception:
    Fernet = None  # type: ignore
    _HAS_FERNET = False

BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = BASE_DIR / "config"
CONEXOES_FILE = CONFIG_DIR / "conexoes.json"
USUARIOS_FILE = CONFIG_DIR / "usuarios.json"
CHAVE_FILE = CONFIG_DIR / "chave.key"

CONFIG_DIR.mkdir(parents=True, exist_ok=True)

def _get_fernet():
    if not _HAS_FERNET:
        return None
    try:
        if not CHAVE_FILE.exists():
            CHAVE_FILE.write_bytes(Fernet.generate_key())
        return Fernet(CHAVE_FILE.read_bytes())
    except Exception:
        return None

_F = _get_fernet()

def criptografar(txt: str) -> str:
    if _F is None:
        return txt
    return _F.encrypt(txt.encode()).decode()

def descriptografar(ct: str) -> str:
    if _F is None:
        return ct
    try:
        return _F.decrypt(ct.encode()).decode()
    except Exception:
        return ""

# ====== I/O JSON ======
def _load_json(path: Path, default):
    try:
        if path.exists():
            with path.open("r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return default

def _save_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# ====== Repo ======
class Repo:
    def __init__(self) -> None:
        self.conexoes: List[Dict[str, Any]] = _load_json(CONEXOES_FILE, [])
        self.usuarios: List[Dict[str, Any]] = _load_json(USUARIOS_FILE, [])

    # conexões
    def salvar_conexoes(self) -> None:
        _save_json(CONEXOES_FILE, self.conexoes)

    def add_conexao(self, dados: Dict) -> None:
        self.conexoes.append(dados)
        self.salvar_conexoes()

    def update_conexao(self, idx: int, dados: Dict) -> None:
        self.conexoes[idx] = dados
        self.salvar_conexoes()

    def del_conexao(self, idx: int) -> None:
        self.conexoes.pop(idx)
        self.salvar_conexoes()

    # usuários
    def salvar_usuarios(self) -> None:
        _save_json(USUARIOS_FILE, self.usuarios)

# ====== Helpers UI ======
def show_toast(page: ft.Page, message: str):
    page.snack_bar = ft.SnackBar(content=ft.Text(message))
    page.snack_bar.open = True
    page.update()

def dialog_alert(page: ft.Page, title: str, message: str):
    dlg = ft.AlertDialog(
        title=ft.Text(title),
        content=ft.Text(message),
        actions_alignment=ft.MainAxisAlignment.END,
        actions=[ft.TextButton("OK", on_click=lambda e: _close())]
    )
    def _close():
        dlg.open = False
        page.update()
    page.dialog = dlg
    dlg.open = True
    page.update()

def push_view(page: ft.Page, view: ft.View):
    page.views.append(view)
    page.update()

def pop_view(page: ft.Page):
    try:
        if len(page.views) > 1:
            page.views.pop()
            page.update()
    except Exception:
        pass

# ====== Views secundárias ======
def open_conexao_view(page: ft.Page, titulo: str, dados_iniciais: Optional[Dict], on_save):
    nome = ft.TextField(label="Nome", autofocus=True, value=(dados_iniciais or {}).get("nome",""))
    host = ft.TextField(label="Host", value=(dados_iniciais or {}).get("host",""))
    port = ft.TextField(label="Porta", value=str((dados_iniciais or {}).get("port","5432")), keyboard_type=ft.KeyboardType.NUMBER)
    db   = ft.TextField(label="Banco de dados", value=(dados_iniciais or {}).get("database",""))

    def salvar(_e=None):
        if not all([nome.value.strip(), host.value.strip(), port.value.strip(), db.value.strip()]):
            dialog_alert(page, "Aviso", "Preencha todos os campos.")
            return
        try:
            p = int(port.value)
        except ValueError:
            dialog_alert(page, "Erro", "A porta deve ser numérica.")
            return
        dados = {"nome": nome.value.strip(), "host": host.value.strip(), "port": p, "database": db.value.strip()}
        try:
            on_save(dados)
            show_toast(page, "Conexão salva.")
            pop_view(page)
        except Exception as e:
            dialog_alert(page, "Erro", f"Falha ao salvar: {e}")

    view = ft.View(
        route="/conexao",
        appbar=ft.AppBar(title=ft.Text(titulo), center_title=True,
                         leading=ft.IconButton(ft.Icons.ARROW_BACK, on_click=lambda e: pop_view(page))),
        controls=[
            ft.Container(
                content=ft.Column([
                    nome, host, port, db,
                    ft.Row([ft.ElevatedButton("Cancelar", on_click=lambda e: pop_view(page)),
                            ft.FilledButton("Salvar", on_click=salvar)],
                           alignment=ft.MainAxisAlignment.END),
                ], tight=True, scroll=ft.ScrollMode.AUTO),
                padding=20, expand=True
            )
        ]
    )
    push_view(page, view)

def open_usuarios_view(page: ft.Page, repo: Repo, on_change=None):
    nome  = ft.TextField(label="Usuário (login)")
    senha = ft.TextField(label="Senha", password=True, can_reveal_password=True)
    lista = ft.ListView(expand=True, spacing=6, auto_scroll=False)

    def refresh_list():
        lista.controls = [ft.ListTile(title=ft.Text(u["nome"])) for u in repo.usuarios]
        page.update()

    def adicionar(_e=None):
        if not nome.value.strip() or not senha.value.strip():
            dialog_alert(page, "Aviso", "Informe usuário e senha.")
            return
        repo.usuarios.append({"nome": nome.value.strip(), "senha": criptografar(senha.value)})
        repo.salvar_usuarios()
        if on_change:
            on_change()
        nome.value, senha.value = "", ""
        refresh_list()
        show_toast(page, "Usuário adicionado.")

    def remover(_e=None):
        alvo = nome.value.strip()
        if not alvo and lista.controls:
            alvo = getattr(lista.controls[-1].title, "value", "")
        if not alvo:
            dialog_alert(page, "Aviso", "Informe o usuário a remover (campo Usuário) ou selecione na lista.")
            return
        idx = next((i for i,u in enumerate(repo.usuarios) if u["nome"] == alvo), -1)
        if idx < 0:
            dialog_alert(page, "Aviso", "Usuário não encontrado.")
            return
        repo.usuarios.pop(idx)
        repo.salvar_usuarios()
        if on_change:
            on_change()
        refresh_list()
        show_toast(page, "Usuário removido.")

    refresh_list()
    view = ft.View(
        route="/usuarios",
        appbar=ft.AppBar(title=ft.Text("Usuários"), center_title=True,
                         leading=ft.IconButton(ft.Icons.ARROW_BACK, on_click=lambda e: pop_view(page))),
        controls=[
            ft.Container(
                content=ft.Column([
                    ft.Row([nome, senha], expand=True),
                    ft.Row([ft.ElevatedButton("Adicionar", on_click=adicionar),
                            ft.OutlinedButton("Remover", on_click=remover)],
                           alignment=ft.MainAxisAlignment.END),
                    lista
                ], tight=True, scroll=ft.ScrollMode.AUTO),
                padding=20, expand=True
            )
        ]
    )
    push_view(page, view)

# ====== Resultados (Tabs) ======
class ResultadoTabs:
    def __init__(self) -> None:
        self.tabs = ft.Tabs(selected_index=0, animation_duration=200, tabs=[], height=360, expand=True)
        self._datasets: Dict[str, Dict[str, Any]] = {}

    def limpar(self) -> None:
        self.tabs.tabs = []
        self._datasets.clear()
        self.tabs.update()

    def adicionar_tabela(self, titulo: str, colunas: List[str], dados: List[tuple]) -> None:
        dados_safe = list(dados)
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

        dt = ft.DataTable(columns=columns, rows=rows, divider_thickness=0)

        # Scroll horizontal (Row) + vertical (Column)

        conteudo = ft.Container(

            content=ft.Row(

                controls=[

                    ft.Column(

                        controls=[dt],

                        scroll=ft.ScrollMode.AUTO,  # vertical

                        expand=True,

                    )

                ],

                scroll=ft.ScrollMode.AUTO,          # horizontal

                expand=True,

            ),

            expand=True, padding=0

        )
        for t in self.tabs.tabs:
            if t.text == titulo:
                t.content = conteudo
                break
        self.tabs.update()

# ====== Executor SQL ======
    def build_tabs_clone(self) -> ft.Tabs:
        """Cria um clone das abas de resultados (somente leitura), preservando selected_index e estética."""
        # Recria as abas a partir do _datasets
        tabs_list: list[ft.Tab] = []
        # Ordena por ordem atual das tabs visíveis
        current_order = [t.text for t in self.tabs.tabs] if getattr(self.tabs, "tabs", None) else list(self._datasets.keys())
        for titulo in current_order:
            info = self._datasets.get(titulo, {})
            colunas = list(info.get("colunas", []))
            dados = list(info.get("dados", []))

            columns = [ft.DataColumn(ft.Text(c)) for c in colunas]
            rows: list[ft.DataRow] = []
            for row in dados:
                cells = [ft.DataCell(ft.Text("" if v is None else str(v))) for v in row]
                rows.append(ft.DataRow(cells=cells))

            dt = ft.DataTable(columns=columns, rows=rows, divider_thickness=0)

            # Scroll horizontal (Row) + vertical (Column), igual ao _render_table atual
            conteudo = ft.Container(
                content=ft.Row(
                    controls=[
                        ft.Column(
                            controls=[dt],
                            scroll=ft.ScrollMode.AUTO,  # vertical
                            expand=True,
                        )
                    ],
                    scroll=ft.ScrollMode.AUTO,      # horizontal
                    expand=True,
                ),
                expand=True,
                padding=0,
            )
            tabs_list.append(ft.Tab(text=titulo, content=conteudo))

        clone = ft.Tabs(selected_index=getattr(self.tabs, "selected_index", 0),
                        animation_duration=200, tabs=tabs_list, expand=True)
        return clone

class ExecutorSQL:
    def __init__(self, page: ft.Page, resultados: ResultadoTabs, logger_txt: ft.Text) -> None:
        self.page = page
        self.resultados = resultados
        self.txt_log = logger_txt
        self.pool = ThreadPoolExecutor(max_workers=6)
        self._lock = threading.Lock()

    def log(self, msg: str) -> None:
        with self._lock:
            self.txt_log.value = (self.txt_log.value or "") + msg
        try:
            self.txt_log.update()
        except Exception:
            pass

    def executar(self, conexoes: List[Dict[str, Any]], usuario: str, senha: str, sql: str) -> None:
        if not conexoes:
            dialog_alert(self.page, "Aviso", "Nenhuma conexão selecionada.")
            return
        if not sql or not sql.strip():
            dialog_alert(self.page, "Aviso", "Digite um comando SQL.")
            return
        self.resultados.limpar()
        self.log(f"Executando em {len(conexoes)} conexão(ões)...\n")
        for cx in conexoes:
            self.pool.submit(self._run_sql, cx, usuario, senha, sql)

    def testar(self, conexoes: List[Dict[str, Any]], usuario: str, senha: str) -> None:
        if not conexoes:
            dialog_alert(self.page, "Aviso", "Nenhuma conexão selecionada para testar.")
            return
        self.log(f"Testando {len(conexoes)} conexão(ões)...\n")
        for cx in conexoes:
            self.pool.submit(self._test_conn, cx, usuario, senha)

    def _test_conn(self, cx: Dict[str, Any], user: str, senha: str) -> None:
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=cx["host"],
                port=int(cx["port"]),
                user=user,
                password=senha,
                database=cx["database"],
                connect_timeout=4,
            )
            conn.close()
            self.log(f"✅ [{cx.get('nome','?')}] OK\n")
        except Exception as e:
            self.log(f"❌ [{cx.get('nome','?')}] Erro: {e}\n")

    def _run_sql(self, cx: Dict[str, Any], user: str, senha: str, sql: str) -> None:
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=cx["host"],
                port=int(cx["port"]),
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
                self.resultados.adicionar_tabela(cx.get("nome","?"), colunas, dados)
            conn.commit()
            cur.close()
            conn.close()
            self.log(f"✅ [{cx.get('nome','?')}] Comando executado\n")
        except Exception as e:
            self.log(f"❌ [{cx.get('nome','?')}] Erro: {e}\n")

# ====== Main View ======
class MainView:
    def __init__(self, page: ft.Page) -> None:
        self.page = page
        self.repo = Repo()

        # Estado de seleção (multi)
        self.sel_indices: set[int] = set()

        # UI esquerda (conexões)
        self.lst_con = ft.ListView(expand=False, spacing=8, auto_scroll=False, height=360)
        self.btn_add = ft.FilledButton("Adicionar", icon=ft.Icons.ADD, on_click=lambda e: self._adicionar_conexao())
        self.btn_edit = ft.ElevatedButton("Editar", icon=ft.Icons.EDIT, on_click=lambda e: self._editar_conexao())
        self.btn_del = ft.OutlinedButton("Excluir", icon=ft.Icons.DELETE, on_click=lambda e: self._excluir_conexao())
        self.btn_users = ft.OutlinedButton("Usuários", icon=ft.Icons.PEOPLE, on_click=lambda e: self._gerenciar_usuarios())

        self.btn_sel_all = ft.TextButton("Selecionar tudo", on_click=lambda e: self._selecionar_tudo())
        self.btn_clear_sel = ft.TextButton("Limpar seleção", on_click=lambda e: self._limpar_selecao())
        self.btn_test = ft.OutlinedButton("Testar selecionadas", icon=ft.Icons.CHECK_CIRCLE, on_click=lambda e: self._testar())
        self._refresh_con_list()

        # UI direita (execução)
        self.usuario_combo = ft.Dropdown(label="Usuário executor", options=[], value=None)
        self._refresh_usuarios_combo()

        self.sql_input = ft.TextField(label="Comando SQL", multiline=True, min_lines=6, max_lines=12, expand=True)
        self.btn_exec = ft.FilledButton("Executar SQL", icon=ft.Icons.PLAY_ARROW, on_click=lambda e: self._executar())

        self.txt_log = ft.Text(selectable=True)
        self.result_tabs = ResultadoTabs()
        self.executor = ExecutorSQL(page, self.result_tabs, self.txt_log)

        # Layout
        left = ft.Card(
            ft.Container(
                content=ft.Column([
                    ft.Text("Conexões", size=18, weight=ft.FontWeight.BOLD),
                    ft.Row([self.btn_add, self.btn_edit, self.btn_del, self.btn_users], wrap=True, spacing=8),
                    ft.Row([self.btn_sel_all, self.btn_clear_sel, self.btn_test], wrap=True, spacing=8),
                    self.lst_con
                ], tight=True, scroll=ft.ScrollMode.AUTO),
                padding=16
            )
        )

        right = ft.Column([
            ft.Card(ft.Container(content=ft.Column([
                ft.Row([self.usuario_combo, self.btn_exec], alignment=ft.MainAxisAlignment.END),
                self.sql_input,
            ], tight=True), padding=16)),
            ft.Card(ft.Container(content=ft.Column([
                ft.Text("Log", weight=ft.FontWeight.BOLD),
                ft.Container(self.txt_log, border_radius=8, padding=10, height=160),
            ], tight=True), padding=16)),
            ft.Card(ft.Container(content=ft.Column([
                ft.Row([ft.Text("Resultados (SELECT)", weight=ft.FontWeight.BOLD), ft.IconButton(ft.Icons.FULLSCREEN, tooltip="Abrir em tela cheia", on_click=lambda e: self._abrir_tela_cheia())], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
            self.result_tabs.tabs
            ], tight=True), padding=16)),
        ], expand=True)

        self.root = ft.ResponsiveRow([
            ft.Column([left], col={"xs": 12, "sm": 12, "md": 5, "lg": 4, "xl": 4}),
            ft.Column([right], col={"xs": 12, "sm": 12, "md": 7, "lg": 8, "xl": 8}),
        ])

    # ===== Conexões =====
    def _refresh_con_list(self) -> None:
        items = []
        for idx, c in enumerate(self.repo.conexoes):
            selected = (idx in self.sel_indices)
            bg = ft.Colors.PRIMARY_CONTAINER if hasattr(ft,"Colors") and selected else None
            border = ft.border.all(1, ft.Colors.PRIMARY) if selected and hasattr(ft,"Colors") else None
            cont = ft.Container(
                bgcolor=bg, border=border, border_radius=8, padding=12,
                content=ft.Column([
                    ft.Text(c.get("nome","(sem nome)"), size=16, weight=ft.FontWeight.W_600),
                    ft.Text(f"{c.get('host','')}:{c.get('port','')} — {c.get('database','')}", size=12),
                ], tight=True),
                on_click=lambda e, i=idx: self._toggle_sel(i)
            )
            items.append(cont)
        self.lst_con.controls = items
        self.page.update()

    def _toggle_sel(self, idx: int):
        if idx in self.sel_indices:
            self.sel_indices.remove(idx)
        else:
            self.sel_indices.add(idx)
        self._refresh_con_list()

    def _selecionar_tudo(self):
        self.sel_indices = set(range(len(self.repo.conexoes)))
        self._refresh_con_list()

    def _limpar_selecao(self):
        self.sel_indices.clear()
        self._refresh_con_list()

    def _adicionar_conexao(self) -> None:
        open_conexao_view(self.page, "Nova conexão", None,
                          lambda dados: (self.repo.add_conexao(dados), self._refresh_con_list()))

    def _editar_conexao(self) -> None:
        if not self.sel_indices:
            dialog_alert(self.page, "Aviso", "Selecione ao menos uma conexão para editar (a primeira selecionada será usada).")
            return
        idx = sorted(self.sel_indices)[0]
        dados = self.repo.conexoes[idx]
        open_conexao_view(self.page, "Editar conexão", dados,
                          lambda d: (self.repo.update_conexao(idx, d), self._refresh_con_list()))

    def _excluir_conexao(self) -> None:
        if not self.sel_indices:
            dialog_alert(self.page, "Aviso", "Selecione ao menos uma conexão para excluir.")
            return
        nomes = [self.repo.conexoes[i].get("nome","(sem nome)") for i in sorted(self.sel_indices)]
        confirm = ft.AlertDialog(
            title=ft.Text("Confirmar exclusão"),
            content=ft.Text("Excluir as conexões:\n- " + "\n- ".join(nomes)),
            actions_alignment=ft.MainAxisAlignment.END
        )
        def cancelar(_e=None):
            confirm.open = False
            self.page.update()
        def confirmar(_e=None):
            confirm.open = False
            self.page.update()
            for i in sorted(self.sel_indices, reverse=True):
                self.repo.del_conexao(i)
            self.sel_indices.clear()
            self._refresh_con_list()
            show_toast(self.page, "Conexões excluídas.")
        confirm.actions = [ft.TextButton("Cancelar", on_click=cancelar),
                           ft.FilledButton("Excluir", on_click=confirmar)]
        self.page.dialog = confirm
        confirm.open = True
        self.page.update()

    def _gerenciar_usuarios(self) -> None:
        open_usuarios_view(self.page, self.repo, on_change=self._refresh_usuarios_combo)

    # ===== Execução =====
    def _refresh_usuarios_combo(self) -> None:
        self.usuario_combo.options = [ft.dropdown.Option(u["nome"]) for u in self.repo.usuarios]
        if self.repo.usuarios and (self.usuario_combo.value is None):
            self.usuario_combo.value = self.repo.usuarios[0]["nome"]
        try:
            self.usuario_combo.update()
        except Exception:
            pass

    def _get_credenciais(self) -> Optional[Dict[str,str]]:
        if not self.usuario_combo.value:
            dialog_alert(self.page, "Aviso", "Selecione um usuário executor.")
            return None
        cred = next((u for u in self.repo.usuarios if u["nome"] == self.usuario_combo.value), None)
        if not cred:
            dialog_alert(self.page, "Erro", "Usuário não encontrado.")
            return None
        senha = descriptografar(cred.get("senha",""))
        return {"usuario": cred["nome"], "senha": senha}

    def _selecionadas(self) -> List[Dict[str,Any]]:
        return [self.repo.conexoes[i] for i in sorted(self.sel_indices)]

    def _executar(self) -> None:
        creds = self._get_credenciais()
        if not creds:
            return
        sql = self.sql_input.value or ""
        self.executor.executar(self._selecionadas(), creds["usuario"], creds["senha"], sql)

    def _testar(self) -> None:
        creds = self._get_credenciais()
        if not creds:
            return
        self.executor.testar(self._selecionadas(), creds["usuario"], creds["senha"])


    def _abrir_tela_cheia(self) -> None:
        
        try:
            # Clona as abas dos resultados, preservando selected_index
            if not self.result_tabs.tabs.tabs:
                dialog_alert(self.page, "Aviso", "Não há resultados para exibir em tela cheia.")
                return
            tabs_clone = self.result_tabs.build_tabs_clone()

            header = ft.Row(
                [
                    ft.Text("Resultados (SELECT)", weight=ft.FontWeight.BOLD),
                    ft.IconButton(ft.Icons.FULLSCREEN_EXIT, tooltip="Fechar tela cheia", on_click=lambda e: pop_view(self.page)),
                ],
                alignment=ft.MainAxisAlignment.SPACE_BETWEEN
            )

            view = ft.View(
                route="/fullscreen",
                appbar=ft.AppBar(
                    title=ft.Text("Resultados (SELECT)"),
                    center_title=False,
                    leading=ft.IconButton(ft.Icons.ARROW_BACK, on_click=lambda e: pop_view(self.page)),
                    actions=[ft.IconButton(ft.Icons.FULLSCREEN_EXIT, tooltip="Fechar", on_click=lambda e: pop_view(self.page))]
                ),
                controls=[
                    ft.Container(
                        content=ft.Column([
                            header,
                            ft.Container(content=tabs_clone, expand=True),
                        ], expand=True),
                        expand=True, padding=16
                    )
                ]
            )
            push_view(self.page, view)
        except Exception as ex:
            dialog_alert(self.page, "Erro", f"Falha ao abrir tela cheia: {ex}")



def main(page: ft.Page):
    page.title = "Executor PostgreSQL Multi-Conexões - Flet"
    page.theme_mode = ft.ThemeMode.SYSTEM
    page.window_min_width = 1000
    page.window_min_height = 720
    page.padding = 12
    page.scroll = ft.ScrollMode.AUTO

    ui = MainView(page)
    page.add(ui.root)

if __name__ == "__main__":
    ft.app(target=main)
