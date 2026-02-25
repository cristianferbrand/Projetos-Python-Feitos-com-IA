import os
import json
from pathlib import Path
from typing import List, Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from cryptography.fernet import Fernet as FernetT
else:
    class FernetT:
        def decrypt(self, token): ...
        def encrypt(self, data): ...


import flet as ft
# Colors alias compatible with older/newer Flet versions
C = getattr(ft, 'Colors', None)

# ====== Persistência e Criptografia ======
try:
    from cryptography.fernet import Fernet
    _HAS_FERNET = True
except Exception:
    Fernet = None  # type: ignore
    _HAS_FERNET = False

BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = BASE_DIR / "config"
CONFIG_DIR.mkdir(parents=True, exist_ok=True)
CONEXOES_FILE = CONFIG_DIR / "conexoes.json"
USUARIOS_FILE = CONFIG_DIR / "usuarios.json"
KEY_FILE = CONFIG_DIR / "chave.key"

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

def _get_fernet() -> Optional[FernetT]:
    if not _HAS_FERNET:
        return None
    try:
        if not KEY_FILE.exists():
            key = Fernet.generate_key()
            KEY_FILE.write_bytes(key)
        key = KEY_FILE.read_bytes()
        return Fernet(key)
    except Exception:
        return None

_F = _get_fernet()

def criptografar(txt: str) -> str:
    if _F is None:
        return txt  # fallback sem criptografia
    return _F.encrypt(txt.encode()).decode()

def descriptografar(ct: str) -> str:
    if _F is None:
        return ct
    try:
        return _F.decrypt(ct.encode()).decode()
    except Exception:
        return ""

# ====== Helpers UI ======
def show_toast(page: ft.Page, message: str):
    page.snack_bar = ft.SnackBar(content=ft.Text(message))
    page.snack_bar.open = True
    page.update()

def dialog_alert(page: ft.Page, title: str, message: str):
    dlg = ft.AlertDialog(title=ft.Text(title), content=ft.Text(message), actions=[ft.TextButton("OK", on_click=lambda e: close())])
    def close():
        dlg.open = False
        page.update()
    page.dialog = dlg
    dlg.open = True
    page.update()

# ====== Views fullscreen (em vez de modais) ======
def _pop_view(page: ft.Page):
    try:
        if len(page.views) > 1:
            page.views.pop()
            page.update()
    except Exception:
        pass

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
            show_toast(page, "Salvo com sucesso.")
            _pop_view(page)
        except Exception as e:
            dialog_alert(page, "Erro", f"Falha ao salvar: {e}")

    def cancelar(_e=None):
        _pop_view(page)

    view = ft.View(
        route="/conexao",
        appbar=ft.AppBar(title=ft.Text(titulo), center_title=True, leading=ft.IconButton(ft.Icons.ARROW_BACK, on_click=cancelar)),
        controls=[
            ft.Container(
                content=ft.Column([
                    nome, host, port, db,
                    ft.Row(
                        [ft.ElevatedButton("Cancelar", on_click=cancelar),
                         ft.FilledButton("Salvar", on_click=salvar)],
                        alignment=ft.MainAxisAlignment.END
                    )
                ], tight=True, scroll=ft.ScrollMode.AUTO),
                padding=20, expand=True
            )
        ]
    )
    page.views.append(view)
    page.update()

def open_usuarios_view(page: ft.Page, repo: "Repo", on_change=None):
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

    def fechar(_e=None):
        _pop_view(page)

    refresh_list()
    view = ft.View(
        route="/usuarios",
        appbar=ft.AppBar(title=ft.Text("Usuários"), center_title=True, leading=ft.IconButton(ft.Icons.ARROW_BACK, on_click=fechar)),
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
    page.views.append(view)
    page.update()

# ====== Repositório (persistência de conexões e usuários) ======
class Repo:
    def __init__(self) -> None:
        self.conexoes: List[Dict] = _load_json(CONEXOES_FILE, [])
        self.usuarios: List[Dict] = _load_json(USUARIOS_FILE, [])

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

# ====== UI Principal ======
COLOR_BG = (getattr(C, 'SURFACE', None) or '#1E1E1E') if hasattr(ft, "Colors") else "#121212"
COLOR_CARD = (getattr(C, 'SURFACE_VARIANT', None) or getattr(C, 'SURFACE', None) or '#1E1E1E') if hasattr(ft, "Colors") else "#1E1E1E"
COLOR_CARD_SEL = (getattr(C, 'PRIMARY_CONTAINER', None) or getattr(C, 'PRIMARY', None) or '#2B2B2B') if hasattr(ft, "Colors") else "#2A3B7D"

class MainView:
    def __init__(self, page: ft.Page) -> None:
        self.page = page
        self.repo = Repo()
        self.idx_sel: Optional[int] = None

        self.lst_con = ft.ListView(expand=False, spacing=8, auto_scroll=False, height=360)

        # Barra de ações
        self.btn_add = ft.FilledButton("Adicionar", icon=ft.Icons.ADD, on_click=lambda e: self._adicionar_conexao())
        self.btn_edit = ft.ElevatedButton("Editar", icon=ft.Icons.EDIT, on_click=lambda e: self._editar_conexao())
        self.btn_del = ft.OutlinedButton("Excluir", icon=ft.Icons.DELETE, on_click=lambda e: self._excluir_conexao())
        self.btn_users = ft.OutlinedButton("Usuários", icon=ft.Icons.PEOPLE, on_click=lambda e: self._gerenciar_usuarios())

        self._refresh_con_list()

        # Layout
        self.root = ft.ResponsiveRow([
            ft.Column([
                ft.Card(
                    ft.Container(
                        content=ft.Column([
                            ft.Text("Conexões", size=18, weight=ft.FontWeight.BOLD),
                            ft.Row([self.btn_add, self.btn_edit, self.btn_del, self.btn_users], wrap=True, spacing=8),
                            self.lst_con
                        ], tight=True, scroll=ft.ScrollMode.AUTO), 
                        padding=16
                    )
                )
            ], col={"xs": 12, "sm": 12, "md": 6, "lg": 5, "xl": 4}, expand=True),
        ])

    # ===== Listagem/Seleção =====
    def _refresh_con_list(self) -> None:
        items = []
        for idx, c in enumerate(self.repo.conexoes):
            # Um container clicável por conexão
            bg = COLOR_CARD_SEL if self.idx_sel == idx else COLOR_CARD
            cont = ft.Container(
                bgcolor=bg,
                border_radius=8,
                padding=12,
                content=ft.Column([
                    ft.Text(c.get("nome","(sem nome)"), size=16, weight=ft.FontWeight.W_600),
                    ft.Text(f"{c.get('host','')}:{c.get('port','')} — {c.get('database','')}", size=12, color=(getattr(C, 'ON_SURFACE_VARIANT', None) or getattr(C, 'ON_SURFACE', None) or '#FFFFFF') if hasattr(ft,"Colors") else None),
                ], tight=True),
                on_click=lambda e, i=idx: self._select(i)
            )
            items.append(cont)
        self.lst_con.controls = items
        self.page.update()

    def _select(self, idx: int) -> None:
        self.idx_sel = idx
        self._refresh_con_list()

    # ===== Ações =====
    def _adicionar_conexao(self) -> None:
        open_conexao_view(
            self.page,
            "Nova conexão",
            None,
            lambda dados: (self.repo.add_conexao(dados), self._refresh_con_list())
        )

    def _editar_conexao(self) -> None:
        if self.idx_sel is None:
            dialog_alert(self.page, "Aviso", "Clique em uma conexão para editar.")
            return
        dados = self.repo.conexoes[self.idx_sel]
        open_conexao_view(
            self.page,
            "Editar conexão",
            dados,
            lambda d: (self.repo.update_conexao(self.idx_sel, d), self._refresh_con_list())
        )

    def _excluir_conexao(self) -> None:
        if self.idx_sel is None:
            dialog_alert(self.page, "Aviso", "Clique em uma conexão para excluir.")
            return
        nome = self.repo.conexoes[self.idx_sel].get("nome","(sem nome)")
        confirm = ft.AlertDialog(
            title=ft.Text("Confirmar exclusão"),
            content=ft.Text(f"Excluir conexão '{nome}'?"),
            actions_alignment=ft.MainAxisAlignment.END
        )

        def cancelar(_e=None):
            confirm.open = False
            self.page.update()

        def confirmar(_e=None):
            confirm.open = False
            self.page.update()
            self.repo.del_conexao(self.idx_sel)
            self.idx_sel = None
            self._refresh_con_list()
            show_toast(self.page, "Conexão excluída.")

        confirm.actions = [ft.TextButton("Cancelar", on_click=cancelar),
                           ft.FilledButton("Excluir", on_click=confirmar)]
        self.page.dialog = confirm
        confirm.open = True
        self.page.update()

    def _gerenciar_usuarios(self) -> None:
        open_usuarios_view(self.page, self.repo, on_change=lambda: None)

def main(page: ft.Page):
    page.title = "Executor SQL Multi-Conexões (Flet)"
    page.theme_mode = ft.ThemeMode.SYSTEM
    page.window_min_width = 900
    page.window_min_height = 600

    ui = MainView(page)
    page.add(ui.root)

if __name__ == "__main__":

    ft.app(target=main)
