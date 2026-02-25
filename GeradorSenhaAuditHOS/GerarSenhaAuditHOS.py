import hashlib
from datetime import datetime
from pathlib import Path
import sys

import flet as ft

# ----------------------------------------------------
# Shim de compatibilidade (C, I e op()) – padrão Cristianfer
# ----------------------------------------------------
try:
    C = ft.Colors  # versões novas
except AttributeError:
    C = ft.colors  # fallback

try:
    I = ft.Icons
except AttributeError:
    I = ft.icons


def op(alpha: float, base_color: str) -> str:
    """Aplica opacidade em uma cor, com fallback."""
    try:
        return ft.colors.with_opacity(alpha, base_color)
    except Exception:
        return base_color


# ----------------------------------------------------
#  Lógica da senha do dia (HASH SHA-256)
# ----------------------------------------------------
def gerar_senha_dia_hash(data: datetime | None = None) -> str:
    """
    Gera a senha do dia com base na data (YYYY-MM-DD),
    usando SHA-256 e pegando os 6 primeiros caracteres do hash
    em minúsculo.
    """
    if data is None:
        data = datetime.now()
    data_str = data.strftime("%Y-%m-%d")
    hash_senha = hashlib.sha256(data_str.encode()).hexdigest()[:6]
    return hash_senha.lower()  # garante minúsculo


# ----------------------------------------------------
#  UI em Flet – mesmo tamanho / layout parecido com SenhasHOS
# ----------------------------------------------------
def main(page: ft.Page):
    page.title = "Senha do Dia (AuditHOS)"
    page.theme_mode = ft.ThemeMode.DARK

    # --- Locale pt-BR (se disponível) ---
    try:
        page.locale_configuration = ft.LocaleConfiguration(
            supported_locales=[ft.Locale("pt", "BR")],
            current_locale=ft.Locale("pt", "BR"),
        )
    except AttributeError:
        pass

    # Base dir (caso queira reaproveitar pra config no futuro)
    try:
        if getattr(sys, "frozen", False):
            BASE_DIR = Path(sys.executable).resolve().parent
        else:
            BASE_DIR = Path(__file__).resolve().parent
    except NameError:
        BASE_DIR = Path(".").resolve()

    # Tamanhos – replicando o app de senhas
    ICON_WINDOW_SIZE = 90           # janela no modo ícone (bolinha)
    ICON_DIAMETER = 72              # diâmetro da bolinha
    WIDTH_EXPANDED = 340            # largura do app expandido
    HEIGHT_EXPANDED = 260           # altura fixa no modo expandido

    # --- Config da janela (começa como ícone) ---
    page.window.always_on_top = True
    page.window.frameless = True
    page.window.title_bar_hidden = True
    page.window.skip_task_bar = True
    page.window.shadow = True
    page.window.resizable = False

    page.window.width = ICON_WINDOW_SIZE
    page.window.height = ICON_WINDOW_SIZE

    page.window.center()
    page.window.top = 20  # um pouco abaixo do topo

    # Janela transparente: só o conteúdo aparece
    try:
        page.window.bgcolor = C.TRANSPARENT
    except Exception:
        page.window_bgcolor = C.TRANSPARENT
    page.bgcolor = C.TRANSPARENT

    # --- Fecha com ESC ---
    def on_key(e: ft.KeyboardEvent):
        if e.key == "Escape":
            page.window.close()

    page.on_keyboard_event = on_key

    # Estado: se está expandido ou só ícone
    estado_expandido = {"value": False}

    # ------------------------------------------------
    # Textos / controles principais (modo expandido)
    # ------------------------------------------------
    label_senha = ft.Text(
        "Senha do dia (AuditHOS):",
        size=12,
        weight=ft.FontWeight.BOLD,
        opacity=0.9,
    )

    senha_text = ft.Text(
        value="",
        size=26,
        weight=ft.FontWeight.BOLD,
    )

    label_data = ft.Text(
        value=f"Data selecionada: {datetime.now().strftime('%d/%m/%Y')}",
        size=10,
        opacity=0.8,
    )

    esc_hint = ft.Text(
        "ESC: Fechar",
        size=10,
        opacity=0.7,
    )

    # Função para atualizar senha (sempre pega data atual do sistema)
    def atualizar_senha(usar_data_atual: bool = True):
        if usar_data_atual:
            data_ref = datetime.now()
        else:
            data_ref = datetime.now()

        senha_text.value = gerar_senha_dia_hash(data_ref)
        label_data.value = f"Data selecionada: {data_ref.strftime('%d/%m/%Y')}"
        page.update()

    # Copiar senha
    def copiar_senha(e):
        if not senha_text.value:
            return
        page.set_clipboard(senha_text.value)
        page.snack_bar = ft.SnackBar(
            content=ft.Text("Senha copiada!"),
            bgcolor=op(0.9, C.GREEN),
            behavior=ft.SnackBarBehavior.FLOATING,
            duration=1500,
        )
        page.snack_bar.open = True
        page.update()

    btn_copy = ft.IconButton(
        icon=I.CONTENT_COPY,
        icon_size=20,
        tooltip="Copiar senha do dia",
        on_click=copiar_senha,
        style=ft.ButtonStyle(
            padding=0,
            shape=ft.RoundedRectangleBorder(radius=8),
        ),
    )

    # Botão refresh (recalcular com a data do computador)
    btn_refresh = ft.IconButton(
        icon=I.REFRESH,
        icon_size=18,
        tooltip="Atualizar senha com a data atual do computador",
        style=ft.ButtonStyle(
            padding=0,
            shape=ft.CircleBorder(),
        ),
    )

    # Botão minimizar (recolher para ícone)
    botao_minimizar = ft.IconButton(
        icon=I.FULLSCREEN_EXIT,
        icon_size=18,
        tooltip="Recolher para ícone",
        style=ft.ButtonStyle(
            padding=0,
            shape=ft.CircleBorder(),
        ),
    )

    # Callback do refresh
    def on_refresh(e):
        atualizar_senha(usar_data_atual=True)

    btn_refresh.on_click = on_refresh

    # Linha da senha (label + valor + copiar)
    linha_senha = ft.Row(
        spacing=6,
        controls=[
            label_senha,
            senha_text,
            btn_copy,
        ],
        alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
        vertical_alignment=ft.CrossAxisAlignment.CENTER,
    )

    # Linha de controles (refresh + minimizar)
    linha_controles = ft.Row(
        spacing=8,
        controls=[
            ft.Row(
                spacing=4,
                controls=[
                    btn_refresh,
                ],
            ),
            botao_minimizar,
        ],
        alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
        vertical_alignment=ft.CrossAxisAlignment.CENTER,
    )

    # Linha de info no rodapé do card
    linha_info = ft.Row(
        spacing=8,
        controls=[
            label_data,
            esc_hint,
        ],
        alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
        vertical_alignment=ft.CrossAxisAlignment.CENTER,
    )

    # Card principal
    card = ft.Container(
        padding=16,
        border_radius=18,
        bgcolor=op(0.96, C.BLUE_GREY_900),
        content=ft.Column(
            spacing=10,
            tight=True,
            controls=[
                linha_senha,
                linha_controles,
                linha_info,
            ],
        ),
    )

    # Área arrastável da janela (card inteiro arrasta)
    draggable_card = ft.WindowDragArea(
        content=card,
        maximizable=False,
    )

    conteudo_expandido = ft.Column(
        spacing=0,
        controls=[
            draggable_card,
        ],
        visible=False,
    )

    # ------------------------------------------------
    # Modo ícone (bolinha com sigla, tamanho igual ao outro app)
    # ------------------------------------------------
    hos_logo = ft.Column(
        spacing=0,
        alignment=ft.MainAxisAlignment.CENTER,
        horizontal_alignment=ft.CrossAxisAlignment.CENTER,
        controls=[
            ft.Text(
                "SENHA",
                size=14,
                weight=ft.FontWeight.BOLD,
                color=C.WHITE,
                text_align=ft.TextAlign.CENTER,
            ),
            ft.Text(
                "AUDITHOS",
                size=10,
                weight=ft.FontWeight.BOLD,
                color=C.WHITE,
                text_align=ft.TextAlign.CENTER,
            ),
        ],
    )

    icone_hos = ft.Container(
        width=ICON_DIAMETER,
        height=ICON_DIAMETER,
        bgcolor=op(0.95, C.BLUE),
        border_radius=ICON_DIAMETER / 2,
        alignment=ft.alignment.center,
        content=hos_logo,
    )

    icone_drag = ft.WindowDragArea(
        content=icone_hos,
        maximizable=False,
    )

    # ------------------------------------------------
    # Alternar entre ícone e expandido
    # ------------------------------------------------
    def alternar_expandido(e=None):
        estado_expandido["value"] = not estado_expandido["value"]
        expandido = estado_expandido["value"]

        if expandido:
            icone_drag.visible = False
            conteudo_expandido.visible = True
            page.window.width = WIDTH_EXPANDED
            page.window.height = HEIGHT_EXPANDED
        else:
            conteudo_expandido.visible = False
            icone_drag.visible = True
            page.window.width = ICON_WINDOW_SIZE
            page.window.height = ICON_WINDOW_SIZE

        page.update()

    icone_hos.on_click = alternar_expandido
    botao_minimizar.on_click = alternar_expandido

    # Root layout
    root = ft.Column(
        spacing=0,
        controls=[
            icone_drag,
            conteudo_expandido,
        ],
    )

    page.add(root)

    # Senha inicial
    atualizar_senha(usar_data_atual=True)

    # Ajusta cores de texto (para o tema escuro fixo)
    fg = C.WHITE
    for t in [label_senha, senha_text, label_data, esc_hint]:
        t.color = fg
    for b in [btn_copy, btn_refresh, botao_minimizar]:
        if isinstance(b, ft.IconButton):
            b.icon_color = fg
    page.update()


if __name__ == "__main__":
    ft.app(target=main)