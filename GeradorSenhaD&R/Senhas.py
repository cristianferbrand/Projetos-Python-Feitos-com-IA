import math
import calendar
import json
import sys
from pathlib import Path
from datetime import datetime, date

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
#  Lógica das senhas (espelhando o C#)
# ----------------------------------------------------
def _normalizar_data(data):
    """Garante que vamos trabalhar sempre com datetime."""
    if data is None:
        return datetime.now()
    if isinstance(data, datetime):
        return data
    if isinstance(data, date):
        return datetime(data.year, data.month, data.day)
    if isinstance(data, str):
        # tenta ISO (AAAA-MM-DD), depois BR (DD/MM/AAAA)
        try:
            return datetime.fromisoformat(data)
        except ValueError:
            try:
                return datetime.strptime(data, "%d/%m/%Y")
            except ValueError:
                return datetime.now()
    return datetime.now()


def _gera_senha_generica(const1, const2, const3, const4, const5, data=None) -> str:
    data = _normalizar_data(data)

    dia = data.day
    mes = data.month
    ano = data.year

    n = dia * const1
    n = math.pow((n / const2), dia)
    n = (n * mes) / const3
    n = (((n / 56.18) + mes) / const4)
    n = (n / ano) * const5

    # Aproxima o comportamento do double.ToString() do C# (15 dígitos significativos)
    s = format(n, ".15g").replace(".", "").replace(",", "")

    # Garante pelo menos 8 caracteres
    if len(s) < 8:
        s = s.ljust(8, "0")

    # resultado.Substring(2, 6)  -> começa no índice 2, pega 6 caracteres
    return s[2:8]


def gerar_senha_diaria(data=None) -> str:
    return _gera_senha_generica(
        const1=45.81,
        const2=7.25,
        const3=34.59,
        const4=23.46,
        const5=9.74,
        data=data,
    )


def gerar_senha_restrita(data=None) -> str:
    return _gera_senha_generica(
        const1=47.17,
        const2=9.21,
        const3=23.71,
        const4=37.19,
        const5=3.27,
        data=data,
    )


# ----------------------------------------------------
#  UI em Flet – widget flutuante com calendário (pt-BR)
# ----------------------------------------------------
def main(page: ft.Page):
    page.title = "Senhas do Dia"
    page.theme_mode = ft.ThemeMode.DARK  # texto claro em card escuro

    # --- Locale pt-BR para controles ---
    try:
        page.locale_configuration = ft.LocaleConfiguration(
            supported_locales=[ft.Locale("pt", "BR")],
            current_locale=ft.Locale("pt", "BR"),
        )
    except AttributeError:
        pass

    # Base dir para guardar config/settings.json (funciona em .py e .exe)
    try:
        if getattr(sys, "frozen", False):
            BASE_DIR = Path(sys.executable).resolve().parent
        else:
            BASE_DIR = Path(__file__).resolve().parent
    except NameError:
        BASE_DIR = Path(".").resolve()

    config_dir = BASE_DIR / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    settings_path = config_dir / "settings.json"

    # Tamanhos
    ICON_WINDOW_SIZE = 90           # janela no modo ícone (bolinha)
    ICON_DIAMETER = 72              # diâmetro da bolinha HOS
    WIDTH_EXPANDED = 340            # largura do app expandido
    HEIGHT_COMPACT = 240            # altura sem calendário
    HEIGHT_COM_CAL = 500            # altura com calendário aberto

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

    hoje = datetime.now()

    # Estado: data selecionada / mês/ano do calendário / visibilidade / expandido
    estado_data = {
        "value": None,              # datetime ou None
        "mes": hoje.month,
        "ano": hoje.year,
        "cal_visivel": False,
        "expandido": False,
    }

    # ------------------------------------------------
    # Presets de cor para o CARD das senhas
    # (inclui as principais do arco-íris)
    # ------------------------------------------------
    CARD_COLOR_PRESETS = {
        "Padrão (Dark)": op(0.96, C.BLUE_GREY_900),
        "Branco": op(0.98, C.WHITE),
        "Transparente": C.TRANSPARENT,
        "Dark sólido": C.BLACK,
        # Arco-íris:
        "Vermelho": op(0.96, C.RED),
        "Laranja": op(0.96, C.ORANGE),
        "Amarelo": op(0.96, C.AMBER),
        "Verde": op(0.96, C.GREEN),
        "Azul": op(0.96, C.BLUE),
        "Anil": op(0.96, C.INDIGO),
        "Violeta": op(0.96, C.DEEP_PURPLE),
    }
    DEFAULT_CARD_COLOR_KEY = "Padrão (Dark)"

    # Para cada tema, definimos se o texto deve ser escuro (True) ou claro (False)
    CARD_USE_DARK_TEXT = {
        "Padrão (Dark)": False,
        "Branco": True,
        "Transparente": False,  # assume fundo escuro atrás
        "Dark sólido": False,
        "Vermelho": False,
        "Laranja": True,
        "Amarelo": True,
        "Verde": False,
        "Azul": False,
        "Anil": False,
        "Violeta": False,
    }

    # Carrega último tema salvo (se existir)
    selected_color_key = DEFAULT_CARD_COLOR_KEY
    if settings_path.exists():
        try:
            data_cfg = json.loads(settings_path.read_text(encoding="utf-8"))
            key = data_cfg.get("card_color_key")
            if key in CARD_COLOR_PRESETS:
                selected_color_key = key
        except Exception:
            pass

    # --- Fecha com ESC ---
    def on_key(e: ft.KeyboardEvent):
        if e.key == "Escape":
            page.window.close()

    page.on_keyboard_event = on_key

    # --- Textos das senhas ---
    label_diaria = ft.Text(
        "Senha Diária:",
        size=12,
        weight=ft.FontWeight.BOLD,
        opacity=0.9,
    )
    senha_diaria_text = ft.Text(
        value="",
        size=18,
        weight=ft.FontWeight.BOLD,
    )

    label_restrita = ft.Text(
        "Senha Restrita:",
        size=12,
        weight=ft.FontWeight.BOLD,
        opacity=0.9,
    )
    senha_restrita_text = ft.Text(
        value="",
        size=18,
        weight=ft.FontWeight.BOLD,
    )

    # Label da data (dd/mm/aaaa)
    label_data = ft.Text(
        value=f"Data selecionada: {datetime.now().strftime('%d/%m/%Y')}",
        size=10,
        opacity=0.8,
    )
    esc_hint = ft.Text("ESC: Fechar", size=10, opacity=0.7)

    def obter_data_corrente():
        # Se tiver uma data escolhida no calendário, usa ela;
        # senão, usa hoje.
        if estado_data["value"] is not None:
            return estado_data["value"]
        return datetime.now()

    def atualizar_senhas(e=None, usar_data_atual: bool = False):
        """
        - usar_data_atual = True  -> força usar a data atual do computador
        - usar_data_atual = False -> usa a data selecionada (ou hoje se não tiver)
        """
        if usar_data_atual:
            data_ref = datetime.now()
            estado_data["value"] = None  # volta a seguir o "hoje"
            label_data.value = (
                f"Data selecionada: {data_ref.strftime('%d/%m/%Y')}"
            )
            # sincroniza mês/ano do calendário com hoje
            estado_data["mes"] = data_ref.month
            estado_data["ano"] = data_ref.year
        else:
            data_ref = obter_data_corrente()

        senha_diaria_text.value = gerar_senha_diaria(data_ref)
        senha_restrita_text.value = gerar_senha_restrita(data_ref)
        page.update()

    def copiar(valor: str):
        if not valor:
            return
        page.set_clipboard(valor)
        page.snack_bar = ft.SnackBar(
            content=ft.Text("Senha copiada!"),
            bgcolor=op(0.9, C.GREEN),
            behavior=ft.SnackBarBehavior.FLOATING,
            duration=1500,
        )
        page.snack_bar.open = True
        page.update()

    # ------------------------------------------------
    # Calendário custom (abaixo do card)
    # ------------------------------------------------
    MESES_PT = [
        "Janeiro",
        "Fevereiro",
        "Março",
        "Abril",
        "Maio",
        "Junho",
        "Julho",
        "Agosto",
        "Setembro",
        "Outubro",
        "Novembro",
        "Dezembro",
    ]

    calendario_coluna = ft.Column(spacing=4, tight=True)
    calendario_container = ft.Container(
        visible=False,
        padding=8,
        margin=ft.margin.only(top=8),
        border_radius=12,
        bgcolor=op(0.98, C.BLUE_GREY_900),
        content=calendario_coluna,
    )

    def mudar_mes(delta: int):
        mes = estado_data["mes"] + delta
        ano = estado_data["ano"]

        if mes < 1:
            mes = 12
            ano -= 1
        elif mes > 12:
            mes = 1
            ano += 1

        estado_data["mes"] = mes
        estado_data["ano"] = ano
        atualizar_calendario()

    def selecionar_dia(dia: int):
        ano = estado_data["ano"]
        mes = estado_data["mes"]
        data_escolhida = datetime(ano, mes, dia)

        estado_data["value"] = data_escolhida
        label_data.value = (
            f"Data selecionada: {data_escolhida.strftime('%d/%m/%Y')}"
        )

        # Atualiza senhas para a data escolhida
        atualizar_senhas()
        # Atualiza visual do calendário (destacar dia selecionado)
        atualizar_calendario()

    def atualizar_calendario():
        mes = estado_data["mes"]
        ano = estado_data["ano"]

        # Cabeçalho: mês / ano e botões de navegação
        header = ft.Row(
            alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
            vertical_alignment=ft.CrossAxisAlignment.CENTER,
            controls=[
                ft.IconButton(
                    icon=I.CHEVRON_LEFT,
                    icon_size=18,
                    tooltip="Mês anterior",
                    on_click=lambda e: mudar_mes(-1),
                    style=ft.ButtonStyle(
                        padding=0,
                        shape=ft.CircleBorder(),
                    ),
                ),
                ft.Text(
                    f"{MESES_PT[mes - 1]} / {ano}",
                    size=12,
                    weight=ft.FontWeight.BOLD,
                ),
                ft.IconButton(
                    icon=I.CHEVRON_RIGHT,
                    icon_size=18,
                    tooltip="Próximo mês",
                    on_click=lambda e: mudar_mes(1),
                    style=ft.ButtonStyle(
                        padding=0,
                        shape=ft.CircleBorder(),
                    ),
                ),
            ],
        )

        # Cabeçalho de dias da semana
        dias_semana_row = ft.Row(
            alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
            controls=[
                ft.Text("D", size=10, opacity=0.8, width=30, text_align=ft.TextAlign.CENTER),
                ft.Text("S", size=10, opacity=0.8, width=30, text_align=ft.TextAlign.CENTER),
                ft.Text("T", size=10, opacity=0.8, width=30, text_align=ft.TextAlign.CENTER),
                ft.Text("Q", size=10, opacity=0.8, width=30, text_align=ft.TextAlign.CENTER),
                ft.Text("Q", size=10, opacity=0.8, width=30, text_align=ft.TextAlign.CENTER),
                ft.Text("S", size=10, opacity=0.8, width=30, text_align=ft.TextAlign.CENTER),
                ft.Text("S", size=10, opacity=0.8, width=30, text_align=ft.TextAlign.CENTER),
            ],
        )

        # Cálculo dos dias do mês
        first_weekday, num_days = calendar.monthrange(ano, mes)
        # calendar.monthrange: Monday=0 ... Sunday=6
        # Vamos começar no domingo (coluna 0)
        col_start = (first_weekday + 1) % 7

        hoje_date = datetime.now().date()
        selected_date = (
            estado_data["value"].date() if estado_data["value"] else None
        )

        rows = []
        current_row_controls = []

        # Preenche espaços em branco antes do primeiro dia
        for _ in range(col_start):
            current_row_controls.append(
                ft.Container(width=30, height=30)
            )

        for dia in range(1, num_days + 1):
            data_dia = date(ano, mes, dia)
            is_selected = selected_date == data_dia
            is_today = hoje_date == data_dia

            bg = None
            fg = None
            if is_selected:
                bg = op(0.95, C.BLUE)
                fg = C.WHITE
            elif is_today:
                bg = op(0.25, C.BLUE)

            btn = ft.TextButton(
                text=str(dia),
                width=30,
                height=30,
                style=ft.ButtonStyle(
                    padding=0,
                    shape=ft.RoundedRectangleBorder(radius=8),
                    bgcolor=bg,
                    color=fg,
                ),
                on_click=lambda e, d=dia: selecionar_dia(d),
            )
            current_row_controls.append(btn)

            if len(current_row_controls) == 7:
                rows.append(
                    ft.Row(
                        controls=current_row_controls,
                        alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                    )
                )
                current_row_controls = []

        # Completa a última linha com espaços vazios, se necessário
        if current_row_controls:
            while len(current_row_controls) < 7:
                current_row_controls.append(
                    ft.Container(width=30, height=30)
                )
            rows.append(
                ft.Row(
                    controls=current_row_controls,
                    alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                )
            )

        calendario_coluna.controls = [header, dias_semana_row, *rows]
        page.update()

    def abrir_calendario(e):
        if not estado_data["expandido"]:
            return

        # Alterna visibilidade
        estado_data["cal_visivel"] = not estado_data["cal_visivel"]
        calendario_container.visible = estado_data["cal_visivel"]

        if estado_data["cal_visivel"]:
            # Ajusta altura da janela para caber o calendário
            page.window.height = HEIGHT_COM_CAL
            # Sincroniza mês/ano com a data atual ou selecionada
            base = obter_data_corrente()
            estado_data["mes"] = base.month
            estado_data["ano"] = base.year
            atualizar_calendario()
        else:
            # Volta para modo compacto (sem calendário)
            page.window.height = HEIGHT_COMPACT

        page.update()

    # ------------------------------------------------
    # Layout expandido (card + calendário)
    # ------------------------------------------------
    # Botões de cópia
    btn_copy_diaria = ft.IconButton(
        icon=I.CONTENT_COPY,
        icon_size=18,
        tooltip="Copiar Senha Diária",
        on_click=lambda e: copiar(senha_diaria_text.value),
        style=ft.ButtonStyle(
            padding=0,
            shape=ft.RoundedRectangleBorder(radius=8),
        ),
    )

    btn_copy_restrita = ft.IconButton(
        icon=I.CONTENT_COPY,
        icon_size=18,
        tooltip="Copiar Senha Restrita",
        on_click=lambda e: copiar(senha_restrita_text.value),
        style=ft.ButtonStyle(
            padding=0,
            shape=ft.RoundedRectangleBorder(radius=8),
        ),
    )

    linha_diaria = ft.Row(
        spacing=6,
        controls=[
            label_diaria,
            senha_diaria_text,
            btn_copy_diaria,
        ],
        alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
        vertical_alignment=ft.CrossAxisAlignment.CENTER,
    )

    linha_restrita = ft.Row(
        spacing=6,
        controls=[
            label_restrita,
            senha_restrita_text,
            btn_copy_restrita,
        ],
        alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
        vertical_alignment=ft.CrossAxisAlignment.CENTER,
    )

    # Botão minimizar
    botao_minimizar = ft.IconButton(
        icon=I.FULLSCREEN_EXIT,
        icon_size=18,
        tooltip="Recolher para ícone",
        style=ft.ButtonStyle(
            padding=0,
            shape=ft.CircleBorder(),
        ),
    )

    # Botões de atualizar e calendário
    btn_refresh = ft.IconButton(
        icon=I.REFRESH,
        icon_size=18,
        tooltip="Atualizar senhas com a data atual do computador",
        on_click=lambda e: atualizar_senhas(e, usar_data_atual=True),
        style=ft.ButtonStyle(
            padding=0,
            shape=ft.CircleBorder(),
        ),
    )

    btn_calendar = ft.IconButton(
        icon=I.CALENDAR_MONTH,
        icon_size=18,
        tooltip="Mostrar/ocultar calendário",
        on_click=abrir_calendario,
        style=ft.ButtonStyle(
            padding=0,
            shape=ft.CircleBorder(),
        ),
    )

    # Dropdown para escolher a cor do container das senhas
    dropdown_cor_card = ft.Dropdown(
        label="Tema do painel",
        value=selected_color_key,
        width=180,
        dense=True,
        options=[
            ft.dropdown.Option("Padrão (Dark)"),
            ft.dropdown.Option("Branco"),
            ft.dropdown.Option("Transparente"),
            ft.dropdown.Option("Dark sólido"),
            ft.dropdown.Option("Vermelho"),
            ft.dropdown.Option("Laranja"),
            ft.dropdown.Option("Amarelo"),
            ft.dropdown.Option("Verde"),
            ft.dropdown.Option("Azul"),
            ft.dropdown.Option("Anil"),
            ft.dropdown.Option("Violeta"),
        ],
        border_radius=12,
        border_color=op(0.6, C.GREY_600),
    )

    # Linha de controles superior (data, atualizar, calendário, minimizar)
    linha_controles = ft.Row(
        spacing=8,
        controls=[
            ft.Row(
                spacing=4,
                controls=[
                    btn_refresh,
                    btn_calendar,
                ],
            ),
            ft.Column(
                spacing=0,
                controls=[
                    label_data,
                    esc_hint,
                ],
            ),
            botao_minimizar,
        ],
        alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
        vertical_alignment=ft.CrossAxisAlignment.CENTER,
    )

    # Linha de tema – bem visível dentro do card
    linha_tema = ft.Row(
        spacing=8,
        controls=[
            ft.Icon(I.PALETTE, size=18),
            dropdown_cor_card,
        ],
        alignment=ft.MainAxisAlignment.START,
        vertical_alignment=ft.CrossAxisAlignment.CENTER,
    )

    # Card principal (vamos ajustar cor via função abaixo)
    card = ft.Container(
        padding=16,
        border_radius=18,
        bgcolor=CARD_COLOR_PRESETS[selected_color_key],
        content=ft.Column(
            spacing=8,
            tight=True,
            controls=[
                linha_diaria,
                linha_restrita,
                linha_controles,
                linha_tema,
            ],
        ),
    )

    # Lista de textos e ícones que precisam mudar conforme o tema
    textos_tema = [
        label_diaria,
        senha_diaria_text,
        label_restrita,
        senha_restrita_text,
        label_data,
        esc_hint,
    ]
    icones_tema = [
        btn_copy_diaria,
        btn_copy_restrita,
        btn_refresh,
        btn_calendar,
        botao_minimizar,
    ]
    # Ícone da paleta também muda de cor
    palette_icon = linha_tema.controls[0]
    icones_tema.append(palette_icon)

    # ----- Função para mudar a cor do CARD e harmonizar texto/botões -----
    def atualizar_cor_card(e=None):
        if e is not None and hasattr(e, "control") and e.control is not None:
            chave = e.control.value
        else:
            chave = dropdown_cor_card.value or selected_color_key

        if chave not in CARD_COLOR_PRESETS:
            chave = DEFAULT_CARD_COLOR_KEY

        card.bgcolor = CARD_COLOR_PRESETS[chave]

        # Decide cor do texto (claro/escuro)
        use_dark = CARD_USE_DARK_TEXT.get(chave, False)
        fg = C.BLACK if use_dark else C.WHITE

        # Ajusta textos
        for t in textos_tema:
            t.color = fg

        # Ajusta ícones
        for b in icones_tema:
            # IconButton ou Icon
            if isinstance(b, ft.IconButton):
                b.icon_color = fg
            elif isinstance(b, ft.Icon):
                b.color = fg

        # Dropdown: texto/label
        dropdown_cor_card.color = fg
        dropdown_cor_card.border_color = op(0.6, fg)

        # Se for transparente, coloca uma borda leve pra manter o contorno
        if chave == "Transparente":
            card.border = ft.border.all(1, op(0.7, fg))
        else:
            card.border = None

        # Salva em settings.json
        try:
            settings_path.write_text(
                json.dumps({"card_color_key": chave}, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception:
            pass

        page.update()

    # Ligar evento de mudança do dropdown ao tema
    dropdown_cor_card.on_change = atualizar_cor_card

    # Card é a área de arrasto da janela no modo expandido
    draggable_card = ft.WindowDragArea(
        content=card,
        maximizable=False,
    )

    # Coluna do conteúdo expandido: card + calendário
    conteudo_expandido = ft.Column(
        spacing=0,
        controls=[
            draggable_card,
            calendario_container,
        ],
        scroll=ft.ScrollMode.ADAPTIVE,
        visible=False,
    )

    # ------------------------------------------------
    # Modo ícone (bolinha com símbolo da HOS)
    # ------------------------------------------------
    hos_logo = ft.Column(
        spacing=0,
        alignment=ft.MainAxisAlignment.CENTER,
        horizontal_alignment=ft.CrossAxisAlignment.CENTER,
        controls=[
            ft.Text(
                "HOS",
                size=14,
                weight=ft.FontWeight.BOLD,
                color=C.WHITE,
                text_align=ft.TextAlign.CENTER,
            ),
            ft.Text(
                "SENHAS",
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
        bgcolor=op(0.95, C.RED),
        border_radius=ICON_DIAMETER / 2,
        alignment=ft.alignment.center,
        content=hos_logo,
    )

    icone_drag = ft.WindowDragArea(
        content=icone_hos,
        maximizable=False,
    )

    # ------------------------------------------------
    # Função para alternar entre ícone e app expandido
    # ------------------------------------------------
    def alternar_expandido(e=None):
        estado_data["expandido"] = not estado_data["expandido"]
        expandido = estado_data["expandido"]

        if expandido:
            # Expandir para o app completo
            icone_drag.visible = False
            conteudo_expandido.visible = True
            page.window.width = WIDTH_EXPANDED
            # altura depende se o calendário já está visível
            if estado_data["cal_visivel"]:
                page.window.height = HEIGHT_COM_CAL
            else:
                page.window.height = HEIGHT_COMPACT
        else:
            # Voltar para modo ícone
            estado_data["cal_visivel"] = False
            calendario_container.visible = False
            conteudo_expandido.visible = False
            icone_drag.visible = True
            page.window.width = ICON_WINDOW_SIZE
            page.window.height = ICON_WINDOW_SIZE

        page.update()

    # liga os cliques
    icone_hos.on_click = alternar_expandido
    botao_minimizar.on_click = alternar_expandido

    # ------------------------------------------------
    # Root: ícone + conteúdo expandido
    # ------------------------------------------------
    root = ft.Column(
        spacing=0,
        controls=[
            icone_drag,
            conteudo_expandido,
        ],
    )

    page.add(root)

    # Aplica cor inicial do card (carregada do settings.json ou padrão)
    atualizar_cor_card()

    # Calcula as senhas ao abrir (sempre usando a data atual do computador)
    atualizar_senhas(usar_data_atual=True)


if __name__ == "__main__":
    ft.app(target=main)