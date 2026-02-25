# path: MapaClienteHOS-Flet.py
# Requisitos:
#   pip install flet plotly pandas
# Observação: estilos do Mapbox que exigem token usam a env "MAPBOX_TOKEN".

from __future__ import annotations

import os
import json
import re
import math
from threading import Event, Thread
from typing import Dict, List, Optional, Tuple

import pandas as pd
import flet as ft
from flet.plotly_chart import PlotlyChart

# ==========================
# Helpers de dados/CSV
# ==========================

def read_csv_semicolon(path: str, required: Optional[List[str]] = None) -> pd.DataFrame:
    """Lê CSV com separador ';' e valida colunas. Lida com UTF-8/latin-1.
    Por quê: muitos dumps BR usam ';' e podem vir em latin-1.
    """
    if not os.path.exists(path):
        raise RuntimeError(f"Arquivo não encontrado: {path}")
    try:
        df = pd.read_csv(path, sep=";", encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_csv(path, sep=";", encoding="latin-1")
    _validate_cols(path, df, required)
    return df


def read_csv_comma(path: str, required: Optional[List[str]] = None) -> pd.DataFrame:
    """Lê CSV com separador ',' e valida colunas. Lida com UTF-8/latin-1."""
    if not os.path.exists(path):
        raise RuntimeError(f"Arquivo não encontrado: {path}")
    try:
        df = pd.read_csv(path, sep=",", encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_csv(path, sep=",", encoding="latin-1")
    _validate_cols(path, df, required)
    return df


def _validate_cols(path: str, df: pd.DataFrame, required: Optional[List[str]]):
    if required:
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise RuntimeError(
                f"{os.path.basename(path)} precisa das colunas {missing}. Encontradas: {list(df.columns)}"
            )


def only_digits(s: object) -> str:
    return re.sub(r"\D", "", str(s)) if pd.notna(s) else ""


def pad7_ibge(s: object) -> Optional[str]:
    d = only_digits(s)
    return d.zfill(7) if d else None


# ==========================
# Helpers de UI/Plotly
# ==========================

def cm_to_px(cm: float) -> int:
    return int(round(cm * 37.795))


def indicador_horizontal(total: int) -> ft.Container:
    return ft.Container(
        height=cm_to_px(3),
        bgcolor=ft.Colors.WHITE,
        padding=12,
        border_radius=12,
        shadow=ft.BoxShadow(
            blur_radius=10,
            spread_radius=1,
            color=ft.Colors.with_opacity(0.08, ft.Colors.BLACK),
        ),
        content=ft.Row(
            alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
            vertical_alignment=ft.CrossAxisAlignment.CENTER,
            controls=[
                ft.Row(
                    spacing=12,
                    vertical_alignment=ft.CrossAxisAlignment.CENTER,
                    controls=[
                        ft.Container(
                            content=ft.Icon(ft.Icons.STORE_MALL_DIRECTORY, size=32, color=ft.Colors.BLUE_600),
                            bgcolor=ft.Colors.BLUE_50,
                            padding=10,
                            border_radius=10,
                        ),
                        ft.Column(
                            spacing=2,
                            tight=True,
                            controls=[
                                ft.Text("Total de Clientes", size=14, color=ft.Colors.GREY_700),
                                ft.Text(f"{total:,}".replace(",", "."), size=36, weight=ft.FontWeight.BOLD, selectable=False),
                            ],
                        ),
                    ],
                ),
                ft.Container(content=ft.Text("visão geral", size=12, color=ft.Colors.GREY_600), padding=ft.padding.all(8)),
            ],
        ),
    )


def make_color_map(estados: List[str]) -> Dict[str, str]:
    # Importa plotly.express sob demanda
    import plotly.express as px
    base = px.colors.qualitative.Vivid + px.colors.qualitative.Bold + px.colors.qualitative.Set3
    palette = (base * ((len(estados) // len(base)) + 1))[: len(estados)]
    return {estado: palette[i] for i, estado in enumerate(estados)}


def _dynamic_bottom_margin(fig: object, items_per_row: int = 10, base: int = 60, per_line: int = 22) -> int:
    names: List[str] = []
    for tr in fig.data:
        nm = getattr(tr, "name", None)
        if nm and nm not in names:
            names.append(nm)
    n = max(1, len(names))
    rows = math.ceil(n / max(1, items_per_row))
    return base + rows * per_line


def criar_mapa(
    merged_df: pd.DataFrame,
    df_totalizado_estado: pd.DataFrame,
    color_map: Dict[str, str],
    estado_selecionado: Optional[str] = None,
    altura: int = 600,
    legend_mode: str = "wide",
    map_style: str = "carto-positron",
    heatmap: bool = False,
    heat_radius: int = 20,
    token: Optional[str] = None,
) -> object:
    # Importa plotly.graph_objects sob demanda
    import plotly.graph_objects as go

    fig = go.Figure()
    estados_ordenados = (
        df_totalizado_estado.sort_values(by="Clientes", ascending=False)["ESTADO"].tolist()
        if df_totalizado_estado is not None and not df_totalizado_estado.empty
        else []
    )

    todos_estados = [
        "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS","MG","PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC","SP","SE","TO"
    ]

    if heatmap:
        all_pts = merged_df[merged_df["lat"].notna() & merged_df["lon"].notna()]
        if not all_pts.empty:
            fig.add_trace(
                go.Densitymapbox(
                    lat=all_pts["lat"],
                    lon=all_pts["lon"],
                    radius=int(heat_radius),
                    colorscale="Turbo",
                    opacity=0.35,
                    showscale=False,
                    name="Densidade",
                )
            )

    ufs_com_traces = set()
    for uf in estados_ordenados:
        estado_data = merged_df[(merged_df["ESTADO"] == uf) & merged_df["lat"].notna() & merged_df["lon"].notna()]
        if estado_data.empty:
            continue
        total_clientes_estado = int(estado_data["FANTASIA"].notna().sum())
        ufs_com_traces.add(uf)
        nome_legenda = f"{uf} ({total_clientes_estado} Farmácias)"
        if estado_selecionado == uf:
            nome_legenda = f"▶ {uf} ({total_clientes_estado} Farmácias) ◀"

        # halo branco para contraste
        fig.add_trace(
            go.Scattermapbox(
                lat=estado_data["lat"],
                lon=estado_data["lon"],
                mode="markers",
                marker=dict(size=12, color="white", opacity=1),
                hoverinfo="skip",
                showlegend=False,
            )
        )
        # marcador colorido
        fig.add_trace(
            go.Scattermapbox(
                lat=estado_data["lat"],
                lon=estado_data["lon"],
                mode="markers",
                marker=dict(size=9, color=color_map.get(uf, "#556")),
                hovertemplate=(
                    "<b>%{customdata[0]}</b><br>UF: %{customdata[1]}<br>Município: %{customdata[2]}<extra></extra>"
                ),
                customdata=estado_data[["FANTASIA", "ESTADO", "CIDADE"]].values,
                name=nome_legenda,
                showlegend=True,
            )
        )

    for uf in todos_estados:
        if uf not in ufs_com_traces:
            fig.add_trace(
                go.Scattermapbox(
                    lat=[0],
                    lon=[0],
                    mode="markers",
                    marker=dict(size=0, color=color_map.get(uf, "#444")),
                    hoverinfo="skip",
                    name=f"{uf} (0 Farmácias)",
                    visible="legendonly",
                    showlegend=True,
                )
            )

    if estado_selecionado and estado_selecionado in merged_df["ESTADO"].dropna().unique().tolist():
        st = merged_df[(merged_df["ESTADO"] == estado_selecionado) & merged_df["lat"].notna() & merged_df["lon"].notna()]
        if len(st):
            center_lat, center_lon, zoom = float(st["lat"].mean()), float(st["lon"].mean()), 5
        else:
            center_lat, center_lon, zoom = -16.0, -54.0, 2.3
    else:
        center_lat, center_lon, zoom = -16.0, -54.0, 2.3

    legend_cfg = dict(
        font=dict(size=10, color="black"),
        orientation="h",
        x=0, xanchor="left",
        y=-0.14 if legend_mode == "wide" else -0.18,
        yanchor="top",
        bgcolor="rgba(255,255,255,0.9)",
        bordercolor="rgba(0,0,0,0.15)",
        borderwidth=1,
        tracegroupgap=6 if legend_mode == "wide" else 4,
        itemsizing="constant",
        itemwidth=32 if legend_mode == "wide" else 26,
    )

    b_dynamic = _dynamic_bottom_margin(
        fig,
        items_per_row=12 if legend_mode == "wide" else 8,
        base=72 if legend_mode == "wide" else 88,
        per_line=22,
    )
    margins = dict(r=12, t=12, l=12, b=b_dynamic)

    mapbox_kwargs = dict(style=map_style, zoom=zoom, center=dict(lat=center_lat, lon=center_lon))
    if token:
        mapbox_kwargs["accesstoken"] = token

    fig.update_layout(
        mapbox=mapbox_kwargs,
        height=altura,
        margin=margins,
        showlegend=True,
        paper_bgcolor="rgba(0,0,0,0)",
        hoverlabel=dict(bgcolor="white", bordercolor="black", font=dict(color="black")),
        legend=legend_cfg,
    )
    return fig


# ==========================
# Threads utilitárias
# ==========================

def safe_call_ui(page: ft.Page, fn) -> None:
    """Tenta chamar a UI a partir de thread. Fallback direto.
    Por quê: evitar update fora da thread principal em alguns ambientes.
    """
    try:
        page.call_from_thread(fn)
    except Exception:
        try:
            fn()
        except Exception:
            pass


class RepeatingJob:
    """Thread com Event e intervalo.
    Por quê: controlar parada e reduzir code-duplication nas automações.
    """

    def __init__(self, job, interval_seconds: int = 10):
        self._job = job
        self._interval = max(1, int(interval_seconds))
        self._stop = Event()
        self._thread: Optional[Thread] = None

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()

    def alive(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    def update_interval(self, seconds: int):
        self._interval = max(1, int(seconds))

    def _run(self):
        while not self._stop.is_set():
            # espera com chance de interrupção
            self._stop.wait(self._interval)
            if self._stop.is_set():
                break
            try:
                self._job()
            except Exception:
                pass


# ==========================
# App Flet
# ==========================

def main(page: ft.Page):
    page.title = "Mapa Clientes HOS - Flet"
    page.theme_mode = ft.ThemeMode.LIGHT
    page.padding = 12
    page.bgcolor = ft.Colors.SURFACE
    page.scroll = ft.ScrollMode.AUTO

    # ---------- estado  ----------
    base = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base, "mapa_cliente_config.json")

    csv_folder = base
    try:
        if os.path.exists(config_path):
            with open(config_path, "r", encoding="utf-8") as _f:
                _cfg = json.load(_f)
                _saved = _cfg.get("csv_folder")
                if _saved and os.path.isdir(_saved):
                    csv_folder = os.path.abspath(_saved)
    except Exception:
        pass

    def make_paths(folder: str) -> Tuple[str, str, str, str]:
        folder = os.path.abspath(folder)
        return (
            os.path.join(folder, "clientes_hos.csv"),
            os.path.join(folder, "clientes_modulos.csv"),
            os.path.join(folder, "clientes_rep.csv"),
            os.path.join(folder, "municipios.csv"),
        )

    p_cli, p_mod, p_rep, p_munis = make_paths(csv_folder)

    MAPBOX_TOKEN = os.getenv("MAPBOX_TOKEN")

    # flags e dados
    map_style_val = {"value": "carto-positron"}
    heat_state = {"on": False, "radius": 20}

    legend_cycle_state = {"phase": "map"}  # map | legend

    clientes_hos_df = clientes_mod_df = clientes_rep_df = municipios_df = None
    merged_df = None
    df_totalizado_estado = df_totalizado_cidade = None
    color_map: Dict[str, str] = {}

    todos_estados_const = [
        "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS","MG","PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC","SP","SE","TO"
    ]

    # ---------- UI controls ----------
    def mapa_height() -> int:
        w = getattr(page, "width", None)
        try:
            w = float(w) if w is not None else 1200.0
        except Exception:
            w = 1200.0
        if w < 600:
            return 420
        if w < 900:
            return 520
        if w < 1200:
            return 580
        return 640

    def legend_mode_by_width() -> str:
        w = getattr(page, "width", None)
        try:
            w = float(w) if w is not None else 1200.0
        except Exception:
            w = 1200.0
        return "wide" if w >= 900 else "narrow"

    top = ft.Container(padding=0, expand=True)

    ddl_estado = ft.Dropdown(
        label="UF",
        options=[ft.dropdown.Option("")] + [ft.dropdown.Option(uf) for uf in sorted(todos_estados_const)],
        value="",
    )
    btn_reset = ft.OutlinedButton("Resetar Mapa", icon=ft.Icons.CENTER_FOCUS_WEAK)

    # Inicializar sem figura para evitar chamadas a to_image em dict
    plot_mapa = PlotlyChart(None, expand=True)

    lv_ufs = ft.ListView(expand=True, spacing=4, padding=0, auto_scroll=False)
    panel_ufs = ft.Container(
        width=260,
        height=420,
        bgcolor=ft.Colors.WHITE,
        border_radius=12,
        padding=12,
        visible=False,
        content=ft.Column([
            ft.Row([ft.Text("UFs e Totais", size=14, weight=ft.FontWeight.BOLD)], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
            lv_ufs,
        ], tight=True, spacing=8),
    )

    lv_ufs_full = ft.ListView(expand=True, spacing=6, padding=0, auto_scroll=False)
    legend_full = ft.Container(
        visible=False,
        bgcolor=ft.Colors.WHITE,
        border_radius=12,
        padding=12,
        content=ft.Column([
            ft.Row([ft.Text("UFs e Totais", size=18, weight=ft.FontWeight.BOLD)], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
            lv_ufs_full,
        ], tight=True, spacing=10),
    )

    def select_uf_from_panel(uf: str):
        ddl_estado.value = uf
        ddl_estado.update()
        refresh_map_only()

    def refresh_ufs_panel():
        if df_totalizado_estado is None or df_totalizado_estado.empty:
            lv_ufs.controls = []
            lv_ufs_full.controls = []
            lv_ufs.update(); lv_ufs_full.update()
            return
        df_sorted = df_totalizado_estado.sort_values("Clientes", ascending=False)
        items = []
        items_full = []
        for _, r in df_sorted.iterrows():
            uf = str(r["ESTADO"]) ; total = int(r["Clientes"]) if pd.notna(r["Clientes"]) else 0
            row = ft.Container(
                content=ft.Row([
                    ft.Text(uf, weight=ft.FontWeight.BOLD),
                    ft.Text(f"{total:,}".replace(",", ".")),
                ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
                padding=8,
                border_radius=8,
                on_click=lambda e, uf=uf: select_uf_from_panel(uf),
                ink=True,
                bgcolor=ft.Colors.with_opacity(0.0, ft.Colors.BLACK),
            )
            items.append(row)
            items_full.append(
                ft.Container(
                    content=ft.Row([
                        ft.Text(uf, weight=ft.FontWeight.BOLD),
                        ft.Text(f"{total:,}".replace(",", ".")),
                    ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
                    padding=10, border_radius=10, bgcolor=ft.Colors.GREY_50,
                )
            )
        lv_ufs.controls = items
        lv_ufs.update()
        lv_ufs_full.controls = items_full
        lv_ufs_full.update()

    btn_exit_full = ft.FloatingActionButton(icon=ft.Icons.FULLSCREEN_EXIT, tooltip="Sair do modo tela cheia", visible=False)
    btn_enter_full = ft.FloatingActionButton(icon=ft.Icons.FULLSCREEN, tooltip="Somente o mapa", visible=True)

    # Aparência do mapa
    ddl_style = ft.Dropdown(
        label="Estilo do mapa",
        value=map_style_val["value"],
        options=[ft.dropdown.Option(v) for v in ["carto-positron", "carto-darkmatter", "open-street-map"]],
        width=220,
    )
    sw_heat = ft.Switch(label="Heatmap", value=bool(heat_state["on"]))
    sl_heat_radius = ft.Slider(min=5, max=50, divisions=9, value=int(heat_state["radius"]), label="{value}", width=220)

    look_card = ft.Container(
        bgcolor=ft.Colors.WHITE,
        border_radius=12,
        padding=14,
        visible=False,
        content=ft.Column([
            ft.Text("Aparência do Mapa", size=16, weight=ft.FontWeight.BOLD),
            ft.Row([ddl_style, sw_heat, sl_heat_radius], spacing=12, wrap=True),
        ], tight=True, spacing=10),
    )

    def set_style_value(val: str):
        map_style_val["value"] = val
        ddl_style.value = val
        refresh_map_only()

    def toggle_heat(_=None):
        heat_state["on"] = not bool(heat_state["on"])
        sw_heat.value = heat_state["on"]
        refresh_map_only()

    def heat_radius_step(step: int):
        heat_state["radius"] = max(5, min(50, int(heat_state["radius"]) + int(step)))
        sl_heat_radius.value = heat_state["radius"]
        refresh_map_only()

    btn_style_menu = ft.PopupMenuButton(
        icon=ft.Icons.PALETTE,
        tooltip="Aparência do mapa",
        items=[
            ft.PopupMenuItem(text="Claro (Carto)",   on_click=lambda e: set_style_value("carto-positron")),
            ft.PopupMenuItem(text="Escuro (Carto)",  on_click=lambda e: set_style_value("carto-darkmatter")),
            ft.PopupMenuItem(text="OpenStreetMap",   on_click=lambda e: set_style_value("open-street-map")),
            ft.PopupMenuItem(),
            ft.PopupMenuItem(text="Heatmap ligar/desligar", on_click=toggle_heat),
            ft.PopupMenuItem(text="Heatmap + raio",        on_click=lambda e: heat_radius_step(+5)),
            ft.PopupMenuItem(text="Heatmap - raio",        on_click=lambda e: heat_radius_step(-5)),
        ],
    )

    # Cartões de dados
    table_rep = ft.DataTable(columns=[ft.DataColumn(ft.Text("Representante")), ft.DataColumn(ft.Text("Quantidade"))], rows=[], column_spacing=14, heading_row_color=ft.Colors.GREY_100)
    rep_card = ft.Container(content=ft.Column([ft.Text("Clientes por Representante", size=16, weight=ft.FontWeight.BOLD), ft.Container(ft.Column([table_rep], scroll=ft.ScrollMode.AUTO, expand=True), height=300)], tight=True, spacing=10), bgcolor=ft.Colors.WHITE, border_radius=12, padding=14)

    table_sem = ft.DataTable(columns=[ft.DataColumn(ft.Text("Estado"))], rows=[], heading_row_color=ft.Colors.GREY_100, column_spacing=18)
    sem_hos_card = ft.Container(content=ft.Column([ft.Text("Estados sem HOS", size=16, weight=ft.FontWeight.BOLD), ft.Container(ft.Column([table_sem], scroll=ft.ScrollMode.AUTO, expand=True), height=150)], tight=True, spacing=10), bgcolor=ft.Colors.WHITE, border_radius=12, padding=14)

    table_mod = ft.DataTable(columns=[ft.DataColumn(ft.Text("Módulo")), ft.DataColumn(ft.Text("Quantidade"))], rows=[], column_spacing=18, heading_row_color=ft.Colors.GREY_100)
    mod_card = ft.Container(content=ft.Column([ft.Text("Módulos Utilizados", size=16, weight=ft.FontWeight.BOLD), ft.Container(ft.Column([table_mod], scroll=ft.ScrollMode.AUTO, expand=True), height=230)], tight=True, spacing=10), bgcolor=ft.Colors.WHITE, border_radius=12, padding=14)

    table_cid_sample = ft.DataTable(columns=[ft.DataColumn(ft.Text("UF")), ft.DataColumn(ft.Text("Cidade")), ft.DataColumn(ft.Text("Quantidade"))], rows=[], column_spacing=18, heading_row_color=ft.Colors.GREY_100)
    cidades_card = ft.Container(content=ft.Column([ft.Text("Clientes por Cidade", size=16, weight=ft.FontWeight.BOLD), ft.Container(ft.Column([table_cid_sample], scroll=ft.ScrollMode.AUTO, expand=True), height=230)], tight=True, spacing=10), bgcolor=ft.Colors.WHITE, border_radius=12, padding=14)

    ddl_style.on_change = lambda e: (map_style_val.__setitem__("value", ddl_style.value), refresh_map_only())
    sw_heat.on_change = lambda e: (heat_state.__setitem__("on", bool(sw_heat.value)), refresh_map_only())
    sl_heat_radius.on_change = lambda e: (heat_state.__setitem__("radius", int(sl_heat_radius.value)), refresh_map_only())

    # Atualização/paths
    txt_folder = ft.TextField(label="Pasta dos CSVs", value=csv_folder, width=520)

    def update_paths_from_folder():
        nonlocal csv_folder, p_cli, p_mod, p_rep, p_munis
        folder = str(txt_folder.value).strip() or base
        folder = os.path.abspath(folder)
        csv_folder = folder
        p_cli, p_mod, p_rep, p_munis = make_paths(csv_folder)
        try:
            with open(config_path, "w", encoding="utf-8") as _f:
                json.dump({"csv_folder": csv_folder}, _f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    def on_file_picker_result(e: ft.FilePickerResultEvent):
        try:
            if e.files and len(e.files) > 0:
                chosen = e.files[0].path
                folder = os.path.dirname(chosen)
                txt_folder.value = folder
                update_paths_from_folder()
                txt_folder.update()
                page.snack_bar = ft.SnackBar(ft.Text(f"Pasta definida: {folder}"))
                page.snack_bar.open = True
                page.update()
        except Exception:
            pass

    file_picker = ft.FilePicker(on_result=on_file_picker_result)
    page.overlay.append(file_picker)
    btn_browse = ft.FilledButton("Procurar CSV", on_click=lambda e: file_picker.pick_files(allow_multiple=False))

    btn_refresh = ft.FilledButton("Atualizar dados", icon=ft.Icons.REFRESH)
    sw_auto = ft.Switch(label="Auto-atualizar", value=False)
    ddl_intervalo = ft.Dropdown(label="Intervalo (s)", value="30", options=[ft.dropdown.Option(v) for v in ["10", "30", "60", "120", "300"]], width=140)

    # Navegação automática UF
    sw_auto_uf = ft.Switch(label="Auto-navegar UF", value=False)
    ddl_intervalo_uf = ft.Dropdown(label="Intervalo UF (s)", value="10", options=[ft.dropdown.Option(v) for v in ["5", "10", "20", "30", "60"]], width=140)
    uf_nav_index = {"index": -1}

    # Ciclo Mapa ↔ Legenda
    sw_legend_cycle = ft.Switch(label="Alternar Mapa ↔ Legenda (tela cheia)", value=False)
    ddl_legend_interval = ft.Dropdown(label="Intervalo troca (s)", value="10", options=[ft.dropdown.Option(v) for v in ["5", "10", "15", "20", "30", "60"]], width=140)

    # Cards de configuração e atalhos
    nav_card = ft.Container(content=ft.Column([ft.Text("Navegação", size=16, weight=ft.FontWeight.BOLD), ft.Row([ddl_estado, btn_reset], spacing=10, wrap=True)], tight=True, spacing=10), bgcolor=ft.Colors.WHITE, border_radius=12, padding=14, visible=False)
    atualizacao_card = ft.Container(content=ft.Column([ft.Text("Atualização de Dados", size=16, weight=ft.FontWeight.BOLD), ft.Row([txt_folder, btn_browse], spacing=10, wrap=True), ft.Row([btn_refresh, sw_auto, ddl_intervalo], spacing=10, wrap=True)], tight=True, spacing=10), bgcolor=ft.Colors.WHITE, border_radius=12, padding=14, visible=False)
    auto_uf_card = ft.Container(content=ft.Column([ft.Text("Navegação Automática UF", size=16, weight=ft.FontWeight.BOLD), ft.Row([sw_auto_uf, ddl_intervalo_uf], spacing=10, wrap=True)], tight=True, spacing=10), bgcolor=ft.Colors.WHITE, border_radius=12, padding=14, visible=False)
    exibicao_card = ft.Container(content=ft.Column([ft.Text("Exibição Mapa/Legenda", size=16, weight=ft.FontWeight.BOLD), ft.Row([sw_legend_cycle, ddl_legend_interval], spacing=10, wrap=True)], tight=True, spacing=10), bgcolor=ft.Colors.WHITE, border_radius=12, padding=14, visible=False)
    btn_toggle_atual = ft.TextButton("Config. Atualização", icon=ft.Icons.TUNE)
    btn_toggle_auto_uf = ft.TextButton("Config. Navegação UF", icon=ft.Icons.TUNE)
    btn_toggle_nav = ft.TextButton("Config. Navegação", icon=ft.Icons.TUNE)
    btn_toggle_look = ft.TextButton("Config. Aparência", icon=ft.Icons.BRUSH)
    btn_toggle_exibicao = ft.TextButton("Config. Exibição", icon=ft.Icons.VIEW_WEEK)

    btns_col = ft.Column([
        ft.Row([btn_toggle_atual, btn_toggle_auto_uf], spacing=8),
        ft.Row([btn_toggle_nav, btn_toggle_look], spacing=8),
        ft.Row([btn_toggle_exibicao], spacing=8),
    ], tight=True)

    def toggle_card(card: ft.Container, btn: ft.TextButton, show_label: str, hide_label: str):
        card.visible = not getattr(card, "visible", False)
        btn.text = hide_label if card.visible else show_label
        card.update(); btn.update(); btns_col.update()

    btn_toggle_atual.on_click = lambda e: toggle_card(atualizacao_card, btn_toggle_atual, "Config. Atualização", "Ocultar Atualização")
    btn_toggle_auto_uf.on_click = lambda e: toggle_card(auto_uf_card, btn_toggle_auto_uf, "Config. Navegação UF", "Ocultar Navegação UF")
    btn_toggle_nav.on_click = lambda e: toggle_card(nav_card, btn_toggle_nav, "Config. Navegação", "Ocultar Navegação")
    btn_toggle_look.on_click = lambda e: toggle_card(look_card, btn_toggle_look, "Config. Aparência", "Ocultar Aparência")
    btn_toggle_exibicao.on_click = lambda e: toggle_card(exibicao_card, btn_toggle_exibicao, "Config. Exibição", "Ocultar Exibição")

    # Stack do mapa
    mapa_stack = ft.Stack([
        plot_mapa,
        ft.Container(btn_style_menu, alignment=ft.alignment.bottom_left, padding=12),
        ft.Container(btn_enter_full, alignment=ft.alignment.bottom_right, padding=12),
        ft.Container(btn_exit_full,  alignment=ft.alignment.bottom_right, padding=12),
        ft.Container(panel_ufs,     alignment=ft.alignment.top_right,    padding=12),
        ft.Container(legend_full,   alignment=ft.alignment.center,       padding=0),
    ])

    col_esq  = ft.Column([rep_card, sem_hos_card], spacing=12, expand=True)
    col_meio = ft.Column([ft.Container(bgcolor=ft.Colors.WHITE, border_radius=12, padding=8, content=ft.Column([ft.Text("Mapa de Clientes", size=16, weight=ft.FontWeight.BOLD), mapa_stack], spacing=8))], spacing=12, expand=True)
    col_dir  = ft.Column([mod_card, cidades_card, btns_col, atualizacao_card, nav_card, auto_uf_card, exibicao_card, look_card], spacing=12, expand=True)

    container_esq  = ft.Container(col_esq,  col={"xs": 12, "md": 12, "lg": 3})
    container_meio = ft.Container(col_meio, col={"xs": 12, "md": 12, "lg": 6})
    container_dir  = ft.Container(col_dir,  col={"xs": 12, "md": 12, "lg": 3})

    main_row = ft.ResponsiveRow(controls=[container_esq, container_meio, container_dir], spacing=12, run_spacing=12)

    # ---------- dados ----------
    def reload_csvs():
        nonlocal clientes_hos_df, clientes_mod_df, clientes_rep_df, municipios_df
        nonlocal merged_df, df_totalizado_estado, df_totalizado_cidade, color_map

        clientes_hos_df = read_csv_semicolon(p_cli, required=["CODIGO_IBGE", "CIDADE", "ESTADO", "FANTASIA"])
        clientes_mod_df = read_csv_semicolon(p_mod, required=["MODULO", "QUANTIDADE"])
        clientes_rep_df = read_csv_semicolon(p_rep, required=["COD_REP", "NOME_REP", "QTD_CLIENTES"])
        municipios_df   = read_csv_comma(p_munis, required=["municipio", "uf", "lon", "lat", "name"])

        clientes_hos_df["CODIGO_IBGE"] = clientes_hos_df["CODIGO_IBGE"].map(pad7_ibge)
        municipios_df["municipio"]     = municipios_df["municipio"].map(pad7_ibge)
        mun_key = municipios_df[["municipio", "uf", "lon", "lat", "name"]].rename(columns={"municipio": "CODIGO_IBGE"})

        merged_df = pd.merge(clientes_hos_df, mun_key, on="CODIGO_IBGE", how="left").sort_values(by="CIDADE", na_position="last")

        faltantes = merged_df[merged_df["lat"].isna() | merged_df["lon"].isna()]
        if not faltantes.empty:
            def norm(s):
                return re.sub(r"\s+", " ", str(s).strip().lower())
            muni_alt = municipios_df.copy()
            muni_alt["key"] = muni_alt["name"].map(norm) + "|" + muni_alt["uf"].map(norm)
            merged_df["key"] = merged_df["CIDADE"].map(norm) + "|" + merged_df["ESTADO"].map(norm)
            merged_df = merged_df.merge(muni_alt[["key", "lon", "lat"]], on="key", how="left", suffixes=("", "_fb"))
            merged_df["lon"] = merged_df["lon"].fillna(merged_df["lon_fb"]) ; merged_df["lat"] = merged_df["lat"].fillna(merged_df["lat_fb"])
            merged_df.drop(columns=["key", "lon_fb", "lat_fb"], inplace=True, errors="ignore")

        df_totalizado_estado = clientes_hos_df.groupby("ESTADO").size().reset_index(name="Clientes")
        df_totalizado_cidade = clientes_hos_df.groupby(["CIDADE", "ESTADO"]).size().reset_index(name="Clientes").rename(columns={"ESTADO": "UF"})

        estados = sorted(merged_df["ESTADO"].dropna().unique().tolist()) if merged_df is not None else []
        color_map = make_color_map(estados if estados else todos_estados_const)

    # ---------- render ----------
    def refresh_map_only():
        uf_atual = ddl_estado.value or None
        plot_mapa.figure = criar_mapa(
            merged_df=merged_df,
            df_totalizado_estado=df_totalizado_estado,
            color_map=color_map,
            estado_selecionado=uf_atual,
            altura=mapa_height(),
            legend_mode=legend_mode_by_width(),
            map_style=map_style_val["value"],
            heatmap=bool(heat_state["on"]),
            heat_radius=int(heat_state["radius"]),
            token=MAPBOX_TOKEN,
        )
        plot_mapa.update()

    def refresh_ui():
        total_clientes_new = int(df_totalizado_estado["Clientes"].sum()) if df_totalizado_estado is not None else 0
        top.content = indicador_horizontal(total_clientes_new)
        top.update()

        refresh_map_only()

        if clientes_rep_df is not None:
            table_rep.rows = [
                ft.DataRow(cells=[ft.DataCell(ft.Text(str(r["NOME_REP"]))), ft.DataCell(ft.Text(str(r["QTD_CLIENTES"])) )])
                for _, r in clientes_rep_df.iterrows()
            ]
            table_rep.update()

        estados_sem = sorted(list(set(todos_estados_const) - set(df_totalizado_estado["ESTADO"])) ) if df_totalizado_estado is not None else todos_estados_const
        table_sem.rows = [ft.DataRow(cells=[ft.DataCell(ft.Text(uf))]) for uf in estados_sem]
        table_sem.update()

        if clientes_mod_df is not None:
            table_mod.rows = [ft.DataRow(cells=[ft.DataCell(ft.Text(str(r["MODULO"]))), ft.DataCell(ft.Text(str(r["QUANTIDADE"])) )]) for _, r in clientes_mod_df.iterrows()]
            table_mod.update()

        if df_totalizado_cidade is not None and not df_totalizado_cidade.empty:
            df_cid_sorted = df_totalizado_cidade.sort_values("Clientes", ascending=False)
            table_cid_sample.rows = [
                ft.DataRow(cells=[ft.DataCell(ft.Text(str(r["UF"]))), ft.DataCell(ft.Text(str(r["CIDADE"]))), ft.DataCell(ft.Text(str(r["Clientes"])) )])
                for _, r in df_cid_sorted.head(120).iterrows()
            ]
            table_cid_sample.update()

        refresh_ufs_panel()

    def refresh_data(_=None):
        try:
            update_paths_from_folder()
            reload_csvs()
            refresh_ui()
        except Exception as ex:
            page.snack_bar = ft.SnackBar(ft.Text(f"Falha ao atualizar: {ex}"))
            page.snack_bar.open = True
            page.update()

    # ---------- fullscreen ----------
    def show_map_view():
        legend_full.visible = False
        plot_mapa.visible = True
        legend_cycle_state["phase"] = "map"
        legend_full.update(); plot_mapa.update()

    def show_legend_view():
        plot_mapa.visible = False
        legend_full.visible = True
        legend_cycle_state["phase"] = "legend"
        legend_full.update(); plot_mapa.update()

    def set_fullscreen(flag: bool):
        if flag:
            container_esq.visible = False
            container_dir.visible = False
            container_meio.col = {"xs": 12, "md": 12, "lg": 12}
            btn_exit_full.visible = True
            btn_enter_full.visible = False
            panel_ufs.visible = True
            show_map_view()
        else:
            legend_cycle.stop()
            if sw_legend_cycle.value:
                sw_legend_cycle.value = False
            container_esq.visible = True
            container_dir.visible = True
            container_meio.col = {"xs": 12, "md": 12, "lg": 6}
            btn_exit_full.visible = False
            btn_enter_full.visible = True
            panel_ufs.visible = False
            show_map_view()
        container_esq.update(); container_dir.update(); container_meio.update(); btn_exit_full.update(); btn_enter_full.update(); panel_ufs.update(); main_row.update()

    def exit_fullscreen(_=None):
        try:
            if sw_auto_uf.value:
                sw_auto_uf.value = False
                auto_uf.stop()
        except Exception:
            pass
        try:
            if sw_legend_cycle.value:
                sw_legend_cycle.value = False
            legend_cycle.stop()
        except Exception:
            pass
        set_fullscreen(False)
        page.update()

    def enter_only_map(_=None):
        try:
            if sw_auto_uf.value:
                sw_auto_uf.value = False
                auto_uf.stop()
        except Exception:
            pass
        set_fullscreen(True)
        page.update()

    btn_exit_full.on_click = exit_fullscreen
    btn_enter_full.on_click = enter_only_map

    # ---------- navegação UF ----------
    def navegar_uf():
        if df_totalizado_estado is None or df_totalizado_estado.empty:
            return
        uf_list = df_totalizado_estado.sort_values("Clientes", ascending=False)["ESTADO"].tolist()
        if not uf_list:
            return
        curr = uf_nav_index.get("index", -1)
        nxt = (curr + 1) % len(uf_list)
        uf_nav_index["index"] = nxt
        ddl_estado.value = uf_list[nxt]
        ddl_estado.update()
        refresh_map_only()

    # ---------- jobs ----------
    auto_refresher = RepeatingJob(job=lambda: safe_call_ui(page, refresh_data), interval_seconds=int(ddl_intervalo.value))
    auto_uf = RepeatingJob(job=lambda: safe_call_ui(page, navegar_uf), interval_seconds=int(ddl_intervalo_uf.value))

    def legend_tick():
        if legend_cycle_state["phase"] == "map":
            safe_call_ui(page, show_legend_view)
        else:
            safe_call_ui(page, show_map_view)

    legend_cycle = RepeatingJob(job=legend_tick, interval_seconds=int(ddl_legend_interval.value))

    # ---------- events ----------
    ddl_estado.on_change = lambda e: refresh_map_only()

    def on_reset(_):
        ddl_estado.value = ""
        uf_nav_index["index"] = -1
        ddl_estado.update(); refresh_map_only()
    btn_reset.on_click = on_reset

    btn_refresh.on_click = refresh_data

    def on_auto_change(_):
        if sw_auto.value:
            auto_refresher.update_interval(int(ddl_intervalo.value or 30))
            auto_refresher.start()
        else:
            auto_refresher.stop()
        page.update()
    sw_auto.on_change = on_auto_change

    ddl_intervalo.on_change = lambda e: auto_refresher.update_interval(int(ddl_intervalo.value or 30))

    def on_auto_uf_change(_):
        if sw_auto_uf.value:
            auto_uf.update_interval(int(ddl_intervalo_uf.value or 10))
            auto_uf.start()
        else:
            auto_uf.stop()
        page.update()
    sw_auto_uf.on_change = on_auto_uf_change

    ddl_intervalo_uf.on_change = lambda e: auto_uf.update_interval(int(ddl_intervalo_uf.value or 10))

    def on_legend_cycle_change(_):
        if sw_legend_cycle.value:
            set_fullscreen(True)
            if sw_auto_uf.value:
                sw_auto_uf.value = False
                auto_uf.stop()
            legend_cycle.update_interval(int(ddl_legend_interval.value or 10))
            legend_cycle.start()
        else:
            legend_cycle.stop()
            show_map_view()
        page.update()
    sw_legend_cycle.on_change = on_legend_cycle_change

    ddl_legend_interval.on_change = lambda e: legend_cycle.update_interval(int(ddl_legend_interval.value or 10))

    ddl_style.on_change = lambda e: (map_style_val.__setitem__("value", ddl_style.value), refresh_map_only())

    page.on_resize = lambda e: refresh_map_only()
    page.on_keyboard_event = lambda e: (exit_fullscreen() if e.key == "Escape" else None)

    # ---------- layout root ----------
    page.add(ft.Column([top, main_row], expand=True, scroll=ft.ScrollMode.AUTO, spacing=12))

    # ---------- primeira render ----------
    try:
        reload_csvs()
        refresh_ui()
    except Exception as ex:
        page.add(
            ft.Container(
                bgcolor=ft.Colors.AMBER_50,
                border_radius=8,
                padding=16,
                content=ft.Column([
                    ft.Text("Falha ao iniciar a aplicação", size=20, weight=ft.FontWeight.BOLD),
                    ft.Text(str(ex), color=ft.Colors.RED_700, selectable=True),
                    ft.Text("Verifique os CSVs, separadores e nomes de colunas e tente novamente."),
                ]),
            )
        )


if __name__ == "__main__":
    ft.app(target=main)