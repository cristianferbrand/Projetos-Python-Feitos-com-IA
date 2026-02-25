#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DashboardClientesHOS — PostgreSQL
Mapa com CÍRCULOS por CIDADE (cor por UF) usando coordenadas da TABELA public.municipios (IBGE7)
- WEB: Iframe(srcdoc=...) igual ao app SQLite
- DESKTOP: HTTP local + WebView
"""

from __future__ import annotations

import os, re, json, unicodedata, functools, socketserver, http.server
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd
import flet as ft


# --- WebView compat (Flet >=0.26) -----------------------------------------
import flet as ft

def make_webview(url: str, height=None, expand=False):
    """
    Tenta usar flet_webview.WebView (pacote opcional) via importlib;
    se indisponível, cai para ft.WebView (deprecado até 0.28) e, por fim,
    tenta ft.IFrame / ft.Iframe ou um link de fallback.
    """
    # 1) Tenta pacote novo (sem 'import' no topo para não gerar warning do Pylance)
    try:
        import importlib
        wv_mod = importlib.import_module("flet_webview")
        WV = getattr(wv_mod, "WebView", None)
        if WV is not None:
            return WV(url=url, height=height, expand=expand)
    except Exception:
        pass

    # 2) Fallback para ft.WebView (enquanto existir no core)
    try:
        if hasattr(ft, "WebView"):
            return ft.WebView(url=url, height=height, expand=expand)
    except Exception:
        pass

    # 3) Último recurso: IFrame (nome pode ser IFrame ou Iframe, conforme versão)
    for name in ("IFrame", "Iframe"):
        IFr = getattr(ft, name, None)
        if IFr is not None:
            try:
                # Para conteúdo remoto/servido local via HTTP, use 'src='
                return IFr(src=url, height=height, expand=expand)
            except TypeError:
                # Algumas versões aceitam 'srcdoc='
                try:
                    return IFr(srcdoc=f"<iframe src='{url}'></iframe>", height=height, expand=expand)
                except Exception:
                    pass

    # 4) Fallback textual
    return ft.Column(
        [
            ft.Text("Não foi possível embutir o mapa neste cliente."),
            ft.TextButton("Abrir mapa em nova aba", on_click=lambda e: e.page.launch_url(url)),
        ]
    )
# ---- Compat ----

C = getattr(ft, "Colors", None) or getattr(ft, "colors", None)
I = getattr(ft, "Icons", None) or getattr(ft, "icons", None)
def op(alpha: float, color: str) -> str:
    try:
        return ft.Colors.with_opacity(alpha, color)
    except Exception:
        return color

BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = BASE_DIR / "config"; CONFIG_DIR.mkdir(parents=True, exist_ok=True)
SETTINGS_PATH = CONFIG_DIR / "settings_dashboard_pg.json"
MAP_DIR = BASE_DIR / "mapa"; MAP_DIR.mkdir(parents=True, exist_ok=True)
MAP_HTML_PATH = MAP_DIR / "mapa_folium_tmp.html"

DEFAULT_SETTINGS: Dict[str, object] = {
    "postgres": {"host":"127.0.0.1","port":5432,"dbname":"mapacliente","user":"postgres","password":"postgres","schema":"public"},
    "theme_dark": True,
    "map_style": "carto-positron",
    "heatmap_enabled": False,
    "heat_radius": 22,
    "refresh_on_sync": True,
    "sync_check_seconds": 15,
    "sync_runs_schema": "public",
    "sync_runs_table": "sync_runs"
}

def load_settings() -> Dict[str, object]:
    if not SETTINGS_PATH.exists():
        SETTINGS_PATH.write_text(json.dumps(DEFAULT_SETTINGS, ensure_ascii=False, indent=2), encoding="utf-8")
        return dict(DEFAULT_SETTINGS)
    try:
        data = json.loads(SETTINGS_PATH.read_text(encoding="utf-8"))
    except Exception:
        data = {}
    out = dict(DEFAULT_SETTINGS)
    for k, v in (data or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = {**out[k], **v}
        else:
            out[k] = v
    return out

# ---- PostgreSQL via SQLAlchemy ----
def pg_connect(cfg: Dict[str, object]):
    try:
        from sqlalchemy import create_engine
        from sqlalchemy.engine import URL
    except Exception as e:
        return None, None, f"SQLAlchemy ausente: {e}"
    pg = cfg.get("postgres", {}) if isinstance(cfg, dict) else {}
    try:
        url = URL.create(
            "postgresql+psycopg2",
            username=str(pg.get("user", "postgres")),
            password=str(pg.get("password", "postgres")),
            host=str(pg.get("host", "127.0.0.1")),
            port=int(pg.get("port", 5432)),
            database=str(pg.get("dbname", "mapacliente")),
        )
        eng = create_engine(url, pool_pre_ping=True)
        schema = str(pg.get("schema", "public"))
        with eng.connect() as con:
            con.exec_driver_sql("SELECT 1")
        return eng, schema, None
    except Exception as e:
        return None, None, f"[PG] conexão falhou: {e}"

def pg_read_views_and_municipios(engine, schema: str):
    """
    Lê:
      - clientes_hos_latest (clientes)
      - clientes_modulos_latest (módulos)
      - clientes_rep_latest (representantes)
      - public.municipios (municipios, uf, lon, lat, name) — TABELA DO SYNC
    """
    try:
        with engine.connect() as con:
            df_cli = pd.read_sql_query(f'SELECT * FROM "{schema}"."clientes_hos_latest"', con)
            df_mod = pd.read_sql_query(f'SELECT * FROM "{schema}"."clientes_modulos_latest"', con)
            df_rep = pd.read_sql_query(f'SELECT * FROM "{schema}"."clientes_rep_latest"', con)
            # municipios sempre em public (conforme sync)
            df_mun = pd.read_sql_query('SELECT municipio, uf, lon, lat, name FROM public.municipios', con)
        return df_cli, df_mod, df_rep, df_mun, None
    except Exception as e:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), f"[PG] leitura falhou: {e}"

# ---- Normalizações (IBGE7, nomes) ----
def only_digits(s: str) -> str:
    return re.sub(r"\D+", "", s or "")

def to_ibge7(x) -> Optional[str]:
    s = str(x) if x is not None else ""
    d = only_digits(s)
    return d.zfill(7) if d else None

def strip_accents_upper(s: str) -> str:
    if s is None:
        return ""
    s_nfkd = unicodedata.normalize("NFKD", str(s))
    return "".join(ch for ch in s_nfkd if not unicodedata.combining(ch)).upper().strip()

def prepare_clientes_with_ibge_and_coords(clientes: pd.DataFrame, municipios: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Retorna:
      - merged: clientes enriquecido com IBGE7 e coordenadas do municipio (lat, lon) quando faltarem nos clientes
      - df_uf:   clientes por UF
      - df_cid:  clientes por CIDADE/UF
    """
    if clientes is None:
        clientes = pd.DataFrame()
    if municipios is None:
        municipios = pd.DataFrame(columns=["municipio","uf","lon","lat","name"])

    # Normaliza municipios
    mun = municipios.copy()
    if not mun.empty:
        mun["municipio"] = mun["municipio"].apply(to_ibge7)
        mun["UF"] = mun["uf"].astype(str).str.upper()
        mun["CITY_NORM"] = mun["name"].apply(strip_accents_upper)
        # garante numéricos
        mun["lat"] = pd.to_numeric(mun["lat"], errors="coerce")
        mun["lon"] = pd.to_numeric(mun["lon"], errors="coerce")

    cli = clientes.copy()
    if not cli.empty:
        # Tenta obter IBGE7 a partir de várias colunas possíveis
        ibge_source_cols = ["CODIGO_IBGE","IBGE","MUNICIPIO","COD_MUNICIPIO","COD_IBGE","municipio"]
        ibge_col = None
        for c in ibge_source_cols:
            if c in cli.columns:
                ibge_col = c; break
        if ibge_col:
            cli["IBGE7"] = cli[ibge_col].apply(to_ibge7)
        else:
            cli["IBGE7"] = None

        # UF/Cidade normalizados para fallback de join por nome
        if "ESTADO" in cli.columns:
            cli["UF"] = cli["ESTADO"].astype(str).str.upper()
        elif "UF" in cli.columns:
            cli["UF"] = cli["UF"].astype(str).str.upper()
        else:
            cli["UF"] = None

        if "CIDADE" in cli.columns:
            cli["CITY_NORM"] = cli["CIDADE"].apply(strip_accents_upper)
        elif "MUNICIPIO_NOME" in cli.columns:
            cli["CITY_NORM"] = cli["MUNICIPIO_NOME"].apply(strip_accents_upper)
        else:
            cli["CITY_NORM"] = ""

        # Primeiro: join por IBGE7
        merged = pd.merge(cli, mun[["municipio","lat","lon"]], left_on="IBGE7", right_on="municipio", how="left")
        # Preenche lat/lon faltantes com join por (CITY_NORM, UF)
        need_city_join = merged["lat"].isna() | merged["lon"].isna()
        if need_city_join.any():
            tmp = pd.merge(
                merged[need_city_join].drop(columns=["lat","lon"], errors="ignore"),
                mun[["CITY_NORM","UF","lat","lon"]],
                on=["CITY_NORM","UF"],
                how="left",
                suffixes=("","_from_city")
            )
            merged.loc[need_city_join, "lat"] = tmp["lat"]
            merged.loc[need_city_join, "lon"] = tmp["lon"]

        # garante numéricos/limpos
        if "lat" in merged.columns:
            merged["lat"] = pd.to_numeric(merged["lat"], errors="coerce")
        if "lon" in merged.columns:
            merged["lon"] = pd.to_numeric(merged["lon"], errors="coerce")
    else:
        merged = pd.DataFrame(columns=["ESTADO","CIDADE","FANTASIA","lat","lon"])

    # Agrupamentos
    if not merged.empty and "ESTADO" in merged.columns:
        df_uf = merged.groupby("ESTADO").size().reset_index(name="Clientes")
    else:
        df_uf = pd.DataFrame(columns=["ESTADO","Clientes"])

    if not merged.empty and {"ESTADO","CIDADE"}.issubset(merged.columns):
        df_cid = merged.groupby(["ESTADO","CIDADE"]).size().reset_index(name="Clientes")
    else:
        df_cid = pd.DataFrame(columns=["ESTADO","CIDADE","Clientes"])

    return merged, df_uf, df_cid

# ---- Folium ----
try:
    import folium
    from folium import plugins as _folium_plugins
except Exception:
    folium = None
    _folium_plugins = None

def map_style_to_folium(style: str) -> str:
    s = (style or "").lower().strip()
    if s in ("carto-positron","positron"): return "CartoDB positron"
    if s in ("carto-darkmatter","dark","darkmatter","dark-matter"): return "CartoDB dark_matter"
    if s in ("open-street-map","openstreetmap","osm"): return "OpenStreetMap"
    return "CartoDB positron"

# Paleta por UF (estável)
UF_ALL = ["AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS","MG","PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC","SP","SE","TO"]
BASE_PALETTE = [
    "#1f77b4","#ff7f0e","#2ca02c","#d62728","#9467bd","#8c564b","#e377c2","#7f7f7f","#bcbd22","#17becf",
    "#393b79","#637939","#8c6d31","#843c39","#7b4173","#3182bd","#e6550d","#31a354","#756bb1","#636363",
    "#9c9ede","#e7ba52","#74c476","#9e9ac8","#bdbdbd","#6b6ecf","#b5cf6b"
]
def make_color_map(ufs_present) -> Dict[str,str]:
    ufs = [u for u in UF_ALL if u in set(ufs_present)]
    if not ufs: ufs = UF_ALL[:]
    pal = (BASE_PALETTE * ((len(ufs)//len(BASE_PALETTE))+1))[:len(ufs)]
    return {u: pal[i] for i,u in enumerate(ufs)}

def coerce_latlon(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or not {"lat","lon"}.issubset(df.columns):
        return df.iloc[0:0].copy()
    out = df.copy()
    out["lat"] = pd.to_numeric(out["lat"], errors="coerce")
    out["lon"] = pd.to_numeric(out["lon"], errors="coerce")
    out = out[out["lat"].notna() & out["lon"].notna()]
    out = out[out["lat"].between(-90,90) & out["lon"].between(-180,180)]
    return out

def criar_mapa_html_cidades(df: pd.DataFrame, *, estilo: str, color_map: Dict[str,str]) -> str:
    if folium is None:
        return "<div style='padding:12px;color:#b00'>Folium não instalado. pip install folium</div>"
    df = coerce_latlon(df)
    if df.empty:
        # Sem dados — não fixa mais em Bento. Mostra texto amigável.
        return "<div style='padding:12px'>Sem dados de coordenadas. Verifique a tabela public.municipios e os codigos IBGE7 nos clientes.</div>"
    lat0 = float(df["lat"].mean()); lon0 = float(df["lon"].mean())
    tiles = map_style_to_folium(estilo)
    m = folium.Map(location=[lat0, lon0], zoom_start=4, tiles=tiles, control_scale=True)

    # agrega por cidade/UF e usa lat/lon do município
    grp = df.groupby(["CIDADE","ESTADO"]).agg(Clientes=("CIDADE","size"), lat=("lat","mean"), lon=("lon","mean")).reset_index()
    grp = grp.sort_values("Clientes", ascending=False)

    for _, r in grp.iterrows():
        try: lat = float(r["lat"]); lon = float(r["lon"])
        except Exception: continue
        uf = str(r["ESTADO"]); cid = str(r["CIDADE"]); qtd = int(r["Clientes"])
        radius = max(6, min(36, 6 + 3 * (qtd ** 0.5)))
        color = color_map.get(uf, "#3388ff")
        folium.CircleMarker(
            [lat, lon],
            radius=radius,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.55,
            weight=2,
            popup=folium.Popup(f"<b>{cid}/{uf}</b><br>Clientes: {qtd}", max_width=300)
        ).add_to(m)
    try:
        return m.get_root().render()
    except Exception as e:
        return f"<div style='padding:12px;color:#b00'>Falha ao renderizar mapa: {e}</div>"

def embed_folium_map(page: ft.Page, html: str, *, height: int) -> ft.Control:
    # Grava o HTML (melhora performance do caso WebView; IFrame pode usar srcdoc)
    try:
        MAP_HTML_PATH.write_text(html, encoding="utf-8")
    except Exception:
        pass

    platform = str(getattr(page, "platform", "")).lower()

    # Ambiente Web: preferir IFrame inline
    if platform in ("web", "fuchsia"):
        IFr = getattr(ft, "IFrame", None) or getattr(ft, "Iframe", None)
        if IFr is not None:
            try:
                return IFr(srcdoc=html, width=getattr(page, "width", None) or 1200, height=height, expand=True)
            except Exception:
                pass

    # Desktop: HTTP local + WebView (novo pacote ou fallback)
    port = ensure_map_http_server()
    if port:
        try:
            return make_webview(url=f"http://192.168.1.167:{port}/{MAP_HTML_PATH.name}", height=height, expand=True)
        except Exception:
            pass

    # Fallback: IFrame (se disponível)
    IFr = getattr(ft, "IFrame", None) or getattr(ft, "Iframe", None)
    if IFr is not None:
        try:
            return IFr(srcdoc=html, height=height, expand=True)
        except Exception:
            pass

    # Último recurso: container com mensagem
    return ft.Container(height=height, expand=True, padding=8, content=ft.Text("Não foi possível embutir o mapa."))
# ==== Servidor HTTP local (desktop) ====

_MAP_HTTPD = None
_MAP_HTTPD_PORT = None
def ensure_map_http_server():
    global _MAP_HTTPD, _MAP_HTTPD_PORT
    if _MAP_HTTPD is not None and _MAP_HTTPD_PORT:
        return _MAP_HTTPD_PORT
    class _SilentTCPServer(socketserver.TCPServer):
        allow_reuse_address = True
    try:
        handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(MAP_DIR))
    except TypeError:
        import os as _os
        _os.chdir(str(MAP_DIR))
        handler = http.server.SimpleHTTPRequestHandler
    for port_try in (8780, 8781, 0):
        try:
            httpd = _SilentTCPServer(("192.168.1.167", port_try), handler)
            _MAP_HTTPD = httpd
            _MAP_HTTPD_PORT = httpd.server_address[1]
            import threading
            th = threading.Thread(target=httpd.serve_forever, name="MapHTTP", daemon=True)
            th.start()
            break
        except OSError:
            continue
    return _MAP_HTTPD_PORT

# ---- UI helpers ----
def mapa_height(h) -> int:
    try:
        h = float(h or 800)
    except Exception:
        h = 800.0
    return int(max(360, min(720, h*0.58)))

def table_height(h) -> int:
    try:
        h = float(h or 800)
    except Exception:
        h = 800.0
    return int(max(200, min(420, h*0.30)))

def main(page: ft.Page):
    cfg = load_settings()
    page.title = "DashboardClientesHOS — PG (Cidades com círculos por UF via public.municipios)"
    page.theme_mode = ft.ThemeMode.DARK if bool(cfg.get("theme_dark", True)) else ft.ThemeMode.LIGHT
    page.padding = 12; page.scroll = ft.ScrollMode.AUTO
    page.window_min_width = 1060; page.window_min_height = 700

    surface = getattr(C, "SURFACE_CONTAINER_LOW", getattr(C, "GREY_900", "#1e1e1e")) if page.theme_mode==ft.ThemeMode.DARK else getattr(C, "WHITE", "#fff")
    divider = op(0.08, "#000")

    def card(title: str, child: ft.Control, *, height: int | None = None) -> ft.Container:
        inner = ft.Container(ft.Column([child], expand=True, scroll=ft.ScrollMode.AUTO), height=height) if height else child
        return ft.Container(
            content=ft.Column([
                ft.Row([ft.Text(title, size=16, weight=ft.FontWeight.BOLD)], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
                ft.Divider(height=1, color=divider),
                inner,
            ], spacing=10, tight=True),
            bgcolor=surface, border_radius=16, padding=16
        )

    # Conexão PG (não derruba a UI se falhar)
    engine, schema, err = pg_connect(cfg)
    if err:
        page.snack_bar = ft.SnackBar(ft.Text(err), bgcolor="#b71c1c", open=True)
    if engine is not None and schema:
        df_cli, df_mod, df_rep, df_mun, err2 = pg_read_views_and_municipios(engine, schema)
    else:
        df_cli, df_mod, df_rep, df_mun, err2 = pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), None
    if err2:
        page.snack_bar = ft.SnackBar(ft.Text(err2), bgcolor="#c62828", open=True)

    # Fallback mínimo para tabelas (mapa agora não fixa em Bento; depende de municipios)
    if df_mod.empty:
        df_mod = pd.DataFrame([{"MODULO":"ERP","QUANTIDADE":1}])
    if df_rep.empty:
        df_rep = pd.DataFrame([{"NOME_REP":"Representante X","QTD_CLIENTES":1}])

    # Enriquecimento com public.municipios
    merged, df_uf, df_cid = prepare_clientes_with_ibge_and_coords(df_cli, df_mun)

    # >>> COLOR MAP por UF (usado no mapa e na legenda) <<<
    ufs_present = sorted(df_uf["ESTADO"].astype(str).unique().tolist()) if not df_uf.empty else []
    color_map = make_color_map(ufs_present)

    # ---- Controles ----
    btn_conf = ft.IconButton(getattr(I,"SETTINGS","settings"))
    total_card = card("Resumo", ft.Row([]), height=110)

    rep_tbl = ft.DataTable(columns=[ft.DataColumn(ft.Text("Representante")), ft.DataColumn(ft.Text("Clientes"))], rows=[], heading_row_color=op(0.04,"#000"))
    mod_tbl = ft.DataTable(columns=[ft.DataColumn(ft.Text("Módulo")), ft.DataColumn(ft.Text("Qtd"))], rows=[], heading_row_color=op(0.04,"#000"))
    cid_tbl = ft.DataTable(columns=[ft.DataColumn(ft.Text("Cidade")), ft.DataColumn(ft.Text("UF")), ft.DataColumn(ft.Text("Qtd"))], rows=[], heading_row_color=op(0.04,"#000"))
    sem_tbl = ft.DataTable(columns=[ft.DataColumn(ft.Text("Estado"))], rows=[], heading_row_color=op(0.04,"#000"))
    rep_card = card("Clientes por Representante", rep_tbl, height=table_height(page.height))
    mod_card = card("Clientes por Módulo", mod_tbl, height=table_height(page.height))
    cid_card = card("Clientes por Cidade", cid_tbl, height=table_height(page.height))

    mapa_box = ft.Container(height=mapa_height(page.height))
    mapa_card = card("Mapa de Clientes", mapa_box)

    legenda_content = ft.Row([], spacing=12, alignment=ft.MainAxisAlignment.SPACE_BETWEEN)
    legenda = card("Legenda", legenda_content)

    page.add(ft.Row([btn_conf], alignment=ft.MainAxisAlignment.END))

    # ---- Builders ----
    def build_kpis():
        total = len(merged)
        total_card.content.controls[2] = ft.Row([
            ft.Container(bgcolor=op(0.06,"#000"), border_radius=12, padding=12, content=ft.Column([
                ft.Text("Total de clientes", size=18, bgcolor=op(0.06,"#000")),
                ft.Text(f"{total:,}".replace(",", "."), size=32, weight=ft.FontWeight.BOLD),
            ], spacing=2))
        ])
        total_card.update()

    def build_tables():
        # reps
        rows = []
        dfr = df_rep.copy()
        if "QTD_CLIENTES" not in dfr.columns:
            key = "REPRESENTANTE" if "REPRESENTANTE" in dfr.columns else dfr.columns[0]
            dfr = dfr.groupby(key).size().reset_index(name="QTD_CLIENTES").rename(columns={key:"NOME_REP"})
        for _, r in dfr.sort_values("QTD_CLIENTES", ascending=False).head(50).iterrows():
            rows.append(ft.DataRow(cells=[ft.DataCell(ft.Text(str(r.get("NOME_REP","")))), ft.DataCell(ft.Text(str(int(r.get("QTD_CLIENTES",0)))))]))
        rep_tbl.rows = rows; rep_tbl.update()
        # mod
        rows = []
        dfm = df_mod.copy()
        if "QUANTIDADE" not in dfm.columns:
            key = "MODULO" if "MODULO" in dfm.columns else dfm.columns[0]
            dfm = dfm.groupby(key).size().reset_index(name="QUANTIDADE").rename(columns={key:"MODULO"})
        for _, r in dfm.sort_values("QUANTIDADE", ascending=False).head(50).iterrows():
            rows.append(ft.DataRow(cells=[ft.DataCell(ft.Text(str(r.get("MODULO","")))), ft.DataCell(ft.Text(str(int(r.get("QUANTIDADE",0)))))]))
        mod_tbl.rows = rows; mod_tbl.update()
        # cidades (amostra top 50)
        rows = []
        dfc = df_cid.sort_values(["Clientes","ESTADO","CIDADE"], ascending=[False, True, True]).head(50)
        for _, r in dfc.iterrows():
            rows.append(ft.DataRow(cells=[ft.DataCell(ft.Text(str(r["CIDADE"]))), ft.DataCell(ft.Text(str(r["ESTADO"]))), ft.DataCell(ft.Text(str(int(r["Clientes"]))))]))
        cid_tbl.rows = rows; cid_tbl.update()
        # estados sem HOS
        todas = set(UF_ALL)
        presentes = set(df_uf["ESTADO"].astype(str).tolist()) if not df_uf.empty else set()
        faltantes = sorted(list(todas - presentes))
        sem_tbl.rows = [ft.DataRow(cells=[ft.DataCell(ft.Text(uf))]) for uf in faltantes]; sem_tbl.update()

    def build_legend():
        if df_uf.empty:
            legenda_content.controls = [ft.Text("Sem dados")]
            legenda.update(); return
        ordered = df_uf.sort_values(["Clientes","ESTADO"], ascending=[False, True]).values.tolist()
        col_count = 4; per_col = (len(ordered) + col_count - 1) // col_count
        cols = []
        for i in range(col_count):
            chunk = ordered[i*per_col:(i+1)*per_col]
            col_rows = []
            for uf, qtd in chunk:
                color = color_map.get(str(uf), "#7f7f7f")
                col_rows.append(ft.Row([ft.Container(width=10, height=10, border_radius=2, bgcolor=color), ft.Text(f"{uf}: {int(qtd)}")], spacing=8))
            cols.append(ft.Column(col_rows, spacing=4, tight=True))
        legenda_content.controls = cols
        legenda.update()

    # >>> MAPA: SEM BENTO FIXO; usa coordenadas de public.municipios
    def build_map():
        html = criar_mapa_html_cidades(
            merged,
            estilo=str(cfg.get("map_style","carto-positron")),
            color_map=color_map
        )
        mapa_box.content = embed_folium_map(page, html, height=mapa_height(page.height))
        mapa_box.update()

    def rebuild_all():
        build_kpis(); build_tables(); build_legend(); build_map()

    
    # --- WATCHER DO SYNC (MAX(snapshot_id) com ok=1 em sync_runs) -----------
    import asyncio

    def _get_sync_token(engine, schema_name: str, table_name: str):
        """Retorna o token do sync: MAX(snapshot_id) onde ok=1."""
        try:
            with engine.connect() as con:
                q = f'''
                    SELECT MAX(snapshot_id)::text AS token
                    FROM "{schema_name}"."{table_name}"
                    WHERE ok = 1
                '''
                df = pd.read_sql_query(q, con)
                tok = df["token"].iloc[0] if not df.empty else None
                return (str(tok) if pd.notna(tok) else None), None
        except Exception as e:
            return None, f"Falha ao ler {schema_name}.{table_name}: {e}"

    def _refresh_all_now():
        """Relê as views *latest* e reconstrói a UI."""
        nonlocal df_cli, df_mod, df_rep, df_mun, merged, df_uf, df_cid, color_map
        if engine is None or not schema:
            return
        df1, df2, df3, df4, err2 = pg_read_views_and_municipios(engine, schema)
        if err2:
            page.snack_bar = ft.SnackBar(ft.Text(err2), bgcolor="#c62828", open=True)
            return
        df_cli, df_mod, df_rep, df_mun = df1, df2, df3, df4
        merged, df_uf, df_cid = prepare_clientes_with_ibge_and_coords(df_cli, df_mun)
        ufs_present = sorted(df_uf["ESTADO"].astype(str).unique().tolist()) if not df_uf.empty else []
        color_map = make_color_map(ufs_present)
        rebuild_all()
        page.snack_bar = ft.SnackBar(ft.Text("Dados atualizados após novo sync."), open=True)

    async def watch_sync_runs():
        """Checa periodicamente MAX(snapshot_id) (ok=1). Se mudou, atualiza dashboard."""
        secs = int(cfg.get("sync_check_seconds", 15))
        secs = max(5, min(secs, 24*60*60))
        sr_schema = str(cfg.get("sync_runs_schema", "public"))
        sr_table  = str(cfg.get("sync_runs_table", "sync_runs"))
        last_token, _ = _get_sync_token(engine, sr_schema, sr_table)
        while True:
            await asyncio.sleep(secs)
            try:
                token, errx = _get_sync_token(engine, sr_schema, sr_table)
                if errx:
                    page.snack_bar = ft.SnackBar(ft.Text(errx), bgcolor="#b71c1c", open=True)
                    continue
                if token and token != last_token:
                    last_token = token
                    _refresh_all_now()
            except Exception as e:
                page.snack_bar = ft.SnackBar(ft.Text(f"watch_sync_runs: {e}"), bgcolor="#b71c1c", open=True)
# Grid 3-7-2
    col_esq  = ft.Column([total_card, card("Clientes por Representante", rep_tbl, height=table_height(page.height)), card("Estados sem HOS", sem_tbl, height=table_height(page.height))], spacing=12, expand=True)
    col_meio = ft.Column([mapa_card, legenda], spacing=12, expand=True)
    col_dir  = ft.Column([mod_card, cid_card], spacing=12, expand=True)

    grid = ft.ResponsiveRow(
        controls=[
            ft.Container(col_esq,  col={'xs':12,'md':12,'lg':3}),
            ft.Container(col_meio, col={'xs':12,'md':12,'lg':7}),
            ft.Container(col_dir,  col={'xs':12,'md':12,'lg':2})
        ],
        spacing=12, run_spacing=12
    )
    page.add(grid)

    page.on_resize = lambda e: build_map()
    rebuild_all()
    # Inicia o watcher de sync (após primeira renderização)
    if bool(cfg.get("refresh_on_sync", True)):
        page.run_task(watch_sync_runs)


if __name__ == "__main__":
    # Porta fixa (ex.: 8787). 
    # host="0.0.0.0" expõe o app na rede local; use "127.0.0.1" para acesso só na máquina.
    ft.app(
        target=main,
        view=ft.AppView.WEB_BROWSER,
        assets_dir=str(BASE_DIR),
        port=8787,
        host="192.168.1.167"
    )