# operacao_vendas_flet_v2.py
# -*- coding: utf-8 -*-
"""
App Flet: Analisador de Operação de Vendas (Questor) com Interface
- Lê TXT/CSV (separador configurável) com layout:
  tipo,empresa,data(DDMMAAAA),conta_debito,conta_credito,valor,filler,historico,cupom_id
- Normaliza: data, débito/crédito, cliente/cupom/série.
- Valida D=C por total, dia e cupom; detecta duplicidades.
- Exibe KPIs e prévias em abas; exporta Excel consolidado.
- (Novo) Botão "Gerar modelo de mapa" a partir das contas do arquivo.

Requisitos:
  pip install flet pandas openpyxl python-dateutil pytz
"""

from __future__ import annotations
import os
import sys
import json
import re
from pathlib import Path
from typing import Optional, Dict, Tuple

import pandas as pd
from dateutil import parser as dtparser
import pytz
import flet as ft

# =========================
# Compat/shim Colors/Icons
# =========================
try:
    C = ft.Colors  # preferido
except AttributeError:
    C = ft.colors  # fallback
try:
    I = ft.Icons   # preferido
except AttributeError:
    I = ft.icons   # fallback

def op(alpha: float, color: str) -> str:
    try:
        return ft.Colors.with_opacity(color, alpha)  # type: ignore
    except Exception:
        return color

# =========================
# Constantes e diretórios
# =========================
TZ = pytz.timezone("America/Sao_Paulo")
BASE_DIR = Path(getattr(sys, "_MEIPASS", Path(__file__).resolve().parent)).resolve()
DIR_EXPORT = (BASE_DIR / "export").resolve()
DIR_LOGS = (BASE_DIR / "logs").resolve()
DIR_CONFIG = (BASE_DIR / "config").resolve()
SETTINGS_PATH = DIR_CONFIG / "settings.json"

for d in (DIR_EXPORT, DIR_LOGS, DIR_CONFIG):
    d.mkdir(parents=True, exist_ok=True)

DEFAULT_SETTINGS = {
    "separator": ",",
    "encoding": "",  # vazio = deixar padrão do pandas
    "default_output": str(DIR_EXPORT / "Relatorio_OperacaoVendas.xlsx"),
}

# =========================
# Persistência de settings
# =========================
def load_settings() -> Dict:
    if SETTINGS_PATH.exists():
        try:
            return json.loads(SETTINGS_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return DEFAULT_SETTINGS.copy()

def save_settings(data: Dict):
    try:
        SETTINGS_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        print(f"[WARN] Falha ao salvar settings: {e}")

# =========================
# Parsing / Normalização
# =========================
def to_date_ddmmyyyy(s: str) -> pd.Timestamp:
    s = str(s).strip()
    if re.fullmatch(r"\d{8}", s):
        s_fmt = f"{s[0:2]}/{s[2:4]}/{s[4:8]}"
        try:
            return pd.to_datetime(s_fmt, dayfirst=True)
        except Exception:
            pass
    try:
        return pd.to_datetime(dtparser.parse(s, dayfirst=True))
    except Exception:
        return pd.NaT

def to_float(s) -> float:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return 0.0
    st = str(s).strip()
    # tenta direto (se já vier com ponto decimal)
    try:
        return float(st)
    except Exception:
        pass
    # tenta modo BR
    st_try = st.replace(".", "").replace(",", ".")
    try:
        return float(st_try)
    except Exception:
        pass
    m = re.findall(r"[-\d\.]+", st)
    return float(m[0]) if m else 0.0

def extrair_campos_historico(hist: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if not isinstance(hist, str):
        return None, None, None
    cliente = None
    mc = re.search(r"Cliente:\s*(.+?)\s+Cupom:", hist, flags=re.IGNORECASE)
    if mc:
        cliente = mc.group(1).strip()
    cupom = None
    mcp = re.search(r"Cupom:\s*([A-Za-z0-9\-_/]+)", hist, flags=re.IGNORECASE)
    if mcp:
        cupom = mcp.group(1).strip()
    serie = None
    ms = re.search(r"Serie:\s*([A-Za-z0-9\-_/]+)", hist, flags=re.IGNORECASE)
    if ms:
        serie = ms.group(1).strip()
    return cliente, cupom, serie

def ler_arquivo(path: str, sep: str = ",", encoding: Optional[str] = None) -> pd.DataFrame:
    df = pd.read_csv(
        path, sep=sep, encoding=encoding if encoding else None,
        header=None, dtype=str, quotechar='"', engine="python"
    )
    df.columns = [
        "tipo", "empresa", "data_raw", "conta_debito", "conta_credito",
        "valor", "filler", "historico", "cupom_id"
    ]
    return df

def normalizar(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["empresa"] = out["empresa"].astype(str).str.strip()
    out["data"] = out["data_raw"].apply(to_date_ddmmyyyy)
    out["conta_debito"] = out["conta_debito"].astype(str).str.strip()
    out["conta_credito"] = out["conta_credito"].astype(str).str.strip()
    out["valor"] = out["valor"].apply(to_float)

    extra = out["historico"].apply(extrair_campos_historico)
    out["cliente"] = extra.apply(lambda t: t[0])
    out["cupom"] = extra.apply(lambda t: t[1])
    out["serie"] = extra.apply(lambda t: t[2])

    def define_dc(row):
        cd = row["conta_debito"]
        cc = row["conta_credito"]
        if cd and cd != "0" and (not cc or cc == "0"):
            return "D", row["valor"], 0.0, cd
        if cc and cc != "0" and (not cd or cd == "0"):
            return "C", 0.0, row["valor"], cc
        return None, 0.0, 0.0, cd or cc or ""

    dc = out.apply(define_dc, axis=1, result_type="expand")
    out["tipo_dc"] = dc[0]
    out["debito"] = dc[1].astype(float)
    out["credito"] = dc[2].astype(float)
    out["conta"] = dc[3].astype(str).str.strip()
    out["valor_alg"] = out["debito"] - out["credito"]

    out = out.sort_values(["data", "empresa", "cupom_id", "tipo_dc"], ascending=[True, True, True, False]).reset_index(drop=True)
    cols = ["data", "empresa", "cupom_id", "cupom", "serie", "cliente",
            "conta", "tipo_dc", "debito", "credito", "valor", "historico"]
    return out[cols]

def aplicar_periodo(df: pd.DataFrame, data_ini: Optional[str], data_fim: Optional[str]) -> pd.DataFrame:
    if not data_ini and not data_fim:
        return df
    try:
        mask = pd.Series(True, index=df.index)
        if data_ini:
            p1 = pd.to_datetime(data_ini)
            mask &= df["data"] >= p1
        if data_fim:
            p2 = pd.to_datetime(data_fim)
            mask &= df["data"] <= p2
        return df.loc[mask].copy()
    except Exception:
        return df

def carregar_mapa_contas(path: str) -> Optional[pd.DataFrame]:
    if not path:
        return None
    df = pd.read_csv(path, sep=";", dtype=str, engine="python")
    for c in ["cod_sintetico", "conta", "conta_nome"]:
        if c not in df.columns:
            raise ValueError("Mapa de contas precisa das colunas: cod_sintetico;conta;conta_nome")
    df = df.fillna("")
    df["cod_sintetico"] = df["cod_sintetico"].astype(str).str.strip()
    df["conta"] = df["conta"].astype(str).str.strip()
    df["conta_nome"] = df["conta_nome"].astype(str).str.strip()
    return df

def enriquecer_com_mapa(df_base: pd.DataFrame, df_mapa: Optional[pd.DataFrame]) -> pd.DataFrame:
    if df_mapa is None:
        out = df_base.copy()
        out["conta_nome"] = ""
        return out
    join = df_base.merge(df_mapa, left_on="conta", right_on="cod_sintetico", how="left")
    join["conta_full"] = join["conta_y"].where(join["conta_y"].notna(), join["conta_x"])
    join["conta_nome"] = join["conta_nome"].fillna("")
    join = join.drop(columns=["cod_sintetico", "conta_x", "conta_y"])
    join = join.rename(columns={"conta_full": "conta"})
    cols = ["data", "empresa", "cupom_id", "cupom", "serie", "cliente",
            "conta", "conta_nome", "tipo_dc", "debito", "credito", "valor", "historico"]
    return join[cols]

def validar(df: pd.DataFrame) -> Dict[str, pd.DataFrame | float | int]:
    total_deb = float(df["debito"].sum())
    total_cred = float(df["credito"].sum())
    diff_total = round(total_deb - total_cred, 2)

    por_dia = df.groupby("data", dropna=False).agg(
        debitos=("debito", "sum"),
        creditos=("credito", "sum")
    ).reset_index()
    por_dia["diferenca"] = (por_dia["debitos"] - por_dia["creditos"]).round(2)

    por_cupom = df.groupby(["data", "cupom_id", "cupom", "serie", "cliente"], dropna=False).agg(
        debitos=("debito", "sum"),
        creditos=("credito", "sum"),
        movimentos=("cupom_id", "count")
    ).reset_index()
    por_cupom["diferenca"] = (por_cupom["debitos"] - por_cupom["creditos"]).round(2)

    dup_keys = ["data", "conta", "valor", "cupom_id", "tipo_dc"]
    dups = df[df.duplicated(dup_keys, keep=False)].sort_values(dup_keys).reset_index(drop=True)

    return {
        "total_debitos": round(total_deb, 2),
        "total_creditos": round(total_cred, 2),
        "diferenca_total": diff_total,
        "por_dia": por_dia,
        "por_cupom": por_cupom,
        "duplicidades": dups
    }

def agregacoes(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    por_conta = df.groupby(["conta", "conta_nome"], dropna=False).agg(
        debitos=("debito", "sum"),
        creditos=("credito", "sum"),
        saldo=("valor", "sum"),
        movimentos=("conta", "count")
    ).reset_index().sort_values(["conta"])
    por_cliente = df.groupby(["cliente"], dropna=False).agg(
        debitos=("debito", "sum"),
        creditos=("credito", "sum"),
        saldo=("valor", "sum"),
        cupons=("cupom_id", "nunique")
    ).reset_index().sort_values(["cliente"])
    por_dia = df.groupby(["data"], dropna=False).agg(
        debitos=("debito", "sum"),
        creditos=("credito", "sum"),
        saldo=("valor", "sum"),
        cupons=("cupom_id", "nunique")
    ).reset_index().sort_values(["data"])
    return {"por_conta": por_conta, "por_cliente": por_cliente, "por_dia": por_dia}

def exportar_excel(saida: Path,
                   base: pd.DataFrame,
                   valids: Dict[str, pd.DataFrame | float | int],
                   aggs: Dict[str, pd.DataFrame]) -> None:
    saida.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(str(saida), engine="openpyxl") as w:
        base.to_excel(w, sheet_name="Base_Normalizada", index=False)
        resumo = pd.DataFrame({
            "Métrica": ["Total Débitos", "Total Créditos", "Diferença (D-C)"],
            "Valor": [valids["total_debitos"], valids["total_creditos"], valids["diferenca_total"]],
        })
        resumo.to_excel(w, sheet_name="Validacoes", index=False)

        start = len(resumo) + 3
        def dump_df(sheet, start_row, titulo, dfx):
            title = pd.DataFrame({"Seção": [titulo]})
            title.to_excel(w, sheet_name=sheet, startrow=start_row, index=False, header=False)
            start_row += 1
            if isinstance(dfx, pd.DataFrame) and not dfx.empty:
                dfx.to_excel(w, sheet_name=sheet, startrow=start_row, index=False)
                start_row += len(dfx) + 2
            else:
                pd.DataFrame({"Info": ["(vazio)"]}).to_excel(w, sheet_name=sheet, startrow=start_row, index=False, header=False)
                start_row += 3
            return start_row

        start = dump_df("Validacoes", start, "Por Dia (D x C)", valids["por_dia"])
        start = dump_df("Validacoes", start, "Por Cupom (D x C)", valids["por_cupom"])
        start = dump_df("Validacoes", start, "Duplicidades Potenciais", valids["duplicidades"])

        aggs["por_cupom"] = valids["por_cupom"]
        for nome, dfa in [("Por_Cupom", aggs["por_cupom"]),
                          ("Por_Conta", aggs["por_conta"]),
                          ("Por_Cliente", aggs["por_cliente"]),
                          ("Por_Dia", aggs["por_dia"])]:
            dfa.to_excel(w, sheet_name=nome, index=False)

# =========================
# Helpers de UI
# =========================
def kpi_card(title: str, value: str, icon=I.ANALYTICS, color=C.BLUE_50):
    return ft.Card(
        elevation=2,
        content=ft.Container(
            padding=16,
            content=ft.Row(
                alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                vertical_alignment=ft.CrossAxisAlignment.CENTER,
                controls=[
                    ft.Column(spacing=4, controls=[
                        ft.Text(title, size=12, color=C.GREY_700),
                        ft.Text(value, size=18, weight=ft.FontWeight.W_700),
                    ]),
                    ft.Icon(icon, size=28, color=op(0.9, C.BLUE))
                ]
            )
        )
    )

def df_preview(df: Optional[pd.DataFrame], max_rows: int = 200) -> ft.Control:
    if df is None or df.empty:
        return ft.Text("Sem dados.", size=12, color=C.GREY_600)
    dfv = df.head(max_rows).copy()
    buf = dfv.to_string(index=False)
    return ft.Text(buf, size=12, selectable=True, no_wrap=False)

# =========================
# Main Flet App
# =========================
def main(page: ft.Page):
    page.title = "Operação de Vendas (Questor) - Analisador"
    page.theme_mode = ft.ThemeMode.LIGHT
    page.window_width = 1200
    page.window_height = 900
    page.padding = 16
    page.scroll = ft.ScrollMode.AUTO

    settings = load_settings()

    # Estados
    state = {
        "input_path": "",
        "mapa_path": "",
        "data_ini": "",
        "data_fim": "",
        "sep": settings.get("separator", ","),
        "enc": settings.get("encoding", ""),
        "out_path": settings.get("default_output", str(DIR_EXPORT / "Relatorio_OperacaoVendas.xlsx")),
        "base": None,
        "valids": None,
        "aggs": None,
        "busy": False,
    }

    # Inputs
    input_field = ft.TextField(label="Arquivo de entrada (TXT/CSV)", expand=True, read_only=True)
    mapa_field = ft.TextField(label="Mapa de Contas (opcional; csv com ;)", expand=True, read_only=True)
    data_ini = ft.TextField(label="Data inicial (AAAA-MM-DD)", width=200)
    data_fim = ft.TextField(label="Data final (AAAA-MM-DD)", width=200)
    sep_field = ft.TextField(label="Separador", width=100, value=state["sep"], tooltip="Ex.: , ou ;")
    enc_field = ft.TextField(label="Encoding", width=160, value=state["enc"], tooltip="Ex.: utf-8, latin-1 (vazio = padrão)")
    out_field = ft.TextField(label="Arquivo de saída (.xlsx)", expand=True, value=state["out_path"])

    # FilePickers (corrigidos)
    def on_pick_in(e: ft.FilePickerResultEvent):
        if e.files and len(e.files) > 0:
            p = e.files[0].path
            input_field.value = p
            state["input_path"] = p
            input_field.update()
            page.update()

    def on_pick_mapa(e: ft.FilePickerResultEvent):
        if e.files and len(e.files) > 0:
            p = e.files[0].path
            mapa_field.value = p
            state["mapa_path"] = p
            mapa_field.update()
            page.update()

    pick_in = ft.FilePicker(on_result=on_pick_in)
    pick_mapa = ft.FilePicker(on_result=on_pick_mapa)
    page.overlay.append(pick_in)
    page.overlay.append(pick_mapa)

    # Dialog de Configurações
    def open_settings_dialog(_):
        def on_save(_e):
            state["sep"] = sep_field.value.strip() or ","
            state["enc"] = enc_field.value.strip()
            state["out_path"] = out_field.value.strip() or str(DIR_EXPORT / "Relatorio_OperacaoVendas.xlsx")
            settings["separator"] = state["sep"]
            settings["encoding"] = state["enc"]
            settings["default_output"] = state["out_path"]
            save_settings(settings)
            page.snack_bar = ft.SnackBar(ft.Text("Configurações salvas."), bgcolor=C.GREEN_100)
            page.snack_bar.open = True
            page.update()

        dlg = ft.AlertDialog(
            modal=True,
            title=ft.Text("Configurações"),
            content=ft.Column(
                tight=True,
                spacing=8,
                controls=[
                    ft.Row([sep_field, enc_field], alignment=ft.MainAxisAlignment.START),
                    out_field
                ]
            ),
            actions=[
                ft.TextButton("Fechar", on_click=lambda e: setattr(dlg, "open", False)),
                ft.ElevatedButton("Salvar", icon=I.SAVE, on_click=on_save),
            ],
            actions_alignment=ft.MainAxisAlignment.END,
        )
        dlg.on_dismiss = lambda e: page.update()
        page.dialog = dlg
        dlg.open = True
        page.update()

    page.appbar = ft.AppBar(
        title=ft.Text("Analisador - Operação de Vendas (Questor)"),
        center_title=False,
        bgcolor=C.BLUE_50,
        actions=[ft.IconButton(icon=I.SETTINGS, tooltip="Configurações", on_click=open_settings_dialog)],
    )

    # KPIs
    kpi_deb = kpi_card("Total Débitos", "0,00", icon=I.TRENDING_UP)
    kpi_cred = kpi_card("Total Créditos", "0,00", icon=I.TRENDING_DOWN)
    kpi_diff = kpi_card("Diferença (D-C)", "0,00", icon=I.FUNCTIONS, color=C.YELLOW_50)
    kpi_rows = kpi_card("Registros", "0", icon=I.TABLE_ROWS, color=C.GREY_50)

    # Abas (prévia)
    base_preview = ft.Container(padding=8, content=ft.Text("Carregue e analise para ver a base."))
    por_cupom_preview = ft.Container(padding=8, content=ft.Text("Carregue e analise para ver o D x C por cupom."))
    por_conta_preview = ft.Container(padding=8, content=ft.Text("Carregue e analise para ver o agrupamento por conta."))
    por_cliente_preview = ft.Container(padding=8, content=ft.Text("Carregue e analise para ver o agrupamento por cliente."))
    por_dia_preview = ft.Container(padding=8, content=ft.Text("Carregue e analise para ver o agrupamento por dia."))
    valid_preview = ft.Container(padding=8, content=ft.Text("Carregue e analise para ver as validações."))

    tabs = ft.Tabs(
        selected_index=0,
        tabs=[
            ft.Tab(text="Base", icon=I.TABLE_CHART, content=base_preview),
            ft.Tab(text="Por Cupom", icon=I.RECEIPT, content=por_cupom_preview),
            ft.Tab(text="Por Conta", icon=I.ACCOUNT_BALANCE, content=por_conta_preview),
            ft.Tab(text="Por Cliente", icon=I.PEOPLE, content=por_cliente_preview),
            ft.Tab(text="Por Dia", icon=I.CALENDAR_MONTH, content=por_dia_preview),
            ft.Tab(text="Validações", icon=I.VERIFIED, content=valid_preview),
        ],
        expand=True
    )

    # Barra de status
    busy_indicator = ft.ProgressBar(visible=False)

    # Ações
    def set_busy(flag: bool):
        state["busy"] = flag
        busy_indicator.visible = flag
        page.update()

    def executar_analise(_):
        if state["busy"]:
            return
        state["data_ini"] = data_ini.value.strip()
        state["data_fim"] = data_fim.value.strip()
        if not state["input_path"]:
            page.snack_bar = ft.SnackBar(ft.Text("Selecione um arquivo de entrada."), bgcolor=C.RED_100)
            page.snack_bar.open = True
            page.update()
            return

        set_busy(True)
        try:
            df_raw = ler_arquivo(state["input_path"], sep=state["sep"], encoding=state["enc"] or None)
        except Exception as e:
            set_busy(False)
            page.snack_bar = ft.SnackBar(ft.Text(f"Erro ao ler arquivo: {e}"), bgcolor=C.RED_100)
            page.snack_bar.open = True
            page.update()
            return

        try:
            base = normalizar(df_raw)
            base = aplicar_periodo(base, state["data_ini"], state["data_fim"])

            df_mapa = None
            if state["mapa_path"]:
                try:
                    df_mapa = carregar_mapa_contas(state["mapa_path"])
                except Exception as e:
                    page.snack_bar = ft.SnackBar(ft.Text(f"Mapa de contas ignorado: {e}"), bgcolor=C.YELLOW_100)
                    page.snack_bar.open = True

            base = enriquecer_com_mapa(base, df_mapa)
            valids = validar(base)
            aggs = agregacoes(base)

            # Atualiza estado
            state["base"] = base
            state["valids"] = valids
            state["aggs"] = aggs

            # KPIs
            kpi_deb.content.content.controls[0].controls[1].value = f"{valids['total_debitos']:.2f}".replace(".", ",")
            kpi_cred.content.content.controls[0].controls[1].value = f"{valids['total_creditos']:.2f}".replace(".", ",")
            kpi_diff.content.content.controls[0].controls[1].value = f"{valids['diferenca_total']:.2f}".replace(".", ",")
            kpi_rows.content.content.controls[0].controls[1].value = f"{len(base)}"

            # Prévias
            base_preview.content = df_preview(base)
            por_cupom_preview.content = df_preview(valids["por_cupom"])
            por_conta_preview.content = df_preview(aggs["por_conta"])
            por_cliente_preview.content = df_preview(aggs["por_cliente"])
            por_dia_preview.content = df_preview(aggs["por_dia"])

            # Validações
            resumo_txt = (
                f"Total Débitos: {valids['total_debitos']:.2f}\n"
                f"Total Créditos: {valids['total_creditos']:.2f}\n"
                f"Diferença (D-C): {valids['diferenca_total']:.2f}\n"
            )
            anom_cupons = valids["por_cupom"][valids["por_cupom"]["diferenca"] != 0]
            if not anom_cupons.empty:
                resumo_txt += f"\nALERTA: {len(anom_cupons)} cupom(ns) com diferença D-C != 0.\n"
                resumo_txt += anom_cupons.head(50).to_string(index=False)
            dups = valids["duplicidades"]
            if dups is not None and not dups.empty:
                resumo_txt += f"\n\nPossíveis duplicidades: {len(dups)} linhas.\n"
                resumo_txt += dups.head(50).to_string(index=False)
            valid_preview.content = ft.Text(resumo_txt, size=12, selectable=True)

            page.update()

        except Exception as e:
            page.snack_bar = ft.SnackBar(ft.Text(f"Erro na análise: {e}"), bgcolor=C.RED_100)
            page.snack_bar.open = True
            page.update()
        finally:
            set_busy(False)

    def exportar(_):
        if state["busy"]:
            return
        if state["base"] is None or state["valids"] is None or state["aggs"] is None:
            page.snack_bar = ft.SnackBar(ft.Text("Faça a análise antes de exportar."), bgcolor=C.YELLOW_100)
            page.snack_bar.open = True
            page.update()
            return
        try:
            saida = Path(state["out_path"])
            exportar_excel(saida, state["base"], state["valids"], state["aggs"])
            page.snack_bar = ft.SnackBar(ft.Text(f"Excel exportado em: {saida}"), bgcolor=C.GREEN_100)
            page.snack_bar.open = True
            page.update()
        except Exception as e:
            page.snack_bar = ft.SnackBar(ft.Text(f"Erro ao exportar: {e}"), bgcolor=C.RED_100)
            page.snack_bar.open = True
            page.update()

    # (Novo) Gerar modelo de mapa
    def gerar_modelo_mapa(_):
        if state["busy"]:
            return
        if state["base"] is None or state["base"].empty:
            page.snack_bar = ft.SnackBar(ft.Text("Analise um arquivo antes de gerar o modelo de mapa."), bgcolor=C.YELLOW_100)
            page.snack_bar.open = True
            page.update()
            return
        contas = (
            state["base"]["conta"]
            .dropna()
            .astype(str)
            .str.strip()
            .replace("", pd.NA)
            .dropna()
            .unique()
            .tolist()
        )
        if not contas:
            page.snack_bar = ft.SnackBar(ft.Text("Nenhuma conta encontrada para gerar modelo."), bgcolor=C.YELLOW_100)
            page.snack_bar.open = True
            page.update()
            return
        df_modelo = pd.DataFrame({"cod_sintetico": contas, "conta": "", "conta_nome": ""})
        out_path = DIR_EXPORT / "modelo_mapa_contas.csv"
        df_modelo.to_csv(out_path, sep=";", index=False, encoding="utf-8")
        page.snack_bar = ft.SnackBar(ft.Text(f"Modelo salvo em: {out_path}"), bgcolor=C.GREEN_100)
        page.snack_bar.open = True
        page.update()

    # Botões
    btn_pick_in = ft.ElevatedButton("Escolher Arquivo", icon=I.ATTACH_FILE, on_click=lambda e: pick_in.pick_files(allow_multiple=False))
    btn_pick_mapa = ft.OutlinedButton("Mapa de Contas", icon=I.MAP, on_click=lambda e: pick_mapa.pick_files(allow_multiple=False))
    btn_analisar = ft.FilledButton("Analisar", icon=I.PLAY_ARROW, on_click=executar_analise)
    btn_exportar = ft.FilledTonalButton("Exportar Excel", icon=I.DOWNLOAD, on_click=exportar)
    btn_modelo = ft.OutlinedButton("Gerar modelo de mapa", icon=I.DESCRIPTION, on_click=gerar_modelo_mapa)

    # Layout
    page.add(
        ft.Column(
            spacing=12,
            controls=[
                ft.Row([input_field, btn_pick_in], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
                ft.Row([mapa_field, btn_pick_mapa], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
                ft.Row([data_ini, data_fim, sep_field, enc_field], alignment=ft.MainAxisAlignment.START),
                ft.Row([btn_analisar, btn_exportar, btn_modelo], alignment=ft.MainAxisAlignment.START),
                busy_indicator,
                ft.Row([kpi_deb, kpi_cred, kpi_diff, kpi_rows], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
                tabs
            ]
        )
    )

    # Handlers campos
    def on_sep_change(e):
        state["sep"] = sep_field.value.strip() or ","
    def on_enc_change(e):
        state["enc"] = enc_field.value.strip()
    def on_input_change(e):
        state["input_path"] = input_field.value.strip()
    def on_mapa_change(e):
        state["mapa_path"] = mapa_field.value.strip()
    sep_field.on_change = on_sep_change
    enc_field.on_change = on_enc_change
    input_field.on_change = on_input_change
    mapa_field.on_change = on_mapa_change

ft.app(target=main)