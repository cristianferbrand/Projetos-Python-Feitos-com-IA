
# -*- coding: utf-8 -*-
# App: Auditoria de XMLs NF-e / NFC-e (mod 55/65) – v1

import os
import sys
import csv
import math
import decimal
from decimal import Decimal, ROUND_HALF_UP
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, date
import flet as ft
import webbrowser

# ======= Diretórios =======
def get_base_dir() -> Path:
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return Path(sys.executable).parent.resolve()
    return Path(__file__).parent.resolve()

BASE_DIR = get_base_dir()
EXPORT_DIR = BASE_DIR / "export"
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

# =============================
# === UTILIDADES & FORMATO ====
# =============================
decimal.getcontext().prec = 28
NS = {"nfe": "http://www.portalfiscal.inf.br/nfe"}

def D(v):
    if v is None or v == "":
        return Decimal("0.00")
    try:
        return Decimal(str(v)).quantize(Decimal("0.00"), rounding=ROUND_HALF_UP)
    except Exception:
        try:
            return Decimal(str(v).replace(",", ".")).quantize(Decimal("0.00"), rounding=ROUND_HALF_UP)
        except Exception:
            return Decimal("0.00")

def brl(x: Decimal) -> str:
    s = f"{x:,.2f}"
    return "R$ " + s.replace(",", "X").replace(".", ",").replace("X", ".")

def br_dec(x: Decimal) -> str:
    s = f"{x:,.2f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")

def br_pct(x: float) -> str:
    s = f"{x:.2f}%"
    return s.replace(".", ",")

def text_or_none(elem):
    return elem.text.strip() if (elem is not None and elem.text is not None) else None

def find_text(root, path):
    el = root.find(path, NS)
    return text_or_none(el)

def find_first_decimal_under(node, local_name: str) -> Decimal:
    if node is None:
        return Decimal("0.00")
    for child in node.iter():
        if child.tag.endswith("}" + local_name) or child.tag == local_name:
            return D(text_or_none(child))
    return Decimal("0.00")

def find_first_text_under(node, local_name: str) -> str | None:
    if node is None:
        return None
    for child in node.iter():
        if child.tag.endswith("}" + local_name) or child.tag == local_name:
            return text_or_none(child)
    return None

def serie_str(s):
    return s if s is not None else ""

def parse_dhEmi_to_date(dh: str):
    if not dh:
        return None
    try:
        return datetime.fromisoformat(dh.replace("Z", "+00:00")).date()
    except Exception:
        try:
            return datetime.strptime(dh[:10], "%Y-%m-%d").date()
        except Exception:
            return None

def safe_os_open(path: str):
    try:
        if sys.platform.startswith("win"):
            os.startfile(path)  # type: ignore
        elif sys.platform == "darwin":
            os.system(f'open "{path}"')
        else:
            os.system(f'xdg-open "{path}"')
    except Exception:
        try:
            webbrowser.open(f"file://{path}")
        except Exception:
            pass

# ==================================
# === PARSER NF-e / NFC-e (4.00) ===
# ==================================
def parse_xml_nfe(path):
    """
    Retorna um dicionário com:
      'chave','modelo','tpNF','serie','numero','dhEmi','emitente','destinatario',
      'totais': {...},
      'itens': [...],
      'somatorio_itens_vProd_indTot1', 'divergencia_vProd', 'path'
    Itens recebem: 'ICMS_CST', 'ICMS_pICMS', 'PIS_CST', 'COFINS_CST', 'vPIS', 'vCOFINS'.
    """
    try:
        tree = ET.parse(path)
        root = tree.getroot()
    except Exception as e:
        raise RuntimeError(f"Falha ao ler XML: {path} ({e})")

    infNFe = root.find("nfe:NFe/nfe:infNFe", NS)
    if infNFe is None:
        infNFe = root.find("nfe:infNFe", NS)
    if infNFe is None:
        raise RuntimeError(f"Não encontrei infNFe em: {path}")

    ide = infNFe.find("nfe:ide", NS)
    emit = infNFe.find("nfe:emit", NS)
    dest = infNFe.find("nfe:dest", NS)

    modelo = find_text(ide, "nfe:mod")
    serie = find_text(ide, "nfe:serie")
    numero = find_text(ide, "nfe:nNF")
    dhEmi = find_text(ide, "nfe:dhEmi")
    tpNF  = (find_text(ide, "nfe:tpNF") or "").strip()  # "0" entrada, "1" saída

    inf_id = infNFe.attrib.get("Id", "")
    chave = inf_id.replace("NFe", "") if inf_id.startswith("NFe") else inf_id

    emitente = find_text(emit, "nfe:xNome") or ""
    destinatario = ""
    if dest is not None:
        destinatario = find_text(dest, "nfe:xNome") or ""

    icms_tot = infNFe.find("nfe:total/nfe:ICMSTot", NS)
    vProd = D(find_text(icms_tot, "nfe:vProd"))
    vDesc = D(find_text(icms_tot, "nfe:vDesc"))
    vNF   = D(find_text(icms_tot, "nfe:vNF"))
    vBC   = D(find_text(icms_tot, "nfe:vBC"))
    vICMS = D(find_text(icms_tot, "nfe:vICMS"))
    vTotTrib = D(find_text(icms_tot, "nfe:vTotTrib"))

    # Totais fiscais adicionais
    vFCP = D(find_text(icms_tot, "nfe:vFCP"))
    vBCST = D(find_text(icms_tot, "nfe:vBCST"))
    vST = D(find_text(icms_tot, "nfe:vST"))
    vFCPST = D(find_text(icms_tot, "nfe:vFCPST"))
    vFCPSTRet = D(find_text(icms_tot, "nfe:vFCPSTRet"))
    vPIS = D(find_text(icms_tot, "nfe:vPIS"))
    vCOFINS = D(find_text(icms_tot, "nfe:vCOFINS"))
    vFrete = D(find_text(icms_tot, "nfe:vFrete"))
    vSeg = D(find_text(icms_tot, "nfe:vSeg"))
    vOutro = D(find_text(icms_tot, "nfe:vOutro"))
    vII = D(find_text(icms_tot, "nfe:vII"))
    vIPI = D(find_text(icms_tot, "nfe:vIPI"))
    vICMSDeson = D(find_text(icms_tot, "nfe:vICMSDeson"))

    itens = []
    soma_vProd_indTot1 = Decimal("0.00")

    for det in infNFe.findall("nfe:det", NS):
        prod = det.find("nfe:prod", NS)
        imp  = det.find("nfe:imposto", NS)
        icms = imp.find("nfe:ICMS", NS) if imp is not None else None
        pis  = imp.find("nfe:PIS", NS) if imp is not None else None
        cof  = imp.find("nfe:COFINS", NS) if imp is not None else None

        nItem  = det.attrib.get("nItem", "")
        cProd  = find_text(prod, "nfe:cProd")
        xProd  = find_text(prod, "nfe:xProd")
        NCM    = find_text(prod, "nfe:NCM")
        CEST   = find_text(prod, "nfe:CEST")
        CFOP   = find_text(prod, "nfe:CFOP")
        qCom   = D(find_text(prod, "nfe:qCom"))
        vUnCom = D(find_text(prod, "nfe:vUnCom"))
        vProd_i = D(find_text(prod, "nfe:vProd"))
        vDesc_i = D(find_text(prod, "nfe:vDesc"))
        indTot = (find_text(prod, "nfe:indTot") or "1").strip()

        # Dados ICMS no item
        vBC_i   = find_first_decimal_under(icms, "vBC")
        vICMS_i = find_first_decimal_under(icms, "vICMS")
        pICMS_i = find_first_decimal_under(icms, "pICMS")

        # CST/CSOSN do grupo ICMS
        icms_cst = None
        if icms is not None:
            for child in icms:
                icms_cst = find_first_text_under(child, "CST") or find_first_text_under(child, "CSOSN")
                if icms_cst:
                    break

        # PIS/COFINS do item
        vPIS_i = find_first_decimal_under(pis, "vPIS")
        vCOFINS_i = find_first_decimal_under(cof, "vCOFINS")
        pis_cst = None
        cofins_cst = None
        if pis is not None:
            for child in pis:
                pis_cst = find_first_text_under(child, "CST")
                if pis_cst:
                    break
        if cof is not None:
            for child in cof:
                cofins_cst = find_first_text_under(child, "CST")
                if cofins_cst:
                    break

        if indTot == "1":
            soma_vProd_indTot1 += vProd_i

        itens.append(
            {
                "nItem": nItem,
                "cProd": cProd,
                "xProd": xProd,
                "NCM": NCM,
                "CEST": CEST,
                "CFOP": CFOP,
                "qCom": qCom,
                "vUnCom": vUnCom,
                "vProd": vProd_i,
                "vDesc": vDesc_i,
                "vBC": vBC_i,
                "vICMS": vICMS_i,
                "ICMS_pICMS": pICMS_i,
                "ICMS_CST": icms_cst or "",
                "vPIS": vPIS_i,
                "vCOFINS": vCOFINS_i,
                "PIS_CST": pis_cst or "",
                "COFINS_CST": cofins_cst or "",
                "indTot": indTot,
            }
        )

    divergencia_vProd = (soma_vProd_indTot1.quantize(Decimal("0.00")) != vProd)

    return {
        "chave": chave,
        "modelo": modelo,
        "tpNF": tpNF,
        "serie": serie_str(serie),
        "numero": numero,
        "dhEmi": dhEmi,
        "emitente": emitente,
        "destinatario": destinatario,
        "totais": {
            "vProd": vProd,
            "vDesc": vDesc,
            "vNF": vNF,
            "vBC": vBC,
            "vICMS": vICMS,
            "vTotTrib": vTotTrib,
            "vFCP": vFCP,
            "vBCST": vBCST,
            "vST": vST,
            "vFCPST": vFCPST,
            "vFCPSTRet": vFCPSTRet,
            "vPIS": vPIS,
            "vCOFINS": vCOFINS,
            "vFrete": vFrete,
            "vSeg": vSeg,
            "vOutro": vOutro,
            "vII": vII,
            "vIPI": vIPI,
            "vICMSDeson": vICMSDeson,
        },
        "itens": itens,
        "somatorio_itens_vProd_indTot1": soma_vProd_indTot1,
        "divergencia_vProd": divergencia_vProd,
        "path": path,
    }

# ==================================
# === A P P   F L E T (UI/UX) ======
# ==================================

# Shim de compatibilidade: Colors/Icons e with_opacity
try:
    C = ft.Colors  # Flet novo
except Exception:
    from flet import colors as _colors  # Flet antigo
    C = _colors
try:
    I = ft.Icons
except Exception:
    from flet import icons as _icons
    I = _icons

def op(alpha: float, color: str) -> str:
    try:
        return C.with_opacity(alpha, color)  # Flet novo
    except Exception:
        return color  # fallback

def is_attached(ctrl: ft.Control) -> bool:
    try:
        return getattr(ctrl, 'page', None) is not None
    except Exception:
        return False

def safe_update(ctrl: ft.Control):
    if is_attached(ctrl):
        ctrl.update()

def _short(s: str, lim=40):
    s = s or ""
    return s if len(s) <= lim else s[:lim-1] + "…"

def _kpi_card(titulo: str, valor_ctrl: ft.Text, icon_name, color_icon=None, subtitle: str | None = None):
    col = [ft.Text(titulo)]
    if subtitle:
        col.append(ft.Text(subtitle, size=12, color=C.GREY))
    col.append(valor_ctrl)
    return ft.Card(
        content=ft.Container(
            padding=12,
            content=ft.Row(
                alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                controls=[
                    ft.Column(spacing=4, controls=col),
                    ft.Icon(icon_name, size=32, color=(color_icon or C.BLUE_200)),
                ],
            ),
        ),
        elevation=2,
    )

def main(page: ft.Page):
    page.title = "Auditoria de XMLs - NF-e/NFC-e (55/65) – v1"
    page.theme_mode = ft.ThemeMode.DARK
    page.padding = 16
    page.window_width = 1360
    page.window_height = 900
    page.scroll = ft.ScrollMode.AUTO

    # === Barra de carregamento no cabeçalho (progresso da análise) ===
    header_loading_msg = ft.Text("Analisando XMLs...", size=12, weight=ft.FontWeight.W_600)
    header_loading_pct = ft.Text("", size=12, color=C.GREY_400)
    header_loading_bar_top = ft.ProgressBar(value=0.0)

    header_loading_banner = ft.Container(
        visible=False,
        padding=ft.padding.symmetric(horizontal=12, vertical=10),
        bgcolor=op(0.12, C.AMBER),
        border_radius=10,
        content=ft.Column(
            spacing=6,
            controls=[
                ft.Row(
                    spacing=8,
                    vertical_alignment=ft.CrossAxisAlignment.CENTER,
                    controls=[
                        ft.ProgressRing(width=16, height=16, stroke_width=3),
                        header_loading_msg,
                        ft.Container(expand=True),
                        header_loading_pct,
                    ],
                ),
                header_loading_bar_top,
            ],
        ),
    )

    # === Loading dialog for XML parsing ===
    loading_text = ft.Text("Carregando XMLs...", size=16, weight=ft.FontWeight.W_600)
    loading_bar = ft.ProgressBar(width=420, value=0.0)
    loading_dlg = ft.AlertDialog(
        modal=True,
        content=ft.Column(
            controls=[
                ft.Row([ft.ProgressRing(), loading_text], alignment=ft.MainAxisAlignment.START, spacing=12),
                loading_bar,
            ],
            tight=True,
            spacing=12,
        ),
    )

    def show_loading(msg: str = "Carregando XMLs..."):
        # Banner no cabeçalho
        header_loading_msg.value = msg
        header_loading_pct.value = ""
        header_loading_bar_top.value = 0.0
        header_loading_banner.visible = True

        # Dialog (mantido)
        loading_text.value = msg
        page.dialog = loading_dlg
        loading_dlg.open = True
        page.update()

    def set_loading_progress(current: int, total: int):
        try:
            v = max(0.0, min(1.0, (current / total) if total else 0.0))
            loading_bar.value = v
            header_loading_bar_top.value = v
            header_loading_pct.value = f"{int(v*100)}% ({current}/{total})" if total else ""
        except Exception:
            pass
        page.update()

    def hide_loading():
        # Banner no cabeçalho
        try:
            header_loading_banner.visible = False
            header_loading_pct.value = ""
            header_loading_bar_top.value = 0.0
        except Exception:
            pass

        try:
            if page.dialog == loading_dlg:
                loading_dlg.open = False
        except Exception:
            pass

        page.update()
    # === end loading dialog ===

    # ======= ESTADO =======
    notas: list[dict] = []
    itens_por_chave: dict[str, list[dict]] = {}
    lookup_nota_por_chave: dict[str, dict] = {}

    # ======= FILTROS =======
    data_de = ft.TextField(label="Data inicial (AAAA-MM-DD)", width=180, on_submit=lambda e: aplicar_filtros())
    data_ate = ft.TextField(label="Data final (AAAA-MM-DD)", width=180, on_submit=lambda e: aplicar_filtros())
    emitente_f = ft.TextField(label="Emitente contém", width=220, on_submit=lambda e: aplicar_filtros())
    destinatario_f = ft.TextField(label="Destinatário contém", width=220, on_submit=lambda e: aplicar_filtros())
    modelo_f = ft.Dropdown(
        label="Modelo", width=120, value="Ambos",
        options=[ft.dropdown.Option("Ambos"), ft.dropdown.Option("55"), ft.dropdown.Option("65")],
        on_change=lambda e: aplicar_filtros(),
    )
    tipo_f = ft.Dropdown(
        label="Tipo", width=120, value="Todas",
        options=[ft.dropdown.Option("Todas"), ft.dropdown.Option("Entradas"), ft.dropdown.Option("Saídas")],
        on_change=lambda e: aplicar_filtros(),
    )
    busca_f = ft.TextField(label="Pesquisar (chave/emitente/dest/CFOP)", width=320, on_submit=lambda e: aplicar_filtros())

    # ======= KPIs GERAIS =======
    kpi_saldo_bruto = ft.Text("R$ 0,00", size=22, weight=ft.FontWeight.BOLD)
    kpi_saldo_liquido = ft.Text("R$ 0,00", size=22, weight=ft.FontWeight.BOLD)
    kpi_desc_pct_saida = ft.Text("0,00%", size=22, weight=ft.FontWeight.BOLD)
    kpi_aliq_icms_eff = ft.Text("0,00%", size=22, weight=ft.FontWeight.BOLD)
    kpi_diverg_pct = ft.Text("0,00%", size=22, weight=ft.FontWeight.BOLD)

    # ======= KPIs ENTRADAS =======
    kpi_e_bruto = ft.Text("R$ 0,00", size=20, weight=ft.FontWeight.BOLD)
    kpi_e_liquido = ft.Text("R$ 0,00", size=20, weight=ft.FontWeight.BOLD)
    kpi_e_desc = ft.Text("R$ 0,00", size=20, weight=ft.FontWeight.BOLD)
    kpi_e_bc = ft.Text("R$ 0,00", size=20, weight=ft.FontWeight.BOLD)
    kpi_e_icms = ft.Text("R$ 0,00", size=20, weight=ft.FontWeight.BOLD)
    # Fiscais Entradas
    kpi_e_fcp = ft.Text("R$ 0,00", size=16, weight=ft.FontWeight.BOLD)
    kpi_e_st = ft.Text("R$ 0,00", size=16, weight=ft.FontWeight.BOLD)
    kpi_e_pis = ft.Text("R$ 0,00", size=16, weight=ft.FontWeight.BOLD)
    kpi_e_cofins = ft.Text("R$ 0,00", size=16, weight=ft.FontWeight.BOLD)
    kpi_e_tottrib = ft.Text("R$ 0,00", size=16, weight=ft.FontWeight.BOLD)

    # ======= KPIs SAÍDAS =======
    kpi_s_bruto = ft.Text("R$ 0,00", size=20, weight=ft.FontWeight.BOLD)
    kpi_s_liquido = ft.Text("R$ 0,00", size=20, weight=ft.FontWeight.BOLD)
    kpi_s_desc = ft.Text("R$ 0,00", size=20, weight=ft.FontWeight.BOLD)
    kpi_s_bc = ft.Text("R$ 0,00", size=20, weight=ft.FontWeight.BOLD)
    kpi_s_icms = ft.Text("R$ 0,00", size=20, weight=ft.FontWeight.BOLD)
    # Fiscais Saídas
    kpi_s_fcp = ft.Text("R$ 0,00", size=16, weight=ft.FontWeight.BOLD)
    kpi_s_st = ft.Text("R$ 0,00", size=16, weight=ft.FontWeight.BOLD)
    kpi_s_pis = ft.Text("R$ 0,00", size=16, weight=ft.FontWeight.BOLD)
    kpi_s_cofins = ft.Text("R$ 0,00", size=16, weight=ft.FontWeight.BOLD)
    kpi_s_tottrib = ft.Text("R$ 0,00", size=16, weight=ft.FontWeight.BOLD)
    kpi_s_pis_eff = ft.Text("0,00%", size=14)
    kpi_s_cofins_eff = ft.Text("0,00%", size=14)

    # ======= ALERTAS (contagens) =======
    alerta_cfop_tp_count = ft.Text("0", size=16, weight=ft.FontWeight.BOLD)
    alerta_ncm_inval_count = ft.Text("0", size=16, weight=ft.FontWeight.BOLD)
    alerta_icms0_vbcpos_count = ft.Text("0", size=16, weight=ft.FontWeight.BOLD)
    alerta_vnf_formula_count = ft.Text("0", size=16, weight=ft.FontWeight.BOLD)

    # ======= PAGINAÇÃO =======
    page_index = 0
    page_size = 25
    page_label = ft.Text("Página 1/1", size=14)
    page_size_dd = ft.Dropdown(
        label="Itens/página", width=140, value=str(page_size),
        options=[ft.dropdown.Option(str(n)) for n in [10, 25, 50, 100, 200]],
        on_change=lambda e: set_page_size(int(page_size_dd.value))
    )

    # ======= TABELA =======
    tabela_dados = ft.DataTable(
        columns=[
            ft.DataColumn(ft.Text("Tipo")),
            ft.DataColumn(ft.Text("Modelo")),
            ft.DataColumn(ft.Text("Série")),
            ft.DataColumn(ft.Text("Número")),
            ft.DataColumn(ft.Text("Emissão")),
            ft.DataColumn(ft.Text("Emitente")),
            ft.DataColumn(ft.Text("Destinatário")),
            ft.DataColumn(ft.Text("vProd")),
            ft.DataColumn(ft.Text("vDesc")),
            ft.DataColumn(ft.Text("vNF")),
            ft.DataColumn(ft.Text("vBC")),
            ft.DataColumn(ft.Text("vICMS")),
            ft.DataColumn(ft.Text("Ações")),
            ft.DataColumn(ft.Text("OK vProd?")),
        ],
        rows=[],
        heading_row_height=38,
        data_row_min_height=42,
        horizontal_lines=ft.BorderSide(0.5, op(0.2, C.GREY)),
        vertical_lines=ft.BorderSide(0.5, op(0.1, C.GREY)),
    )

    # ======= FILE PICKER =======
    file_picker = ft.FilePicker(on_result=lambda e: carregar_arquivos(e.files))
    page.overlay.append(file_picker)

    # ======= FUNÇÕES DE FILTRO =======
    def get_filtered_notas():
        res = list(notas)

        def in_range(n):
            d = parse_dhEmi_to_date(n.get("dhEmi"))
            if data_de.value:
                try:
                    d0 = datetime.strptime(data_de.value.strip(), "%Y-%m-%d").date()
                    if not d or d < d0:
                        return False
                except Exception:
                    pass
            if data_ate.value:
                try:
                    d1 = datetime.strptime(data_ate.value.strip(), "%Y-%m-%d").date()
                    if not d or d > d1:
                        return False
                except Exception:
                    pass
            return True

        res = [n for n in res if in_range(n)]

        if emitente_f.value:
            q = emitente_f.value.casefold()
            res = [n for n in res if (n.get("emitente","").casefold().find(q) >= 0)]
        if destinatario_f.value:
            q = destinatario_f.value.casefold()
            res = [n for n in res if (n.get("destinatario","").casefold().find(q) >= 0)]

        if modelo_f.value and modelo_f.value in ("55", "65"):
            res = [n for n in res if n.get("modelo") == modelo_f.value]

        if tipo_f.value == "Entradas":
            res = [n for n in res if n.get("tpNF") == "0"]
        elif tipo_f.value == "Saídas":
            res = [n for n in res if n.get("tpNF") == "1"]

        if busca_f.value:
            q = busca_f.value.casefold()
            def hit(n):
                if (n.get("chave","").casefold().find(q) >= 0): return True
                if (n.get("emitente","").casefold().find(q) >= 0): return True
                if (n.get("destinatario","").casefold().find(q) >= 0): return True
                for it in itens_por_chave.get(n["chave"], []):
                    if (it.get("CFOP","") or "").casefold().find(q) >= 0:
                        return True
                return False
            res = [n for n in res if hit(n)]
        return res

    # ======= KPI / MÉTRICAS =======
    def recomputar_kpis():
        filtradas = get_filtered_notas()
        entradas = [n for n in filtradas if n.get("tpNF") == "0"]
        saidas   = [n for n in filtradas if n.get("tpNF") == "1"]

        def soma(notas_l, campo):
            return sum((n["totais"][campo] for n in notas_l), Decimal("0.00"))

        e_vProd = soma(entradas, "vProd"); e_vNF = soma(entradas, "vNF")
        e_vDesc = soma(entradas, "vDesc"); e_vBC = soma(entradas, "vBC")
        e_vICMS = soma(entradas, "vICMS")
        e_vFCP = soma(entradas, "vFCP"); e_vST = soma(entradas, "vST")
        e_vPIS = soma(entradas, "vPIS"); e_vCOF = soma(entradas, "vCOFINS")
        e_vTT  = soma(entradas, "vTotTrib")

        s_vProd = soma(saidas, "vProd"); s_vNF = soma(saidas, "vNF")
        s_vDesc = soma(saidas, "vDesc"); s_vBC = soma(saidas, "vBC")
        s_vICMS = soma(saidas, "vICMS")
        s_vFCP = soma(saidas, "vFCP"); s_vST = soma(saidas, "vST")
        s_vPIS = soma(saidas, "vPIS"); s_vCOF = soma(saidas, "vCOFINS")
        s_vTT  = soma(saidas, "vTotTrib")

        saldo_bruto = (s_vProd - e_vProd)
        saldo_liquido = (s_vNF - e_vNF)
        desc_pct_saida = float((s_vDesc / s_vProd * Decimal(100)) if s_vProd > 0 else Decimal(0))
        aliq_icms_eff = float((s_vICMS / s_vBC * Decimal(100)) if s_vBC > 0 else Decimal(0))
        diverg_count = sum(1 for n in filtradas if n.get("divergencia_vProd"))
        diverg_pct = float((Decimal(diverg_count) / Decimal(len(filtradas)) * Decimal(100)) if filtradas else Decimal(0))

        kpi_saldo_bruto.value = brl(saldo_bruto)
        kpi_saldo_liquido.value = brl(saldo_liquido)
        kpi_desc_pct_saida.value = br_pct(desc_pct_saida)
        kpi_aliq_icms_eff.value = br_pct(aliq_icms_eff)
        kpi_diverg_pct.value = br_pct(diverg_pct)

        # Entradas
        kpi_e_bruto.value = brl(e_vProd)
        kpi_e_liquido.value = brl(e_vNF)
        kpi_e_desc.value = brl(e_vDesc)
        kpi_e_bc.value = brl(e_vBC)
        kpi_e_icms.value = brl(e_vICMS)
        kpi_e_fcp.value = brl(e_vFCP)
        kpi_e_st.value = brl(e_vST)
        kpi_e_pis.value = brl(e_vPIS)
        kpi_e_cofins.value = brl(e_vCOF)
        kpi_e_tottrib.value = brl(e_vTT)

        # Saídas
        kpi_s_bruto.value = brl(s_vProd)
        kpi_s_liquido.value = brl(s_vNF)
        kpi_s_desc.value = brl(s_vDesc)
        kpi_s_bc.value = brl(s_vBC)
        kpi_s_icms.value = brl(s_vICMS)
        kpi_s_fcp.value = brl(s_vFCP)
        kpi_s_st.value = brl(s_vST)
        kpi_s_pis.value = brl(s_vPIS)
        kpi_s_cofins.value = brl(s_vCOF)
        kpi_s_tottrib.value = brl(s_vTT)
        kpi_s_pis_eff.value = br_pct(float((s_vPIS / s_vProd * Decimal(100)) if s_vProd > 0 else Decimal(0)))
        kpi_s_cofins_eff.value = br_pct(float((s_vCOF / s_vProd * Decimal(100)) if s_vProd > 0 else Decimal(0)))

        # Alertas
        cfop_bad, ncm_bad, icms0_vbcpos, vnf_bad = gerar_alertas_listas(filtradas)
        alerta_cfop_tp_count.value = str(len(cfop_bad))
        alerta_ncm_inval_count.value = str(len(ncm_bad))
        alerta_icms0_vbcpos_count.value = str(len(icms0_vbcpos))
        alerta_vnf_formula_count.value = str(len(vnf_bad))

        page.update()

    def gerar_alertas_listas(notas_base: list[dict]):
        cfop_bad = []
        ncm_bad = []
        icms0_vbcpos = []
        vnf_bad = []
        for n in notas_base:
            tipo = n.get("tpNF")
            chave = n.get("chave")
            for it in itens_por_chave.get(chave, []):
                cfop = (it.get("CFOP") or "").strip()
                if cfop:
                    if tipo == "0" and (not cfop[0] in ("1","2","3")):
                        cfop_bad.append((chave, it["nItem"], cfop, "tpNF=0(Entrada)"))
                    if tipo == "1" and (not cfop[0] in ("5","6","7")):
                        cfop_bad.append((chave, it["nItem"], cfop, "tpNF=1(Saída)"))
            for it in itens_por_chave.get(chave, []):
                ncm = (it.get("NCM") or "").strip()
                if (not ncm) or (len(ncm) != 8) or (not ncm.isdigit()):
                    ncm_bad.append((chave, it["nItem"], ncm))
            for it in itens_por_chave.get(chave, []):
                if it.get("vBC", Decimal("0.00")) > 0 and it.get("vICMS", Decimal("0.00")) == 0:
                    icms0_vbcpos.append((chave, it["nItem"], it.get("vBC"), it.get("vICMS")))
            t = n["totais"]
            vNF_calc = (t["vProd"] - t["vDesc"] + t["vST"] + t["vFrete"] + t["vSeg"] + t["vOutro"] + t["vII"] + t["vIPI"] - t["vICMSDeson"])
            if abs(vNF_calc - t["vNF"]) > Decimal("0.05"):
                vnf_bad.append((chave, t["vNF"], vNF_calc))
        return cfop_bad, ncm_bad, icms0_vbcpos, vnf_bad

    # ======= TABELA / PAGINAÇÃO =======
    def set_page_size(n: int):
        nonlocal page_size, page_index
        page_size = max(1, n)
        page_index = 0
        preencher_tabela()

    def goto_page(delta: int):
        nonlocal page_index
        filtradas = get_filtered_notas()
        total_pages = max(1, math.ceil(len(filtradas) / page_size))
        page_index = max(0, min(page_index + delta, total_pages - 1))
        preencher_tabela()

    def preencher_tabela():
        tabela_dados.rows.clear()
        filtradas = get_filtered_notas()
        total = len(filtradas)
        total_pages = max(1, math.ceil(total / page_size))
        start = page_index * page_size
        end = min(start + page_size, total)
        page_label.value = f"Página {page_index+1}/{total_pages} — notas {start+1}–{end} de {total}"

        for n in filtradas[start:end]:
            tipo_txt = "Entrada" if n.get("tpNF") == "0" else "Saída"
            chave = n["chave"]
            v = n["totais"]
            ok = not n["divergencia_vProd"]
            icone = ft.Icon(
                I.CHECK_CIRCLE if ok else I.WARNING_AMBER,
                color=C.GREEN if ok else C.AMBER,
                size=20,
                tooltip="vProd (ICMSTot) confere com a soma dos itens indTot=1"
                if ok else "Divergência: vProd x soma itens (indTot=1)",
            )
            menu = ft.PopupMenuButton(
                items=[
                    ft.PopupMenuItem(text="Listar itens", on_click=lambda e, ch=chave: page.go(f"/itens/{ch}")),
                    ft.PopupMenuItem(text="Abrir XML", on_click=lambda e, p=n["path"]: safe_os_open(p)),
                    ft.PopupMenuItem(text="Abrir pasta", on_click=lambda e, p=n["path"]: safe_os_open(os.path.dirname(p))),
                ],
                icon=I.MENU,
                tooltip="Ações",
            )
            tabela_dados.rows.append(
                ft.DataRow(
                    cells=[
                        ft.DataCell(ft.Text(tipo_txt)),
                        ft.DataCell(ft.Text(n["modelo"] or "")),
                        ft.DataCell(ft.Text(n["serie"] or "")),
                        ft.DataCell(ft.Text(n["numero"] or "")),
                        ft.DataCell(ft.Text(n["dhEmi"] or "")),
                        ft.DataCell(ft.Text(_short(n["emitente"]))),
                        ft.DataCell(ft.Text(_short(n["destinatario"]))),
                        ft.DataCell(ft.Text(brl(v["vProd"]))),
                        ft.DataCell(ft.Text(brl(v["vDesc"]))),
                        ft.DataCell(ft.Text(brl(v["vNF"]))),
                        ft.DataCell(ft.Text(brl(v["vBC"]))),
                        ft.DataCell(ft.Text(brl(v["vICMS"]))),
                        ft.DataCell(menu),
                        ft.DataCell(icone),
                    ]
                )
            )
        safe_update(tabela_dados)

    # ======= CARREGAMENTO / EXPORTAÇÕES =======
    def carregar_arquivos(file_list):
        if not file_list:
            return
        show_loading("Carregando XMLs...")
        try:
            total = len(file_list)
            novos = []
            for i, f in enumerate(file_list, start=1):
                path = f.path or f.name
                if not os.path.isfile(path):
                    if os.path.isfile(os.path.join(os.getcwd(), path)):
                        path = os.path.join(os.getcwd(), path)
                    else:
                        set_loading_progress(i, total)
                        continue
                try:
                    n = parse_xml_nfe(path)
                    chave = n["chave"]
                    itens_por_chave[chave] = n.get("itens", [])
                    lookup_nota_por_chave[chave] = n
                    novos.append(n)
                except Exception as ex:
                    snack(f"Erro ao parsear {os.path.basename(path)}: {ex}", erro=True)
                set_loading_progress(i, total)
            if novos:
                notas.extend(novos)
            recomputar_kpis()
            recomputar_gestao()
            aplicar_filtros()
        finally:
            hide_loading()

    def limpar(_=None):
        notas.clear()
        itens_por_chave.clear()
        lookup_nota_por_chave.clear()
        aplicar_filtros()

    def snack(msg: str, erro: bool=False):
        page.snack_bar = ft.SnackBar(
            content=ft.Text(msg),
            bgcolor=C.RED_900 if erro else C.BLUE_GREY_900,
        )
        page.snack_bar.open = True
        page.update()

    def exportar_notas_csv(_=None):
        filtradas = get_filtered_notas()
        if not filtradas:
            snack("Nenhuma nota para exportar (considerando filtros).")
            return
        caminho = EXPORT_DIR / "notas_resumo.csv"
        try:
            with open(caminho, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f, delimiter=';')
                writer.writerow([
                    "chave","tipo","modelo","serie","numero","dhEmi",
                    "emitente","destinatario",
                    "vProd","vDesc","vNF","vBC","vICMS","vFCP","vBCST","vST","vPIS","vCOFINS","vTotTrib",
                    "ok_vProd"
                ])
                for n in filtradas:
                    t = n["totais"]
                    tipo_txt = "Entrada" if n.get("tpNF") == "0" else "Saída"
                    writer.writerow([
                        n["chave"], tipo_txt, n["modelo"] or "", n["serie"] or "", n["numero"] or "", n["dhEmi"] or "",
                        (n["emitente"] or "").strip(), (n["destinatario"] or "").strip(),
                        br_dec(t["vProd"]), br_dec(t["vDesc"]), br_dec(t["vNF"]), br_dec(t["vBC"]), br_dec(t["vICMS"]),
                        br_dec(t["vFCP"]), br_dec(t["vBCST"]), br_dec(t["vST"]), br_dec(t["vPIS"]), br_dec(t["vCOFINS"]), br_dec(t["vTotTrib"]),
                        "OK" if not n["divergencia_vProd"] else "DIVERGENTE"
                    ])
            snack(f"Exportado: {caminho}")
        except Exception as ex:
            snack(f"Falha ao exportar CSV: {ex}", erro=True)

    def exportar_itens_csv_todos(_=None):
        filtradas = get_filtered_notas()
        if not filtradas:
            snack("Nenhuma nota carregada (considerando filtros).")
            return
        caminho = EXPORT_DIR / "itens_todas_notas.csv"
        try:
            with open(caminho, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f, delimiter=';')
                writer.writerow(["chave","tipo","nItem","cProd","xProd","NCM","CEST","CFOP",
                                 "qCom","vUnCom","vProd","vDesc","vBC","vICMS","pICMS","ICMS_CST",
                                 "vPIS","vCOFINS","PIS_CST","COFINS_CST","indTot"])
                chaves = {n["chave"] for n in filtradas}
                for chave in chaves:
                    n = lookup_nota_por_chave.get(chave)
                    tipo_txt = "Entrada" if (n and n.get("tpNF") == "0") else "Saída"
                    for it in itens_por_chave.get(chave, []):
                        writer.writerow([
                            chave, tipo_txt, it["nItem"], it["cProd"] or "", (it["xProd"] or "").strip(),
                            it["NCM"] or "", it["CEST"] or "", it["CFOP"] or "",
                            str(it["qCom"]).replace(".", ","),
                            br_dec(it["vUnCom"]), br_dec(it["vProd"]), br_dec(it["vDesc"]),
                            br_dec(it["vBC"]), br_dec(it["vICMS"]), br_dec(it["ICMS_pICMS"]), it["ICMS_CST"],
                            br_dec(it["vPIS"]), br_dec(it["vCOFINS"]), it["PIS_CST"], it["COFINS_CST"],
                            it["indTot"],
                        ])
            snack(f"Exportado: {caminho}")
        except Exception as ex:
            snack(f"Falha ao exportar CSV: {ex}", erro=True)

    def exportar_divergencias(_=None):
        filtradas = get_filtered_notas()
        diverg = [n for n in filtradas if n.get("divergencia_vProd")]
        if not diverg:
            snack("Sem divergências (considerando filtros).")
            return
        caminho = EXPORT_DIR / "divergencias.csv"
        with open(caminho, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, delimiter=';')
            w.writerow(["chave","tipo","numero","serie","emitente","vProd(ICMSTot)","itens indTot=1"])
            for n in diverg:
                w.writerow([
                    n["chave"],
                    "Entrada" if n.get("tpNF") == "0" else "Saída",
                    n["numero"], n["serie"], (n["emitente"] or "").strip(),
                    br_dec(n["totais"]["vProd"]),
                    br_dec(n["somatorio_itens_vProd_indTot1"]),
                ])
        snack(f"Exportado: {caminho}")

    def exportar_resumo_periodo(periodo="mensal"):
        filtradas = get_filtered_notas()
        if not filtradas:
            snack("Sem notas para resumir (considerando filtros).")
            return
        buckets = {}
        for n in filtradas:
            d = parse_dhEmi_to_date(n.get("dhEmi"))
            if not d:
                continue
            if periodo == "mensal":
                key = f"{d.year}-{d.month:02d}"
            else:
                t = (d.month-1)//3 + 1
                key = f"{d.year}-T{t}"
            b = buckets.setdefault(key, {"E": [], "S": []})
            (b["E"] if n.get("tpNF")=="0" else b["S"]).append(n)

        caminho = EXPORT_DIR / (f"resumo_{periodo}.csv")
        with open(caminho, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, delimiter=';')
            w.writerow(["periodo","tipo","vProd","vNF","vDesc","vBC","vICMS","vFCP","vST","vPIS","vCOFINS","vTotTrib",
                        "aliq_ICMS_eff(%)","desc_%_sobre_vProd(%)"])
            for k, dct in sorted(buckets.items()):
                for tipo_key, arr in dct.items():
                    if not arr:
                        continue
                    def soma(notas_l, campo):
                        return sum((n["totais"][campo] for n in notas_l), Decimal("0.00"))
                    vProd = soma(arr, "vProd"); vNF = soma(arr, "vNF"); vDesc = soma(arr, "vDesc")
                    vBC = soma(arr, "vBC"); vICMS = soma(arr, "vICMS"); vFCP = soma(arr, "vFCP")
                    vST = soma(arr, "vST"); vPIS = soma(arr, "vPIS"); vCOF = soma(arr, "vCOFINS"); vTT = soma(arr, "vTotTrib")
                    aliq = float((vICMS / vBC * Decimal(100)) if vBC > 0 else Decimal(0))
                    desc_pct = float((vDesc / vProd * Decimal(100)) if vProd > 0 else Decimal(0))
                    w.writerow([k, ("Entrada" if tipo_key=="E" else "Saída"),
                                br_dec(vProd), br_dec(vNF), br_dec(vDesc), br_dec(vBC), br_dec(vICMS),
                                br_dec(vFCP), br_dec(vST), br_dec(vPIS), br_dec(vCOF), br_dec(vTT),
                                str(aliq).replace(".", ","), str(desc_pct).replace(".", ",")])
        snack(f"Exportado: {caminho}")

    # ======= GESTÃO – DETALHAMENTOS =======
    ncm_all_table = ft.DataTable(columns=[
        ft.DataColumn(ft.Text("NCM")),
        ft.DataColumn(ft.Text("vProd total")),
        ft.DataColumn(ft.Text("Líquido total")),
        ft.DataColumn(ft.Text("Desconto total")),
        ft.DataColumn(ft.Text("Itens"))
    ], rows=[], heading_row_height=36, data_row_min_height=36)

    cfop_e_table = ft.DataTable(columns=[
        ft.DataColumn(ft.Text("CFOP")),
        ft.DataColumn(ft.Text("CST ICMS")),
        ft.DataColumn(ft.Text("Alíquota ICMS (%)")),
        ft.DataColumn(ft.Text("vProd total")),
        ft.DataColumn(ft.Text("Líquido total")),
        ft.DataColumn(ft.Text("Desconto total")),
        ft.DataColumn(ft.Text("Itens"))
    ], rows=[], heading_row_height=36, data_row_min_height=36)

    cfop_s_table = ft.DataTable(columns=[
        ft.DataColumn(ft.Text("CFOP")),
        ft.DataColumn(ft.Text("CST ICMS")),
        ft.DataColumn(ft.Text("Alíquota ICMS (%)")),
        ft.DataColumn(ft.Text("vProd total")),
        ft.DataColumn(ft.Text("Líquido total")),
        ft.DataColumn(ft.Text("Desconto total")),
        ft.DataColumn(ft.Text("Itens"))
    ], rows=[], heading_row_height=36, data_row_min_height=36)

    # NOVOS: Tabelas PIS/COFINS separadas
    piscofins_cst_table_e = ft.DataTable(columns=[
        ft.DataColumn(ft.Text("CST PIS")),
        ft.DataColumn(ft.Text("CST COFINS")),
        ft.DataColumn(ft.Text("vPIS total")),
        ft.DataColumn(ft.Text("vCOFINS total")),
        ft.DataColumn(ft.Text("vProd total")),
        ft.DataColumn(ft.Text("Líquido total")),
        ft.DataColumn(ft.Text("Desconto total")),
        ft.DataColumn(ft.Text("Itens"))
    ], rows=[], heading_row_height=36, data_row_min_height=36)

    piscofins_cst_table_s = ft.DataTable(columns=[
        ft.DataColumn(ft.Text("CST PIS")),
        ft.DataColumn(ft.Text("CST COFINS")),
        ft.DataColumn(ft.Text("vPIS total")),
        ft.DataColumn(ft.Text("vCOFINS total")),
        ft.DataColumn(ft.Text("vProd total")),
        ft.DataColumn(ft.Text("Líquido total")),
        ft.DataColumn(ft.Text("Desconto total")),
        ft.DataColumn(ft.Text("Itens"))
    ], rows=[], heading_row_height=36, data_row_min_height=36)

    def recomputar_gestao():
        filtradas = get_filtered_notas()

        # ============================================================
        # 1) NCM (TODOS) – respeita filtros e inclui vProd/vLiq/vDesc
        # ============================================================
        acc_ncm_vprod: dict[str, Decimal] = {}
        acc_ncm_vdesc: dict[str, Decimal] = {}
        cnt_ncm: dict[str, int] = {}

        for n in filtradas:
            chave = n.get("chave")
            for it in itens_por_chave.get(chave, []):
                ncm = (it.get("NCM") or "").strip()
                vprod = it.get("vProd", Decimal("0.00"))
                vdesc = it.get("vDesc", Decimal("0.00"))
                acc_ncm_vprod[ncm] = acc_ncm_vprod.get(ncm, Decimal("0.00")) + vprod
                acc_ncm_vdesc[ncm] = acc_ncm_vdesc.get(ncm, Decimal("0.00")) + vdesc
                cnt_ncm[ncm] = cnt_ncm.get(ncm, 0) + 1

        ncm_rows = []
        for ncm, vprod in sorted(acc_ncm_vprod.items(), key=lambda x: x[1], reverse=True):
            vdesc = acc_ncm_vdesc.get(ncm, Decimal("0.00"))
            vliq = vprod - vdesc
            ncm_rows.append(
                ft.DataRow(
                    cells=[
                        ft.DataCell(ft.Text(ncm)),
                        ft.DataCell(ft.Text(brl(vprod))),
                        ft.DataCell(ft.Text(brl(vliq))),
                        ft.DataCell(ft.Text(brl(vdesc))),
                        ft.DataCell(ft.Text(str(cnt_ncm.get(ncm, 0)))),
                    ]
                )
            )
        ncm_all_table.rows = ncm_rows

        # ============================================================
        # 2) CFOP por (CFOP, ICMS_CST, pICMS) – separado Entradas/Saídas
        #    + vProd/vLiq/vDesc
        # ============================================================
        def agrega_cfop(tipo_alvo: str):
            acc_vprod: dict[tuple[str, str, Decimal], Decimal] = {}
            acc_vdesc: dict[tuple[str, str, Decimal], Decimal] = {}
            cnt: dict[tuple[str, str, Decimal], int] = {}

            for n in filtradas:
                if n.get("tpNF") != tipo_alvo:
                    continue
                chave = n.get("chave")
                for it in itens_por_chave.get(chave, []):
                    cfop = (it.get("CFOP") or "").strip()
                    cst = (it.get("ICMS_CST") or "").strip()
                    aliq = it.get("ICMS_pICMS", Decimal("0.00"))
                    key = (cfop, cst, aliq)

                    vprod = it.get("vProd", Decimal("0.00"))
                    vdesc = it.get("vDesc", Decimal("0.00"))

                    acc_vprod[key] = acc_vprod.get(key, Decimal("0.00")) + vprod
                    acc_vdesc[key] = acc_vdesc.get(key, Decimal("0.00")) + vdesc
                    cnt[key] = cnt.get(key, 0) + 1

            return acc_vprod, acc_vdesc, cnt

        acc_e_vprod, acc_e_vdesc, cnt_e = agrega_cfop("0")
        acc_s_vprod, acc_s_vdesc, cnt_s = agrega_cfop("1")

        def monta_rows_cfop(acc_vprod, acc_vdesc, cnt):
            rows = []
            for (cfop, cst, aliq), vprod in sorted(acc_vprod.items(), key=lambda x: x[1], reverse=True):
                vdesc = acc_vdesc.get((cfop, cst, aliq), Decimal("0.00"))
                vliq = vprod - vdesc
                rows.append(
                    ft.DataRow(
                        cells=[
                            ft.DataCell(ft.Text(cfop)),
                            ft.DataCell(ft.Text(cst)),
                            ft.DataCell(ft.Text(br_pct(float(aliq)))),
                            ft.DataCell(ft.Text(brl(vprod))),
                            ft.DataCell(ft.Text(brl(vliq))),
                            ft.DataCell(ft.Text(brl(vdesc))),
                            ft.DataCell(ft.Text(str(cnt.get((cfop, cst, aliq), 0)))),
                        ]
                    )
                )
            return rows

        cfop_e_table.rows = monta_rows_cfop(acc_e_vprod, acc_e_vdesc, cnt_e)
        cfop_s_table.rows = monta_rows_cfop(acc_s_vprod, acc_s_vdesc, cnt_s)

        # ============================================================
        # 3) CST PIS/COFINS – separado Entradas/Saídas
        #    Apenas itens com vPIS>0 e vCOFINS>0
        #    + vProd/vLiq/vDesc
        # ============================================================
        def agrega_pc(tipo_alvo: str):
            acc_vprod: dict[tuple[str, str], Decimal] = {}
            acc_vdesc: dict[tuple[str, str], Decimal] = {}
            cnt: dict[tuple[str, str], int] = {}
            tot_pis: dict[tuple[str, str], Decimal] = {}
            tot_cof: dict[tuple[str, str], Decimal] = {}

            for n in filtradas:
                if n.get("tpNF") != tipo_alvo:
                    continue
                chave = n.get("chave")
                for it in itens_por_chave.get(chave, []):
                    vpis = it.get("vPIS", Decimal("0.00"))
                    vcof = it.get("vCOFINS", Decimal("0.00"))
                    if not (vpis > 0 and vcof > 0):
                        continue

                    key = ((it.get("PIS_CST") or "").strip(), (it.get("COFINS_CST") or "").strip())

                    vprod = it.get("vProd", Decimal("0.00"))
                    vdesc = it.get("vDesc", Decimal("0.00"))

                    acc_vprod[key] = acc_vprod.get(key, Decimal("0.00")) + vprod
                    acc_vdesc[key] = acc_vdesc.get(key, Decimal("0.00")) + vdesc
                    cnt[key] = cnt.get(key, 0) + 1
                    tot_pis[key] = tot_pis.get(key, Decimal("0.00")) + vpis
                    tot_cof[key] = tot_cof.get(key, Decimal("0.00")) + vcof

            return acc_vprod, acc_vdesc, cnt, tot_pis, tot_cof

        acc_e_vprod, acc_e_vdesc, cnt_e, tot_pis_e, tot_cof_e = agrega_pc("0")
        acc_s_vprod, acc_s_vdesc, cnt_s, tot_pis_s, tot_cof_s = agrega_pc("1")

        def monta_rows_pc(acc_vprod, acc_vdesc, cnt, tot_pis, tot_cof):
            rows = []
            for (cst_p, cst_c), vprod in sorted(acc_vprod.items(), key=lambda x: x[1], reverse=True):
                vdesc = acc_vdesc.get((cst_p, cst_c), Decimal("0.00"))
                vliq = vprod - vdesc
                rows.append(
                    ft.DataRow(
                        cells=[
                            ft.DataCell(ft.Text(cst_p)),
                            ft.DataCell(ft.Text(cst_c)),
                            ft.DataCell(ft.Text(brl(tot_pis.get((cst_p, cst_c), Decimal("0.00"))))),
                            ft.DataCell(ft.Text(brl(tot_cof.get((cst_p, cst_c), Decimal("0.00"))))),
                            ft.DataCell(ft.Text(brl(vprod))),
                            ft.DataCell(ft.Text(brl(vliq))),
                            ft.DataCell(ft.Text(brl(vdesc))),
                            ft.DataCell(ft.Text(str(cnt.get((cst_p, cst_c), 0)))),
                        ]
                    )
                )
            return rows

        piscofins_cst_table_e.rows = monta_rows_pc(acc_e_vprod, acc_e_vdesc, cnt_e, tot_pis_e, tot_cof_e)
        piscofins_cst_table_s.rows = monta_rows_pc(acc_s_vprod, acc_s_vdesc, cnt_s, tot_pis_s, tot_cof_s)

        safe_update(ncm_all_table)
        safe_update(cfop_e_table)
        safe_update(cfop_s_table)
        safe_update(piscofins_cst_table_e)
        safe_update(piscofins_cst_table_s)
        page.update()
    def exportar_ncm_todos_csv(_=None):
        filtradas = get_filtered_notas()
        if not filtradas:
            snack("Nenhuma nota filtrada para exportar.", erro=True)
            return

        caminho = EXPORT_DIR / "gestao_ncm_todos.csv"

        acc_vprod: dict[str, Decimal] = {}
        acc_vdesc: dict[str, Decimal] = {}
        cnt: dict[str, int] = {}

        for n in filtradas:
            for it in n.get("itens", []):
                ncm = (it.get("NCM") or "").strip() or "SEM_NCM"
                vprod = Decimal(str(it.get("vProd", 0) or 0))
                vdesc = Decimal(str(it.get("vDesc", 0) or 0))
                acc_vprod[ncm] = acc_vprod.get(ncm, Decimal("0.00")) + vprod
                acc_vdesc[ncm] = acc_vdesc.get(ncm, Decimal("0.00")) + vdesc
                cnt[ncm] = cnt.get(ncm, 0) + 1

        rows = []
        for ncm, vprod in sorted(acc_vprod.items(), key=lambda x: x[1], reverse=True):
            vdesc = acc_vdesc.get(ncm, Decimal("0.00"))
            vliq = vprod - vdesc
            rows.append((ncm, vprod, vdesc, vliq, cnt.get(ncm, 0)))

        with open(caminho, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow(["NCM", "vProd_total", "vLiq_total", "vDesc_total", "itens"])
            for ncm, vprod, vdesc, vliq, c in rows:
                w.writerow([ncm, br_dec(vprod), br_dec(vliq), br_dec(vdesc), c])

        snack(f"CSV exportado: {caminho}")

    def exportar_cfop_e_csv(_=None):
        filtradas = get_filtered_notas()
        if not filtradas:
            snack("Nenhuma nota filtrada para exportar.", erro=True)
            return
        caminho = EXPORT_DIR / "gestao_cfop_entrada.csv"

        acc_vprod: dict[tuple[str, str, str], Decimal] = {}
        acc_vdesc: dict[tuple[str, str, str], Decimal] = {}
        cnt: dict[tuple[str, str, str], int] = {}

        for n in filtradas:
            if n.get("tpNF") != "0":
                continue
            for it in n.get("itens", []):
                key = (
                    (it.get("CFOP") or "SEM_CFOP"),
                    (it.get("ICMS_CST") or "SEM_CST"),
                    str(it.get("pICMS", Decimal("0"))),
                )
                vprod = Decimal(str(it.get("vProd", 0) or 0))
                vdesc = Decimal(str(it.get("vDesc", 0) or 0))
                acc_vprod[key] = acc_vprod.get(key, Decimal("0.00")) + vprod
                acc_vdesc[key] = acc_vdesc.get(key, Decimal("0.00")) + vdesc
                cnt[key] = cnt.get(key, 0) + 1

        with open(caminho, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow(["CFOP", "ICMS_CST", "pICMS(%)", "vProd_total", "vLiq_total", "vDesc_total", "itens"])
            for (cfop, cst, aliq), vprod in sorted(acc_vprod.items(), key=lambda x: x[1], reverse=True):
                vdesc = acc_vdesc.get((cfop, cst, aliq), Decimal("0.00"))
                vliq = vprod - vdesc
                w.writerow([cfop, cst, aliq, br_dec(vprod), br_dec(vliq), br_dec(vdesc), cnt.get((cfop, cst, aliq), 0)])

        snack(f"Exportado: {caminho}")

    def exportar_cfop_s_csv(_=None):
        filtradas = get_filtered_notas()
        if not filtradas:
            snack("Nenhuma nota filtrada para exportar.", erro=True)
            return
        caminho = EXPORT_DIR / "gestao_cfop_saida.csv"

        acc_vprod: dict[tuple[str, str, str], Decimal] = {}
        acc_vdesc: dict[tuple[str, str, str], Decimal] = {}
        cnt: dict[tuple[str, str, str], int] = {}

        for n in filtradas:
            if n.get("tpNF") != "1":
                continue
            for it in n.get("itens", []):
                key = (
                    (it.get("CFOP") or "SEM_CFOP"),
                    (it.get("ICMS_CST") or "SEM_CST"),
                    str(it.get("pICMS", Decimal("0"))),
                )
                vprod = Decimal(str(it.get("vProd", 0) or 0))
                vdesc = Decimal(str(it.get("vDesc", 0) or 0))
                acc_vprod[key] = acc_vprod.get(key, Decimal("0.00")) + vprod
                acc_vdesc[key] = acc_vdesc.get(key, Decimal("0.00")) + vdesc
                cnt[key] = cnt.get(key, 0) + 1

        with open(caminho, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow(["CFOP", "ICMS_CST", "pICMS(%)", "vProd_total", "vLiq_total", "vDesc_total", "itens"])
            for (cfop, cst, aliq), vprod in sorted(acc_vprod.items(), key=lambda x: x[1], reverse=True):
                vdesc = acc_vdesc.get((cfop, cst, aliq), Decimal("0.00"))
                vliq = vprod - vdesc
                w.writerow([cfop, cst, aliq, br_dec(vprod), br_dec(vliq), br_dec(vdesc), cnt.get((cfop, cst, aliq), 0)])

        snack(f"Exportado: {caminho}")

    def exportar_piscofins_cst_csv_e(_=None):
        filtradas = get_filtered_notas()
        if not filtradas:
            snack("Nenhuma nota filtrada para exportar.", erro=True)
            return
        caminho = EXPORT_DIR / "gestao_piscofins_cst_entrada.csv"

        acc_vprod: dict[tuple[str, str], Decimal] = {}
        acc_vdesc: dict[tuple[str, str], Decimal] = {}
        cnt: dict[tuple[str, str], int] = {}
        tot_pis: dict[tuple[str, str], Decimal] = {}
        tot_cof: dict[tuple[str, str], Decimal] = {}

        for n in filtradas:
            if n.get("tpNF") != "0":
                continue
            for it in n.get("itens", []):
                if (it.get("vPIS", Decimal("0")) or Decimal("0")) <= 0 and (it.get("vCOFINS", Decimal("0")) or Decimal("0")) <= 0:
                    continue
                key = ((it.get("PIS_CST") or "SEM_CST"), (it.get("COFINS_CST") or "SEM_CST"))
                vprod = Decimal(str(it.get("vProd", 0) or 0))
                vdesc = Decimal(str(it.get("vDesc", 0) or 0))
                vpis = Decimal(str(it.get("vPIS", 0) or 0))
                vcof = Decimal(str(it.get("vCOFINS", 0) or 0))

                acc_vprod[key] = acc_vprod.get(key, Decimal("0.00")) + vprod
                acc_vdesc[key] = acc_vdesc.get(key, Decimal("0.00")) + vdesc
                cnt[key] = cnt.get(key, 0) + 1
                tot_pis[key] = tot_pis.get(key, Decimal("0.00")) + vpis
                tot_cof[key] = tot_cof.get(key, Decimal("0.00")) + vcof

        with open(caminho, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow(["CST_PIS", "CST_COFINS", "vPIS_total", "vCOFINS_total", "vProd_total", "vLiq_total", "vDesc_total", "itens"])
            for (cst_p, cst_c), vprod in sorted(acc_vprod.items(), key=lambda x: x[1], reverse=True):
                vdesc = acc_vdesc.get((cst_p, cst_c), Decimal("0.00"))
                vliq = vprod - vdesc
                w.writerow([
                    cst_p, cst_c,
                    br_dec(tot_pis.get((cst_p, cst_c), Decimal("0.00"))),
                    br_dec(tot_cof.get((cst_p, cst_c), Decimal("0.00"))),
                    br_dec(vprod),
                    br_dec(vliq),
                    br_dec(vdesc),
                    cnt.get((cst_p, cst_c), 0),
                ])
        snack(f"Exportado: {caminho}")

    def exportar_piscofins_cst_csv_s(_=None):
        filtradas = get_filtered_notas()
        if not filtradas:
            snack("Nenhuma nota filtrada para exportar.", erro=True)
            return
        caminho = EXPORT_DIR / "gestao_piscofins_cst_saida.csv"

        acc_vprod: dict[tuple[str, str], Decimal] = {}
        acc_vdesc: dict[tuple[str, str], Decimal] = {}
        cnt: dict[tuple[str, str], int] = {}
        tot_pis: dict[tuple[str, str], Decimal] = {}
        tot_cof: dict[tuple[str, str], Decimal] = {}

        for n in filtradas:
            if n.get("tpNF") != "1":
                continue
            for it in n.get("itens", []):
                if (it.get("vPIS", Decimal("0")) or Decimal("0")) <= 0 and (it.get("vCOFINS", Decimal("0")) or Decimal("0")) <= 0:
                    continue
                key = ((it.get("PIS_CST") or "SEM_CST"), (it.get("COFINS_CST") or "SEM_CST"))
                vprod = Decimal(str(it.get("vProd", 0) or 0))
                vdesc = Decimal(str(it.get("vDesc", 0) or 0))
                vpis = Decimal(str(it.get("vPIS", 0) or 0))
                vcof = Decimal(str(it.get("vCOFINS", 0) or 0))

                acc_vprod[key] = acc_vprod.get(key, Decimal("0.00")) + vprod
                acc_vdesc[key] = acc_vdesc.get(key, Decimal("0.00")) + vdesc
                cnt[key] = cnt.get(key, 0) + 1
                tot_pis[key] = tot_pis.get(key, Decimal("0.00")) + vpis
                tot_cof[key] = tot_cof.get(key, Decimal("0.00")) + vcof

        with open(caminho, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow(["CST_PIS", "CST_COFINS", "vPIS_total", "vCOFINS_total", "vProd_total", "vLiq_total", "vDesc_total", "itens"])
            for (cst_p, cst_c), vprod in sorted(acc_vprod.items(), key=lambda x: x[1], reverse=True):
                vdesc = acc_vdesc.get((cst_p, cst_c), Decimal("0.00"))
                vliq = vprod - vdesc
                w.writerow([
                    cst_p, cst_c,
                    br_dec(tot_pis.get((cst_p, cst_c), Decimal("0.00"))),
                    br_dec(tot_cof.get((cst_p, cst_c), Decimal("0.00"))),
                    br_dec(vprod),
                    br_dec(vliq),
                    br_dec(vdesc),
                    cnt.get((cst_p, cst_c), 0),
                ])
        snack(f"Exportado: {caminho}")

    def aplicar_filtros(_=None):
        nonlocal page_index
        page_index = 0
        recomputar_kpis()
        recomputar_gestao()
        preencher_tabela()

    def limpar_filtros(_=None):
        data_de.value = ""; data_ate.value = ""
        emitente_f.value = ""; destinatario_f.value = ""
        modelo_f.value = "Ambos"; tipo_f.value = "Todas"
        busca_f.value = ""
        aplicar_filtros()

    # =========================
    # === VIEW: / (raiz) ======
    # =========================
    filtros_row = ft.Row(
        controls=[data_de, data_ate, emitente_f, destinatario_f, modelo_f, tipo_f, busca_f,
                  ft.FilledButton("Aplicar", icon=I.FILTER_ALT, on_click=aplicar_filtros),
                  ft.OutlinedButton("Limpar filtros", icon=I.CLEAR_ALL, on_click=limpar_filtros)],
        spacing=8, wrap=True
    )

    # KPIs gerais
    kpi_cards_geral = ft.Row(
        controls=[
            _kpi_card("Saldo Bruto (Saídas − Entradas)", kpi_saldo_bruto, I.SSID_CHART, C.BLUE_200),
            _kpi_card("Saldo Líquido (Saídas − Entradas)", kpi_saldo_liquido, I.SHOW_CHART, C.BLUE_200),
            _kpi_card("Desconto Médio nas Saídas (%)", kpi_desc_pct_saida, I.PERCENT, C.BLUE_200),
            _kpi_card("Alíquota Efetiva ICMS Saídas (%)", kpi_aliq_icms_eff, I.PIE_CHART, C.BLUE_200),
            _kpi_card("% Notas com Divergência", kpi_diverg_pct, I.WARNING_AMBER, C.AMBER_200),
        ],
        spacing=12, wrap=False, alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
    )

    # KPIs Entradas
    kpi_cards_entradas = ft.Row(
        controls=[
            _kpi_card("ENTRADA: Bruto", kpi_e_bruto, I.INPUT, C.CYAN_200),
            _kpi_card("ENTRADA: Líquido", kpi_e_liquido, I.RECEIPT, C.CYAN_200),
            _kpi_card("ENTRADA: Descontos", kpi_e_desc, I.LOCAL_OFFER, C.CYAN_200),
            _kpi_card("ENTRADA: Base ICMS", kpi_e_bc, I.ACCOUNT_BALANCE, C.CYAN_200),
            _kpi_card("ENTRADA: ICMS", kpi_e_icms, I.PIE_CHART, C.CYAN_200),
        ],
        spacing=12, wrap=False, alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
    )
    kpi_cards_entradas_fisc = ft.Row(
        controls=[
            _kpi_card("ENTRADA: FCP (vFCP)", kpi_e_fcp, I.SAVINGS, C.CYAN_200),
            _kpi_card("ENTRADA: ST (vST)", kpi_e_st, I.REQUEST_QUOTE, C.CYAN_200),
            _kpi_card("ENTRADA: PIS", kpi_e_pis, I.RECEIPT_LONG, C.CYAN_200),
            _kpi_card("ENTRADA: COFINS", kpi_e_cofins, I.RECEIPT_LONG, C.CYAN_200),
            _kpi_card("ENTRADA: vTotTrib", kpi_e_tottrib, I.ASSIGNMENT, C.CYAN_200),
        ],
        spacing=12, wrap=False, alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
    )

    # KPIs Saídas
    kpi_cards_saidas = ft.Row(
        controls=[
            _kpi_card("SAÍDA: Bruto", kpi_s_bruto, I.LAUNCH, C.TEAL_200),
            _kpi_card("SAÍDA: Líquido", kpi_s_liquido, I.RECEIPT, C.TEAL_200),
            _kpi_card("SAÍDA: Descontos", kpi_s_desc, I.LOCAL_OFFER, C.TEAL_200),
            _kpi_card("SAÍDA: Base ICMS", kpi_s_bc, I.ACCOUNT_BALANCE, C.TEAL_200),
            _kpi_card("SAÍDA: ICMS", kpi_s_icms, I.PIE_CHART, C.TEAL_200),
        ],
        spacing=12, wrap=False, alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
    )
    kpi_cards_saidas_fisc = ft.Row(
        controls=[
            _kpi_card("SAÍDA: FCP (vFCP)", kpi_s_fcp, I.SAVINGS, C.TEAL_200),
            _kpi_card("SAÍDA: ST (vST)", kpi_s_st, I.REQUEST_QUOTE, C.TEAL_200),
            _kpi_card("SAÍDA: PIS", kpi_s_pis, I.RECEIPT_LONG, C.TEAL_200),
            _kpi_card("SAÍDA: COFINS", kpi_s_cofins, I.RECEIPT_LONG, C.TEAL_200),
            _kpi_card("SAÍDA: vTotTrib", kpi_s_tottrib, I.ASSIGNMENT, C.TEAL_200,
                      subtitle=f"PIS eff / COFINS eff: {kpi_s_pis_eff.value} / {kpi_s_cofins_eff.value}"),
        ],
        spacing=12, wrap=False, alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
    )

    # Alertas - painel
    alertas_panel = ft.ExpansionTile(
        title=ft.Text("Alertas de Auditoria"),
        affinity=ft.TileAffinity.PLATFORM,
        initially_expanded=False,
        controls=[
            ft.Row([
                _kpi_card("CFOP incompatível com tpNF", alerta_cfop_tp_count, I.RULE, C.AMBER_200),
                _kpi_card("NCM ausente/inválido", alerta_ncm_inval_count, I.QR_CODE_2, C.AMBER_200),
                _kpi_card("ICMS 0 com vBC>0", alerta_icms0_vbcpos_count, I.REPORT_GMAILERRORRED, C.AMBER_200),
                _kpi_card("vNF fora da fórmula (aprox.)", alerta_vnf_formula_count, I.SUMMARIZE, C.AMBER_200),
            ], spacing=12),
        ]
    )

    # Painéis recolhíveis para KPIs
    kpi_panel_geral = ft.ExpansionTile(
        title=ft.Text("KPIs Gerais"),
        affinity=ft.TileAffinity.PLATFORM,
        initially_expanded=True,
        controls=[kpi_cards_geral],
    )
    kpi_panel_entradas_all = ft.ExpansionTile(
        title=ft.Text("KPIs Entradas"),
        affinity=ft.TileAffinity.PLATFORM,
        initially_expanded=False,
        controls=[
            kpi_cards_entradas,
            kpi_cards_entradas_fisc,
        ],
    )
    kpi_panel_saidas_all = ft.ExpansionTile(
        title=ft.Text("KPIs Saídas"),
        affinity=ft.TileAffinity.PLATFORM,
        initially_expanded=False,
        controls=[
            kpi_cards_saidas,
            kpi_cards_saidas_fisc,
        ],
    )

    # Botões principais
    botoes = ft.Row(
        controls=[
            ft.FilledButton(
                text="Selecionar XMLs…",
                icon=I.UPLOAD_FILE,
                on_click=lambda e: file_picker.pick_files(allow_multiple=True, allowed_extensions=["xml"]),
            ),
            ft.PopupMenuButton(
                content=ft.ElevatedButton(text="Exportar", icon=I.DOWNLOAD),
                items=[
                    ft.PopupMenuItem(text="Notas (CSV)", icon=I.TABLE_VIEW, on_click=lambda e: exportar_notas_csv()),
                    ft.PopupMenuItem(text="Itens (todas) CSV", icon=I.DOWNLOAD, on_click=lambda e: exportar_itens_csv_todos()),
                    ft.PopupMenuItem(text="Divergências", icon=I.WARNING_AMBER, on_click=lambda e: exportar_divergencias()),
                    ft.PopupMenuItem(text="────────────", disabled=True),
                    ft.PopupMenuItem(text="Resumo Mensal (CSV)", icon=I.CALENDAR_MONTH, on_click=lambda e: exportar_resumo_periodo("mensal")),
                    ft.PopupMenuItem(text="Resumo Trimestral (CSV)", icon=I.DATE_RANGE, on_click=lambda e: exportar_resumo_periodo("trimestral")),
                ],
            ),
            ft.OutlinedButton("Limpar", icon=I.DELETE_SWEEP, on_click=limpar),
            ft.Container(expand=True),
            page_size_dd,
            ft.IconButton(I.ARROW_BACK, tooltip="Página anterior", on_click=lambda e: goto_page(-1)),
            page_label,
            ft.IconButton(I.ARROW_FORWARD, tooltip="Próxima página", on_click=lambda e: goto_page(1)),
        ],
        spacing=10,
    )

    # ======= VIEW HOME =======
    gestao_detalhes_header = ft.Text("Gestão – Detalhamentos (NCM / CFOP / CST PIS-COFINS)", size=18, weight=ft.FontWeight.BOLD)

    ncm_all_block = ft.Column([
        ft.Row([ft.Text("NCM – Total por vProd (respeita filtros)", size=14),
                ft.OutlinedButton("Exportar NCM (CSV)", icon=I.DOWNLOAD, on_click=exportar_ncm_todos_csv)]),
        ft.Row([ncm_all_table], scroll=ft.ScrollMode.AUTO)
    ], expand=True)

    cfop_e_block = ft.Column([
        ft.Row([ft.Text("CFOP (Entradas) – por CFOP, CST ICMS, Alíquota", size=14),
                ft.OutlinedButton("Exportar CFOP Entradas (CSV)", icon=I.DOWNLOAD, on_click=exportar_cfop_e_csv)]),
        ft.Row([cfop_e_table], scroll=ft.ScrollMode.AUTO)
    ], expand=True)

    cfop_s_block = ft.Column([
        ft.Row([ft.Text("CFOP (Saídas) – por CFOP, CST ICMS, Alíquota", size=14),
                ft.OutlinedButton("Exportar CFOP Saídas (CSV)", icon=I.DOWNLOAD, on_click=exportar_cfop_s_csv)]),
        ft.Row([cfop_s_table], scroll=ft.ScrollMode.AUTO)
    ], expand=True)

    piscofins_block_e = ft.Column([
        ft.Row([ft.Text("CST PIS/COFINS – ENTRADAS (apenas itens com vPIS>0 e vCOFINS>0)", size=14),
                ft.OutlinedButton("Exportar CST PIS/COFINS ENTRADAS (CSV)", icon=I.DOWNLOAD, on_click=exportar_piscofins_cst_csv_e)]),
        ft.Row([piscofins_cst_table_e], scroll=ft.ScrollMode.AUTO)
    ], expand=True)

    piscofins_block_s = ft.Column([
        ft.Row([ft.Text("CST PIS/COFINS – SAÍDAS (apenas itens com vPIS>0 e vCOFINS>0)", size=14),
                ft.OutlinedButton("Exportar CST PIS/COFINS SAÍDAS (CSV)", icon=I.DOWNLOAD, on_click=exportar_piscofins_cst_csv_s)]),
        ft.Row([piscofins_cst_table_s], scroll=ft.ScrollMode.AUTO)
    ], expand=True)

    


    # >>> PATCH: Tabs Gestão Detalhamentos – BEGIN
    gestao_tabs = ft.Tabs(
        selected_index=0,
        animation_duration=250,
        tab_alignment=ft.TabAlignment.START,
        indicator_tab_size=True,
        expand=1,
        tabs=[
            ft.Tab(text="NCM", icon=I.CATEGORY, content=ncm_all_block),
            ft.Tab(text="CFOP – Entradas", icon=I.LOGIN, content=cfop_e_block),
            ft.Tab(text="CFOP – Saídas", icon=I.LOGOUT, content=cfop_s_block),
            ft.Tab(text="PIS/COFINS – Entradas", icon=I.RECEIPT_LONG, content=piscofins_block_e),
            ft.Tab(text="PIS/COFINS – Saídas", icon=I.RECEIPT_LONG, content=piscofins_block_s),
        ],
    )
    # >>> PATCH: Tabs Gestão Detalhamentos – END
    home_view = ft.View(
        route="/",
        scroll=ft.ScrollMode.AUTO,
        controls=[
            ft.Text("Auditoria de XMLs – NF-e (55) e NFC-e (65)", size=22, weight=ft.FontWeight.BOLD),
            header_loading_banner,
            filtros_row,
            ft.Divider(height=8, color=op(0.2, C.GREY)),
            kpi_panel_geral,
            ft.Divider(height=8, color=op(0.2, C.GREY)),
            kpi_panel_entradas_all,
            ft.Divider(height=8, color=op(0.2, C.GREY)),
            kpi_panel_saidas_all,
            ft.Divider(height=12, color=op(0.2, C.GREY)),
            alertas_panel,
            ft.Divider(height=12, color=op(0.2, C.GREY)),
            botoes,
            ft.Container(content=ft.Column([ft.Row([tabela_dados], scroll=ft.ScrollMode.AUTO)], expand=True, scroll=ft.ScrollMode.AUTO), expand=True),
            ft.Divider(height=12, color=op(0.2, C.GREY)),
            gestao_detalhes_header,
            gestao_tabs,
            ],
        padding=8,
    )

    # =========================
    # === VIEW: itens XXXXX ===
    # =========================
    itens_table = ft.DataTable(
        columns=[
            ft.DataColumn(ft.Text("#")),
            ft.DataColumn(ft.Text("cProd")),
            ft.DataColumn(ft.Text("xProd")),
            ft.DataColumn(ft.Text("NCM")),
            ft.DataColumn(ft.Text("CFOP")),
            ft.DataColumn(ft.Text("qCom")),
            ft.DataColumn(ft.Text("vUnCom")),
            ft.DataColumn(ft.Text("vProd")),
            ft.DataColumn(ft.Text("vDesc")),
            ft.DataColumn(ft.Text("vBC (item)")),
            ft.DataColumn(ft.Text("vICMS (item)")),
            ft.DataColumn(ft.Text("pICMS")),
            ft.DataColumn(ft.Text("ICMS_CST")),
            ft.DataColumn(ft.Text("vPIS (item)")),
            ft.DataColumn(ft.Text("vCOFINS (item)")),
            ft.DataColumn(ft.Text("PIS_CST")),
            ft.DataColumn(ft.Text("COFINS_CST")),
            ft.DataColumn(ft.Text("indTot")),
        ],
        rows=[],
        heading_row_height=38,
        data_row_min_height=38,
    )

    titulo_itens = ft.Text("", size=20, weight=ft.FontWeight.BOLD)

    def preencher_itens(chave: str):
        itens_table.rows.clear()
        lista = itens_por_chave.get(chave, [])
        for it in lista:
            itens_table.rows.append(
                ft.DataRow(
                    cells=[
                        ft.DataCell(ft.Text(str(it["nItem"]))),
                        ft.DataCell(ft.Text(it["cProd"] or "")),
                        ft.DataCell(ft.Text(_short(it["xProd"]))),
                        ft.DataCell(ft.Text(it["NCM"] or "")),
                        ft.DataCell(ft.Text(it["CFOP"] or "")),
                        ft.DataCell(ft.Text(f"{it['qCom']:.4f}".replace(".", ","))),
                        ft.DataCell(ft.Text(brl(it["vUnCom"]))),
                        ft.DataCell(ft.Text(brl(it["vProd"]))),
                        ft.DataCell(ft.Text(brl(it["vDesc"]))),
                        ft.DataCell(ft.Text(brl(it["vBC"]))),
                        ft.DataCell(ft.Text(brl(it["vICMS"]))),
                        ft.DataCell(ft.Text(br_dec(it["ICMS_pICMS"]))),
                        ft.DataCell(ft.Text(it["ICMS_CST"])),
                        ft.DataCell(ft.Text(brl(it["vPIS"]))),
                        ft.DataCell(ft.Text(brl(it["vCOFINS"]))),
                        ft.DataCell(ft.Text(it["PIS_CST"])),
                        ft.DataCell(ft.Text(it["COFINS_CST"])),
                        ft.DataCell(ft.Text(it["indTot"])),
                    ]
                )
            )
        safe_update(itens_table)

    def exportar_itens_view(_=None):
        rota = page.route or "/"
        ch = rota.split("/itens/")[-1] if rota.startswith("/itens/") else None
        if not ch:
            snack("Nenhuma nota selecionada.")
            return
        caminho = EXPORT_DIR / f"itens_{ch}.csv"
        lista = itens_por_chave.get(ch, [])
        if not lista:
            snack("Sem itens para exportar.")
            return
        with open(caminho, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, delimiter=';')
            w.writerow(["chave","nItem","cProd","xProd","NCM","CFOP","qCom","vUnCom","vProd","vDesc","vBC","vICMS","pICMS","ICMS_CST","vPIS","vCOFINS","PIS_CST","COFINS_CST","indTot"])
            for it in lista:
                w.writerow([
                    ch, it["nItem"], it["cProd"] or "", (it["xProd"] or "").strip(),
                    it["NCM"] or "", it["CFOP"] or "",
                    str(it["qCom"]).replace(".", ","),
                    br_dec(it["vUnCom"]), br_dec(it["vProd"]), br_dec(it["vDesc"]),
                    br_dec(it["vBC"]), br_dec(it["vICMS"]), br_dec(it["ICMS_pICMS"]), it["ICMS_CST"],
                    br_dec(it["vPIS"]), br_dec(it["vCOFINS"]), it["PIS_CST"], it["COFINS_CST"], it["indTot"]
                ])
        snack(f"Exportado: {caminho}")

    itens_view = ft.View(
        route="/itens",
        scroll=ft.ScrollMode.AUTO,
        controls=[
            ft.Row(
                controls=[
                    ft.IconButton(I.ARROW_BACK, tooltip="Voltar", on_click=lambda e: page.go("/")),
                    titulo_itens,
                    ft.Container(expand=True),
                    ft.OutlinedButton(text="Exportar Itens (CSV)", icon=I.DOWNLOAD, on_click=exportar_itens_view),
                ],
                alignment=ft.MainAxisAlignment.START,
            ),
            ft.Divider(height=8, color=op(0.2, C.GREY)),
            ft.Container(content=ft.Column([ft.Row([itens_table], scroll=ft.ScrollMode.AUTO)], expand=True, scroll=ft.ScrollMode.AUTO), expand=True),
        ],
        padding=8,
    )

    # ======= Roteamento =======
    def route_change(e: ft.RouteChangeEvent):
        page.views.clear()
        if e.route.startswith("/itens/"):
            chave = e.route.split("/itens/")[-1]
            n = lookup_nota_por_chave.get(chave)
            tipo_txt = "Entrada" if (n and n.get("tpNF") == "0") else "Saída"
            titulo_itens.value = (
                f"Itens da Nota {n['numero']} (Série {n['serie']}) – {tipo_txt} – Chave {chave}" if n else f"Itens – Chave {chave}"
            )
            preencher_itens(chave)
            page.views.append(home_view)
            page.views.append(itens_view)
        else:
            page.views.append(home_view)
        page.update()

    def view_pop(e: ft.ViewPopEvent):
        page.views.pop()
        page.go(page.views[-1].route)

    page.on_route_change = route_change
    page.on_view_pop = view_pop

    page.go(page.route or "/")
    aplicar_filtros()

if __name__ == "__main__":
    ft.app(target=main)