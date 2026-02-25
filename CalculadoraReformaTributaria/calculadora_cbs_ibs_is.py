# -*- coding: utf-8 -*-
"""
Calculadora IBS/CBS/IS - Simulador de Transição 2026..2033 (com PageView)
- Importa múltiplos XML de NF-e
- Mostra Emissor/Destinatário
- Grade com itens, edição de CST / cClassTrib e botão "Simular"
- Abre uma View (pageview) para simulação com gráficos Matplotlib
- Lê classificações de ./config/classificacao_tributaria.json (padrão em PT-BR)
- (Opcional) Chama API http://localhost:8080/api/calculadora/regime-geral

Requisitos:
    pip install flet requests matplotlib

Coloque o arquivo:
    ./config/classificacao_tributaria.json
"""

import os
import io
import json
import base64
import uuid
import datetime as dt
import xml.etree.ElementTree as ET

import requests
import flet as ft

# Matplotlib em modo "Agg" (sem GUI) para evitar warning de thread
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


# ---------------------------
# Utilidades
# ---------------------------

APP_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = os.path.join(APP_DIR, "config")
DEFAULT_CLASS_JSON = os.path.join(CONFIG_DIR, "classificacao_tributaria.json")
SETTINGS_FILE = os.path.join(APP_DIR, "settings.json")

DEFAULT_API_BASE = "http://localhost:8080/api"

# pesos default (ajuste livre) para a transição 2026..2033 (0..1)
TRANSICAO_PESOS = {
    2026: 0.10,
    2027: 0.20,
    2028: 0.35,
    2029: 0.50,
    2030: 0.70,
    2031: 0.85,
    2032: 0.95,
    2033: 1.00,
}


def load_settings():
    s = {
        "class_json_path": DEFAULT_CLASS_JSON,
        "api_base": DEFAULT_API_BASE
    }
    try:
        if os.path.isfile(SETTINGS_FILE):
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                got = json.load(f)
                s.update(got or {})
    except Exception:
        pass
    return s


def save_settings(s):
    try:
        with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(s, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def ensure_config_dir():
    try:
        os.makedirs(CONFIG_DIR, exist_ok=True)
    except Exception:
        pass


def parse_nfe_namespace(root):
    """Descobre o namespace do XML NF-e de forma resiliente."""
    if root.tag.startswith("{"):
        ns = root.tag.split("}")[0][1:]
        return ns
    return "http://www.portalfiscal.inf.br/nfe"


def find_one(root, xpath, ns_uri):
    """Busca um elemento único com ou sem namespace."""
    try:
        el = root.find(xpath.format(ns=ns_uri))
        if el is None:
            return root.find(xpath.replace("{ns}", ""))
        return el
    except Exception:
        return None


def find_text(root, xpath, ns_uri):
    el = find_one(root, xpath, ns_uri)
    return el.text.strip() if el is not None and el.text else ""


def parse_nfe_xml(file_path):
    """Retorna dict com emissor, destinatario, ide e itens."""
    tree = ET.parse(file_path)
    root = tree.getroot()

    ns_uri = parse_nfe_namespace(root)

    # Suporta dois formatos de raiz: NFe ou procNFe
    nfe = root
    if root.tag.endswith("procNFe"):
        nfe = find_one(root, ".//{{{ns}}}NFe".format(ns=ns_uri), ns_uri) or root

    ide_path = ".//{{{ns}}}ide"
    emit_path = ".//{{{ns}}}emit"
    dest_path = ".//{{{ns}}}dest"
    det_path = ".//{{{ns}}}det"

    ide = find_one(nfe, ide_path, ns_uri)
    emit = find_one(nfe, emit_path, ns_uri)
    dest = find_one(nfe, dest_path, ns_uri)

    # IDE
    nNF = find_text(ide, ".//{{{ns}}}nNF", ns_uri)
    serie = find_text(ide, ".//{{{ns}}}serie", ns_uri)
    dEmi = find_text(ide, ".//{{{ns}}}dEmi", ns_uri) or find_text(ide, ".//{{{ns}}}dhEmi", ns_uri)
    cMunFG = find_text(ide, ".//{{{ns}}}cMunFG", ns_uri)
    uf = find_text(ide, ".//{{{ns}}}cUF", ns_uri)
    ide_info = {"nNF": nNF, "serie": serie, "emissao": dEmi, "cMunFG": cMunFG, "cUF": uf}

    # EMIT
    emit_cnpj = find_text(emit, ".//{{{ns}}}CNPJ", ns_uri) or find_text(emit, ".//{{{ns}}}CPF", ns_uri)
    emit_nome = find_text(emit, ".//{{{ns}}}xNome", ns_uri)
    emit_uf = find_text(emit, ".//{{{ns}}}UF", ns_uri)
    emit_mun = find_text(emit, ".//{{{ns}}}cMun", ns_uri)
    emissor = {"doc": emit_cnpj, "nome": emit_nome, "uf": emit_uf, "cMun": emit_mun}

    # DEST
    dest_cnpj = find_text(dest, ".//{{{ns}}}CNPJ", ns_uri) or find_text(dest, ".//{{{ns}}}CPF", ns_uri)
    dest_nome = find_text(dest, ".//{{{ns}}}xNome", ns_uri)
    dest_uf = find_text(dest, ".//{{{ns}}}UF", ns_uri)
    dest_mun = find_text(dest, ".//{{{ns}}}cMun", ns_uri)
    destinatario = {"doc": dest_cnpj, "nome": dest_nome, "uf": dest_uf, "cMun": dest_mun}

    # Itens
    itens = []
    for det in nfe.findall(det_path.format(ns=ns_uri)):
        prod = find_one(det, ".//{{{ns}}}prod", ns_uri)
        if prod is None:
            continue
        xProd = find_text(prod, ".//{{{ns}}}xProd", ns_uri)
        cProd = find_text(prod, ".//{{{ns}}}cProd", ns_uri)
        ncm = find_text(prod, ".//{{{ns}}}NCM", ns_uri)
        uCom = find_text(prod, ".//{{{ns}}}uCom", ns_uri)
        qCom = find_text(prod, ".//{{{ns}}}qCom", ns_uri)
        vUnCom = find_text(prod, ".//{{{ns}}}vUnCom", ns_uri)
        vProd = find_text(prod, ".//{{{ns}}}vProd", ns_uri)

        try:
            qCom_f = float(qCom.replace(",", ".")) if qCom else 0.0
        except Exception:
            qCom_f = 0.0
        try:
            vUn_f = float(vUnCom.replace(",", ".")) if vUnCom else 0.0
        except Exception:
            vUn_f = 0.0
        try:
            vProd_f = float(vProd.replace(",", ".")) if vProd else (qCom_f * vUn_f)
        except Exception:
            vProd_f = qCom_f * vUn_f

        itens.append({
            "xProd": xProd,
            "cProd": cProd,
            "NCM": ncm,
            "uCom": uCom,
            "qCom": qCom_f,
            "vUnCom": vUn_f,
            "vProd": vProd_f,
        })

    return {
        "arquivo": os.path.basename(file_path),
        "ide": ide_info,
        "emissor": emissor,
        "destinatario": destinatario,
        "itens": itens,
    }


def load_classificacoes(json_path):
    """
    Carrega o JSON com chaves PT-BR:
    - "Código da Situação Tributária"
    - "Código da Classificação Tributária"
    - "Descrição do Código da Classificação Tributária"
    - "Percentual Redução IBS" / "Percentual Redução CBS"
    """
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    cst_to_classes = {}
    classes_info = {}

    for row in data:
        cst = str(row.get("Código da Situação Tributária", "")).strip()
        cclass = str(row.get("Código da Classificação Tributária", "")).strip()
        desc = str(row.get("Descrição do Código da Classificação Tributária", "")).strip()
        red_ibs = float(str(row.get("Percentual Redução IBS", "0")).replace(",", ".") or 0) if "Percentual Redução IBS" in row else 0.0
        red_cbs = float(str(row.get("Percentual Redução CBS", "0")).replace(",", ".") or 0) if "Percentual Redução CBS" in row else 0.0

        if cst and cclass:
            cst_to_classes.setdefault(cst, []).append((cclass, desc))
            classes_info[cclass] = {
                "descricao": desc,
                "red_ibs": red_ibs,
                "red_cbs": red_cbs,
                "cst": cst,
            }

    csts = sorted(list(cst_to_classes.keys()))
    return csts, cst_to_classes, classes_info


def fig_to_base64(fig):
    buf = io.BytesIO()
    fig.tight_layout()
    fig.savefig(buf, format="png", dpi=140, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("ascii")


def simulate_series(item, anos, ref_ibs, ref_cbs, classes_info, cclass, pesos):
    """Retorna lista de valores por ano (novo regime)."""
    red_ibs = classes_info.get(cclass, {}).get("red_ibs", 0.0)
    red_cbs = classes_info.get(cclass, {}).get("red_cbs", 0.0)
    base = max(0.0, float(item.get("vProd", 0.0)))

    anos_sorted = sorted(anos)
    valores = []
    for ano in anos_sorted:
        peso = pesos.get(ano, 0.0)
        eff_ibs = ref_ibs * (1 - red_ibs / 100.0) * peso
        eff_cbs = ref_cbs * (1 - red_cbs / 100.0) * peso
        trib = base * (eff_ibs + eff_cbs)
        valores.append(trib)
    return anos_sorted, valores


def chart_transition(item, anos, ref_ibs, ref_cbs, classes_info, cclass, pesos):
    anos_sorted, valores = simulate_series(item, anos, ref_ibs, ref_cbs, classes_info, cclass, pesos)
    fig, ax = plt.subplots(figsize=(7.2, 4.0), dpi=140)
    ax.bar([str(a) for a in anos_sorted], valores)
    ax.set_title(f"Transição 2026→2033 — {item.get('xProd','(sem desc.)')[:50]}")
    ax.set_xlabel("Ano")
    ax.set_ylabel("Tributo estimado (IBS+CBS)")
    ax.grid(True, axis="y", alpha=0.25)
    return fig_to_base64(fig)


def chart_compare_atual_novo(item, ref_ibs, ref_cbs, classes_info, cclass, aliq_atual_total):
    """Comparativo de barras: Regime Atual × 2033 (novo)."""
    base = max(0.0, float(item.get("vProd", 0.0)))
    # Atual (aprox. entrada do usuário)
    atual = base * max(0.0, aliq_atual_total)

    # 2033: peso = 1.0
    red_ibs = classes_info.get(cclass, {}).get("red_ibs", 0.0)
    red_cbs = classes_info.get(cclass, {}).get("red_cbs", 0.0)
    novo_ibs = ref_ibs * (1 - red_ibs / 100.0) * 1.0
    novo_cbs = ref_cbs * (1 - red_cbs / 100.0) * 1.0
    novo = base * (novo_ibs + novo_cbs)

    fig, ax = plt.subplots(figsize=(6.2, 3.8), dpi=140)
    ax.bar(["Atual", "Novo (2033)"], [atual, novo])
    ax.set_title("Comparativo — Atual × Novo")
    ax.set_ylabel("Valor de tributo")
    ax.grid(True, axis="y", alpha=0.25)
    return fig_to_base64(fig)


def call_api_regime_geral(api_base, uf_sigla, cod_mun, data_emissao_iso, cst, cclass, item):
    """Chama /calculadora/regime-geral montando OperacaoInput mínimo."""
    url = f"{api_base.rstrip('/')}/calculadora/regime-geral"
    payload = {
        "id": uuid.uuid4().hex,
        "versao": "0.0.1",
        "dataHoraEmissao": data_emissao_iso,
        "municipio": int(cod_mun) if str(cod_mun).isdigit() else 0,
        "uf": uf_sigla or "SP",
        "itens": [
            {
                "numero": 1,
                "ncm": item.get("NCM") or "",
                "cst": str(cst),
                "baseCalculo": float(item.get("vProd", 0.0)),
                "quantidade": float(item.get("qCom", 0.0) or 0.0),
                "unidade": item.get("uCom") or "UN",
                "cClassTrib": str(cclass),
            }
        ],
    }
    resp = requests.post(url, json=payload, timeout=15)
    resp.raise_for_status()
    return resp.json()


# ---------------------------
# App principal (Flet)
# ---------------------------

def main(page: ft.Page):
    page.title = "Calculadora IBS/CBS/IS — Simulador de Transição"
    page.window_width = 1200
    page.window_height = 800
    page.theme_mode = ft.ThemeMode.LIGHT
    page.horizontal_alignment = ft.CrossAxisAlignment.STRETCH
    page.vertical_alignment = ft.MainAxisAlignment.START

    ensure_config_dir()
    settings = load_settings()

    # Estado do app
    xml_docs = []      # lista de dicts parseados
    all_items = []     # itens combinados
    csts = []
    cst_to_classes = {}
    classes_info = {}

    # ---- LOG SEGURO (buffer até estar no page) ----
    pending_logs = []
    log_view = ft.ListView(expand=False, height=140, auto_scroll=True)

    def log(msg: str):
        pending_logs.append(msg)
        if getattr(log_view, "page", None) is not None:
            for m in pending_logs:
                log_view.controls.append(ft.Text(m))
            pending_logs.clear()
            log_view.update()

    def snackbar(msg: str):
        page.snack_bar = ft.SnackBar(ft.Text(msg), open=True)
        page.update()

    # Área de Emissor / Destinatário
    emissor_txt = ft.Text("-", selectable=True)
    destinatario_txt = ft.Text("-", selectable=True)

    def refresh_header_info():
        if not xml_docs:
            emissor_txt.value = "-"
            destinatario_txt.value = "-"
        else:
            doc = xml_docs[-1]
            em = doc["emissor"]
            de = doc["destinatario"]
            emissor_txt.value = f"{em.get('nome','')} | {em.get('doc','')} | {em.get('uf','')}-{em.get('cMun','')}"
            destinatario_txt.value = f"{de.get('nome','')} | {de.get('doc','')} | {de.get('uf','')}-{de.get('cMun','')}"
        emissor_txt.update()
        destinatario_txt.update()

    # Tabela
    table = ft.DataTable(
        columns=[
            ft.DataColumn(ft.Text("NF")),
            ft.DataColumn(ft.Text("Produto")),
            ft.DataColumn(ft.Text("NCM")),
            ft.DataColumn(ft.Text("Base (vProd)")),
            ft.DataColumn(ft.Text("CST")),
            ft.DataColumn(ft.Text("cClassTrib")),
            ft.DataColumn(ft.Text("Simular")),
        ],
        rows=[]
    )

    # ===================== PageView de Simulação =====================

    def open_simulation_view(row_idx: int):
        """Abre uma View (nova rota) com a simulação do item."""
        try:
            it = all_items[row_idx]
        except Exception:
            snackbar("Índice inválido para simulação.")
            return

        # Controles locais da Sim View
        ref_ibs = ft.TextField(label="Alíquota de referência IBS (ex.: 0.14)", value="0.14", width=220)
        ref_cbs = ft.TextField(label="Alíquota de referência CBS (ex.: 0.095)", value="0.095", width=220)
        aliq_atual = ft.TextField(label="Alíquota total do regime atual (aprox.)", value="0.27", width=260)
        cb_api = ft.Checkbox(label="Usar API /calculadora/regime-geral para cada ano", value=False)

        anos_checks = [ft.Checkbox(label=str(y), value=True) for y in range(2026, 2034)]
        img_transition = ft.Image(src_base64="", width=740, height=400, fit=ft.ImageFit.CONTAIN)
        img_compare = ft.Image(src_base64="", width=600, height=340, fit=ft.ImageFit.CONTAIN)

        def render_charts(_=None):
            try:
                anos_sel = [int(c.label) for c in anos_checks if c.value]
                if not anos_sel:
                    img_transition.src_base64 = ""
                    img_compare.src_base64 = ""
                    page.update()
                    return

                _ibs = float(ref_ibs.value.replace(",", ".") or 0)
                _cbs = float(ref_cbs.value.replace(",", ".") or 0)
                _atual = float(aliq_atual.value.replace(",", ".") or 0)

                # cClass escolhida (do item)
                cclass = it.get("cClassTrib") or (it.get("dd_cclass").value if it.get("dd_cclass") else None)
                if not cclass:
                    current_cst = it.get("CST") or (it.get("dd_cst").value if it.get("dd_cst") else None)
                    lst = cst_to_classes.get(current_cst, [])
                    if lst:
                        cclass = lst[0][0]

                # Se API marcada, tenta ano a ano; em caso de erro, cai no local.
                if cb_api.value:
                    uf_sigla = it.get("UF") or "SP"
                    cod_mun = it.get("cMunFG") or "3550308"
                    values, labels = [], []
                    for ano in sorted(anos_sel):
                        data_iso = dt.datetime(ano, 1, 2, 10, 0, 0).isoformat()
                        try:
                            resp = call_api_regime_geral(
                                settings.get("api_base", DEFAULT_API_BASE),
                                uf_sigla,
                                cod_mun,
                                data_iso,
                                it.get("CST") or current_cst or "000",
                                cclass or "",
                                it,
                            )
                            total = 0.0
                            try:
                                tot = resp.get("total", {}).get("tribCalc", {}).get("IBSCBSTot", {})
                                total += float(tot.get("gIBS", {}).get("vIBS", 0.0) or 0.0)
                                total += float(tot.get("gCBS", {}).get("vCBS", 0.0) or 0.0)
                            except Exception:
                                pass
                            values.append(total)
                            labels.append(str(ano))
                        except Exception as ex:
                            log(f"[AVISO] API falhou no ano {ano}: {ex}")
                            # fallback local
                            img_transition.src_base64 = chart_transition(it, anos_sel, _ibs, _cbs, classes_info, cclass, TRANSICAO_PESOS)
                            img_compare.src_base64 = chart_compare_atual_novo(it, _ibs, _cbs, classes_info, cclass, _atual)
                            page.update()
                            return

                    fig, ax = plt.subplots(figsize=(7.2, 4.0), dpi=140)
                    ax.bar(labels, values)
                    ax.set_title(f"API: Tributo por Ano — {it.get('xProd','')[:50]}")
                    ax.set_xlabel("Ano")
                    ax.set_ylabel("Valor (IBS+CBS)")
                    ax.grid(True, axis="y", alpha=0.25)
                    img_transition.src_base64 = fig_to_base64(fig)
                else:
                    img_transition.src_base64 = chart_transition(it, anos_sel, _ibs, _cbs, classes_info, cclass, TRANSICAO_PESOS)

                img_compare.src_base64 = chart_compare_atual_novo(it, _ibs, _cbs, classes_info, cclass, _atual)
                page.update()
            except Exception as ex:
                log(f"[ERRO] Simulação: {ex}")
                snackbar(f"Erro ao simular: {ex}")

        # Botão calcular
        btn_calc = ft.FilledButton("Calcular & Gerar Gráficos", icon=ft.Icons.BAR_CHART, on_click=render_charts)

        # Cabeçalho com info do item
        hdr = ft.Column([
            ft.Text(f"Simulando item: {it.get('xProd','(sem desc.)')}", size=18, weight=ft.FontWeight.BOLD),
            ft.Text(f"NCM: {it.get('NCM') or '-'}  |  vProd: {it.get('vProd',0.0):,.2f}  |  CST: {it.get('CST') or it.get('dd_cst').value if it.get('dd_cst') else '-'}  |  cClassTrib: {it.get('cClassTrib') or it.get('dd_cclass').value if it.get('dd_cclass') else '-'}"),
        ], spacing=4)

        # Grades de anos (duas linhas)
        anos_rows = ft.Column([
            ft.Row(anos_checks[0:4], wrap=False, spacing=8),
            ft.Row(anos_checks[4:8], wrap=False, spacing=8),
        ])

        # View (pageview) propriamente dita
        view = ft.View(
            route=f"/simular/{row_idx}",
            controls=[
                ft.AppBar(title=ft.Text("Simulação — Transição da Reforma Tributária"), leading=ft.IconButton(ft.Icons.ARROW_BACK, on_click=lambda _: page.go("/"))),
                ft.Container(
                    content=ft.Column([
                        hdr,
                        ft.Divider(),
                        ft.Row([ref_ibs, ref_cbs, aliq_atual], alignment=ft.MainAxisAlignment.START, spacing=12),
                        ft.Row([cb_api], alignment=ft.MainAxisAlignment.START),
                        ft.Text("Anos de simulação:", size=14),
                        anos_rows,
                        btn_calc,
                        ft.Divider(),
                        ft.Text("Transição 2026→2033 (Novo Regime)", size=16, weight=ft.FontWeight.BOLD),
                        img_transition,
                        ft.Divider(),
                        ft.Text("Comparativo Regime Atual × Novo (2033)", size=16, weight=ft.FontWeight.BOLD),
                        img_compare,
                        ft.Divider(),
                        ft.Text("Dica: ajuste as alíquotas para refletir seu cenário real (UF/Município/segmento)."),
                    ], tight=False, spacing=10),
                    padding=16,
                    expand=True,
                ),
            ],
            vertical_alignment=ft.MainAxisAlignment.START,
            horizontal_alignment=ft.CrossAxisAlignment.STRETCH,
        )

        # Navega para a view
        page.views.append(view)
        page.go(view.route)

    # ===================== Fim da View de Simulação =====================

    def rebuild_table():
        table.rows.clear()
        for idx, it in enumerate(all_items):
            dd_cst = ft.Dropdown(
                value=it.get("CST") or (csts[0] if csts else None),
                options=[ft.dropdown.Option(x) for x in csts],
                width=100,
                on_change=lambda e, row_idx=idx: on_cst_changed(row_idx, e.control.value),
            )

            current_cst = dd_cst.value
            cclass_list = cst_to_classes.get(current_cst, [])
            dd_cclass = ft.Dropdown(
                value=it.get("cClassTrib") or (cclass_list[0][0] if cclass_list else None),
                options=[ft.dropdown.Option(cc) for cc, _ in cclass_list],
                width=140,
                on_change=lambda e, row_idx=idx: on_cclass_changed(row_idx, e.control.value),
            )

            it["dd_cst"] = dd_cst
            it["dd_cclass"] = dd_cclass

            btn_sim = ft.ElevatedButton(
                "Simular",
                icon=ft.Icons.SSID_CHART,
                on_click=lambda e, row_idx=idx: open_simulation_view(row_idx),
            )

            nf_label = f"{it['NF']['serie']}/{it['NF']['nNF']}" if it.get("NF") else "-"
            row = ft.DataRow(
                cells=[
                    ft.DataCell(ft.Text(nf_label)),
                    ft.DataCell(ft.Text(it.get("xProd", "")[:32] or "-")),
                    ft.DataCell(ft.Text(it.get("NCM") or "-")),
                    ft.DataCell(ft.Text(f"{it.get('vProd', 0.0):,.2f}")),
                    ft.DataCell(dd_cst),
                    ft.DataCell(dd_cclass),
                    ft.DataCell(btn_sim),
                ]
            )
            table.rows.append(row)

        table.update()

    def on_cst_changed(row_idx, new_cst):
        it = all_items[row_idx]
        it["CST"] = new_cst
        dd_cclass = it.get("dd_cclass")
        if dd_cclass:
            lst = cst_to_classes.get(new_cst, [])
            dd_cclass.options = [ft.dropdown.Option(cc) for cc, _ in lst]
            if lst:
                dd_cclass.value = lst[0][0]
                it["cClassTrib"] = lst[0][0]
            dd_cclass.update()

    def on_cclass_changed(row_idx, new_cclass):
        it = all_items[row_idx]
        it["cClassTrib"] = new_cclass

    # FilePicker (XML)
    fp_xml = ft.FilePicker(on_result=lambda e: on_files_selected(e))
    page.overlay.append(fp_xml)

    def on_files_selected(e: ft.FilePickerResultEvent):
        if not e.files:
            return
        added = 0
        for f in e.files:
            try:
                doc = parse_nfe_xml(f.path)
                xml_docs.append(doc)
                for prod in doc["itens"]:
                    all_items.append({
                        **prod,
                        "NF": {"nNF": doc["ide"]["nNF"], "serie": doc["ide"]["serie"]},
                        "UF": doc["emissor"].get("uf") or doc["destinatario"].get("uf") or "",
                        "cMunFG": doc["ide"].get("cMunFG") or doc["destinatario"].get("cMun") or doc["emissor"].get("cMun") or "",
                        "CST": None,
                        "cClassTrib": None,
                        "emissor": doc["emissor"],
                        "destinatario": doc["destinatario"],
                    })
                added += 1
            except Exception as ex:
                log(f"[ERRO] Falha ao ler {f.name}: {ex}")

        log(f"[OK] Importados {added} arquivo(s). Itens totais: {len(all_items)}")
        refresh_header_info()
        rebuild_table()

    # Botões topo
    btn_import_xml = ft.ElevatedButton(
        "Carregar NF-e XML(s)",
        icon=ft.Icons.UPLOAD_FILE,
        on_click=lambda e: fp_xml.pick_files(allow_multiple=True, allowed_extensions=["xml"])
    )

    def open_config_dialog(e=None):
        txt_json = ft.TextField(
            label="Arquivo classificacao_tributaria.json",
            value=settings.get("class_json_path", ""),
            width=500
        )
        txt_api = ft.TextField(
            label="Base da API",
            value=settings.get("api_base", DEFAULT_API_BASE),
            width=500
        )

        fp_json = ft.FilePicker(on_result=lambda ev: setattr(txt_json, "value", ev.files[0].path) or txt_json.update())
        page.overlay.append(fp_json)

        def on_save_conf(ev):
            p = txt_json.value.strip()
            api_b = txt_api.value.strip() or DEFAULT_API_BASE
            settings["api_base"] = api_b
            save_settings(settings)
            if p and os.path.isfile(p):
                load_classes_from(p)
            dlg.open = False
            page.update()

        dlg = ft.AlertDialog(
            modal=True,
            title=ft.Text("Configurações"),
            content=ft.Column([
                txt_json,
                ft.TextButton("Procurar arquivo...", on_click=lambda _ : fp_json.pick_files(allow_multiple=False, allowed_extensions=["json"])),
                ft.Divider(),
                txt_api,
            ], tight=True),
            actions=[
                ft.TextButton("Salvar", on_click=on_save_conf),
                ft.TextButton("Fechar", on_click=lambda _: setattr(dlg, "open", False) or page.update()),
            ]
        )
        page.dialog = dlg
        dlg.open = True
        page.update()

    btn_config = ft.OutlinedButton("Configurações", icon=ft.Icons.SETTINGS, on_click=open_config_dialog)

    btn_limpar = ft.TextButton(
        "Limpar tudo",
        icon=ft.Icons.DELETE_SWEEP,
        on_click=lambda e: (xml_docs.clear(), all_items.clear(), rebuild_table(), refresh_header_info(), log("[OK] Limpo."))
    )

    # Layout principal (View "/")
    header = ft.Container(
        content=ft.Column([
            ft.Row([
                btn_import_xml,
                btn_config,
                btn_limpar,
            ], alignment=ft.MainAxisAlignment.START),
            ft.Divider(),
            ft.Text("Emissor"),
            emissor_txt,
            ft.Text("Destinatário"),
            destinatario_txt,
        ], tight=False),
        padding=10
    )
    table_container = ft.Container(content=table, expand=True, padding=10)
    logs_container = ft.Container(content=ft.Column([ft.Text("Log"), log_view], tight=True), padding=10)

    # Instala a View raiz e rota
    def route_change(e: ft.RouteChangeEvent):
        page.views.clear()
        main_view = ft.View(
            route="/",
            controls=[
                ft.AppBar(title=ft.Text("Calculadora IBS/CBS/IS — Simulador")),
                header,
                table_container,
                logs_container
            ],
            vertical_alignment=ft.MainAxisAlignment.START,
            horizontal_alignment=ft.CrossAxisAlignment.STRETCH,
        )
        page.views.append(main_view)

        # Se rota for /simular/N, mantém o push feito por open_simulation_view
        if e.route.startswith("/simular/"):
            # A view de simulação é criada em open_simulation_view()
            pass
        page.update()

    def view_pop(e: ft.ViewPopEvent):
        page.views.pop()
        if len(page.views) == 0:
            page.go("/")
        else:
            page.go(page.views[-1].route)

    page.on_route_change = route_change
    page.on_view_pop = view_pop

    # Adiciona o FilePicker e os containers ao page ANTES de logar algo
    page.go("/")  # desenha a View principal

    # -------------------
    # Carrega classificações (usa log() com buffer flush)
    # -------------------
    def load_classes_from(path):
        nonlocal csts, cst_to_classes, classes_info
        try:
            csts, cst_to_classes, classes_info = load_classificacoes(path)
            log(f"[OK] Classificações carregadas: {len(classes_info)} (CST únicos: {len(csts)})")
            settings["class_json_path"] = path
            save_settings(settings)
        except Exception as e:
            log(f"[ERRO] Falha ao carregar classificações: {e}")

    if os.path.isfile(settings.get("class_json_path", "")):
        load_classes_from(settings["class_json_path"])
    else:
        if os.path.isfile(DEFAULT_CLASS_JSON):
            load_classes_from(DEFAULT_CLASS_JSON)
        else:
            log("[AVISO] JSON de classificações não encontrado. Ajuste em Configurações.")

    log("[PRONTO] Carregue seus XMLs de NF-e e edite CST/cClassTrib antes de simular.")


if __name__ == "__main__":
    ft.app(target=main)