# -*- coding: utf-8 -*-
"""
App: Consulta de Tributação (Flet)
Autor: Conversão a partir do projeto Tkinter do Cristianfer (HOS) + melhorias (ChatGPT)
Descrição:
  - Parâmetros (CNPJ, CRT, UF, Regime, Ambiente, Contribuinte ICMS)
  - Entrada de múltiplos códigos de barras
  - Consulta à API HOS/Imendes com token interno
  - Tabela dinâmica com seleção de colunas visíveis
  - Filtro rápido na tabela
  - Exportar para Excel (openpyxl) e CSV
  - Exibição do JSON de resposta + botão "Copiar JSON"
  - Lista de códigos não encontrados com salvamento em TXT
  - UX: diálogo de carregamento com ProgressRing e desabilitar botões durante a consulta
Requisitos:
  pip install flet requests openpyxl python-dotenv
Execução:
  python consulta_tributacaov4.py
Observação:
  - Credenciais podem ser lidas do .env (HOS_CLIENT_ID, HOS_CLIENT_SECRET). Se não existirem, usa fallback.
"""

from __future__ import annotations
import flet as ft
import json
import os
import csv
import threading
import logging
from datetime import datetime
from typing import List, Dict, Any
from requests.exceptions import RequestException
from time import sleep

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

try:
    from openpyxl import Workbook
except Exception:
    Workbook = None

# ------------------------ Configuração e Constantes ------------------------- #
HOS_BASE_URL = os.getenv("HOS_BASE_URL", "http://autorizadorfarma.hos.com.br/HOSImendes")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "60"))
TOKEN_TIMEOUT = int(os.getenv("TOKEN_TIMEOUT", "30"))
REQUEST_RETRIES = int(os.getenv("REQUEST_RETRIES", "2"))
REQUEST_BACKOFF = float(os.getenv("REQUEST_BACKOFF", "0.5"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

CRT_OPCOES = [
    ("1 - Simples Nacional", "1"),
    ("2 - Simples - Excesso de Sublimite", "2"),
    ("3 - Regime Normal", "3"),
]
REGIME_OPCOES = [
    ("1 - Simples", "1"),
    ("2 - Lucro Presumido", "2"),
    ("3 - Lucro Real", "3"),
]
UFS = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG",
    "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"
]

COLUNAS_TODAS: List[str] = [
    "ID", "Código de Barras", "NCM", "CEST", "Tipo", "Lista", "CodANP", "Código Int.", "EX", "CodENQ",
    "CST IPI Ent", "CST IPI Sai", "Aliq IPI", "NRI", "CST PIS Ent", "CST PIS Sai", "Aliq PIS", "Aliq COFINS",
    "Amp Legal PIS/COFINS", "Dt Vig PIS Início", "Dt Vig PIS Fim", "UF", "CST", "FCP", "IVA", "CSOSN",
    "Código Regra", "% Diferimento", "Exceção", "Simb PDV", "Aliq ICMS", "Amp Legal ICMS", "Cod Benefício",
    "Dt Vig Regra Início", "Dt Vig Regra Fim", "% ICMS PDV", "CFOP Venda", "CFOP Compra", "ICMS Desonerado",
    "Aliq ICMS ST", "Antecipado", "Desonerado", "% Isenção", "Redução BC ICMS", "Redução BC ST", "Finalidade",
    "Ind. Deduz Deson."
]

# --------------------------- Validações ----------------------------------- #
def validar_cnpj(cnpj_raw: str) -> bool:
    """Validação básica de CNPJ (formato + dígitos verificadores)."""
    cnpj = "".join(ch for ch in (cnpj_raw or "") if ch.isdigit())
    if len(cnpj) != 14:
        return False
    # sequências inválidas
    if cnpj == cnpj[0] * 14:
        return False

    def _calc_digitos(cnpj_base: str) -> str:
        pesos1 = [5,4,3,2,9,8,7,6,5,4,3,2]
        pesos2 = [6] + pesos1
        def _calc(base: str, pesos: List[int]) -> int:
            s = sum(int(d)*p for d,p in zip(base, pesos[-len(base):]))
            r = s % 11
            return 0 if r < 2 else 11 - r
        d1 = _calc(cnpj_base, pesos1)
        d2 = _calc(cnpj_base + str(d1), pesos2)
        return f"{d1}{d2}"

    base = cnpj[:12]
    return cnpj.endswith(_calc_digitos(base))

# --------------------------- API / Token (melhorado) ----------------------- #
def obter_token_interno() -> str:
    url = f"{HOS_BASE_URL}/connect/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    client_id = os.getenv("HOS_CLIENT_ID") or "hosfarma"
    client_secret = os.getenv("HOS_CLIENT_SECRET") or "HoS@44#00*"

    data = {
        "grant_type": "client_credentials",
        "scope": "api",
        "client_id": client_id,
        "client_secret": client_secret,
    }

    try:
        r = requests.post(url, headers=headers, data=data, timeout=TOKEN_TIMEOUT)
        r.raise_for_status()
        token = r.json().get("access_token")
        if not token:
            logging.error("Token não retornado pela API de autenticação.")
            raise RuntimeError("Falha ao obter token (resposta inválida).")
        return token
    except RequestException as exc:
        logging.exception("Falha ao obter token (network/timeout).")
        raise RuntimeError("Erro ao obter token: verifique rede/credenciais.") from exc
    except ValueError:
        logging.exception("Resposta inválida ao obter token.")
        raise RuntimeError("Erro ao obter token: resposta inválida.") 

def consultar_api(
    produtos: List[Dict[str, str]],
    cnpj: str,
    uf: str,
    crt: str,
    regime: str,
    icms_contrib: bool,
    ambiente: str,
    token: str
) -> List[Dict[str, Any]]:
    url = (
        f"{HOS_BASE_URL}/v1/tributacao"
        f"?cnpj={cnpj}&uf={uf}&crt={crt}&regimeTrib={regime}"
        f"&contribuinteIcms={'true' if icms_contrib else 'false'}&ambiente={ambiente}"
    )
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    last_exc = None
    for attempt in range(1, REQUEST_RETRIES + 1):
        try:
            logging.info("Consultando API HOS (attempt %d): %s", attempt, url)
            resp = requests.post(url, headers=headers, json=produtos, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            try:
                return resp.json()
            except ValueError as ve:
                logging.exception("JSON inválido na resposta da API.")
                raise RuntimeError("Resposta inválida da API.") from ve
        except RequestException as exc:
            last_exc = exc
            logging.warning("Tentativa %d falhou: %s", attempt, exc)
            if attempt < REQUEST_RETRIES:
                sleep(REQUEST_BACKOFF * attempt)
            else:
                logging.exception("Todas as tentativas falharam ao consultar a API.")
    raise RuntimeError("Erro ao consultar API: verifique rede/parametros/ambiente.") from last_exc

# ---------------------------- App Flet ------------------------------------- #
def main(page: ft.Page):
    page.title = "Consulta de Tributação - Flet"
    page.theme_mode = ft.ThemeMode.LIGHT
    page.theme = ft.Theme(color_scheme_seed=ft.Colors.BLUE, use_material3=True)
    page.padding = 12
    page.window_min_width = 1100
    page.window_min_height = 720

    # --------------------- Widgets de Parâmetros ---------------------------- #
    cnpj = ft.TextField(label="CNPJ", value="00115150000140", width=260)

    crt = ft.Dropdown(
        label="CRT",
        width=260,
        options=[ft.dropdown.Option(text=l, key=v) for l, v in CRT_OPCOES],
        value="1",  # Simples Nacional
    )

    uf = ft.Dropdown(
        label="UF",
        width=160,
        options=[ft.dropdown.Option(u) for u in UFS],
        value="RS",
    )

    regime = ft.Dropdown(
        label="RegimeTrib",
        width=220,
        options=[ft.dropdown.Option(text=l, key=v) for l, v in REGIME_OPCOES],
        value="1",
    )

    ambiente = ft.TextField(label="Ambiente", value="2", width=120)

    icms_cb = ft.Checkbox(label="Contribuinte ICMS", value=True)

    status_text = ft.Text("", size=12)

    # ---------------------- Entrada de Códigos ------------------------------ #
    codigos_field = ft.TextField(
        label="Códigos de Barras (um por linha)",
        multiline=True,
        min_lines=4,
        max_lines=8,
    )

    # ---------------------- Colunas Visíveis (ExpansionTile) ---------------- #
    colunas_visiveis: Dict[str, ft.Checkbox] = {c: ft.Checkbox(label=c, value=True) for c in COLUNAS_TODAS}

    checkboxes_grid = ft.ResponsiveRow(
        [ft.Container(colunas_visiveis[c], col={"xs": 12, "sm": 6, "md": 4, "lg": 3}) for c in COLUNAS_TODAS],
        spacing=12,
        run_spacing=6,
    )

    colunas_expansion = ft.ExpansionTile(
        title=ft.Text("Colunas a serem exibidas", weight=ft.FontWeight.BOLD),
        subtitle=ft.Text("Selecione/deselecione para atualizar a tabela"),
        controls=[],
    )

    # ----------------------- Tabela de Resultados --------------------------- #
    CABECALHO_COR = ft.Colors.BLUE_50
    LINHA_PAR = ft.Colors.GREY_50
    LINHA_IMPAR = None  # usa padrão do tema

    tabela = ft.DataTable(
        columns=[ft.DataColumn(ft.Text(c, weight=ft.FontWeight.W_600)) for c in COLUNAS_TODAS],
        rows=[],
        column_spacing=18,
        heading_row_height=44,
        data_row_min_height=40,
        divider_thickness=0,
        heading_row_color=CABECALHO_COR,
        sort_column_index=0,
        sort_ascending=True,
        show_checkbox_column=False,
    )

    # container com rolagem horizontal (Row com scroll) e vertical (ListView)
    tabela_container = ft.Container(
        content=ft.ListView(
            [ft.Row(controls=[tabela], scroll=ft.ScrollMode.ALWAYS)],  # <- HORIZONTAL SCROLL
            expand=1,
            auto_scroll=False,
        ),
        expand=True,
        bgcolor=ft.Colors.WHITE,
        border_radius=12,
        padding=8,
    )

    # ----------------------- Filtro Rápido ---------------------------------- #
    filtro_field = ft.TextField(
        label="Filtro (busca em qualquer coluna visível)",
        prefix_icon=ft.Icons.SEARCH,
        on_change=lambda e: filtrar_tabela(),
        expand=True,
    )

    # ----------------------- JSON de Resposta ------------------------------- #
    json_field = ft.TextField(
        label="Resposta JSON (formatado)",
        multiline=True,
        min_lines=6,
        max_lines=12,
        expand=True,
        read_only=True,
        bgcolor=ft.Colors.GREY_100,
    )

    copiar_json_btn = ft.OutlinedButton(
        text="Copiar JSON",
        icon=ft.Icons.CONTENT_COPY,
        on_click=lambda e: (page.set_clipboard(json_field.value or ""), mostrar_snackbar("JSON copiado.")),
    )

    # ----------------------- FilePicker (Salvar) ---------------------------- #
    file_picker_save = ft.FilePicker()
    page.overlay.append(file_picker_save)

    # ----------------------- Estado em Memória ------------------------------ #
    linhas_atual: List[Dict[str, Any]] = []
    linhas_filtradas: List[Dict[str, Any]] = []
    colunas_em_uso: List[str] = COLUNAS_TODAS.copy()
    codigos_nao_encontrados: List[str] = []

    # ----------------------- Funções ---------------------------------------- #
    def atualizar_tabela() -> None:
        """Re-renderiza a DataTable com base em linhas_filtradas e colunas_em_uso."""
        nonlocal colunas_em_uso
        colunas_em_uso = [c for c in COLUNAS_TODAS if colunas_visiveis[c].value]
        # Se nenhuma coluna estiver selecionada, deixe a tabela sem colunas/linhas.
        tabela.columns = [ft.DataColumn(ft.Text(c, weight=ft.FontWeight.W_600)) for c in colunas_em_uso]

        tabela.rows = []
        for idx, linha in enumerate(linhas_filtradas):
            vals = [str(linha.get(c, "")) for c in colunas_em_uso]
            row_color = LINHA_PAR if idx % 2 == 0 else LINHA_IMPAR
            tabela.rows.append(
                ft.DataRow(
                    cells=[ft.DataCell(ft.Text(v, no_wrap=True)) for v in vals],
                    color=row_color,
                )
            )
        tabela.update()

    def filtrar_tabela():
        """Aplica filtro de substring em qualquer coluna visível."""
        termo = (filtro_field.value or "").strip().lower()
        if not termo:
            linhas_filtradas[:] = linhas_atual
        else:
            linhas_filtradas[:] = [
                ln for ln in linhas_atual
                if any(termo in str(ln.get(c, "")).lower() for c in colunas_em_uso)
            ]
        atualizar_tabela()

    # tornar checkboxes reativos agora que atualizar_tabela existe
    for cb in colunas_visiveis.values():
        cb.on_change = lambda e: (filtrar_tabela(), page.update())

    def marcar_tudo(_):
        # Marca todas as colunas e força atualização visual dos checkboxes
        for cb in colunas_visiveis.values():
            cb.value = True
            cb.update()
        filtrar_tabela()
        # Atualiza o container do ExpansionTile para refletir as mudanças imediatamente
        try:
            colunas_expansion.update()
        except Exception:
            pass
        page.update()

    def desmarcar_tudo(_):
        # Desmarcar todas as colunas e força atualização visual dos checkboxes
        for cb in colunas_visiveis.values():
            cb.value = False
            cb.update()
        filtrar_tabela()
        # Atualiza o container do ExpansionTile para refletir as mudanças imediatamente
        try:
            colunas_expansion.update()
        except Exception:
            pass
        page.update()

    # preencher controls do ExpansionTile
    colunas_expansion.controls = [
        ft.Row([ft.TextButton("Marcar tudo", on_click=marcar_tudo),
                ft.TextButton("Desmarcar tudo", on_click=desmarcar_tudo)], spacing=8),
        checkboxes_grid,
    ]

    def mostrar_snackbar(msg: str) -> None:
        page.snack_bar = ft.SnackBar(ft.Text(msg), open=True)
        page.update()

    # ----------------------- Exportações ------------------------------------ #
    def exportar_excel(_):
        if Workbook is None:
            mostrar_snackbar("Instale 'openpyxl' para exportar Excel (pip install openpyxl)")
            return
        if not linhas_atual:
            mostrar_snackbar("Não há dados para exportar.")
            return

        def save_result(e: ft.FilePickerResultEvent):
            if not e.path:
                return
            try:
                wb = Workbook()
                ws_res = wb.active
                ws_res.title = "Resultado"
                ws_res.append(colunas_em_uso)
                for ln in linhas_filtradas or linhas_atual:
                    ws_res.append([ln.get(c, "") for c in colunas_em_uso])

                ws_cols = wb.create_sheet("Colunas")
                ws_cols.append(["Coluna", "Selecionada"])
                for c in COLUNAS_TODAS:
                    ws_cols.append([c, "Sim" if colunas_visiveis[c].value else "Não"])

                wb.save(e.path)
                mostrar_snackbar(f"Excel exportado: {e.path}")
            except Exception as ex:
                mostrar_snackbar(f"Falha ao exportar: {ex}")

        file_picker_save.on_result = save_result
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_picker_save.save_file(
            dialog_title="Salvar como Excel",
            file_name=f"resultado_{ts}.xlsx",
            allowed_extensions=["xlsx"],
        )

    def exportar_csv(_):
        if not linhas_atual:
            mostrar_snackbar("Não há dados para exportar.")
            return

        def save_csv(e: ft.FilePickerResultEvent):
            if not e.path:
                return
            try:
                with open(e.path, "w", encoding="utf-8", newline="") as f:
                    writer = csv.writer(f, delimiter=";")
                    writer.writerow(colunas_em_uso)
                    for ln in (linhas_filtradas or linhas_atual):
                        writer.writerow([ln.get(c, "") for c in colunas_em_uso])
                mostrar_snackbar(f"CSV exportado: {e.path}")
            except Exception as ex:
                mostrar_snackbar(f"Falha ao exportar: {ex}")

        file_picker_save.on_result = save_csv
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_picker_save.save_file(
            dialog_title="Salvar como CSV",
            file_name=f"resultado_{ts}.csv",
            allowed_extensions=["csv"],
        )

    def salvar_codigos_nao_encontrados_para_txt():
        if not codigos_nao_encontrados:
            mostrar_snackbar("Não há códigos não encontrados.")
            return

        def save_txt(e: ft.FilePickerResultEvent):
            if not e.path:
                return
            try:
                with open(e.path, "w", encoding="utf-8") as f:
                    f.writelines((c + "\n") for c in codigos_nao_encontrados)
                mostrar_snackbar(f"Arquivo salvo: {e.path}")
            except Exception as ex:
                mostrar_snackbar(f"Falha ao salvar: {ex}")

        file_picker_save.on_result = save_txt
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_picker_save.save_file(
            dialog_title="Salvar códigos não encontrados",
            file_name=f"codigos_nao_encontrados_{ts}.txt",
            allowed_extensions=["txt"],
        )

    # ----------------------- UI: Loading Dialog ----------------------------- #
    loading_dialog = ft.AlertDialog(
        modal=True,
        title=ft.Text("Processando consulta"),
        content=ft.Row([ft.ProgressRing(), ft.Text("Consultando...")], spacing=16, alignment=ft.MainAxisAlignment.START),
    )

    def set_loading(v: bool):
        try:
            if v:
                page.dialog = loading_dialog
                loading_dialog.open = True
            else:
                loading_dialog.open = False
            page.update()
        except Exception:
            pass

    # ----------------------- Ações auxiliares ------------------------------- #
    def limpar_campos(_=None):
        codigos_field.value = ""
        filtro_field.value = ""
        json_field.value = ""
        linhas_atual.clear()
        linhas_filtradas.clear()
        atualizar_tabela()
        page.update()

    # ----------------------- Consulta --------------------------------------- #
    consultar_btn = ft.ElevatedButton(text="Consultar Tributação", icon=ft.Icons.SEARCH)
    exportar_excel_btn = ft.OutlinedButton(text="Exportar Excel", icon=ft.Icons.TABLE_VIEW, on_click=exportar_excel)
    exportar_csv_btn = ft.OutlinedButton(text="Exportar CSV", icon=ft.Icons.DOWNLOAD, on_click=exportar_csv)
    limpar_btn = ft.TextButton(text="Limpar", icon=ft.Icons.CLEAR, on_click=limpar_campos)

    def bloquear_botoes(b: bool):
        consultar_btn.disabled = b
        exportar_excel_btn.disabled = b
        exportar_csv_btn.disabled = b
        limpar_btn.disabled = b
        page.update()

    def consultar_click(_):
        threading.Thread(target=_consultar_thread, daemon=True).start()

    def _consultar_thread():
        status_text.value = "Consultando..."
        bloquear_botoes(True)
        set_loading(True)
        try:
            codigos = [c.strip() for c in (codigos_field.value or "").splitlines() if c.strip()]
            if not codigos:
                status_text.value = ""
                bloquear_botoes(False)
                set_loading(False)
                page.update()
                mostrar_snackbar("Informe ao menos um código de barras.")
                return

            # Monta payload de produtos
            produtos = [{"codigo": c, "descricao": f"Produto {i+1}"} for i, c in enumerate(codigos)]

            token = obter_token_interno()
            dados = consultar_api(
                produtos=produtos,
                cnpj=cnpj.value.strip(),
                uf=uf.value,
                crt=crt.value or "1",
                regime=regime.value or "1",
                icms_contrib=bool(icms_cb.value),
                ambiente=ambiente.value.strip(),
                token=token,
            )

            # Garante JSON legível no painel
            try:
                json_field.value = json.dumps(dados, indent=2, ensure_ascii=False)
            except Exception:
                json_field.value = str(dados)

            nonlocal linhas_atual, linhas_filtradas, codigos_nao_encontrados
            linhas_atual = []
            linhas_filtradas = []
            codigos_nao_encontrados = []

            for item in dados:
                raw = item.get("tributacao")
                if not raw or str(raw).strip() in ("", "null", "None", "[]", "{}"):
                    tributacao = {}
                    # item pode ter "codigo" dentro ou fora
                    codigo_barra = item.get("codigo") or item.get("Codigo") or ""
                    if codigo_barra:
                        codigos_nao_encontrados.append(codigo_barra)
                else:
                    try:
                        # Pode vir já como dict ou string JSON
                        tributacao = raw if isinstance(raw, dict) else json.loads(raw)
                    except Exception:
                        tributacao = {}
                        codigo_barra = item.get("codigo") or item.get("Codigo") or ""
                        if codigo_barra:
                            codigos_nao_encontrados.append(codigo_barra)

                # Algumas APIs retornam "regra" como lista; pega o primeiro dict
                regra = (tributacao.get("regra") or [{}])
                if isinstance(regra, list):
                    regra = (regra[0] if regra else {})
                elif not isinstance(regra, dict):
                    regra = {}

                ipi = tributacao.get("ipi", {})
                pis = tributacao.get("piscofins", {})

                linha = {
                    "ID": item.get("id"),
                    "Código de Barras": item.get("codigo") or item.get("Codigo"),
                    "NCM": tributacao.get("ncm"),
                    "CEST": tributacao.get("cest"),
                    "Tipo": tributacao.get("tipo"),
                    "Lista": tributacao.get("lista"),
                    "CodANP": tributacao.get("codanp"),
                    "Código Int.": tributacao.get("codigo"),
                    "EX": ipi.get("ex"),
                    "CodENQ": ipi.get("codenq"),
                    "CST IPI Ent": ipi.get("cstEnt"),
                    "CST IPI Sai": ipi.get("cstSai"),
                    "Aliq IPI": ipi.get("aliqIPI"),
                    "NRI": pis.get("nri"),
                    "CST PIS Ent": pis.get("cstEnt"),
                    "CST PIS Sai": pis.get("cstSai"),
                    "Aliq PIS": pis.get("aliqPIS"),
                    "Aliq COFINS": pis.get("aliqCOFINS"),
                    "Amp Legal PIS/COFINS": pis.get("ampLegal"),
                    "Dt Vig PIS Início": pis.get("dtVigIni"),
                    "Dt Vig PIS Fim": pis.get("dtVigFin"),
                    "UF": regra.get("uf"),
                    "CST": regra.get("cst"),
                    "FCP": regra.get("fcp"),
                    "IVA": regra.get("iva"),
                    "CSOSN": regra.get("csosn"),
                    "Código Regra": regra.get("codigo"),
                    "% Diferimento": regra.get("pDifer"),
                    "Exceção": regra.get("excecao"),
                    "Simb PDV": regra.get("simbPDV"),
                    "Aliq ICMS": regra.get("aliqicms"),
                    "Amp Legal ICMS": regra.get("ampLegal"),
                    "Cod Benefício": regra.get("codBenef"),
                    "Dt Vig Regra Início": regra.get("dtVigIni"),
                    "Dt Vig Regra Fim": regra.get("dtVigFin"),
                    "% ICMS PDV": regra.get("pICMSPDV"),
                    "CFOP Venda": regra.get("cfopVenda"),
                    "CFOP Compra": regra.get("cfopCompra"),
                    "ICMS Desonerado": regra.get("icmsdeson"),
                    "Aliq ICMS ST": regra.get("aliqicmsst"),
                    "Antecipado": regra.get("antecipado"),
                    "Desonerado": regra.get("desonerado"),
                    "% Isenção": regra.get("percIsencao"),
                    "Redução BC ICMS": regra.get("reducaobcicms"),
                    "Redução BC ST": regra.get("reducaobcicmsst"),
                    "Finalidade": regra.get("estd_finalidade"),
                    "Ind. Deduz Deson.": regra.get("IndicDeduzDesonerado"),
                }
                linhas_atual.append(linha)

            # Aplica filtro inicial (sem termo -> copia tudo)
            linhas_filtradas = list(linhas_atual)
            filtrar_tabela()

            if codigos_nao_encontrados:
                preview = "\n".join(codigos_nao_encontrados[:20])
                if len(codigos_nao_encontrados) > 20:
                    preview += "\n..."
                dlg = ft.AlertDialog(
                    title=ft.Text("Códigos sem tributação"),
                    content=ft.Column(
                        [
                            ft.Text("Os seguintes códigos não retornaram tributação:"),
                            ft.Text(preview),
                            ft.Text("Deseja salvar a lista completa em .txt?"),
                        ],
                        tight=True,
                    ),
                    actions=[
                        ft.TextButton("Não", on_click=lambda e: (setattr(dlg, "open", False), page.update())),
                        ft.TextButton("Salvar", on_click=lambda e: (setattr(dlg, "open", False), page.update(), salvar_codigos_nao_encontrados_para_txt())),
                    ],
                    actions_alignment=ft.MainAxisAlignment.END,
                )
                page.dialog = dlg
                dlg.open = True
                page.update()

            mostrar_snackbar("Consulta concluída.")
        except Exception as ex:
            mostrar_snackbar(str(ex))
        finally:
            status_text.value = ""
            set_loading(False)
            bloquear_botoes(False)
            page.update()

    consultar_btn.on_click = consultar_click

    # ----------------------- Layout ----------------------------------------- #
    botoes_row = ft.Row(
        [
            consultar_btn,
            exportar_excel_btn,
            exportar_csv_btn,
            limpar_btn,
            copiar_json_btn,
        ],
        spacing=12,
        wrap=True,
    )

    parametros = ft.ResponsiveRow(
        [
            ft.Container(cnpj, col={"xs": 12, "sm": 6, "md": 3, "lg": 3}),
            ft.Container(crt, col={"xs": 12, "sm": 6, "md": 3, "lg": 3}),
            ft.Container(uf, col={"xs": 6, "sm": 3, "md": 2, "lg": 2}),
            ft.Container(regime, col={"xs": 12, "sm": 6, "md": 3, "lg": 3}),
            ft.Container(ambiente, col={"xs": 6, "sm": 3, "md": 1, "lg": 1}),
            ft.Container(icms_cb, col={"xs": 12, "sm": 12, "md": 3, "lg": 2}),
        ],
        spacing=10,
        run_spacing=10,
    )

    # --- Ajuste de layout: usar 'expand' (flex) em vez de ft.Expanded ---
    tabela_container.expand = 4  # flex 4
    json_card = ft.Card(content=ft.Container(json_field, padding=12))
    json_card.expand = 2          # flex 2

    page.add(
        ft.Column(
            [
                ft.Text("Consulta de Tributação", size=20, weight=ft.FontWeight.BOLD),
                ft.Card(content=ft.Container(ft.Column([parametros, botoes_row, status_text]), padding=12)),
                ft.Card(content=ft.Container(colunas_expansion, padding=12)),
                ft.Card(content=ft.Container(codigos_field, padding=12)),
                ft.Row(
                    [
                        ft.Text("Resultados", size=16, weight=ft.FontWeight.BOLD),
                        ft.Icon(ft.Icons.TABLE_ROWS),
                        ft.Container(filtro_field, expand=True),
                    ],
                    alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                ),
                tabela_container,
                json_card,
            ],
            expand=True,
            spacing=10,
        )
    )

if __name__ == "__main__":
    ft.app(target=main)
