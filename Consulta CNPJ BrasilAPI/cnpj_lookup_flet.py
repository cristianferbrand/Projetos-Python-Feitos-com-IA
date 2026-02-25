#!/usr/bin/env python3
"""
CNPJ Lookup UI – Flet (web-like desktop app)

• UI moderna com Flet (parece um app web, mas roda desktop ou web).
• Consulta CNPJ na Minha Receita ou BrasilAPI.
• Validação de CNPJ (com dígitos verificadores).
• Exibição em cartões: dados cadastrais, endereço/contato, CNAE, QSA e regime tributário.
• Consulta em lote via CSV (coluna "cnpj").
• Exporta para CSV e JSONL.

Instalação:
  pip install flet requests

Execução (modo app de desktop):
  python cnpj_lookup_flet.py

Execução como web (abre no navegador):
  python cnpj_lookup_flet.py --web

Empacotar em .EXE (exemplo PyInstaller):
  pyinstaller --noconfirm --onefile --windowed --name CNPJLookup \
    --add-data "{site_packages}/flet;flet" cnpj_lookup_flet.py
(Substitua {site_packages} pelo caminho do seu site-packages.)
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import flet as ft
import requests

# ==========================
# Utilidades de CNPJ
# ==========================
CNPJ_RE = re.compile(r"\d{14}")


def only_digits(s: str) -> str:
    return re.sub(r"\D", "", s or "")


def calc_dv_cnpj(cnpj12: str) -> Tuple[int, int]:
    pesos1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    pesos2 = [6] + pesos1
    nums = [int(x) for x in cnpj12]
    s1 = sum(a * b for a, b in zip(nums, pesos1))
    dv1 = 11 - (s1 % 11)
    dv1 = 0 if dv1 >= 10 else dv1
    nums2 = nums + [dv1]
    s2 = sum(a * b for a, b in zip(nums2, pesos2))
    dv2 = 11 - (s2 % 11)
    dv2 = 0 if dv2 >= 10 else dv2
    return dv1, dv2


def is_valid_cnpj(cnpj: str) -> bool:
    d = only_digits(cnpj)
    if len(d) != 14:
        return False
    if len(set(d)) == 1:
        return False
    dv1, dv2 = calc_dv_cnpj(d[:12])
    return d[-2:] == f"{dv1}{dv2}"


# ==========================
# Provedores
# ==========================
@dataclass
class Provider:
    name: str
    base_url: str
    headers: Dict[str, str]

    def url(self, cnpj: str) -> str:
        return self.base_url.format(cnpj=cnpj)


MINHA_RECEITA = Provider(
    name="Minha Receita",
    base_url="https://minhareceita.org/{cnpj}",
    headers={"Accept": "application/json"},
)

BRASILAPI = Provider(
    name="BrasilAPI",
    base_url="https://brasilapi.com.br/api/cnpj/v1/{cnpj}",
    headers={"Accept": "application/json"},
)

PROVIDERS = {
    "Minha Receita": MINHA_RECEITA,
    "BrasilAPI": BRASILAPI,
}


# ==========================
# HTTP
# ==========================
class CNPJClient:
    def __init__(self, provider: Provider, timeout: int = 20, retries: int = 2, backoff: float = 0.8):
        self.provider = provider
        self.timeout = timeout
        self.retries = retries
        self.backoff = backoff

    def fetch(self, raw_cnpj: str) -> Tuple[int, Dict[str, Any]]:
        cnpj = only_digits(raw_cnpj)
        if not CNPJ_RE.fullmatch(cnpj) or not is_valid_cnpj(cnpj):
            return 400, {"error": "CNPJ inválido ou mal formatado (DV).", "cnpj": cnpj}
        last_err: Optional[str] = None
        url = self.provider.url(cnpj)
        for attempt in range(self.retries + 1):
            try:
                r = requests.get(url, headers=self.provider.headers, timeout=self.timeout)
                if r.status_code == 200:
                    return 200, r.json()
                if r.status_code in (400, 404):
                    try:
                        err = r.json()
                        msg = err.get("message") or err.get("detail") or err.get("error") or r.text
                    except Exception:
                        msg = r.text
                    return r.status_code, {"error": msg, "cnpj": cnpj}
                if r.status_code in (429, 502, 503, 504):
                    last_err = f"HTTP {r.status_code} – limite/capacidade"  # retry
                else:
                    last_err = f"HTTP {r.status_code}: {r.text[:200]}"
            except requests.RequestException as e:
                last_err = str(e)
            if attempt < self.retries:
                time.sleep(self.backoff * (attempt + 1))
        return 500, {"error": last_err or "Erro desconhecido", "cnpj": cnpj}


# ==========================
# Normalização para UI/CSV
# ==========================

def flatten(provider: str, cnpj: str, d: Dict[str, Any]) -> Dict[str, Any]:
    def get(k: str, default: Any = ""):
        return d.get(k, default)

    out: Dict[str, Any] = {
        "provider": provider,
        "cnpj": cnpj,
        "razao_social": get("razao_social") or get("razao"),
        "nome_fantasia": get("nome_fantasia") or get("fantasia"),
        "situacao": get("descricao_situacao_cadastral") or get("situacao"),
        "data_situacao": get("data_situacao_cadastral") or get("data_situacao"),
        "porte": get("porte") or get("descricao_porte"),
        "natureza_juridica": get("natureza_juridica") or get("codigo_natureza_juridica"),
        "capital_social": get("capital_social") or get("capital_social_str"),
        "email": get("email"),
        "telefone1": get("ddd_telefone_1"),
        "telefone2": get("ddd_telefone_2"),
        "fax": get("ddd_fax"),
        "tipo_logradouro": get("descricao_tipo_de_logradouro"),
        "logradouro": get("logradouro") or get("descricao_logradouro"),
        "numero": get("numero"),
        "complemento": get("complemento"),
        "bairro": get("bairro"),
        "municipio": get("municipio"),
        "uf": get("uf"),
        "cep": get("cep"),
        "cnae_fiscal": get("cnae_fiscal") or get("cnae"),
        "cnae_fiscal_descricao": get("cnae_fiscal_descricao"),
        "cnaes_secundarios": "",
        "socios": "",
        "data_abertura": get("data_abertura") or get("abertura") or get("data_inicio_atividade"),
        "identificador_matriz_filial": get("descricao_identificador_matriz_filial") or get("identificador_matriz_filial"),
        "regime_tributario": "",
    }

    # cnaes secundários
    secund = d.get("cnaes_secundarios") or d.get("cnaes_secundarias") or d.get("cnaesSecundarios")
    if isinstance(secund, list):
        parts = []
        for item in secund:
            if isinstance(item, dict):
                code = item.get("codigo") or item.get("code") or item.get("cnae") or item.get("cnae_fiscal")
                desc = item.get("descricao") or item.get("text")
                parts.append(f"{code}-{desc}" if code and desc else str(code or desc))
            else:
                parts.append(str(item))
        out["cnaes_secundarios"] = "; ".join(parts)

    # QSA
    qsa = d.get("qsa") or d.get("socios") or d.get("quadro_societario")
    if isinstance(qsa, list):
        socios_fmt = []
        for s in qsa:
            if isinstance(s, dict):
                nome = s.get("nome_socio") or s.get("nome") or s.get("nome_rep_legal")
                qual = s.get("qualificacao_socio") or s.get("qual") or s.get("qualificacao")
                faixa = s.get("faixa_etaria")
                parte = ", ".join([x for x in [nome, qual, faixa] if x])
                if parte:
                    socios_fmt.append(parte)
            else:
                socios_fmt.append(str(s))
        out["socios"] = "; ".join(socios_fmt)

    # Regime tributário (lista de dicts)
    rt = d.get("regime_tributario")
    if isinstance(rt, list):
        out["regime_tributario"] = "; ".join(
            [f"{r.get('ano')}: {r.get('forma_de_tributacao')} ({r.get('quantidade_de_escrituracoes')} escritur.)" for r in rt if isinstance(r, dict)]
        )

    return out


# ==========================
# Export helpers
# ==========================

def export_csv(path: Path, rows: List[Dict[str, Any]]):
    if not rows:
        return
    preferred = [
        "provider","cnpj","razao_social","nome_fantasia","situacao","data_situacao",
        "porte","natureza_juridica","capital_social","data_abertura",
        "cnae_fiscal","cnae_fiscal_descricao","cnaes_secundarios",
        "tipo_logradouro","logradouro","numero","complemento","bairro","municipio","uf","cep",
        "email","telefone1","telefone2","fax",
        "regime_tributario","identificador_matriz_filial",
        "socios",
    ]
    keys = list({k for r in rows for k in r.keys()})
    fieldnames = preferred + [k for k in keys if k not in preferred]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def append_jsonl(path: Path, obj: Dict[str, Any]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


# ==========================
# UI (Flet)
# ==========================

def main(page: ft.Page):
    page.title = "CNPJ Lookup"
    page.theme_mode = ft.ThemeMode.SYSTEM
    page.horizontal_alignment = ft.CrossAxisAlignment.CENTER
    page.vertical_alignment = ft.MainAxisAlignment.START
    page.padding = 24
    # Habilita rolagem
    page.scroll = ft.ScrollMode.AUTO

    # Estado
    provider_dd = ft.Dropdown(
        label="Provedor",
        options=[ft.dropdown.Option(k) for k in PROVIDERS.keys()],
        value="BrasilAPI",
        width=300,
    )
    cnpj_tf = ft.TextField(label="CNPJ", hint_text="00.000.000/0001-00 ou 00000000000100", width=320)
    status_text = ft.Text(size=12, opacity=0.0)

    # Result container com rolagem
    result_cards = ft.ListView(expand=1, spacing=12, padding=0, auto_scroll=False)

    # File pickers
    fp_save_csv = ft.FilePicker()
    fp_save_jsonl = ft.FilePicker()
    fp_open_csv = ft.FilePicker()
    page.overlay.extend([fp_save_csv, fp_save_jsonl, fp_open_csv])

    # Dados da última consulta (para export)
    last_rows: List[Dict[str, Any]] = []

    def set_status(msg: str, ok: bool = True):
        status_text.value = msg
        status_text.color = ft.Colors.GREEN_400 if ok else ft.Colors.RED_400
        status_text.opacity = 1.0
        page.update()

    def clear_results():
        result_cards.controls.clear()
        page.update()

    def add_card(title: str, items: List[Tuple[str, Optional[str]]]):
        rows: List[ft.Control] = []
        for k, v in items:
            if v is None:
                continue
            rows.append(
                ft.Row([
                    ft.Text(k + ":", weight=ft.FontWeight.W_600),
                    ft.Text(v or "-"),
                ])
            )
        card = ft.Card(
            content=ft.Container(
                content=ft.Column([
                    ft.Text(title, style=ft.TextThemeStyle.TITLE_MEDIUM, weight=ft.FontWeight.BOLD),
                    ft.Divider(),
                    *rows
                ], tight=True, spacing=6),
                padding=16,
            ),
            elevation=3,
        )
        result_cards.controls.append(card)

    def add_section(title: str, lines: List[str]):
        if not lines:
            return
        card = ft.Card(
            content=ft.Container(
                content=ft.Column([
                    ft.Text(title, style=ft.TextThemeStyle.TITLE_MEDIUM, weight=ft.FontWeight.BOLD),
                    ft.Divider(),
                    *[ft.Text(l) for l in lines],
                ], spacing=4),
                padding=16,
            ),
            elevation=2,
        )
        result_cards.controls.append(card)

    def do_lookup(e=None, batch: Optional[List[str]] = None):
        nonlocal last_rows
        clear_results()
        last_rows = []
        prov = PROVIDERS.get(provider_dd.value, BRASILAPI)
        client = CNPJClient(prov)

        cnpjs: List[str] = batch if batch is not None else [cnpj_tf.value.strip()]
        if not cnpjs or not cnpjs[0]:
            set_status("Informe um CNPJ.", False)
            return

        for idx, raw in enumerate(cnpjs, 1):
            status, data = client.fetch(raw)
            if status != 200:
                add_section("Erro", [f"{raw}: {data.get('error')}"])
                set_status(f"Erro ao consultar {raw}", False)
                continue
            cnpj = only_digits(raw)
            flat = flatten(prov.name, cnpj, data)
            last_rows.append(flat)

            # Cartões
            add_card("Cadastro Básico", [
                ("Razão social", flat.get("razao_social")),
                ("Nome fantasia", flat.get("nome_fantasia")),
                ("CNPJ", cnpj),
                ("Situação", flat.get("situacao")),
                ("Data situação", flat.get("data_situacao")),
                ("Porte", flat.get("porte")),
                ("Natureza jurídica", flat.get("natureza_juridica")),
                ("Capital social", str(flat.get("capital_social"))),
                ("Data abertura", flat.get("data_abertura")),
                ("Matriz/Filial", str(flat.get("identificador_matriz_filial"))),
            ])

            # CNAEs secundários em linhas separadas
            cnaes_lines = [c.strip() for c in (flat.get("cnaes_secundarios") or "").split(";") if c.strip()]

            add_card("Endereço / Contato", [
                ("Tipo de logradouro", flat.get("tipo_logradouro")),
                ("Logradouro", flat.get("logradouro")),
                ("Número", flat.get("numero")),
                ("Complemento", flat.get("complemento")),
                ("Bairro", flat.get("bairro")),
                ("Município", flat.get("municipio")),
                ("UF", flat.get("uf")),
                ("CEP", flat.get("cep")),
                ("E-mail", flat.get("email")),
                ("Telefone 1", flat.get("telefone1")),
                ("Telefone 2", flat.get("telefone2")),
                ("Fax", flat.get("fax")),
            ])

            add_card("Atividade Econômica", [
                ("CNAE fiscal", str(flat.get("cnae_fiscal"))),
                ("Descrição do CNAE", flat.get("cnae_fiscal_descricao")),
                ("CNAEs secundários", "\n".join(cnaes_lines)),
            ])

            # QSA e Regime
            socios_lines = [f"• {s.strip()}" for s in (flat.get("socios") or "").split(";") if s.strip()]
            if socios_lines:
                add_section("QSA (Sócios/Qualificações)", socios_lines)

            if flat.get("regime_tributario"):
                rts = [f"• {rt.strip()}" for rt in flat["regime_tributario"].split(";") if rt.strip()]
                add_section("Regime Tributário (histórico)", rts)

            page.update()

        ok_count = len(last_rows)
        if ok_count:
            set_status(f"Consulta concluída: {ok_count} registro(s) válido(s).", True)

    # Ações de arquivo (lote e exportações)
    def open_batch(_):
        def picked(e: ft.FilePickerResultEvent):
            if not e.files:
                return
            file = e.files[0]
            path = Path(file.path)
            try:
                with path.open("r", encoding="utf-8") as f:
                    rd = csv.DictReader(f)
                    cnpjs = [row.get("cnpj", "").strip() for row in rd if row.get("cnpj")]
                do_lookup(batch=cnpjs)
            except Exception as ex:
                set_status(f"Falha ao ler CSV: {ex}", False)
        fp_open_csv.on_result = picked
        fp_open_csv.pick_files(allow_multiple=False, file_type=ft.FilePickerFileType.CUSTOM, allowed_extensions=["csv"]) 

    def export_to_csv(_):
        if not last_rows:
            set_status("Nada para exportar.", False)
            return
        def on_save(e: ft.FilePickerResultEvent):
            if not e.path:
                return
            try:
                export_csv(Path(e.path), last_rows)
                set_status("CSV salvo com sucesso.")
            except Exception as ex:
                set_status(f"Erro salvando CSV: {ex}", False)
        fp_save_csv.on_result = on_save
        fp_save_csv.save_file(file_name="cnpj_resultados.csv")

    def export_to_jsonl(_):
        if not last_rows:
            set_status("Nada para exportar.", False)
            return
        def on_save(e: ft.FilePickerResultEvent):
            if not e.path:
                return
            try:
                p = Path(e.path)
                for row in last_rows:
                    append_jsonl(p, row)
                set_status("JSONL salvo com sucesso.")
            except Exception as ex:
                set_status(f"Erro salvando JSONL: {ex}", False)
        fp_save_jsonl.on_result = on_save
        fp_save_jsonl.save_file(file_name="cnpj_resultados.jsonl")

    # Botões principais
    btn_lookup = ft.ElevatedButton("Consultar", icon=ft.Icons.SEARCH, on_click=do_lookup)
    btn_batch = ft.OutlinedButton("Consultar em lote (CSV)", icon=ft.Icons.UPLOAD_FILE, on_click=open_batch)
    btn_export_csv = ft.ElevatedButton("Exportar CSV", icon=ft.Icons.DOWNLOAD, on_click=export_to_csv)
    btn_export_jsonl = ft.OutlinedButton("Exportar JSONL", icon=ft.Icons.CODE, on_click=export_to_jsonl)

    # Header / Toolbar
    toolbar = ft.Row(
        controls=[
            ft.Icon(ft.Icons.BUSINESS),
            ft.Text("CNPJ Lookup", style=ft.TextThemeStyle.TITLE_LARGE, weight=ft.FontWeight.BOLD),
            ft.Container(expand=True),
            provider_dd,
        ],
        alignment=ft.MainAxisAlignment.START,
        vertical_alignment=ft.CrossAxisAlignment.CENTER,
    )

    query_row = ft.Row(
        controls=[cnpj_tf, btn_lookup, btn_batch, btn_export_csv, btn_export_jsonl],
        wrap=True,
        spacing=12,
        alignment=ft.MainAxisAlignment.START,
    )

    # Layout com rolagem
    root = ft.Column([toolbar, query_row, status_text, ft.Divider(), result_cards], expand=True, spacing=8)
    page.add(root)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--web", action="store_true", help="Executa como web (abre no navegador)")
    args = parser.parse_args()

    if args.web:
        ft.app(target=main, view=ft.WEB_BROWSER)
    else:
        ft.app(target=main)