
import json
import threading
import uuid
from typing import Any, Dict, List

import flet as ft
import requests
from requests.auth import HTTPBasicAuth

# ---- Config ----
AFFILIATION_KEY = "oOb/mDlgFcqVixxk3NXhg7TuNWk="
TIMEOUT = 30  # seconds

def endpoints(env: str):
    if env == "Sandbox":
        return ("https://auth.sbx.rvhub.com.br/oauth2/token", "https://api.sbx.rvhub.com.br")
    else:
        return ("https://auth.rvhub.com.br/oauth2/token", "https://api.rvhub.com.br")

def pretty(x: Any) -> str:
    try:
        return json.dumps(x, ensure_ascii=False, indent=2)
    except Exception:
        return str(x)

def union_keys(items: List[Dict[str, Any]]) -> List[str]:
    ks = set()
    for it in items:
        if isinstance(it, dict):
            ks.update(it.keys())
    return sorted(ks)

def extract_items(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for key in ["items", "data", "result", "results", "content", "portfolio", "records"]:
            v = payload.get(key)
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]
        return [payload]
    return []

def app_main(page: ft.Page):
    page.title = "RV Hub — Consulta Portfólio"
    page.window_width = 1020
    page.window_height = 860
    page.padding = 16
    page.theme_mode = ft.ThemeMode.LIGHT
    page.scroll = ft.ScrollMode.AUTO

    env_dd = ft.Dropdown(
        label="Ambiente",
        value="Produção",
        options=[ft.dropdown.Option("Produção"), ft.dropdown.Option("Sandbox")],
        width=180,
    )

    # 1) Client Credentials do Parceiro (pré-preenchidos e editáveis)
    partner_client_id = ft.TextField(label="Client ID (Parceiro)", expand=True, value="1l912blabjk4afd0eutqvti3dg")
    partner_client_secret = ft.TextField(label="Client Secret (Parceiro)", password=True, can_reveal_password=True, expand=True, value="e6khviighrgng9fr7n47ma69rbd1k4pnei4cvkeai3cpft83a6j")

    # 2) store/login/password (affiliation_key fixo)
    store = ft.TextField(label="store", hint_text="ex.: hossistemas", expand=True)
    login = ft.TextField(label="login", hint_text="ex.: hos", expand=True)
    password = ft.TextField(label="password", password=True, can_reveal_password=True, hint_text="ex.: 00000000", expand=True)
    affiliation = ft.TextField(label="affiliation_key (fixo)", value=AFFILIATION_KEY, read_only=True, expand=True)

    one_page = ft.Switch(label="one_page=true (Portfólio)", value=True)

    # 3) Resultado do Portfólio (tabela)
    table_container = ft.Container(height=320, content=ft.Column([], scroll=ft.ScrollMode.ALWAYS))

    # 4) Resposta Bruta e Log debug
    raw_output = ft.TextField(label="Resposta Bruta (JSON)", multiline=True, min_lines=10, max_lines=24, expand=True)
    debug_output = ft.TextField(label="Log Debug", multiline=True, min_lines=8, max_lines=20, expand=True)

    status_text = ft.Text("", selectable=True)
    pr = ft.ProgressRing(visible=False)

    def set_busy(b: bool):
        pr.visible = b
        btn_run.disabled = b
        page.update()

    def fmt_debug(method: str, url: str, headers: Dict[str, str], body: Any, resp: requests.Response, req_id: str) -> str:
        lines = [f"# {method} {url}", f"# X-Request-Id: {req_id}"]
        if headers: lines += ["## Request headers:", pretty(headers)]
        if body not in (None, "", {}): lines += ["## Request body:", pretty(body) if isinstance(body, (dict, list)) else str(body)]
        lines.append(f"## Response status: {resp.status_code} {resp.reason}")
        lines.append("## Response headers:")
        try:
            lines.append(pretty(dict(resp.headers)))
        except Exception:
            lines.append(str(resp.headers))
        lines.append("## Response body:")
        try:
            lines.append(pretty(resp.json()))
        except Exception:
            lines.append(resp.text)
        return "\n".join(lines)

    def build_table(items: List[Dict[str, Any]]) -> ft.DataTable:
        cols = union_keys(items) if items else []
        columns = [ft.DataColumn(ft.Text(k)) for k in cols]
        rows = []
        for it in items:
            cells = []
            for k in cols:
                v = it.get(k, "")
                if isinstance(v, (dict, list)):
                    v = json.dumps(v, ensure_ascii=False)
                cells.append(ft.DataCell(ft.Text(str(v))))
            rows.append(ft.DataRow(cells=cells))
        return ft.DataTable(columns=columns, rows=rows, column_spacing=14, heading_row_height=40, data_row_max_height=72)

    def run_flow(e):
        # limpa saídas
        raw_output.value = ""
        debug_output.value = ""
        table_container.content = ft.Column([], scroll=ft.ScrollMode.ALWAYS)
        status_text.value = ""
        status_text.color = ft.Colors.BLACK87
        page.update()

        pci = (partner_client_id.value or "").strip()
        pcs = (partner_client_secret.value or "").strip()
        s = (store.value or "").strip()
        l = (login.value or "").strip()
        p = (password.value or "").strip()

        if not (pci and pcs):
            status_text.value = "Informe Client ID e Client Secret do Parceiro."
            status_text.color = ft.Colors.RED
            page.update()
            return
        if not (s and l and p):
            status_text.value = "Preencha store, login e password."
            status_text.color = ft.Colors.RED
            page.update()
            return

        AUTH_URL, API_BASE = endpoints(env_dd.value or "Produção")
        MIGRATE_URL = f"{API_BASE}/auth/cellcard/migrate"
        PORTFOLIO_URL = f"{API_BASE}/portfolio/"
        req_id_tok_parc = str(uuid.uuid4())
        req_id_mig = str(uuid.uuid4())
        req_id_tok_cli = str(uuid.uuid4())
        req_id_port = str(uuid.uuid4())

        def worker():
            set_busy(True)
            try:
                # 1) Token do PARCEIRO (client_credentials)
                headers_tp = {"X-Request-Id": req_id_tok_parc, "Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"}
                data_tp = {"grant_type": "client_credentials"}
                resp_tp = requests.post(AUTH_URL, data=data_tp, auth=HTTPBasicAuth(pci, pcs), headers=headers_tp, timeout=TIMEOUT)
                try:
                    payload_tp = resp_tp.json()
                except Exception:
                    payload_tp = resp_tp.text
                debug_output.value += fmt_debug("POST", AUTH_URL, headers_tp, data_tp, resp_tp, req_id_tok_parc) + "\n\n"
                page.update()

                if resp_tp.status_code != 200 or not isinstance(payload_tp, dict) or "access_token" not in payload_tp:
                    raw_output.value = pretty(payload_tp)
                    status_text.value = f"Falha ao obter token do parceiro (HTTP {resp_tp.status_code})."
                    status_text.color = ft.Colors.RED
                    page.update()
                    return
                tok_partner = payload_tp["access_token"]

                # 2) MIGRATE usando o token do parceiro
                headers_m = {
                    "Authorization": f"Bearer {tok_partner}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "X-Request-Id": req_id_mig,
                }
                body_m = {"store": s, "login": l, "password": p, "affiliation_key": AFFILIATION_KEY}
                resp_m = requests.post(MIGRATE_URL, headers=headers_m, json=body_m, timeout=TIMEOUT)
                try:
                    payload_m = resp_m.json()
                except Exception:
                    payload_m = resp_m.text

                debug_output.value += fmt_debug("POST", MIGRATE_URL, headers_m, body_m, resp_m, req_id_mig) + "\n\n"
                page.update()

                if resp_m.status_code != 200 or not isinstance(payload_m, dict) or "client_id" not in payload_m or "client_secret" not in payload_m:
                    raw_output.value = pretty(payload_m)
                    status_text.value = f"Falha no migrate (HTTP {resp_m.status_code})."
                    status_text.color = ft.Colors.RED
                    page.update()
                    return

                client_id = str(payload_m["client_id"])
                client_secret = str(payload_m["client_secret"])

                # 3) Token do CLIENTE (client_credentials)
                headers_t = {"X-Request-Id": req_id_tok_cli, "Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"}
                data_t = {"grant_type": "client_credentials"}
                resp_t = requests.post(AUTH_URL, data=data_t, auth=HTTPBasicAuth(client_id, client_secret), headers=headers_t, timeout=TIMEOUT)
                try:
                    payload_t = resp_t.json()
                except Exception:
                    payload_t = resp_t.text
                debug_output.value += fmt_debug("POST", AUTH_URL, headers_t, data_t, resp_t, req_id_tok_cli) + "\n\n"
                page.update()

                if resp_t.status_code != 200 or not isinstance(payload_t, dict) or "access_token" not in payload_t:
                    raw_output.value = pretty(payload_t)
                    status_text.value = f"Falha ao obter token do cliente (HTTP {resp_t.status_code})."
                    status_text.color = ft.Colors.RED
                    page.update()
                    return

                tok_cliente = payload_t["access_token"]

                # 4) Portfólio com token do cliente
                params = {"one_page": "true"} if one_page.value else {}
                headers_p = {"Authorization": f"Bearer {tok_cliente}", "X-Request-Id": req_id_port, "Accept": "application/json"}
                resp_p = requests.get(PORTFOLIO_URL, headers=headers_p, params=params, timeout=TIMEOUT)
                try:
                    payload_p = resp_p.json()
                except Exception:
                    payload_p = resp_p.text
                # saída principal
                raw_output.value = pretty(payload_p)
                debug_output.value += fmt_debug("GET", PORTFOLIO_URL, headers_p | {"params": params}, None, resp_p, req_id_port) + "\n\n"

                # tabela
                items = extract_items(payload_p)
                table_container.content = ft.Column(
                    controls=[
                        ft.Text(f"Itens detectados: {len(items)}"),
                        ft.Container(content=build_table(items), expand=False)
                    ],
                    scroll=ft.ScrollMode.ALWAYS
                )

                if resp_p.status_code == 200:
                    status_text.value = f"Fluxo concluído com sucesso ({env_dd.value})."
                    status_text.color = ft.Colors.GREEN
                else:
                    status_text.value = f"Portfólio respondeu HTTP {resp_p.status_code}."
                    status_text.color = ft.Colors.RED

                page.update()

            except requests.exceptions.RequestException as ex:
                raw_output.value = str(ex)
                status_text.value = f"Erro de conexão: {ex}"
                status_text.color = ft.Colors.RED
                page.update()
            finally:
                set_busy(False)

        threading.Thread(target=worker, daemon=True).start()

    # UI
    header = ft.Row([ft.Icon(ft.Icons.HUB), ft.Text("RV Hub — Consulta Portfólio", size=22, weight=ft.FontWeight.BOLD), env_dd], alignment=ft.MainAxisAlignment.SPACE_BETWEEN)

    card_inputs = ft.Card(
        content=ft.Container(
            content=ft.Column([
                ft.Text("Informe apenas:", weight=ft.FontWeight.BOLD),
                ft.Row([partner_client_id, partner_client_secret], spacing=10),
                ft.Row([store, login], spacing=10),
                ft.Row([password, affiliation], spacing=10),
                ft.Row([one_page], spacing=10),
            ], spacing=10),
            padding=12
        )
    )

    btn_run = ft.ElevatedButton("Executar fluxo (Token Parceiro → Migrate → Token Cliente → Portfólio)", icon=ft.Icons.PLAY_ARROW, on_click=run_flow)
    status_bar = ft.Row([pr, status_text, btn_run], alignment=ft.MainAxisAlignment.SPACE_BETWEEN)

    card_table = ft.Card(
        content=ft.Container(
            content=ft.Column([
                ft.Text("Resultado do Portfólio (Tabela)", weight=ft.FontWeight.BOLD),
                table_container,
            ], spacing=10),
            padding=12
        )
    )

    card_outputs = ft.Card(
        content=ft.Container(
            content=ft.Column([
                ft.Text("Resposta Bruta (JSON) e Log Debug", weight=ft.FontWeight.BOLD),
                raw_output,
                debug_output,
            ], spacing=10),
            padding=12
        )
    )

    page.add(header, ft.Divider(), card_inputs, status_bar, card_table, card_outputs)

if __name__ == "__main__":
    ft.app(target=app_main)
