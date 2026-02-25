# MonitorCloudBackup.py
import time
import os
import json
import requests
import pandas as pd
import zipfile
import flet as ft
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict

# =============================
# CREDENCIAIS FIXAS (com override opcional por ENV)
# =============================
BASE_URL = (os.getenv("BASE_URL") or "https://backupcloud.fsassistencia.com.br").rstrip("/")
CLIENT_ID = os.getenv("CLIENT_ID") or "8857d247-853a-408d-9189-861c4d3efdfe"
CLIENT_SECRET = os.getenv("CLIENT_SECRET") or "mwh4sfypidsarqnk6ixpucy5dizuzv64br6olsc4dmrw476rg6fq"

DEFAULT_LIMIT = int(os.getenv("ALERTS_LIMIT", "200"))
TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "30"))
VERIFY_SSL = (os.getenv("VERIFY_SSL", "true").lower() != "false")

# Retries/backoff
RETRY_TOTAL = int(os.getenv("HTTP_RETRY_TOTAL", "3"))
RETRY_BACKOFF = float(os.getenv("HTTP_RETRY_BACKOFF", "0.8"))
RETRY_STATUS = (429, 500, 502, 503, 504)

tenant_name_cache: Dict[str, str] = {}

def _ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def _save_bytes(path: str, data: bytes) -> str:
    try:
        parent = os.path.dirname(path)
        if parent:
            _ensure_dir(parent)
        with open(path, "wb") as f:
            f.write(data)
        return path
    except Exception as e:
        return f"[erro ao salvar {path}: {e}]"

def _save_text(path: str, text: str) -> str:
    try:
        parent = os.path.dirname(path)
        if parent:
            _ensure_dir(parent)
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)
        return path
    except Exception as e:
        return f"[erro ao salvar {path}: {e}]"

# -----------------------------
# Sessão HTTP com retries/backoff
# -----------------------------
def build_session() -> requests.Session:
    s = requests.Session()
    try:
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        retry = Retry(
            total=RETRY_TOTAL,
            read=RETRY_TOTAL,
            connect=RETRY_TOTAL,
            backoff_factor=RETRY_BACKOFF,
            status_forcelist=RETRY_STATUS,
            allowed_methods=frozenset(["GET","POST","PUT","PATCH","DELETE","HEAD","OPTIONS"])
        )
        s.mount("https://", HTTPAdapter(max_retries=retry))
        s.mount("http://", HTTPAdapter(max_retries=retry))
    except Exception:
        # Se falhar, segue sem adapter (melhor do que quebrar)
        pass
    return s

session = build_session()

class ApiError(Exception):
    pass

def base_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}

def get_token() -> str:
    url = f"{BASE_URL}/api/2/idp/token"
    auth = (CLIENT_ID, CLIENT_SECRET)
    data = {"grant_type": "client_credentials"}
    r = session.post(url, auth=auth, data=data, timeout=TIMEOUT, verify=VERIFY_SSL)
    if r.status_code != 200:
        # Não logar secret — apenas status
        raise ApiError(f"Erro ao obter token ({r.status_code}).")
    return r.json().get("access_token", "")

def get_me(token: str) -> dict:
    url = f"{BASE_URL}/api/2/users/me"
    r = session.get(url, headers=base_headers(token), timeout=TIMEOUT, verify=VERIFY_SSL)
    if r.status_code != 200:
        raise ApiError(f"Erro em /users/me ({r.status_code}).")
    return r.json()

def list_subtenants(token: str, parent_id: str, limit: int = 500) -> List[dict]:
    if not parent_id:
        return []
    url = f"{BASE_URL}/api/2/tenants"
    params = {"parent_id": parent_id, "limit": limit}
    r = session.get(url, headers=base_headers(token), params=params, timeout=TIMEOUT, verify=VERIFY_SSL)
    if r.status_code != 200:
        return []
    js = r.json() if r.text else {}
    items = js.get("items") or js.get("tenants") or []
    return items

def headers_with_tenant(token: str, tenant_id: str) -> dict:
    h = base_headers(token)
    if tenant_id:
        h["X-Apigw-Tenant-Id"] = tenant_id
    return h

def busca_nome_tenant(token: str, tenant_id: str) -> str:
    if not tenant_id:
        return ""
    url = f"{BASE_URL}/api/2/tenants/{tenant_id}"
    r = session.get(url, headers=base_headers(token), timeout=TIMEOUT, verify=VERIFY_SSL)

    js = r.json()
    print(js)
    nome = js.get("name")
    return nome or ""


# Tentativa genérica de paginação: suporta 'next' (link/offset) se vier; se não, pega 1 página só.
def fetch_alerts(
    token: str,
    tenant_id: str,
    limit: int = DEFAULT_LIMIT,
    order: Optional[str] = None,     # mantido para compatibilidade
    os_filter: Optional[str] = None, # mantido para compatibilidade
    category: Optional[str] = None   # mantido para compatibilidade
) -> List[dict]:
    url = f"{BASE_URL}/api/alert_manager/v1/alerts"
    params = {"limit": limit}
    if category:
        params["category"] = category

    all_items: List[dict] = []
    seen = set()
    page_count = 0

    while True:
        r = session.get(
            url,
            headers=headers_with_tenant(token, tenant_id),
            params=params,
            timeout=TIMEOUT,
            verify=VERIFY_SSL,
        )
        if r.status_code != 200:
            raise ApiError(f"Erro ao listar alertas (tenant {tenant_id}, {r.status_code}).")
        js = r.json() if r.text else {}
        # Estruturas possíveis: {"items":[...], "next": "..."} | lista simples [...]
        items = js.get("items") if isinstance(js, dict) else js
        items = items or []
        for it in items:
            # evita duplicado em paginações esquisitas
            key = it.get("id") or json.dumps(it, sort_keys=True)
            if key not in seen:
                seen.add(key)
                all_items.append(it)

        # Tenta descobrir paginação
        next_token = None
        if isinstance(js, dict):
            next_token = js.get("next") or js.get("next_token") or js.get("offset") or js.get("continuation")
        page_count += 1

        if not next_token or page_count >= 50:  # hard-stop de segurança
            break

        # Ajusta parâmetros para próxima página — heurística
        if "next" in js and isinstance(js["next"], str) and js["next"].startswith("http"):
            # Alguns serviços devolvem URL completa; usamos ela e zeramos params
            url = js["next"]
            params = {}
        else:
            # Caso devolva "offset"/"continuation", reaproveita a mesma URL com param
            params["offset"] = next_token

    return all_items

def fetch_workload_name(token: str, workload_id: str) -> str:
    if not workload_id:
        return ""
    url = f"{BASE_URL}/api/workload_management/v5/workloads/{workload_id}"
    r = session.get(url, headers=base_headers(token), timeout=TIMEOUT, verify=VERIFY_SSL)
    if r.status_code == 200:
        j = r.json() if r.text else {}
        return j.get("name") or j.get("display_name") or j.get("hostname") or ""
    return ""

def fetch_plan_name_by_ids(token: str, policy_id: str = "", plan_id: str = "") -> str:
    if policy_id:
        url = f"{BASE_URL}/api/resource_management/v2/policies/{policy_id}"
        r = session.get(url, headers=base_headers(token), timeout=TIMEOUT, verify=VERIFY_SSL)
        if r.status_code == 200:
            j = r.json() if r.text else {}
            return j.get("name") or j.get("policy", {}).get("name") or ""
    if plan_id:
        url = f"{BASE_URL}/api/resource_management/v2/plans/{plan_id}"
        r = session.get(url, headers=base_headers(token), timeout=TIMEOUT, verify=VERIFY_SSL)
        if r.status_code == 200:
            j = r.json() if r.text else {}
            return j.get("name") or j.get("plan", {}).get("name") or ""
    return ""

def fetch_tenant_name(token: str, tenant_id: str) -> str:
    if not tenant_id:
        return ""
    if tenant_id in tenant_name_cache:
        return tenant_name_cache[tenant_id]
    url = f"{BASE_URL}/api/2/tenants/{tenant_id}"
    r = session.get(url, headers=base_headers(token), timeout=TIMEOUT, verify=VERIFY_SSL)
    if r.status_code == 200:
        j = r.json() if r.text else {}
        name = j.get("TenantName") or j.get("tenantName") or j.get("name") or j.get("display_name") or ""
        if name:
            tenant_name_cache[tenant_id] = name
            return name
    tenant_name_cache[tenant_id] = tenant_id
    return tenant_id

def normalize_alert(token: str, a: dict) -> dict:
    det = a.get("details") or {}
    tenant = a.get("tenant") or {}
    ctx = det.get("context") or {}

    severity = a.get("severity", "")
    alert_type = a.get("type", "")
    message = det.get("message") or det.get("reason") or det.get("description") or ""
    dt = a.get("receivedAt") or a.get("createdAt") or ""

    # Adiciona daysPassed ao tipo do alerta se for NoBackupForXDays
    if alert_type == "NoBackupForXDays":
        days = det.get("daysPassed")
        if days is not None:
            alert_type = f"NoBackupForXDays - {days} dias"

    err = (det.get("error") or {})
    err_fields = (err.get("fields") or {})
    err_ctx = ((det.get("errorMessage") or {}).get("context") or {})
    client = (
        tenant.get("TenantName") or tenant.get("tenantName") or
        err_fields.get("TenantName") or err_ctx.get("tenantName") or
        tenant.get("name") or tenant.get("display_name") or ""
    )
    if not client:
        tid = tenant.get("id") or tenant.get("uuid") or err_fields.get("TenantID") or ""
        client = fetch_tenant_name(token, tid) if tid else ""

    client = busca_nome_tenant(token, tenant.get("uuid"))

    workload_name = det.get("machineName") or det.get("resourceName") or ctx.get("resource_name") or ""
    if not workload_name:
        workload_id = det.get("workloadId") or det.get("resourceId") or ctx.get("resource_id") or ctx.get("workload_id") or ""
        if workload_id:
            try:
                workload_name = fetch_workload_name(token, workload_id)
            except Exception:
                workload_name = ""


    plan_name = det.get("protectionPlanName") or det.get("planName") or ctx.get("plan_name") or ""
    if not plan_name:
        policy_id = det.get("policyId") or ctx.get("policy_id") or ""
        plan_id = det.get("planId") or ctx.get("plan_id") or det.get("protectionPlanId") or ""
        try:
            plan_name = fetch_plan_name_by_ids(token, policy_id, plan_id)
        except Exception:
            plan_name = ""

    return {
        "Severidade": severity,
        "Tipo do alerta": alert_type,
        "Mensagem": message,
        "Carga de trabalho": workload_name,
        "Cliente": client,
        "Data e hora": dt,
        "Plano": plan_name,
        "_raw": a,
    }
# ...existing code...

def build_rows(token: str, alerts: List[dict]) -> List[dict]:
    return [normalize_alert(token, a) for a in alerts]

def build_csv(rows: List[dict], sep: str = ";") -> bytes:
    df = pd.DataFrame(rows, columns=[
        "Severidade", "Tipo do alerta", "Mensagem",
        "Carga de trabalho", "Cliente", "Data e hora", "Plano"
    ])
    return df.to_csv(index=False, sep=sep).encode("utf-8-sig")

# -------- UI (Flet) --------
def main(page: ft.Page):
    page.title = "Painel de Alertas — Acronis (Flet)"
    page.window_width = 1200
    page.window_height = 800
    page.horizontal_alignment = "stretch"
    page.vertical_alignment = "start"

    info = ft.Text("Autenticando...", size=14)
    include_subs_cb = ft.Checkbox(label="Incluir subtenants (todos)", value=True)
    limit_slider = ft.Slider(min=50, max=1000, value=DEFAULT_LIMIT, divisions=19, label="{value}", width=400)

    tipo_alerta_tf = ft.TextField(label="Tipo de alerta (contém)", width=320, hint_text="ex.: backup_failed")

    refresh_btn = ft.ElevatedButton("Atualizar dados", icon=ft.Icons.REFRESH)
    export_btn = ft.ElevatedButton("Exportar CSV", icon=ft.Icons.DOWNLOAD, disabled=True)
    open_folder_btn = ft.ElevatedButton("Abrir pasta de export", icon=ft.Icons.FOLDER_OPEN)
    progress = ft.ProgressBar(width=400, visible=False)

    export_folder_cb = ft.Checkbox(label="Salvar em subpasta 'export/'", value=True)
    zip_cb = ft.Checkbox(label="Compactar em ZIP", value=True)
    csv_sep_dd = ft.Dropdown(label="Separador CSV", width=160, options=[
        ft.dropdown.Option(";"), ft.dropdown.Option(","), ft.dropdown.Option("\t")
    ], value=";")

    columns = [
        ft.DataColumn(ft.Text("Severidade")),
        ft.DataColumn(ft.Text("Tipo do alerta")),
        ft.DataColumn(ft.Text("Mensagem")),
        ft.DataColumn(ft.Text("Carga de trabalho")),
        ft.DataColumn(ft.Text("Cliente")),
        ft.DataColumn(ft.Text("Data e hora")),
        ft.DataColumn(ft.Text("Plano")),
    ]
    table = ft.DataTable(
        columns=columns,
        rows=[],
        column_spacing=16,
        data_row_max_height=80,
        heading_text_style=ft.TextStyle(weight=ft.FontWeight.BOLD)
    )

    page.add(
        ft.Column([
            info,
            ft.Row(
                [
                    include_subs_cb,
                    ft.Text("Qtd.:"),
                    limit_slider,
                    tipo_alerta_tf,
                    csv_sep_dd,
                    export_folder_cb,
                    zip_cb,
                    refresh_btn,
                    export_btn,
                    open_folder_btn,
                ],
                wrap=True
            ),
            progress,
            table,
        ], expand=True, scroll=ft.ScrollMode.AUTO)
    )

    state = {"token": "", "me_tid": "", "subs": [], "rows": [], "csv": b"", "last_export_dir": os.getcwd()}

    def set_progress(on: bool, text: str = ""):
        progress.visible = on
        if text:
            info.value = text
        page.update()

    # Auto-auth and tenant discovery
    try:
        set_progress(True, "Autenticando...")
        token = get_token()
        state["token"] = token
        me = get_me(token)
        my_tid = me.get("tenant_id") or me.get("tenantId") or ""
        state["me_tid"] = my_tid
        info.value = f"Autenticado. Tenant: {my_tid}"
        set_progress(True, "Listando subtenants...")
        state["subs"] = list_subtenants(token, my_tid) if my_tid else []
        set_progress(False, "Pronto. Clique em Atualizar dados.")
    except Exception as e:
        set_progress(False, f"Erro de autenticação/listagem: {e}")

    def fill_table(rows: List[dict]):
        table.rows = []
        for r in rows:
            table.rows.append(ft.DataRow(cells=[
                ft.DataCell(ft.Text(r.get("Severidade",""))),
                ft.DataCell(ft.Text(r.get("Tipo do alerta",""))),
                ft.DataCell(ft.Text(r.get("Mensagem",""), selectable=True)),
                ft.DataCell(ft.Text(r.get("Carga de trabalho",""))),
                ft.DataCell(ft.Text(r.get("Cliente",""))),
                ft.DataCell(ft.Text(r.get("Data e hora",""))),
                ft.DataCell(ft.Text(r.get("Plano",""))),
            ]))
        page.update()

    def on_export_click(e):
        if not state["csv"]:
            return
        try:
            import base64
            b64 = base64.b64encode(state["csv"]).decode("ascii")
            page.launch_url(f"data:text/csv;base64,{b64}")
        except Exception:
            fname = f"alertas_acronis_{int(time.time())}.csv"
            with open(fname, "wb") as f:
                f.write(state["csv"])
            info.value = f"CSV salvo: {fname}"
            page.update()

    export_btn.on_click = on_export_click

    def on_open_folder_click(e):
        out_dir = os.path.join(os.getcwd(), "export") if export_folder_cb.value else os.getcwd()
        state["last_export_dir"] = out_dir
        # Tenta abrir no SO (Windows/macOS/Linux)
        try:
            if os.name == "nt":
                os.startfile(out_dir)  # type: ignore[attr-defined]
            elif os.uname().sysname == "Darwin":
                os.system(f'open "{out_dir}"')
            else:
                os.system(f'xdg-open "{out_dir}"')
        except Exception:
            info.value = f"Pasta de export: {out_dir}"
            page.update()

    open_folder_btn.on_click = on_open_folder_click

    def on_refresh_click(e):
        try:
            set_progress(True, "Buscando alertas...")
            token = state["token"]
            tenants_to_fetch = [state["me_tid"]] if state["me_tid"] else []
            if include_subs_cb.value and state["subs"]:
                tenants_to_fetch += [t.get("id") or t.get("uuid") for t in state["subs"] if (t.get("id") or t.get("uuid"))]
            tenants_to_fetch = [t for t in dict.fromkeys(tenants_to_fetch) if t]

            limit = int(limit_slider.value)
            all_alerts: List[dict] = []
            for tid in tenants_to_fetch or [state["me_tid"]]:
                try:
                    alerts = fetch_alerts(token, tid, limit=limit)
                    all_alerts.extend(alerts)
                except Exception as ex_tenant:
                    print(f"Falha ao buscar alerts para tenant {tid}: {ex_tenant}")

            rows = build_rows(token, all_alerts)

            # Filtro "Tipo (contém)"
            filtro_tipo = (tipo_alerta_tf.value or "").strip().lower()
            if filtro_tipo:
                rows = [r for r in rows if filtro_tipo in (r.get("Tipo do alerta", "") or "").lower()]

            state["rows"] = rows
            state["csv"] = build_csv(rows, sep=("\t" if csv_sep_dd.value == "\t" else csv_sep_dd.value))

            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            out_dir = os.path.join(os.getcwd(), "export") if export_folder_cb.value else os.getcwd()
            state["last_export_dir"] = out_dir

            # 1) JSON bruto
            raw_json_bytes = json.dumps(all_alerts, ensure_ascii=False, indent=2).encode("utf-8")
            raw_path = _save_bytes(os.path.join(out_dir, f"alerts_raw_{ts}.json"), raw_json_bytes)

            # 2) Linhas JSON (normalizadas + filtro)
            rows_json_bytes = json.dumps(rows, ensure_ascii=False, indent=2).encode("utf-8")
            rows_json_path = _save_bytes(os.path.join(out_dir, f"alerts_rows_{ts}.json"), rows_json_bytes)

            # 3) CSV (normalizado + filtro)
            csv_path = _save_bytes(os.path.join(out_dir, f"alerts_rows_{ts}.csv"), state["csv"])

            # 4) ZIP opcional
            zip_path = ""
            if zip_cb.value:
                try:
                    zip_path = os.path.join(out_dir, f"alerts_export_{ts}.zip")
                    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
                        if os.path.exists(raw_path): z.write(raw_path, arcname=os.path.basename(raw_path))
                        if os.path.exists(rows_json_path): z.write(rows_json_path, arcname=os.path.basename(rows_json_path))
                        if os.path.exists(csv_path): z.write(csv_path, arcname=os.path.basename(csv_path))
                except Exception as _ez:
                    zip_path = f"[erro ao criar ZIP: {_ez}]"

            info.value = (
                f"Arquivos salvos em: {out_dir}\n"
                f" - {raw_path}\n - {rows_json_path}\n - {csv_path}"
                + (f"\n - {zip_path}" if zip_path else "")
                + (f"\nFiltro aplicado: tipo contém '{filtro_tipo}'" if filtro_tipo else "")
            )

            fill_table(rows)
            export_btn.disabled = False if rows else True
            set_progress(False, f"{len(rows)} alertas carregados.")
        except Exception as ex:
            export_btn.disabled = True
            table.rows = []
            set_progress(False, f"Erro: {ex}")

    refresh_btn.on_click = on_refresh_click

if __name__ == "__main__":
    ft.app(target=main)