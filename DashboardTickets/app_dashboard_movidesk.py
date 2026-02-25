# app_flet_movidesk.py
# Dashboard Executivo Movidesk (Flet + Plotly) - Somente API (sem CSV)
# Ajustes solicitados:
# - Token pré-preenchido
# - "Testar conexão" robusto (sempre usa $select=id, mostra status, URL e preview)
# - Mensagens claras quando vazio/erro
# - $select obrigatório em TODAS as chamadas

import time
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone

import flet as ft
from flet.plotly_chart import PlotlyChart
import plotly.express as px
import plotly.graph_objects as go
import traceback

MOVIDESK_BASE = "https://api.movidesk.com/public/v1"
DATE_FMT = "%Y-%m-%dT%H:%M:%S.%fZ"

# Compatibilidade Flet: garantir que tanto ft.Colors quanto ft.colors existam
try:
    _ = ft.Colors  # type: ignore[attr-defined]
except Exception:
    if hasattr(ft, "colors"):
        try:
            ft.Colors = ft.colors  # type: ignore[attr-defined]
        except Exception:
            pass
if not hasattr(ft, "colors") and hasattr(ft, "Colors"):
    try:
        ft.colors = ft.Colors  # type: ignore[attr-defined]
    except Exception:
        pass

def _with_opacity(alpha: float, base_color):
    """Usa with_opacity do Flet se existir; caso contrário, fallback rgba()."""
    for attr in ("Colors", "colors"):
        c = getattr(ft, attr, None)
        if c and hasattr(c, "with_opacity"):
            try:
                return c.with_opacity(alpha, base_color)
            except Exception:
                pass
    # Fallback para rgba simples
    if isinstance(base_color, str):
        s = base_color.strip()
        if s.startswith("#") and len(s) == 7:
            try:
                r = int(s[1:3], 16)
                g = int(s[3:5], 16)
                b = int(s[5:7], 16)
                return f"rgba({r},{g},{b},{alpha})"
            except Exception:
                pass
        if s.lower() == "white":
            return f"rgba(255,255,255,{alpha})"
        if s.lower() == "black":
            return f"rgba(0,0,0,{alpha})"
    return f"rgba(255,255,255,{alpha})"

# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# Deixei o token pré-preenchido aqui:
DEFAULT_TOKEN = "d543de0b-4d36-401a-8591-c85121818feb"
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

class MovideskAPIError(Exception):
    pass

def _utc_iso(d: datetime) -> str:
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    else:
        d = d.astimezone(timezone.utc)
    return d.strftime(DATE_FMT)

def _handle_rate(resp: requests.Response) -> bool:
    if resp.status_code == 429:
        wait = float(resp.headers.get("retry-after", "10"))
        time.sleep(wait)
        return True
    return False

def _fetch_page(path: str, params: dict) -> list:
    url = f"{MOVIDESK_BASE}/{path}"
    while True:
        r = requests.get(url, params=params, timeout=60)
        # Log seguro (sem token) da chamada HTTP
        try:
            safe_url = r.url.replace(params.get("token", ""), "***") if hasattr(r, "url") else url
            print(f"[HTTP] GET {safe_url} -> {r.status_code}", flush=True)
        except Exception:
            pass
        if _handle_rate(r):
            try:
                print("[HTTP] 429 recebido. Aguardando antes de tentar novamente...", flush=True)
            except Exception:
                pass
            continue
        if not r.ok:
            preview = (r.text or "")[:500]
            # NÃO exibir o token em mensagens: a URL aqui já virá sem ele se ocultarmos antes
            safe_url = r.url.replace(params.get("token", ""), "***")
            raise MovideskAPIError(f"[GET {path}] {r.status_code} – {safe_url}\n{preview}")
        return r.json()

def _build_filter(created_ge: str | None, created_le: str | None, extra_filters: list[str] | None) -> str | None:
    parts = []
    if created_ge:
        parts.append(f"createdDate ge {created_ge}")
    if created_le:
        parts.append(f"createdDate le {created_le}")
    if extra_filters:
        parts.extend([x.strip() for x in extra_filters if x and x.strip()])
    return " and ".join(parts) if parts else None

def fetch_tickets_union(
    token: str,
    created_ge_iso: str | None,
    created_le_iso: str | None,
    select: str,
    expand: str | None,
    extra_filters: list[str] | None,
    page_size: int = 200,
    max_pages: int | None = 5,
) -> list:
    if not select or not select.strip():
        raise ValueError("O parâmetro $select é obrigatório e não pode ser vazio.")

    all_items = {}
    for path in ["tickets", "tickets/past"]:
        skip = 0
        pages = 0
        while True:
            params = {
                "token": token,
                "$top": page_size,
                "$skip": skip,
                "$select": select,
            }
            if expand:
                params["$expand"] = expand
            f = _build_filter(created_ge_iso, created_le_iso, extra_filters)
            if f:
                params["$filter"] = f

            data = _fetch_page(path, params)
            if not data:
                break
            for item in data:
                all_items[item.get("id")] = item
            skip += page_size
            pages += 1
            if max_pages and pages >= max_pages:
                break
    return list(all_items.values())

def normalize_tickets(raw: list) -> pd.DataFrame:
    if not raw:
        return pd.DataFrame()
    df = pd.json_normalize(raw, sep=".")
    base_cols = [
        "id","protocol","type","subject","category","urgency","status","baseStatus",
        "createdDate","lastUpdate",
        "owner.id","owner.businessName","owner.email","ownerTeam",
        "createdBy.id","createdBy.businessName","createdBy.email",
        "actionCount","lifetimeWorkingTime","stoppedTime","stoppedTimeWorkingTime",
        "slaAgreement","slaResponseTime","slaSolutionTime","slaSolutionDate","slaResponseDate","slaRealResponseDate",
    ]
    for c in base_cols:
        if c not in df.columns:
            df[c] = np.nan
    for c in ["createdDate","lastUpdate","slaSolutionDate","slaResponseDate","slaRealResponseDate"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce", utc=True)
    df = df.rename(columns={
        "owner.businessName": "ownerName",
        "createdBy.businessName": "createdByName"
    })

    # Cliente principal (primeiro da lista), se disponível no raw
    primary_client = {}
    try:
        for item in raw:
            tid = item.get("id")
            cname = None
            clist = item.get("clients")
            if isinstance(clist, list) and clist:
                c0 = clist[0] or {}
                cname = c0.get("businessName") or c0.get("email") or c0.get("id")
            primary_client[tid] = cname
        df["clientPrimaryName"] = df["id"].map(primary_client)
    except Exception:
        df["clientPrimaryName"] = np.nan

    # Campos auxiliares
    try:
        if "createdDate" in df and "lastUpdate" in df:
            df["duration_h"] = (df["lastUpdate"] - df["createdDate"]).dt.total_seconds() / 3600.0
    except Exception:
        df["duration_h"] = np.nan
    return df

def derive_metrics(df: pd.DataFrame, tma_mode: str = "resolved_closed") -> dict:
    if df.empty:
        return dict(total=0, em_at=0, resolvidos=0, fechados=0, tma_h=0.0, parados_h=0.0, first_resp=0.0)
    total = len(df)
    em_at = (df["baseStatus"] == "InAttendance").sum()
    resolvidos = (df["baseStatus"] == "Resolved").sum()
    fechados = (df["baseStatus"] == "Closed").sum()

    # TMA conforme modo selecionado:
    # - resolved_closed (padrão): apenas Resolved/Closed; preferir lifetimeWorkingTime>0; fallback para (lastUpdate - createdDate)
    # - lifetime_all: todos os tickets; preferir lifetimeWorkingTime>0; fallback para (lastUpdate - createdDate)
    # - elapsed_all: todos os tickets; usar somente (lastUpdate - createdDate)
    tma_h = 0.0
    try:
        mode = (tma_mode or "resolved_closed").strip().lower()
        if mode == "lifetime_all":
            base = df.copy()
            prefer_lifetime = True
        elif mode == "elapsed_all":
            base = df.copy()
            prefer_lifetime = False
        else:  # resolved_closed (default)
            base = df[df["baseStatus"].isin(["Resolved", "Closed"])].copy()
            prefer_lifetime = True

        if not base.empty and prefer_lifetime:
            s = pd.to_numeric(base.get("lifetimeWorkingTime"), errors="coerce")
            if s is not None:
                s = s[(~pd.isna(s)) & (s > 0)]
                if not s.empty:
                    tma_h = float(s.mean()) / 60.0  # minutos -> horas

        if (tma_h == 0.0 or pd.isna(tma_h)) and "createdDate" in base and "lastUpdate" in base:
            cd = pd.to_datetime(base["createdDate"], errors="coerce", utc=True)
            lu = pd.to_datetime(base["lastUpdate"], errors="coerce", utc=True)
            dur_h = (lu - cd).dt.total_seconds() / 3600.0
            dur_h = dur_h[(~pd.isna(dur_h)) & (dur_h >= 0)]
            if not dur_h.empty:
                tma_h = float(dur_h.mean())
    except Exception:
        tma_h = 0.0

    # Tempo parado: média apenas de valores positivos
    parados_h = 0.0
    try:
        stw = pd.to_numeric(df.get("stoppedTimeWorkingTime"), errors="coerce")
        if stw is not None:
            stw = stw[(~pd.isna(stw)) & (stw > 0)]
            if not stw.empty:
                parados_h = float(stw.mean()) / 60.0  # minutos -> horas
    except Exception:
        parados_h = 0.0

    tma_h = 0.0 if pd.isna(tma_h) else round(tma_h, 2)
    parados_h = 0.0 if pd.isna(parados_h) else round(parados_h, 2)
    first_resp = float((~df["slaRealResponseDate"].isna()).mean() * 100.0)

    return dict(
        total=int(total),
        em_at=int(em_at),
        resolvidos=int(resolvidos),
        fechados=int(fechados),
        tma_h=round(tma_h, 2),
        parados_h=round(parados_h, 2),
        first_resp=round(first_resp, 1),
    )

def test_basic_ping(token: str) -> dict:
    """
    Valida token/rede puxando 1 item em cada rota.
    Usa SEMPRE $select=id (a API exige).
    Mostra status, URL (sem token) e preview do retorno.
    """
    out = {}
    for path in ["tickets", "tickets/past"]:
        url = f"{MOVIDESK_BASE}/{path}"
        params = {"token": token, "$top": 1, "$select": "id"}
        r = requests.get(url, params=params, timeout=30)
        # esconder token
        safe_url = r.url.replace(token, "***")
        out[path] = {
            "status": r.status_code,
            "ok": r.ok,
            "url": safe_url,
            "text": (r.text or "")[:220],
        }
    return out

def main(page: ft.Page):
    page.title = "Painel Executivo • Movidesk (Flet)"
    page.theme_mode = ft.ThemeMode.DARK
    page.padding = 16
    page.window_min_width = 1100
    page.window_min_height = 720
    # Habilitar rolagem vertical para ver todos os blocos/KPIs
    page.scroll = ft.ScrollMode.AUTO

    ACCENT = ft.Colors.CYAN_ACCENT_400
    CARD_BG = _with_opacity(0.08, ft.Colors.WHITE)

    title = ft.Text("Painel Executivo • Atendimento Movidesk", size=20, weight=ft.FontWeight.BOLD)
    subtitle = ft.Text("Visão 360° de tickets, produtividade e SLA (somente API)", color=ft.Colors.GREY_400, size=12)

    # token pré-preenchido aqui:
    token_field = ft.TextField(label="Token Movidesk", password=True, can_reveal_password=True, width=420, value=DEFAULT_TOKEN)

    # Usar timezone-aware para evitar avisos de depreciação
    today = datetime.now(timezone.utc).date()
    default_ini = today - timedelta(days=30)
    dt_ini = ft.TextField(label="Data inicial (UTC) YYYY-MM-DD", value=str(default_ini), width=200)
    dt_fim = ft.TextField(label="Data final (UTC) YYYY-MM-DD", value=str(today), width=200)

    status_tf = ft.TextField(label="Status (ex.: InAttendance)", width=200)
    urg_tf = ft.TextField(label="Urgência (ex.: Alta)", width=200)
    cat_tf = ft.TextField(label="Categoria", width=240)
    cli_tf = ft.TextField(label="Cliente (clients.id ou email)", width=300)

    expand_actions_cb = ft.Checkbox(label="Expandir ações e apontamentos", value=False)
    expand_clients_cb = ft.Checkbox(label="Expandir clientes", value=True)

    # Seletor de modo do TMA
    tma_mode_dd = ft.Dropdown(
        label="Modo do TMA",
        width=280,
        value="Resolvidos/Fechados (útil)",
        options=[
            ft.dropdown.Option("Resolvidos/Fechados (útil)"),
            ft.dropdown.Option("Lifetime (útil) - todos"),
            ft.dropdown.Option("Decorrido (calendário)"),
        ],
    )

    top_tf = ft.TextField(label="$top (página)", value="200", width=120)
    max_pages_tf = ft.TextField(label="Máx. páginas por rota", value="5", width=160)

    fetch_btn = ft.ElevatedButton("🚀 BUSCAR DADOS", bgcolor=ACCENT, color=ft.Colors.BLACK, height=45)
    ping_btn = ft.OutlinedButton("🔌 Testar conexão", height=40)

    def kpi(title, value, subtitle):
        return ft.Container(
            bgcolor=CARD_BG, border_radius=16, padding=16,
            content=ft.Column([
                ft.Text(title, size=12, color=ft.Colors.GREY_400),
                ft.Text(value, size=26, weight=ft.FontWeight.W_800),
                ft.Text(subtitle, size=11, color=ft.Colors.GREY_500),
            ], spacing=2),
        )

    kpi_total = kpi("Total", "—", "tickets no período")
    kpi_em_at = kpi("Em Atendimento", "—", "baseStatus=InAttendance")
    kpi_resolv = kpi("Resolvidos", "—", "baseStatus=Resolved")
    kpi_fech = kpi("Fechados", "—", "baseStatus=Closed")
    kpi_tma = kpi("TMA (h)", "—", "tempo médio útil")
    kpi_first = kpi("1ª Resposta", "—", "% com resposta registrada")

    # Novos gráficos (placeholders com go.Figure vazio)
    chart_open_close = PlotlyChart(go.Figure().update_layout(template="plotly_dark", title="Abertos vs Encerrados"), expand=True)
    chart_backlog = PlotlyChart(go.Figure().update_layout(template="plotly_dark", title="Backlog ao longo do tempo"), expand=True)
    chart_status_mix = PlotlyChart(go.Figure().update_layout(template="plotly_dark", title="Status mix por dia (aprox.)"), expand=True)

    chart_tma_team = PlotlyChart(go.Figure().update_layout(template="plotly_dark", title="TMA por Equipe"), expand=True)
    chart_tma_cat = PlotlyChart(go.Figure().update_layout(template="plotly_dark", title="TMA por Categoria"), expand=True)
    chart_tma_client = PlotlyChart(go.Figure().update_layout(template="plotly_dark", title="TMA por Cliente (principal)"), expand=True)

    chart_sla_resp = PlotlyChart(go.Figure().update_layout(template="plotly_dark", title="SLA Resposta: Dentro x Fora (mês)"), expand=True)
    chart_sla_sol = PlotlyChart(go.Figure().update_layout(template="plotly_dark", title="SLA Solução: Dentro x Fora (mês)"), expand=True)
    chart_first_resp_trend = PlotlyChart(go.Figure().update_layout(template="plotly_dark", title="Tempo até 1ª resposta — p50 e p95 (h)"), expand=True)

    chart_hist_lifetime = PlotlyChart(go.Figure().update_layout(template="plotly_dark", title="Distribuição — Tempo útil (horas)"), expand=True)
    chart_hist_stopped = PlotlyChart(go.Figure().update_layout(template="plotly_dark", title="Distribuição — Tempo parado (horas)"), expand=True)

    chart_heat_owner_day = PlotlyChart(go.Figure().update_layout(template="plotly_dark", title="Heatmap — Volume por Responsável x Dia"), expand=True)
    chart_hist_actions = PlotlyChart(go.Figure().update_layout(template="plotly_dark", title="Ações por ticket (histograma)"), expand=True)
    chart_worktime_owner = PlotlyChart(go.Figure().update_layout(template="plotly_dark", title="Tempo apontado por responsável (requer expand)"), expand=True)

    chart_pareto_clients = PlotlyChart(go.Figure().update_layout(template="plotly_dark", title="Pareto de Clientes (volume)"), expand=True)
    chart_heat_cat_urg = PlotlyChart(go.Figure().update_layout(template="plotly_dark", title="Heatmap — Categoria x Urgência"), expand=True)
    chart_origin_month = PlotlyChart(go.Figure().update_layout(template="plotly_dark", title="Origem do ticket (ações) por mês"), expand=True)

    chart_heat_dow_hour = PlotlyChart(go.Figure().update_layout(template="plotly_dark", title="Heatmap — Criações por Dia da Semana x Hora"), expand=True)
    chart_monthly_season = PlotlyChart(go.Figure().update_layout(template="plotly_dark", title="Sazonalidade mensal (aberturas)"), expand=True)

    chart_top_tma = PlotlyChart(go.Figure().update_layout(template="plotly_dark", title="Top 20 TMA por ticket (horas)"), expand=True)
    chart_top_stopped = PlotlyChart(go.Figure().update_layout(template="plotly_dark", title="Top 20 Tempo parado por ticket (horas)"), expand=True)

    table = ft.DataTable(
        columns=[
            ft.DataColumn(ft.Text("ID")),
            ft.DataColumn(ft.Text("Protocolo")),
            ft.DataColumn(ft.Text("Assunto")),
            ft.DataColumn(ft.Text("Categoria")),
            ft.DataColumn(ft.Text("Urgência")),
            ft.DataColumn(ft.Text("Status")),
            ft.DataColumn(ft.Text("BaseStatus")),
            ft.DataColumn(ft.Text("Responsável")),
            ft.DataColumn(ft.Text("Equipe")),
            ft.DataColumn(ft.Text("Criado em (UTC)")),
        ],
        rows=[],
        heading_row_height=36,
        data_row_min_height=32,
        column_spacing=20,
        divider_thickness=0,
    )

    def on_ping(e):
        token = token_field.value.strip()
        if not token:
            page.snack_bar = ft.SnackBar(ft.Text("Informe o token Movidesk."), bgcolor=ft.Colors.RED_600)
            page.snack_bar.open = True
            page.update()
            try:
                print("[PING] Token ausente. Informe o token Movidesk.", flush=True)
            except Exception:
                pass
            return
        try:
            page.splash = ft.ProgressBar()
            page.update()
            res = test_basic_ping(token)
            page.splash = None
            msgs = []
            for path, info in res.items():
                msgs.append(f"{path}: {info['status']} | ok={info['ok']}\n{info['url']}\npreview={info['text']!r}")
            page.snack_bar = ft.SnackBar(ft.Text("\n\n".join(msgs)), bgcolor=ft.Colors.BLUE_GREY_700, duration=6000)
            page.snack_bar.open = True
            page.update()
            try:
                print("[PING] Resultado:", flush=True)
                for m in msgs:
                    print(m, flush=True)
            except Exception:
                pass
        except Exception as ex:
            page.splash = None
            page.snack_bar = ft.SnackBar(ft.Text(f"Erro no ping: {ex}"), bgcolor=ft.Colors.RED_700)
            page.snack_bar.open = True
            page.update()
            try:
                print(f"[PING][ERRO] {ex}", flush=True)
            except Exception:
                pass

    def on_fetch(e):
        try:
            token = token_field.value.strip()
            if not token:
                page.snack_bar = ft.SnackBar(ft.Text("Informe o token Movidesk."), bgcolor=ft.Colors.RED_600)
                page.snack_bar.open = True
                page.update()
                try:
                    print("[FETCH] Token ausente. Informe o token Movidesk.", flush=True)
                except Exception:
                    pass
                return

            ge_iso = f"{dt_ini.value.strip()}T00:00:00.00Z" if dt_ini.value.strip() else None
            le_iso = f"{dt_fim.value.strip()}T23:59:59.99Z" if dt_fim.value.strip() else None

            # OData da Movidesk não aceita paths (owner.id) em $select do recurso raiz.
            # Em vez disso, selecione apenas campos escalares no $select e use $expand com $select aninhado para owner/createdBy/clients/actions.
            select = ",".join([
                "id","protocol","type","subject","category","urgency","status","baseStatus",
                "createdDate","lastUpdate",
                # Somente escalares no topo:
                "ownerTeam",
                "actionCount","lifetimeWorkingTime","stoppedTime","stoppedTimeWorkingTime",
                "slaAgreement","slaResponseTime","slaSolutionTime","slaSolutionDate","slaResponseDate","slaRealResponseDate",
            ])

            expand_parts = []
            # Sempre traga dados de owner e createdBy via expand (pois removemos do $select raiz)
            expand_parts.append("owner($select=id,businessName,email)")
            expand_parts.append("createdBy($select=id,businessName,email)")

            if expand_clients_cb.value:
                expand_parts.append("clients($select=id,businessName,email,personType,profileType,organization)")
            if expand_actions_cb.value:
                actions_sel = "id,origin,createdDate,status,justification,description"
                timeappt_sel = "id,date,periodStart,periodEnd,workTime,accountedTime,createdBy"
                expand_parts.append(
                    f"actions($select={actions_sel};$expand=timeAppointments($select={timeappt_sel}))"
                )
            expand = ",".join(expand_parts) if expand_parts else None

            extra = []
            if status_tf.value.strip():
                extra.append(f"status eq '{status_tf.value.strip()}'")
            if urg_tf.value.strip():
                extra.append(f"urgency eq '{urg_tf.value.strip()}'")
            if cat_tf.value.strip():
                extra.append(f"category eq '{cat_tf.value.strip()}'")
            if cli_tf.value.strip():
                cli = cli_tf.value.strip()
                extra.append(f"(clients/any(c: c/id eq '{cli}') or clients/any(c: c/email eq '{cli}'))")

            page.splash = ft.ProgressBar()
            page.update()
            # Mapear modo TMA a códigos internos
            mode_label = tma_mode_dd.value or "Resolvidos/Fechados (útil)"
            mode_map = {
                "Resolvidos/Fechados (útil)": "resolved_closed",
                "Lifetime (útil) - todos": "lifetime_all",
                "Decorrido (calendário)": "elapsed_all",
            }
            tma_mode_code = mode_map.get(mode_label, "resolved_closed")

            try:
                print("[FETCH] Iniciando busca...", flush=True)
                print(f"[FETCH] Periodo: ge={ge_iso} le={le_iso}", flush=True)
                print(f"[FETCH] Expand: {expand}", flush=True)
                print(f"[FETCH] Extra filters: {extra}", flush=True)
                print(f"[FETCH] Page size: {top_tf.value} | Max pages: {max_pages_tf.value}", flush=True)
                print(f"[FETCH] TMA mode: {tma_mode_code} ({mode_label})", flush=True)
            except Exception:
                pass

            raw = fetch_tickets_union(
                token=token,
                created_ge_iso=ge_iso,
                created_le_iso=le_iso,
                select=select,
                expand=expand,
                extra_filters=extra,
                page_size=int(top_tf.value or "200"),
                max_pages=int(max_pages_tf.value or "5"),
            )
            try:
                print(f"[FETCH] Registros retornados (bruto): {len(raw)}", flush=True)
            except Exception:
                pass
            df = normalize_tickets(raw)
            try:
                print(f"[FETCH] Linhas normalizadas: {len(df)} | Colunas: {len(df.columns)}", flush=True)
            except Exception:
                pass

            if df.empty:
                page.splash = None
                page.snack_bar = ft.SnackBar(
                    ft.Text("Nenhum ticket encontrado com os filtros atuais. Amplie o período e limpe status/urgência/categoria/cliente."),
                    bgcolor=ft.Colors.AMBER_700,
                )
                page.snack_bar.open = True
                page.update()
                try:
                    print("[FETCH] Nenhum ticket encontrado.", flush=True)
                except Exception:
                    pass
                return

            m = derive_metrics(df, tma_mode=tma_mode_code)
            kpi_total.content.controls[1].value = f"{m['total']}"
            kpi_em_at.content.controls[1].value = f"{m['em_at']}"
            kpi_resolv.content.controls[1].value = f"{m['resolvidos']}"
            kpi_fech.content.controls[1].value = f"{m['fechados']}"
            kpi_tma.content.controls[1].value = f"{m['tma_h']}"
            # Atualizar subtítulo do KPI TMA conforme seleção
            tma_subtitle_map = {
                "resolved_closed": "útil • apenas Resolvidos/Fechados",
                "lifetime_all": "útil • lifetime (todos)",
                "elapsed_all": "calendário • (lastUpdate - createdDate)",
            }
            kpi_tma.content.controls[2].value = tma_subtitle_map.get(tma_mode_code, "tempo médio útil")
            kpi_first.content.controls[1].value = f"{m['first_resp']}%"

            # Datas auxiliares
            df["dt_open"] = df["createdDate"].dt.tz_convert("UTC").dt.date
            df["dt_last"] = df["lastUpdate"].dt.tz_convert("UTC").dt.date

            # 1) Abertos vs Encerrados por dia (Encerrados ~ baseStatus Resolved/Closed por lastUpdate)
            opened = df.groupby("dt_open")["id"].count().rename("Abertos")
            closed = df[df["baseStatus"].isin(["Resolved","Closed"])].groupby("dt_last")["id"].count().rename("Encerrados")
            # Guardar contra NaT em limites
            start_dates = [d for d in [df["dt_open"].min(), df["dt_last"].min()] if pd.notna(d)]
            end_dates = [d for d in [df["dt_open"].max(), df["dt_last"].max()] if pd.notna(d)]
            if start_dates and end_dates:
                idx = pd.date_range(min(start_dates), max(end_dates))
            else:
                idx = pd.date_range(df["dt_open"].min(), df["dt_open"].max())
            oc = pd.concat([opened.reindex(idx, fill_value=0), closed.reindex(idx, fill_value=0)], axis=1).reset_index()
            oc.columns = ["Data","Abertos","Encerrados"]
            try:
                fig_oc = go.Figure()
                fig_oc.add_trace(go.Scatter(x=oc["Data"], y=oc["Abertos"], name="Abertos", mode="lines+markers"))
                fig_oc.add_trace(go.Scatter(x=oc["Data"], y=oc["Encerrados"], name="Encerrados", mode="lines+markers"))
                fig_oc.update_layout(template="plotly_dark", title="Abertos vs Encerrados (aprox.)")
                chart_open_close.figure = fig_oc
            except Exception as _ex:
                print(f"[CHART][open_close] erro: {_ex}", flush=True)

            # 2) Backlog ao longo do tempo
            try:
                oc_cum = oc.copy()
                oc_cum["Backlog"] = oc_cum["Abertos"].cumsum() - oc_cum["Encerrados"].cumsum()
                chart_backlog.figure = px.area(oc_cum, x="Data", y="Backlog", title="Backlog ao longo do tempo", template="plotly_dark")
            except Exception as _ex:
                print(f"[CHART][backlog] erro: {_ex}", flush=True)

            # 3) Status mix por dia (aprox. usando dt_open)
            try:
                mix = df.groupby(["dt_open","baseStatus"])['id'].count().reset_index()
                mix_pivot = mix.pivot(index="dt_open", columns="baseStatus", values="id").fillna(0)
                mix_pivot = mix_pivot.reset_index().rename(columns={"dt_open":"Data"})
                chart_status_mix.figure = px.area(mix_pivot, x="Data", y=[c for c in mix_pivot.columns if c != "Data"], title="Status mix por dia (aprox.)", template="plotly_dark")
            except Exception as _ex:
                print(f"[CHART][status_mix] erro: {_ex}", flush=True)

            # Helper: TMA por ticket conforme modo
            def compute_ticket_tma_hours(_df: pd.DataFrame, mode: str) -> pd.Series:
                if mode == "elapsed_all":
                    return pd.to_numeric(_df.get("duration_h"), errors="coerce")
                lif = pd.to_numeric(_df.get("lifetimeWorkingTime"), errors="coerce") / 60.0
                lif = lif.where(lif > 0)
                elapsed = pd.to_numeric(_df.get("duration_h"), errors="coerce")
                if mode == "lifetime_all":
                    return lif.fillna(elapsed)
                # resolved_closed
                base = _df["baseStatus"].isin(["Resolved","Closed"]).astype(bool)
                out = pd.Series(np.nan, index=_df.index, dtype=float)
                out[base] = lif[base].fillna(elapsed[base])
                return out

            tma_per_ticket = compute_ticket_tma_hours(df, tma_mode_code)

            # 4) TMA por Equipe, Categoria, Cliente
            tma_team = df.assign(tma_h=tma_per_ticket).assign(ownerTeam=df["ownerTeam"].fillna("—").astype(str)).groupby("ownerTeam", dropna=False)["tma_h"].mean().reset_index()
            tma_team = tma_team.sort_values("tma_h", ascending=False).head(20)
            try:
                chart_tma_team.figure = px.bar(tma_team, x="tma_h", y="ownerTeam", orientation="h", title="TMA por Equipe (h)", template="plotly_dark")
            except Exception as _ex:
                print(f"[CHART][tma_team] erro: {_ex}", flush=True)

            tma_cat = df.assign(tma_h=tma_per_ticket).assign(category=df["category"].fillna("—").astype(str)).groupby("category", dropna=False)["tma_h"].mean().reset_index()
            tma_cat = tma_cat.sort_values("tma_h", ascending=False).head(20)
            try:
                chart_tma_cat.figure = px.bar(tma_cat, x="tma_h", y="category", orientation="h", title="TMA por Categoria (h)", template="plotly_dark")
            except Exception as _ex:
                print(f"[CHART][tma_cat] erro: {_ex}", flush=True)

            tma_cli = df.assign(tma_h=tma_per_ticket).assign(clientPrimaryName=df["clientPrimaryName"].fillna("—").astype(str)).groupby("clientPrimaryName", dropna=False)["tma_h"].mean().reset_index()
            tma_cli = tma_cli.sort_values("tma_h", ascending=False).head(20)
            try:
                chart_tma_client.figure = px.bar(tma_cli, x="tma_h", y="clientPrimaryName", orientation="h", title="TMA por Cliente (h)", template="plotly_dark")
            except Exception as _ex:
                print(f"[CHART][tma_client] erro: {_ex}", flush=True)

            # 5) SLA Resposta e Solução (mês)
            dfx = df.copy()
            dfx["mes"] = dfx["createdDate"].dt.to_period("M").astype(str)
            # Resposta
            has_resp_target = ~dfx["slaResponseDate"].isna()
            within_resp = has_resp_target & (~dfx["slaRealResponseDate"].isna()) & (dfx["slaRealResponseDate"] <= dfx["slaResponseDate"])
            # FIX: evitar Named Aggregation com Series — usar colunas 0/1 e somar
            dfx["_Dentro_resp"] = within_resp.astype(int)
            dfx["_Fora_resp"]   = (has_resp_target & ~within_resp).astype(int)
            sla_resp = (
                dfx.groupby("mes", as_index=False)[["_Dentro_resp", "_Fora_resp"]]
                   .sum()
                   .rename(columns={"_Dentro_resp": "Dentro", "_Fora_resp": "Fora"})
            )
            try:
                chart_sla_resp.figure = px.bar(sla_resp, x="mes", y=["Dentro", "Fora"], barmode="stack",
                    title="SLA Resposta: Dentro x Fora (mês)", template="plotly_dark")
            except Exception as _ex:
                print(f"[CHART][sla_resp] erro: {_ex}", flush=True)
            # Solução
            solved = dfx["baseStatus"].isin(["Resolved","Closed"]) & (~dfx["slaSolutionDate"].isna())
            within_sol = solved & (dfx["lastUpdate"] <= dfx["slaSolutionDate"])
            # FIX: evitar Named Aggregation com Series — usar colunas 0/1 e somar
            dfx["_Dentro_sol"] = within_sol.astype(int)
            dfx["_Fora_sol"]   = (solved & ~within_sol).astype(int)
            sla_sol = (
                dfx.groupby("mes", as_index=False)[["_Dentro_sol", "_Fora_sol"]]
                   .sum()
                   .rename(columns={"_Dentro_sol": "Dentro", "_Fora_sol": "Fora"})
            )
            try:
                chart_sla_sol.figure = px.bar(sla_sol, x="mes", y=["Dentro","Fora"], barmode="stack", title="SLA Solução: Dentro x Fora (mês)", template="plotly_dark")
            except Exception as _ex:
                print(f"[CHART][sla_sol] erro: {_ex}", flush=True)

            # 6) Tempo até 1ª resposta — p50/p95 (h)
            dfr = df[~df["slaRealResponseDate"].isna()].copy()
            if not dfr.empty:
                dfr["resp_h"] = (dfr["slaRealResponseDate"] - dfr["createdDate"]).dt.total_seconds() / 3600.0
                dfr["dia"] = dfr["createdDate"].dt.date
                agg = dfr.groupby("dia")["resp_h"].agg(p50=lambda x: np.nanpercentile(x, 50), p95=lambda x: np.nanpercentile(x, 95)).reset_index()
                try:
                    fig_resp = go.Figure()
                    fig_resp.add_trace(go.Scatter(x=agg["dia"], y=agg["p50"], name="p50", mode="lines+markers"))
                    fig_resp.add_trace(go.Scatter(x=agg["dia"], y=agg["p95"], name="p95", mode="lines+markers"))
                    fig_resp.update_layout(template="plotly_dark", title="Tempo até 1ª resposta — p50 e p95 (h)")
                    chart_first_resp_trend.figure = fig_resp
                except Exception as _ex:
                    print(f"[CHART][first_resp_trend] erro: {_ex}", flush=True)

            # 7) Distribuições
            lif_h = pd.to_numeric(df.get("lifetimeWorkingTime"), errors="coerce") / 60.0
            try:
                chart_hist_lifetime.figure = px.histogram(lif_h.dropna(), nbins=40, title="Distribuição — Tempo útil (h)", template="plotly_dark")
            except Exception as _ex:
                print(f"[CHART][hist_lifetime] erro: {_ex}", flush=True)
            stp_h = pd.to_numeric(df.get("stoppedTimeWorkingTime"), errors="coerce") / 60.0
            try:
                chart_hist_stopped.figure = px.histogram(stp_h.dropna(), nbins=40, title="Distribuição — Tempo parado (h)", template="plotly_dark")
            except Exception as _ex:
                print(f"[CHART][hist_stopped] erro: {_ex}", flush=True)

            # 8) Heatmap por responsável x dia
            ow = df.assign(ownerName=df["ownerName"].fillna("—").astype(str)).groupby(["dt_open","ownerName"], dropna=False)["id"].count().reset_index()
            top_owners = ow.groupby("ownerName")["id"].sum().sort_values(ascending=False).head(12).index
            ow = ow[ow["ownerName"].isin(top_owners)]
            if not ow.empty:
                try:
                    pv = ow.pivot(index="ownerName", columns="dt_open", values="id").fillna(0)
                    chart_heat_owner_day.figure = px.imshow(pv.values, labels=dict(x="Dia", y="Responsável", color="Tickets"), x=pv.columns, y=pv.index, aspect="auto", title="Heatmap — Volume por Responsável x Dia", template="plotly_dark")
                except Exception as _ex:
                    print(f"[CHART][heat_owner_day] erro: {_ex}", flush=True)

            # 9) Ações por ticket (hist)
            if "actionCount" in df.columns:
                try:
                    chart_hist_actions.figure = px.histogram(df, x="actionCount", nbins=40, title="Ações por ticket (histograma)", template="plotly_dark")
                except Exception as _ex:
                    print(f"[CHART][hist_actions] erro: {_ex}", flush=True)

            # 10) Tempo apontado por responsável (requer expand de actions/timeAppointments)
            work_by_owner = {}
            if expand_actions_cb.value:
                try:
                    for item in raw:
                        acts = item.get("actions") or []
                        for a in acts:
                            tapps = a.get("timeAppointments") or []
                            for t in tapps:
                                wt = t.get("workTime")
                                crb = t.get("createdBy") or {}
                                name = crb.get("businessName") or crb.get("id") or "—"
                                if wt is not None:
                                    work_by_owner[name] = work_by_owner.get(name, 0) + float(wt)
                    if work_by_owner:
                        try:
                            wdf = pd.DataFrame([{"Responsável": k, "Minutos": v} for k, v in work_by_owner.items()]).sort_values("Minutos", ascending=False).head(20)
                            wdf["Horas"] = wdf["Minutos"] / 60.0
                            chart_worktime_owner.figure = px.bar(wdf, x="Horas", y="Responsável", orientation="h", title="Tempo apontado por responsável (h)", template="plotly_dark")
                        except Exception as _ex:
                            print(f"[CHART][worktime_owner] erro: {_ex}", flush=True)
                except Exception:
                    pass

            # 11) Pareto de clientes (volume)
            cli_counts = df["clientPrimaryName"].fillna("—").value_counts().reset_index()
            cli_counts.columns = ["Cliente","Qtd"]
            cli_counts["%Acum"] = (cli_counts["Qtd"].cumsum() / cli_counts["Qtd"].sum() * 100.0)
            try:
                fig_pareto = go.Figure()
                fig_pareto.add_trace(go.Bar(x=cli_counts["Cliente"].head(20), y=cli_counts["Qtd"].head(20), name="Volume"))
                fig_pareto.add_trace(go.Scatter(x=cli_counts["Cliente"].head(20), y=cli_counts["%Acum"].head(20), name="% Acumulado", yaxis="y2"))
                fig_pareto.update_layout(template="plotly_dark", title="Pareto de Clientes (volume)", yaxis2=dict(overlaying='y', side='right', title='%'), xaxis_tickangle=-30)
                chart_pareto_clients.figure = fig_pareto
            except Exception as _ex:
                print(f"[CHART][pareto_clients] erro: {_ex}", flush=True)

            # 12) Heatmap Categoria x Urgência
            cu = df.assign(category=df["category"].fillna("—").astype(str), urgency=df["urgency"].fillna("—").astype(str)).groupby(["category","urgency"])["id"].count().reset_index()
            if not cu.empty:
                try:
                    pv = cu.pivot(index="category", columns="urgency", values="id").fillna(0)
                    chart_heat_cat_urg.figure = px.imshow(pv.values, labels=dict(x="Urgência", y="Categoria", color="Tickets"), x=pv.columns, y=pv.index, aspect="auto", title="Heatmap — Categoria x Urgência", template="plotly_dark")
                except Exception as _ex:
                    print(f"[CHART][heat_cat_urg] erro: {_ex}", flush=True)

            # 13) Origem por mês (de actions.origin)
            if expand_actions_cb.value:
                try:
                    rows = []
                    for item in raw:
                        acts = item.get("actions") or []
                        for a in acts:
                            org = a.get("origin") or "—"
                            cdate = a.get("createdDate")
                            if cdate:
                                dt = pd.to_datetime(cdate, errors="coerce", utc=True)
                                if not pd.isna(dt):
                                    rows.append((dt.to_period("M").strftime("%Y-%m"), org))
                    if rows:
                        a_df = pd.DataFrame(rows, columns=["mes","origem"]) 
                        try:
                            org_pv = a_df.value_counts(["mes","origem"]).reset_index(name="Qtd")
                            top_orgs = org_pv.groupby("origem")["Qtd"].sum().sort_values(ascending=False).head(6).index
                            org_pv = org_pv[org_pv["origem"].isin(top_orgs)]
                            chart_origin_month.figure = px.line(org_pv, x="mes", y="Qtd", color="origem", markers=True, title="Origem do ticket (ações) por mês", template="plotly_dark")
                        except Exception as _ex:
                            print(f"[CHART][origin_month] erro: {_ex}", flush=True)
                except Exception:
                    pass

            # 14) Heatmap criação DOW x hora
            dwh = df.copy()
            dwh["dow"] = dwh["createdDate"].dt.day_name()
            dwh["hora"] = dwh["createdDate"].dt.hour
            pv = dwh.groupby(["dow","hora"])["id"].count().reset_index()
            if not pv.empty:
                pvt = pv.pivot(index="dow", columns="hora", values="id").fillna(0)
                # Ordenar dias
                order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
                pvt = pvt.reindex(order)
                try:
                    chart_heat_dow_hour.figure = px.imshow(pvt.values, labels=dict(x="Hora", y="Dia da semana", color="Aberturas"), x=pvt.columns, y=pvt.index, aspect="auto", title="Heatmap — Criações por Dia x Hora", template="plotly_dark")
                except Exception as _ex:
                    print(f"[CHART][heat_dow_hour] erro: {_ex}", flush=True)

            # 15) Sazonalidade mensal (aberturas)
            month_open = df[~df["createdDate"].isna()].groupby(df["createdDate"].dt.to_period("M").astype(str))["id"].count().reset_index()
            month_open.columns = ["Mês","Aberturas"]
            try:
                chart_monthly_season.figure = px.bar(month_open, x="Mês", y="Aberturas", title="Sazonalidade mensal (aberturas)", template="plotly_dark")
            except Exception as _ex:
                print(f"[CHART][monthly_season] erro: {_ex}", flush=True)

            # 16) Outliers Top 20 — TMA e Parado
            top_tma = df.assign(tma_h=tma_per_ticket).sort_values("tma_h", ascending=False).head(20)
            try:
                chart_top_tma.figure = px.bar(top_tma, x="tma_h", y="id", orientation="h", title="Top 20 TMA por ticket (h)", template="plotly_dark")
            except Exception as _ex:
                print(f"[CHART][top_tma] erro: {_ex}", flush=True)
            stp_h2 = pd.to_numeric(df.get("stoppedTimeWorkingTime"), errors="coerce") / 60.0
            top_stp = df.assign(stopped_h=stp_h2).sort_values("stopped_h", ascending=False).head(20)
            try:
                chart_top_stopped.figure = px.bar(top_stp, x="stopped_h", y="id", orientation="h", title="Top 20 Tempo parado por ticket (h)", template="plotly_dark")
            except Exception as _ex:
                print(f"[CHART][top_stopped] erro: {_ex}", flush=True)

            cols_show = [
                "id","protocol","subject","category","urgency","status","baseStatus",
                "ownerName","ownerTeam","createdDate"
            ]
            rows = []
            for _, r in df[cols_show].fillna("—").sort_values("createdDate", ascending=False).iterrows():
                rows.append(
                    ft.DataRow(
                        cells=[
                            ft.DataCell(ft.Text(str(r["id"]))),
                            ft.DataCell(ft.Text(str(r["protocol"]))),
                            ft.DataCell(ft.Text(str(r["subject"]))),
                            ft.DataCell(ft.Text(str(r["category"]))),
                            ft.DataCell(ft.Text(str(r["urgency"]))),
                            ft.DataCell(ft.Text(str(r["status"]))),
                            ft.DataCell(ft.Text(str(r["baseStatus"]))),
                            ft.DataCell(ft.Text(str(r["ownerName"]))),
                            ft.DataCell(ft.Text(str(r["ownerTeam"]))),
                            ft.DataCell(ft.Text(str(r["createdDate"]))),
                        ]
                    )
                )
            table.rows = rows

            page.splash = None
            page.update()
            try:
                print("[FETCH] KPIs e gráficos atualizados com sucesso.", flush=True)
            except Exception:
                pass

        except Exception as ex:
            page.splash = None
            tb = traceback.format_exc()
            page.snack_bar = ft.SnackBar(ft.Text(f"Erro: {ex}"), bgcolor=ft.Colors.RED_700, duration=8000)
            page.snack_bar.open = True
            page.update()
            try:
                print(f"[FETCH][ERRO] {ex}", flush=True)
                print(tb, flush=True)
            except Exception:
                pass

    fetch_btn.on_click = on_fetch
    ping_btn.on_click = on_ping

    def box(c):
        return ft.Container(content=c, bgcolor=_with_opacity(0.06, ft.Colors.WHITE), padding=16, border_radius=16)

    filters = box(ft.Column(
        [
            ft.Text("Filtros & Configuração", size=14, weight=ft.FontWeight.BOLD),
            token_field,
            ft.Row([dt_ini, dt_fim], spacing=12),
            ft.Row([status_tf, urg_tf, cat_tf], spacing=12, wrap=True),
            cli_tf,
            ft.Row([expand_clients_cb, expand_actions_cb], spacing=20),
            ft.Row([tma_mode_dd], spacing=12),
            ft.Row([top_tf, max_pages_tf], spacing=12),
            fetch_btn,
            ping_btn,
        ],
        spacing=10
    ))

    kpis = ft.Row(
        controls=[kpi_total, kpi_em_at, kpi_resolv, kpi_fech, kpi_tma, kpi_first],
        alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
        wrap=True,
    )

    # Seções de gráficos
    section1 = ft.Column([
        ft.Text("Volume e Fluxo", size=14, weight=ft.FontWeight.BOLD),
        ft.Row([ft.Container(chart_open_close, expand=True), ft.Container(chart_backlog, expand=True)], spacing=16),
        ft.Container(chart_status_mix, expand=True),
    ], spacing=10)

    section2 = ft.Column([
        ft.Text("Performance e SLA", size=14, weight=ft.FontWeight.BOLD),
        ft.Row([ft.Container(chart_tma_team, expand=True), ft.Container(chart_tma_cat, expand=True)], spacing=16),
        ft.Container(chart_tma_client, expand=True),
        ft.Row([ft.Container(chart_sla_resp, expand=True), ft.Container(chart_sla_sol, expand=True)], spacing=16),
        ft.Container(chart_first_resp_trend, expand=True),
    ], spacing=10)

    section3 = ft.Column([
        ft.Text("Produtividade e Carga", size=14, weight=ft.FontWeight.BOLD),
        ft.Row([ft.Container(chart_hist_actions, expand=True), ft.Container(chart_worktime_owner, expand=True)], spacing=16),
        ft.Container(chart_heat_owner_day, expand=True),
    ], spacing=10)

    section4 = ft.Column([
        ft.Text("Segmentação e Mix", size=14, weight=ft.FontWeight.BOLD),
        ft.Row([ft.Container(chart_pareto_clients, expand=True), ft.Container(chart_heat_cat_urg, expand=True)], spacing=16),
        ft.Container(chart_origin_month, expand=True),
    ], spacing=10)

    section5 = ft.Column([
        ft.Text("Padrões temporais", size=14, weight=ft.FontWeight.BOLD),
        ft.Row([ft.Container(chart_heat_dow_hour, expand=True), ft.Container(chart_monthly_season, expand=True)], spacing=16),
    ], spacing=10)

    section6 = ft.Column([
        ft.Text("Outliers e Alertas", size=14, weight=ft.FontWeight.BOLD),
        ft.Row([ft.Container(chart_top_tma, expand=True), ft.Container(chart_top_stopped, expand=True)], spacing=16),
    ], spacing=10)

    table_box = box(ft.Column([ft.Text("Detalhes dos Tickets", size=14, weight=ft.FontWeight.BOLD), table], spacing=8))

    page.add(
        ft.Column(
            [
                title, subtitle,
                ft.Divider(height=12, color=_with_opacity(0.1, ft.Colors.WHITE)),
                filters,
                ft.Divider(height=16, color=_with_opacity(0.05, ft.Colors.WHITE)),
                kpis,
                section1,
                section2,
                section3,
                section4,
                section5,
                section6,
                ft.Divider(height=16, color=_with_opacity(0.05, ft.Colors.WHITE)),
                table_box,
                ft.Text("Obs.: Dados >90 dias de lastUpdate são obtidos também em /tickets/past.", size=11, color=ft.Colors.GREY_500),
            ],
            spacing=14,
            scroll=ft.ScrollMode.AUTO,
            expand=True,
        )
    )

ft.app(target=main)