# -*- coding: utf-8 -*-
"""
PG Monitor UI (Flet + Psycopg3)
- Interface gráfica para monitorar PostgreSQL em tempo real.
- KPIs, abas (Conexões, Locks, Long Queries, Replicação, Tabelas, Bancos, Bloat).
- Start/Stop com intervalo configurável e salvamento de config.yaml.

Compatibilidade Windows (Python 3.8+ / 3.13):
- O Flet Desktop usa subprocessos e trabalha bem com o loop padrão (Proactor) do Windows.
- O psycopg async, porém, NÃO é compatível com Proactor.
- Para evitar conflito, este app usa coleta **SÍNCRONA** em thread (`asyncio.to_thread`) no Windows.
  Em Linux/macOS, tenta a coleta assíncrona nativa; se falhar, cai no mesmo fallback síncrono.
"""

import asyncio
import socket
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import flet as ft

# ---------- Compatibilidade ft.Colors/ft.Icons & util ----------
try:
    C = ft.Colors  # novo
except Exception:
    C = ft.colors  # legado

try:
    I = ft.Icons  # novo
except Exception:
    I = ft.icons  # legado

def op(alpha: float, color: str) -> str:
    """with_opacity compatível com versões antigas do Flet."""
    try:
        return ft.colors.with_opacity(alpha, color)
    except Exception:
        try:
            return ft.Colors.with_opacity(alpha, color)  # type: ignore
        except Exception:
            return color

def brazil_now():
    return datetime.now(timezone(timedelta(hours=-3)))

def pretty_bytes(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    f = float(n or 0)
    i = 0
    while f >= 1024 and i < len(units) - 1:
        f /= 1024.0
        i += 1
    return f"{f:.2f} {units[i]}"

def pct(part: float, total: float) -> float:
    if not total:
        return 0.0
    return round(100.0 * float(part) / float(total), 2)

# ---------- Psycopg / YAML ----------
try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:
    psycopg = None  # deixamos mensagem na UI

try:
    import yaml
except Exception:
    yaml = None

DEFAULT_CFG = {
    "dsn": "postgresql://postgres:postgres@localhost:5432/postgres",
    "interval_seconds": 30,
    "top_n": 10,
    "alerts": {
        "connections_pct_warn": 80,
        "blocked_locks_warn": 5,
        "long_running_query_sec_warn": 120,
        "replication_lag_sec_warn": 60,
        "dead_tuples_pct_warn": 20,
        "table_size_bytes_warn": 500_000_000
    }
}

def load_config(path: str) -> Dict[str, Any]:
    if yaml is None:
        return dict(DEFAULT_CFG)
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        cfg = dict(DEFAULT_CFG)
        cfg.update(data or {})
        if "alerts" in data and isinstance(data["alerts"], dict):
            cfg["alerts"].update(data["alerts"] or {})
        return cfg
    except FileNotFoundError:
        return dict(DEFAULT_CFG)

def save_config(path: str, data: Dict[str, Any]):
    if yaml is None:
        return
    try:
        with open(path, "w", encoding="utf-8") as f:
            yaml.safe_dump(data, f, sort_keys=False, allow_unicode=True)
    except Exception:
        pass

# --------- Helpers para "Testar conexão" (DNS e Porta) ----------
import urllib.parse, shlex, socket as _socket

def parse_host_port_from_dsn(dsn: str):
    """
    Extrai host e porta tanto do DSN em formato URL quanto libpq (key=value).
    """
    dsn = (dsn or "").strip().strip('"').strip("'")
    # URL style
    if "://" in dsn:
        u = urllib.parse.urlparse(dsn)
        host = u.hostname
        port = u.port or 5432
        return host, int(port)
    # libpq style: host=... port=... dbname=... user=... password=...
    kv = {}
    try:
        for tok in shlex.split(dsn):
            if "=" in tok:
                k, v = tok.split("=", 1)
                kv[k.strip()] = v.strip()
    except Exception:
        pass
    host = kv.get("host") or "localhost"
    port = int(kv.get("port") or 5432)
    return host, port

async def quick_socket_tests(host: str, port: int, timeout: float = 3.0) -> List[str]:
    """
    Testes de DNS + TCP connect em thread para não travar a UI.
    Retorna lista de mensagens humanas.
    """
    msgs: List[str] = []
    try:
        infos = await asyncio.to_thread(_socket.getaddrinfo, host, port, 0, _socket.SOCK_STREAM)
        fams = ", ".join(sorted({str(i[0]) for i in infos}))
        msgs.append(f"DNS OK: {host} resolve para {len(infos)} endereços (famílias: {fams}).")
    except _socket.gaierror as ex:
        msgs.append(f"DNS FALHOU para '{host}': {ex}. Verifique hostname ou use IP.")
        return msgs

    try:
        def _try_conn():
            s = _socket.create_connection((host, port), timeout=timeout)
            s.close()
        await asyncio.to_thread(_try_conn)
        msgs.append(f"Conexão TCP OK em {host}:{port} (porta aberta).")
    except ConnectionRefusedError:
        msgs.append(f"Conexão recusada em {host}:{port} (servidor respondeu, mas serviço indisponível).")
    except TimeoutError:
        msgs.append(f"Timeout conectando em {host}:{port} (rota/firewall).")
    except OSError as ex:
        msgs.append(f"Falha de socket para {host}:{port}: {ex}")
    return msgs

# --------- Coleta SÍNCRONA (compatível com qualquer loop) ----------
def collect_metrics_sync(dsn: str, top_n: int) -> Dict[str, Any]:
    if psycopg is None:
        raise RuntimeError("psycopg3 não está instalado. Instale com: pip install 'psycopg[binary]'")
    metrics: Dict[str, Any] = {"collected_at": brazil_now().isoformat(), "hostname": socket.gethostname()}
    with psycopg.connect(dsn, autocommit=True) as conn:
        def one(sql: str, args: Optional[tuple]=None):
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(sql, args or ())
                return cur.fetchone()
        def all(sql: str, args: Optional[tuple]=None):
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(sql, args or ())
                return cur.fetchall()

        row = one("""
            SELECT version(),
                   inet_server_addr()::text AS server_addr,
                   inet_server_port() AS server_port,
                   pg_postmaster_start_time() AS start_time
        """)
        if row:
            metrics["version"] = row["version"]
            metrics["server_addr"] = row["server_addr"]
            metrics["server_port"] = row["server_port"]
            metrics["postmaster_start_time"] = row["start_time"].isoformat() if row["start_time"] else None

        row = one("SELECT current_setting('max_connections')::int AS max_conn")
        max_conn = row["max_conn"] if row else 100
        row = one("SELECT count(*)::int AS used FROM pg_stat_activity")
        used_conn = row["used"] if row else 0

        metrics["connections"] = {
            "used": used_conn,
            "max": max_conn,
            "pct": pct(used_conn, max_conn),
            "by_state": all("""
                SELECT coalesce(state, 'unknown') AS state, count(*) AS qty
                FROM pg_stat_activity
                GROUP BY state
                ORDER BY qty DESC
            """),
            "by_db_top": all("""
                SELECT datname AS database, count(*) AS connections
                FROM pg_stat_activity
                GROUP BY datname
                ORDER BY connections DESC
                LIMIT %s
            """, (top_n,))
        }

        lrq = all("""
            SELECT pid, usename, datname, state, now() - query_start AS runtime, left(query, 500) AS query
            FROM pg_stat_activity
            WHERE state = 'active' AND query_start IS NOT NULL
            ORDER BY runtime DESC
            LIMIT %s
        """, (top_n,))
        for r in lrq:
            td = r["runtime"]
            r["runtime_sec"] = int(td.total_seconds()) if td else None
            r["runtime"] = str(td)
        metrics["long_running_queries_top"] = lrq

        blocked = all("""
            SELECT a.pid, a.usename, a.datname,
                   now() - a.query_start AS blocked_for,
                   left(a.query, 400) AS query,
                   cardinality(pg_blocking_pids(a.pid)) AS blockers,
                   pg_blocking_pids(a.pid) AS blocking_pids
            FROM pg_stat_activity a
            WHERE cardinality(pg_blocking_pids(a.pid)) > 0
            ORDER BY blocked_for DESC
            LIMIT %s
        """, (top_n,))
        for r in blocked:
            td = r["blocked_for"]
            r["blocked_for_sec"] = int(td.total_seconds()) if td else None
            r["blocked_for"] = str(td)
        metrics["blocked_processes"] = blocked
        metrics["blocked_locks_count"] = len(blocked)

        repl = all("""
            SELECT application_name, state, sync_state,
                   write_lag, flush_lag, replay_lag,
                   client_addr::text, sent_lsn, write_lsn, flush_lsn, replay_lag, replay_lsn
            FROM pg_stat_replication
            ORDER BY application_name
        """)
        def interval_to_sec(v):
            if v is None: return None
            try: return int(v.total_seconds())
            except Exception: return None
        for r in repl:
            r["write_lag_sec"]  = interval_to_sec(r["write_lag"])
            r["flush_lag_sec"]  = interval_to_sec(r["flush_lag"])
            r["replay_lag_sec"] = interval_to_sec(r["replay_lag"])
            r["write_lag"] = str(r["write_lag"]) if r["write_lag"] is not None else None
            r["flush_lag"] = str(r["flush_lag"]) if r["flush_lag"] is not None else None
            r["replay_lag"] = str(r["replay_lag"]) if r["replay_lag"] is not None else None
        metrics["replication"] = repl

        row = one("SELECT pg_is_in_recovery() AS in_recovery")
        metrics["in_recovery"] = row["in_recovery"] if row else False
        if metrics["in_recovery"]:
            s = one("""
                SELECT now() - pg_last_xact_replay_timestamp() AS replay_delay,
                       pg_last_wal_receive_lsn() AS receive_lsn,
                       pg_last_wal_replay_lsn() AS replay_lsn
            """)
            if s:
                s["replay_delay_sec"] = int(s["replay_delay"].total_seconds()) if s["replay_delay"] else None
                s["replay_delay"] = str(s["replay_delay"]) if s["replay_delay"] is not None else None
            metrics["standby"] = s

        bloat = all("""
            SELECT schemaname, relname, n_live_tup, n_dead_tup,
                   ROUND(100.0 * n_dead_tup / GREATEST(n_live_tup + n_dead_tup, 1), 2) AS dead_pct
            FROM pg_stat_user_tables
            ORDER BY dead_pct DESC
            LIMIT %s
        """, (top_n,))
        metrics["table_bloat_top"] = bloat

        biggest = all("""
            SELECT n.nspname AS schema, c.relname AS table,
                   pg_total_relation_size(c.oid) AS total_bytes
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'r'
            ORDER BY total_bytes DESC
            LIMIT %s
        """, (top_n,))
        for r in biggest:
            r["total_pretty"] = pretty_bytes(r["total_bytes"])
        metrics["biggest_tables"] = biggest

        dbsizes = all("""
            SELECT datname AS database, pg_database_size(datname) AS bytes
            FROM pg_database
            ORDER BY bytes DESC
        """)
        for r in dbsizes:
            r["pretty"] = pretty_bytes(r["bytes"])
        metrics["database_sizes"] = dbsizes

        bg = one("""
            SELECT checkpoints_timed, checkpoints_req, buffers_checkpoint,
                   buffers_clean, maxwritten_clean, buffers_backend, buffers_alloc
            FROM pg_stat_bgwriter
        """)
        metrics["bgwriter"] = bg or {}

    return metrics

# --------- Coleta ASSÍNCRONA (Linux/macOS) ----------
async def _collect_metrics_async(dsn: str, top_n: int) -> Dict[str, Any]:
    if psycopg is None:
        raise RuntimeError("psycopg3 não está instalado. Instale com: pip install 'psycopg[binary]'")
    metrics: Dict[str, Any] = {"collected_at": brazil_now().isoformat(), "hostname": socket.gethostname()}
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
        async def one(sql: str, args: Optional[tuple]=None):
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(sql, args or ())
                return await cur.fetchone()
        async def all(sql: str, args: Optional[tuple]=None):
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(sql, args or ())
                return await cur.fetchall()

        row = await one("""
            SELECT version(),
                   inet_server_addr()::text AS server_addr,
                   inet_server_port() AS server_port,
                   pg_postmaster_start_time() AS start_time
        """)
        if row:
            metrics["version"] = row["version"]
            metrics["server_addr"] = row["server_addr"]
            metrics["server_port"] = row["server_port"]
            metrics["postmaster_start_time"] = row["start_time"].isoformat() if row["start_time"] else None

        row = await one("SELECT current_setting('max_connections')::int AS max_conn")
        max_conn = row["max_conn"] if row else 100
        row = await one("SELECT count(*)::int AS used FROM pg_stat_activity")
        used_conn = row["used"] if row else 0

        metrics["connections"] = {
            "used": used_conn,
            "max": max_conn,
            "pct": pct(used_conn, max_conn),
            "by_state": await all("""
                SELECT coalesce(state, 'unknown') AS state, count(*) AS qty
                FROM pg_stat_activity
                GROUP BY state
                ORDER BY qty DESC
            """),
            "by_db_top": await all("""
                SELECT datname AS database, count(*) AS connections
                FROM pg_stat_activity
                GROUP BY datname
                ORDER BY connections DESC
                LIMIT %s
            """, (top_n,))
        }

        lrq = await all("""
            SELECT pid, usename, datname, state, now() - query_start AS runtime, left(query, 500) AS query
            FROM pg_stat_activity
            WHERE state = 'active' AND query_start IS NOT NULL
            ORDER BY runtime DESC
            LIMIT %s
        """, (top_n,))
        for r in lrq:
            td = r["runtime"]
            r["runtime_sec"] = int(td.total_seconds()) if td else None
            r["runtime"] = str(td)
        metrics["long_running_queries_top"] = lrq

        blocked = await all("""
            SELECT a.pid, a.usename, a.datname,
                   now() - a.query_start AS blocked_for,
                   left(a.query, 400) AS query,
                   cardinality(pg_blocking_pids(a.pid)) AS blockers,
                   pg_blocking_pids(a.pid) AS blocking_pids
            FROM pg_stat_activity a
            WHERE cardinality(pg_blocking_pids(a.pid)) > 0
            ORDER BY blocked_for DESC
            LIMIT %s
        """, (top_n,))
        for r in blocked:
            td = r["blocked_for"]
            r["blocked_for_sec"] = int(td.total_seconds()) if td else None
            r["blocked_for"] = str(td)
        metrics["blocked_processes"] = blocked
        metrics["blocked_locks_count"] = len(blocked)

        repl = await all("""
            SELECT application_name, state, sync_state,
                   write_lag, flush_lag, replay_lag,
                   client_addr::text, sent_lsn, write_lsn, flush_lsn, replay_lag, replay_lsn
            FROM pg_stat_replication
            ORDER BY application_name
        """)
        def interval_to_sec(v):
            if v is None: return None
            try: return int(v.total_seconds())
            except Exception: return None
        for r in repl:
            r["write_lag_sec"]  = interval_to_sec(r["write_lag"])
            r["flush_lag_sec"]  = interval_to_sec(r["flush_lag"])
            r["replay_lag_sec"] = interval_to_sec(r["replay_lag"])
            r["write_lag"] = str(r["write_lag"]) if r["write_lag"] is not None else None
            r["flush_lag"] = str(r["flush_lag"]) if r["flush_lag"] is not None else None
            r["replay_lag"] = str(r["replay_lag"]) if r["replay_lag"] is not None else None
        metrics["replication"] = repl

        row = await one("SELECT pg_is_in_recovery() AS in_recovery")
        metrics["in_recovery"] = row["in_recovery"] if row else False
        if metrics["in_recovery"]:
            s = await one("""
                SELECT now() - pg_last_xact_replay_timestamp() AS replay_delay,
                       pg_last_wal_receive_lsn() AS receive_lsn,
                       pg_last_wal_replay_lsn() AS replay_lsn
            """)
            if s:
                s["replay_delay_sec"] = int(s["replay_delay"].total_seconds()) if s["replay_delay"] else None
                s["replay_delay"] = str(s["replay_delay"]) if s["replay_delay"] is not None else None
            metrics["standby"] = s

        bloat = await all("""
            SELECT schemaname, relname, n_live_tup, n_dead_tup,
                   ROUND(100.0 * n_dead_tup / GREATEST(n_live_tup + n_dead_tup, 1), 2) AS dead_pct
            FROM pg_stat_user_tables
            ORDER BY dead_pct DESC
            LIMIT %s
        """, (top_n,))
        metrics["table_bloat_top"] = bloat

        biggest = await all("""
            SELECT n.nspname AS schema, c.relname AS table,
                   pg_total_relation_size(c.oid) AS total_bytes
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'r'
            ORDER BY total_bytes DESC
            LIMIT %s
        """, (top_n,))
        for r in biggest:
            r["total_pretty"] = pretty_bytes(r["total_bytes"])
        metrics["biggest_tables"] = biggest

        dbsizes = await all("""
            SELECT datname AS database, pg_database_size(datname) AS bytes
            FROM pg_database
            ORDER BY bytes DESC
        """)
        for r in dbsizes:
            r["pretty"] = pretty_bytes(r["bytes"])
        metrics["database_sizes"] = dbsizes

        bg = await one("""
            SELECT checkpoints_timed, checkpoints_req, buffers_checkpoint,
                   buffers_clean, maxwritten_clean, buffers_backend, buffers_alloc
            FROM pg_stat_bgwriter
        """)
        metrics["bgwriter"] = bg or {}

    return metrics

# --------- Despachante que escolhe o modo de coleta ----------
async def collect_metrics_once(dsn: str, top_n: int) -> Dict[str, Any]:
    # No Windows, usa thread para evitar conflito Flet(Proactor) x psycopg async.
    try:
        import sys as _sys
        if _sys.platform.startswith("win"):
            return await asyncio.to_thread(collect_metrics_sync, dsn, top_n)
    except Exception:
        pass
    # Fora do Windows: tenta async; se falhar, cai no fallback em thread
    try:
        return await _collect_metrics_async(dsn, top_n)
    except Exception:
        return await asyncio.to_thread(collect_metrics_sync, dsn, top_n)

# ---------- UI Components ----------
def kpi_card(title: str, value: str, icon: str, color: str, tooltip: str="") -> ft.Container:
    return ft.Container(
        bgcolor=op(0.08, color),
        border_radius=16,
        padding=16,
        content=ft.Row([
            ft.Icon(icon, size=32, color=color),
            ft.Column([
                ft.Text(title, size=12, color=op(0.8, color)),
                ft.Text(value, size=22, weight=ft.FontWeight.BOLD),
            ], tight=True, spacing=2),
        ], alignment=ft.MainAxisAlignment.START, spacing=12),
        tooltip=tooltip
    )

def build_table(columns: List[ft.DataColumn], rows: List[ft.DataRow], height: int = 300) -> ft.Container:
    return ft.Container(
        content=ft.DataTable(
            columns=columns,
            rows=rows,
            heading_row_color=op(0.04, C.PRIMARY),
            column_spacing=16,
            divider_thickness=0.4,
        ),
        border=ft.border.all(1, op(0.08, C.ON_SURFACE)),
        border_radius=12,
        padding=8,
        height=height,
        expand=False,
        bgcolor=op(0.02, C.ON_SURFACE),
    )

def row_to_cells(*vals) -> List[ft.DataCell]:
    cells = []
    for v in vals:
        if isinstance(v, str) and len(v) > 180:
            txt = v[:180] + "…"
        else:
            txt = v if v is not None else ""
        cells.append(ft.DataCell(ft.Text(str(txt))))
    return cells

# ---------- Página Principal ----------
def main(page: ft.Page):
    page.title = "PG Monitor UI"
    page.theme_mode = ft.ThemeMode.DARK
    page.window_width = 1200
    page.window_height = 800
    page.padding = 16

    # Tema toggle
    def toggle_theme(e):
        page.theme_mode = ft.ThemeMode.LIGHT if page.theme_mode == ft.ThemeMode.DARK else ft.ThemeMode.DARK
        page.update()

    theme_btn = ft.IconButton(I.BRIGHTNESS_6, tooltip="Tema claro/escuro", on_click=toggle_theme)

    # Config
    cfg_path = "config.yaml"
    cfg = load_config(cfg_path)
    dsn = ft.TextField(value=cfg["dsn"], label="DSN", expand=True, dense=True)
    interval = ft.TextField(value=str(cfg["interval_seconds"]), label="Intervalo (s)", width=140, dense=True, keyboard_type=ft.KeyboardType.NUMBER)
    topn = ft.TextField(value=str(cfg["top_n"]), label="Top N", width=100, dense=True, keyboard_type=ft.KeyboardType.NUMBER)

    page.snack_bar = ft.SnackBar(ft.Text("Configuração salva."), bgcolor=C.SECONDARY)
    def on_save(e):
        save_config(cfg_path, {
            "dsn": dsn.value.strip(),
            "interval_seconds": int(interval.value.strip() or "30"),
            "top_n": int(topn.value.strip() or "10"),
            "alerts": cfg.get("alerts", {}),
        })
        page.snack_bar.open = True
        page.update()

    save_btn = ft.ElevatedButton("Salvar Config", icon=I.SAVE, on_click=on_save)

    # ---- Testar Conexão (DNS + Porta) ----
    test_conn_btn = ft.OutlinedButton("Testar conexão", icon=I.NETWORK_CHECK)
    test_result = ft.AlertDialog(
        modal=False,
        title=ft.Text("Teste de Conexão"),
        content=ft.Column([], tight=True),
        actions=[ft.TextButton("Fechar", on_click=lambda e: setattr(test_result, "open", False) or page.update())]
    )

    async def on_test_conn():
        host, port = parse_host_port_from_dsn(dsn.value)
        msgs = [ft.Text(f"DSN alvo: {dsn.value}")]
        res = await quick_socket_tests(host, port)
        msgs += [ft.Text(m) for m in res]
        test_result.content.controls = msgs
        test_result.open = True
        page.dialog = test_result
        page.update()

    # IMPORTANTE: passe a **referência** da coroutine, não o retorno!
    test_conn_btn.on_click = lambda e: page.run_task(on_test_conn)

    # Estado de execução
    running = False
    stop_event: Optional[asyncio.Event] = None

    # KPIs
    kpi_conn = kpi_card("Conexões (usadas/total)", "-", I.SIGNAL_WIFI_STATUSBAR_4_BAR, C.BLUE, "Conexões atuais")
    kpi_blocks = kpi_card("Sessões bloqueadas", "-", I.LOCK, C.AMBER, "Processos com bloqueio")
    kpi_longq = kpi_card("Queries longas (≥ limiar)", "-", I.SCHEDULE, C.DEEP_ORANGE, "Consultas ativas demoradas")
    kpi_repl = kpi_card("Maior lag de replicação", "-", I.SYNC, C.TEAL, "Primário e Standby")
    kpi_bigtab = kpi_card("Maior tabela", "-", I.TABLE_CHART, C.PURPLE, "Top por tamanho")
    kpi_dbsize = kpi_card("Maior banco", "-", I.STORAGE, C.GREEN, "Top por tamanho")

    kpi_row = ft.ResponsiveRow([
        ft.Container(kpi_conn, col={"xs":12,"md":6,"lg":4}),
        ft.Container(kpi_blocks, col={"xs":12,"md":6,"lg":4}),
        ft.Container(kpi_longq, col={"xs":12,"md":6,"lg":4}),
        ft.Container(kpi_repl, col={"xs":12,"md":6,"lg":4}),
        ft.Container(kpi_bigtab, col={"xs":12,"md":6,"lg":4}),
        ft.Container(kpi_dbsize, col={"xs":12,"md":6,"lg":4}),
    ], spacing=8)

    # Tabelas
    tbl_connections = ft.Column(scroll=ft.ScrollMode.AUTO, expand=True)
    tbl_locks = ft.Column(scroll=ft.ScrollMode.AUTO, expand=True)
    tbl_longq = ft.Column(scroll=ft.ScrollMode.AUTO, expand=True)
    tbl_replication = ft.Column(scroll=ft.ScrollMode.AUTO, expand=True)
    tbl_tables = ft.Column(scroll=ft.ScrollMode.AUTO, expand=True)
    tbl_dbs = ft.Column(scroll=ft.ScrollMode.AUTO, expand=True)
    tbl_bloat = ft.Column(scroll=ft.ScrollMode.AUTO, expand=True)

    # Logs/alertas simples
    alerts_view = ft.ListView(expand=True, spacing=6, padding=0)

    # Controles de execução
    status_txt = ft.Text("Parado", size=12, color=op(0.8, C.ON_SURFACE))
    start_btn = ft.FilledButton("Iniciar", icon=I.PLAY_ARROW)
    stop_btn  = ft.OutlinedButton("Parar", icon=I.STOP, disabled=True)

    async def monitor_loop():
        nonlocal running, stop_event
        running = True
        stop_event = asyncio.Event()
        start_btn.disabled = True
        stop_btn.disabled = False
        status_txt.value = "Executando..."
        page.update()

        while not stop_event.is_set():
            try:
                metrics = await collect_metrics_once(dsn.value.strip(), int(topn.value or "10"))
                # KPIs
                con = metrics.get("connections", {})
                kpi_conn.content.controls[1].controls[1].value = f"{con.get('used')}/{con.get('max')}  ({con.get('pct')}%)"

                blkc = metrics.get("blocked_locks_count", 0)
                kpi_blocks.content.controls[1].controls[1].value = str(blkc)

                lrq = metrics.get("long_running_queries_top", [])
                thr = int(load_config(cfg_path)["alerts"]["long_running_query_sec_warn"])
                offenders = [r for r in lrq if (r.get("runtime_sec") or 0) >= thr]
                kpi_longq.content.controls[1].controls[1].value = f"{len(offenders)} ≥ {thr}s"

                # replicação
                repl = metrics.get("replication", [])
                max_lag = 0
                for r in repl:
                    for k in ("write_lag_sec","flush_lag_sec","replay_lag_sec"):
                        v = r.get(k) or 0
                        if v and v > max_lag:
                            max_lag = v
                if metrics.get("in_recovery"):
                    s = metrics.get("standby") or {}
                    v = s.get("replay_delay_sec") or 0
                    if v and v > max_lag:
                        max_lag = v
                kpi_repl.content.controls[1].controls[1].value = f"{max_lag}s"

                # maior tabela
                biggest = metrics.get("biggest_tables", [])
                if biggest:
                    kpi_bigtab.content.controls[1].controls[1].value = f"{biggest[0]['schema']}.{biggest[0]['table']} = {biggest[0]['total_pretty']}"
                else:
                    kpi_bigtab.content.controls[1].controls[1].value = "-"

                # maior DB
                dbsizes = metrics.get("database_sizes", [])
                if dbsizes:
                    kpi_dbsize.content.controls[1].controls[1].value = f"{dbsizes[0]['database']} = {dbsizes[0]['pretty']}"
                else:
                    kpi_dbsize.content.controls[1].controls[1].value = "-"

                # Tabelas detalhadas
                # Conexões por estado
                cols = [ft.DataColumn(ft.Text("Estado")), ft.DataColumn(ft.Text("Qtd"))]
                rows = [ft.DataRow(cells=row_to_cells(x["state"], x["qty"])) for x in (con.get("by_state") or [])]
                tbl_connections.controls = [build_table(cols, rows)]

                # Conexões por banco
                cols = [ft.DataColumn(ft.Text("Banco")), ft.DataColumn(ft.Text("Conexões"))]
                rows = [ft.DataRow(cells=row_to_cells(x["database"], x["connections"])) for x in (con.get("by_db_top") or [])]
                tbl_connections.controls.append(build_table(cols, rows))

                # Locks
                cols = [
                    ft.DataColumn(ft.Text("PID")),
                    ft.DataColumn(ft.Text("Usuário")),
                    ft.DataColumn(ft.Text("DB")),
                    ft.DataColumn(ft.Text("Tempo bloqueado")),
                    ft.DataColumn(ft.Text("Blockers")),
                    ft.DataColumn(ft.Text("Query")),
                ]
                rows = []
                for r in metrics.get("blocked_processes", []):
                    rows.append(ft.DataRow(cells=row_to_cells(r["pid"], r["usename"], r["datname"], r["blocked_for"], r["blockers"], r["query"])))
                tbl_locks.controls = [build_table(cols, rows, height=280)]

                # Long queries
                cols = [
                    ft.DataColumn(ft.Text("PID")),
                    ft.DataColumn(ft.Text("Usuário")),
                    ft.DataColumn(ft.Text("DB")),
                    ft.DataColumn(ft.Text("Runtime")),
                    ft.DataColumn(ft.Text("Segundos")),
                    ft.DataColumn(ft.Text("Query")),
                ]
                rows = []
                for r in lrq:
                    rows.append(ft.DataRow(cells=row_to_cells(r["pid"], r["usename"], r["datname"], r["runtime"], r["runtime_sec"], r["query"])))
                tbl_longq.controls = [build_table(cols, rows, height=320)]

                # Replicação
                cols = [
                    ft.DataColumn(ft.Text("Aplicação")),
                    ft.DataColumn(ft.Text("Estado")),
                    ft.DataColumn(ft.Text("Sync")),
                    ft.DataColumn(ft.Text("write_lag")),
                    ft.DataColumn(ft.Text("flush_lag")),
                    ft.DataColumn(ft.Text("replay_lag")),
                    ft.DataColumn(ft.Text("Cliente")),
                ]
                rows = []
                for r in repl:
                    rows.append(ft.DataRow(cells=row_to_cells(r["application_name"], r["state"], r["sync_state"], r["write_lag"], r["flush_lag"], r["replay_lag"], r["client_addr"])))
                tbl_replication.controls = [build_table(cols, rows, height=280)]

                # Maiores tabelas
                cols = [
                    ft.DataColumn(ft.Text("Schema")),
                    ft.DataColumn(ft.Text("Tabela")),
                    ft.DataColumn(ft.Text("Tamanho")),
                    ft.DataColumn(ft.Text("Bytes")),
                ]
                rows = []
                for r in biggest:
                    rows.append(ft.DataRow(cells=row_to_cells(r["schema"], r["table"], r["total_pretty"], r["total_bytes"])))
                tbl_tables.controls = [build_table(cols, rows, height=320)]

                # Tamanhos DB
                cols = [
                    ft.DataColumn(ft.Text("Banco")),
                    ft.DataColumn(ft.Text("Tamanho")),
                    ft.DataColumn(ft.Text("Bytes")),
                ]
                rows = []
                for r in dbsizes:
                    rows.append(ft.DataRow(cells=row_to_cells(r["database"], r["pretty"], r["bytes"])))
                tbl_dbs.controls = [build_table(cols, rows, height=320)]

                # Bloat (aprox.)
                cols = [
                    ft.DataColumn(ft.Text("Schema")),
                    ft.DataColumn(ft.Text("Tabela")),
                    ft.DataColumn(ft.Text("Live")),
                    ft.DataColumn(ft.Text("Dead")),
                    ft.DataColumn(ft.Text("% Dead")),
                ]
                rows = []
                for r in metrics.get("table_bloat_top", []):
                    rows.append(ft.DataRow(cells=row_to_cells(r["schemaname"], r["relname"], r["n_live_tup"], r["n_dead_tup"], r["dead_pct"])))
                tbl_bloat.controls = [build_table(cols, rows, height=320)]

                # Alertas simples na UI
                alerts_view.controls = []
                if (con.get("pct") or 0) >= load_config(cfg_path)["alerts"]["connections_pct_warn"]:
                    alerts_view.controls.append(ft.ListTile(leading=ft.Icon(I.WARNING_AMBER), title=ft.Text(f"ALERTA: Conexões em uso {con['pct']}%")))
                if blkc >= load_config(cfg_path)["alerts"]["blocked_locks_warn"]:
                    alerts_view.controls.append(ft.ListTile(leading=ft.Icon(I.LOCK), title=ft.Text(f"ALERTA: {blkc} sessão(ões) bloqueada(s)")))
                if offenders:
                    alerts_view.controls.append(ft.ListTile(leading=ft.Icon(I.SCHEDULE), title=ft.Text(f"ALERTA: {len(offenders)} query(s) ≥ {thr}s")))
                if max_lag and max_lag >= load_config(cfg_path)["alerts"]["replication_lag_sec_warn"]:
                    alerts_view.controls.append(ft.ListTile(leading=ft.Icon(I.SYNC_PROBLEM), title=ft.Text(f"ALERTA: Lag de replicação {max_lag}s")))

                status_txt.value = f"Última coleta: {metrics.get('collected_at')}"
            except Exception as ex:
                alerts_view.controls = [ft.ListTile(leading=ft.Icon(I.ERROR), title=ft.Text(f"Erro na coleta: {ex}"))]
                status_txt.value = "Erro na coleta (ver alerta)."
            page.update()
            try:
                await asyncio.sleep(int(interval.value or "30"))
            except Exception:
                await asyncio.sleep(30)

        running = False
        start_btn.disabled = False
        stop_btn.disabled = True
        status_txt.value = "Parado"
        page.update()

    def start_click(e):
        nonlocal running
        if running:
            return
        on_save(e)  # salva cfg antes de iniciar
        page.run_task(monitor_loop)

    def stop_click(e):
        nonlocal stop_event
        if stop_event:
            stop_event.set()

    start_btn.on_click = start_click
    stop_btn.on_click = stop_click

    # Layout
    header = ft.Row([
        ft.Icon(I.MONITOR_HEART, color=C.PRIMARY),
        ft.Text("PG Monitor UI", size=20, weight=ft.FontWeight.BOLD),
        ft.Container(expand=True),
        theme_btn
    ])

    controls_bar = ft.ResponsiveRow([
        ft.Container(dsn, col={"xs":12,"md":12,"lg":8}),
        ft.Container(interval, col={"xs":6,"md":3,"lg":2}),
        ft.Container(topn, col={"xs":6,"md":3,"lg":2}),
        ft.Row([start_btn, stop_btn, ft.ElevatedButton("Salvar", icon=I.SAVE, on_click=on_save), test_conn_btn], spacing=8),
    ], spacing=8)

    tabs = ft.Tabs(
        expand=True,
        tabs=[
            ft.Tab(text="Visão Geral", icon=I.DASHBOARD, content=ft.Column([kpi_row, ft.Divider(), ft.Text("Alertas"), alerts_view], expand=True)),
            ft.Tab(text="Conexões", icon=I.LAN, content=tbl_connections),
            ft.Tab(text="Locks", icon=I.LOCK, content=tbl_locks),
            ft.Tab(text="Long Queries", icon=I.SCHEDULE, content=tbl_longq),
            ft.Tab(text="Replicação", icon=I.SYNC, content=tbl_replication),
            ft.Tab(text="Maiores Tabelas", icon=I.TABLE_CHART, content=tbl_tables),
            ft.Tab(text="Tamanho dos Bancos", icon=I.STORAGE, content=tbl_dbs),
            ft.Tab(text="Bloat (aprox.)", icon=I.DATA_ARRAY, content=tbl_bloat),
        ],
    )

    footer = ft.Row([status_txt], alignment=ft.MainAxisAlignment.START)

    page.add(header, controls_bar, ft.Divider(), tabs, ft.Divider(), footer)

if __name__ == "__main__":
    # Executa como app desktop; para rodar no navegador, use ft.app(..., view=ft.WEB_BROWSER)
    ft.app(target=main)