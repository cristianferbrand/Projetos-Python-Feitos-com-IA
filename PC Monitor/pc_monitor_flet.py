# === pc_monitor_flet.py ===
# Monitor local OU remoto (via agente HTTP). Requisitos:
#   pip install flet psutil requests
# Uso:
#   python pc_monitor_flet.py
# No modo Remoto, a URL do agente pode ser:
#   - Base:     http://26.92.146.196:8000
#   - Completa: http://26.92.146.196:8000/metrics?token=SEU_TOKEN
# Se usar a base, informe o Token no campo. Se usar a completa com ?token=..., o campo Token pode ficar vazio.

import asyncio
import csv
import os
import platform
import socket
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Any, Dict, List

import psutil
import requests
import flet as ft

# -----------------------------
# SHIM ft.Colors/ft.Icons (preferência do usuário) + helper op()
# -----------------------------
try:
    C = ft.Colors  # Flet mais recente
    I = ft.Icons
except Exception:
    C = ft.colors
    I = ft.icons

def op(alpha: float, color: str) -> str:
    try:
        return ft.colors.with_opacity(alpha, color)  # type: ignore
    except Exception:
        try:
            return ft.Colors.with_opacity(alpha, color)  # type: ignore
        except Exception:
            return color

# -----------------------------
# Utilidades
# -----------------------------
TZ_BR = ZoneInfo("America/Sao_Paulo")

def fmt_ts(dt: datetime) -> str:
    return dt.astimezone(TZ_BR).strftime("%Y-%m-%d %H:%M:%S")

def safe_disk_root() -> str:
    if os.name == "nt":
        root = os.environ.get("SystemDrive", "C:") + "\\"
        return root if os.path.exists(root) else "C:\\"
    return "/"

def boot_time_dt() -> datetime:
    return datetime.fromtimestamp(psutil.boot_time(), tz=TZ_BR)

def uptime_td() -> timedelta:
    return datetime.now(tz=TZ_BR) - boot_time_dt()

# -----------------------------
# App principal
# -----------------------------
class MonitorApp:
    def __init__(self, page: ft.Page):
        self.page = page
        self.page.title = "Monitor de Computador - Flet (Local/Remoto)"
        self.page.padding = 16
        self.page.theme_mode = ft.ThemeMode.DARK
        self.page.window_min_width = 1100
        self.page.window_min_height = 720

        # Estado principal
        self.monitor_running = True
        self.monitor_task_started = False
        self.stop_event: asyncio.Event | None = None

        # CSV/log
        self.log_switch: ft.Switch | None = None
        self.log_dir = os.path.join(os.getcwd(), "logs")
        self.log_path = os.path.join(self.log_dir, "monitor.csv")

        # Modo remoto
        self.remote_switch: ft.Switch | None = None
        self.remote_url_tf: ft.TextField | None = None
        self.remote_token_tf: ft.TextField | None = None

        # Auto/intervalo
        self.auto_switch: ft.Switch | None = None
        self.interval_slider: ft.Slider | None = None
        self.interval_value_text: ft.Text | None = None

        # Botões
        self.btn_start_stop: ft.ElevatedButton | None = None
        self.btn_refresh: ft.TextButton | None = None

        # Métricas de rede (local)
        self._last_net = psutil.net_io_counters()
        self._last_net_ts = time.time()
        self._peak_up = 0.0
        self._peak_down = 0.0

        # KPIs
        self.cpu_ring = ft.ProgressRing(value=0, width=60, height=60, color=C.AMBER)
        self.mem_ring = ft.ProgressRing(value=0, width=60, height=60, color=C.BLUE)
        self.disk_ring = ft.ProgressRing(value=0, width=60, height=60, color=C.GREEN)

        self.cpu_value = ft.Text("0%", weight=ft.FontWeight.BOLD, size=20)
        self.mem_value = ft.Text("0%", weight=ft.FontWeight.BOLD, size=20)
        self.disk_value = ft.Text("0%", weight=ft.FontWeight.BOLD, size=20)

        self.net_up_text = ft.Text("Up: 0.0 kB/s (pico 0.0)", size=14)
        self.net_down_text = ft.Text("Down: 0.0 kB/s (pico 0.0)", size=14)

        # Tabelas de processos (TODOS, com rolagem)
        self.tbl_cpu = self._make_process_table("Processos por CPU (%)")
        self.tbl_mem = self._make_process_table("Processos por Memória (%)")

        # Info do sistema
        self.sys_info_text = ft.Text("", size=14)

        # Constrói UI
        self._build_ui()

    # -------------------------
    # UI
    # -------------------------
    def _kpi_card(self, title: str, ring: ft.ProgressRing, value_text: ft.Text, subtitle: str) -> ft.Card:
        return ft.Card(
            content=ft.Container(
                padding=16,
                content=ft.Row(
                    alignment=ft.MainAxisAlignment.START,
                    spacing=12,
                    controls=[
                        ring,
                        ft.Column(
                            spacing=2,
                            controls=[
                                ft.Text(title, size=13, color=op(0.8, C.ON_SURFACE)),
                                value_text,
                                ft.Text(subtitle, size=12, color=op(0.6, C.ON_SURFACE)),
                            ],
                        ),
                    ],
                ),
            )
        )

    def _net_card(self) -> ft.Card:
        return ft.Card(
            content=ft.Container(
                padding=16,
                content=ft.Column(
                    spacing=6,
                    controls=[
                        ft.Row(
                            alignment=ft.MainAxisAlignment.START,
                            spacing=8,
                            controls=[
                                ft.Icon(I.SPEED, color=C.CYAN),
                                ft.Text("Rede (kB/s)", weight=ft.FontWeight.BOLD),
                            ],
                        ),
                        self.net_up_text,
                        self.net_down_text,
                    ],
                ),
            )
        )

    def _make_process_table(self, title: str) -> ft.Column:
        table = ft.DataTable(
            columns=[
                ft.DataColumn(ft.Text("PID")),
                ft.DataColumn(ft.Text("Nome")),
                ft.DataColumn(ft.Text("CPU %")),
                ft.DataColumn(ft.Text("Mem %")),
            ],
            rows=[],
            heading_row_color=op(0.08, C.PRIMARY),
            data_row_color={"hovered": op(0.08, C.PRIMARY)},
            divider_thickness=0.6,
        )
        return ft.Column(
            expand=True,
            scroll=ft.ScrollMode.AUTO,
            controls=[
                ft.Row(
                    spacing=8,
                    controls=[ft.Icon(I.LIST, color=C.ORANGE), ft.Text(title, weight=ft.FontWeight.BOLD)],
                ),
                table,
            ],
        )

    def _controls_row(self) -> ft.Column:
        # Linha 1: Auto/Intervalo + Log + Start/Stop/Refresh + Tema
        self.auto_switch = ft.Switch(label="Auto", value=True, on_change=self._on_auto_change)
        self.interval_slider = ft.Slider(min=0.5, max=10, value=2, divisions=19, width=200, on_change=self._on_interval_change)
        self.interval_value_text = ft.Text("2.0 s", size=12, color=op(0.8, C.ON_SURFACE))
        self.log_switch = ft.Switch(label="Log CSV", value=False)
        self.btn_refresh = ft.TextButton("Atualizar agora", icon=I.REFRESH, on_click=self._on_refresh_click)
        self.btn_start_stop = ft.ElevatedButton(
            "Parar", icon=I.STOP_CIRCLE, bgcolor=C.RED, color=C.ON_PRIMARY, on_click=self._on_start_stop_click
        )
        btn_open_logs = ft.OutlinedButton("Abrir pasta de logs", icon=I.FOLDER_OPEN, on_click=self._on_open_logs)
        theme_toggle = ft.IconButton(icon=I.DARK_MODE, tooltip="Alternar tema", on_click=lambda e: self._toggle_theme())

        row1 = ft.Row(
            alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
            vertical_alignment=ft.CrossAxisAlignment.CENTER,
            controls=[
                ft.Row(spacing=16, controls=[self.auto_switch, ft.Text("Intervalo:"), self.interval_slider, self.interval_value_text]),
                ft.Row(spacing=8, controls=[self.log_switch, btn_open_logs, self.btn_refresh, self.btn_start_stop, theme_toggle]),
            ],
        )

        # Linha 2: Controles do modo Remoto (VPN)
        self.remote_switch = ft.Switch(label="Remoto (VPN)", value=False)
        self.remote_url_tf = ft.TextField(
            label="URL do agente",
            hint_text="http://IP:8000  ou  http://IP:8000/metrics?token=SEU_TOKEN",
            width=460,
        )
        self.remote_token_tf = ft.TextField(label="Token", hint_text="(X-Token, se usar URL base)", password=True, can_reveal_password=True, width=280)

        row2 = ft.Row(
            alignment=ft.MainAxisAlignment.START,
            controls=[
                self.remote_switch,
                self.remote_url_tf,
                self.remote_token_tf,
                ft.Text("(Aceita base ou URL completa)"),
            ],
        )

        return ft.Column(spacing=8, controls=[row1, row2])

    def _build_ui(self):
        self.page.appbar = ft.AppBar(
            title=ft.Text("Monitor de Computador (Local/Remoto)"),
            center_title=False,
            bgcolor=op(0.08, C.PRIMARY),
            actions=[ft.Icon(I.COMPUTER)],
        )

        kpi_row = ft.Row(
            wrap=False,
            spacing=16,
            controls=[
                self._kpi_card("CPU", self.cpu_ring, self.cpu_value, "Uso total da CPU"),
                self._kpi_card("Memória", self.mem_ring, self.mem_value, "Uso da RAM"),
                self._kpi_card("Disco (root)", self.disk_ring, self.disk_value, f"Uso de {safe_disk_root()}"),
                self._net_card(),
            ],
        )

        info_card = ft.Card(
            content=ft.Container(
                padding=16,
                content=ft.Column(
                    spacing=6,
                    controls=[
                        ft.Row(spacing=8, controls=[ft.Icon(I.INFO, color=C.TEAL), ft.Text("Informações do Sistema", weight=ft.FontWeight.BOLD)]),
                        self.sys_info_text,
                    ],
                ),
            )
        )

        self.page.add(
            self._controls_row(),
            kpi_row,
            ft.Row(
                spacing=16,
                expand=True,
                controls=[
                    ft.Container(self.tbl_cpu, expand=1, padding=ft.padding.only(right=8)),
                    ft.Container(self.tbl_mem, expand=1, padding=ft.padding.only(left=8)),
                ],
            ),
            info_card,
        )

        self.page.on_close = self._on_close

        # Inicializa e inicia loop
        psutil.cpu_percent(interval=None)  # pre aquecimento
        self._refresh_all()
        self._start_monitor_task()

    # -------------------------
    # Lógica de monitoramento
    # -------------------------
    def _toggle_theme(self):
        self.page.theme_mode = ft.ThemeMode.LIGHT if self.page.theme_mode == ft.ThemeMode.DARK else ft.ThemeMode.DARK
        self.page.update()

    def _on_open_logs(self, e):
        try:
            os.makedirs(self.log_dir, exist_ok=True)
            if os.name == "nt":
                os.startfile(self.log_dir)
            elif platform.system() == "Darwin":
                os.system(f'open "{self.log_dir}"')
            else:
                os.system(f'xdg-open "{self.log_dir}"')
        except Exception as ex:
            self._toast(f"Erro ao abrir pasta: {ex}")

    def _toast(self, msg: str):
        self.page.snack_bar = ft.SnackBar(ft.Text(msg))
        self.page.snack_bar.open = True
        self.page.update()

    def _on_auto_change(self, e: ft.ControlEvent):
        self.page.update()

    def _on_interval_change(self, e: ft.ControlEvent):
        if self.interval_value_text and self.interval_slider:
            self.interval_value_text.value = f"{self.interval_slider.value:.1f} s"
            self.page.update()

    def _on_refresh_click(self, e: ft.ControlEvent):
        self._refresh_all()

    def _on_start_stop_click(self, e: ft.ControlEvent):
        if self.monitor_running:
            self.monitor_running = False
            if self.stop_event:
                self.stop_event.set()
            if self.btn_start_stop:
                self.btn_start_stop.text = "Iniciar"
                self.btn_start_stop.icon = I.PLAY_CIRCLE
                self.btn_start_stop.bgcolor = C.GREEN
                self.btn_start_stop.color = C.ON_PRIMARY
            self.page.update()
        else:
            self.monitor_running = True
            if self.btn_start_stop:
                self.btn_start_stop.text = "Parar"
                self.btn_start_stop.icon = I.STOP_CIRCLE
                self.btn_start_stop.bgcolor = C.RED
                self.btn_start_stop.color = C.ON_PRIMARY
            self._start_monitor_task()
            self.page.update()

    def _on_close(self, e):
        self.monitor_running = False
        if self.stop_event:
            self.stop_event.set()

    def _start_monitor_task(self):
        if not self.monitor_task_started:
            self.monitor_task_started = True
            self.page.run_task(self._monitor_loop)

    async def _monitor_loop(self):
        while True:
            self.stop_event = asyncio.Event()
            while self.monitor_running:
                self._refresh_all()
                wait_s = self.interval_slider.value if self.interval_slider else 2.0
                try:
                    await asyncio.wait_for(self.stop_event.wait(), timeout=wait_s)
                except asyncio.TimeoutError:
                    pass
                if self.stop_event.is_set():
                    break
            await asyncio.sleep(0.2)

    # -------------------------
    # Atualizações de tela (local/remote)
    # -------------------------
    def _refresh_all(self):
        try:
            if self.remote_switch and self.remote_switch.value:
                self._refresh_remote()  # <<-- PATCH: aceita base OU URL completa
            else:
                self._refresh_local()

            self._maybe_log_csv()
            self.page.update()
        except Exception as ex:
            self._toast(f"Erro ao atualizar: {ex}")

    # -------- LOCAL --------
    def _refresh_local(self):
        # KPIs
        cpu = psutil.cpu_percent(interval=None)
        mem = psutil.virtual_memory().percent
        du = psutil.disk_usage(safe_disk_root()).percent

        self.cpu_ring.value = max(0.0, min(cpu / 100.0, 1.0))
        self.mem_ring.value = max(0.0, min(mem / 100.0, 1.0))
        self.disk_ring.value = max(0.0, min(du / 100.0, 1.0))
        self.cpu_value.value = f"{cpu:.0f}%"
        self.mem_value.value = f"{mem:.0f}%"
        self.disk_value.value = f"{du:.0f}%"

        # Rede
        now = time.time()
        cur = psutil.net_io_counters()
        dt = max(now - self._last_net_ts, 1e-6)
        up_kbs = (cur.bytes_sent - self._last_net.bytes_sent) / dt / 1024.0
        down_kbs = (cur.bytes_recv - self._last_net.bytes_recv) / dt / 1024.0
        self._peak_up = max(self._peak_up * 0.9, up_kbs)
        self._peak_down = max(self._peak_down * 0.9, down_kbs)
        self.net_up_text.value = f"Up: {up_kbs:.1f} kB/s (pico {self._peak_up:.1f})"
        self.net_down_text.value = f"Down: {down_kbs:.1f} kB/s (pico {self._peak_down:.1f})"
        self._last_net = cur
        self._last_net_ts = now

        # Processos (TODOS, ordenados)
        rows = []
        for p in psutil.process_iter(attrs=["pid", "name", "cpu_percent", "memory_percent"]):
            try:
                info = p.info
                rows.append(
                    (int(info.get("pid", 0)), (info.get("name") or "")[:100],
                     float(info.get("cpu_percent") or 0.0), float(info.get("memory_percent") or 0.0))
                )
            except Exception:
                continue

        cpu_sorted = sorted(rows, key=lambda r: r[2], reverse=True)
        mem_sorted = sorted(rows, key=lambda r: r[3], reverse=True)
        self._fill_tables(cpu_sorted, mem_sorted)

        # Info do sistema (local)
        uname = platform.uname()
        host = socket.gethostname()
        bt = boot_time_dt()
        up = uptime_td()
        self.sys_info_text.value = "\n".join([
            f"Sistema: {uname.system} {uname.release} ({uname.version})",
            f"Máquina: {uname.machine} | Processador: {uname.processor or 'N/D'}",
            f"Hostname: {host}",
            f"Boot: {fmt_ts(bt)} | Uptime: {self._fmt_timedelta(up)}",
            f"Fonte: LOCAL",
        ])

    # -------- REMOTO --------
    def _refresh_remote(self):
        """
        PATCH: aceita tanto URL base (http://IP:8000) quanto URL completa
        (http://IP:8000/metrics?token=XYZ). Se for base, envia o token via header X-Token.
        Se for completa e já tiver ?token=, não precisa do header (mas pode enviar também).
        """
        raw = (self.remote_url_tf.value or "").strip()
        if not raw:
            raise RuntimeError("URL do agente não informada.")

        headers = {}
        # Detecta se o usuário já forneceu /metrics e/ou query string (?token=...)
        has_metrics = "/metrics" in raw
        has_query = "?" in raw
        has_token_query = "token=" in raw

        if has_metrics or has_query:
            # Usa a URL exatamente como fornecida
            url = raw
            # Se não há ?token= na URL e o usuário informou token, envia no header
            if (not has_token_query) and self.remote_token_tf and self.remote_token_tf.value:
                headers["X-Token"] = self.remote_token_tf.value
        else:
            # É base → acrescenta /metrics e usa header para o token (se informado)
            base = raw.rstrip("/")
            url = f"{base}/metrics"
            if self.remote_token_tf and self.remote_token_tf.value:
                headers["X-Token"] = self.remote_token_tf.value

        try:
            resp = requests.get(url, headers=headers, timeout=4)
        except Exception as ex:
            raise RuntimeError(f"Falha ao acessar agente remoto: {ex}")

        if resp.status_code == 401:
            raise RuntimeError("Token inválido ou ausente (401).")
        if not resp.ok:
            raise RuntimeError(f"Agente retornou {resp.status_code}: {resp.text[:200]}")

        data: Dict[str, Any] = resp.json()

        # KPIs
        cpu = float(data.get("cpu_percent", 0.0))
        mem = float(data.get("mem_percent", 0.0))
        du = float(data.get("disk_percent", 0.0))
        self.cpu_ring.value = max(0.0, min(cpu / 100.0, 1.0))
        self.mem_ring.value = max(0.0, min(mem / 100.0, 1.0))
        self.disk_ring.value = max(0.0, min(du / 100.0, 1.0))
        self.cpu_value.value = f"{cpu:.0f}%"
        self.mem_value.value = f"{mem:.0f}%"
        self.disk_value.value = f"{du:.0f}%"

        # Rede (valores já calculados no remoto)
        net = data.get("net", {}) or {}
        up_kbs = float(net.get("up_kbs", 0.0))
        down_kbs = float(net.get("down_kbs", 0.0))
        peak_up = float(net.get("peak_up", up_kbs))
        peak_down = float(net.get("peak_down", down_kbs))
        self.net_up_text.value = f"Up: {up_kbs:.1f} kB/s (pico {peak_up:.1f})"
        self.net_down_text.value = f"Down: {down_kbs:.1f} kB/s (pico {peak_down:.1f})"

        # Processos (já ordenados no remoto; ainda assim garantimos)
        procs_cpu = data.get("processes_by_cpu", []) or []
        procs_mem = data.get("processes_by_mem", []) or []
        cpu_sorted = [(int(p["pid"]), p["name"], float(p["cpu_percent"]), float(p["memory_percent"])) for p in procs_cpu]
        mem_sorted = [(int(p["pid"]), p["name"], float(p["cpu_percent"]), float(p["memory_percent"])) for p in procs_mem]
        cpu_sorted.sort(key=lambda r: r[2], reverse=True)
        mem_sorted.sort(key=lambda r: r[3], reverse=True)
        self._fill_tables(cpu_sorted, mem_sorted)

        # Info do sistema (remoto)
        sys = data.get("system", {}) or {}
        host = data.get("host", "N/D")
        boot = data.get("boot_time", "N/D")
        # Mostra a origem (base sanitizada)
        origem = raw.split("/metrics")[0] if "/metrics" in raw else raw
        self.sys_info_text.value = "\n".join([
            f"Sistema: {sys.get('system','?')} {sys.get('release','?')} ({sys.get('version','?')})",
            f"Máquina: {sys.get('machine','?')} | Processador: {sys.get('processor','N/D')}",
            f"Hostname: {host}",
            f"Boot: {boot}",
            f"Fonte: REMOTO ({origem})",
        ])

    # -------- Comum --------
    def _fill_tables(self, cpu_sorted: List[tuple], mem_sorted: List[tuple]):
        def to_row(r):
            pid, name, cpu_p, mem_p = r
            return ft.DataRow(
                cells=[
                    ft.DataCell(ft.Text(str(pid))),
                    ft.DataCell(ft.Text(name[:50])),
                    ft.DataCell(ft.Text(f"{cpu_p:.1f}")),
                    ft.DataCell(ft.Text(f"{mem_p:.1f}")),
                ]
            )

        cpu_table: ft.DataTable = self.tbl_cpu.controls[1]
        mem_table: ft.DataTable = self.tbl_mem.controls[1]
        cpu_table.rows = [to_row(r) for r in cpu_sorted]
        mem_table.rows = [to_row(r) for r in mem_sorted]

    def _fmt_timedelta(self, td: timedelta) -> str:
        total_s = int(td.total_seconds())
        d, r = divmod(total_s, 86400)
        h, r = divmod(r, 3600)
        m, s = divmod(r, 60)
        parts = []
        if d: parts.append(f"{d}d")
        if h: parts.append(f"{h}h")
        if m: parts.append(f"{m}m")
        parts.append(f"{s}s")
        return " ".join(parts)

    def _maybe_log_csv(self):
        if not (self.log_switch and self.log_switch.value):
            return

        os.makedirs(self.log_dir, exist_ok=True)
        cpu = float(self.cpu_value.value.strip("%")) if self.cpu_value.value else 0.0
        mem = float(self.mem_value.value.strip("%")) if self.mem_value.value else 0.0
        disk = float(self.disk_value.value.strip("%")) if self.disk_value.value else 0.0

        def parse_kbs(line: str) -> float:
            try:
                num = line.split(":")[1].split("kB/s")[0].strip()
                return float(num)
            except Exception:
                return 0.0

        up_kbs = parse_kbs(self.net_up_text.value or "")
        down_kbs = parse_kbs(self.net_down_text.value or "")

        fonte = "REMOTO" if (self.remote_switch and self.remote_switch.value) else "LOCAL"
        ts = fmt_ts(datetime.now(TZ_BR))
        header = ["timestamp", "fonte", "cpu_percent", "mem_percent", "disk_percent", "up_kBs", "down_kBs"]
        row = [ts, fonte, f"{cpu:.1f}", f"{mem:.1f}", f"{disk:.1f}", f"{up_kbs:.1f}", f"{down_kbs:.1f}"]

        file_exists = os.path.exists(self.log_path)
        try:
            with open(self.log_path, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f, delimiter=";")
                if not file_exists:
                    w.writerow(header)
                w.writerow(row)
        except Exception as ex:
            self._toast(f"Erro ao gravar log: {ex}")

# -----------------------------
# entrypoint
# -----------------------------
def main(page: ft.Page):
    MonitorApp(page)

if __name__ == "__main__":
    ft.app(target=main)