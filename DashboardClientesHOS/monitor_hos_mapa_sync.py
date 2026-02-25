# monitor_backend_sync.py
import os, json, time, subprocess, threading
import psutil
import flet as ft
import psycopg2

# ===== Shim compatibilidade (ft.Colors/ft.Icons) =====
try:
    C = ft.Colors  # versões novas
except AttributeError:
    C = ft.colors
try:
    I = ft.Icons
except AttributeError:
    I = ft.icons
def op(alpha, color):
    try:
        return color
    except Exception:
        return color
# =====================================================

SERVICE_NAME = "HOSMapaSync"
BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
LOG_FILE     = os.path.join(BASE_DIR, "logs", "backend_sync.log")
CFG_FILE     = os.path.join(BASE_DIR, "config", "backend_sync_settings.json")

def read_pg_latest():
    if not os.path.exists(CFG_FILE):
        return None
    cfg = json.loads(open(CFG_FILE, "r", encoding="utf-8").read())
    pg = cfg.get("postgres", {})
    schema = (cfg.get("schema") or "public")
    try:
        con = psycopg2.connect(
            host=pg.get("host","127.0.0.1"), port=int(pg.get("port",5432)),
            dbname=pg.get("dbname","postgres"), user=pg.get("user","postgres"),
            password=pg.get("password","postgres"), connect_timeout=3
        )
        cur = con.cursor()
        cur.execute(f'''
            SELECT snapshot_id, snapshot_at, rows_hos, rows_mod, rows_rep, ok, message
            FROM "{schema}"."sync_runs"
            ORDER BY snapshot_id DESC
            LIMIT 1;
        ''')
        row = cur.fetchone()
        cur.close()
        con.close()
        if row:
            sid, at, h, m, r, ok, msg = row
            return dict(snapshot_id=sid, snapshot_at=str(at), rows_hos=h, rows_mod=m, rows_rep=r, ok=ok, message=msg)
    except Exception as e:
        return dict(error=str(e))
    return None

def service_status():
    try:
        s = psutil.win_service_get(SERVICE_NAME)
        return s.status()  # running|stopped|start_pending|stop_pending|paused
    except Exception:
        return "not_installed"

def run_cmd(cmd):
    try:
        return subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True, text=True, timeout=10)
    except subprocess.CalledProcessError as e:
        return e.output
    except Exception as e:
        return str(e)

def start_service(): return run_cmd(f"sc start {SERVICE_NAME}")
def stop_service():  return run_cmd(f"sc stop {SERVICE_NAME}")
def restart_service():
    stop_service()
    time.sleep(2)
    return start_service()

def tail_file(path, n=200):
    if not os.path.exists(path):
        return "(log não encontrado)"
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            block = 4096
            data = b""
            while size > 0 and data.count(b"\n") <= n:
                step = min(block, size)
                size -= step
                f.seek(size)
                data = f.read(step) + data
        lines = data.decode("utf-8", errors="ignore").splitlines()
        return "\n".join(lines[-n:])
    except Exception as e:
        return f"(erro lendo log: {e})"

def main(page: ft.Page):
    page.title = "Monitor - HOSMapaSync"
    page.theme_mode = ft.ThemeMode.DARK
    page.padding = 16

    status_chip = ft.Chip(label=ft.Text("..."), bgcolor=C.BLUE_GREY_800)
    last_snapshot_text = ft.Text("Último snapshot: (carregando)")
    counts_text = ft.Text("h=0, m=0, r=0")
    ok_badge = ft.Chip(label=ft.Text("OK?"), bgcolor=C.BLUE_GREY_900)
    log_view = ft.TextField(value="", multiline=True, read_only=True, min_lines=16, max_lines=24, expand=True)

    def paint_status(s):
        if s in ("running", "start_pending"):
            status_chip.label = ft.Text("RUNNING")
            status_chip.bgcolor = C.GREEN_700
        elif s in ("stopped", "stop_pending"):
            status_chip.label = ft.Text("STOPPED")
            status_chip.bgcolor = C.RED_700
        elif s == "not_installed":
            status_chip.label = ft.Text("NOT INSTALLED")
            status_chip.bgcolor = C.GREY_700
        else:
            status_chip.label = ft.Text(s.upper())
            status_chip.bgcolor = C.AMBER_700

    def refresh(_=None):
        s = service_status()
        paint_status(s)

        info = read_pg_latest()
        if isinstance(info, dict) and "error" in info:
            last_snapshot_text.value = f"Erro consultando sync_runs: {info['error']}"
            counts_text.value = ""
            ok_badge.label = ft.Text("OK? erro")
            ok_badge.bgcolor = C.AMBER_800
        elif info:
            last_snapshot_text.value = f"Último snapshot: #{info['snapshot_id']} em {info['snapshot_at']}"
            counts_text.value = f"h={info['rows_hos']}  m={info['rows_mod']}  r={info['rows_rep']}"
            if info.get("ok") == 1:
                ok_badge.label = ft.Text("OK")
                ok_badge.bgcolor = C.GREEN_800
            else:
                ok_badge.label = ft.Text("FALHA")
                ok_badge.bgcolor = C.RED_800
        else:
            last_snapshot_text.value = "Nenhum snapshot registrado."

        log_view.value = tail_file(LOG_FILE, n=200)
        page.update()

    def do_start(e):
        run_cmd(f'cmd /c start "" /b sc start {SERVICE_NAME}')
        time.sleep(1.5)
        refresh()

    def do_stop(e):
        run_cmd(f'cmd /c start "" /b sc stop {SERVICE_NAME}')
        time.sleep(1.5)
        refresh()

    def do_restart(e):
        threading.Thread(target=lambda: (restart_service(), refresh()), daemon=True).start()

    buttons = ft.Row([
        ft.ElevatedButton("Start", icon=I.PLAY_ARROW, on_click=do_start),
        ft.ElevatedButton("Stop",  icon=I.STOP, on_click=do_stop),
        ft.ElevatedButton("Restart", icon=I.RESTART_ALT, on_click=do_restart),
        ft.IconButton(I.REFRESH, on_click=refresh, tooltip="Atualizar agora"),
    ], spacing=12)

    header = ft.Row([status_chip, ok_badge, last_snapshot_text, counts_text], spacing=16, wrap=True)

    page.add(
        ft.Text("Monitor do Serviço HOSMapaSync", size=20, weight=ft.FontWeight.BOLD),
        header, buttons,
        ft.Divider(),
        ft.Text("Logs (backend_sync.log):"),
        log_view
    )

    t = ft.Timer(interval=5000, on_tick=refresh)
    page.overlay.append(t)
    refresh()

ft.app(target=main)