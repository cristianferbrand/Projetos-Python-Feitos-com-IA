# === metrics_agent.py ===
# Serviço de métricas remotas via HTTP (FastAPI) para ser consultado pelo app Flet.
# Requisitos: pip install fastapi uvicorn psutil
# Execução:
#   set SECRET_TOKEN=meutoken123   # Windows (PowerShell: $env:SECRET_TOKEN="meutoken123")
#   export SECRET_TOKEN=meutoken123 # Linux/macOS
#   uvicorn metrics_agent:app --host 0.0.0.0 --port 8000 --workers 1
#
# Em rede VPN, a URL típica será: http://<IP_VPN_REMOTO>:8000/metrics

import os
import time
import socket
import platform
import threading
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any, List

import psutil
from fastapi import FastAPI, Header, Query, HTTPException

TZ_BR = ZoneInfo("America/Sao_Paulo")
SECRET = os.environ.get("SECRET_TOKEN", "")  # se vazio, sem autenticação

app = FastAPI(title="Metrics Agent", version="1.0.0")

_lock = threading.Lock()
_last_net = psutil.net_io_counters()
_last_ts = time.time()
_peak_up = 0.0
_peak_down = 0.0

def _safe_disk_root() -> str:
    if os.name == "nt":
        root = os.environ.get("SystemDrive", "C:") + "\\"
        return root if os.path.exists(root) else "C:\\"
    return "/"

def _fmt_ts(dt: datetime) -> str:
    return dt.astimezone(TZ_BR).strftime("%Y-%m-%d %H:%M:%S")

@app.get("/healthz")
def healthz() -> Dict[str, Any]:
    return {"status": "ok", "time": _fmt_ts(datetime.now(TZ_BR))}

@app.get("/metrics")
def metrics(x_token: Optional[str] = Header(None), token: Optional[str] = Query(None)) -> Dict[str, Any]:
    if SECRET and (x_token != SECRET and token != SECRET):
        raise HTTPException(status_code=401, detail="Unauthorized")

    global _last_net, _last_ts, _peak_up, _peak_down

    now = time.time()
    cur = psutil.net_io_counters()
    dt = max(now - _last_ts, 1e-6)
    up_kbs = (cur.bytes_sent - _last_net.bytes_sent) / dt / 1024.0
    down_kbs = (cur.bytes_recv - _last_net.bytes_recv) / dt / 1024.0

    # picos com leve decaimento (deixa visual mais intuitivo)
    _peak_up = max(_peak_up * 0.9, up_kbs)
    _peak_down = max(_peak_down * 0.9, down_kbs)

    with _lock:
        _last_net = cur
        _last_ts = now

    # KPIs
    cpu = psutil.cpu_percent(interval=None)
    mem = psutil.virtual_memory().percent
    du = psutil.disk_usage(_safe_disk_root()).percent

    # Processos
    procs: List[Dict[str, Any]] = []
    for p in psutil.process_iter(attrs=["pid", "name", "cpu_percent", "memory_percent"]):
        try:
            info = p.info
            procs.append({
                "pid": int(info.get("pid", 0)),
                "name": (info.get("name") or "")[:100],
                "cpu_percent": float(info.get("cpu_percent") or 0.0),
                "memory_percent": float(info.get("memory_percent") or 0.0),
            })
        except Exception:
            continue

    procs_cpu = sorted(procs, key=lambda r: r["cpu_percent"], reverse=True)
    procs_mem = sorted(procs, key=lambda r: r["memory_percent"], reverse=True)

    uname = platform.uname()
    host = socket.gethostname()
    boot_ts = datetime.fromtimestamp(psutil.boot_time(), tz=TZ_BR)

    return {
        "timestamp": _fmt_ts(datetime.now(TZ_BR)),
        "host": host,
        "system": {
            "system": uname.system,
            "release": uname.release,
            "version": uname.version,
            "machine": uname.machine,
            "processor": uname.processor or "N/D",
        },
        "boot_time": _fmt_ts(boot_ts),
        "cpu_percent": cpu,
        "mem_percent": mem,
        "disk_percent": du,
        "net": {
            "up_kbs": up_kbs,
            "down_kbs": down_kbs,
            "peak_up": _peak_up,
            "peak_down": _peak_down,
        },
        "processes_by_cpu": procs_cpu,  # TODOS os processos, ordenados desc.
        "processes_by_mem": procs_mem,  # TODOS os processos, ordenados desc.
    }