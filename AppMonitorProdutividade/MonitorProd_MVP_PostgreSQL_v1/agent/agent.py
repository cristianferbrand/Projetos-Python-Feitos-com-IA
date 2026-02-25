# agent.py - Windows Agent
import base64
import io
import json
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import psutil
import requests
import mss
from PIL import Image, ImageFilter

# Win32 somente no Windows
try:
    import win32gui
    import win32process
except Exception:
    win32gui = None
    win32process = None

CONFIG_PATH = os.getenv("AGENT_CONFIG", "agent_config.json")

@dataclass
class Config:
    server_url: str
    token: str
    device_name: str
    screenshot_interval: int = 30
    event_flush_interval: int = 10
    blur_radius: int = 8
    enable_screenshot: bool = True
    blocklist_keywords: list[str] = None  # títulos/processos que não devem ser capturados

    @staticmethod
    def load(path: str) -> "Config":
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return Config(
            server_url=data["server_url"].rstrip("/"),
            token=data["token"],
            device_name=data.get("device_name", os.getenv("COMPUTERNAME", "PC")),
            screenshot_interval=int(data.get("screenshot_interval", 30)),
            event_flush_interval=int(data.get("event_flush_interval", 10)),
            blur_radius=int(data.get("blur_radius", 8)),
            enable_screenshot=bool(data.get("enable_screenshot", True)),
            blocklist_keywords=data.get("blocklist_keywords", []),
        )

class EventBuffer:
    def __init__(self):
        self._lock = threading.Lock()
        self._events: list[dict] = []

    def add(self, e: dict):
        with self._lock:
            self._events.append(e)

    def drain(self) -> list[dict]:
        with self._lock:
            out = self._events
            self._events = []
            return out

def get_active_window_info() -> tuple[str, str]:
    """Retorna (process_name, window_title)."""
    if not win32gui:
        # fallback limitado
        return ("unknown", "unknown")
    hwnd = win32gui.GetForegroundWindow()
    title = win32gui.GetWindowText(hwnd)
    try:
        _tid, pid = win32process.GetWindowThreadProcessId(hwnd)
        proc = psutil.Process(pid)
        pname = proc.name()
    except Exception:
        pname = "unknown"
    return (pname or "unknown", title or "")

def should_block(title: str, proc: str, keywords: list[str]) -> bool:
    s = f"{title} {proc}".lower()
    return any(kw.lower() in s for kw in (keywords or []))

def jpeg_base64_from_screen(blur_radius: int) -> Optional[str]:
    try:
        with mss.mss() as sct:
            mon = sct.monitors[1]
            shot = sct.grab(mon)
        img = Image.frombytes("RGB", shot.size, shot.rgb)
        # Reduz para thumbnail p/ privacidade e tráfego
        img.thumbnail((1280, 720))
        if blur_radius > 0:
            img = img.filter(ImageFilter.GaussianBlur(radius=blur_radius))
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=70)
        return base64.b64encode(buf.getvalue()).decode("ascii")
    except Exception:
        return None

def post_json(cfg: Config, path: str, payload: dict) -> bool:
    try:
        r = requests.post(
            f"{cfg.server_url}{path}",
            json=payload,
            headers={"X-Token": cfg.token},
            timeout=10,
        )
        return r.ok
    except Exception:
        return False

def loop_heartbeat(cfg: Config):
    while True:
        post_json(cfg, "/api/agent/heartbeat", {"name": cfg.device_name})
        time.sleep(30)

def loop_screenshot(cfg: Config):
    if not cfg.enable_screenshot:
        return
    while True:
        # não captura screenshot se janela atual está bloqueada
        proc, title = get_active_window_info()
        if not should_block(title, proc, cfg.blocklist_keywords):
            b64 = jpeg_base64_from_screen(cfg.blur_radius)
            if b64:
                post_json(cfg, "/api/agent/screenshot", {"image_b64": b64})
        time.sleep(cfg.screenshot_interval)

def loop_events(cfg: Config, buf: EventBuffer):
    last_proc, last_title = None, None
    last_start = datetime.utcnow()
    while True:
        proc, title = get_active_window_info()
        now = datetime.utcnow()
        if (proc, title) != (last_proc, last_title):
            # fecha evento anterior
            if last_proc is not None:
                dur = (now - last_start).total_seconds()
                if dur > 0:
                    buf.add({
                        "process_name": last_proc,
                        "window_title": last_title,
                        "started_at": last_start.isoformat() + "Z",
                        "duration_sec": dur,
                    })
            # inicia novo
            last_proc, last_title = proc, title
            last_start = now
        time.sleep(1)

def loop_flush(cfg: Config, buf: EventBuffer):
    while True:
        batch = buf.drain()
        if batch:
            post_json(cfg, "/api/agent/events", {"events": batch})
        time.sleep(cfg.event_flush_interval)

def main():
    cfg = Config.load(CONFIG_PATH)
    # Primeiro heartbeat (e registro, se necessário)
    post_json(cfg, "/api/agent/heartbeat", {"name": cfg.device_name})

    buf = EventBuffer()
    threading.Thread(target=loop_heartbeat, args=(cfg,), daemon=True).start()
    threading.Thread(target=loop_events, args=(cfg, buf), daemon=True).start()
    threading.Thread(target=loop_flush, args=(cfg, buf), daemon=True).start()
    threading.Thread(target=loop_screenshot, args=(cfg,), daemon=True).start()

    # Mantém vivo
    while True:
        time.sleep(60)

if __name__ == "__main__":
    main()
