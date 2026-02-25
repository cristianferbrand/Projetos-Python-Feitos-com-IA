# dashboard_flet.py (compat version: sem UserControl/Card/DataTable)
import os
import flet as ft
import requests

API_URL = os.getenv("API_URL", "http://127.0.0.1:8000").rstrip("/")
REFRESH_SECONDS = int(os.getenv("REFRESH_SECONDS", "5"))

# === Shim p/ Colors/Icons sem avaliar atributos inexistentes ===
C = ft.__dict__.get("Colors") or ft.__dict__.get("colors")
I = ft.__dict__.get("Icons") or ft.__dict__.get("icons")

# Fallbacks
PRIMARY = getattr(C, "PRIMARY", "#1E88E5") if C else "#1E88E5"
SECONDARY = getattr(C, "SECONDARY", "#8AB4F8") if C else "#8AB4F8"
GREEN = getattr(C, "GREEN", "#00C853") if C else "#00C853"
GREY = getattr(C, "GREY", "#9E9E9E") if C else "#9E9E9E"
ON_SURFACE_VARIANT = getattr(C, "ON_SURFACE_VARIANT", "#9AA0A6") if C else "#9AA0A6"
SURFACE = getattr(C, "SURFACE", "#202124") if C else "#202124"

def icon_name(name, default):
    return getattr(I, name, default) if I else default

def op(alpha: float, color: str):
    # tenta Colors.with_opacity / colors.with_opacity; se não houver, retorna cor
    try:
        return getattr(ft, "Colors").with_opacity(color, alpha)
    except Exception:
        try:
            return getattr(ft, "colors").with_opacity(color, alpha)
        except Exception:
            return color

def build_device_tile(device, on_open):
    online = device.get("online", False)
    return ft.Container(
        padding=10,
        border_radius=8,
        bgcolor=op(0.04, PRIMARY),
        content=ft.Row([
            ft.Icon(icon_name("COMPUTER", "computer"), color=GREEN if online else GREY, size=20),
            ft.Column([
                ft.Text(device.get("name",""), weight=ft.FontWeight.BOLD if hasattr(ft, "FontWeight") else None),
                ft.Text(f"Janela: {device.get('current_window','—')}\nProc.: {device.get('current_process','—')}", size=12, color=ON_SURFACE_VARIANT),
                ft.Text(f"Último contato: {device.get('last_seen','—')}", size=11, color=SECONDARY),
            ], expand=True, spacing=2),
            ft.IconButton(icon_name("OPEN_IN_NEW", "open_in_new"), tooltip="Abrir detalhes", on_click=lambda e: on_open(device)),
        ])
    )

def build_top_list(items):
    # items: [{"process_name":..., "minutes":...}, ...]
    rows = []
    for it in items:
        rows.append(ft.Row([
            ft.Text(it.get("process_name",""), expand=True),
            ft.Text(str(it.get("minutes",""))),
        ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN if hasattr(ft, "MainAxisAlignment") else None))
    return ft.Column(rows, spacing=4)

def main(page: ft.Page):
    page.title = "Painel de Produtividade"
    try:
        page.theme_mode = ft.ThemeMode.DARK
    except Exception:
        pass
    page.padding = 12

    # Esquerda: lista de devices
    devices_col = ft.Column(scroll=ft.ScrollMode.AUTO if hasattr(ft, "ScrollMode") else None, expand=True, spacing=8)
    banner = ft.Row([
        ft.Icon(icon_name("DASHBOARD", "dashboard")),
        ft.Text("Monitor de Produtividade", size=20, weight=ft.FontWeight.BOLD if hasattr(ft, "FontWeight") else None),
        ft.Container(expand=True),
        ft.TextButton("Atualizar"),
    ])
    left = ft.Column([banner, devices_col], expand=True)

    # Direita: detalhes
    detail = ft.Container(padding=16, expand=True)

    def render_devices(devices):
        tiles = [build_device_tile(d, on_open=open_detail) for d in devices]
        devices_col.controls = tiles
        page.update()

    def refresh(e=None):
        try:
            r = requests.get(f"{API_URL}/api/devices", timeout=10)
            data = r.json()
            render_devices(data.get("devices", []))
        except Exception as ex:
            devices_col.controls = [ft.Text(f"Falha ao consultar API: {ex}")]
            page.update()

    def open_detail(device: dict):
        last_url, created_at, top = None, "—", []
        try:
            j = requests.get(f"{API_URL}/api/device/{device['id']}/last_screenshot", timeout=10).json()
            last_url = j.get("url")
            created_at = j.get("created_at", "—")
        except Exception:
            pass
        try:
            top = requests.get(f"{API_URL}/api/device/{device['id']}/summary/today", timeout=10).json().get("top_process_minutes", [])
        except Exception:
            pass

        kpis = ft.Row([
            ft.Container(bgcolor=op(0.08, PRIMARY), padding=12, border_radius=12, content=ft.Column([
                ft.Text("Status", size=12, color=SECONDARY),
                ft.Text("ONLINE" if device.get("online") else "OFFLINE", size=18, weight=ft.FontWeight.BOLD if hasattr(ft, "FontWeight") else None),
            ])),
            ft.Container(bgcolor=op(0.08, PRIMARY), padding=12, border_radius=12, content=ft.Column([
                ft.Text("Janela atual", size=12, color=SECONDARY),
                ft.Text(device.get("current_window", "—"), max_lines=2),
            ])),
        ])

        img = ft.Image(src=f"{API_URL}{last_url}" if last_url else None, fit=ft.ImageFit.CONTAIN if hasattr(ft, "ImageFit") else None, height=420)

        detail.content = ft.Column([
            ft.Row([ft.Text(device.get("name",""), size=18, weight=ft.FontWeight.BOLD if hasattr(ft, "FontWeight") else None)]),
            kpis,
            ft.Text(f"Última captura: {created_at}", size=12, color=SECONDARY),
            ft.Container(img, border_radius=12, bgcolor=op(0.04, SURFACE)),
            ft.Text("Top aplicativos (hoje)", weight=ft.FontWeight.BOLD if hasattr(ft, "FontWeight") else None),
            build_top_list(top),
        ], spacing=10, scroll=ft.ScrollMode.AUTO if hasattr(ft, "ScrollMode") else None)
        page.update()

    # Liga botão Atualizar
    banner.controls[-1].on_click = refresh

    layout = ft.Row([
        ft.Container(width=420, content=left),
        ft.VerticalDivider() if hasattr(ft, "VerticalDivider") else ft.Text("|"),
        ft.Container(expand=True, content=detail),
    ], expand=True)

    page.add(layout)

    # Auto refresh com Timer se existir; caso contrário, só botão
    timer = None
    if hasattr(ft, "Timer"):
        timer = ft.Timer(interval=REFRESH_SECONDS*1000, on_tick=refresh)
        page.add(timer)

    # Primeira carga
    refresh()

if __name__ == "__main__":
    ft.app(target=main)
