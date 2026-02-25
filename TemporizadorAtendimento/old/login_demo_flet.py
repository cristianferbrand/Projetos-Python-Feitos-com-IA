# -*- coding: utf-8 -*-
# Flet Login UI Demo (centered header) - run: python login_demo_flet_centered.py

import flet as ft

C = ft.Colors
I = ft.Icons

def login_demo(page: ft.Page):
    page.title = "Login - Demo (Centered Header)"
    page.theme_mode = ft.ThemeMode.DARK
    page.theme = ft.Theme(
        color_scheme_seed=C.TEAL,
        use_material3=True,
        visual_density=ft.VisualDensity.COMPACT,
    )
    page.padding = 0

    # --- Handlers
    def do_login(e):
        if not user.value or not password.value:
            toast("Informe usuário e senha.")
            return
        toast(f"Login demo: {user.value}")

    def go_create_admin(e):
        toast("Abrir tela de criação de administrador (demo).")

    def toast(msg: str):
        page.snack_bar = ft.SnackBar(ft.Text(msg))
        page.snack_bar.open = True
        page.update()

    # --- Header (strictly centered)
    logo = ft.Container(
        width=72,
        height=72,
        border_radius=36,
        bgcolor=C.TEAL_900,
        alignment=ft.alignment.center,
        content=ft.Icon(I.LOCK, size=36, color=C.TEAL_200),
    )
    title = ft.Text(
        "Acessar sua conta",
        size=22,
        weight=ft.FontWeight.W_700,
        text_align=ft.TextAlign.CENTER,   # ensure centered text
    )
    subtitle = ft.Text(
        "Entre para iniciar sua jornada e registrar atendimentos",
        size=12,
        color=C.GREY_500,
        text_align=ft.TextAlign.CENTER,   # ensure centered text
    )

    # wrap header in a container with explicit center alignment
    header = ft.Container(
        alignment=ft.alignment.center,
        expand=True,
        content=ft.Column(
            [
                ft.Row([logo], alignment=ft.MainAxisAlignment.CENTER),
                ft.Container(title, alignment=ft.alignment.center, expand=True),
                ft.Container(subtitle, alignment=ft.alignment.center, expand=True),
            ],
            spacing=10,
            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
        ),
    )

    # --- Form fields
    user = ft.TextField(
        label="Usuário",
        autofocus=True,
        expand=True,
        prefix_icon=I.PERSON,
    )
    password = ft.TextField(
        label="Senha",
        password=True,
        can_reveal_password=True,
        expand=True,
        prefix_icon=I.LOCK,
    )
    remember = ft.Checkbox(label="Lembrar credenciais", value=False)

    # --- Actions
    login_btn = ft.FilledButton("Entrar", icon=I.CHEVRON_RIGHT, on_click=do_login, expand=True)
    forgot_btn = ft.TextButton("Esqueci minha senha", icon=I.HELP_OUTLINE, on_click=lambda e: toast("Fluxo de recuperação (demo)."))
    create_admin_btn = ft.TextButton("Primeiro acesso? Criar administrador", icon=I.ADMIN_PANEL_SETTINGS, on_click=go_create_admin)

    # --- Card (centered)
    card = ft.Card(
        elevation=12,
        clip_behavior=ft.ClipBehavior.NONE,
        content=ft.Container(
            width=460,
            padding=24,
            border_radius=ft.border_radius.all(18),
            content=ft.Column(
                [
                    header,                       # <- strict centered header
                    ft.Divider(color=C.GREY_800),
                    user,
                    password,
                    remember,
                    login_btn,
                    ft.Row([forgot_btn], alignment=ft.MainAxisAlignment.CENTER),
                    ft.Divider(color=C.GREY_900),
                    ft.Row([create_admin_btn], alignment=ft.MainAxisAlignment.CENTER),
                ],
                spacing=12,
                tight=True,
            ),
        ),
    )

    # --- Backdrop
    backdrop = ft.Container(
        expand=True,
        gradient=ft.LinearGradient(
            begin=ft.alignment.top_left,
            end=ft.alignment.bottom_right,
            colors=[C.BLUE_GREY_900, C.BLACK],
        ),
    )

    # --- Center everything
    root = ft.Stack(
        controls=[
            backdrop,
            ft.Container(content=card, alignment=ft.alignment.center, expand=True, padding=20),
        ]
    )

    page.add(root)

if __name__ == "__main__":
    ft.app(target=login_demo)
