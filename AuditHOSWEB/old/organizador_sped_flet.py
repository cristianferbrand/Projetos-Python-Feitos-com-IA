# -*- coding: utf-8 -*-
"""
Organizador de Arquivos SPED (Flet)
----------------------------------------------------------------------------------

Recursos desta versão:
- Detecta automaticamente se o arquivo é EFD **Fiscal** ou **Contribuições** (detector robusto).
- Cria, se necessário, as subpastas: **FISCAL**, **CONTRIBUIÇÕES**, **INDETERMINADO**, **CONFLITANTE**.
- **Subpasta por cliente**: dentro do tipo, cria **<CRM - FANTASIA>** (com CSV opcional `clientes_hos.csv`).
  - Se não achar o cliente pelo CNPJ do 0000, usa a subpasta **SEM CLIENTE**.
- **Monitor automático**: quando ativado, já processa imediatamente os .txt soltos na pasta destino
  e continua monitorando periodicamente.
- Atualização opcional da versão do **0000** (somente Fiscal).

Como executar:
    python organizador_sped_flet.py
"""
from __future__ import annotations
import os
import re
import sys
import csv
import shutil
import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set

import flet as ft

# -------------------------------------------------------------
# Compat shim: Colors / Icons (C / I) e helper op(opacity, color)
# -------------------------------------------------------------
try:
    from flet import Colors as _Colors
    C = _Colors
except Exception:
    from flet import colors as _colors
    C = _colors  # fallback

try:
    from flet import Icons as _Icons
    I = _Icons
except Exception:
    from flet import icons as _icons
    I = _icons  # fallback


def op(alpha: float, color: str) -> str:
    """Devolve a cor com opacidade quando suportado; caso contrário retorna a cor original."""
    try:
        return ft.colors.with_opacity(alpha, color)
    except Exception:
        try:
            return ft.Colors.with_opacity(alpha, color)  # type: ignore[attr-defined]
        except Exception:
            return color

# -------------------------------------------------------------
# Utilidades, parsing e DETECÇÃO DE TIPO
# -------------------------------------------------------------
BASE_DIR = Path(sys.executable).parent if getattr(sys, "frozen", False) else Path(__file__).parent
CSV_NAME = "clientes_hos.csv"
CSV_PATH = BASE_DIR / CSV_NAME


@dataclass
class Cliente:
    cnpj: str
    crm: str
    fantasia: str


def remover_mascara_cnpj(cnpj: str) -> str:
    return re.sub(r"\D", "", str(cnpj or "")).zfill(14)


def carregar_clientes(csv_path: Path) -> Dict[str, Cliente]:
    """Lê `clientes_hos.csv` (delimitador ';', UTF-8) e retorna dict por CNPJ limpo.
       O CSV é OPCIONAL (apenas para nomear a subpasta <CRM - FANTASIA>)."""
    mapa: Dict[str, Cliente] = {}
    if not csv_path.exists():
        return mapa
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter=';')
        if not reader.fieldnames:
            return mapa
        field_map = {k.lower(): k for k in reader.fieldnames}
        get = lambda row, key: row.get(field_map.get(key, key), "")
        for row in reader:
            cnpj = remover_mascara_cnpj(get(row, "cnpj"))
            if not cnpj:
                continue
            mapa[cnpj] = Cliente(
                cnpj=cnpj,
                crm=str(get(row, "crm")).strip(),
                fantasia=str(get(row, "fantasia")).strip(),
            )
    return mapa


def ler_primeira_linha(path: Path) -> str:
    with path.open("r", encoding="latin-1", errors="ignore") as f:
        return f.readline().strip()


# --- Registros-âncora exclusivos por obrigação ---
UNIQUE_CONTRIB = {
    # Bloco A (serviços) / Bloco M (apuração PIS/COFINS) / Bloco P (financeiro específico)
    "A001","A100","A170",
    "M001","M100","M105","M200","M205","M210","M400","M410","M800","M810",
    "P001","P100","P200","P500"
}
UNIQUE_FISCAL = {
    # Bloco E (apuração ICMS), Bloco H (inventário), Bloco K (produção), Bloco G (CIAP)
    "E001","E100","E110","E111","E116","E200","E210","E250",
    "H001","H005","H010","H020",
    "K001","K100","K200","K280","K291","K292","K300","K301","K310","K315",
    "G001","G110","G130","G140"
}


def _parse_registro(line: str) -> Optional[str]:
    """
    Retorna o código do registro (ex.: 'C100', 'M100') a partir de uma linha SPED.
    Robusto contra BOM/espacos no início e quebras no fim.
    """
    if not line:
        return None
    # remove BOM e espaços à esquerda; remove quebras no fim
    line = line.lstrip("\ufeff \t").rstrip("\r\n")
    if not line or not line.startswith("|"):
        return None
    # split rápido, sem regex
    try:
        reg = line.split("|", 2)[1]
    except Exception:
        return None
    if not reg:
        return None
    return reg.upper()


def detectar_efd_kind(path: Path, max_lines: int = 2_000_000) -> Tuple[str, Optional[str]]:
    """
    Retorna: (tipo, ancora)
      - tipo: "fiscal", "contribuicoes", "indeterminado" ou "conflitante"
      - ancora: primeiro registro-âncora que determinou o tipo (ex.: 'M100', 'E110'), se houver
    Estratégia: varre em streaming e para na 1ª âncora exclusiva encontrada.
    """
    found_contrib = False
    found_fiscal = False
    with path.open("r", encoding="latin-1", errors="ignore") as f:
        for i, raw in enumerate(f, start=1):
            if i > max_lines:
                break
            reg = _parse_registro(raw)
            if not reg:
                continue
            if reg in UNIQUE_CONTRIB:
                if found_fiscal:
                    return "conflitante", None
                return "contribuicoes", reg
            if reg in UNIQUE_FISCAL:
                if found_contrib:
                    return "conflitante", None
                return "fiscal", reg
            # Flags (fallback, caso não haja âncoras explícitas)
            if reg[0] in ("M","A","P"):
                found_contrib = True
            elif reg[0] in ("E","H","K","G"):
                found_fiscal = True
    # Fallbacks fracos
    if found_contrib and not found_fiscal:
        return "contribuicoes", None
    if found_fiscal and not found_contrib:
        return "fiscal", None
    return "indeterminado", None


def extrair_cnpj_e_versao(linha0000: str, tipo: str) -> Tuple[Optional[str], Optional[str]]:
    """Retorna (cnpj, versao) para fiscal; (cnpj, None) para contribuições."""
    if not linha0000:
        return None, None
    # Normaliza a linha 0000
    linha0000 = linha0000.lstrip("\ufeff \t").rstrip("\r\n")
    if not (linha0000.startswith("|0000|")):
        return None, None

    if tipo == "fiscal":
        # |0000|VVV|...|NOME|CNPJ|
        m = re.search(r"\|0000\|(\d{3})\|\d\|\d+\|\d+\|[^|]*\|(\d{14})\|", linha0000)
        if m:
            return m.group(2), m.group(1)
        return None, None
    else:
        # contribuições: |0000|..|..|..|..|..|..|NOME|CNPJ|
        m = re.search(r"\|0000\|\d+\|\d\|\|\|\d+\|\d+\|[^|]*\|(\d{14})\|", linha0000)
        if m:
            return m.group(1), None
        return None, None


def extrair_cnpj_auto(linha0000: str) -> Tuple[Optional[str], Optional[str], str]:
    """Tenta Fiscal; se falhar, tenta Contribuições. Retorna (cnpj, versao, tipo_inferido)."""
    cnpj, ver = extrair_cnpj_e_versao(linha0000, "fiscal")
    if cnpj:
        return cnpj, ver, "fiscal"
    cnpj, ver = extrair_cnpj_e_versao(linha0000, "contribuicoes")
    if cnpj:
        return cnpj, None, "contribuicoes"
    return None, None, "indeterminado"


def atualizar_versao_fiscal_no_arquivo(path: Path, nova_versao: str) -> bool:
    """Troca os 3 dígitos de versão do 0000 na 1ª linha do arquivo fiscal. Retorna True se alterado."""
    with path.open("r", encoding="latin-1", errors="ignore") as f:
        linhas = f.readlines()
    if not linhas:
        return False
    original = linhas[0]
    atualizado = re.sub(r"(\|0000\|)\d{3}(\|)", rf"\g<1>{nova_versao}\g<2>", original)
    if atualizado != original:
        linhas[0] = atualizado
        with path.open("w", encoding="latin-1", newline="") as f:
            f.writelines(linhas)
        return True
    return False


# ---------- Pastas por TIPO e Cliente ----------
TIPO_DIR_MAP = {
    "fiscal": "FISCAL",
    "contribuicoes": "CONTRIBUIÇÕES",
    "indeterminado": "INDETERMINADO",
    "conflitante": "CONFLITANTE",
}

SEM_CLIENTE_DIR = "SEM CLIENTE"  # usado quando não encontra CNPJ no CSV

def garantir_subpastas_tipo(base: Path) -> None:
    for nome in ("FISCAL", "CONTRIBUIÇÕES", "INDETERMINADO", "CONFLITANTE"):
        (base / nome).mkdir(parents=True, exist_ok=True)

def safe_folder_name(name: str) -> str:
    """Sanitiza nome de pasta para Windows/Linux (remove caracteres inválidos e espaços finais)."""
    if not name:
        return SEM_CLIENTE_DIR
    cleaned = re.sub(r'[<>:"/\\|?*]+', ' ', name)
    cleaned = re.sub(r'\s{2,}', ' ', cleaned).strip().strip('.')
    return cleaned or SEM_CLIENTE_DIR

def resolver_pasta_destino(destino_base: Path,
                           tipo_detectado: str,
                           cnpj_clean: str,
                           mapa_clientes: Dict[str, Cliente]) -> Tuple[Path, str]:
    """Retorna (pasta_final, label_relativa_exibida). Cria pastas se necessário."""
    tipo_dir = TIPO_DIR_MAP.get(tipo_detectado, "INDETERMINADO")
    pasta_tipo = destino_base / tipo_dir
    pasta_tipo.mkdir(parents=True, exist_ok=True)

    if cnpj_clean and mapa_clientes and cnpj_clean in mapa_clientes:
        cli = mapa_clientes[cnpj_clean]
        cliente_dirname = safe_folder_name(f"{cli.crm} - {cli.fantasia}")
    else:
        cliente_dirname = SEM_CLIENTE_DIR

    pasta_cliente = pasta_tipo / cliente_dirname
    pasta_cliente.mkdir(parents=True, exist_ok=True)

    label_rel = f"{tipo_dir} / {cliente_dirname}"
    return pasta_cliente, label_rel


# -------------------------------------------------------------
# UI (Flet) + Monitor automático
# -------------------------------------------------------------
class OrganizadorSPED:
    def __init__(self, page: ft.Page):
        self.page = page
        self.mapa_clientes: Dict[str, Cliente] = {}
        self.destino: Optional[Path] = None
        self.arquivos: List[Path] = []
        self.tipos: List[str] = []            # tipo textual
        self.ancoras: List[Optional[str]] = [] # primeira âncora encontrada (se houver)

        # Monitor
        self.sw_auto = ft.Switch(label="Monitorar automaticamente a pasta destino", value=False, on_change=self._toggle_auto)
        self.tf_interval = ft.TextField(value="3", width=90, label="Intervalo (s)")
        self.monitor_task: Optional[asyncio.Task] = None
        self.monitor_enabled: bool = False
        self.seen: Set[str] = set()

        # File/Dir pickers
        self.fp_files = ft.FilePicker(on_result=self._on_files)
        self.fp_dir = ft.FilePicker(on_result=self._on_dir)

        # Banner
        self.banner = ft.Banner(
            bgcolor=op(0.07, C.BLUE_900 if hasattr(C, 'BLUE_900') else C.BLUE),
            leading=ft.Icon(I.WARNING_AMBER_ROUNDED),
            content=ft.Text(
                f"CSV {CSV_NAME} é opcional. Se presente, criará subpastas <CRM - FANTASIA>.",
                size=13,
            ),
            actions=[ft.TextButton("Entendi", on_click=lambda e: self._close_banner())],
        )

        self.lbl_modo = ft.Text(
            "Detecção automática (FISCAL / CONTRIBUIÇÕES) com âncora e subpasta <CRM - FANTASIA>.",
            size=12,
            italic=True,
        )

        # Atualização de versão (apenas para Fiscal)
        self.cb_atualizar = ft.Checkbox(
            value=True,
            label="Atualizar versão do registro 0000 (apenas se Fiscal)",
            tooltip="Se marcado, arquivos fiscais com versão diferente serão atualizados antes de mover.",
        )
        self.tf_versao = ft.TextField(value="019", width=90, label="Versão")

        self.lbl_csv = ft.Text("—", size=12)
        self.lbl_destino = ft.Text("—", size=12)

        self.bt_pick_csv = ft.TextButton(
            "Recarregar CSV (opcional)",
            icon=I.REFRESH_ROUNDED,
            on_click=lambda e: self._carregar_csv(),
        )

        self.bt_pick_dir = ft.ElevatedButton(
            text="Escolher Pasta de Destino",
            icon=I.FOLDER_OPEN,
            on_click=lambda e: self.fp_dir.get_directory_path(),
        )

        self.bt_pick_files = ft.ElevatedButton(
            text="Selecionar Arquivos SPED (.txt)",
            icon=I.UPLOAD_FILE,
            on_click=lambda e: self.fp_files.pick_files(allow_multiple=True, allowed_extensions=["txt"]),
        )

        # Tabela
        self.rows: List[ft.DataRow] = []
        self.table = ft.DataTable(
            columns=[
                ft.DataColumn(ft.Text("Arquivo")),
                ft.DataColumn(ft.Text("Tipo")),
                ft.DataColumn(ft.Text("Âncora")),
                ft.DataColumn(ft.Text("CNPJ (0000)")),
                ft.DataColumn(ft.Text("Cliente (se CSV)")),
                ft.DataColumn(ft.Text("Pasta Destino")),
                ft.DataColumn(ft.Text("Status")),
            ],
            rows=self.rows,
            heading_row_color=op(0.05, C.BLUE),
            column_spacing=14,
            data_row_color={"hovered": op(0.03, C.BLUE)},
        )

        self.pb = ft.ProgressBar(width=400, visible=False)
        self.log = ft.Text("Pronto.", selectable=True, size=12)

        self.bt_process = ft.FilledButton(
            "Processar e Organizar (manual)",
            icon=I.PLAY_ARROW_ROUNDED,
            on_click=self._processar,
        )
        self.bt_limpar = ft.OutlinedButton("Limpar Lista", icon=I.CLEAR_ALL, on_click=self._limpar)
        self.bt_abrir_destino_btn = ft.OutlinedButton("Abrir Pasta de Destino", icon=I.FOLDER, on_click=self._abrir_destino)

    # --- Banner helpers ---
    def _open_banner(self):
        self.page.banner = self.banner
        self.page.banner.open = True
        self.page.update()

    def _close_banner(self):
        if getattr(self.page, "banner", None) is None:
            self.page.banner = self.banner
        self.page.banner.open = False
        self.page.update()

    # ------------- eventos -------------
    def _on_files(self, e: ft.FilePickerResultEvent):
        if e.files:
            self.arquivos = [Path(f.path) for f in e.files if f.path]
            tipos, ancoras = [], []
            for p in self.arquivos:
                t, a = detectar_efd_kind(p)
                tipos.append(t); ancoras.append(a)
            self.tipos = tipos
            self.ancoras = ancoras
            self._recarregar_tabela()

    def _on_dir(self, e: ft.FilePickerResultEvent):
        if e.path:
            self.destino = Path(e.path)
            # Cria as subpastas por tipo automaticamente
            try:
                garantir_subpastas_tipo(self.destino)
            except Exception:
                pass
            self.lbl_destino.value = str(self.destino)
            self.page.update()
            # se auto estiver ligado, inicia monitor e já faz varredura inicial
            if self.sw_auto.value:
                self._start_monitor(initial_scan=True)

    def _toggle_auto(self, e: ft.ControlEvent):
        if self.sw_auto.value:
            self._start_monitor(initial_scan=True)
        else:
            self._stop_monitor()

    # ------------- helpers UI -------------
    def _tipo_texto(self, tipo: str) -> str:
        return {
            "fiscal": "Fiscal",
            "contribuicoes": "Contribuições",
            "conflitante": "Conflitante",
            "indeterminado": "Indeterminado",
        }.get(tipo or "", "—")

    def _recarregar_tabela(self):
        self.rows.clear()
        for i, p in enumerate(self.arquivos):
            tipo_txt = self._tipo_texto(self.tipos[i] if i < len(self.tipos) else "")
            ancora = self.ancoras[i] if i < len(self.ancoras) else None
            self.rows.append(
                ft.DataRow(
                    cells=[
                        ft.DataCell(ft.Text(p.name)),
                        ft.DataCell(ft.Text(tipo_txt)),
                        ft.DataCell(ft.Text(ancora or "—")),
                        ft.DataCell(ft.Text("—")),
                        ft.DataCell(ft.Text("—")),
                        ft.DataCell(ft.Text("—")),
                        ft.DataCell(ft.Text("Pendente", color=C.GREY)),
                    ]
                )
            )
        self.table.rows = self.rows
        self.page.update()

    def _append_row_pending(self, path: Path, tipo: str, ancora: Optional[str]):
        """Adiciona uma linha 'Pendente' na tabela para um novo arquivo detectado pelo monitor."""
        self.arquivos.append(path)
        self.tipos.append(tipo)
        self.ancoras.append(ancora)
        self.rows.append(
            ft.DataRow(
                cells=[
                    ft.DataCell(ft.Text(path.name)),
                    ft.DataCell(ft.Text(self._tipo_texto(tipo))),
                    ft.DataCell(ft.Text(ancora or "—")),
                    ft.DataCell(ft.Text("—")),
                    ft.DataCell(ft.Text("—")),
                    ft.DataCell(ft.Text("—")),
                    ft.DataCell(ft.Text("Pendente", color=C.GREY)),
                ]
            )
        )
        self.table.rows = self.rows
        self.page.update()
        return len(self.rows) - 1  # index da linha

    def _set_row(self, idx: int, tipo_txt: str, ancora: Optional[str], cnpj: str, cliente_txt: str, pasta: str, status: str, ok: bool):
        self.rows[idx] = ft.DataRow(
            cells=[
                ft.DataCell(ft.Text(self.arquivos[idx].name)),
                ft.DataCell(ft.Text(tipo_txt or "—")),
                ft.DataCell(ft.Text(ancora or "—")),
                ft.DataCell(ft.Text(cnpj or "—")),
                ft.DataCell(ft.Text(cliente_txt or "—")),
                ft.DataCell(ft.Text(pasta or "—")),
                ft.DataCell(ft.Text(status, color=C.GREEN if ok else C.RED)),
            ]
        )
        self.table.rows = self.rows
        self.page.update()

    def _carregar_csv(self):
        try:
            self.mapa_clientes = carregar_clientes(CSV_PATH)
            if self.mapa_clientes:
                self.lbl_csv.value = f"CSV carregado ({len(self.mapa_clientes)} clientes) → {CSV_PATH}"
            else:
                self.lbl_csv.value = f"CSV não encontrado ou vazio (opcional): {CSV_PATH}"
        except Exception as ex:
            self.lbl_csv.value = f"Erro ao carregar CSV: {ex}"
        self.page.update()

    # ---------------- Monitor automático ----------------
    def _get_interval(self) -> float:
        try:
            v = float(self.tf_interval.value)
            return max(0.5, min(v, 60.0))
        except Exception:
            return 3.0

    def _start_monitor(self, initial_scan: bool = False):
        if not self.destino:
            self.log.value = "Escolha a pasta de destino para iniciar o monitor."
            self.page.update()
            return
        if self.monitor_task and not self.monitor_task.done():
            # já rodando
            if initial_scan:
                # ainda assim, roda uma varredura inicial
                self.page.run_task(self._scan_once)
            return
        self.monitor_enabled = True
        self.log.value = f"Monitor automático ATIVO (intervalo {self._get_interval():.1f}s)."
        self.page.update()
        # Varredura inicial IMEDIATA (processa .txt soltos já existentes)
        if initial_scan:
            self.page.run_task(self._scan_once)
        # Loop contínuo
        self.monitor_task = self.page.run_task(self._monitor_loop)

    def _stop_monitor(self):
        self.monitor_enabled = False
        if self.monitor_task and not self.monitor_task.done():
            try:
                self.monitor_task.cancel()
            except Exception:
                pass
        self.log.value = "Monitor automático DESLIGADO."
        self.page.update()

    async def _monitor_loop(self):
        try:
            while self.monitor_enabled:
                await self._scan_once()
                await asyncio.sleep(self._get_interval())
        except asyncio.CancelledError:
            pass

    async def _scan_once(self):
        if not self.destino:
            return
        try:
            # somente arquivos diretamente na pasta destino (não recursivo)
            files = [p for p in self.destino.iterdir() if p.is_file() and p.suffix.lower() == ".txt"]
        except Exception:
            return
        for p in files:
            sp = str(p)
            if sp in self.seen:
                continue
            # aguarda estabilizar tamanho (arquivo ainda salvando?)
            try:
                s1 = p.stat().st_size
                await asyncio.sleep(0.2)
                s2 = p.stat().st_size
                if s1 != s2:
                    continue  # ainda gravando; tenta no próximo ciclo
            except Exception:
                continue
            # processa
            await self._process_single_auto(p)
            self.seen.add(sp)

    async def _process_single_auto(self, arquivo: Path):
        # Detecta tipo / ancora
        tipo_detectado, ancora = detectar_efd_kind(arquivo)
        idx = self._append_row_pending(arquivo, tipo_detectado, ancora)
        tipo_txt = self._tipo_texto(tipo_detectado)

        # 0000
        linha0 = ler_primeira_linha(arquivo)
        if tipo_detectado in ("fiscal", "contribuicoes"):
            cnpj, versao = extrair_cnpj_e_versao(linha0, tipo_detectado)
        else:
            cnpj, versao, tipo_inferido = extrair_cnpj_auto(linha0)
            if tipo_detectado == "indeterminado" and tipo_inferido in ("fiscal","contribuicoes"):
                tipo_detectado = tipo_inferido
                tipo_txt = self._tipo_texto(tipo_detectado)

        cnpj_clean = remover_mascara_cnpj(cnpj) if cnpj else ""
        cliente_txt = "—"
        if cnpj_clean and self.mapa_clientes:
            cli = self.mapa_clientes.get(cnpj_clean)
            if cli:
                cliente_txt = f"{cli.crm} - {cli.fantasia}"

        # Atualizar versão (somente se fiscal)
        nova_versao = (self.tf_versao.value or "019").strip()
        if self.cb_atualizar.value and tipo_detectado == "fiscal" and versao and re.fullmatch(r"\d{3}", nova_versao) and versao != nova_versao:
            try:
                atualizar_versao_fiscal_no_arquivo(arquivo, nova_versao)
            except Exception as ex:
                self._set_row(idx, tipo_txt, ancora, cnpj_clean or "—", cliente_txt, "—", f"Falha ao atualizar versão: {ex}", False)
                return

        # Pasta destino por TIPO → <CRM - FANTASIA>
        pasta_final, label_rel = resolver_pasta_destino(self.destino, tipo_detectado, cnpj_clean, self.mapa_clientes)

        # Mover
        destino_arquivo = pasta_final / arquivo.name
        try:
            shutil.move(str(arquivo), str(destino_arquivo))
            self._set_row(idx, tipo_txt, ancora, cnpj_clean or "—", cliente_txt, label_rel, "Movido com sucesso (auto)", True)
        except Exception as ex:
            self._set_row(idx, tipo_txt, ancora, cnpj_clean or "—", cliente_txt, label_rel, f"Erro ao mover: {ex}", False)

    # ------------- processamento manual (botão) -------------
    def _processar(self, e: ft.ControlEvent):
        if not self.arquivos:
            self.log.value = "Selecione arquivos SPED (.txt) ou use o monitor automático."
            self.page.update(); return
        if not self.destino:
            self.log.value = "Escolha a pasta de destino."
            self.page.update(); return

        atualizar = self.cb_atualizar.value
        nova_versao = (self.tf_versao.value or "019").strip()
        if atualizar and not re.fullmatch(r"\d{3}", nova_versao):
            self.log.value = "Versão inválida. Use 3 dígitos (ex.: 019)."
            self.page.update(); return

        if not self.mapa_clientes:
            self.lbl_csv.value = f"Sem CSV → criará subpasta '{SEM_CLIENTE_DIR}' quando não achar o cliente."
            self.page.update()

        self.pb.visible = True
        self.pb.value = 0
        self.log.value = "Processando..."
        self.page.update()

        total = len(self.arquivos)
        for idx, arquivo in enumerate(self.arquivos):
            try:
                tipo_detectado, ancora = detectar_efd_kind(arquivo)
                tipo_txt = self._tipo_texto(tipo_detectado)

                linha0 = ler_primeira_linha(arquivo)

                if tipo_detectado in ("fiscal", "contribuicoes"):
                    cnpj, versao = extrair_cnpj_e_versao(linha0, tipo_detectado)
                else:
                    cnpj, versao, tipo_inferido = extrair_cnpj_auto(linha0)
                    if tipo_detectado == "indeterminado" and tipo_inferido in ("fiscal","contribuicoes"):
                        tipo_detectado = tipo_inferido
                        tipo_txt = self._tipo_texto(tipo_detectado)

                cnpj_clean = remover_mascara_cnpj(cnpj) if cnpj else ""
                cliente_txt = "—"
                if cnpj_clean and self.mapa_clientes:
                    cli = self.mapa_clientes.get(cnpj_clean)
                    if cli:
                        cliente_txt = f"{cli.crm} - {cli.fantasia}"

                if atualizar and tipo_detectado == "fiscal" and versao and versao != nova_versao:
                    try:
                        atualizar_versao_fiscal_no_arquivo(arquivo, nova_versao)
                    except Exception as ex:
                        self._set_row(idx, tipo_txt, ancora, cnpj_clean or "—", cliente_txt, "—", f"Falha ao atualizar versão: {ex}", False)
                        continue

                # Pasta destino por TIPO → <CRM - FANTASIA>
                pasta_final, label_rel = resolver_pasta_destino(self.destino, tipo_detectado, cnpj_clean, self.mapa_clientes)

                destino_arquivo = pasta_final / arquivo.name
                try:
                    shutil.move(str(arquivo), str(destino_arquivo))
                    self._set_row(
                        idx,
                        tipo_txt,
                        ancora,
                        cnpj_clean or "—",
                        cliente_txt,
                        label_rel,
                        "Movido com sucesso",
                        True,
                    )
                except Exception as ex:
                    self._set_row(idx, tipo_txt, ancora, cnpj_clean or "—", cliente_txt, label_rel, f"Erro ao mover: {ex}", False)
            finally:
                self.pb.value = (idx + 1) / total
                self.page.update()

        self.pb.visible = False
        self.log.value = "Concluído."
        self.page.update()

    def _limpar(self, e: ft.ControlEvent):
        self.arquivos.clear()
        self.tipos.clear()
        self.ancoras.clear()
        self.rows.clear()
        self.table.rows = self.rows
        self.page.update()
        self.log.value = "Lista limpa."
        self.page.update()

    def _abrir_destino(self, e: ft.ControlEvent):
        if not self.destino:
            return
        try:
            if sys.platform.startswith("win"):
                os.startfile(self.destino)  # type: ignore[attr-defined]
            elif sys.platform == "darwin":
                os.system(f"open '{self.destino}'")
            else:
                os.system(f"xdg-open '{self.destino}'")
        except Exception:
            pass

    # ------------- build -------------
    def build(self) -> ft.Control:
        # Registra pickers na página
        self.page.overlay.extend([self.fp_files, self.fp_dir])

        header = ft.Row([
            ft.Icon(I.SNIPPET_FOLDER_ROUNDED, size=28, color=C.BLUE),
            ft.Text("Organizador de Arquivos SPED", size=20, weight=ft.FontWeight.BOLD),
        ], alignment=ft.MainAxisAlignment.START)

        csv_row = ft.Row([
            ft.Text("Status do CSV (opcional):"), self.lbl_csv, self.bt_pick_csv
        ], wrap=True)

        destino_row = ft.Row([
            self.bt_pick_dir, ft.Icon(I.ARROW_RIGHT_ALT), ft.Text("Destino:"), self.lbl_destino
        ], wrap=True)

        cfg_row = ft.Row([
            self.lbl_modo, ft.Container(width=20),
            self.cb_atualizar, self.tf_versao,
            ft.Container(width=20),
            self.sw_auto, self.tf_interval,
        ], wrap=True)

        actions = ft.Row([
            self.bt_pick_files,
            self.bt_process,
            self.bt_limpar,
            self.bt_abrir_destino_btn,
            self.pb,
        ], alignment=ft.MainAxisAlignment.START, wrap=True, run_spacing=8)

        layout = ft.Column([
            header,
            ft.Divider(),
            csv_row,
            destino_row,
            cfg_row,
            ft.Container(
                content=ft.Column([
                    ft.Row([self.table], scroll=ft.ScrollMode.AUTO),
                ], scroll=ft.ScrollMode.AUTO, expand=True),
                height=380,
                border=ft.border.all(1, op(0.15, C.BLUE)),
                border_radius=10,
                padding=10,
            ),
            actions,
            ft.Text("Log:"),
            ft.Container(
                content=ft.Column([self.log], scroll=ft.ScrollMode.AUTO, expand=True),
                height=140,
                padding=10,
                border_radius=8,
                bgcolor=op(0.03, C.BLUE),
            ),
        ], spacing=10, expand=True, scroll=ft.ScrollMode.AUTO)

        # Carrega CSV (opcional) ao abrir
        self._carregar_csv()
        return layout


# ========================
# ### INÍCIO main(page)
# ========================

def main(page: ft.Page):
    page.title = "AuditHOS • Organizador SPED"
    page.theme_mode = ft.ThemeMode.DARK
    page.window_width = 1100
    page.window_height = 740
    page.horizontal_alignment = ft.CrossAxisAlignment.START
    page.vertical_alignment = ft.MainAxisAlignment.START
    page.padding = 14

    app = OrganizadorSPED(page)
    page.add(app.build())


if __name__ == "__main__":
    ft.app(target=main)
# ========================
# ### FIM main(page)
# ========================
