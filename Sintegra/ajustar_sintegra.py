"""
Ajustador de SINTEGRA (Registros 54 e 50) com interface Flet.

Registro 54
- Corrige alíquota no final da linha:
  "4000" -> "0400"
  "7000" -> "0700"

Registro 50 (IE) — SEM mexer no layout (sem deslocar campos)
- Ajusta apenas o conteúdo do campo IE, mantendo as posições do Registro 50.
- Regra MG (única) conforme solicitado:
  - SOMENTE quando UF=MG
  - remove pontuação
  - IE com 13 posições úteis:
      * se numérica e < 13 dígitos => completa com zeros à esquerda até 13
      * se vazia => "ISENTO"
      * alinha à esquerda e completa com espaços até 13
  - mantém o layout do arquivo (campo IE no Registro 50 com 14 posições no padrão do Convênio),
    gravando: IE13 + 1 espaço (14ª posição)

IMPORTANTE:
- Se o switch "Regra MG (UF=MG + zeros)" estiver DESMARCADO, NÃO é feita nenhuma alteração de IE.

UF no Registro 50
- Detecta automaticamente UF validando Data(8 dígitos)+UF(2 letras).
  Suporta:
    Layout A: data idx 30:38, UF idx 38:40
    Layout B: data idx 29:37, UF idx 37:39

Compatibilidade Flet
- Não usa ft.colors / ft.icons diretamente.
- Não usa ft.asyncio; usa asyncio.sleep().
- Não usa page.call_from_thread.

Requisitos:
    pip install flet
"""

from __future__ import annotations

import os
import re
import threading
import traceback
import queue
import asyncio
from dataclasses import dataclass
from typing import Optional, Dict, Tuple

import flet as ft


# -----------------------------
# Compat shim (ft.Icons/ft.icons)
# -----------------------------
def _get_icons():
    if hasattr(ft, "Icons"):
        return ft.Icons
    if hasattr(ft, "icons"):
        return ft.icons
    return None


I = _get_icons()


# -----------------------------
# Helpers SINTEGRA
# -----------------------------
IE_CONTENT_LEN_MG = 13  # FAQ SEF/MG


def _strip_eol(line: str) -> Tuple[str, str]:
    """Returns (body, eol) preserving original EOL."""
    if line.endswith("\r\n"):
        return line[:-2], "\r\n"
    if line.endswith("\n"):
        return line[:-1], "\n"
    if line.endswith("\r"):
        return line[:-1], "\r"
    return line, ""


def _fix_reg54_aliquota(line_body: str, mapping: Dict[str, str]) -> Tuple[str, bool]:
    """Registro 54: alíquota do ICMS é um campo de 4 dígitos no final do registro."""
    if len(line_body) < 4:
        return line_body, False
    old = line_body[-4:]
    new = mapping.get(old)
    if new and len(new) == 4:
        return line_body[:-4] + new, True
    return line_body, False


_IE_SAN_RE = re.compile(r"[^0-9A-Za-z]")


def _sanitize_ie_to_13_mg(ie_raw: str) -> str:
    """
    Converte IE para o padrão MG (13 posições úteis):
    - remove pontuação
    - upper
    - se vazio: ISENTO
    - se numérica e < 13: zfill até 13
    - trunca em 13 se exceder
    - alinha à esquerda e completa com espaços até 13
    """
    ie = (ie_raw or "").strip()
    ie = _IE_SAN_RE.sub("", ie)
    ie = ie.upper()

    if not ie:
        ie = "ISENTO"

    if ie.isdigit() and len(ie) < IE_CONTENT_LEN_MG:
        ie = ie.zfill(IE_CONTENT_LEN_MG)

    ie = ie[:IE_CONTENT_LEN_MG]
    ie = ie.ljust(IE_CONTENT_LEN_MG)
    return ie


@dataclass
class Reg50Layout:
    """Campo IE no layout padrão do Registro 50: idx 16:30 (14 chars)."""
    ie_start: int = 16
    ie_end: int = 30  # exclusivo => 14 chars


REG50_STD = Reg50Layout()


def _detect_reg50_uf(line_body: str) -> Optional[str]:
    """
    Detecta UF em Registro 50 validando Data(8 dígitos)+UF(2 letras).
    Suporta:
    - Layout A: data idx 30:38, UF idx 38:40
    - Layout B: data idx 29:37, UF idx 37:39
    """
    # Layout A
    if len(line_body) >= 40:
        d = line_body[30:38]
        uf = line_body[38:40]
        if d.isdigit() and uf.isalpha():
            return uf.strip().upper() or None

    # Layout B (-1)
    if len(line_body) >= 39:
        d = line_body[29:37]
        uf = line_body[37:39]
        if d.isdigit() and uf.isalpha():
            return uf.strip().upper() or None

    return None


def _fix_reg50_ie_keep_layout_mg(line_body: str) -> Tuple[str, bool, Optional[str]]:
    """
    Ajusta IE do Registro 50 mantendo layout/posições (NÃO desloca o restante da linha),
    aplicando a regra MG SOMENTE quando UF=MG.

    Retorna (new_body, changed, uf_detected)
    """
    if len(line_body) < REG50_STD.ie_end:
        return line_body, False, None

    uf = _detect_reg50_uf(line_body)
    if uf != "MG":
        return line_body, False, uf

    ie_raw_14 = line_body[REG50_STD.ie_start:REG50_STD.ie_end]
    ie13 = _sanitize_ie_to_13_mg(ie_raw_14)
    ie14 = ie13 + " "  # mantém 14 chars do layout

    if ie_raw_14 == ie14:
        return line_body, False, uf

    new_body = line_body[:REG50_STD.ie_start] + ie14 + line_body[REG50_STD.ie_end:]
    return new_body, True, uf


# -----------------------------
# Processing worker
# -----------------------------
@dataclass
class ProcessingOptions:
    fix_reg54: bool
    reg50_mg_enabled: bool  # único switch para IE
    count_lines: bool
    encoding: str
    output_dir: Optional[str]


@dataclass
class ProcessingStats:
    total_lines: int = 0
    changed_lines: int = 0
    changed_reg54: int = 0
    changed_reg50: int = 0
    kept_lines: int = 0
    reg50_skipped_non_mg: int = 0  # somente quando switch ligado


def process_sintegra_file(
    input_path: str,
    options: ProcessingOptions,
    q: "queue.Queue[tuple]",
) -> Tuple[str, ProcessingStats]:
    stats = ProcessingStats()

    if not os.path.isfile(input_path):
        raise FileNotFoundError(f"Arquivo não encontrado: {input_path}")

    base_dir = os.path.dirname(os.path.abspath(input_path))
    out_dir = options.output_dir or base_dir
    os.makedirs(out_dir, exist_ok=True)

    name, ext = os.path.splitext(os.path.basename(input_path))
    out_path = os.path.join(out_dir, f"{name}_AJUSTADO{ext or '.txt'}")

    reg54_map = {"4000": "0400", "7000": "0700"}

    total = None
    if options.count_lines:
        q.put(("log", "Contando linhas para barra de progresso..."))
        with open(input_path, "r", encoding=options.encoding, errors="replace") as f:
            total = sum(1 for _ in f)
        q.put(("log", f"Total de linhas: {total}"))

    q.put(("log", f"Processando: {input_path}"))
    q.put(("log", f"Saída: {out_path}"))
    q.put(("log", f"Reg 50 Regra MG (UF=MG + zeros): {'ON' if options.reg50_mg_enabled else 'OFF'}"))
    q.put(("progress", 0, total))

    processed = 0

    with open(input_path, "r", encoding=options.encoding, errors="replace", newline="") as fin, open(
        out_path, "w", encoding=options.encoding, errors="replace", newline=""
    ) as fout:
        for raw_line in fin:
            body, eol = _strip_eol(raw_line)
            stats.total_lines += 1
            changed = False

            rec = body[:2]

            if options.fix_reg54 and rec == "54":
                new_body, did = _fix_reg54_aliquota(body, reg54_map)
                if did:
                    body = new_body
                    changed = True
                    stats.changed_reg54 += 1

            # IE somente se o switch estiver ligado
            if options.reg50_mg_enabled and rec == "50":
                new_body, did, uf = _fix_reg50_ie_keep_layout_mg(body)
                if uf is not None and uf != "MG":
                    stats.reg50_skipped_non_mg += 1
                if did:
                    body = new_body
                    changed = True
                    stats.changed_reg50 += 1

            if changed:
                stats.changed_lines += 1
            else:
                stats.kept_lines += 1

            fout.write(body + eol)

            processed += 1
            if processed % 500 == 0:
                q.put(("progress", processed, total))
                q.put(("log", f"Linhas processadas: {processed}"))

    q.put(("progress", processed, total))
    q.put(("log", "Processamento concluído."))
    q.put(("done", out_path, stats))
    return out_path, stats


# -----------------------------
# Flet App
# -----------------------------
def main(page: ft.Page):
    page.title = "Ajustador SINTEGRA (Reg 54 e Reg 50 IE MG)"
    page.window_width = 1020
    page.window_height = 840
    page.scroll = ft.ScrollMode.AUTO

    selected_file = {"path": None}
    selected_output_dir = {"path": None}
    worker_running = {"running": False}

    q: "queue.Queue[tuple]" = queue.Queue()

    txt_file = ft.Text(value="Nenhum arquivo selecionado.")
    txt_outdir = ft.Text(value="(Padrão: mesma pasta do arquivo)")

    chk_count_lines = ft.Checkbox(label="Contar linhas (progresso preciso)", value=True)

    dd_encoding = ft.Dropdown(
        label="Encoding do arquivo",
        value="latin-1",
        options=[
            ft.dropdown.Option("latin-1"),
            ft.dropdown.Option("utf-8"),
            ft.dropdown.Option("cp1252"),
        ],
        width=220,
    )

    sw_fix_reg54 = ft.Switch(
        label="Ajustar alíquota no Registro 54 (4000→0400; 7000→0700)",
        value=True,
    )

    # ÚNICO switch para IE (se OFF, não altera IE)
    sw_reg50_mg = ft.Switch(
        label="Regra MG no Registro 50 (somente UF=MG + IE numérica com zeros à esquerda até 13)",
        value=True,
    )

    pb = ft.ProgressBar(value=0.0, width=720)
    txt_progress = ft.Text(value="Aguardando...")

    log_box = ft.TextField(
        label="Log",
        multiline=True,
        min_lines=12,
        max_lines=12,
        read_only=True,
        expand=True,
    )

    btn_process = ft.ElevatedButton(text="Processar", disabled=True)

    file_picker = ft.FilePicker()
    dir_picker = ft.FilePicker()
    page.overlay.extend([file_picker, dir_picker])

    def _append_log(msg: str):
        if not msg:
            return
        current = log_box.value or ""
        new_val = (current + ("\n" if current else "") + msg)
        if len(new_val) > 120_000:
            new_val = new_val[-120_000:]
        log_box.value = new_val

    def _set_processing(is_processing: bool):
        worker_running["running"] = is_processing
        btn_process.disabled = is_processing or (not selected_file["path"])
        btn_pick_file.disabled = is_processing
        btn_pick_outdir.disabled = is_processing
        sw_fix_reg54.disabled = is_processing
        sw_reg50_mg.disabled = is_processing
        chk_count_lines.disabled = is_processing
        dd_encoding.disabled = is_processing

        if is_processing:
            txt_progress.value = "Processando..."
        page.update()

    def _set_progress(processed: int, total: Optional[int]):
        if total and total > 0:
            pb.value = min(1.0, max(0.0, processed / total))
            txt_progress.value = f"Processando: {processed}/{total}"
        else:
            pb.value = None
            txt_progress.value = f"Processando: {processed} linhas"
        page.update()

    def on_file_result(e: ft.FilePickerResultEvent):
        if e.files and len(e.files) > 0:
            selected_file["path"] = e.files[0].path
            txt_file.value = selected_file["path"]
            btn_process.disabled = worker_running["running"] or (not selected_file["path"])
            _append_log(f"Arquivo selecionado: {selected_file['path']}")
            page.update()

    def on_dir_result(e: ft.FilePickerResultEvent):
        if e.path:
            selected_output_dir["path"] = e.path
            txt_outdir.value = e.path
            _append_log(f"Pasta de saída: {e.path}")
            page.update()

    file_picker.on_result = on_file_result
    dir_picker.on_result = on_dir_result

    def pick_file(_):
        file_picker.pick_files(
            allow_multiple=False,
            dialog_title="Selecione o arquivo SINTEGRA (TXT)",
            file_type=ft.FilePickerFileType.CUSTOM,
            allowed_extensions=["txt", "TXT"],
        )

    def pick_outdir(_):
        dir_picker.get_directory_path(dialog_title="Selecione a pasta de saída")

    btn_pick_file = ft.ElevatedButton(
        text="Selecionar arquivo",
        icon=I.FOLDER_OPEN if I else None,
        on_click=pick_file,
    )

    btn_pick_outdir = ft.OutlinedButton(
        text="Selecionar pasta de saída (opcional)",
        icon=I.DRIVE_FOLDER_UPLOAD if I else None,
        on_click=pick_outdir,
    )

    def worker():
        try:
            opts = ProcessingOptions(
                fix_reg54=bool(sw_fix_reg54.value),
                reg50_mg_enabled=bool(sw_reg50_mg.value),
                count_lines=bool(chk_count_lines.value),
                encoding=str(dd_encoding.value),
                output_dir=selected_output_dir["path"],
            )
            process_sintegra_file(selected_file["path"], opts, q)
        except Exception:
            q.put(("error", traceback.format_exc()))

    def start_processing(_):
        if worker_running["running"]:
            return
        if not selected_file["path"]:
            return

        log_box.value = ""
        pb.value = 0.0
        txt_progress.value = "Iniciando..."
        page.update()

        _set_processing(True)

        if hasattr(page, "run_thread"):
            page.run_thread(worker)
        else:
            t = threading.Thread(target=worker, daemon=True)
            t.start()

    btn_process.on_click = start_processing

    async def pump_queue():
        while True:
            try:
                ev = q.get_nowait()
            except queue.Empty:
                await asyncio.sleep(0.10)
                continue

            etype = ev[0]

            if etype == "log":
                _append_log(str(ev[1]))
                page.update()

            elif etype == "progress":
                processed, total = ev[1], ev[2]
                _set_progress(int(processed), None if total is None else int(total))

            elif etype == "done":
                out_path, stats = ev[1], ev[2]
                _append_log("")
                _append_log("Resumo:")
                _append_log(f"  Total de linhas: {stats.total_lines}")
                _append_log(f"  Alteradas: {stats.changed_lines}")
                _append_log(f"    - Registro 54 (alíquota): {stats.changed_reg54}")
                _append_log(f"    - Registro 50 (IE MG): {stats.changed_reg50}")
                if bool(sw_reg50_mg.value):
                    _append_log(f"    - Registro 50 ignorados (UF != MG): {stats.reg50_skipped_non_mg}")
                _append_log(f"  Mantidas: {stats.kept_lines}")
                _append_log("")
                _append_log(f"Arquivo gerado: {out_path}")

                _set_processing(False)
                page.snack_bar = ft.SnackBar(content=ft.Text("Concluído! Arquivo ajustado gerado."), open=True)
                page.update()

            elif etype == "error":
                _append_log("ERRO ao processar:")
                _append_log(str(ev[1]))
                _set_processing(False)
                page.snack_bar = ft.SnackBar(content=ft.Text("Erro no processamento. Veja o log."), open=True)
                page.update()

    if hasattr(page, "run_task"):
        page.run_task(pump_queue)
    else:
        try:
            asyncio.get_running_loop().create_task(pump_queue())
        except Exception:
            _append_log("Aviso: não foi possível iniciar a task de UI (atualize o Flet).")
            page.update()

    page.add(
        ft.Column(
            controls=[
                ft.Text("Ajustador SINTEGRA", size=22, weight=ft.FontWeight.BOLD),
                ft.Text(
                    "Corrige: (1) alíquota do Registro 54; (2) IE do Registro 50 conforme regra MG "
                    "(somente UF=MG, sem mexer no layout). Se o switch de MG estiver desligado, não altera IE.",
                    size=12,
                ),
                ft.Divider(),
                ft.Row(
                    controls=[
                        btn_pick_file,
                        ft.Container(width=10),
                        ft.Column([ft.Text("Arquivo selecionado:"), txt_file], expand=True),
                    ],
                    vertical_alignment=ft.CrossAxisAlignment.START,
                ),
                ft.Row(
                    controls=[
                        btn_pick_outdir,
                        ft.Container(width=10),
                        ft.Column([ft.Text("Pasta de saída:"), txt_outdir], expand=True),
                    ],
                    vertical_alignment=ft.CrossAxisAlignment.START,
                ),
                ft.Divider(),
                ft.Row(controls=[dd_encoding, ft.Container(width=12), chk_count_lines], wrap=True),
                ft.Column(
                    controls=[
                        sw_fix_reg54,
                        sw_reg50_mg,
                    ],
                    spacing=4,
                ),
                ft.Divider(),
                ft.Row(
                    controls=[
                        btn_process,
                        ft.Container(width=16),
                        pb,
                        ft.Container(width=12),
                        txt_progress,
                    ],
                    wrap=True,
                    vertical_alignment=ft.CrossAxisAlignment.CENTER,
                ),
                ft.Container(height=10),
                log_box,
            ],
            spacing=10,
        )
    )

    btn_process.disabled = True
    page.update()


if __name__ == "__main__":
    ft.app(target=main)