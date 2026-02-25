#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Interface (GUI) para atualizar dhEmi e dhSaiEnt em XMLs em massa.

- Permite selecionar um arquivo XML ou uma pasta
- Permite informar:
    a) Datetime completo (ex.: 2025-12-18T15:03:00-03:00)
    OU
    b) Data + hora (+ offset opcional)
- Opções: recursivo, sobrescrever (inplace), backup .bak, saída em pasta, dry-run
- Preserva o XML como texto (não reserializa); altera apenas o conteúdo entre as tags.

Requisitos: Python 3.x (Tkinter normalmente já vem instalado).
Execução:
    python interface_atualiza_dhEmi_dhSaiEnt.py
"""

from __future__ import annotations

import re
import threading
import queue
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Tuple

import tkinter as tk
from tkinter import ttk, filedialog, messagebox

ENC_RE = re.compile(br'encoding=[\'"]([^\'"]+)[\'"]', re.I)

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
TIME_RE = re.compile(r"^\d{2}:\d{2}(:\d{2})?$")
OFFSET_RE = re.compile(r"^(Z|[+-]\d{2}:\d{2})$")

DT_FULL_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?(Z|[+-]\d{2}:\d{2})$")


def detect_encoding(raw: bytes) -> str:
    head = raw[:300]
    m = ENC_RE.search(head)
    if m:
        try:
            enc = m.group(1).decode("ascii", errors="ignore").strip()
            return enc or "utf-8"
        except Exception:
            pass
    return "utf-8"


def ensure_seconds(time_str: str) -> str:
    if re.fullmatch(r"\d{2}:\d{2}", time_str):
        return time_str + ":00"
    return time_str


def extract_offset(dt_value: str) -> Optional[str]:
    m = re.search(r"(Z|[+-]\d{2}:\d{2})$", dt_value)
    return m.group(1) if m else None


def normalize_datetime(dt: str) -> str:
    if not DT_FULL_RE.fullmatch(dt):
        raise ValueError("Datetime inválido. Ex.: 2025-12-18T15:03:00-03:00")
    # se vier sem segundos: YYYY-MM-DDTHH:MM-03:00 ou ...Z
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(Z|[+-]\d{2}:\d{2})", dt):
        off = extract_offset(dt) or "-03:00"
        base = dt[:-len(off)]
        return base + ":00" + off
    return dt


def build_datetime(date: str, time: str, offset: Optional[str], sample_xml_text: str) -> str:
    if not DATE_RE.fullmatch(date):
        raise ValueError("Data inválida. Use YYYY-MM-DD (ex.: 2025-12-18).")
    if not TIME_RE.fullmatch(time):
        raise ValueError("Hora inválida. Use HH:MM ou HH:MM:SS (ex.: 15:03:00).")

    time = ensure_seconds(time)

    if offset:
        if not OFFSET_RE.fullmatch(offset):
            raise ValueError("Offset inválido. Use Z ou +HH:MM / -HH:MM (ex.: -03:00).")
        off = offset
    else:
        off = None
        for tag in ("dhEmi", "dhSaiEnt"):
            m = re.search(rf"<{tag}>([^<]+)</{tag}>", sample_xml_text)
            if m:
                off = extract_offset(m.group(1))
                if off:
                    break
        off = off or "-03:00"

    return f"{date}T{time}{off}"


@dataclass
class FileResult:
    path: Path
    changed: bool
    count_dhEmi: int
    count_dhSaiEnt: int


def replace_tag_values(xml_text: str, tag: str, new_value: str) -> Tuple[str, int]:
    """
    Substitui o conteúdo interno da tag (com ou sem prefixo), preservando o restante.
    Suporta:
      <dhEmi>...</dhEmi>
      <nfe:dhEmi>...</nfe:dhEmi>
    """
    pattern = re.compile(
        rf"(?P<open><(?:(?P<prefix>\w+):)?{tag}>\s*)"
        rf"(?P<val>[^<]*?)"
        rf"(?P<close>\s*</(?(prefix)(?P=prefix):){tag}>)"
    )

    count = 0

    def _repl(m: re.Match) -> str:
        nonlocal count
        count += 1
        return m.group("open") + new_value + m.group("close")

    new_text = pattern.sub(_repl, xml_text)
    return new_text, count


def iter_xml_files(path: Path, recursive: bool) -> Iterable[Path]:
    if path.is_file():
        yield path
        return
    if recursive:
        yield from sorted(path.rglob("*.xml"))
    else:
        yield from sorted(path.glob("*.xml"))


def process_file(file_path: Path, new_dt: str, inplace: bool, backup: bool, outdir: Optional[Path], dry_run: bool) -> FileResult:
    raw = file_path.read_bytes()
    enc = detect_encoding(raw)
    try:
        text = raw.decode(enc)
    except UnicodeDecodeError:
        text = raw.decode("utf-8", errors="replace")

    text2, c1 = replace_tag_values(text, "dhEmi", new_dt)
    text3, c2 = replace_tag_values(text2, "dhSaiEnt", new_dt)

    changed = (text3 != text)

    if changed and not dry_run:
        if outdir is not None:
            outdir.mkdir(parents=True, exist_ok=True)
            out_path = outdir / file_path.name
            out_path.write_text(text3, encoding=enc, newline="")
        else:
            if backup and inplace:
                bak = file_path.with_suffix(file_path.suffix + ".bak")
                if not bak.exists():
                    bak.write_bytes(raw)

            if inplace:
                file_path.write_text(text3, encoding=enc, newline="")
            else:
                out_path = file_path.with_name(file_path.stem + ".novo" + file_path.suffix)
                out_path.write_text(text3, encoding=enc, newline="")

    return FileResult(file_path, changed, c1, c2)


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Atualizar dhEmi e dhSaiEnt (XML)")

        self.path_var = tk.StringVar()
        self.mode_var = tk.StringVar(value="folder")  # folder | file

        self.dt_full_var = tk.StringVar()
        self.date_var = tk.StringVar()
        self.time_var = tk.StringVar()
        self.offset_var = tk.StringVar()

        self.recursive_var = tk.BooleanVar(value=True)
        self.inplace_var = tk.BooleanVar(value=True)
        self.backup_var = tk.BooleanVar(value=True)
        self.dryrun_var = tk.BooleanVar(value=False)

        self.use_outdir_var = tk.BooleanVar(value=False)
        self.outdir_var = tk.StringVar()

        self.log_q: "queue.Queue[str]" = queue.Queue()
        self.worker_thread: Optional[threading.Thread] = None

        self._build_ui()
        self.after(120, self._drain_log)

    def _build_ui(self):
        padx = 10
        pady = 6

        frm = ttk.Frame(self)
        frm.pack(fill="both", expand=True, padx=padx, pady=padx)

        # Seleção de alvo
        target = ttk.LabelFrame(frm, text="Alvo")
        target.pack(fill="x", pady=(0, pady))

        r1 = ttk.Radiobutton(target, text="Pasta", value="folder", variable=self.mode_var, command=self._mode_changed)
        r2 = ttk.Radiobutton(target, text="Arquivo", value="file", variable=self.mode_var, command=self._mode_changed)
        r1.grid(row=0, column=0, sticky="w", padx=padx, pady=(pady, 0))
        r2.grid(row=0, column=1, sticky="w", padx=padx, pady=(pady, 0))

        ttk.Entry(target, textvariable=self.path_var).grid(row=1, column=0, columnspan=3, sticky="we", padx=padx, pady=pady)
        self.btn_browse = ttk.Button(target, text="Selecionar...", command=self._browse)
        self.btn_browse.grid(row=1, column=3, sticky="e", padx=padx, pady=pady)

        target.columnconfigure(2, weight=1)
        target.columnconfigure(0, weight=1)
        target.columnconfigure(1, weight=0)
        target.columnconfigure(3, weight=0)

        # Data/hora
        dtf = ttk.LabelFrame(frm, text="Nova data/hora (mesmo formato do XML)")
        dtf.pack(fill="x", pady=(0, pady))

        ttk.Label(dtf, text="Opção 1: Datetime completo").grid(row=0, column=0, sticky="w", padx=padx, pady=(pady, 0))
        ttk.Entry(dtf, textvariable=self.dt_full_var).grid(row=1, column=0, columnspan=4, sticky="we", padx=padx, pady=pady)
        ttk.Label(dtf, text='Ex.: 2025-12-18T15:03:00-03:00').grid(row=2, column=0, columnspan=4, sticky="w", padx=padx, pady=(0, pady))

        ttk.Separator(dtf).grid(row=3, column=0, columnspan=4, sticky="we", padx=padx, pady=pady)

        ttk.Label(dtf, text="Opção 2: Data + hora (+ offset opcional)").grid(row=4, column=0, sticky="w", padx=padx, pady=(0, 0))
        ttk.Label(dtf, text="Data (YYYY-MM-DD)").grid(row=5, column=0, sticky="w", padx=padx, pady=(pady, 0))
        ttk.Label(dtf, text="Hora (HH:MM ou HH:MM:SS)").grid(row=5, column=1, sticky="w", padx=padx, pady=(pady, 0))
        ttk.Label(dtf, text="Offset (opcional)").grid(row=5, column=2, sticky="w", padx=padx, pady=(pady, 0))

        ttk.Entry(dtf, textvariable=self.date_var, width=14).grid(row=6, column=0, sticky="we", padx=padx, pady=pady)
        ttk.Entry(dtf, textvariable=self.time_var, width=14).grid(row=6, column=1, sticky="we", padx=padx, pady=pady)
        ttk.Entry(dtf, textvariable=self.offset_var, width=10).grid(row=6, column=2, sticky="we", padx=padx, pady=pady)
        ttk.Label(dtf, text="Se vazio, tenta reaproveitar do XML (senão usa -03:00).").grid(row=6, column=3, sticky="w", padx=padx, pady=pady)

        dtf.columnconfigure(0, weight=1)
        dtf.columnconfigure(1, weight=1)
        dtf.columnconfigure(2, weight=0)
        dtf.columnconfigure(3, weight=2)

        # Opções
        opt = ttk.LabelFrame(frm, text="Opções")
        opt.pack(fill="x", pady=(0, pady))

        ttk.Checkbutton(opt, text="Recursivo (subpastas)", variable=self.recursive_var).grid(row=0, column=0, sticky="w", padx=padx, pady=pady)
        ttk.Checkbutton(opt, text="Sobrescrever originais (--inplace)", variable=self.inplace_var, command=self._inplace_changed).grid(row=0, column=1, sticky="w", padx=padx, pady=pady)
        ttk.Checkbutton(opt, text="Backup .bak (recomendado)", variable=self.backup_var).grid(row=0, column=2, sticky="w", padx=padx, pady=pady)
        ttk.Checkbutton(opt, text="Dry-run (não grava)", variable=self.dryrun_var).grid(row=0, column=3, sticky="w", padx=padx, pady=pady)

        ttk.Checkbutton(opt, text="Salvar em pasta de saída (não altera originais)", variable=self.use_outdir_var, command=self._outdir_toggle).grid(row=1, column=0, columnspan=2, sticky="w", padx=padx, pady=(0, pady))
        self.out_entry = ttk.Entry(opt, textvariable=self.outdir_var)
        self.out_entry.grid(row=2, column=0, columnspan=3, sticky="we", padx=padx, pady=(0, pady))
        self.out_btn = ttk.Button(opt, text="Selecionar pasta de saída...", command=self._browse_outdir)
        self.out_btn.grid(row=2, column=3, sticky="e", padx=padx, pady=(0, pady))

        opt.columnconfigure(0, weight=1)
        opt.columnconfigure(1, weight=1)
        opt.columnconfigure(2, weight=1)
        opt.columnconfigure(3, weight=1)

        # Ações
        act = ttk.Frame(frm)
        act.pack(fill="x", pady=(0, pady))

        self.btn_run = ttk.Button(act, text="Executar", command=self._run)
        self.btn_run.pack(side="left")

        self.btn_clear = ttk.Button(act, text="Limpar log", command=self._clear_log)
        self.btn_clear.pack(side="left", padx=(10, 0))

        # Log
        logf = ttk.LabelFrame(frm, text="Log")
        logf.pack(fill="both", expand=True)

        self.log = tk.Text(logf, height=18, wrap="word")
        self.log.pack(fill="both", expand=True, padx=padx, pady=padx)

        self._mode_changed()
        self._outdir_toggle()
        self._inplace_changed()

    def _log(self, msg: str):
        self.log_q.put(msg)

    def _drain_log(self):
        try:
            while True:
                msg = self.log_q.get_nowait()
                self.log.insert("end", msg + "\n")
                self.log.see("end")
        except queue.Empty:
            pass
        self.after(120, self._drain_log)

    def _clear_log(self):
        self.log.delete("1.0", "end")

    def _mode_changed(self):
        mode = self.mode_var.get()
        if mode == "folder":
            self.recursive_var.set(True)
        # nada a desabilitar aqui além do texto de botão
        # (filedialog muda no browse)
        return

    def _inplace_changed(self):
        # Backup só faz sentido com inplace e sem outdir
        if not self.inplace_var.get() or self.use_outdir_var.get():
            self.backup_var.set(False)

    def _outdir_toggle(self):
        use_out = self.use_outdir_var.get()
        state = "normal" if use_out else "disabled"
        self.out_entry.configure(state=state)
        self.out_btn.configure(state=state)
        if use_out:
            self.inplace_var.set(False)
            self.backup_var.set(False)
        else:
            # se sair de outdir, volta para inplace por padrão
            self.inplace_var.set(True)
            self.backup_var.set(True)

    def _browse(self):
        if self.mode_var.get() == "folder":
            p = filedialog.askdirectory(title="Selecione a pasta com os XMLs")
            if p:
                self.path_var.set(p)
        else:
            p = filedialog.askopenfilename(
                title="Selecione um arquivo XML",
                filetypes=[("XML", "*.xml"), ("Todos os arquivos", "*.*")]
            )
            if p:
                self.path_var.set(p)

    def _browse_outdir(self):
        p = filedialog.askdirectory(title="Selecione a pasta de saída")
        if p:
            self.outdir_var.set(p)

    def _compute_new_dt(self, target: Path) -> str:
        dt_full = self.dt_full_var.get().strip()
        if dt_full:
            return normalize_datetime(dt_full)

        date = self.date_var.get().strip()
        time = self.time_var.get().strip()
        offset = self.offset_var.get().strip() or None

        if not (date and time):
            raise ValueError("Preencha o Datetime completo OU então Data e Hora.")

        # sample_text para reaproveitar offset
        sample_text = ""
        sample_path = target
        if target.is_dir():
            first = next(iter_xml_files(target, self.recursive_var.get()), None)
            if first is None:
                raise ValueError("Nenhum XML encontrado na pasta selecionada.")
            sample_path = first

        raw = sample_path.read_bytes()
        enc = detect_encoding(raw)
        try:
            sample_text = raw.decode(enc)
        except UnicodeDecodeError:
            sample_text = raw.decode("utf-8", errors="replace")

        return build_datetime(date, time, offset, sample_text)

    def _run(self):
        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showinfo("Em execução", "Já existe uma execução em andamento.")
            return

        path_str = self.path_var.get().strip()
        if not path_str:
            messagebox.showerror("Erro", "Selecione uma pasta ou arquivo XML.")
            return

        target = Path(path_str)
        if not target.exists():
            messagebox.showerror("Erro", "Caminho não encontrado.")
            return

        try:
            new_dt = self._compute_new_dt(target)
        except Exception as e:
            messagebox.showerror("Erro no datetime", str(e))
            return

        recursive = self.recursive_var.get()
        inplace = self.inplace_var.get()
        backup = self.backup_var.get()
        dry_run = self.dryrun_var.get()

        outdir = None
        if self.use_outdir_var.get():
            out_str = self.outdir_var.get().strip()
            if not out_str:
                messagebox.showerror("Erro", "Informe a pasta de saída.")
                return
            outdir = Path(out_str)

        self.btn_run.configure(state="disabled")
        self._log(f"Novo datetime: {new_dt}")
        self._log(f"Alvo: {target}")
        self._log(f"Recursivo: {recursive} | Inplace: {inplace} | Backup: {backup} | Outdir: {outdir} | Dry-run: {dry_run}")
        self._log("-" * 80)

        def worker():
            try:
                files = list(iter_xml_files(target, recursive))
                if not files:
                    self._log("Nenhum .xml encontrado.")
                    return

                changed_files = 0
                total_dhEmi = 0
                total_dhSaiEnt = 0

                for f in files:
                    r = process_file(f, new_dt, inplace=inplace, backup=backup, outdir=outdir, dry_run=dry_run)
                    total_dhEmi += r.count_dhEmi
                    total_dhSaiEnt += r.count_dhSaiEnt
                    if r.changed:
                        changed_files += 1
                        self._log(f"[OK] {f}  (dhEmi: {r.count_dhEmi}, dhSaiEnt: {r.count_dhSaiEnt})")
                    else:
                        self._log(f"[SEM ALTERAÇÃO] {f}  (dhEmi: {r.count_dhEmi}, dhSaiEnt: {r.count_dhSaiEnt})")

                self._log("")
                self._log(f"Arquivos processados: {len(files)}")
                self._log(f"Arquivos alterados:   {changed_files}")
                self._log(f"Substituições dhEmi:  {total_dhEmi}")
                self._log(f"Substituições dhSaiEnt:{total_dhSaiEnt}")
                if dry_run:
                    self._log("Dry-run: nenhuma gravação foi realizada.")
            except Exception as e:
                self._log(f"ERRO: {e}")
            finally:
                self._log("-" * 80)
                self.btn_run.configure(state="normal")

        self.worker_thread = threading.Thread(target=worker, daemon=True)
        self.worker_thread.start()


if __name__ == "__main__":
    app = App()
    app.geometry("980x680")
    app.mainloop()
