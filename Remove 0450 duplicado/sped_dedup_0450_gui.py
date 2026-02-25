#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import re
import threading
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
import tkinter as tk
from tkinter import ttk, filedialog, messagebox


def detect_encoding(path: Path) -> str:
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            with open(path, "r", encoding=enc, newline="") as f:
                f.read()
            return enc
        except UnicodeDecodeError:
            pass
    return "latin-1"


def read_lines_keepends(path: Path, encoding: str) -> list[str]:
    with open(path, "r", encoding=encoding, newline="") as f:
        content = f.read()
    return content.splitlines(keepends=True)


def detect_newline(lines: list[str]) -> str:
    for ln in lines:
        if ln.endswith("\r\n"):
            return "\r\n"
        if ln.endswith("\n"):
            return "\n"
    return "\n"


def get_reg(line: str) -> str:
    s = line.lstrip()
    if not s.startswith("|"):
        return ""
    parts = s.split("|")
    if len(parts) >= 3:
        return parts[1].strip()
    return ""


def is_block_total_reg(reg: str) -> bool:
    # 0990, A990, C990, 1990 etc (menos 9990)
    return len(reg) == 4 and reg.endswith("990") and reg != "9990"


def order_regs_for_9900(regs: set[str]) -> list[str]:
    block_order = {b: i for i, b in enumerate(["0", "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "1", "9"])}

    def key(r: str):
        b = r[:1] if r else "~"
        return (block_order.get(b, 99), r)

    return sorted(regs, key=key)


@dataclass
class Reg0450:
    cod_inf: str
    txt_inf: str


def parse_0450(line: str) -> Reg0450:
    # Formato: |0450|COD_INF|TXT_INF|
    parts = line.strip("\r\n").split("|")
    # parts: ["", "0450", "COD_INF", "TXT_INF", ""]
    cod_inf = parts[2].strip() if len(parts) > 2 else ""
    txt_inf = parts[3].strip() if len(parts) > 3 else ""
    return Reg0450(cod_inf=cod_inf, txt_inf=txt_inf)


def is_numeric_cod_inf(cod_inf: str) -> bool:
    # “numérico” = só dígitos
    c = cod_inf.strip()
    return bool(c) and c.isdigit()


def normalize_txt(txt: str) -> str:
    # Normaliza espaços internos pra dedupe por texto ficar mais consistente
    t = re.sub(r"\s+", " ", (txt or "").strip())
    return t


def process_sped_dedup_0450(
    input_path: Path,
    output_path: Path,
    dedupe_by: str = "TXT_INF",  # "TXT_INF" ou "COD_INF"
    log_cb=None,
) -> dict:
    def log(msg: str):
        if log_cb:
            log_cb(msg)

    if not input_path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {input_path}")

    enc = detect_encoding(input_path)
    raw_lines = read_lines_keepends(input_path, enc)
    nl = detect_newline(raw_lines)

    log(f"Encoding detectado: {enc}")
    log(f"Linhas lidas: {len(raw_lines)}")
    log(f"Deduplicação por: {dedupe_by}")

    # 1) Remove 9900 existente (vamos regenerar) e deduplica 0450
    removed_9900 = 0
    removed_0450_dupes = 0

    out_lines: list[str] = []
    # Mapa: chave -> (pos_no_out_lines, score_numeric)
    kept_map: dict[str, tuple[int, bool]] = {}

    for line in raw_lines:
        reg = get_reg(line)

        if reg == "9900":
            removed_9900 += 1
            continue

        if reg != "0450":
            out_lines.append(line)
            continue

        r0450 = parse_0450(line)
        cod = r0450.cod_inf
        txt = normalize_txt(r0450.txt_inf)

        if dedupe_by == "COD_INF":
            key = cod.strip()
        else:
            key = txt

        # Se a chave ficou vazia (ex: TXT vazio), usa a linha inteira como fallback
        if not key:
            key = f"__RAW__{line.strip()}"

        numeric = is_numeric_cod_inf(cod)

        if key not in kept_map:
            kept_map[key] = (len(out_lines), numeric)
            out_lines.append(line)
        else:
            pos, prev_numeric = kept_map[key]
            # Preferência: manter a versão com COD_INF numérico
            if (not prev_numeric) and numeric:
                # substitui a anterior pela atual
                out_lines[pos] = line
                kept_map[key] = (pos, True)
                removed_0450_dupes += 1
                log(f"0450 duplicado removido (substituído por COD_INF numérico) | COD_INF={cod} | TXT='{txt[:60]}'")
            else:
                removed_0450_dupes += 1
                log(f"0450 duplicado removido | COD_INF={cod} | TXT='{txt[:60]}'")

    log(f"9900 antigos removidos: {removed_9900}")
    log(f"0450 duplicados removidos: {removed_0450_dupes}")

    # 2) Conta registros na base (sem 9900)
    regs_in_base = [get_reg(l) for l in out_lines]
    counts_base = Counter(r for r in regs_in_base if r)

    # 3) Define REGs finais e quantidade de linhas 9900
    final_regs = set(counts_base.keys())
    final_regs.add("9900")
    n_9900_lines = len(final_regs)

    counts_final = dict(counts_base)
    counts_final["9900"] = n_9900_lines

    # 4) Gera 9900
    ordered_regs = order_regs_for_9900(final_regs)
    new_9900_lines = [f"|9900|{r}|{counts_final.get(r, 0)}|{nl}" for r in ordered_regs]

    # 5) Insere 9900 após 9001 (ou antes de 9990; fallback: final)
    idx_9001 = next((i for i, l in enumerate(out_lines) if get_reg(l) == "9001"), None)
    idx_9990 = next((i for i, l in enumerate(out_lines) if get_reg(l) == "9990"), None)

    if idx_9001 is not None:
        insert_pos = idx_9001 + 1
        log("Inserindo 9900 após o 9001.")
    elif idx_9990 is not None:
        insert_pos = idx_9990
        log("Inserindo 9900 antes do 9990 (9001 não encontrado).")
    else:
        insert_pos = len(out_lines)
        log("Inserindo 9900 no final (9001/9990 não encontrados).")

    lines = out_lines[:insert_pos] + new_9900_lines + out_lines[insert_pos:]

    # 6) Recalcula totais por bloco
    block_counts = defaultdict(int)
    for l in lines:
        r = get_reg(l)
        if r:
            block_counts[r[0]] += 1

    total_lines = len(lines)

    # 7) Atualiza 0990, X990, 9990, 9999
    fixed_lines: list[str] = []
    for l in lines:
        r = get_reg(l)

        if r == "9999":
            fixed_lines.append(f"|9999|{total_lines}|{nl}")
            continue

        if r == "9990":
            fixed_lines.append(f"|9990|{block_counts.get('9', 0)}|{nl}")
            continue

        if r == "0990":
            fixed_lines.append(f"|0990|{block_counts.get('0', 0)}|{nl}")
            continue

        if is_block_total_reg(r):
            blk = r[0]
            fixed_lines.append(f"|{r}|{block_counts.get(blk, 0)}|{nl}")
            continue

        fixed_lines.append(l)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding=enc, newline="") as f:
        f.write("".join(fixed_lines))

    log(f"Arquivo gerado: {output_path}")
    log(f"Total de linhas final: {len(fixed_lines)}")

    return {
        "input": str(input_path),
        "output": str(output_path),
        "encoding": enc,
        "removed_0450_dupes": removed_0450_dupes,
        "removed_9900_old": removed_9900,
        "generated_9900": n_9900_lines,
        "total_lines_final": len(fixed_lines),
        "dedupe_by": dedupe_by,
    }


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("SPED - Deduplicar Registro 0450 (mantém COD_INF numérico)")
        self.geometry("920x560")
        self.minsize(920, 560)

        self.var_in = tk.StringVar()
        self.var_out = tk.StringVar()
        self.var_dedupe_by = tk.StringVar(value="TXT_INF")  # TXT_INF ou COD_INF

        self._build_ui()

    def _build_ui(self):
        frm = ttk.Frame(self, padding=12)
        frm.pack(fill="both", expand=True)

        # Entrada
        row1 = ttk.Frame(frm)
        row1.pack(fill="x", pady=(0, 8))
        ttk.Label(row1, text="Arquivo SPED (entrada):").pack(side="left")
        ttk.Entry(row1, textvariable=self.var_in).pack(side="left", fill="x", expand=True, padx=8)
        ttk.Button(row1, text="Procurar...", command=self.pick_input).pack(side="left")

        # Saída
        row2 = ttk.Frame(frm)
        row2.pack(fill="x", pady=(0, 10))
        ttk.Label(row2, text="Arquivo de saída:").pack(side="left")
        ttk.Entry(row2, textvariable=self.var_out).pack(side="left", fill="x", expand=True, padx=8)
        ttk.Button(row2, text="Salvar como...", command=self.pick_output).pack(side="left")

        # Opções
        row_opts = ttk.Frame(frm)
        row_opts.pack(fill="x", pady=(0, 10))
        ttk.Label(row_opts, text="Considerar duplicado por:").pack(side="left")

        ttk.Radiobutton(row_opts, text="TXT_INF (texto do 0450)", variable=self.var_dedupe_by, value="TXT_INF").pack(side="left", padx=10)
        ttk.Radiobutton(row_opts, text="COD_INF (código do 0450)", variable=self.var_dedupe_by, value="COD_INF").pack(side="left", padx=10)

        # Botões
        row3 = ttk.Frame(frm)
        row3.pack(fill="x", pady=(0, 12))
        self.btn_run = ttk.Button(row3, text="Executar (Deduplicar |0450|)", command=self.run)
        self.btn_run.pack(side="left")
        ttk.Button(row3, text="Limpar log", command=self.clear_log).pack(side="left", padx=8)

        self.progress = ttk.Progressbar(row3, mode="indeterminate")
        self.progress.pack(side="right", fill="x", expand=True)

        # Log
        ttk.Label(frm, text="Log:").pack(anchor="w")
        self.txt = tk.Text(frm, height=22, wrap="none")
        self.txt.pack(fill="both", expand=True)

        xscroll = ttk.Scrollbar(frm, orient="horizontal", command=self.txt.xview)
        xscroll.pack(fill="x")
        yscroll = ttk.Scrollbar(frm, orient="vertical", command=self.txt.yview)
        yscroll.place(relx=1.0, rely=0.0, relheight=1.0, anchor="ne")

        self.txt.configure(xscrollcommand=xscroll.set, yscrollcommand=yscroll.set)

        self._log("Regras:")
        self._log("- Remove apenas 0450 duplicados (por TXT_INF ou COD_INF).")
        self._log("- Mantém 1 só e dá preferência ao 0450 com COD_INF numérico.")
        self._log("- Recalcula 9900, 0990, X990, 9990 e 9999.")

    def _log(self, msg: str):
        self.txt.insert("end", msg + "\n")
        self.txt.see("end")

    def clear_log(self):
        self.txt.delete("1.0", "end")

    def pick_input(self):
        path = filedialog.askopenfilename(
            title="Selecione o arquivo SPED",
            filetypes=[("Arquivos TXT", "*.txt"), ("Todos os arquivos", "*.*")]
        )
        if path:
            self.var_in.set(path)
            p = Path(path)
            default_out = p.with_name(p.stem + "_dedup_0450" + p.suffix)
            if not self.var_out.get():
                self.var_out.set(str(default_out))

    def pick_output(self):
        path = filedialog.asksaveasfilename(
            title="Salvar arquivo de saída",
            defaultextension=".txt",
            filetypes=[("Arquivos TXT", "*.txt"), ("Todos os arquivos", "*.*")]
        )
        if path:
            self.var_out.set(path)

    def run(self):
        in_str = self.var_in.get().strip()
        out_str = self.var_out.get().strip()

        if not in_str:
            messagebox.showwarning("Atenção", "Selecione o arquivo de entrada.")
            return
        if not out_str:
            messagebox.showwarning("Atenção", "Defina o arquivo de saída.")
            return

        in_path = Path(in_str)
        out_path = Path(out_str)
        dedupe_by = self.var_dedupe_by.get()

        if out_path.exists():
            ok = messagebox.askyesno("Confirmar", "O arquivo de saída já existe. Deseja sobrescrever?")
            if not ok:
                return

        self.btn_run.configure(state="disabled")
        self.progress.start(12)
        self._log("=== Iniciando processamento ===")

        def worker():
            try:
                result = process_sped_dedup_0450(
                    input_path=in_path,
                    output_path=out_path,
                    dedupe_by=dedupe_by,
                    log_cb=lambda m: self.after(0, self._log, m),
                )
                self.after(0, self._on_success, result)
            except Exception as e:
                self.after(0, self._on_error, str(e))

        threading.Thread(target=worker, daemon=True).start()

    def _on_success(self, result: dict):
        self.progress.stop()
        self.btn_run.configure(state="normal")

        self._log("=== Concluído com sucesso ===")
        self._log(f"Entrada: {result['input']}")
        self._log(f"Saída  : {result['output']}")
        self._log(f"Encoding: {result['encoding']}")
        self._log(f"Dedup por: {result['dedupe_by']}")
        self._log(f"0450 duplicados removidos: {result['removed_0450_dupes']}")
        self._log(f"9900 antigos removidos: {result['removed_9900_old']}")
        self._log(f"9900 gerados: {result['generated_9900']}")
        self._log(f"Linhas finais: {result['total_lines_final']}")

        messagebox.showinfo("OK", "Arquivo gerado com sucesso e totais recalculados.")

    def _on_error(self, err: str):
        self.progress.stop()
        self.btn_run.configure(state="normal")
        self._log("=== ERRO ===")
        self._log(err)
        messagebox.showerror("Erro", err)


if __name__ == "__main__":
    App().mainloop()