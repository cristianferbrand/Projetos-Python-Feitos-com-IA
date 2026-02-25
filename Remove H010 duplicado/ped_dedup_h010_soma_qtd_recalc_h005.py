#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import re
import threading
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
import tkinter as tk
from tkinter import ttk, filedialog, messagebox


# -------------------------
# Utilitários de arquivo
# -------------------------

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


# -------------------------
# Decimal parse/format (SPED)
# -------------------------

def parse_decimal_br(s: str) -> Decimal:
    """
    Aceita:
      - "123,45"
      - "123.45"
      - "1.234,56" (remove milhar)
    """
    s = (s or "").strip()
    if not s:
        return Decimal("0")
    # Se tem ponto e vírgula: assume ponto milhar e vírgula decimal
    if "." in s and "," in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", ".")
    try:
        return Decimal(s)
    except InvalidOperation:
        return Decimal("0")


def decimals_in_str(s: str, default: int) -> int:
    s = (s or "").strip()
    if not s:
        return default
    if "," in s:
        return len(s.split(",")[1])
    if "." in s:
        return len(s.split(".")[1])
    return 0


def format_decimal_br(value: Decimal, decimals: int) -> str:
    q = Decimal("1").scaleb(-decimals)  # 10^-decimals
    v = value.quantize(q, rounding=ROUND_HALF_UP)
    txt = f"{v:f}"
    if "." not in txt and decimals > 0:
        txt += "." + ("0" * decimals)
    if "." in txt:
        i, d = txt.split(".")
        d = (d + "0" * decimals)[:decimals] if decimals > 0 else ""
        txt = i + (("." + d) if decimals > 0 else "")
    return txt.replace(".", ",")


# -------------------------
# SPED Totais (9900/0990/*990/9990/9999)
# -------------------------

def is_block_total_reg(reg: str) -> bool:
    return len(reg) == 4 and reg.endswith("990") and reg != "9990"


def order_regs_for_9900(regs: set[str]) -> list[str]:
    block_order = {b: i for i, b in enumerate(["0", "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "1", "9"])}

    def key(r: str):
        b = r[:1] if r else "~"
        return (block_order.get(b, 99), r)

    return sorted(regs, key=key)


def rebuild_9900_and_totals(lines: list[str], nl: str, log) -> list[str]:
    # Remove 9900 antigos
    base_lines = []
    removed_9900 = 0
    for l in lines:
        if get_reg(l) == "9900":
            removed_9900 += 1
            continue
        base_lines.append(l)

    log(f"9900 antigos removidos: {removed_9900}")

    # Conta regs (sem 9900)
    regs_in_base = [get_reg(l) for l in base_lines]
    counts_base = Counter(r for r in regs_in_base if r)

    # Regs finais + 9900
    final_regs = set(counts_base.keys())
    final_regs.add("9900")
    n_9900_lines = len(final_regs)

    counts_final = dict(counts_base)
    counts_final["9900"] = n_9900_lines

    ordered_regs = order_regs_for_9900(final_regs)
    new_9900_lines = [f"|9900|{r}|{counts_final.get(r, 0)}|{nl}" for r in ordered_regs]

    # Inserir após 9001 (ou antes do 9990)
    idx_9001 = next((i for i, l in enumerate(base_lines) if get_reg(l) == "9001"), None)
    idx_9990 = next((i for i, l in enumerate(base_lines) if get_reg(l) == "9990"), None)
    if idx_9001 is not None:
        insert_pos = idx_9001 + 1
        log("Inserindo 9900 após 9001.")
    elif idx_9990 is not None:
        insert_pos = idx_9990
        log("Inserindo 9900 antes do 9990 (9001 não encontrado).")
    else:
        insert_pos = len(base_lines)
        log("Inserindo 9900 no final (9001/9990 não encontrados).")

    with_9900 = base_lines[:insert_pos] + new_9900_lines + base_lines[insert_pos:]

    # Recalcular contagens por bloco
    block_counts = defaultdict(int)
    for l in with_9900:
        r = get_reg(l)
        if r:
            block_counts[r[0]] += 1
    total_lines = len(with_9900)

    fixed = []
    for l in with_9900:
        r = get_reg(l)
        if r == "9999":
            fixed.append(f"|9999|{total_lines}|{nl}")
            continue
        if r == "9990":
            fixed.append(f"|9990|{block_counts.get('9', 0)}|{nl}")
            continue
        if r == "0990":
            fixed.append(f"|0990|{block_counts.get('0', 0)}|{nl}")
            continue
        if is_block_total_reg(r):
            blk = r[0]
            fixed.append(f"|{r}|{block_counts.get(blk, 0)}|{nl}")
            continue
        fixed.append(l)

    return fixed


# -------------------------
# Lógica H005 / H010
# -------------------------

@dataclass
class H010Agg:
    idx_out: int               # posição do placeholder no output da seção
    template_fields: list[str] # campos do H010 (parts sem os pipes externos)
    qty_dec: int
    unit_dec: int
    item_dec: int
    qtd_sum: Decimal
    unit_value: Decimal
    item_sum: Decimal          # usado p/ média ponderada se necessário
    had_children_kept: bool


def split_sped_fields(line: str) -> list[str]:
    # Retorna campos sem os pipes vazios do começo e fim:
    # |H010|A|B| => ["H010","A","B"]
    raw = line.strip("\r\n")
    parts = raw.split("|")
    if parts and parts[0] == "":
        parts = parts[1:]
    if parts and parts[-1] == "":
        parts = parts[:-1]
    return parts


def join_sped_fields(fields: list[str], nl: str) -> str:
    return "|" + "|".join(fields) + "|" + nl


def h010_key(fields: list[str], use_unid: bool, use_ind_prop: bool, use_cod_part: bool, use_cod_cta: bool) -> str:
    # H010: [0]=H010, [1]=COD_ITEM, [2]=UNID, [3]=QTD, [4]=VL_UNIT, [5]=VL_ITEM, [6]=IND_PROP, [7]=COD_PART, [8]=TXT_COMPL, [9]=COD_CTA, ...
    cod_item = fields[1].strip() if len(fields) > 1 else ""
    unid = fields[2].strip() if len(fields) > 2 else ""
    ind_prop = fields[6].strip() if len(fields) > 6 else ""
    cod_part = fields[7].strip() if len(fields) > 7 else ""
    cod_cta = fields[9].strip() if len(fields) > 9 else ""

    parts = [cod_item]
    if use_unid:
        parts.append(unid)
    if use_ind_prop:
        parts.append(ind_prop)
    if use_cod_part:
        parts.append(cod_part)
    if use_cod_cta:
        parts.append(cod_cta)
    return "|".join(parts)


def process_h_blocks_dedup_h010(lines: list[str], nl: str, opts: dict, log) -> list[str]:
    """
    Deduplica H010 dentro de cada H005:
      - soma QTD
      - VL_ITEM = VL_UNIT * QTD (com QTD somada)
      - H005.VL_INV = soma VL_ITEM
    """
    out = []
    i = 0
    merged_count = 0
    h005_recalc_count = 0

    use_unid = opts["use_unid"]
    use_ind_prop = opts["use_ind_prop"]
    use_cod_part = opts["use_cod_part"]
    use_cod_cta = opts["use_cod_cta"]
    prefer_weighted_unit = opts["weighted_unit_if_conflict"]

    while i < len(lines):
        reg = get_reg(lines[i])

        if reg != "H005":
            out.append(lines[i])
            i += 1
            continue

        # --- Entrou numa seção H005 ---
        h005_line = lines[i]
        h005_fields = split_sped_fields(h005_line)  # ["H005", DT_INV, VL_INV, MOT_INV]
        i += 1

        section_out = []
        aggs: dict[str, H010Agg] = {}
        section_vl_inv = Decimal("0")

        # Copia H005 por enquanto (vamos recalcular VL_INV e substituir depois)
        section_out.append(h005_line)
        h005_pos_in_section = 0

        # percorre linhas até sair do bloco H005 (novo H005, H990, ou sair do bloco H)
        while i < len(lines):
            r = get_reg(lines[i])
            if r in ("H005", "H990") or (r and not r.startswith("H")):
                break

            if r != "H010":
                section_out.append(lines[i])
                i += 1
                continue

            # Captura item H010 e seus filhos imediatos (H020/H030)
            item_lines = [lines[i]]
            i += 1
            children_lines = []
            while i < len(lines):
                rr = get_reg(lines[i])
                if rr in ("H020", "H030"):
                    children_lines.append(lines[i])
                    i += 1
                    continue
                break

            fields = split_sped_fields(item_lines[0])
            # Garantir tamanho mínimo para índices usados
            while len(fields) < 10:
                fields.append("")

            key = h010_key(fields, use_unid, use_ind_prop, use_cod_part, use_cod_cta)

            qtd_str = fields[3] if len(fields) > 3 else ""
            unit_str = fields[4] if len(fields) > 4 else ""
            item_str = fields[5] if len(fields) > 5 else ""

            qtd = parse_decimal_br(qtd_str)
            vl_unit = parse_decimal_br(unit_str)
            vl_item = parse_decimal_br(item_str)

            qtd_dec = decimals_in_str(qtd_str, default=3)
            unit_dec = decimals_in_str(unit_str, default=6)
            item_dec = max(2, decimals_in_str(item_str, default=2))

            # Se VL_UNIT vier zerado, tenta inferir por VL_ITEM/QTD
            if vl_unit == 0 and qtd != 0 and vl_item != 0:
                vl_unit = (vl_item / qtd)

            # Se VL_ITEM vier zerado, calcula
            if vl_item == 0 and vl_unit != 0:
                vl_item = vl_unit * qtd

            if key not in aggs:
                # cria placeholder do H010 que será substituído no final da seção
                placeholder_idx = len(section_out)
                section_out.append("<<H010_PLACEHOLDER>>" + nl)

                # mantém os filhos do primeiro (se existirem)
                for cl in children_lines:
                    section_out.append(cl)

                aggs[key] = H010Agg(
                    idx_out=placeholder_idx,
                    template_fields=fields[:],  # copia
                    qty_dec=qtd_dec,
                    unit_dec=unit_dec,
                    item_dec=item_dec,
                    qtd_sum=qtd,
                    unit_value=vl_unit,
                    item_sum=vl_item,  # para média ponderada se necessário
                    had_children_kept=bool(children_lines),
                )
            else:
                # duplicado -> soma quantidade (e guarda valor p/ média ponderada opcional)
                agg = aggs[key]
                old_qty = agg.qtd_sum
                agg.qtd_sum += qtd

                # se houver conflito de unitário, decide estratégia
                # (tolerância simples por comparação arredondada)
                if vl_unit != 0:
                    if agg.unit_value == 0:
                        agg.unit_value = vl_unit
                    else:
                        # conflito: usa média ponderada se habilitado
                        if prefer_weighted_unit and qtd != 0:
                            agg.item_sum += vl_item if vl_item != 0 else (vl_unit * qtd)
                        # se não usar média ponderada, mantém o unit do primeiro

                # atualiza decimais para preservar precisão “máxima” vista
                agg.qty_dec = max(agg.qty_dec, qtd_dec)
                agg.unit_dec = max(agg.unit_dec, unit_dec)
                agg.item_dec = max(agg.item_dec, item_dec)

                # Se duplicado tinha filhos, descarta para não quebrar hierarquia
                if children_lines:
                    log(f"AVISO: duplicado H010 com filhos H020/H030 descartado (mantidos apenas do primeiro) | KEY={key}")

                merged_count += 1
                log(f"H010 duplicado mesclado | KEY={key} | QTD {old_qty} + {qtd} => {agg.qtd_sum}")

        # Finaliza seção: substitui placeholders H010 e calcula VL_INV
        for key, agg in aggs.items():
            qtd_final = agg.qtd_sum

            # Define unitário final:
            unit_final = agg.unit_value
            if unit_final == 0:
                # fallback: se item_sum e qtd_final disponíveis
                if qtd_final != 0 and agg.item_sum != 0:
                    unit_final = agg.item_sum / qtd_final

            # Se habilitado e houve conflito, podemos recalcular unitário por média ponderada:
            if prefer_weighted_unit and qtd_final != 0 and agg.item_sum != 0:
                # unit = soma_valores / soma_qtd
                unit_final = agg.item_sum / qtd_final

            vl_item_final = unit_final * qtd_final

            # atualiza campos H010:
            # [3]=QTD, [4]=VL_UNIT, [5]=VL_ITEM
            fields = agg.template_fields
            fields[3] = format_decimal_br(qtd_final, agg.qty_dec)
            fields[4] = format_decimal_br(unit_final, agg.unit_dec)
            fields[5] = format_decimal_br(vl_item_final, max(2, agg.item_dec))

            # grava no placeholder
            section_out[agg.idx_out] = join_sped_fields(fields, nl)

            section_vl_inv += vl_item_final

        # Atualiza H005.VL_INV (campo 2 do H005_fields: ["H005", DT_INV, VL_INV, MOT_INV])
        if len(h005_fields) < 4:
            while len(h005_fields) < 4:
                h005_fields.append("")
        old_vl_inv = h005_fields[2]
        h005_fields[2] = format_decimal_br(section_vl_inv, 2)
        section_out[h005_pos_in_section] = join_sped_fields(h005_fields, nl)
        h005_recalc_count += 1
        log(f"H005 recalculado | DT_INV={h005_fields[1]} | VL_INV {old_vl_inv} => {h005_fields[2]}")

        # anexa seção final ao output geral
        out.extend(section_out)

        # aqui NÃO incrementa i; já está posicionado no próximo reg fora da seção

    log(f"Total de H010 duplicados mesclados: {merged_count}")
    log(f"Total de H005 recalculados: {h005_recalc_count}")
    return out


# -------------------------
# Pipeline geral
# -------------------------

def process_file(input_path: Path, output_path: Path, opts: dict, log_cb=None) -> dict:
    def log(msg: str):
        if log_cb:
            log_cb(msg)

    if not input_path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {input_path}")

    enc = detect_encoding(input_path)
    raw_lines = read_lines_keepends(input_path, enc)
    nl = detect_newline(raw_lines)

    log(f"Encoding: {enc}")
    log(f"Linhas lidas: {len(raw_lines)}")

    # 1) Ajuste H005/H010 (dedupe + recálculos)
    adjusted = process_h_blocks_dedup_h010(raw_lines, nl, opts, log)

    # 2) Rebuild 9900 + Totais de blocos/arquivo
    final_lines = rebuild_9900_and_totals(adjusted, nl, log)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding=enc, newline="") as f:
        f.write("".join(final_lines))

    log(f"Arquivo gerado: {output_path}")
    log(f"Linhas finais: {len(final_lines)}")

    return {
        "input": str(input_path),
        "output": str(output_path),
        "encoding": enc,
        "lines_in": len(raw_lines),
        "lines_out": len(final_lines),
    }


# -------------------------
# GUI Tkinter
# -------------------------

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("SPED - Deduplicar H010 (soma QTD) + Recalcular H005/VL_INV")
        self.geometry("980x620")
        self.minsize(980, 620)

        self.var_in = tk.StringVar()
        self.var_out = tk.StringVar()

        # opções de chave p/ duplicidade
        self.var_use_unid = tk.BooleanVar(value=True)
        self.var_use_ind_prop = tk.BooleanVar(value=True)
        self.var_use_cod_part = tk.BooleanVar(value=True)
        self.var_use_cod_cta = tk.BooleanVar(value=True)

        # unitário em conflito
        self.var_weighted_unit = tk.BooleanVar(value=True)

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
        box = ttk.LabelFrame(frm, text="Critério para considerar H010 duplicado (chave)")
        box.pack(fill="x", pady=(0, 10))

        r = ttk.Frame(box)
        r.pack(fill="x", padx=10, pady=8)

        ttk.Label(r, text="Sempre usa: COD_ITEM  + (marque os complementos abaixo)").pack(anchor="w")

        r2 = ttk.Frame(box)
        r2.pack(fill="x", padx=10, pady=(0, 10))

        ttk.Checkbutton(r2, text="UNID", variable=self.var_use_unid).pack(side="left", padx=10)
        ttk.Checkbutton(r2, text="IND_PROP", variable=self.var_use_ind_prop).pack(side="left", padx=10)
        ttk.Checkbutton(r2, text="COD_PART", variable=self.var_use_cod_part).pack(side="left", padx=10)
        ttk.Checkbutton(r2, text="COD_CTA", variable=self.var_use_cod_cta).pack(side="left", padx=10)

        box2 = ttk.LabelFrame(frm, text="Recalcular VL_UNIT quando houver conflito entre duplicados")
        box2.pack(fill="x", pady=(0, 10))
        ttk.Checkbutton(
            box2,
            text="Usar média ponderada (soma VL_ITEM / soma QTD). Se desmarcar, mantém o VL_UNIT do primeiro.",
            variable=self.var_weighted_unit
        ).pack(anchor="w", padx=10, pady=8)

        # Botões
        row3 = ttk.Frame(frm)
        row3.pack(fill="x", pady=(0, 12))
        self.btn_run = ttk.Button(row3, text="Executar (Dedup H010 + Recalcular H005)", command=self.run)
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

        self._log("O script:")
        self._log("- Deduplica H010 dentro de cada H005 somando QTD;")
        self._log("- Recalcula VL_ITEM = VL_UNIT * QTD;")
        self._log("- Recalcula H005.VL_INV = soma dos VL_ITEM;")
        self._log("- Recalcula 9900, 0990, H990, 9990 e 9999.")

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
            default_out = p.with_name(p.stem + "_dedup_H010" + p.suffix)
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

        if out_path.exists():
            ok = messagebox.askyesno("Confirmar", "O arquivo de saída já existe. Deseja sobrescrever?")
            if not ok:
                return

        opts = {
            "use_unid": bool(self.var_use_unid.get()),
            "use_ind_prop": bool(self.var_use_ind_prop.get()),
            "use_cod_part": bool(self.var_use_cod_part.get()),
            "use_cod_cta": bool(self.var_use_cod_cta.get()),
            "weighted_unit_if_conflict": bool(self.var_weighted_unit.get()),
        }

        self.btn_run.configure(state="disabled")
        self.progress.start(12)
        self._log("=== Iniciando processamento ===")

        def worker():
            try:
                result = process_file(
                    input_path=in_path,
                    output_path=out_path,
                    opts=opts,
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
        self._log(f"Linhas: {result['lines_in']} -> {result['lines_out']}")

        messagebox.showinfo("OK", "Arquivo gerado com sucesso (H010/H005 ajustados e totais recalculados).")

    def _on_error(self, err: str):
        self.progress.stop()
        self.btn_run.configure(state="normal")
        self._log("=== ERRO ===")
        self._log(err)
        messagebox.showerror("Erro", err)


if __name__ == "__main__":
    App().mainloop()