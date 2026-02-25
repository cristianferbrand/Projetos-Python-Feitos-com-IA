import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP, getcontext
from collections import defaultdict
import threading
from queue import Queue, Empty
import os

getcontext().prec = 28  # boa precisão p/ somas


# =========================
# Helpers de parsing/format
# =========================
def parse_decimal_ptbr(s: str) -> Decimal:
    """
    SPED costuma vir com decimal em vírgula. Aceita:
      - "123,45"
      - "123.45"
      - "1.234,56" (remove milhar)
      - "" / None -> 0
    """
    if s is None:
        return Decimal("0")
    s = str(s).strip()
    if not s:
        return Decimal("0")

    # Normaliza (milhar/decimal)
    if "," in s and "." in s:
        # assume '.' milhar e ',' decimal
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    # se só tiver '.', fica como decimal padrão

    try:
        return Decimal(s)
    except InvalidOperation:
        return Decimal("0")


def fmt_money_ptbr(d: Decimal, places: int = 2) -> str:
    """
    Formata Decimal no padrão pt-BR com separador de milhar '.' e decimal ','.
    """
    if d is None:
        d = Decimal("0")
    q = Decimal("1").scaleb(-places)  # 0.01 para 2 casas
    d = d.quantize(q, rounding=ROUND_HALF_UP)

    sign = "-" if d < 0 else ""
    d = abs(d)

    s = f"{d:.{places}f}"  # "1234.56"
    int_part, dec_part = s.split(".")

    # agrupa milhar
    chunks = []
    while int_part:
        chunks.append(int_part[-3:])
        int_part = int_part[:-3]
    int_grouped = ".".join(reversed(chunks)) if chunks else "0"

    return f"{sign}{int_grouped},{dec_part}"


def safe_read_lines(path: str):
    """
    Lê linhas do arquivo tentando encodings comuns de SPED.
    """
    encodings = ["utf-8-sig", "utf-8", "latin-1", "cp1252"]
    last_err = None
    for enc in encodings:
        try:
            with open(path, "r", encoding=enc, errors="strict") as f:
                for line in f:
                    yield line
            return
        except Exception as e:
            last_err = e
            continue

    # fallback: ignora erros
    with open(path, "r", encoding="latin-1", errors="replace") as f:
        for line in f:
            yield line


# =========================
# Processamento SPED
# =========================
def process_sped_h020(path: str, multiply_by_qtd: bool, q: Queue):
    """
    Processa SPED:
      - Captura QTD do H010 (campo 04)
      - Totaliza H020 por CST_ICMS:
          BC_ICMS (campo 03) e VL_ICMS (campo 04)
        (multiplicando pela QTD do H010 se marcado)
    """
    agg = defaultdict(lambda: {
        "count_h020": 0,
        "sum_qtd": Decimal("0"),
        "sum_bc": Decimal("0"),
        "sum_vl": Decimal("0"),
        "sum_credito": Decimal("0"),
        "sum_debito": Decimal("0"),
    })

    current_qtd = Decimal("1")
    lines = 0
    h010_seen = 0
    h020_seen = 0
    warnings = 0

    q.put(("status", "Lendo arquivo e processando..."))

    for line in safe_read_lines(path):
        lines += 1
        line = line.strip()
        if not line:
            continue

        # SPED geralmente é pipe-delimited e começa/termina com '|'
        # Ex.: |H020|xxx|yyy|zzz|
        parts = line.split("|")
        if len(parts) < 2:
            continue

        reg = parts[1].strip().upper()

        if reg == "H010":
            # Campos (VRi): |H010|COD_ITEM|UNID|QTD|VL_UNIT|VL_ITEM|...|
            # QTD = campo 04 => parts[4]
            h010_seen += 1
            try:
                qtd_str = parts[4] if len(parts) > 4 else ""
                current_qtd = parse_decimal_ptbr(qtd_str)
                if current_qtd == 0:
                    # ainda pode existir, mas evita zerar multiplicador em cascata
                    current_qtd = Decimal("0")
            except Exception:
                current_qtd = Decimal("1")
                warnings += 1

        elif reg == "H020":
            # Campos (VRi): |H020|CST_ICMS|BC_ICMS|VL_ICMS|
            h020_seen += 1
            cst = parts[2].strip() if len(parts) > 2 else ""
            bc = parse_decimal_ptbr(parts[3] if len(parts) > 3 else "")
            vl = parse_decimal_ptbr(parts[4] if len(parts) > 4 else "")

            # fator
            factor = current_qtd if multiply_by_qtd else Decimal("1")

            # Se o H020 aparecer antes de qualquer H010, o factor vai ser 1 (padrão).
            # (Não trava; só registra warning se for o caso e multiply_by_qtd estiver ligado)
            if multiply_by_qtd and h010_seen == 0:
                warnings += 1
                factor = Decimal("1")

            bc_total = bc * factor
            vl_total = vl * factor

            rec = agg[cst or "(vazio)"]
            rec["count_h020"] += 1
            if multiply_by_qtd:
                rec["sum_qtd"] += factor
            rec["sum_bc"] += bc_total
            rec["sum_vl"] += vl_total

            if vl_total >= 0:
                rec["sum_credito"] += vl_total
            else:
                rec["sum_debito"] += abs(vl_total)

        # Atualiza status periodicamente (sem travar a UI)
        if lines % 5000 == 0:
            q.put(("status", f"Processando... {lines:,} linhas lidas | H010: {h010_seen} | H020: {h020_seen}"))

    # Ordena CST (tenta numérico, senão textual)
    def cst_sort_key(k):
        kk = k.replace("(vazio)", "").strip()
        try:
            return (0, int(kk))
        except Exception:
            return (1, kk)

    rows = []
    total_bc = Decimal("0")
    total_vl = Decimal("0")
    total_cred = Decimal("0")
    total_deb = Decimal("0")
    total_regs = 0
    total_qtd = Decimal("0")

    for cst in sorted(agg.keys(), key=cst_sort_key):
        rec = agg[cst]
        total_regs += rec["count_h020"]
        total_bc += rec["sum_bc"]
        total_vl += rec["sum_vl"]
        total_cred += rec["sum_credito"]
        total_deb += rec["sum_debito"]
        total_qtd += rec["sum_qtd"]

        rows.append((
            cst,
            rec["count_h020"],
            rec["sum_qtd"] if multiply_by_qtd else None,
            rec["sum_bc"],
            rec["sum_vl"],
            rec["sum_credito"],
            rec["sum_debito"],
        ))

    q.put(("done", {
        "path": path,
        "lines": lines,
        "h010_seen": h010_seen,
        "h020_seen": h020_seen,
        "warnings": warnings,
        "multiply_by_qtd": multiply_by_qtd,
        "rows": rows,
        "totals": {
            "regs": total_regs,
            "qtd": total_qtd,
            "bc": total_bc,
            "vl": total_vl,
            "credito": total_cred,
            "debito": total_deb
        }
    }))


# =========================
# UI Tkinter
# =========================
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("SPED - Totalizador H020 por CST (BC ICMS e VL ICMS)")
        self.geometry("1050x650")
        self.minsize(950, 600)

        self.queue = Queue()
        self.worker = None

        self.path_var = tk.StringVar(value="")
        self.multiply_var = tk.BooleanVar(value=True)

        self._build_ui()
        self.after(150, self._poll_queue)

    def _build_ui(self):
        pad = 10

        # Top controls
        top = ttk.Frame(self, padding=pad)
        top.pack(fill="x")

        ttk.Label(top, text="Arquivo SPED:").grid(row=0, column=0, sticky="w")
        self.path_entry = ttk.Entry(top, textvariable=self.path_var)
        self.path_entry.grid(row=0, column=1, sticky="we", padx=(8, 8))

        ttk.Button(top, text="Selecionar...", command=self.pick_file).grid(row=0, column=2, padx=(0, 8))
        ttk.Button(top, text="Processar", command=self.on_process).grid(row=0, column=3)

        top.columnconfigure(1, weight=1)

        # Options
        opts = ttk.Frame(self, padding=(pad, 0, pad, 0))
        opts.pack(fill="x")

        ttk.Checkbutton(
            opts,
            text="Multiplicar BC_ICMS e VL_ICMS do H020 pela QTD do H010 (recomendado)",
            variable=self.multiply_var
        ).pack(anchor="w")

        # Status
        self.status_var = tk.StringVar(value="Selecione um arquivo SPED e clique em Processar.")
        status = ttk.Label(self, textvariable=self.status_var, padding=(pad, 6))
        status.pack(fill="x")

        # Table
        table_frame = ttk.Frame(self, padding=pad)
        table_frame.pack(fill="both", expand=True)

        columns = ("cst", "regs", "qtd", "bc", "vl", "credito", "debito")
        self.tree = ttk.Treeview(table_frame, columns=columns, show="headings", height=16)

        self.tree.heading("cst", text="CST_ICMS")
        self.tree.heading("regs", text="Registros H020")
        self.tree.heading("qtd", text="Soma QTD (H010)")
        self.tree.heading("bc", text="BC_ICMS (Total)")
        self.tree.heading("vl", text="VL_ICMS (Saldo)")
        self.tree.heading("credito", text="VL_ICMS Crédito")
        self.tree.heading("debito", text="VL_ICMS Débito")

        self.tree.column("cst", width=90, anchor="center")
        self.tree.column("regs", width=110, anchor="center")
        self.tree.column("qtd", width=140, anchor="e")
        self.tree.column("bc", width=150, anchor="e")
        self.tree.column("vl", width=150, anchor="e")
        self.tree.column("credito", width=150, anchor="e")
        self.tree.column("debito", width=150, anchor="e")

        vsb = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(table_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")

        table_frame.rowconfigure(0, weight=1)
        table_frame.columnconfigure(0, weight=1)

        # Totals panel
        totals = ttk.Frame(self, padding=(pad, 0, pad, pad))
        totals.pack(fill="x")

        self.totals_var = tk.StringVar(value="Totais: -")
        ttk.Label(totals, textvariable=self.totals_var).pack(side="left")

        ttk.Button(totals, text="Copiar tabela (TSV)", command=self.copy_table).pack(side="right")

    def pick_file(self):
        path = filedialog.askopenfilename(
            title="Selecione o arquivo SPED",
            filetypes=[("Arquivos SPED/TXT", "*.txt *.sped *.efd *.dat *.*"), ("Todos", "*.*")]
        )
        if path:
            self.path_var.set(path)

    def on_process(self):
        path = self.path_var.get().strip()
        if not path:
            messagebox.showwarning("Atenção", "Selecione um arquivo SPED.")
            return
        if not os.path.exists(path):
            messagebox.showerror("Erro", "Arquivo não encontrado.")
            return
        if self.worker and self.worker.is_alive():
            messagebox.showinfo("Processando", "Já existe um processamento em andamento.")
            return

        # limpa tabela
        for item in self.tree.get_children():
            self.tree.delete(item)

        self.totals_var.set("Totais: -")
        self.status_var.set("Iniciando processamento...")

        multiply = bool(self.multiply_var.get())
        self.worker = threading.Thread(
            target=process_sped_h020,
            args=(path, multiply, self.queue),
            daemon=True
        )
        self.worker.start()

    def _poll_queue(self):
        try:
            while True:
                msg = self.queue.get_nowait()
                kind = msg[0]

                if kind == "status":
                    self.status_var.set(msg[1])

                elif kind == "done":
                    data = msg[1]
                    self._render_results(data)

        except Empty:
            pass

        self.after(150, self._poll_queue)

    def _render_results(self, data: dict):
        rows = data["rows"]
        totals = data["totals"]
        multiply = data["multiply_by_qtd"]

        # render rows
        for cst, regs, qtd_sum, bc_sum, vl_sum, cred, deb in rows:
            qtd_disp = fmt_money_ptbr(qtd_sum, places=3) if (multiply and qtd_sum is not None) else "-"
            self.tree.insert("", "end", values=(
                cst,
                regs,
                qtd_disp,
                fmt_money_ptbr(bc_sum, 2),
                fmt_money_ptbr(vl_sum, 2),
                fmt_money_ptbr(cred, 2),
                fmt_money_ptbr(deb, 2),
            ))

        tot_qtd_disp = fmt_money_ptbr(totals["qtd"], places=3) if multiply else "-"
        self.totals_var.set(
            "Totais | "
            f"H020: {totals['regs']} | "
            f"QTD: {tot_qtd_disp} | "
            f"BC_ICMS: {fmt_money_ptbr(totals['bc'])} | "
            f"VL_ICMS (saldo): {fmt_money_ptbr(totals['vl'])} | "
            f"Crédito: {fmt_money_ptbr(totals['credito'])} | "
            f"Débito: {fmt_money_ptbr(totals['debito'])}"
        )

        self.status_var.set(
            f"Concluído. Linhas lidas: {data['lines']:,} | "
            f"H010: {data['h010_seen']} | H020: {data['h020_seen']} | "
            f"Avisos: {data['warnings']} | "
            f"Arquivo: {os.path.basename(data['path'])}"
        )

    def copy_table(self):
        """
        Copia tabela para área de transferência como TSV (cola direto no Excel).
        """
        headers = ["CST_ICMS", "Registros_H020", "Soma_QTD(H010)", "BC_ICMS_Total", "VL_ICMS_Saldo", "VL_ICMS_Credito", "VL_ICMS_Debito"]
        lines = ["\t".join(headers)]
        for iid in self.tree.get_children():
            vals = self.tree.item(iid, "values")
            lines.append("\t".join(str(v) for v in vals))

        text = "\n".join(lines)
        self.clipboard_clear()
        self.clipboard_append(text)
        self.update()
        messagebox.showinfo("Copiado", "Tabela copiada (TSV). Cole no Excel/Sheets.")


if __name__ == "__main__":
    try:
        # Usa tema mais moderno quando disponível (Windows)
        from ctypes import windll  # noqa
        windll.shcore.SetProcessDpiAwareness(1)
    except Exception:
        pass

    app = App()
    app.mainloop()