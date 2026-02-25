import os
import re
import csv
import threading
from datetime import datetime
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

# =========================
# Regex NF-e (bytes)
# =========================
RE_ID_NFE = re.compile(rb'Id="NFe(\d{44})"')
RE_CHNFE = re.compile(rb"<chNFe>(\d{44})</chNFe>")
RE_ANY_44 = re.compile(rb"(?<!\d)\d{44}(?!\d)")
RE_TPNF = re.compile(rb"<tpNF>([01])</tpNF>")  # 0 entrada / 1 saída

# SPED
# |0000|COD_VER|COD_FIN|DT_INI|DT_FIN|NOME|CNPJ|CPF|UF|IE|...
RE_0000 = re.compile(rb"^\|0000\|")

def only_digits(s: str) -> str:
    return "".join(ch for ch in (s or "") if ch.isdigit())

def cnpj_from_key(chave44: str) -> str:
    """
    Extrai CNPJ do emitente da chave NF-e.
    Estrutura: UF(2)+AAMM(4)+CNPJ(14)+...
    índice: chave[6:20] (0-based)
    """
    if not chave44 or len(chave44) != 44 or not chave44.isdigit():
        return ""
    return chave44[6:20]

def chave_comparador(chave_44: str, modo: str) -> str:
    """
    modo:
      - prefixo34: UF+AAMM+CNPJ+mod+serie+nNF (34) ignora tpEmis/cNF/cDV
      - full44: chave completa
    """
    if not chave_44 or len(chave_44) != 44 or not chave_44.isdigit():
        return ""
    return chave_44[:34] if modo == "prefixo34" else chave_44

def extrair_chave_e_tipo_xml(xml_path: str):
    """
    Retorna: (chave44|None, tpNF|None, obs)
      tpNF: 0 entrada / 1 saída (se encontrar), senão None
    """
    try:
        with open(xml_path, "rb") as f:
            content = f.read()
    except Exception as e:
        return None, None, f"erro_leitura_xml: {e}"

    tp = None
    mt = RE_TPNF.search(content)
    if mt:
        try:
            tp = int(mt.group(1).decode("ascii"))
        except Exception:
            tp = None

    m = RE_ID_NFE.search(content)
    if m:
        return m.group(1).decode("ascii", errors="ignore"), tp, "ok_id_NFe"

    m = RE_CHNFE.search(content)
    if m:
        return m.group(1).decode("ascii", errors="ignore"), tp, "ok_chNFe"

    candidatos = [c.decode("ascii", errors="ignore") for c in RE_ANY_44.findall(content)]
    vistos = set()
    candidatos = [c for c in candidatos if not (c in vistos or vistos.add(c))]
    if not candidatos:
        return None, tp, "sem_chave_no_xml"

    return candidatos[0], tp, "ok_fallback_44"

def iterar_xmls(xml_dir: str):
    for root, _, files in os.walk(xml_dir):
        for name in files:
            if name.lower().endswith(".xml"):
                yield os.path.join(root, name)

def ler_cnpj_escrituracao_0000(sped_path: str) -> str:
    """
    Lê o CNPJ do registro 0000 do SPED (EFD ICMS/IPI).
    Retorna 14 dígitos ou "" se não achar.
    """
    try:
        with open(sped_path, "rb") as f:
            for raw in f:
                line = raw.strip()
                if not line:
                    continue
                if RE_0000.match(line):
                    parts = line.split(b"|")
                    # ['', '0000', COD_VER, COD_FIN, DT_INI, DT_FIN, NOME, CNPJ, CPF, UF, IE, ...]
                    if len(parts) > 7:
                        cnpj = parts[7].decode("latin1", errors="ignore").strip()
                        cnpj = only_digits(cnpj)
                        if len(cnpj) == 14:
                            return cnpj
                    break
    except Exception:
        pass
    return ""

def coletar_chaves_sped_c100(sped_path: str, incluir_entrada: bool, incluir_saida: bool):
    """
    Coleta CHV_NFE do C100, filtrando por IND_OPER:
      IND_OPER 0 = entrada
      IND_OPER 1 = saída

    Retorna: (set_chaves44, total_linhas_c100, total_filtradas_por_tipo)
    """
    chaves = set()
    total_c100 = 0
    filtradas = 0

    with open(sped_path, "rb") as f:
        for raw in f:
            line = raw.strip()
            if not line or not line.startswith(b"|C100|"):
                continue

            parts = line.split(b"|")
            if len(parts) <= 9:
                continue

            # parts[2] = IND_OPER (0/1)
            ind_oper = parts[2].decode("ascii", errors="ignore").strip()
            if ind_oper not in ("0", "1"):
                continue

            total_c100 += 1
            tipo = int(ind_oper)

            if (tipo == 0 and not incluir_entrada) or (tipo == 1 and not incluir_saida):
                filtradas += 1
                continue

            chave = parts[9].decode("ascii", errors="ignore").strip()
            if len(chave) == 44 and chave.isdigit():
                chaves.add(chave)

    return chaves, total_c100, filtradas

def coletar_chaves_sped_any44(sped_path: str):
    """
    Coleta qualquer 44 dígitos do SPED (SEM tipo).
    Retorna: (set_chaves44, total_ocorrencias)
    """
    chaves = set()
    total = 0
    with open(sped_path, "rb") as f:
        for line in f:
            for m in RE_ANY_44.finditer(line):
                chave = m.group(0).decode("ascii", errors="ignore")
                if len(chave) == 44 and chave.isdigit():
                    chaves.add(chave)
                    total += 1
    return chaves, total

def passa_filtro_cnpj_emitente(chave44: str, cnpj_escr: str, modo_cnpj: str) -> bool:
    """
    modo_cnpj:
      - "todos": não filtra
      - "igual": somente se CNPJ do emitente na chave == CNPJ 0000
      - "diferente": somente se CNPJ do emitente na chave != CNPJ 0000
    """
    if modo_cnpj == "todos":
        return True

    emit = cnpj_from_key(chave44)
    if not emit or len(emit) != 14:
        # se a chave é inválida, não passa
        return False
    if not cnpj_escr:
        # se não conseguiu ler o CNPJ do 0000, não dá para aplicar igual/diferente com segurança
        return False

    if modo_cnpj == "igual":
        return emit == cnpj_escr
    if modo_cnpj == "diferente":
        return emit != cnpj_escr
    return True


# =========================
# Tkinter App
# =========================
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Comparador XML x SPED (NF-e) - Entrada/Saída + CNPJ emitente")
        self.geometry("1060x820")
        self.minsize(980, 700)

        base_dir = os.path.dirname(os.path.abspath(__file__))
        self.default_out = os.path.join(base_dir, "saida_relatorio")

        self.var_xml_dir = tk.StringVar()
        self.var_sped_file = tk.StringVar()
        self.var_out_dir = tk.StringVar(value=self.default_out)

        # Tipos
        self.var_incluir_entrada = tk.BooleanVar(value=True)
        self.var_incluir_saida = tk.BooleanVar(value=True)

        # SPED leitura
        self.var_sped_mode = tk.StringVar(value="c100")  # c100 | any44

        # Comparação
        self.var_compare_mode = tk.StringVar(value="prefixo34")  # prefixo34 | full44

        # ✅ Filtro CNPJ emitente (da chave)
        self.var_cnpj_mode = tk.StringVar(value="todos")  # todos | igual | diferente

        self._running = False
        self._xml_missing_cache = []
        self._sped_missing_cache = []

        self._build_ui()

    def _build_ui(self):
        pad = 10

        frm_top = ttk.Frame(self, padding=pad)
        frm_top.pack(fill="x")

        ttk.Label(frm_top, text="Pasta com XML (varre subpastas):").grid(row=0, column=0, sticky="w")
        ttk.Entry(frm_top, textvariable=self.var_xml_dir).grid(row=1, column=0, sticky="ew", padx=(0, 8))
        ttk.Button(frm_top, text="Selecionar pasta", command=self.pick_xml_dir).grid(row=1, column=1, sticky="ew")

        ttk.Label(frm_top, text="Arquivo SPED EFD ICMS/IPI (.txt):").grid(row=2, column=0, sticky="w", pady=(10, 0))
        ttk.Entry(frm_top, textvariable=self.var_sped_file).grid(row=3, column=0, sticky="ew", padx=(0, 8))
        ttk.Button(frm_top, text="Selecionar arquivo", command=self.pick_sped_file).grid(row=3, column=1, sticky="ew")

        ttk.Label(frm_top, text="Pasta de saída (relatórios):").grid(row=4, column=0, sticky="w", pady=(10, 0))
        ttk.Entry(frm_top, textvariable=self.var_out_dir).grid(row=5, column=0, sticky="ew", padx=(0, 8))
        ttk.Button(frm_top, text="Selecionar pasta", command=self.pick_out_dir).grid(row=5, column=1, sticky="ew")

        frm_top.columnconfigure(0, weight=1)

        frm_opts = ttk.LabelFrame(self, text="Opções", padding=pad)
        frm_opts.pack(fill="x", padx=pad, pady=(0, pad))

        # Entrada/Saída
        ttk.Label(frm_opts, text="Tipos para considerar:").grid(row=0, column=0, sticky="w")
        ttk.Checkbutton(frm_opts, text="Entrada (tpNF/IND_OPER = 0)", variable=self.var_incluir_entrada)\
            .grid(row=1, column=0, sticky="w")
        ttk.Checkbutton(frm_opts, text="Saída (tpNF/IND_OPER = 1)", variable=self.var_incluir_saida)\
            .grid(row=2, column=0, sticky="w")

        ttk.Separator(frm_opts, orient="horizontal").grid(row=3, column=0, sticky="ew", pady=10)

        # ✅ Filtro CNPJ emitente (da chave)
        ttk.Label(frm_opts, text="Filtro por CNPJ do EMITENTE na chave (chave[6:20]) vs CNPJ do 0000:").grid(row=4, column=0, sticky="w")

        ttk.Radiobutton(frm_opts, text="Não filtrar (aceitar emitente = empresa e terceiros)",
                        value="todos", variable=self.var_cnpj_mode).grid(row=5, column=0, sticky="w")
        ttk.Radiobutton(frm_opts, text="Somente emitente = CNPJ da escrituração (0000)",
                        value="igual", variable=self.var_cnpj_mode).grid(row=6, column=0, sticky="w")
        ttk.Radiobutton(frm_opts, text="Somente emitente ≠ CNPJ da escrituração (terceiros)",
                        value="diferente", variable=self.var_cnpj_mode).grid(row=7, column=0, sticky="w")

        ttk.Separator(frm_opts, orient="horizontal").grid(row=8, column=0, sticky="ew", pady=10)

        # SPED leitura
        ttk.Label(frm_opts, text="Modo de leitura do SPED:").grid(row=9, column=0, sticky="w")
        ttk.Radiobutton(frm_opts, text="Somente C100 (CHV_NFE) — recomendado (permite Entrada/Saída)",
                        value="c100", variable=self.var_sped_mode).grid(row=10, column=0, sticky="w")
        ttk.Radiobutton(frm_opts, text="Qualquer 44 dígitos — fallback (não garante Entrada/Saída)",
                        value="any44", variable=self.var_sped_mode).grid(row=11, column=0, sticky="w")

        ttk.Separator(frm_opts, orient="horizontal").grid(row=12, column=0, sticky="ew", pady=10)

        # Comparar por
        ttk.Label(frm_opts, text="Comparar por:").grid(row=13, column=0, sticky="w")
        ttk.Radiobutton(frm_opts, text="Prefixo (34) — ignora tpEmis/cNF/cDV",
                        value="prefixo34", variable=self.var_compare_mode).grid(row=14, column=0, sticky="w")
        ttk.Radiobutton(frm_opts, text="Chave completa (44) — inclui tpEmis/cNF/cDV",
                        value="full44", variable=self.var_compare_mode).grid(row=15, column=0, sticky="w")

        # Ação
        frm_action = ttk.Frame(self, padding=(pad, 0, pad, pad))
        frm_action.pack(fill="x")

        self.btn_run = ttk.Button(frm_action, text="Executar comparação", command=self.start_compare)
        self.btn_run.pack(side="right")

        self.pb = ttk.Progressbar(frm_action, mode="indeterminate")
        self.pb.pack(side="right", padx=(0, 10), fill="x", expand=True)

        # KPIs
        frm_kpi = ttk.LabelFrame(self, text="KPIs", padding=pad)
        frm_kpi.pack(fill="x", padx=pad, pady=(0, pad))

        self.k_total = ttk.Label(frm_kpi, text="XML analisados: 0")
        self.k_sem = ttk.Label(frm_kpi, text="XML sem chave: 0")
        self.k_xml = ttk.Label(frm_kpi, text="Chaves únicas (XML): 0")
        self.k_sped = ttk.Label(frm_kpi, text="Chaves únicas (SPED): 0")
        self.k_xml_only = ttk.Label(frm_kpi, text="XML faltando no SPED: 0")
        self.k_sped_only = ttk.Label(frm_kpi, text="SPED faltando na pasta XML: 0")

        self.k_total.grid(row=0, column=0, sticky="w")
        self.k_sem.grid(row=0, column=1, sticky="w", padx=(20, 0))
        self.k_xml.grid(row=1, column=0, sticky="w", pady=(8, 0))
        self.k_sped.grid(row=1, column=1, sticky="w", padx=(20, 0), pady=(8, 0))
        self.k_xml_only.grid(row=2, column=0, sticky="w", pady=(8, 0))
        self.k_sped_only.grid(row=2, column=1, sticky="w", padx=(20, 0), pady=(8, 0))

        # Abas resultado/log
        nb = ttk.Notebook(self)
        nb.pack(fill="both", expand=True, padx=pad, pady=(0, pad))

        tab1 = ttk.Frame(nb, padding=pad)
        nb.add(tab1, text="XML faltando no SPED")
        self.lbl_xml_missing = ttk.Label(tab1, text="0 chave(s) do XML não encontrada(s) no SPED")
        self.lbl_xml_missing.pack(anchor="w")
        self.btn_copy_xml_missing = ttk.Button(tab1, text="Copiar todas", command=self.copy_xml_missing)
        self.btn_copy_xml_missing.pack(anchor="e", pady=(0, 6))
        self.btn_copy_xml_missing.configure(state="disabled")
        self.txt_xml_missing = tk.Text(tab1, height=10)
        self.txt_xml_missing.pack(fill="both", expand=True)
        self.txt_xml_missing.configure(state="disabled")

        tab2 = ttk.Frame(nb, padding=pad)
        nb.add(tab2, text="SPED faltando na pasta XML")
        self.lbl_sped_missing = ttk.Label(tab2, text="0 chave(s) do SPED não encontrada(s) na pasta XML")
        self.lbl_sped_missing.pack(anchor="w")
        self.btn_copy_sped_missing = ttk.Button(tab2, text="Copiar todas", command=self.copy_sped_missing)
        self.btn_copy_sped_missing.pack(anchor="e", pady=(0, 6))
        self.btn_copy_sped_missing.configure(state="disabled")
        self.txt_sped_missing = tk.Text(tab2, height=10)
        self.txt_sped_missing.pack(fill="both", expand=True)
        self.txt_sped_missing.configure(state="disabled")

        tab_log = ttk.Frame(nb, padding=pad)
        nb.add(tab_log, text="Log")
        self.txt_log = tk.Text(tab_log)
        self.txt_log.pack(fill="both", expand=True)
        self.txt_log.configure(state="disabled")

    # ---- UI helpers ----
    def ui_log(self, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {msg}\n"
        self.txt_log.configure(state="normal")
        self.txt_log.insert("end", line)
        self.txt_log.see("end")
        self.txt_log.configure(state="disabled")

    def ui_clear_log(self):
        self.txt_log.configure(state="normal")
        self.txt_log.delete("1.0", "end")
        self.txt_log.configure(state="disabled")

    def ui_set_kpis(self, total_xml, sem_chave, unicas_xml, unicas_sped, xml_missing, sped_missing):
        self.k_total.configure(text=f"XML analisados: {total_xml}")
        self.k_sem.configure(text=f"XML sem chave: {sem_chave}")
        self.k_xml.configure(text=f"Chaves únicas (XML): {unicas_xml}")
        self.k_sped.configure(text=f"Chaves únicas (SPED): {unicas_sped}")
        self.k_xml_only.configure(text=f"XML faltando no SPED: {xml_missing}")
        self.k_sped_only.configure(text=f"SPED faltando na pasta XML: {sped_missing}")

    def ui_set_xml_missing(self, keys):
        self._xml_missing_cache = keys[:]
        self.lbl_xml_missing.configure(text=f"{len(keys)} chave(s) do XML não encontrada(s) no SPED")
        self.txt_xml_missing.configure(state="normal")
        self.txt_xml_missing.delete("1.0", "end")
        if keys:
            self.txt_xml_missing.insert("end", "\n".join(keys))
        self.txt_xml_missing.configure(state="disabled")
        self.btn_copy_xml_missing.configure(state=("normal" if keys else "disabled"))

    def ui_set_sped_missing(self, keys):
        self._sped_missing_cache = keys[:]
        self.lbl_sped_missing.configure(text=f"{len(keys)} chave(s) do SPED não encontrada(s) na pasta XML")
        self.txt_sped_missing.configure(state="normal")
        self.txt_sped_missing.delete("1.0", "end")
        if keys:
            self.txt_sped_missing.insert("end", "\n".join(keys))
        self.txt_sped_missing.configure(state="disabled")
        self.btn_copy_sped_missing.configure(state=("normal" if keys else "disabled"))

    def copy_xml_missing(self):
        if not self._xml_missing_cache:
            return
        self.clipboard_clear()
        self.clipboard_append("\n".join(self._xml_missing_cache))
        self.update()
        messagebox.showinfo("Copiado", "Lista (XML faltando no SPED) copiada.")

    def copy_sped_missing(self):
        if not self._sped_missing_cache:
            return
        self.clipboard_clear()
        self.clipboard_append("\n".join(self._sped_missing_cache))
        self.update()
        messagebox.showinfo("Copiado", "Lista (SPED faltando na pasta XML) copiada.")

    # ---- Pickers ----
    def pick_xml_dir(self):
        path = filedialog.askdirectory(title="Selecione a pasta com XML")
        if path:
            self.var_xml_dir.set(path)

    def pick_sped_file(self):
        path = filedialog.askopenfilename(
            title="Selecione o SPED (TXT)",
            filetypes=[("Arquivo TXT", "*.txt"), ("Todos", "*.*")]
        )
        if path:
            self.var_sped_file.set(path)

    def pick_out_dir(self):
        path = filedialog.askdirectory(title="Selecione a pasta de saída")
        if path:
            self.var_out_dir.set(path)

    # ---- Execução ----
    def start_compare(self):
        if self._running:
            return

        xml_dir = self.var_xml_dir.get().strip()
        sped_file = self.var_sped_file.get().strip()
        out_dir = self.var_out_dir.get().strip()

        if not xml_dir or not os.path.isdir(xml_dir):
            messagebox.showerror("Erro", "Selecione uma pasta de XML válida.")
            return
        if not sped_file or not os.path.isfile(sped_file):
            messagebox.showerror("Erro", "Selecione um arquivo SPED válido.")
            return
        if not out_dir:
            messagebox.showerror("Erro", "Selecione uma pasta de saída válida.")
            return

        if not (self.var_incluir_entrada.get() or self.var_incluir_saida.get()):
            messagebox.showerror("Erro", "Marque pelo menos um tipo: Entrada e/ou Saída.")
            return

        # Se vai filtrar por CNPJ e precisa do 0000, garantimos que dá pra ler
        cnpj_mode = self.var_cnpj_mode.get().strip()
        if cnpj_mode in ("igual", "diferente"):
            cnpj_0000 = ler_cnpj_escrituracao_0000(sped_file)
            if not cnpj_0000:
                messagebox.showerror("Erro", "Não consegui ler o CNPJ do registro 0000 do SPED. Sem isso não dá para aplicar filtro 'igual/diferente'.")
                return

        self._running = True
        self.btn_run.configure(state="disabled")
        self.pb.start(10)

        self.ui_clear_log()
        self.ui_set_kpis(0, 0, 0, 0, 0, 0)
        self.ui_set_xml_missing([])
        self.ui_set_sped_missing([])

        threading.Thread(target=self._worker, daemon=True).start()

    def _finish(self):
        self._running = False
        self.btn_run.configure(state="normal")
        self.pb.stop()

    def _worker(self):
        xml_dir = self.var_xml_dir.get().strip()
        sped_file = self.var_sped_file.get().strip()
        out_dir = self.var_out_dir.get().strip()

        modo_sped = self.var_sped_mode.get().strip()
        modo_comp = self.var_compare_mode.get().strip()

        incluir_entrada = bool(self.var_incluir_entrada.get())
        incluir_saida = bool(self.var_incluir_saida.get())

        cnpj_mode = self.var_cnpj_mode.get().strip()
        cnpj_0000 = ler_cnpj_escrituracao_0000(sped_file)

        try:
            os.makedirs(out_dir, exist_ok=True)

            # ---- SPED ----
            self.after(0, lambda: self.ui_log(f"CNPJ do 0000 (escrituração): {cnpj_0000 or '(não lido)'}"))
            self.after(0, lambda: self.ui_log(f"Lendo SPED ({modo_sped})..."))

            if modo_sped == "c100":
                sped_44, total_c100, filtradas_tipo = coletar_chaves_sped_c100(
                    sped_file, incluir_entrada, incluir_saida
                )
                self.after(0, lambda: self.ui_log(f"C100 lidas: {total_c100} | filtradas fora por tipo: {filtradas_tipo}"))
            else:
                sped_44, total_occ = coletar_chaves_sped_any44(sped_file)
                self.after(0, lambda: self.ui_log("ATENÇÃO: modo any44 não garante filtro Entrada/Saída (sem IND_OPER)."))

            # aplica filtro CNPJ do emitente (pela chave)
            sped_44_filtrado = set()
            filtradas_cnpj_sped = 0
            for k in sped_44:
                if passa_filtro_cnpj_emitente(k, cnpj_0000, cnpj_mode):
                    sped_44_filtrado.add(k)
                else:
                    filtradas_cnpj_sped += 1

            self.after(0, lambda: self.ui_log(f"Filtro CNPJ emitente (SPED): modo='{cnpj_mode}' | removidas={filtradas_cnpj_sped}"))

            sped_keys = {chave_comparador(k, modo_comp) for k in sped_44_filtrado}
            sped_keys.discard("")
            self.after(0, lambda: self.ui_log(f"SPED chaves: {len(sped_44_filtrado)} (44 pós-filtro) | {len(sped_keys)} no modo '{modo_comp}'"))

            # ---- XML ----
            registros = []
            total_xml = 0
            sem_chave = 0
            xml_keys = set()

            filtradas_tipo_xml = 0
            filtradas_cnpj_xml = 0

            self.after(0, lambda: self.ui_log(f"Varredura XML em: {xml_dir}"))

            for xml_path in iterar_xmls(xml_dir):
                total_xml += 1
                chave_44, tpNF, obs = extrair_chave_e_tipo_xml(xml_path)

                if not chave_44:
                    sem_chave += 1
                    registros.append({
                        "arquivo_xml": xml_path,
                        "tpNF": "" if tpNF is None else str(tpNF),
                        "chave_xml_44": "",
                        "chave_xml_comp": "",
                        "considerado": "NAO",
                        "motivo_ignorado": "SEM_CHAVE",
                        "encontrada_no_sped": "",
                        "status": "SEM_CHAVE_NO_XML",
                        "observacao": obs,
                    })
                    continue

                # filtro por tipo (tpNF)
                considerado = True
                motivo = ""
                if tpNF is not None:
                    if (tpNF == 0 and not incluir_entrada) or (tpNF == 1 and not incluir_saida):
                        considerado = False
                        motivo = "IGNORADO_POR_TIPO"
                        filtradas_tipo_xml += 1

                # filtro por CNPJ do emitente (pela chave)
                if considerado:
                    if not passa_filtro_cnpj_emitente(chave_44, cnpj_0000, cnpj_mode):
                        considerado = False
                        motivo = "IGNORADO_POR_CNPJ_EMITENTE"
                        filtradas_cnpj_xml += 1

                xml_key = chave_comparador(chave_44, modo_comp)

                if considerado:
                    xml_keys.add(xml_key)
                    encontrada = "SIM" if (xml_key in sped_keys) else "NAO"
                    status = "OK" if encontrada == "SIM" else "NAO_ENCONTRADA_NO_SPED"
                else:
                    encontrada = ""
                    status = motivo

                registros.append({
                    "arquivo_xml": xml_path,
                    "tpNF": "" if tpNF is None else str(tpNF),
                    "chave_xml_44": chave_44,
                    "chave_xml_comp": xml_key,
                    "considerado": "SIM" if considerado else "NAO",
                    "motivo_ignorado": "" if considerado else motivo,
                    "encontrada_no_sped": encontrada,
                    "status": status,
                    "observacao": obs,
                })

                if total_xml % 50 == 0:
                    xml_missing_now = len([k for k in xml_keys if k not in sped_keys])
                    sped_missing_now = len([k for k in sped_keys if k not in xml_keys])
                    self.after(0, lambda tx=total_xml, sc=sem_chave, ux=len(xml_keys), us=len(sped_keys), xm=xml_missing_now, sm=sped_missing_now:
                               self.ui_set_kpis(tx, sc, ux, us, xm, sm))
                    self.after(0, lambda tx=total_xml: self.ui_log(f"Processados {tx} XML..."))

            self.after(0, lambda: self.ui_log(f"XML filtrados fora: por tipo={filtradas_tipo_xml} | por CNPJ emitente={filtradas_cnpj_xml}"))

            # ---- Duas vias ----
            xml_faltando_no_sped = sorted([k for k in xml_keys if k not in sped_keys])
            sped_faltando_na_pasta_xml = sorted([k for k in sped_keys if k not in xml_keys])

            self.after(0, lambda: self.ui_set_kpis(
                total_xml, sem_chave, len(xml_keys), len(sped_keys),
                len(xml_faltando_no_sped), len(sped_faltando_na_pasta_xml)
            ))

            # ---- Relatórios ----
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            rel_csv = os.path.join(out_dir, f"relatorio_xml_vs_sped_{ts}.csv")

            xml_txt = os.path.join(out_dir, f"xml_faltando_no_sped_{ts}.txt")
            xml_csv = os.path.join(out_dir, f"xml_faltando_no_sped_{ts}.csv")

            sped_txt = os.path.join(out_dir, f"sped_faltando_na_pasta_xml_{ts}.txt")
            sped_csv = os.path.join(out_dir, f"sped_faltando_na_pasta_xml_{ts}.csv")

            self.after(0, lambda: self.ui_log("Gerando relatórios..."))

            with open(rel_csv, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(
                    f,
                    fieldnames=[
                        "arquivo_xml", "tpNF", "chave_xml_44", "chave_xml_comp",
                        "considerado", "motivo_ignorado",
                        "encontrada_no_sped", "status", "observacao"
                    ],
                )
                w.writeheader()
                w.writerows(registros)

            with open(xml_txt, "w", encoding="utf-8") as f:
                for k in xml_faltando_no_sped:
                    f.write(k + "\n")
            with open(xml_csv, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["chave_comparada"])
                for k in xml_faltando_no_sped:
                    w.writerow([k])

            with open(sped_txt, "w", encoding="utf-8") as f:
                for k in sped_faltando_na_pasta_xml:
                    f.write(k + "\n")
            with open(sped_csv, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["chave_comparada"])
                for k in sped_faltando_na_pasta_xml:
                    w.writerow([k])

            def done():
                self.ui_log("Concluído ✅")
                self.ui_log(f"Relatório detalhado: {rel_csv}")
                self.ui_log(f"XML faltando no SPED: {xml_txt} | {xml_csv}")
                self.ui_log(f"SPED faltando na pasta XML: {sped_txt} | {sped_csv}")

                self.ui_set_xml_missing(xml_faltando_no_sped)
                self.ui_set_sped_missing(sped_faltando_na_pasta_xml)

                messagebox.showinfo("Finalizado", "Processo finalizado com sucesso.")
                self._finish()

            self.after(0, done)

        except Exception as ex:
            def err():
                self.ui_log(f"ERRO: {ex}")
                messagebox.showerror("Erro", str(ex))
                self._finish()
            self.after(0, err)


if __name__ == "__main__":
    App().mainloop()