# -*- coding: utf-8 -*-
r"""
Auditoria NF - App Simples (v3)
Requisitos do usuário atendidos:
- **SQL fixa no código-fonte** (nada de pasta \sql externa).
- **Apenas dois seletores de pasta na tela principal**:
   1) Selecionar a **pasta** onde está o banco -> o app usa `<pasta>/NOTAS_FISCAIS.FDB`.
   2) Selecionar a **pasta** onde serão salvos os **XML**.
- Conecta automaticamente na extração (se ainda não estiver conectado).

Dependências: sqlalchemy, python-firebird-driver (ou fdb)
"""
from __future__ import annotations

import datetime as dt
import json
import re
import threading
from pathlib import Path
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkinter.scrolledtext import ScrolledText

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

APP_NAME = "Auditoria NF - Simples (v3)"
BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = BASE_DIR / "config"
CONFIG_DIR.mkdir(parents=True, exist_ok=True)
SETTINGS_FILE = CONFIG_DIR / "settings.json"

DEFAULT_SETTINGS = {
    "fb_host": "localhost",
    "fb_port": 3050,
    "fb_user": "sysdba",
    "fb_password": "masterkey",
    "fb_charset": "UTF8",
    "fb_driver": "firebird",  # 'firebird' (python-firebird-driver) ou 'fdb'
    "fb_database": str(BASE_DIR / "NOTAS_FISCAIS.FDB"),
    "xml_folder": str(BASE_DIR / "xml"),
}

def load_settings() -> dict:
    if SETTINGS_FILE.exists():
        try:
            data = json.loads(SETTINGS_FILE.read_text(encoding="utf-8"))
            for k, v in DEFAULT_SETTINGS.items():
                data.setdefault(k, v)
            return data
        except Exception:
            pass
    return DEFAULT_SETTINGS.copy()

def save_settings(data: dict):
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    SETTINGS_FILE.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

def ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True); return p

def log_safe(widget: ScrolledText, msg: str):
    widget.configure(state="normal")
    widget.insert("end", f"{dt.datetime.now():%Y-%m-%d %H:%M:%S}  {msg}\n")
    widget.see("end")
    widget.configure(state="disabled")

class SAAdapter:
    def __init__(self):
        self.engine: Engine | None = None
        self.driver: str = "firebird"

    def build_uri(self, host: str, port: int, database_path: str, user: str, password: str, charset: str="UTF8")->str:
        db_path = database_path.replace("\\", "/")
        if host and str(host).strip():
            auth = f"{user}:{password}@{host}:{int(port)}"; path_part = f"/{db_path}"
        else:
            auth = ""; path_part = f"/{db_path}"
        uri = f"firebird+{self.driver}://{auth}{path_part}?charset={charset}"
        return uri

    def connect(self, host:str, port:int, database_path:str, user:str, password:str, charset:str="UTF8", driver:str="firebird"):
        from pathlib import Path as _P
        self.driver = driver
        _host = (host or "").strip().lower()
        # Se local, verifique existência do arquivo .FDB
        if not _host or _host in {"localhost", "127.0.0.1"}:
            if not _P(database_path).exists():
                raise FileNotFoundError(f"Arquivo .FDB não encontrado: {database_path}")
        uri = self.build_uri(host, port, database_path, user, password, charset)
        self.engine = create_engine(uri, pool_pre_ping=True, future=True)
        # valida rapidamente
        with self.engine.connect() as conn:
            conn.execute(text("SELECT 1 FROM RDB$DATABASE"))
        return self.engine

    def close(self):
        if self.engine:
            self.engine.dispose(close=True); self.engine = None

class SettingsDialog(tk.Toplevel):
    """Janela opcional para editar credenciais. Mantida separada para não poluir a tela principal."""
    def __init__(self, master, settings:dict, adapter:SAAdapter, log_widget: ScrolledText):
        super().__init__(master)
        self.title("Configurações - Firebird")
        self.settings = settings
        self.adapter = adapter
        self.log_widget = log_widget
        self.resizable(False, False)
        self.grab_set()
        self.transient(master)

        frm = ttk.Frame(self, padding=12); frm.grid(sticky="nsew")

        self.var_host = tk.StringVar(value=settings.get("fb_host","localhost"))
        self.var_port = tk.IntVar(value=int(settings.get("fb_port", 3050)))
        self.var_user = tk.StringVar(value=settings.get("fb_user","sysdba"))
        self.var_pw   = tk.StringVar(value=settings.get("fb_password","masterkey"))
        self.var_charset = tk.StringVar(value=settings.get("fb_charset","UTF8"))
        self.var_driver  = tk.StringVar(value=settings.get("fb_driver","firebird"))
        self.var_db = tk.StringVar(value=settings.get("fb_database", ""))

        row=0
        for lbl, var in [
            ("Servidor (host)", self.var_host),
            ("Porta", self.var_port),
            ("Usuário", self.var_user),
            ("Senha", self.var_pw),
            ("Charset", self.var_charset),
        ]:
            ttk.Label(frm, text=lbl).grid(row=row, column=0, sticky="w", pady=2)
            if isinstance(var, tk.IntVar):
                e = ttk.Entry(frm, textvariable=var, width=12)
            else:
                show = "*" if lbl=="Senha" else None
                e = ttk.Entry(frm, textvariable=var, width=32, show=show)
            e.grid(row=row, column=1, sticky="w", pady=2, padx=6); row+=1

        ttk.Label(frm, text="Driver").grid(row=row, column=0, sticky="w", pady=2)
        cb = ttk.Combobox(frm, textvariable=self.var_driver, values=["firebird","fdb"], state="readonly", width=10)
        cb.grid(row=row, column=1, sticky="w", pady=2, padx=6); row+=1

        ttk.Label(frm, text="Arquivo .FDB").grid(row=row, column=0, sticky="w", pady=2)
        e_db = ttk.Entry(frm, textvariable=self.var_db, width=44)
        e_db.grid(row=row, column=1, sticky="w", pady=2, padx=6)
        btns_db = ttk.Frame(frm); btns_db.grid(row=row, column=2, sticky="w")
        ttk.Button(btns_db, text="Selecionar .FDB...", command=self._pick_db).grid(row=0, column=0)
        ttk.Button(btns_db, text="Pasta do .FDB...", command=self._pick_db_folder).grid(row=0, column=1, padx=(6,0))
        row+=1

        btns = ttk.Frame(frm); btns.grid(row=row, column=0, columnspan=3, sticky="e", pady=(10,0))
        ttk.Button(btns, text="Testar conexão", command=self._test_conn).grid(row=0, column=0, padx=6)
        ttk.Button(btns, text="Salvar", command=self._save).grid(row=0, column=1, padx=6)
        ttk.Button(btns, text="Fechar", command=self.destroy).grid(row=0, column=2)

    def _pick_db(self):
        path = filedialog.askopenfilename(title="Selecione o arquivo .FDB", filetypes=[("Firebird DB","*.fdb *.FDB"),("Todos","*.*")])
        if path: self.var_db.set(path)

    def _pick_db_folder(self):
        folder = filedialog.askdirectory(title="Selecione a PASTA que contém NOTAS_FISCAIS.FDB")
        if not folder: return
        candidate = Path(folder) / "NOTAS_FISCAIS.FDB"
        if candidate.exists():
            self.var_db.set(str(candidate))
            try: messagebox.showinfo("Arquivo encontrado", f"Arquivo localizado:\n{candidate}")
            except Exception: pass
        else:
            messagebox.showerror("Arquivo não encontrado", f"Não encontrei 'NOTAS_FISCAIS.FDB' em:\n{folder}")

    def _test_conn(self):
        s = self._collect()
        try:
            engine = self.adapter.connect(
                host=s["fb_host"], port=int(s["fb_port"]), database_path=s["fb_database"],
                user=s["fb_user"], password=s["fb_password"], charset=s["fb_charset"], driver=s["fb_driver"]
            )
            if engine: engine.dispose()
            messagebox.showinfo("Conexão", "Conexão OK!")
            log_safe(self.log_widget, "Conexão Firebird bem-sucedida (teste).")
        except FileNotFoundError as e:
            messagebox.showerror("Arquivo não encontrado", str(e)); log_safe(self.log_widget, f"ERRO: {e}")
        except Exception as e:
            messagebox.showerror("Falha na conexão", str(e)); log_safe(self.log_widget, f"ERRO de conexão: {e}")

    def _save(self):
        save_settings(self._collect()); messagebox.showinfo("Configurações", "Configurações salvas."); self.destroy()

    def _collect(self):
        existing = load_settings()
        return {
            "fb_host": self.var_host.get().strip(),
            "fb_port": int(self.var_port.get() or 3050),
            "fb_user": self.var_user.get().strip(),
            "fb_password": self.var_pw.get(),
            "fb_charset": self.var_charset.get().strip() or "UTF8",
            "fb_driver": self.var_driver.get().strip() or "firebird",
            "fb_database": self.var_db.get().strip(),
            "xml_folder": existing.get("xml_folder", DEFAULT_SETTINGS["xml_folder"]),
        }

class SimpleApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_NAME); self.geometry("900x580"); self.minsize(860, 540)
        self.settings = load_settings(); self.adapter = SAAdapter()
        self.engine: Engine | None = None

        # MENU
        menubar = tk.Menu(self)
        m_app = tk.Menu(menubar, tearoff=False)
        m_app.add_command(label="Configurações (credenciais)...", command=self.open_settings)
        m_app.add_separator(); m_app.add_command(label="Sair", command=self.destroy)
        menubar.add_cascade(label="Aplicativo", menu=m_app); self.config(menu=menubar)

        root = ttk.Frame(self, padding=10); root.pack(fill="both", expand=True)

        # Linha 1: datas e tipo
        row1 = ttk.Frame(root); row1.pack(fill="x", pady=(0,8))
        ttk.Label(row1, text="Data inicial (YYYY-MM-DD)").pack(side="left")
        self.var_ini = tk.StringVar(value=dt.date.today().replace(day=1).isoformat())
        ttk.Entry(row1, textvariable=self.var_ini, width=14).pack(side="left", padx=(6,18))

        ttk.Label(row1, text="Data final (YYYY-MM-DD)").pack(side="left")
        self.var_fim = tk.StringVar(value=dt.date.today().isoformat())
        ttk.Entry(row1, textvariable=self.var_fim, width=14).pack(side="left", padx=(6,18))

        ttk.Label(row1, text="Tipo de nota").pack(side="left")
        self.var_tipo = tk.StringVar(value="Saída")
        ttk.Combobox(row1, textvariable=self.var_tipo, values=["Entrada","Saída"], state="readonly", width=10).pack(side="left", padx=(6,0))

        # Linha 2: Pasta do .FDB (seleciona PASTA, não arquivo)
        row2 = ttk.Frame(root); row2.pack(fill="x", pady=(0,8))
        ttk.Label(row2, text="Pasta do .FDB (contém NOTAS_FISCAIS.FDB)").pack(side="left")
        self.var_db_folder = tk.StringVar(value=str(Path(self.settings["fb_database"]).parent))
        self.var_db_full   = tk.StringVar(value=self.settings["fb_database"])
        ttk.Entry(row2, textvariable=self.var_db_full, width=64, state="readonly").pack(side="left", padx=6)
        ttk.Button(row2, text="Selecionar pasta do .FDB...", command=self.pick_db_folder).pack(side="left", padx=(6,0))

        # Linha 3: Pasta dos XML (saída)
        row3 = ttk.Frame(root); row3.pack(fill="x", pady=(0,8))
        ttk.Label(row3, text="Pasta dos XML (saída)").pack(side="left")
        self.var_xml = tk.StringVar(value=self.settings.get("xml_folder", DEFAULT_SETTINGS["xml_folder"]))
        ttk.Entry(row3, textvariable=self.var_xml, width=64).pack(side="left", padx=6)
        ttk.Button(row3, text="Selecionar pasta dos XML...", command=self.pick_xml_folder).pack(side="left", padx=(6,0))

        # Linha 4: ações
        row4 = ttk.Frame(root); row4.pack(fill="x", pady=(0,8))
        ttk.Button(row4, text="Testar conexão", command=self.on_test_conn).pack(side="left")
        ttk.Button(row4, text="Extrair XML", command=self.on_extract_xml).pack(side="left", padx=(12,0))

        # Log
        frm_log = ttk.LabelFrame(root, text="Console de Log"); frm_log.pack(fill="both", expand=True)
        self.txt_log = ScrolledText(frm_log, wrap="word", height=18, state="disabled")
        self.txt_log.pack(fill="both", expand=True, padx=8, pady=8)

        # Status
        self.var_status = tk.StringVar(value="Pronto."); ttk.Label(root, textvariable=self.var_status, anchor="w").pack(fill="x")
        self.after(200, lambda: log_safe(self.txt_log, "Aplicativo iniciado."))

        # Garante pasta XML existente no boot
        ensure_dir(Path(self.var_xml.get()))

    def open_settings(self):
        SettingsDialog(self, self.settings, self.adapter, self.txt_log)

    def pick_db_folder(self):
        folder = filedialog.askdirectory(title="Selecione a PASTA que contém NOTAS_FISCAIS.FDB")
        if not folder:
            return
        candidate = Path(folder) / "NOTAS_FISCAIS.FDB"
        if candidate.exists():
            # salva nas settings
            s = load_settings()
            s["fb_database"] = str(candidate)
            save_settings(s)
            self.settings = s
            self.var_db_folder.set(folder)
            self.var_db_full.set(str(candidate))
            log_safe(self.txt_log, f".FDB definido: {candidate}")
        else:
            messagebox.showerror("Arquivo não encontrado", f"Não encontrei 'NOTAS_FISCAIS.FDB' em:\n{folder}")

    def pick_xml_folder(self):
        folder = filedialog.askdirectory(title="Selecione a pasta de saída para os XML")
        if not folder:
            return
        ensure_dir(Path(folder))
        s = load_settings()
        s["xml_folder"] = folder
        save_settings(s)
        self.settings = s
        self.var_xml.set(folder)
        log_safe(self.txt_log, f"Pasta dos XML definida: {folder}")

    def _connect_if_needed(self):
        if self.adapter.engine:
            return True
        s = load_settings()
        try:
            self.adapter.connect(
                host=s["fb_host"], port=int(s["fb_port"]), database_path=s["fb_database"],
                user=s["fb_user"], password=s["fb_password"], charset=s["fb_charset"], driver=s["fb_driver"]
            )
            self.var_status.set("Conectado."); log_safe(self.txt_log, "Conectado ao Firebird.")
            return True
        except Exception as e:
            self.var_status.set("Erro de conexão."); log_safe(self.txt_log, f"ERRO ao conectar: {e}")
            messagebox.showerror("Conexão", f"Falha ao conectar: {e}")
            return False

    def on_test_conn(self):
        s = load_settings()
        try:
            engine = self.adapter.connect(
                host=s["fb_host"], port=int(s["fb_port"]), database_path=s["fb_database"],
                user=s["fb_user"], password=s["fb_password"], charset=s["fb_charset"], driver=s["fb_driver"]
            )
            if engine: self.adapter.close()
            messagebox.showinfo("Conexão", "Conexão OK!")
            log_safe(self.txt_log, "Conexão Firebird bem-sucedida (teste).")
        except Exception as e:
            messagebox.showerror("Falha na conexão", str(e)); log_safe(self.txt_log, f"ERRO de conexão: {e}")

    # ================== EXTRAÇÃO DE XML (SQL FIXA NO CÓDIGO) ==================
    def on_extract_xml(self):
        # Validar datas
        try:
            data_ini = dt.datetime.strptime(self.var_ini.get().strip(), "%Y-%m-%d")
            data_fim = dt.datetime.strptime(self.var_fim.get().strip(), "%Y-%m-%d") + dt.timedelta(days=1) - dt.timedelta(seconds=1)
            if data_ini > data_fim:
                raise ValueError("Data inicial maior que a final.")
        except Exception as e:
            messagebox.showerror("Datas inválidas", f"Revise as datas: {e}"); return

        # Conectar se preciso
        if not self._connect_if_needed():
            return

        # Pasta XML
        xml_dir = ensure_dir(Path(self.var_xml.get().strip()))
        tipo_str = self.var_tipo.get().strip().lower()
        origem = "entrada" if tipo_str.startswith("e") else "saida"
        log_safe(self.txt_log, f"Iniciando extração XML: origem={origem}, período {data_ini:%Y-%m-%d} a {data_fim:%Y-%m-%d}.")

        t = threading.Thread(target=self._extract_xml_thread, args=(xml_dir, origem, data_ini, data_fim), daemon=True)
        t.start()

    def _sanitize_name(self, s: str) -> str:
        s = (s or "").strip()
        s = re.sub(r"[^0-9A-Za-z_-]+", "", s)
        return s or "sem_chave"

    def _write_xml_file(self, xml_dir: Path, chavenfe: str, payload):
        fname = self._sanitize_name(chavenfe) + ".xml"
        path = xml_dir / fname
        if isinstance(payload, (bytes, bytearray)):
            data = bytes(payload); path.write_bytes(data)
        else:
            path.write_text(str(payload), encoding="utf-8", errors="replace")
        return path

    def _extract_xml_thread(self, xml_dir: Path, origem: str, dt_ini: dt.datetime, dt_fim: dt.datetime):
        try:
            count = 0; skipped = 0
            with self.adapter.engine.connect() as conn:
                # SQL fixa no fonte (conforme pedido)
                if origem == "entrada":
                    sql = text("""
                        SELECT CHAVENFE, ARQUIVOXML
                          FROM NOTASENTRADA
                         WHERE DATADOWNLOAD BETWEEN :ini AND :fim
                    """)
                else:
                    sql = text("""
                        SELECT CHAVENFE, ARQUIVOXML
                          FROM NOTASLOTENFE
                         WHERE DATAEMISSAO BETWEEN :ini AND :fim
                           AND STATUS IN (100, 150)
                    """)

                params = {"ini": dt_ini, "fim": dt_fim}
                res = conn.execution_options(stream_results=True).execute(sql, params)
                for chavenfe, blob in res:
                    if not blob:
                        skipped += 1
                        self.after(0, lambda c=chavenfe: log_safe(self.txt_log, f"Sem XML para chave: {c} (vazio)."))
                        continue
                    try:
                        p = self._write_xml_file(xml_dir, chavenfe, blob); count += 1
                        self.after(0, lambda s=str(p): log_safe(self.txt_log, f"Salvo: {s}"))
                    except Exception as e:
                        self.after(0, lambda c=chavenfe, err=str(e): log_safe(self.txt_log, f"ERRO ao salvar {c}: {err}"))

            self.after(0, lambda: log_safe(self.txt_log, f"Extração concluída. XMLs salvos: {count}. Ignorados (vazios): {skipped}."))
            self.after(0, lambda: self.var_status.set("Extração concluída."))
        except Exception as e:
            self.after(0, lambda: log_safe(self.txt_log, f"Falha na extração: {e}"))
            self.after(0, lambda: self.var_status.set("Erro na extração."))

if __name__ == "__main__":
    settings = load_settings()
    ensure_dir(Path(settings.get("xml_folder", DEFAULT_SETTINGS["xml_folder"])))
    app = SimpleApp()
    app.mainloop()
