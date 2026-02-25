# -*- coding: utf-8 -*-
"""
Executor SQL Multi‑Conexões – VERSÃO FINAL
-----------------------------------------
• Lista de conexões com checkbox (marcar p/ executar) + clique (selecionar p/ editar/excluir)
• Scroll vertical (altura ≈5 linhas) para qualquer quantidade de conexões
• Cadastro de usuários/senhas (criptografia Fernet) em arquivos JSON
• Execução paralela; SELECT → resultado em Notebook (1 aba por conexão)
• Área de log somente‑leitura
• 100 % funcional: adicionar/editar/excluir, múltiplos usuários, toggle all, etc.
"""

import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
import json
import os
import threading
from cryptography.fernet import Fernet
import psycopg2

# ────────────────────────── Arquivos / paths ─────────────────────────
CONFIG_DIR = "config"
CONEXOES_FILE = os.path.join(CONFIG_DIR, "conexoes.json")
USUARIOS_FILE = os.path.join(CONFIG_DIR, "usuarios.json")
CHAVE_FILE = os.path.join(CONFIG_DIR, "chave.key")

os.makedirs(CONFIG_DIR, exist_ok=True)
if not os.path.exists(CHAVE_FILE):
    with open(CHAVE_FILE, "wb") as f:
        f.write(Fernet.generate_key())
with open(CHAVE_FILE, "rb") as f:
    fernet = Fernet(f.read())

def carregar_json(path):
    if not os.path.exists(path):
        with open(path, "w") as f:
            json.dump([], f)
    with open(path, "r") as f:
        return json.load(f)

def salvar_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=4)

def criptografar(txt: str) -> str:
    return fernet.encrypt(txt.encode()).decode()

def descriptografar(txt: str) -> str:
    return fernet.decrypt(txt.encode()).decode()

# ────────────────────────── App principal ────────────────────────────
class App:
    def __init__(self, root: tk.Tk):
        self.root = root
        root.title("Executor SQL Multi‑Conexões")
        root.geometry("960x800")

        style = ttk.Style(root)
        style.configure("highlight.TFrame", background="#d9edf7")

        # Dados persistentes
        self.conexoes = carregar_json(CONEXOES_FILE)
        self.usuarios = carregar_json(USUARIOS_FILE)
        self.selected_idx = None  # linha clicada

        # ── Lista de conexões com scroll
        lf_con = ttk.LabelFrame(root, text="Conexões (✓ executa | clique seleciona)")
        lf_con.pack(fill="both", padx=10, pady=5)
        self._build_scrollable(lf_con)
        self._refresh_conexoes()

        # ── Botões principais
        frm_btn = ttk.Frame(root)
        frm_btn.pack(pady=4)
        actions = [
            ("Adicionar", self._add_conexao),
            ("Editar", self._edit_conexao),
            ("Excluir", self._del_conexao),
            ("Usuários", self._gerenciar_usuarios),
            ("(De)Selecionar Todos", self._toggle_all),
        ]
        for txt, cmd in actions:
            ttk.Button(frm_btn, text=txt, command=cmd).pack(side="left", padx=4)

        # ── Área SQL
        lf_sql = ttk.LabelFrame(root, text="Comando SQL")
        lf_sql.pack(fill="both", padx=10, pady=5, expand=True)
        self.text_sql = tk.Text(lf_sql, height=6)
        self.text_sql.pack(fill="both", expand=True)

        # Usuário executor
        ttk.Label(root, text="Usuário para executar:").pack()
        self.user_var = tk.StringVar()
        self.combo_user = ttk.Combobox(root, textvariable=self.user_var, values=[u["nome"] for u in self.usuarios])
        self.combo_user.pack()

        ttk.Button(root, text="Executar SQL", command=self._executar_sql).pack(pady=6)

        # ── Log
        lf_log = ttk.LabelFrame(root, text="Log")
        lf_log.pack(fill="both", padx=10, pady=5, expand=True)
        self.txt_log = tk.Text(lf_log, height=8, bg="black", fg="white", state="disabled")
        self.txt_log.pack(fill="both", expand=True)

        # ── Notebook de resultados
        lf_res = ttk.LabelFrame(root, text="Resultados (SELECT)")
        lf_res.pack(fill="both", padx=10, pady=5, expand=True)
        self.nb = ttk.Notebook(lf_res)
        self.nb.pack(fill="both", expand=True)

    def _build_scrollable(self, parent):
        canvas = tk.Canvas(parent, height=190)
        scrollbar = ttk.Scrollbar(parent, orient="vertical", command=canvas.yview)
        canvas.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side="right", fill="y")
        canvas.pack(side="left", fill="both", expand=True)

        self.frm_scroll = ttk.Frame(canvas)
        canvas.create_window((0, 0), window=self.frm_scroll, anchor="nw")
        self.frm_scroll.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        self.canvas = canvas

    def _refresh_conexoes(self):
        for w in self.frm_scroll.winfo_children():
            w.destroy()
        self.chk_vars = []
        for idx, cx in enumerate(self.conexoes):
            var = tk.BooleanVar(value=True)
            frame = ttk.Frame(self.frm_scroll)
            frame.pack(fill="x", anchor="w")
            chk = ttk.Checkbutton(frame, variable=var)
            chk.pack(side="left")
            lbl = ttk.Label(frame, text=f"{cx['nome']} ({cx['host']}:{cx['port']}/{cx['database']})", anchor="w")
            lbl.pack(side="left", fill="x", expand=True)
            frame.bind("<Button-1>", lambda e, i=idx, fr=frame: self._select_row(i, fr))
            lbl.bind("<Button-1>", lambda e, i=idx, fr=frame: self._select_row(i, fr))
            chk.bind("<Button-1>", lambda e: None)
            self.chk_vars.append((var, idx, frame))

    def _select_row(self, idx, row):
        for _, _, fr in self.chk_vars:
            fr.configure(style="TFrame")
        row.configure(style="highlight.TFrame")
        self.selected_idx = idx

    def _toggle_all(self):
        marcar = not all(var.get() for var, _, _ in self.chk_vars)
        for var, _, _ in self.chk_vars:
            var.set(marcar)

    def _add_conexao(self):
        self._form_conexao()

    def _edit_conexao(self):
        if self.selected_idx is None:
            messagebox.showwarning("Aviso", "Clique em uma conexão para editar.")
            return
        self._form_conexao(self.selected_idx)

    def _del_conexao(self):
        if self.selected_idx is None:
            messagebox.showwarning("Aviso", "Clique em uma conexão para excluir.")
            return
        cx = self.conexoes[self.selected_idx]
        if messagebox.askyesno("Confirm", f"Excluir conexão '{cx['nome']}'?"):
            del self.conexoes[self.selected_idx]
            salvar_json(CONEXOES_FILE, self.conexoes)
            self.selected_idx = None
            self._refresh_conexoes()

    def _form_conexao(self, editar_idx=None):
        win = tk.Toplevel(self.root)
        win.title("Conexão")
        win.geometry("420x260")
        ent = {}
        for campo in ("Nome", "Host", "Porta", "Banco"):
            ttk.Label(win, text=campo).pack()
            e = ttk.Entry(win)
            e.pack(fill="x")
            ent[campo.lower()] = e
        if editar_idx is not None:
            c = self.conexoes[editar_idx]
            ent["nome"].insert(0, c["nome"])
            ent["host"].insert(0, c["host"])
            ent["porta"].insert(0, c["port"])
            ent["banco"].insert(0, c["database"])
        def salvar():
            dados = {"nome": ent["nome"].get(), "host": ent["host"].get(), "port": ent["porta"].get(), "database": ent["banco"].get()}
            if not all(dados.values()):
                messagebox.showwarning("Aviso", "Preencha todos os campos.")
                return
            if editar_idx is None:
                self.conexoes.append(dados)
            else:
                self.conexoes[editar_idx] = dados
            salvar_json(CONEXOES_FILE, self.conexoes)
            self._refresh_conexoes()
            win.destroy()
        ttk.Button(win, text="Salvar", command=salvar).pack(pady=6)

    def _gerenciar_usuarios(self):
        win = tk.Toplevel(self.root)
        win.title("Usuários")
        win.geometry("400x300")

        lst = tk.Listbox(win)
        lst.pack(fill="both", expand=True)
        for u in self.usuarios:
            lst.insert("end", u["nome"])

        def add():
            nome = simpledialog.askstring("Usuário", "Nome do usuário:", parent=win)
            senha = simpledialog.askstring("Senha", "Senha do usuário:", parent=win, show="*")
            if nome and senha:
                self.usuarios.append({"nome": nome, "senha": criptografar(senha)})
                salvar_json(USUARIOS_FILE, self.usuarios)
                self.combo_user.config(values=[u["nome"] for u in self.usuarios])
                lst.insert("end", nome)
        def remover():
            sel = lst.curselection()
            if not sel:
                return
            idx = sel[0]
            if messagebox.askyesno("Confirma", f"Remover usuário {self.usuarios[idx]['nome']}?"):
                lst.delete(idx)
                del self.usuarios[idx]
                salvar_json(USUARIOS_FILE, self.usuarios)
                self.combo_user.config(values=[u["nome"] for u in self.usuarios])

        ttk.Button(win, text="Adicionar", command=add).pack(side="left", padx=10, pady=4)
        ttk.Button(win, text="Remover", command=remover).pack(side="left", padx=10, pady=4)

    def _executar_sql(self):
        sql = self.text_sql.get("1.0", "end").strip()
        if not sql:
            messagebox.showwarning("Aviso", "Digite um comando SQL.")
            return
        usuario = self.user_var.get()
        if not usuario:
            messagebox.showwarning("Aviso", "Selecione um usuário.")
            return
        cred = next((u for u in self.usuarios if u["nome"] == usuario), None)
        if not cred:
            messagebox.showerror("Erro", "Usuário não encontrado.")
            return
        senha = descriptografar(cred["senha"])
        for i in self.nb.tabs():
            self.nb.forget(i)
        self._log("Executando...\n")
        for var, idx, _ in self.chk_vars:
            if var.get():
                cx = self.conexoes[idx]
                threading.Thread(target=self._run_sql, args=(cx, cred["nome"], senha, sql)).start()

    def _run_sql(self, cx, user, senha, sql):
        try:
            conn = psycopg2.connect(host=cx["host"], port=cx["port"], user=user, password=senha, database=cx["database"])
            cur = conn.cursor()
            cur.execute(sql)
            if sql.strip().lower().startswith("select"):
                dados = cur.fetchall()
                colunas = [desc[0] for desc in cur.description]
                self._exibir_resultado(cx["nome"], colunas, dados)
            conn.commit()
            cur.close()
            conn.close()
            self._log(f"✅ [{cx['nome']}] Comando executado\n")
        except Exception as e:
            self._log(f"❌ [{cx['nome']}] Erro: {e}\n")

    def _exibir_resultado(self, nome, colunas, dados):
        frame = ttk.Frame(self.nb)
        tree = ttk.Treeview(frame, columns=colunas, show="headings")
        for col in colunas:
            tree.heading(col, text=col)
            tree.column(col, width=120)
        for row in dados:
            tree.insert("", "end", values=row)
        tree.pack(fill="both", expand=True)
        self.nb.add(frame, text=nome)

    def _log(self, txt):
        self.txt_log.config(state="normal")
        self.txt_log.insert("end", txt)
        self.txt_log.config(state="disabled")
        self.txt_log.see("end")

if __name__ == "__main__":
    root = tk.Tk()
    app = App(root)
    root.mainloop()
