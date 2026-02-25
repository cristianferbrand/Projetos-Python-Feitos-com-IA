
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import requests
import json
import threading

# Função para obter o token internamente (sem exibir)
def obter_token_interno():
    url = "http://autorizadorfarma.hos.com.br/HOSImendes/connect/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "grant_type": "client_credentials",
        "scope": "api",
        "client_id": "hosfarma",
        "client_secret": "HoS@44#00*"
    }
    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        return response.json()["access_token"]
    else:
        raise Exception(f"Erro ao obter token: {response.text}")

# Janela principal
janela = tk.Tk()
janela.title("Consulta de Tributação")
janela.geometry("1500x800")

# Frame parâmetros
frame_param = tk.LabelFrame(janela, text="Parâmetros")
frame_param.pack(fill="x", padx=10, pady=5)

tk.Label(frame_param, text="CNPJ:").grid(row=0, column=0, padx=5, sticky='e')
entrada_cnpj = tk.Entry(frame_param, width=18)
entrada_cnpj.insert(0, "00115150000140")
entrada_cnpj.grid(row=0, column=1)

tk.Label(frame_param, text="UF:").grid(row=0, column=2, padx=5, sticky='e')
entrada_uf = tk.Entry(frame_param, width=5)
entrada_uf.insert(0, "RS")
entrada_uf.grid(row=0, column=3)

tk.Label(frame_param, text="CRT:").grid(row=0, column=4, padx=5, sticky='e')
opcoes_crt = {
    "1 - Simples Nacional": "1",
    "2 - Simples Nacional - excesso de sublimite da receita bruta": "2",
    "3 - Regime Normal": "3"
}
entrada_crt = ttk.Combobox(frame_param, values=list(opcoes_crt.keys()), width=45)
entrada_crt.set("1 - Simples Nacional")
entrada_crt.grid(row=0, column=5)

tk.Label(frame_param, text="RegimeTrib:").grid(row=0, column=6, padx=5, sticky='e')
opcoes_regime = {
    "1 - Simples": "1",
    "2 - Lucro Presumido": "2",
    "3 - Lucro Real": "3"
}
entrada_regime = ttk.Combobox(frame_param, values=list(opcoes_regime.keys()), width=20)
entrada_regime.set("1 - Simples")
entrada_regime.grid(row=0, column=7)

tk.Label(frame_param, text="Ambiente:").grid(row=0, column=8, padx=5, sticky='e')
entrada_ambiente = tk.Entry(frame_param, width=5)
entrada_ambiente.insert(0, "2")
entrada_ambiente.grid(row=0, column=9)

var_icms = tk.BooleanVar(value=True)
tk.Checkbutton(frame_param, text="Contribuinte ICMS", variable=var_icms).grid(row=0, column=10, padx=5)

# Entrada de códigos
frame_codigos = tk.LabelFrame(janela, text="Códigos de Barras (um por linha)")
frame_codigos.pack(fill="both", padx=10, pady=5)
entrada_codigos = scrolledtext.ScrolledText(frame_codigos, height=5)
entrada_codigos.pack(fill="both", padx=5, pady=5)

# Colunas disponíveis e controle
colunas_todas = [
    "ID", "Código", "NCM", "CEST", "Tipo", "Lista", "CodANP",
    "Código Int.", "Produto", "EX", "CodENQ", "CST IPI Ent", "CST IPI Sai", "Aliq IPI",
    "NRI", "CST PIS Ent", "CST PIS Sai", "Aliq PIS", "Aliq COFINS",
    "Amp Legal PIS/COFINS", "Dt Vig PIS Início", "Dt Vig PIS Fim",
    "UF", "CST", "FCP", "IVA", "CSOSN", "Código Regra",
    "% Diferimento", "Exceção", "Simb PDV", "Aliq ICMS",
    "Amp Legal ICMS", "Cod Benefício", "Dt Vig Regra Início", "Dt Vig Regra Fim",
    "% ICMS PDV", "CFOP Venda", "CFOP Compra", "ICMS Desonerado",
    "Aliq ICMS ST", "Antecipado", "Desonerado", "% Isenção",
    "Redução BC ICMS", "Redução BC ST", "Finalidade", "Ind. Deduz Deson."
]
colunas_visiveis_vars = {col: tk.BooleanVar(value=True) for col in colunas_todas}

# Seleção de colunas
frame_colunas = tk.LabelFrame(janela, text="Selecionar colunas visíveis")
frame_colunas.pack(fill='x', padx=5, pady=5)
for i, col in enumerate(colunas_todas):
    tk.Checkbutton(frame_colunas, text=col, variable=colunas_visiveis_vars[col]).grid(row=i//4, column=i%4, sticky='w')

# Tabela formatada
frame_tabela = tk.Frame(janela)
frame_tabela.pack(fill='both', expand=True)

def atualizar_tabela_formatada():
    global tree
    for widget in frame_tabela.winfo_children():
        widget.destroy()

    colunas_visiveis = [col for col, var in colunas_visiveis_vars.items() if var.get()]
    tree = ttk.Treeview(frame_tabela, columns=colunas_visiveis, show='headings')
    for col in colunas_visiveis:
        tree.heading(col, text=col)
        tree.column(col, width=160, anchor='center')

    scroll_y = tk.Scrollbar(frame_tabela, orient="vertical", command=tree.yview)
    scroll_x = tk.Scrollbar(frame_tabela, orient="horizontal", command=tree.xview)
    tree.configure(yscroll=scroll_y.set, xscroll=scroll_x.set)
    scroll_y.pack(side='right', fill='y')
    scroll_x.pack(side='bottom', fill='x')
    tree.pack(fill='both', expand=True)

tk.Button(janela, text="Atualizar colunas da Tabela", command=atualizar_tabela_formatada).pack(pady=5)

# Saída formatada JSON
resposta_area = scrolledtext.ScrolledText(janela, height=8)
resposta_area.pack(fill="both", padx=10, pady=5)

# Carregando
carregando_label = tk.Label(janela, text="", fg="blue")
carregando_label.pack()

# Consulta
def consultar_tributacao():
    def processar():
        try:
            carregando_label.config(text="Consultando...")
            atualizar_tabela_formatada()
            for i in tree.get_children():
                tree.delete(i)

            codigos = entrada_codigos.get("1.0", tk.END).strip().splitlines()
            produtos = [{"codigo": cod, "descricao": f"Produto {i+1}"} for i, cod in enumerate(codigos) if cod]

            if not produtos:
                messagebox.showwarning("Atenção", "Informe pelo menos um código de barras.")
                carregando_label.config(text="")
                return

            token = obter_token_interno()

            cnpj = entrada_cnpj.get().strip()
            uf = entrada_uf.get().strip().upper()
            crt = opcoes_crt.get(entrada_crt.get(), "1")
            regime = opcoes_regime.get(entrada_regime.get(), "1")
            icms = "true" if var_icms.get() else "false"
            ambiente = entrada_ambiente.get().strip()

            url = (
                f"http://autorizadorfarma.hos.com.br/HOSImendes/v1/tributacao"
                f"?cnpj={cnpj}&uf={uf}&crt={crt}&regimeTrib={regime}&contribuinteIcms={icms}&ambiente={ambiente}"
            )

            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }

            response = requests.post(url, headers=headers, data=json.dumps(produtos))
            if response.status_code == 200:
                resultado = response.json()
                resposta_area.delete("1.0", tk.END)
                resposta_area.insert(tk.END, json.dumps(resultado, indent=2))

                for item in resultado:
                    tributacao = json.loads(item.get("tributacao", "{}"))
                    regra = tributacao.get("regra", [{}])[0]
                    ipi = tributacao.get("ipi", {})
                    pis = tributacao.get("piscofins", {})

                    linha = {
                        "ID": item.get("id"),
                        "Código": item.get("codigo"),
                        "NCM": tributacao.get("ncm"),
                        "CEST": tributacao.get("cest"),
                        "Tipo": tributacao.get("tipo"),
                        "Lista": tributacao.get("lista"),
                        "CodANP": tributacao.get("codanp"),
                        "Código Int.": tributacao.get("codigo"),
                        "Produto": ", ".join(tributacao.get("produto", [])),
                        "EX": ipi.get("ex"),
                        "CodENQ": ipi.get("codenq"),
                        "CST IPI Ent": ipi.get("cstEnt"),
                        "CST IPI Sai": ipi.get("cstSai"),
                        "Aliq IPI": ipi.get("aliqIPI"),
                        "NRI": pis.get("nri"),
                        "CST PIS Ent": pis.get("cstEnt"),
                        "CST PIS Sai": pis.get("cstSai"),
                        "Aliq PIS": pis.get("aliqPIS"),
                        "Aliq COFINS": pis.get("aliqCOFINS"),
                        "Amp Legal PIS/COFINS": pis.get("ampLegal"),
                        "Dt Vig PIS Início": pis.get("dtVigIni"),
                        "Dt Vig PIS Fim": pis.get("dtVigFin"),
                        "UF": regra.get("uf"),
                        "CST": regra.get("cst"),
                        "FCP": regra.get("fcp"),
                        "IVA": regra.get("iva"),
                        "CSOSN": regra.get("csosn"),
                        "Código Regra": regra.get("codigo"),
                        "% Diferimento": regra.get("pDifer"),
                        "Exceção": regra.get("excecao"),
                        "Simb PDV": regra.get("simbPDV"),
                        "Aliq ICMS": regra.get("aliqicms"),
                        "Amp Legal ICMS": regra.get("ampLegal"),
                        "Cod Benefício": regra.get("codBenef"),
                        "Dt Vig Regra Início": regra.get("dtVigIni"),
                        "Dt Vig Regra Fim": regra.get("dtVigFin"),
                        "% ICMS PDV": regra.get("pICMSPDV"),
                        "CFOP Venda": regra.get("cfopVenda"),
                        "CFOP Compra": regra.get("cfopCompra"),
                        "ICMS Desonerado": regra.get("icmsdeson"),
                        "Aliq ICMS ST": regra.get("aliqicmsst"),
                        "Antecipado": regra.get("antecipado"),
                        "Desonerado": regra.get("desonerado"),
                        "% Isenção": regra.get("percIsencao"),
                        "Redução BC ICMS": regra.get("reducaobcicms"),
                        "Redução BC ST": regra.get("reducaobcicmsst"),
                        "Finalidade": regra.get("estd_finalidade"),
                        "Ind. Deduz Deson.": regra.get("IndicDeduzDesonerado"),
                    }

                    values = [linha[c] for c in tree["columns"]]
                    tree.insert("", "end", values=values)
            else:
                messagebox.showerror("Erro", f"Erro ao consultar: {response.text}")
        except Exception as e:
            messagebox.showerror("Erro", str(e))
        finally:
            carregando_label.config(text="")

    threading.Thread(target=processar).start()

tk.Button(janela, text="Consultar Tributação", command=consultar_tributacao).pack(pady=10)

janela.mainloop()
