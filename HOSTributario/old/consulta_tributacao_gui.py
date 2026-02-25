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

# Função para consultar tributação e formatar a saída em tabela
def consultar_tributacao():
    def processar():
        try:
            carregando_label.config(text="Consultando...")

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

                    values = [
                        item.get("id"),
                        item.get("codigo"),
                        tributacao.get("ncm"),
                        tributacao.get("cest"),
                        tributacao.get("tipo"),
                        tributacao.get("lista"),
                        tributacao.get("codanp"),
                        tributacao.get("codigo"),
                        ", ".join(tributacao.get("produto", [])),
                        ipi.get("ex"),
                        ipi.get("codenq"),
                        ipi.get("cstEnt"),
                        ipi.get("cstSai"),
                        ipi.get("aliqIPI"),
                        pis.get("nri"),
                        pis.get("cstEnt"),
                        pis.get("cstSai"),
                        pis.get("aliqPIS"),
                        pis.get("aliqCOFINS"),
                        pis.get("ampLegal"),
                        pis.get("dtVigIni"),
                        pis.get("dtVigFin") if pis.get("dtVigFin") is not None else "",
                        regra.get("uf"),
                        regra.get("cst"),
                        regra.get("fcp"),
                        regra.get("iva"),
                        regra.get("csosn"),
                        regra.get("codigo"),
                        regra.get("pDifer"),
                        regra.get("excecao"),
                        regra.get("simbPDV"),
                        regra.get("aliqicms"),
                        regra.get("ampLegal"),
                        regra.get("codBenef"),
                        regra.get("dtVigIni"),
                        regra.get("dtVigFin"),
                        regra.get("pICMSPDV"),
                        regra.get("cfopVenda"),
                        regra.get("cfopCompra"),
                        regra.get("icmsdeson"),
                        regra.get("aliqicmsst"),
                        regra.get("antecipado"),
                        regra.get("desonerado"),
                        regra.get("percIsencao"),
                        regra.get("reducaobcicms"),
                        regra.get("reducaobcicmsst"),
                        regra.get("estd_finalidade"),
                        regra.get("IndicDeduzDesonerado"),
                    ]

                    tree.insert("", "end", values=values)
            else:
                messagebox.showerror("Erro", f"Erro ao consultar: {response.text}")
        except Exception as e:
            messagebox.showerror("Erro", str(e))
        finally:
            carregando_label.config(text="")

    threading.Thread(target=processar).start()

# Criar interface
janela = tk.Tk()
janela.title("Consulta de Tributação - HOS/Imendes")
janela.geometry("1200x750")

# Parâmetros da URL
frame_param = tk.Frame(janela)
frame_param.pack(pady=5)

tk.Label(frame_param, text="CNPJ:").grid(row=0, column=0, padx=5, sticky='e')
entrada_cnpj = tk.Entry(frame_param, width=20)
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
entrada_crt.set("1 - Simples Nacional")  # valor padrão
entrada_crt.grid(row=0, column=5)

tk.Label(frame_param, text="RegimeTrib:").grid(row=0, column=6, padx=5, sticky='e')
opcoes_regime = {
    "1 - Simples": "1",
    "2 - Lucro Presumido": "2",
    "3 - Lucro Real": "3"
}

entrada_regime = ttk.Combobox(frame_param, values=list(opcoes_regime.keys()), width=20)
entrada_regime.set("1 - Simples")  # valor padrão
entrada_regime.grid(row=0, column=7)

var_icms = tk.BooleanVar(value=True)
tk.Checkbutton(frame_param, text="Contribuinte ICMS", variable=var_icms).grid(row=1, column=0, columnspan=2, sticky='w')

tk.Label(frame_param, text="Ambiente (2=Prod):").grid(row=1, column=2, columnspan=2, padx=5, sticky='e')
entrada_ambiente = tk.Entry(frame_param, width=5)
entrada_ambiente.insert(0, "2")
entrada_ambiente.grid(row=1, column=4)

# Botão de consulta
tk.Button(janela, text="Consultar Tributação", command=consultar_tributacao).pack(pady=10)

# Indicador de carregando
carregando_label = tk.Label(janela, text="", fg="blue")
carregando_label.pack()

# Códigos de barras
tk.Label(janela, text="Códigos de Barras (um por linha):").pack()
entrada_codigos = scrolledtext.ScrolledText(janela, width=100, height=6)
entrada_codigos.pack(pady=5)

# Resposta bruta da API
tk.Label(janela, text="Resposta da API (JSON):").pack()
resposta_area = scrolledtext.ScrolledText(janela, width=140, height=10)
resposta_area.pack(pady=10)

# Tabela formatada
frame_tabela = tk.Frame(janela)
frame_tabela.pack(fill='both', expand=True)

colunas = [
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

tree = ttk.Treeview(frame_tabela, columns=colunas, show='headings')
for col in colunas:
    tree.heading(col, text=col)
    tree.column(col, width=160, anchor='center')  # aumentei a largura por segurança

scroll_y = tk.Scrollbar(frame_tabela, orient="vertical", command=tree.yview)
scroll_x = tk.Scrollbar(frame_tabela, orient="horizontal", command=tree.xview)
tree.configure(yscroll=scroll_y.set, xscroll=scroll_x.set)

scroll_y.pack(side='right', fill='y')
scroll_x.pack(side='bottom', fill='x')
tree.pack(fill='both', expand=True)

janela.mainloop()