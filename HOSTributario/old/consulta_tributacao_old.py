import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import tkinter.font as tkFont

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

# --- FRAME DE PARÂMETROS ---
frame_param = tk.LabelFrame(janela, text="Parâmetros de Consulta")
frame_param.pack(fill="x", padx=10, pady=5)

# Configuração da grade para alinhamento
frame_param.columnconfigure(1, weight=1)
frame_param.columnconfigure(3, weight=1)
frame_param.columnconfigure(5, weight=2)

# --- Definição dos dados para os Combobox ---
opcoes_crt = {
    "1 - Simples Nacional": "1",
    "2 - Simples - Excesso de Sublimite": "2",
    "3 - Regime Normal": "3"
}
opcoes_regime = {
    "1 - Simples": "1",
    "2 - Lucro Presumido": "2",
    "3 - Lucro Real": "3"
}
ufs = ["AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"]


# --- Criação dos Widgets ---
# Linha 0
tk.Label(frame_param, text="CNPJ:").grid(row=0, column=0, padx=(10,5), pady=5, sticky='e')
entrada_cnpj = tk.Entry(frame_param, width=20)
entrada_cnpj.insert(0, "00115150000140")
entrada_cnpj.grid(row=0, column=1, sticky='w')

tk.Label(frame_param, text="CRT:").grid(row=0, column=2, padx=5, pady=5, sticky='e')
entrada_crt = ttk.Combobox(frame_param, values=list(opcoes_crt.keys()), width=30, state='readonly')
entrada_crt.set("1 - Simples Nacional")
entrada_crt.grid(row=0, column=3, sticky='w')

# Linha 1
tk.Label(frame_param, text="UF:").grid(row=1, column=0, padx=(10,5), pady=5, sticky='e')
entrada_uf = ttk.Combobox(frame_param, values=ufs, width=5, state='readonly')
entrada_uf.set("RS")
entrada_uf.grid(row=1, column=1, sticky='w')

tk.Label(frame_param, text="RegimeTrib:").grid(row=1, column=2, padx=5, pady=5, sticky='e')
entrada_regime = ttk.Combobox(frame_param, values=list(opcoes_regime.keys()), width=20, state='readonly')
entrada_regime.set("1 - Simples")
entrada_regime.grid(row=1, column=3, sticky='w')

# Linha 2
tk.Label(frame_param, text="Ambiente:").grid(row=2, column=0, padx=(10,5), pady=5, sticky='e')
entrada_ambiente = tk.Entry(frame_param, width=5)
entrada_ambiente.insert(0, "2")
entrada_ambiente.grid(row=2, column=1, sticky='w')

var_icms = tk.BooleanVar(value=True)
tk.Checkbutton(frame_param, text="Contribuinte ICMS", variable=var_icms).grid(row=2, column=3, padx=5, sticky='w')

btn_font = ("Segoe UI", 10, "bold")
btn_consultar = tk.Button(frame_param, text="Consultar Tributação", command=lambda: consultar_tributacao(), font=btn_font, bg="#2e7d32", fg="white", relief="flat", cursor="hand2")
btn_consultar.grid(row=0, column=5, rowspan=3, padx=20, pady=10, ipady=10, ipadx=10, sticky='nsew')

# --- Seção de Colunas Retrátil ---
toggle_button = tk.Button(frame_param, text="► Colunas a serem exibidas", command=lambda: toggle_colunas(), relief="flat", fg="black", cursor="hand2")
toggle_button.grid(row=3, column=0, columnspan=4, sticky='w', padx=5, pady=(10,0))

checkbox_container = tk.Frame(frame_param)
checkbox_container.grid(row=4, column=0, columnspan=6, sticky='w', padx=10)
checkbox_container.grid_remove()

def toggle_colunas():
    if checkbox_container.winfo_viewable():
        checkbox_container.grid_remove()
        toggle_button.config(text="► Colunas a serem exibidas")
    else:
        checkbox_container.grid()
        toggle_button.config(text="▼ Colunas a serem exibidas")

colunas_todas = [
    "ID", "Código", "NCM", "CEST", "Tipo", "Lista", "CodANP", "Código Int.", "EX", "CodENQ", "CST IPI Ent", "CST IPI Sai", "Aliq IPI",
    "NRI", "CST PIS Ent", "CST PIS Sai", "Aliq PIS", "Aliq COFINS", "Amp Legal PIS/COFINS", "Dt Vig PIS Início", "Dt Vig PIS Fim", "UF", "CST",
    "FCP", "IVA", "CSOSN", "Código Regra", "% Diferimento", "Exceção", "Simb PDV", "Aliq ICMS", "Amp Legal ICMS", "Cod Benefício", "Dt Vig Regra Início",
    "Dt Vig Regra Fim", "% ICMS PDV", "CFOP Venda", "CFOP Compra", "ICMS Desonerado", "Aliq ICMS ST", "Antecipado", "Desonerado", "% Isenção",
    "Redução BC ICMS", "Redução BC ST", "Finalidade", "Ind. Deduz Deson."
]
colunas_visiveis_vars = {col: tk.BooleanVar(value=True) for col in colunas_todas}

botoes_marcar_frame = tk.Frame(checkbox_container)
botoes_marcar_frame.grid(row=0, column=0, columnspan=5, pady=5, sticky='w')

def marcar_desmarcar_todas(marcar):
    for var in colunas_visiveis_vars.values():
        var.set(marcar)
    atualizar_tabela_formatada()

tk.Button(botoes_marcar_frame, text="Marcar Tudo", command=lambda: marcar_desmarcar_todas(True)).pack(side='left', padx=5)
tk.Button(botoes_marcar_frame, text="Desmarcar Tudo", command=lambda: marcar_desmarcar_todas(False)).pack(side='left', padx=5)

for i, col in enumerate(colunas_todas):
    tk.Checkbutton(checkbox_container, text=col, variable=colunas_visiveis_vars[col], command=lambda: atualizar_tabela_formatada()).grid(row=(i // 6) + 1, column=i % 6, sticky='w', padx=10)

# Entrada de códigos
frame_codigos = tk.LabelFrame(janela, text="Códigos de Barras (um por linha)")
frame_codigos.pack(fill="both", padx=10, pady=5)
entrada_codigos = scrolledtext.ScrolledText(frame_codigos, height=5)
entrada_codigos.pack(fill="both", padx=5, pady=5)

# Tabela formatada
frame_tabela = tk.Frame(janela)
frame_tabela.pack(fill='both', expand=True, padx=10)
tree = None

# <<< ALTERAÇÃO 1: Função de ajuste de largura de coluna modificada >>>
def ajustar_largura_colunas_conteudo(tv):
    """Ajusta a largura da coluna para o máximo entre o cabeçalho e o conteúdo."""
    font_medida = tkFont.nametofont("TkDefaultFont")
    for col in tv["columns"]:
        # Mede a largura do texto do cabeçalho
        header_width = font_medida.measure(tv.heading(col)["text"])
        
        # Encontra a largura máxima do conteúdo na coluna
        max_content_width = 0
        for item in tv.get_children():
            cell_value = tv.set(item, col)
            if cell_value:
                cell_width = font_medida.measure(str(cell_value))
                if cell_width > max_content_width:
                    max_content_width = cell_width
        
        # Define a largura da coluna como o maior valor (cabeçalho ou conteúdo) + um preenchimento
        final_width = max(header_width, max_content_width)
        tv.column(col, width=final_width + 25, stretch=tk.NO)


def atualizar_tabela_formatada():
    global tree
    for widget in frame_tabela.winfo_children():
        widget.destroy()

    colunas_visiveis = [col for col, var in colunas_visiveis_vars.items() if var.get()]
    tree = ttk.Treeview(frame_tabela, columns=colunas_visiveis, show='headings')
    for col in colunas_visiveis:
        tree.heading(col, text=col, command=lambda c=col: sort_treeview(tree, c, False))
        tree.column(col, width=120, anchor='w')

    scroll_y = tk.Scrollbar(frame_tabela, orient="vertical", command=tree.yview)
    scroll_x = tk.Scrollbar(frame_tabela, orient="horizontal", command=tree.xview)
    tree.configure(yscroll=scroll_y.set, xscroll=scroll_x.set)
    scroll_y.pack(side='right', fill='y')
    scroll_x.pack(side='bottom', fill='x')
    tree.pack(fill='both', expand=True)

def sort_treeview(tv, col, reverse):
    l = [(tv.set(k, col), k) for k in tv.get_children('')]
    try:
        # Tenta ordenar numericamente se possível
        l.sort(key=lambda t: float(str(t[0]).replace(",", ".")), reverse=reverse)
    except (ValueError, TypeError):
        # Caso contrário, ordena como texto
        l.sort(key=lambda t: str(t[0]), reverse=reverse)
        
    for index, (val, k) in enumerate(l):
        tv.move(k, '', index)
    tv.heading(col, command=lambda: sort_treeview(tv, col, not reverse))

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
            if tree is None:
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

            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            response = requests.post(url, headers=headers, data=json.dumps(produtos))

            if response.status_code == 200:
                resultado = response.json()
                resposta_area.delete("1.0", tk.END)
                resposta_area.insert(tk.END, json.dumps(resultado, indent=2))
                colunas_visiveis = tree["columns"]

                for item in resultado:
                    tributacao = json.loads(item.get("tributacao", "{}"))
                    regra = tributacao.get("regra", [{}])[0] if tributacao.get("regra") else {}
                    ipi = tributacao.get("ipi", {})
                    pis = tributacao.get("piscofins", {})
                    linha = {
                        "ID": item.get("id"), "Código": item.get("codigo"), "NCM": tributacao.get("ncm"), "CEST": tributacao.get("cest"),
                        "Tipo": tributacao.get("tipo"), "Lista": tributacao.get("lista"), "CodANP": tributacao.get("codanp"), "Código Int.": tributacao.get("codigo"),
                        "EX": ipi.get("ex"), "CodENQ": ipi.get("codenq"), "CST IPI Ent": ipi.get("cstEnt"),
                        "CST IPI Sai": ipi.get("cstSai"), "Aliq IPI": ipi.get("aliqIPI"), "NRI": pis.get("nri"), "CST PIS Ent": pis.get("cstEnt"),
                        "CST PIS Sai": pis.get("cstSai"), "Aliq PIS": pis.get("aliqPIS"), "Aliq COFINS": pis.get("aliqCOFINS"),
                        "Amp Legal PIS/COFINS": pis.get("ampLegal"), "Dt Vig PIS Início": pis.get("dtVigIni"), "Dt Vig PIS Fim": pis.get("dtVigFin"),
                        "UF": regra.get("uf"), "CST": regra.get("cst"), "FCP": regra.get("fcp"), "IVA": regra.get("iva"), "CSOSN": regra.get("csosn"),
                        "Código Regra": regra.get("codigo"), "% Diferimento": regra.get("pDifer"), "Exceção": regra.get("excecao"), "Simb PDV": regra.get("simbPDV"),
                        "Aliq ICMS": regra.get("aliqicms"), "Amp Legal ICMS": regra.get("ampLegal"), "Cod Benefício": regra.get("codBenef"),
                        "Dt Vig Regra Início": regra.get("dtVigIni"), "Dt Vig Regra Fim": regra.get("dtVigFin"), "% ICMS PDV": regra.get("pICMSPDV"),
                        "CFOP Venda": regra.get("cfopVenda"), "CFOP Compra": regra.get("cfopCompra"), "ICMS Desonerado": regra.get("icmsdeson"),
                        "Aliq ICMS ST": regra.get("aliqicmsst"), "Antecipado": regra.get("antecipado"), "Desonerado": regra.get("desonerado"),
                        "% Isenção": regra.get("percIsencao"), "Redução BC ICMS": regra.get("reducaobcicms"), "Redução BC ST": regra.get("reducaobcicmsst"),
                        "Finalidade": regra.get("estd_finalidade"), "Ind. Deduz Deson.": regra.get("IndicDeduzDesonerado"),
                    }
                    values = [linha.get(c, "") for c in colunas_visiveis]
                    tree.insert("", "end", values=values)

                ajustar_largura_colunas_conteudo(tree)

            else:
                messagebox.showerror("Erro", f"Erro ao consultar: {response.text}")
        except Exception as e:
            messagebox.showerror("Erro", str(e))
        finally:
            carregando_label.config(text="")

    threading.Thread(target=processar, daemon=True).start()

# Inicializa a tabela uma vez no início
atualizar_tabela_formatada()
janela.mainloop()