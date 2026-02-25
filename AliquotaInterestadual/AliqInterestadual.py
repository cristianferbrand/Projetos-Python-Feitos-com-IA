import pandas as pd
from tkinter import Tk, Label, Button, ttk, StringVar
import os

# Carregar a tabela de alíquotas de um arquivo Excel ou CSV
def carregar_tabela():
    caminho_arquivo = os.path.join(os.path.dirname(__file__), 'Aliquota_Interestadual.xlsx')  # Certifica-se de usar o caminho absoluto na mesma pasta
    if not os.path.exists(caminho_arquivo):
        raise FileNotFoundError(f"Arquivo '{caminho_arquivo}' não encontrado. Certifique-se de que está na mesma pasta que o script.")
    df = pd.read_excel(caminho_arquivo, index_col=0)
    return df

def buscar_aliquotas(df, origem, destino):
    try:
        aliquota_interna = df.loc[origem, origem]
        aliquota_interestadual = df.loc[origem, destino]
        return aliquota_interna, aliquota_interestadual
    except KeyError:
        return None, None

# Função para mostrar as alíquotas na interface gráfica
def mostrar_aliquotas():
    origem = combo_origem.get()
    destino = combo_destino.get()
    
    if origem and destino:
        interna, interestadual = buscar_aliquotas(tabela_aliquotas, origem, destino)
        if interna is not None and interestadual is not None:
            resultado_var.set(f"Alíquota Interna ({origem}): {interna}%\nAlíquota Interestadual ({origem} -> {destino}): {interestadual}%")
        else:
            resultado_var.set("Erro: UF inválida ou não encontrada na tabela.")
    else:
        resultado_var.set("Por favor, selecione UF de Origem e Destino.")

# Carregar a tabela de alíquotas
tabela_aliquotas = carregar_tabela()

# Criar interface gráfica
janela = Tk()
janela.title("Consulta de Alíquotas")

Label(janela, text="UF Origem:").grid(row=0, column=0, padx=10, pady=5)
combo_origem = ttk.Combobox(janela, values=list(tabela_aliquotas.index))
combo_origem.grid(row=0, column=1, padx=10, pady=5)

Label(janela, text="UF Destino:").grid(row=1, column=0, padx=10, pady=5)
combo_destino = ttk.Combobox(janela, values=list(tabela_aliquotas.index))
combo_destino.grid(row=1, column=1, padx=10, pady=5)

Button(janela, text="Consultar", command=mostrar_aliquotas).grid(row=2, column=0, columnspan=2, pady=10)

resultado_var = StringVar()
Label(janela, textvariable=resultado_var, justify="left").grid(row=3, column=0, columnspan=2, padx=10, pady=10)

janela.mainloop()