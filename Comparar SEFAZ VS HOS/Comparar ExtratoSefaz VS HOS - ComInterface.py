import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox

def selecionar_arquivo1():
    caminho = filedialog.askopenfilename(filetypes=[("CSV Files", "*.csv")])
    if caminho:
        entrada1_var.set(caminho)

def selecionar_arquivo2():
    caminho = filedialog.askopenfilename(filetypes=[("CSV Files", "*.csv")])
    if caminho:
        entrada2_var.set(caminho)

def comparar_arquivos():
    try:
        # Leitura dos arquivos CSV
        arquivo1 = pd.read_csv(entrada1_var.get(), delimiter=';', decimal=',')
        arquivo2 = pd.read_csv(entrada2_var.get(), delimiter=';', decimal=',')
        
        # Remover espaços em branco nas colunas 'Numero'
        arquivo1['Numero'] = arquivo1['Numero'].astype(str).str.strip()
        arquivo2['Numero'] = arquivo2['Numero'].astype(str).str.strip()
        
        # Convertendo as colunas 'Numero' e 'Total_NF-e' para numéricas
        arquivo1['Numero'] = pd.to_numeric(arquivo1['Numero'], errors='coerce')
        arquivo2['Numero'] = pd.to_numeric(arquivo2['Numero'], errors='coerce')
        arquivo1['Total_NF-e'] = arquivo1['Total_NF-e'].replace(',', '.', regex=True).astype(float)
        arquivo2['Total_NF-e'] = arquivo2['Total_NF-e'].replace(',', '.', regex=True).astype(float)
        
        # Comparar arquivos e encontrar divergências
        comparacao = pd.merge(
            arquivo1[['Numero', 'Total_NF-e', 'Chave_NF-e']],
            arquivo2[['Numero', 'Total_NF-e', 'Chave_NF-e']],
            on='Numero', how='outer', suffixes=('_arquivo1', '_arquivo2'), indicator=True
        )
        
        # Filtrar divergências no campo 'Total_NF-e'
        diferencas_total_nfe = comparacao[(comparacao['_merge'] == 'both') & 
                                          (comparacao['Total_NF-e_arquivo1'] != comparacao['Total_NF-e_arquivo2'])]
        
        # Encontrar os registros presentes apenas em um dos arquivos
        faltantes = comparacao[comparacao['_merge'] != 'both']
        
        # Exibir resultados na caixa de texto
        texto_diferencas.delete("1.0", tk.END)  # Limpar texto anterior
        if diferencas_total_nfe.empty and faltantes.empty:
            texto_diferencas.insert(tk.END, "Não foram encontradas divergências no Total_NF-e ou registros faltantes.\n")
        else:
            # Formatação das diferenças no Total_NF-e
            if not diferencas_total_nfe.empty:
                texto_diferencas.insert(tk.END, "=== Diferenças no Total_NF-e ===\n\n")
                for _, row in diferencas_total_nfe.iterrows():
                    texto_diferencas.insert(tk.END, f"Numero: {row['Numero']}\n")
                    texto_diferencas.insert(tk.END, f"  Total_NF-e Arquivo SEFAZ: {row['Total_NF-e_arquivo1']}\n")
                    texto_diferencas.insert(tk.END, f"  Total_NF-e Arquivo HOS: {row['Total_NF-e_arquivo2']}\n")
                    texto_diferencas.insert(tk.END, "-"*40 + "\n\n")
            
            # Formatação dos registros faltantes
            if not faltantes.empty:
                texto_diferencas.insert(tk.END, "=== Registros Faltantes ===\n\n")
                for _, row in faltantes.iterrows():
                    texto_diferencas.insert(tk.END, f"Numero: {row['Numero']}\n")
                    if pd.notna(row['Total_NF-e_arquivo1']):
                        texto_diferencas.insert(tk.END, f"  Presente apenas no Arquivo SEFAZ:\n")
                        texto_diferencas.insert(tk.END, f"    Total_NF-e: {row['Total_NF-e_arquivo1']}\n")
                        texto_diferencas.insert(tk.END, f"    Chave_NF-e: {row['Chave_NF-e_arquivo1']}\n")
                    elif pd.notna(row['Total_NF-e_arquivo2']):
                        texto_diferencas.insert(tk.END, f"  Presente apenas no Arquivo HOS:\n")
                        texto_diferencas.insert(tk.END, f"    Total_NF-e: {row['Total_NF-e_arquivo2']}\n")
                        texto_diferencas.insert(tk.END, f"    Chave_NF-e: {row['Chave_NF-e_arquivo2']}\n")
                    texto_diferencas.insert(tk.END, "-"*40 + "\n\n")
    except Exception as e:
        messagebox.showerror("Erro", f"Ocorreu um erro: {e}")

# Configuração da interface
root = tk.Tk()
root.title("Comparar Arquivos SEFAZ VS HOS")

# Variáveis para armazenar os caminhos dos arquivos
entrada1_var = tk.StringVar()
entrada2_var = tk.StringVar()

# Widgets da interface
tk.Label(root, text="Selecione o arquivo SEFAZ:").grid(row=0, column=0, padx=10, pady=5, sticky="w")
tk.Entry(root, textvariable=entrada1_var, width=50).grid(row=0, column=1, padx=10, pady=5)
tk.Button(root, text="Selecionar", command=selecionar_arquivo1).grid(row=0, column=2, padx=10, pady=5)

tk.Label(root, text="Selecione o arquivo HOS:").grid(row=1, column=0, padx=10, pady=5, sticky="w")
tk.Entry(root, textvariable=entrada2_var, width=50).grid(row=1, column=1, padx=10, pady=5)
tk.Button(root, text="Selecionar", command=selecionar_arquivo2).grid(row=1, column=2, padx=10, pady=5)

tk.Button(root, text="Comparar e Exibir", command=comparar_arquivos, width=20).grid(row=2, column=1, pady=20)

# Caixa de texto para exibir as divergências
texto_diferencas = tk.Text(root, width=100, height=25)
texto_diferencas.grid(row=3, column=0, columnspan=3, padx=10, pady=5)

root.mainloop()