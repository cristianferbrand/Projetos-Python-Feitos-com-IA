import os
import re
import pandas as pd
import shutil
import tkinter as tk
from tkinter import filedialog, messagebox

def remover_mascara(cnpj):
    return re.sub(r'\D', '', cnpj)

# Função para organizar arquivos SPED Fiscal
def organizar_sped_fiscal():
    organizar_arquivos_sped(tipo="fiscal")

# Função para organizar arquivos SPED Contribuições
def organizar_sped_contribuicoes():
    organizar_arquivos_sped(tipo="contribuicoes")

# Função principal que organiza os arquivos com base no tipo de SPED
def organizar_arquivos_sped(tipo):
    # Carregar CSV com codificação UTF-8 e delimitador ";"
    csv_path = filedialog.askopenfilename(title="Selecione o arquivo CSV", filetypes=[("CSV Files", "*.csv")])
    df = pd.read_csv(csv_path, encoding="utf-8", delimiter=";")
    
    # Verificar e exibir as colunas do CSV para debug
    print("Colunas no CSV:", df.columns.tolist())

    # Verificar se a coluna "CNPJ" existe no CSV
    if 'CNPJ' not in df.columns:
        messagebox.showerror("Erro", "Coluna 'CNPJ' não encontrada no arquivo CSV. Verifique o cabeçalho do CSV.")
        return

    # Remover máscara do CNPJ no CSV
    df['CNPJ'] = df['CNPJ'].apply(remover_mascara)

    # Selecionar o diretório onde as pastas serão salvas
    output_dir = filedialog.askdirectory(title="Selecione o diretório de destino para as pastas")
    if not output_dir:
        messagebox.showerror("Erro", "Nenhum diretório selecionado para salvar as pastas.")
        return

    # Normalizar o caminho para o padrão correto de barras no Windows
    output_dir = os.path.normpath(output_dir)

    # Selecionar arquivos SPED
    txt_files = filedialog.askopenfilenames(title="Selecione os arquivos SPED (TXT)", filetypes=[("Text Files", "*.txt")])

    for txt_file in txt_files:
        # Abrir e ler a primeira linha do arquivo para extrair o CNPJ, depois fechar o arquivo
        with open(txt_file, 'r', encoding='latin-1') as f:
            primeira_linha = f.readline()

        # Extrair o CNPJ com base no tipo de SPED
        if tipo == "fiscal":
            # SPED Fiscal: padrão do registro 0000 para o Fiscal
            match = re.search(r'\|0000\|\d+\|\d\|\d+\|\d+\|[^|]+\|(\d{14})\|', primeira_linha)
        elif tipo == "contribuicoes":
            # SPED Contribuições: padrão do registro 0000 para Contribuições
            match = re.search(r'\|0000\|\d+\|\d\|\|\|\d+\|\d+\|[^|]+\|(\d{14})\|', primeira_linha)
        else:
            messagebox.showerror("Erro", "Tipo de SPED desconhecido.")
            return

        if match:
            cnpj_sped = match.group(1)

            # Verificar se o CNPJ existe no CSV
            linha_cliente = df[df['CNPJ'] == cnpj_sped]
            if not linha_cliente.empty:
                codigo = linha_cliente.iloc[0]['CODIGO']
                fantasia = linha_cliente.iloc[0]['FANTASIA']
                nome_pasta = f"{codigo} - {fantasia}".strip()
                pasta_destino = os.path.join(output_dir, nome_pasta)

                # Criar a pasta no diretório selecionado
                try:
                    os.makedirs(pasta_destino, exist_ok=True)
                    messagebox.showinfo("Sucesso", f"Pasta '{nome_pasta}' criada com sucesso.")
                except Exception as e:
                    messagebox.showerror("Erro", f"Erro ao criar pasta '{nome_pasta}': {e}")
                    continue

                # Mover o arquivo TXT para a pasta criada
                try:
                    # Verificar se o arquivo realmente existe antes de tentar movê-lo
                    if os.path.exists(txt_file):
                        destino_arquivo = os.path.join(pasta_destino, os.path.basename(txt_file))
                        shutil.move(txt_file, destino_arquivo)
                        messagebox.showinfo("Sucesso", f"Arquivo '{os.path.basename(txt_file)}' movido para a pasta '{nome_pasta}'.")
                    else:
                        messagebox.showerror("Erro", f"O arquivo '{txt_file}' não foi encontrado.")
                except Exception as e:
                    messagebox.showerror("Erro", f"Erro ao mover o arquivo '{txt_file}' para a pasta '{nome_pasta}': {e}")
            else:
                messagebox.showerror("Erro", f"CNPJ do SPED '{cnpj_sped}' não encontrado no CSV.")
        else:
            messagebox.showerror("Erro", f"Registro 0000 não encontrado ou formato inválido no arquivo {txt_file}.")

# Interface gráfica
root = tk.Tk()
root.title("Organizador Arquivos SPED")
root.geometry("400x200")

# Botões para organizar arquivos SPED Fiscal e SPED Contribuições
btn_fiscal = tk.Button(root, text="Organizar Arquivos SPED Fiscal", command=organizar_sped_fiscal)
btn_fiscal.pack(pady=10)

btn_contribuicoes = tk.Button(root, text="Organizar Arquivos SPED Contribuições", command=organizar_sped_contribuicoes)
btn_contribuicoes.pack(pady=10)

root.mainloop()