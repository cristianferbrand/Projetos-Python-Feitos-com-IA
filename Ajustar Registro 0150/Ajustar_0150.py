import tkinter as tk
from tkinter import filedialog, messagebox
import csv
import os

# Função para obter dados do registro 0000
def obter_dados_registro_0000(caminho_arquivo):
    dados_0000 = {}
    with open(caminho_arquivo, mode='r', encoding='latin-1') as arquivo:
        leitor_csv = csv.reader(arquivo, delimiter='|')
        
        for linha in leitor_csv:
            if linha[1] == "0000":  # Identifica o registro 0000
                dados_0000["CNPJ"] = linha[7].strip()         # Extrai o CNPJ (índice 7 no registro 0000)
                dados_0000["COD_MUN"] = linha[11].strip()     # Extrai o Código do Município (índice 11 no registro 0000)
                break  # Já encontramos o registro 0000, então podemos sair do loop

    return dados_0000

# Função para ajustar o CNPJ, COD_MUN, e ponto final no registro 0150
def ajustar_cnpj_cod_mun_registro_0150(caminho_arquivo):
    # Obter informações do registro 0000
    dados_0000 = obter_dados_registro_0000(caminho_arquivo)
    
    if not dados_0000:
        messagebox.showerror("Erro", "Registro 0000 não encontrado no arquivo.")
        return
    
    # Lista para armazenar as linhas ajustadas sem alterar o arquivo de entrada
    linhas_ajustadas = []
    
    with open(caminho_arquivo, mode='r', encoding='latin-1') as arquivo:
        leitor_csv = csv.reader(arquivo, delimiter='|')
        
        for linha in leitor_csv:
            # Verifica se é o registro 0150 e o nome é PIX
            if linha[1] == "0150" and linha[3] == "PIX":  
                linha[5] = dados_0000["CNPJ"]           # Ajusta o CNPJ no índice 4 (posição 5)
                linha[8] = dados_0000["COD_MUN"]        # Ajusta o Código do Município no índice 7 (posição 8)
                linha[10] = '.'                          # Coloca um ponto final no índice 9 (posição 10)
            
            # Adiciona a linha ajustada ou inalterada à lista
            linhas_ajustadas.append(linha)

    # Obter o diretório do arquivo de entrada para salvar o arquivo ajustado na mesma pasta
    diretorio_saida = os.path.dirname(caminho_arquivo)
    caminho_arquivo_ajustado = os.path.join(diretorio_saida, "arquivo_ajustado.txt")

    # Salvar o arquivo ajustado no mesmo formato SPED
    with open(caminho_arquivo_ajustado, mode='w', encoding='latin-1', newline='') as arquivo_saida:
        escritor_csv = csv.writer(arquivo_saida, delimiter='|', quoting=csv.QUOTE_NONE, escapechar='\\')
        for linha in linhas_ajustadas:
            escritor_csv.writerow(linha)  # Salva cada linha ajustada no formato original

    messagebox.showinfo("Processamento Concluído", f"Arquivo ajustado com sucesso. Salvo como '{caminho_arquivo_ajustado}'.")

# Função para selecionar o arquivo de entrada
def select_input_file():
    input_path = filedialog.askopenfilename(title="Selecione o arquivo de entrada", filetypes=[("Text Files", "*.txt")])
    if input_path:
        entry_input.delete(0, tk.END)
        entry_input.insert(0, input_path)

# Função para iniciar o processamento
def start_processing():
    input_file = entry_input.get()
    
    if not input_file:
        messagebox.showwarning("Aviso", "Por favor, selecione o arquivo de entrada.")
    else:
        ajustar_cnpj_cod_mun_registro_0150(input_file)

# Configuração da janela principal
root = tk.Tk()
root.title("Ajuste de CNPJ e Código do Município - SPED")
root.geometry("500x200")

# Campo para selecionar o arquivo de entrada
tk.Label(root, text="Arquivo de Entrada (SPED):").pack(pady=5)
frame_input = tk.Frame(root)
frame_input.pack(pady=5)
entry_input = tk.Entry(frame_input, width=50)
entry_input.pack(side=tk.LEFT, padx=5)
tk.Button(frame_input, text="Selecionar", command=select_input_file).pack(side=tk.LEFT)

# Botão para iniciar o processamento
tk.Button(root, text="Iniciar Ajuste", command=start_processing).pack(pady=20)

root.mainloop()