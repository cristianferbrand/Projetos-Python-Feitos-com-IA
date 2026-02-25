import tkinter as tk
from tkinter import filedialog, messagebox
import os

def process_file(input_file, output_file):
    try:
        with open(input_file, 'r', encoding='latin-1') as infile, open(output_file, 'w', encoding='latin-1') as outfile:
            for line in infile:
                if line.startswith('|C800|'):
                    fields = line.strip().split('|')
                    if len(fields) > 9:
                        fields[9] = ''  # Limpa o campo CNPJ_CPF
                    new_line = '|'.join(fields) + '\n'
                    outfile.write(new_line)
                else:
                    outfile.write(line)
        messagebox.showinfo("Sucesso", "Arquivo processado e salvo com sucesso!")
    except Exception as e:
        messagebox.showerror("Erro", f"Ocorreu um erro: {e}")

def select_input_file():
    input_file_path = filedialog.askopenfilename(
        title="Selecione o arquivo SPED de entrada",
        filetypes=[("Arquivos TXT", "*.txt")],
    )
    if input_file_path:
        input_file_var.set(input_file_path)

def select_output_file():
    output_file_path = filedialog.asksaveasfilename(
        title="Selecione o local para salvar o arquivo de saída",
        defaultextension=".txt",
        filetypes=[("Arquivos TXT", "*.txt")],
    )
    if output_file_path:
        output_file_var.set(output_file_path)

def start_processing():
    input_file = input_file_var.get()
    output_file = output_file_var.get()

    if not os.path.isfile(input_file):
        messagebox.showwarning("Aviso", "Selecione um arquivo de entrada válido!")
        return
    if not output_file:
        messagebox.showwarning("Aviso", "Selecione um local para salvar o arquivo de saída!")
        return
    
    process_file(input_file, output_file)

# Configuração da interface gráfica
root = tk.Tk()
root.title("Processador de Registros C800 - SPED Fiscal")

# Variáveis para armazenar os caminhos dos arquivos
input_file_var = tk.StringVar()
output_file_var = tk.StringVar()

# Elementos da interface
tk.Label(root, text="Arquivo de Entrada:").grid(row=0, column=0, padx=10, pady=5, sticky="e")
tk.Entry(root, textvariable=input_file_var, width=50).grid(row=0, column=1, padx=10, pady=5)
tk.Button(root, text="Selecionar", command=select_input_file).grid(row=0, column=2, padx=10, pady=5)

tk.Label(root, text="Arquivo de Saída:").grid(row=1, column=0, padx=10, pady=5, sticky="e")
tk.Entry(root, textvariable=output_file_var, width=50).grid(row=1, column=1, padx=10, pady=5)
tk.Button(root, text="Selecionar", command=select_output_file).grid(row=1, column=2, padx=10, pady=5)

tk.Button(root, text="Processar Arquivo", command=start_processing).grid(row=2, column=0, columnspan=3, pady=10)

# Inicia o loop da interface
root.mainloop()
