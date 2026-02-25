import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext
from difflib import unified_diff

# Função para comparar os arquivos
def compare_files(file1_path, file2_path):
    try:
        with open(file1_path, 'r', encoding='latin-1') as file1, open(file2_path, 'r', encoding='latin-1') as file2:
            file1_lines = file1.readlines()
            file2_lines = file2.readlines()

        # Computar as diferenças entre os dois arquivos
        diff = unified_diff(file1_lines, file2_lines, fromfile='Arquivo Correto', tofile='Arquivo Errado', lineterm='')

        # Limpar a exibição e adicionar o resultado das diferenças
        result_text.delete(1.0, tk.END)
        diff_found = False
        for line in diff:
            result_text.insert(tk.END, line + '\n')
            diff_found = True

        if not diff_found:
            result_text.insert(tk.END, "Os arquivos são idênticos.")
    except Exception as e:
        messagebox.showerror("Erro", f"Erro ao comparar os arquivos: {e}")

# Função para selecionar o primeiro arquivo
def select_file1():
    file1_path = filedialog.askopenfilename(title="Selecione o primeiro arquivo", filetypes=[("Text Files", "*.txt")])
    if file1_path:
        entry_file1.delete(0, tk.END)
        entry_file1.insert(0, file1_path)

# Função para selecionar o segundo arquivo
def select_file2():
    file2_path = filedialog.askopenfilename(title="Selecione o segundo arquivo", filetypes=[("Text Files", "*.txt")])
    if file2_path:
        entry_file2.delete(0, tk.END)
        entry_file2.insert(0, file2_path)

# Função para iniciar a comparação
def start_comparison():
    file1_path = entry_file1.get()
    file2_path = entry_file2.get()
    
    if not file1_path or not file2_path:
        messagebox.showwarning("Aviso", "Por favor, selecione ambos os arquivos para comparação.")
    else:
        compare_files(file1_path, file2_path)

# Configuração da janela principal
root = tk.Tk()
root.title("Comparador de Arquivos")
root.geometry("700x500")

# Campo para selecionar o primeiro arquivo
tk.Label(root, text="Arquivo Correto:").pack(pady=5)
frame_file1 = tk.Frame(root)
frame_file1.pack(pady=5)
entry_file1 = tk.Entry(frame_file1, width=50)
entry_file1.pack(side=tk.LEFT, padx=5)
tk.Button(frame_file1, text="Selecionar", command=select_file1).pack(side=tk.LEFT)

# Campo para selecionar o segundo arquivo
tk.Label(root, text="Arquivo Errado:").pack(pady=5)
frame_file2 = tk.Frame(root)
frame_file2.pack(pady=5)
entry_file2 = tk.Entry(frame_file2, width=50)
entry_file2.pack(side=tk.LEFT, padx=5)
tk.Button(frame_file2, text="Selecionar", command=select_file2).pack(side=tk.LEFT)

# Botão para iniciar a comparação
tk.Button(root, text="Iniciar Comparação", command=start_comparison).pack(pady=10)

# Campo de texto para exibir o resultado das diferenças
result_text = scrolledtext.ScrolledText(root, width=80, height=20)
result_text.pack(pady=10)

root.mainloop()