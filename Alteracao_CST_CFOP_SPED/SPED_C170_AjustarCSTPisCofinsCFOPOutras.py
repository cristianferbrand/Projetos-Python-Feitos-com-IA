import tkinter as tk
from tkinter import filedialog, messagebox

# Função para processar o arquivo SPED
def process_sped_file(input_file, output_file, custom_cfop, custom_cst, target_cst):
    try:
        with open(input_file, 'r', encoding='latin-1') as file, open(output_file, 'w', encoding='latin-1') as new_file:
            for line in file:
                fields = line.strip().split('|')
                
                # Verificar se é o registro C170 e se CST_PIS e CST_COFINS são 50
                if fields[1] == 'C170' and fields[25] == '50' and fields[31] == '50':
                    # Condição fixa para CFOPs 1114 e 1910
                    if fields[11] in ('1114', '1910', '2910', '2911'):
                        # Alterar CST_PIS e CST_COFINS para 99
                        fields[25] = '99'
                        fields[31] = '99'                    # Condição personalizada para a CFOP e CST especificados pelo usuário
                    elif fields[11] == custom_cfop and fields[25] == custom_cst and fields[31] == custom_cst:
                        # Alterar CST_PIS e CST_COFINS para o valor desejado (exemplo 99)
                        fields[25] = target_cst
                        fields[31] = target_cst
                        # Zerar os campos desejados
                        fields[26] = '0'  # VL_BC_PIS
                        fields[27] = '0'  # ALIQ_PIS
                        fields[30] = '0'  # VL_PIS
                        fields[32] = '0'  # VL_BC_COFINS
                        fields[33] = '0'  # ALIQ_COFINS
                        fields[36] = '0'  # VL_COFINS
                        
                new_line = '|'.join(fields) + '\n'
                new_file.write(new_line)
                
        messagebox.showinfo("Processamento Concluído", f"O arquivo atualizado foi salvo como {output_file}")
    except Exception as e:
        messagebox.showerror("Erro", f"Erro ao processar o arquivo: {e}")

# Função para selecionar o arquivo de entrada
def select_input_file():
    input_path = filedialog.askopenfilename(title="Selecione o arquivo SPED de entrada", filetypes=[("Text Files", "*.txt")])
    if input_path:
        entry_input.delete(0, tk.END)
        entry_input.insert(0, input_path)

# Função para selecionar o arquivo de saída
def select_output_file():
    output_path = filedialog.asksaveasfilename(title="Selecione o caminho para salvar o arquivo de saída", filetypes=[("Text Files", "*.txt")], defaultextension=".txt")
    if output_path:
        entry_output.delete(0, tk.END)
        entry_output.insert(0, output_path)

# Função para iniciar o processamento
def start_processing():
    input_file = entry_input.get()
    output_file = entry_output.get()
    custom_cfop = entry_custom_cfop.get()
    custom_cst = entry_custom_cst.get()
    target_cst = entry_target_cst.get()
    
    if not input_file or not output_file:
        messagebox.showwarning("Aviso", "Por favor, selecione os arquivos de entrada e saída.")
    elif not custom_cfop or not custom_cst or not target_cst:
        messagebox.showwarning("Aviso", "Por favor, insira o CFOP e os valores de CST personalizados.")
    else:
        process_sped_file(input_file, output_file, custom_cfop, custom_cst, target_cst)

# Configuração da janela principal
root = tk.Tk()
root.title("Processador de Arquivo SPED")
root.geometry("500x400")

# Campo para selecionar o arquivo de entrada
tk.Label(root, text="Arquivo de Entrada (SPED):").pack(pady=5)
frame_input = tk.Frame(root)
frame_input.pack(pady=5)
entry_input = tk.Entry(frame_input, width=50)
entry_input.pack(side=tk.LEFT, padx=5)
tk.Button(frame_input, text="Selecionar", command=select_input_file).pack(side=tk.LEFT)

# Campo para selecionar o arquivo de saída
tk.Label(root, text="Arquivo de Saída:").pack(pady=5)
frame_output = tk.Frame(root)
frame_output.pack(pady=5)
entry_output = tk.Entry(frame_output, width=50)
entry_output.pack(side=tk.LEFT, padx=5)
tk.Button(frame_output, text="Selecionar", command=select_output_file).pack(side=tk.LEFT)

# Campo para inserir o CFOP personalizado
tk.Label(root, text="CFOP Personalizado:").pack(pady=5)
frame_custom_cfop = tk.Frame(root)
frame_custom_cfop.pack(pady=5)
entry_custom_cfop = tk.Entry(frame_custom_cfop, width=10)
entry_custom_cfop.pack(side=tk.LEFT, padx=5)

# Campo para inserir o CST atual personalizado
tk.Label(root, text="CST Atual:").pack(pady=5)
frame_custom_cst = tk.Frame(root)
frame_custom_cst.pack(pady=5)
entry_custom_cst = tk.Entry(frame_custom_cst, width=10)
entry_custom_cst.pack(side=tk.LEFT, padx=5)

# Campo para inserir o CST desejado (novo valor)
tk.Label(root, text="CST Desejado:").pack(pady=5)
frame_target_cst = tk.Frame(root)
frame_target_cst.pack(pady=5)
entry_target_cst = tk.Entry(frame_target_cst, width=10)
entry_target_cst.pack(side=tk.LEFT, padx=5)

# Botão para iniciar o processamento
tk.Button(root, text="Iniciar Processamento", command=start_processing).pack(pady=20)

root.mainloop()