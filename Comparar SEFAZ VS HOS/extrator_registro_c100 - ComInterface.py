import csv
import tkinter as tk
from tkinter import filedialog, messagebox

# Função auxiliar para converter valores numéricos com vírgula para float
def convert_to_float(value):
    try:
        return float(value.replace(',', '.'))
    except ValueError:
        return 0.0  # Retorna 0.0 caso o valor não seja numérico ou esteja vazio

# Função para formatar o valor no formato brasileiro (com vírgula)
def format_brazilian(value):
    return f"{value:.2f}".replace('.', ',')

# Função principal para processar o arquivo SPED e extrair os registros C100 conforme o tipo selecionado
def process_sped_c100(input_filename, output_filename, nota_type):
    fields = ["Modelo", "Serie", "Numero", "Total_NF-e", "Total_BC_ICMS", "Total_ICMS", "Total_BC_ICMS_ST", "Total_ICMS_ST", "Chave_NF-e"]
    registros_c100 = []

    try:
        # Leitura do arquivo .txt com encoding 'latin-1'
        with open(input_filename, 'r', encoding='latin-1') as file:
            for line in file:
                if line.startswith('|C100|'):
                    parts = line.strip().split('|')
                    # Filtrar de acordo com o tipo de nota selecionado
                    if (nota_type == "saida" and parts[2] == "1") or (nota_type == "entrada" and parts[2] == "0") or (nota_type == "ambas"):
                        registro = {
                            "Modelo": parts[5],
                            "Serie": parts[7],  # Ajuste para a posição correta do campo "Serie"
                            "Numero": parts[8],  # Ajuste para a posição correta do campo "Numero"
                            "Total_NF-e": format_brazilian(convert_to_float(parts[12])),
                            "Total_BC_ICMS": format_brazilian(convert_to_float(parts[13])),
                            "Total_ICMS": format_brazilian(convert_to_float(parts[14])),
                            "Total_BC_ICMS_ST": format_brazilian(convert_to_float(parts[15])),
                            "Total_ICMS_ST": format_brazilian(convert_to_float(parts[16])),
                            "Chave_NF-e": parts[9]  # Extração da chave da NF-e
                        }
                        registros_c100.append(registro)

        # Escrita no arquivo CSV com encoding 'latin-1'
        with open(output_filename, mode='w', newline='', encoding='latin-1') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fields, delimiter=';')
            writer.writeheader()
            writer.writerows(registros_c100)
        
        messagebox.showinfo("Processamento Concluído", f"Arquivo '{output_filename}' gerado com sucesso.")
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
    output_path = filedialog.asksaveasfilename(title="Selecione o caminho para salvar o arquivo de saída CSV", filetypes=[("CSV Files", "*.csv")], defaultextension=".csv")
    if output_path:
        entry_output.delete(0, tk.END)
        entry_output.insert(0, output_path)

# Função para iniciar o processamento
def start_processing():
    input_file = entry_input.get()
    output_file = entry_output.get()
    nota_type = nota_type_var.get()
    
    if not input_file or not output_file:
        messagebox.showwarning("Aviso", "Por favor, selecione os arquivos de entrada e saída.")
    else:
        process_sped_c100(input_file, output_file, nota_type)

# Configuração da janela principal
root = tk.Tk()
root.title("Extrator de Registros C100 (Seleção de Tipo de Nota)")
root.geometry("500x300")

# Campo para selecionar o arquivo de entrada
tk.Label(root, text="Arquivo de Entrada (SPED):").pack(pady=5)
frame_input = tk.Frame(root)
frame_input.pack(pady=5)
entry_input = tk.Entry(frame_input, width=50)
entry_input.pack(side=tk.LEFT, padx=5)
tk.Button(frame_input, text="Selecionar", command=select_input_file).pack(side=tk.LEFT)

# Campo para selecionar o arquivo de saída
tk.Label(root, text="Arquivo de Saída (CSV):").pack(pady=5)
frame_output = tk.Frame(root)
frame_output.pack(pady=5)
entry_output = tk.Entry(frame_output, width=50)
entry_output.pack(side=tk.LEFT, padx=5)
tk.Button(frame_output, text="Selecionar", command=select_output_file).pack(side=tk.LEFT)

# Opções de seleção para o tipo de nota (entrada, saída ou ambas)
nota_type_var = tk.StringVar(value="saida")
tk.Label(root, text="Tipo de Nota:").pack(pady=5)
frame_radio = tk.Frame(root)
frame_radio.pack(pady=5)
tk.Radiobutton(frame_radio, text="Entrada", variable=nota_type_var, value="entrada").pack(side=tk.LEFT)
tk.Radiobutton(frame_radio, text="Saída", variable=nota_type_var, value="saida").pack(side=tk.LEFT)
tk.Radiobutton(frame_radio, text="Ambas", variable=nota_type_var, value="ambas").pack(side=tk.LEFT)

# Botão para iniciar o processamento
tk.Button(root, text="Iniciar Processamento", command=start_processing).pack(pady=20)

root.mainloop()