import tkinter as tk
from tkinter import filedialog, messagebox

# Função para ajustar o campo COD_CTA nos registros C170 e C175 com o valor escolhido
def ajustar_cod_cta_c170_c175(arquivo_entrada, arquivo_saida, cod_cta_valor):
    try:
        with open(arquivo_entrada, 'r', encoding='latin-1') as entrada, open(arquivo_saida, 'w', encoding='latin-1') as saida:
            for linha in entrada:
                campos = linha.strip().split('|')
                
                # Verifica se é um registro C170 e ajusta o campo COD_CTA
                if len(campos) > 1 and campos[1] == 'C170':
                    if len(campos) >= 36 and campos[-2] == '':
                        campos[-2] = cod_cta_valor  # Ajusta o campo COD_CTA com o valor fornecido
                
                # Verifica se é um registro C175 e ajusta o campo COD_CTA
                elif len(campos) > 1 and campos[1] == 'C175':
                    if len(campos) >= 20 and campos[-3] == '':
                        campos[-3] = cod_cta_valor  # Ajusta o campo COD_CTA com o valor fornecido
                    
                # Reconstrói a linha ajustada
                linha_ajustada = '|'.join(campos)
                saida.write(linha_ajustada + '\n')
        
        messagebox.showinfo("Processamento Concluído", f"O arquivo ajustado foi salvo como '{arquivo_saida}'")
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
    cod_cta_valor = entry_cod_cta.get()
    
    if not input_file or not output_file:
        messagebox.showwarning("Aviso", "Por favor, selecione os arquivos de entrada e saída.")
    elif not cod_cta_valor:
        messagebox.showwarning("Aviso", "Por favor, insira um valor para o campo COD_CTA.")
    else:
        ajustar_cod_cta_c170_c175(input_file, output_file, cod_cta_valor)

# Configuração da janela principal
root = tk.Tk()
root.title("Ajustador de Campos COD_CTA para C170 e C175")
root.geometry("600x300")

# Campo para selecionar o arquivo de entrada
tk.Label(root, text="Arquivo de Entrada (SPED):").pack(pady=5)
frame_input = tk.Frame(root)
frame_input.pack(pady=5)
entry_input = tk.Entry(frame_input, width=50)
entry_input.pack(side=tk.LEFT, padx=5)
tk.Button(frame_input, text="Selecionar", command=select_input_file).pack(side=tk.LEFT)

# Campo para selecionar o arquivo de saída
tk.Label(root, text="Arquivo de Saída (Ajustado):").pack(pady=5)
frame_output = tk.Frame(root)
frame_output.pack(pady=5)
entry_output = tk.Entry(frame_output, width=50)
entry_output.pack(side=tk.LEFT, padx=5)
tk.Button(frame_output, text="Selecionar", command=select_output_file).pack(side=tk.LEFT)

# Campo para inserir o valor do COD_CTA
tk.Label(root, text="Valor para o Campo COD_CTA:").pack(pady=5)
frame_cod_cta = tk.Frame(root)
frame_cod_cta.pack(pady=5)
entry_cod_cta = tk.Entry(frame_cod_cta, width=10)
entry_cod_cta.pack(side=tk.LEFT, padx=5)

# Botão para iniciar o processamento
tk.Button(root, text="Iniciar Ajuste", command=start_processing).pack(pady=20)

root.mainloop()