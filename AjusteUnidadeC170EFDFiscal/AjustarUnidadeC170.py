import tkinter as tk
from tkinter import filedialog, messagebox


def carregar_registros_0200(arquivo_entrada):
    """Carrega os registros |0200| em memória."""
    registros_0200 = {}
    with open(arquivo_entrada, 'r', encoding='latin-1') as entrada:
        for linha in entrada:
            if linha.startswith('|0200|'):
                campos = linha.strip().split('|')
                if len(campos) > 6:
                    cod_produto = campos[2]  # Código do produto
                    unidade_produto = campos[6]  # Unidade do produto
                    registros_0200[cod_produto] = unidade_produto
    return registros_0200


def processar_arquivos(arquivo_entrada, arquivo_saida):
    """Processa o arquivo e ajusta registros |C170| conforme necessário."""
    try:
        registros_0200 = carregar_registros_0200(arquivo_entrada)

        with open(arquivo_entrada, 'r', encoding='latin-1') as entrada, open(arquivo_saida, 'w', encoding='latin-1') as saida:
            for linha in entrada:
                if not linha.startswith('|C170|'):
                    saida.write(linha)
                else:
                    campos = linha.strip().split('|')
                    if len(campos) > 6:
                        cod_produto = campos[3]  # Código do produto no registro |C170|
                        unidade_atual = campos[6]  # Unidade no registro |C170|

                        # Verificar se o código do produto existe no registro |0200| e se as unidades diferem
                        unidade_correta = registros_0200.get(cod_produto)
                        if unidade_correta and unidade_correta != unidade_atual:
                            campos[6] = unidade_correta  # Atualizar a unidade no registro |C170|
                            linha_atualizada = '|'.join(campos) + '\n'  # Montar linha sem `|` final
                            saida.write(linha_atualizada)
                        else:
                            saida.write(linha)

        messagebox.showinfo("Concluído", "Processamento finalizado com sucesso.")
    except Exception as e:
        messagebox.showerror("Erro", f"Ocorreu um erro: {e}")


def selecionar_arquivo_entrada():
    """Seleciona o arquivo de entrada."""
    caminho = filedialog.askopenfilename(title="Selecione o arquivo de entrada", filetypes=[("Arquivos TXT", "*.txt")])
    if caminho:
        entrada_var.set(caminho)


def selecionar_arquivo_saida():
    """Seleciona o arquivo de saída."""
    caminho = filedialog.asksaveasfilename(title="Selecione o arquivo de saída", defaultextension=".txt", filetypes=[("Arquivos TXT", "*.txt")])
    if caminho:
        saida_var.set(caminho)


# Interface Gráfica
app = tk.Tk()
app.title("Processador de Registros |C170|")
app.geometry("700x300")
app.resizable(False, False)

# Estilo
frame_entrada = tk.Frame(app, padx=10, pady=10, relief=tk.RIDGE, borderwidth=2)
frame_entrada.pack(fill=tk.X, padx=10, pady=5)

frame_saida = tk.Frame(app, padx=10, pady=10, relief=tk.RIDGE, borderwidth=2)
frame_saida.pack(fill=tk.X, padx=10, pady=5)

frame_botoes = tk.Frame(app, padx=10, pady=10)
frame_botoes.pack(fill=tk.X, padx=10, pady=5)

# Variáveis
entrada_var = tk.StringVar()
saida_var = tk.StringVar()

# Layout - Arquivo de Entrada
tk.Label(frame_entrada, text="Arquivo de Entrada:", font=("Arial", 10, "bold")).grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
tk.Entry(frame_entrada, textvariable=entrada_var, width=50).grid(row=0, column=1, padx=5, pady=5)
tk.Button(frame_entrada, text="Selecionar", command=selecionar_arquivo_entrada, width=15).grid(row=0, column=2, padx=5, pady=5)

# Layout - Arquivo de Saída
tk.Label(frame_saida, text="Arquivo de Saída:", font=("Arial", 10, "bold")).grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
tk.Entry(frame_saida, textvariable=saida_var, width=50).grid(row=0, column=1, padx=5, pady=5)
tk.Button(frame_saida, text="Selecionar", command=selecionar_arquivo_saida, width=15).grid(row=0, column=2, padx=5, pady=5)

# Botão de Processar
tk.Button(frame_botoes, text="Processar", command=lambda: processar_arquivos(entrada_var.get(), saida_var.get()), width=20, bg="#4CAF50", fg="white", font=("Arial", 12, "bold")).pack(pady=10)

# Inicializar a aplicação
app.mainloop()