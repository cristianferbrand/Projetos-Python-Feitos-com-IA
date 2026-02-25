import tkinter as tk
from tkinter import filedialog, messagebox
import os


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


def processar_arquivos_multiplos(lista_arquivos):
    """Processa múltiplos arquivos e ajusta registros |C170| conforme necessário."""
    try:
        for arquivo_entrada in lista_arquivos:
            registros_0200 = carregar_registros_0200(arquivo_entrada)

            # Define o nome do arquivo de saída com o sufixo -AjustadoUN
            pasta, nome_arquivo = os.path.split(arquivo_entrada)
            nome_arquivo_saida = os.path.splitext(nome_arquivo)[0] + '-AjustadoUN.txt'
            arquivo_saida = os.path.join(pasta, nome_arquivo_saida)

            # Processar o arquivo
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

        messagebox.showinfo("Concluído", "Todos os arquivos foram processados com sucesso.")
    except Exception as e:
        messagebox.showerror("Erro", f"Ocorreu um erro: {e}")


def selecionar_arquivos_entrada():
    """Seleciona múltiplos arquivos de entrada."""
    caminhos = filedialog.askopenfilenames(title="Selecione os arquivos de entrada", filetypes=[("Arquivos TXT", "*.txt")])
    if caminhos:
        entrada_var.set("\n".join(caminhos))
        lista_arquivos.extend(caminhos)
        atualizar_texto_entrada()


def atualizar_texto_entrada():
    """Atualiza a área de texto com os arquivos selecionados."""
    entrada_text.delete("1.0", tk.END)
    entrada_text.insert(tk.END, "\n".join(lista_arquivos))


# Interface Gráfica
app = tk.Tk()
app.title("Ajuste de Unidade Registro |C170| - Múltiplos Arquivos")
app.geometry("800x600")
app.resizable(False, False)

# Variáveis
entrada_var = tk.StringVar()
lista_arquivos = []

# Layout
frame_entrada = tk.Frame(app, padx=10, pady=10, relief=tk.RIDGE, borderwidth=2)
frame_entrada.pack(fill=tk.BOTH, padx=10, pady=5, expand=True)

frame_botoes = tk.Frame(app, padx=10, pady=10)
frame_botoes.pack(fill=tk.X, padx=10, pady=5)

# Layout - Arquivo de Entrada
tk.Label(frame_entrada, text="Arquivos de Entrada:", font=("Arial", 10, "bold")).pack(anchor=tk.W, padx=5, pady=5)

# Frame interno para o texto e a barra de rolagem
frame_texto_scroll = tk.Frame(frame_entrada)
frame_texto_scroll.pack(fill=tk.BOTH, padx=5, pady=5, expand=True)

# Caixa de texto
entrada_text = tk.Text(frame_texto_scroll, height=15, wrap=tk.WORD)
entrada_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

# Barra de rolagem
scrollbar = tk.Scrollbar(frame_texto_scroll, orient=tk.VERTICAL, command=entrada_text.yview)
scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

# Vincular a barra de rolagem à caixa de texto
entrada_text.config(yscrollcommand=scrollbar.set)

# Botão para selecionar arquivos
tk.Button(frame_entrada, text="Selecionar Arquivos", command=selecionar_arquivos_entrada, width=20).pack(pady=5)

# Botão de Processar
tk.Button(frame_botoes, text="Processar", command=lambda: processar_arquivos_multiplos(lista_arquivos), width=20, bg="#4CAF50", fg="white", font=("Arial", 12, "bold")).pack(pady=10)

# Inicializar a aplicação
app.mainloop()