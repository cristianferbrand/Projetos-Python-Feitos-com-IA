import tkinter as tk
from tkinter import filedialog, messagebox

def ajustar_campo_c116(linha):
    """
    Função para processar e ajustar o campo 3 do registro C116.
    """
    campos = linha.strip().split('|')  # Divide os campos do registro
    if campos[1] == "C116":  # Verifica se é um registro C116
        chv_cfe = campos[4]  # CHV_CFE está no campo 4
        num_serie_sat = chv_cfe[22:31]  # Extrai da posição 22 a 31
        campos[3] = num_serie_sat  # Atualiza o campo 3 com o número de série do SAT
        return '|'.join(campos) + '\n'  # Recompõe a linha ajustada
    return linha  # Retorna a linha original para outros registros

def processar_arquivo():
    """
    Processa o arquivo selecionado e salva o resultado ajustado.
    """
    arquivo_entrada = filedialog.askopenfilename(title="Selecione o arquivo de entrada", filetypes=[("Arquivos TXT", "*.txt")])
    if not arquivo_entrada:
        return

    arquivo_saida = filedialog.asksaveasfilename(title="Salvar arquivo ajustado como", defaultextension=".txt", filetypes=[("Arquivos TXT", "*.txt")])
    if not arquivo_saida:
        return

    try:
        with open(arquivo_entrada, "r") as entrada, open(arquivo_saida, "w") as saida:
            for linha in entrada:
                saida.write(ajustar_campo_c116(linha))

        messagebox.showinfo("Sucesso", f"Arquivo processado e salvo em:\n{arquivo_saida}")
    except Exception as e:
        messagebox.showerror("Erro", f"Ocorreu um erro ao processar o arquivo:\n{e}")

def criar_interface():
    """
    Cria a interface gráfica para o processamento de arquivos.
    """
    janela = tk.Tk()
    janela.title("Processador de Registros C116")

    label_titulo = tk.Label(janela, text="Processador de Registros C116", font=("Arial", 14, "bold"))
    label_titulo.pack(pady=10)

    btn_processar = tk.Button(janela, text="Selecionar e Processar Arquivo", command=processar_arquivo, font=("Arial", 12))
    btn_processar.pack(pady=20)

    btn_sair = tk.Button(janela, text="Sair", command=janela.quit, font=("Arial", 12))
    btn_sair.pack(pady=10)

    janela.geometry("400x200")
    janela.mainloop()

if __name__ == "__main__":
    criar_interface()