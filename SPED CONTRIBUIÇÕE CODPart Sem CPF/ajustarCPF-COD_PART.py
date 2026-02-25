import tkinter as tk
from tkinter import filedialog, messagebox
import os

def corrigir_cpf_0150_interface():
    caminho_entrada = filedialog.askopenfilename(
        title="Selecione o arquivo SPED Contribuições",
        filetypes=[("Arquivos TXT", "*.txt")]
    )

    if not caminho_entrada:
        return

    try:
        with open(caminho_entrada, 'r', encoding='latin-1') as f:
            linhas = f.readlines()

        linhas_corrigidas = []
        contador_corrigido = 0

        for linha in linhas:
            if linha.startswith('|0150|'):
                campos = linha.strip().split('|')

                # Remove campos vazios extras no final
                while campos and campos[-1] == '':
                    campos.pop()

                # Garante exatamente 13 campos após o REG (total 14 com REG incluído)
                while len(campos) < 14:
                    campos.append('')

                cod_part = campos[2]
                cpf = campos[6]

                if cod_part.startswith('CLI') and cpf.strip() == '':
                    campos[6] = '19100000000'
                    contador_corrigido += 1

                linha_corrigida = '|'.join(campos[:14]) + '|\n'
                linhas_corrigidas.append(linha_corrigida)
            else:
                linhas_corrigidas.append(linha)

        nome_base = os.path.basename(caminho_entrada)
        pasta_destino = os.path.dirname(caminho_entrada)
        nome_saida = os.path.join(pasta_destino, f"corrigido_{nome_base}")

        with open(nome_saida, 'w', encoding='latin-1') as f:
            f.writelines(linhas_corrigidas)

        messagebox.showinfo("Concluído",
            f"Arquivo salvo como:\n{nome_saida}\n\n"
            f"Registros 0150 corrigidos: {contador_corrigido}")

    except Exception as e:
        messagebox.showerror("Erro", f"Erro ao processar o arquivo:\n{str(e)}")

# Interface Tkinter
janela = tk.Tk()
janela.title("Correção de CPF em SPED Contribuições")
janela.geometry("480x200")

label = tk.Label(janela, text="Corrigir CPF vazio no Registro 0150 com COD_PART iniciando em 'CLI'", pady=20)
label.pack()

botao = tk.Button(janela, text="Selecionar e Corrigir Arquivo", command=corrigir_cpf_0150_interface)
botao.pack(pady=10)

janela.mainloop()