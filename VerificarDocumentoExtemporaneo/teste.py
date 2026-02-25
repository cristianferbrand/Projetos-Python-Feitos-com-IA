import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext
import os


def analisar_registros():
    mes = entry_mes.get().zfill(2)
    ano = entry_ano.get()

    # Validações
    if not (mes.isdigit() and len(mes) == 2 and 1 <= int(mes) <= 12):
        messagebox.showerror("Erro", "Mês inválido. Informe no formato MM.")
        return

    if not (ano.isdigit() and len(ano) == 4):
        messagebox.showerror("Erro", "Ano inválido. Informe no formato AAAA.")
        return

    arquivo = filedialog.askopenfilename(
        title="Selecione o arquivo SPED",
        filetypes=[("Arquivo TXT", "*.txt")]
    )

    if not arquivo:
        return

    with open(arquivo, 'r', encoding='utf-8') as file:
        linhas = file.readlines()

    resultados = []

    for linha in linhas:
        if linha.startswith('|C100|'):
            campos = linha.strip().split('|')
            dt_doc = campos[9]  # DT_DOC
            cod_sit = campos[5]  # COD_SIT

            if len(dt_doc) == 8 and dt_doc[2:4] == mes and dt_doc[4:8] == ano:
                if cod_sit != '01':
                    serie = campos[6]
                    num_doc = campos[7]
                    chave_nfe = campos[8]
                    resultados.append(
                        f"Série: {serie} - Número: {num_doc} - Chave: {chave_nfe} - DT_DOC: {dt_doc} - COD_SIT: {cod_sit}"
                    )

    if resultados:
        janela_resultado = tk.Toplevel()
        janela_resultado.title("Registros Encontrados")
        janela_resultado.geometry("800x400")

        texto = scrolledtext.ScrolledText(janela_resultado, wrap=tk.WORD, width=100, height=20)
        texto.pack(padx=10, pady=10)

        texto.insert(tk.END, '\n'.join(resultados))
        texto.config(state=tk.DISABLED)

    else:
        messagebox.showinfo("Análise concluída", "Nenhum registro C100 encontrado com mês/ano selecionado diferente de COD_SIT 01.")


def ajustar_cod_sit_personalizado():
    mes = entry_mes.get().zfill(2)
    ano = entry_ano.get()
    novo_cod_sit = entry_cod_sit.get()

    if not (mes.isdigit() and len(mes) == 2 and 1 <= int(mes) <= 12):
        messagebox.showerror("Erro", "Mês inválido. Informe no formato MM.")
        return

    if not (ano.isdigit() and len(ano) == 4):
        messagebox.showerror("Erro", "Ano inválido. Informe no formato AAAA.")
        return

    if not (novo_cod_sit in ['00', '01', '02', '03', '04', '05', '06', '07', '08']):
        messagebox.showerror(
            "Erro",
            "COD_SIT inválido. Informe um dos seguintes códigos:\n00, 01, 02, 03, 04, 05, 06, 07 ou 08."
        )
        return

    arquivo = filedialog.askopenfilename(
        title="Selecione o arquivo SPED",
        filetypes=[("Arquivo TXT", "*.txt")]
    )

    if not arquivo:
        return

    with open(arquivo, 'r', encoding='utf-8') as file:
        linhas = file.readlines()

    novas_linhas = []
    contador_ajustes = 0

    for linha in linhas:
        if linha.startswith('|C100|'):
            campos = linha.strip().split('|')
            dt_doc = campos[9]  # DT_DOC

            if len(dt_doc) == 8 and dt_doc[2:4] == mes and dt_doc[4:8] == ano:
                campos[5] = novo_cod_sit  # Altera COD_SIT
                linha = '|'.join(campos) + '|\n'
                contador_ajustes += 1
        
        novas_linhas.append(linha)

    nome_saida = os.path.splitext(arquivo)[0] + '_ajustado.txt'
    with open(nome_saida, 'w', encoding='utf-8') as file:
        file.writelines(novas_linhas)

    messagebox.showinfo(
        "Processo concluído",
        f"Arquivo salvo como:\n{nome_saida}\n\n"
        f"Total de registros C100 ajustados: {contador_ajustes}"
    )


# Interface gráfica
janela = tk.Tk()
janela.title("Ajustar COD_SIT em C100 por Mês e Ano")
janela.geometry("600x350")

# Label de instruções
label_instrucao = tk.Label(
    janela,
    text="Preencha os campos abaixo para:\n"
         "→ Analisar registros com mês/ano diferente de COD_SIT 01\n"
         "→ Ajustar COD_SIT dos registros C100 com base no mês e ano.",
    justify="center",
    font=("Arial", 10)
)
label_instrucao.pack(pady=10)

# Campos de entrada
frame = tk.Frame(janela)
frame.pack(pady=10)

tk.Label(frame, text="Mês (MM):").grid(row=0, column=0, padx=5, pady=5)
entry_mes = tk.Entry(frame, width=5)
entry_mes.grid(row=0, column=1, padx=5, pady=5)

tk.Label(frame, text="Ano (AAAA):").grid(row=0, column=2, padx=5, pady=5)
entry_ano = tk.Entry(frame, width=7)
entry_ano.grid(row=0, column=3, padx=5, pady=5)

tk.Label(frame, text="Novo COD_SIT:").grid(row=0, column=4, padx=5, pady=5)
entry_cod_sit = tk.Entry(frame, width=5)
entry_cod_sit.grid(row=0, column=5, padx=5, pady=5)

# Botões
frame_botoes = tk.Frame(janela)
frame_botoes.pack(pady=10)

botao_analise = tk.Button(
    frame_botoes,
    text="Analisar Registros Diferentes de 01",
    command=analisar_registros,
    width=30,
    height=2,
    bg="orange",
    fg="black"
)
botao_analise.grid(row=0, column=0, padx=10)

botao_ajustar = tk.Button(
    frame_botoes,
    text="Selecionar Arquivo e Ajustar COD_SIT",
    command=ajustar_cod_sit_personalizado,
    width=30,
    height=2,
    bg="green",
    fg="white"
)
botao_ajustar.grid(row=0, column=1, padx=10)

# Executa a interface
janela.mainloop()