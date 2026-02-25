import tkinter as tk
from tkinter import filedialog, messagebox

def analisar_registro_c175(arquivo_path):
    contador_registros = 0
    
    with open(arquivo_path, 'r', encoding='latin-1') as arquivo:
        for linha in arquivo:
            # Verifica se a linha começa com "C175" e divide os campos por "|"
            if linha.startswith('|C175|'):
                campos = linha.strip().split('|')
                
                # Extraindo os valores dos campos relevantes conforme o layout do SPED
                cst_pis = campos[5]   # CST_PIS é o 6º campo na contagem (índice 5)
                cst_cofins = campos[11]  # CST_COFINS é o 12º campo na contagem (índice 11)
                aliq_pis = campos[7]  # ALIQ_PIS é o 8º campo na contagem (índice 7)
                aliq_cofins = campos[13]  # ALIQ_COFINS é o 14º campo na contagem (índice 13)
                vl_pis = campos[10]   # VL_PIS é o 11º campo na contagem (índice 10)
                vl_cofins = campos[16]  # VL_COFINS é o 17º campo na contagem (índice 16)
                
                # Condição para contagem dos registros conforme os critérios
                if (cst_pis == '01' and cst_cofins == '01' and 
                    (aliq_pis == '0,0000' or aliq_pis == '') and
                    (aliq_cofins == '0,0000' or aliq_cofins == '') and
                    (vl_pis == '0,00' or vl_pis == '') and
                    (vl_cofins == '0,00' or vl_cofins == '')):
                    contador_registros += 1
                    
    return contador_registros

def selecionar_arquivo():
    arquivo_path = filedialog.askopenfilename(
        title="Selecione o arquivo SPED EFD Contribuições",
        filetypes=[("Arquivos TXT", "*.txt")]
    )
    if arquivo_path:
        try:
            quantidade_registros = analisar_registro_c175(arquivo_path)
            messagebox.showinfo("Resultado", f"Quantidade de registros C175 que atendem aos critérios: {quantidade_registros}")
        except Exception as e:
            messagebox.showerror("Erro", f"Ocorreu um erro ao processar o arquivo:\n{e}")

# Configuração da janela principal
janela = tk.Tk()
janela.title("Análise de Registros C175")
janela.geometry("400x200")

# Label de instrução
label_instrucoes = tk.Label(janela, text="Selecione um arquivo SPED EFD Contribuições para análise", padx=20, pady=20)
label_instrucoes.pack()

# Botão para selecionar o arquivo
botao_selecionar = tk.Button(janela, text="Selecionar Arquivo", command=selecionar_arquivo)
botao_selecionar.pack(pady=10)

# Iniciar o loop da interface
janela.mainloop()
