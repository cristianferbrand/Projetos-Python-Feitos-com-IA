import tkinter as tk
from tkinter import filedialog, messagebox
import os

def analisar_e_corrigir_c175(arquivo_path, aliq_pis, aliq_cofins):
    contador_registros = 0
    total_pis = 0.0
    total_cofins = 0.0
    linhas_corrigidas = []

    with open(arquivo_path, 'r', encoding='latin-1') as arquivo:
        for linha in arquivo:
            if linha.startswith('|C175|'):
                campos = linha.strip().split('|')
                
                cst_pis = campos[5]
                cst_cofins = campos[11]
                aliq_pis_campo = campos[7]
                aliq_cofins_campo = campos[13]
                vl_pis = campos[10]
                vl_cofins = campos[16]
                
                if (cst_pis == '01' and cst_cofins == '01' and 
                    (aliq_pis_campo == '0,0000' or aliq_pis_campo == '') and
                    (aliq_cofins_campo == '0,0000' or aliq_cofins_campo == '') and
                    (vl_pis == '0,00' or vl_pis == '') and
                    (vl_cofins == '0,00' or vl_cofins == '')):
                    
                    contador_registros += 1
                    
                    # Calcula os valores de VL_PIS e VL_COFINS
                    valor_base_pis = float(campos[6].replace(',', '.'))
                    valor_base_cofins = float(campos[12].replace(',', '.'))
                    valor_pis_calculado = valor_base_pis * (aliq_pis / 100)
                    valor_cofins_calculado = valor_base_cofins * (aliq_cofins / 100)
                    
                    # Totaliza os valores para C170
                    total_pis += valor_pis_calculado
                    total_cofins += valor_cofins_calculado

                    # Atualiza os campos de alíquota e valor, usando vírgulas
                    campos[7] = f"{aliq_pis:.4f}".replace('.', ',')
                    campos[10] = f"{valor_pis_calculado:.2f}".replace('.', ',')
                    campos[13] = f"{aliq_cofins:.4f}".replace('.', ',')
                    campos[16] = f"{valor_cofins_calculado:.2f}".replace('.', ',')
                
                linhas_corrigidas.append('|'.join(campos) + '\n')
            else:
                linhas_corrigidas.append(linha)
    
    # Salva o arquivo corrigido no mesmo diretório do arquivo importado
    diretorio, nome_arquivo = os.path.split(arquivo_path)
    arquivo_corrigido_path = os.path.join(diretorio, "arquivo_corrigido_" + nome_arquivo)
    with open(arquivo_corrigido_path, 'w', encoding='latin-1') as arquivo_corrigido:
        arquivo_corrigido.writelines(linhas_corrigidas)
    
    return contador_registros, total_pis, total_cofins, arquivo_corrigido_path

def selecionar_arquivo():
    arquivo_path = filedialog.askopenfilename(
        title="Selecione o arquivo SPED EFD Contribuições",
        filetypes=[("Arquivos TXT", "*.txt")]
    )
    if arquivo_path:
        try:
            # Obtém os valores das alíquotas de PIS e COFINS da entrada do usuário
            aliq_pis = float(entry_pis.get())
            aliq_cofins = float(entry_cofins.get())

            # Executa a análise e correção
            quantidade_registros, total_pis, total_cofins, arquivo_corrigido_path = analisar_e_corrigir_c175(
                arquivo_path, aliq_pis, aliq_cofins
            )

            # Exibe os resultados
            messagebox.showinfo(
                "Resultado",
                f"Quantidade de registros C175 corrigidos: {quantidade_registros}\n"
                f"Total de PIS corrigido no C170: {total_pis:.2f}\n"
                f"Total de Cofins corrigido no C170: {total_cofins:.2f}\n"
                f"O arquivo corrigido foi salvo em: {arquivo_corrigido_path}"
            )
        except ValueError:
            messagebox.showerror("Erro", "Por favor, insira valores numéricos válidos para as alíquotas.")
        except Exception as e:
            messagebox.showerror("Erro", f"Ocorreu um erro ao processar o arquivo:\n{e}")

# Configuração da janela principal
janela = tk.Tk()
janela.title("Análise e Correção de Registros C175")
janela.geometry("400x300")

# Label de instrução
label_instrucoes = tk.Label(janela, text="Insira as alíquotas de PIS e Cofins e selecione o arquivo", padx=20, pady=10)
label_instrucoes.pack()

# Campo de entrada para alíquota de PIS
label_pis = tk.Label(janela, text="Alíquota do PIS (%)")
label_pis.pack()
entry_pis = tk.Entry(janela)
entry_pis.pack(pady=5)

# Campo de entrada para alíquota de COFINS
label_cofins = tk.Label(janela, text="Alíquota do Cofins (%)")
label_cofins.pack()
entry_cofins = tk.Entry(janela)
entry_cofins.pack(pady=5)

# Botão para selecionar o arquivo
botao_selecionar = tk.Button(janela, text="Selecionar Arquivo", command=selecionar_arquivo)
botao_selecionar.pack(pady=20)

# Iniciar o loop da interface
janela.mainloop()