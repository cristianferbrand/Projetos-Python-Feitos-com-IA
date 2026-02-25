import tkinter as tk
from tkinter import filedialog, scrolledtext, messagebox
import os

# Definição de variáveis globais
ncm_dict = {}
ncm_farmaceuticos_prefixos = {
    "3001", "3003", "3004", "3002101", "3002102", "3002103", "3002201",
    "3002202", "3006301", "3006302", "30029020", "30029092", "30029099",
    "30051010", "30066000"
}
ncm_excecoes_farmaceuticos = {"30039056", "30049046"}
ncm_perfumaria_prefixos = {"3303", "3307", "34011190", "34012010", "96032100"}

def selecionar_arquivo():
    global arquivo_atual
    arquivo_atual = filedialog.askopenfilename(title="Selecione o arquivo SPED")
    if arquivo_atual:
        processar_arquivo(arquivo_atual)

def processar_arquivo(arquivo):
    global ncm_dict  # Torna a variável global acessível
    c170_farmaceuticos = []
    c170_perfumaria = []
    c170_excecoes = []
    
    with open(arquivo, 'r', encoding='latin-1') as f:
        linhas = f.readlines()
    
    for linha in linhas:
        campos = linha.strip().split('|')
        
        if len(campos) > 1:
            if campos[1] == '0200':  # Captura o NCM do item
                cod_item = campos[2].strip()
                ncm = campos[8].strip() if len(campos) > 8 else 'NCM Não Encontrado'
                ncm_dict[cod_item] = ncm
            
            elif campos[1] == 'C170':  # Verifica os itens do C170
                if len(campos) > 31:  # Garante que CST_PIS e CST_COFINS existem
                    cod_item = campos[3].strip()
                    cst_pis = campos[25].strip()
                    cst_cofins = campos[31].strip()
                    
                    if cst_pis == '50' and cst_cofins == '50':
                        ncm = ncm_dict.get(cod_item, 'NCM Não Encontrado')
                        
                        if any(ncm.startswith(prefix) for prefix in ncm_farmaceuticos_prefixos) and ncm not in ncm_excecoes_farmaceuticos:
                            c170_farmaceuticos.append(f"COD_ITEM: {cod_item}, NCM: {ncm}")
                        elif any(ncm.startswith(prefix) for prefix in ncm_perfumaria_prefixos):
                            c170_perfumaria.append(f"COD_ITEM: {cod_item}, NCM: {ncm}")
                        else:
                            c170_excecoes.append(f"COD_ITEM: {cod_item}, NCM: {ncm}")
    
    atualizar_resultado(c170_farmaceuticos, c170_perfumaria, c170_excecoes)

def atualizar_resultado(farmaceuticos, perfumaria, excecoes):
    text_area.config(state=tk.NORMAL)
    text_area.delete('1.0', tk.END)
    
    resultado = "Produtos Farmacêuticos Sujeitos a Incidência Monofásica:\n\n" + "\n".join(farmaceuticos) + "\n\n" if farmaceuticos else "Nenhum item farmacêutico encontrado.\n\n"
    resultado += "Produtos de Perfumaria, Toucador ou Higiene Pessoal Sujeitos a Incidência Monofásica:\n\n" + "\n".join(perfumaria) + "\n\n" if perfumaria else "Nenhum item de perfumaria encontrado.\n\n"
    resultado += "Itens para analisar:\n\n" + "\n".join(excecoes) if excecoes else "Nenhum item para analisar."
    
    text_area.insert(tk.INSERT, resultado)
    text_area.config(state=tk.DISABLED)

def ajustar_c170():
    if not arquivo_atual:
        return
    
    # Criar o nome do arquivo ajustado
    pasta, nome_arquivo = os.path.split(arquivo_atual)
    nome_base, extensao = os.path.splitext(nome_arquivo)
    novo_nome_arquivo = os.path.join(pasta, f"{nome_base}_AjustadoCSTPISCOFINS{extensao}")
    
    linhas_ajustadas = []
    with open(arquivo_atual, 'r', encoding='latin-1') as f:
        for linha in f:
            campos = linha.strip().split('|')
            if len(campos) > 1 and campos[1] == 'C170':
                cod_item = campos[3].strip()
                ncm = ncm_dict.get(cod_item, 'NCM Não Encontrado')
                
                if any(ncm.startswith(prefix) for prefix in ncm_farmaceuticos_prefixos) or any(ncm.startswith(prefix) for prefix in ncm_perfumaria_prefixos):
                    campos[25] = '70'  # Ajusta CST_PIS para 70
                    campos[31] = '70'  # Ajusta CST_COFINS para 70
                    
                    # Zerar os campos desejados
                    campos[26] = '0'  # VL_BC_PIS 
                    campos[27] = '0'  # ALIQ_PIS
                    campos[30] = '0'  # VL_PIS
                    campos[32] = '0'  # VL_BC_COFINS
                    campos[33] = '0'  # ALIQ_COFINS
                    campos[36] = '0'  # VL_COFINS
                    
                linha = '|'.join(campos) + '\n'
            linhas_ajustadas.append(linha)
    
    with open(novo_nome_arquivo, 'w', encoding='latin-1') as f:
        f.writelines(linhas_ajustadas)
    
    messagebox.showinfo("Sucesso", f"Arquivo ajustado salvo em:\n{novo_nome_arquivo}")

# Interface gráfica
root = tk.Tk()
root.title("Verificar NCM de Itens C170 Sujeitos a Incidência Monofásica")
root.geometry("800x700")

tk.Label(root, text="Selecione o arquivo SPED:").pack(pady=10)
tk.Button(root, text="Escolher Arquivo", command=selecionar_arquivo).pack()

tk.Button(root, text="Ajustar Registros C170", command=ajustar_c170).pack(pady=10)

text_area = scrolledtext.ScrolledText(root, wrap=tk.WORD, width=80, height=30)
text_area.pack(padx=10, pady=10, fill=tk.BOTH, expand=True)
text_area.config(state=tk.DISABLED)

footer = tk.Label(text="Desenvolvido por Cristianfer", font=("Arial", 10))
footer.pack(pady=10)

root.mainloop()