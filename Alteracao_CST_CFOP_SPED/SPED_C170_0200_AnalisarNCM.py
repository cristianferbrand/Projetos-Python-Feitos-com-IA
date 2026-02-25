import tkinter as tk
from tkinter import filedialog, scrolledtext

def selecionar_arquivo():
    arquivo = filedialog.askopenfilename(title="Selecione o arquivo SPED")
    if arquivo:
        processar_arquivo(arquivo)

def processar_arquivo(arquivo):
    c170_farmaceuticos = []
    c170_perfumaria = []
    c170_excecoes = []
    ncm_dict = {}
    
    # Lista de prefixos de NCMs para Produtos Farmacêuticos
    ncm_farmaceuticos_prefixos = {
        "3001", "3003", "3004", "3002101", "3002102", "3002103", "3002201",
        "3002202", "3006301", "3006302", "30029020", "30029092", "30029099",
        "30051010", "30066000"
    }
    
    # Lista de NCMs de exceção para Produtos Farmacêuticos
    ncm_excecoes_farmaceuticos = {"30039056", "30049046"}
    
    # Lista de prefixos de NCMs para Produtos de Perfumaria, Toucador ou Higiene Pessoal
    ncm_perfumaria_prefixos = {"3303", "3307", "34011190", "34012010", "96032100"}
    
    with open(arquivo, 'r', encoding='latin-1') as f:
        for linha in f:
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
    
    resultado = "Produtos Farmacêuticos Sujeitos à Incidência Monofásica da Contribuição Social:\n\n" + "\n".join(farmaceuticos) + "\n\n" if farmaceuticos else "Nenhum item farmacêutico encontrado.\n\n"
    resultado += "Produtos de Perfumaria, Toucador ou Higiene Pessoal Sujeitos à Incidência Monofásica da Contribuição Social:\n\n" + "\n".join(perfumaria) + "\n\n" if perfumaria else "Nenhum item de perfumaria encontrado.\n\n"
    resultado += "Itens para analisar se são Sujeitos à Incidência Monofásica da Contribuição Social :\n\n" + "\n".join(excecoes) if excecoes else "Nenhum item encontrado para analisar."
    
    text_area.insert(tk.INSERT, resultado)
    text_area.config(state=tk.DISABLED)

# Interface gráfica
root = tk.Tk()
root.title("Verificar NCM de Itens C170")
root.geometry("700x600")

tk.Label(root, text="Selecione o arquivo SPED:").pack(pady=10)
tk.Button(root, text="Escolher Arquivo", command=selecionar_arquivo).pack()

text_area = scrolledtext.ScrolledText(root, wrap=tk.WORD, width=80, height=30)
text_area.pack(padx=10, pady=10, fill=tk.BOTH, expand=True)
text_area.config(state=tk.DISABLED)

root.mainloop()
