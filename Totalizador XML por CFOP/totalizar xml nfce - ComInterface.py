import os
import tkinter as tk
from tkinter import filedialog, ttk, messagebox
import xml.etree.ElementTree as ET
from collections import defaultdict
import threading

# Função para parsear um arquivo XML e retornar totais por CFOP, alíquota e modelo
def parse_xml_file(file_path):
    namespaces = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
    tree = ET.parse(file_path)
    root = tree.getroot()
    mod_element = root.find('.//nfe:mod', namespaces)
    modelo_nota = mod_element.text if mod_element is not None else "Desconhecido"
    
    totals_by_cfop_aliquot_model = defaultdict(lambda: {'vBC': 0.0, 'vICMS': 0.0, 'vProd': 0.0})
    
    for det in root.findall('.//nfe:det', namespaces):
        cfop = det.find('.//nfe:CFOP', namespaces).text
        icms_element = det.find('.//nfe:ICMS', namespaces)
        vProd_element = det.find('.//nfe:vProd', namespaces)
        
        if icms_element is not None and cfop is not None and vProd_element is not None:
            vBC = float(icms_element.find('.//nfe:vBC', namespaces).text or 0.0) if icms_element.find('.//nfe:vBC', namespaces) is not None else 0.0
            vICMS = float(icms_element.find('.//nfe:vICMS', namespaces).text or 0.0) if icms_element.find('.//nfe:vICMS', namespaces) is not None else 0.0
            pICMS = float(icms_element.find('.//nfe:pICMS', namespaces).text or 0.0) if icms_element.find('.//nfe:pICMS', namespaces) is not None else 0.0
            vProd = float(vProd_element.text or 0.0)
            
            key = (cfop, pICMS, modelo_nota)
            totals_by_cfop_aliquot_model[key]['vBC'] += vBC
            totals_by_cfop_aliquot_model[key]['vICMS'] += vICMS
            totals_by_cfop_aliquot_model[key]['vProd'] += vProd
    
    return totals_by_cfop_aliquot_model

# Função para somar ICMS por CFOP, alíquota e modelo da nota com barra de progresso
def sum_icms_by_cfop_aliquot_and_model(directory, progress_var, result_callback):
    final_totals = defaultdict(lambda: {'vBC': 0.0, 'vICMS': 0.0, 'vProd': 0.0})
    xml_files = [f for f in os.listdir(directory) if f.endswith('.xml')]
    total_files = len(xml_files)

    for index, file_name in enumerate(xml_files):
        file_path = os.path.join(directory, file_name)
        cfop_totals = parse_xml_file(file_path)
        for key, totals in cfop_totals.items():
            final_totals[key]['vBC'] += totals['vBC']
            final_totals[key]['vICMS'] += totals['vICMS']
            final_totals[key]['vProd'] += totals['vProd']
        
        # Atualiza a barra de progresso
        progress_var.set((index + 1) / total_files * 100)
        root.update_idletasks()  # Atualiza a interface

    result_callback(final_totals)

def load_directory():
    directory = filedialog.askdirectory()
    if not directory:
        return
    
    # Função de retorno para exibir os resultados após o processamento
    def display_results(result):
        # Limpa a tabela antes de inserir novos dados
        for item in tree.get_children():
            tree.delete(item)
        
        # Tabela temporária para calcular os totais por CFOP
        cfop_totals = defaultdict(lambda: {'vBC': 0.0, 'vICMS': 0.0, 'vProd': 0.0})

        # Popula a tabela e acumula os totais
        for (cfop, pICMS, modelo_nota), totals in result.items():
            tree.insert('', 'end', values=(modelo_nota, cfop, f"{pICMS:.2f}", f"{totals['vProd']:.2f}", f"{totals['vBC']:.2f}", f"{totals['vICMS']:.2f}"))
            cfop_totals[cfop]['vBC'] += totals['vBC']
            cfop_totals[cfop]['vICMS'] += totals['vICMS']
            cfop_totals[cfop]['vProd'] += totals['vProd']
        
        # Exibe os totais por CFOP na tabela de totais
        for item in totals_tree.get_children():
            totals_tree.delete(item)
        
        for cfop, totals in cfop_totals.items():
            totals_tree.insert('', 'end', values=(cfop, f"{totals['vProd']:.2f}", f"{totals['vBC']:.2f}", f"{totals['vICMS']:.2f}"))
        
        messagebox.showinfo("Concluído", "Processamento concluído com sucesso!")

    try:
        progress_var.set(0)
        # Executa o processamento em uma thread separada
        processing_thread = threading.Thread(target=sum_icms_by_cfop_aliquot_and_model, args=(directory, progress_var, display_results))
        processing_thread.start()
    except Exception as e:
        messagebox.showerror("Erro", f"Ocorreu um erro ao processar os arquivos: {e}")

# Interface gráfica
root = tk.Tk()
root.title("Totalizador ICMS por CFOP e Alíquota")
root.geometry("800x600")

frame = tk.Frame(root)
frame.pack(pady=10)

select_button = tk.Button(frame, text="Selecionar Diretório", command=load_directory)
select_button.pack()

# Barra de progresso
progress_var = tk.DoubleVar()
progress_bar = ttk.Progressbar(root, variable=progress_var, maximum=100)
progress_bar.pack(fill="x", padx=20, pady=10)

# Tabela para exibir os resultados detalhados
columns = ("Modelo", "CFOP", "Alíquota (%)", "Total vProd", "Total vBC", "Total vICMS")
tree = ttk.Treeview(root, columns=columns, show="headings", height=10)

for col in columns:
    tree.heading(col, text=col)
    tree.column(col, anchor="center", stretch=True)

tree.pack(fill="both", expand=True)

# Tabela para exibir os totais por CFOP
totals_label = tk.Label(root, text="Totais por CFOP", font=("Arial", 12, "bold"))
totals_label.pack(pady=5)

totals_columns = ("CFOP", "Total vProd", "Total vBC", "Total vICMS")
totals_tree = ttk.Treeview(root, columns=totals_columns, show="headings", height=5)

for col in totals_columns:
    totals_tree.heading(col, text=col)
    totals_tree.column(col, anchor="center", stretch=True)

totals_tree.pack(fill="both", expand=True)

# Barra de rolagem para a tabela de resultados detalhados
scrollbar = ttk.Scrollbar(root, orient="vertical", command=tree.yview)
tree.configure(yscroll=scrollbar.set)
scrollbar.pack(side="right", fill="y")

footer = tk.Label(root, text="Totalizador ICMS por CFOP e Alíquota - Desenvolvido por Cristianfer", font=("Arial", 10))
footer.pack(pady=10)

root.mainloop()