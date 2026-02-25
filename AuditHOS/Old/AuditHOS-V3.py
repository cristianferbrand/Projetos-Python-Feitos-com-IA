import time
import os
import csv
import sys
import tkinter as tk
from tkinter import filedialog, ttk, messagebox, scrolledtext, simpledialog
import easygui
import xml.etree.ElementTree as ET
from datetime import datetime
from collections import defaultdict
import pandas as pd
import threading
import shutil
import re
import hashlib
import datetime
import fdb

# Totalizador XML por CFOP e Alíquota

def open_totalizador_xml():
    totalizador_window = tk.Toplevel(root)
    totalizador_window.title("Totalizador XML")
    totalizador_window.geometry("800x600")
    
    tk.Button(totalizador_window, text="Selecionar Diretório de XML", command=load_directory).pack(pady=10)
    
    columns = ("Modelo", "CFOP", "Alíquota (%)", "Total vProd", "Total vBC", "Total vICMS")
    tree = ttk.Treeview(totalizador_window, columns=columns, show="headings", height=10)
    for col in columns:
        tree.heading(col, text=col)
        tree.column(col, anchor="center", stretch=True)
    tree.pack(fill="both", expand=True)
    
    totals_label = tk.Label(totalizador_window, text="Totais por CFOP", font=("Arial", 12, "bold"))
    totals_label.pack(pady=5)
    
    totals_columns = ("CFOP", "Total vProd", "Total vBC", "Total vICMS")
    totals_tree = ttk.Treeview(totalizador_window, columns=totals_columns, show="headings", height=5)
    for col in totals_columns:
        totals_tree.heading(col, text=col)
        totals_tree.column(col, anchor="center", stretch=True)
    totals_tree.pack(fill="both", expand=True)

# Funções auxiliares
def convert_to_float(value):
    try:
        return float(value.replace(',', '.'))
    except ValueError:
        return 0.0

def format_brazilian(value):
    return f"{value:.2f}".replace('.', ',')

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
def sum_icms_by_cfop_aliquot_and_model(directory,  result_callback):
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
        root.update_idletasks()  # Update the UI immediately
        root.update_idletasks()  # Atualiza a interface

    result_callback(final_totals)

def load_directory():
    directory = filedialog.askdirectory()
    if not directory:
        return
    
    results_window = tk.Toplevel(root)
    results_window.title("Resultados do Processamento")
    results_window.geometry("800x600")
    
    columns = ("Modelo", "CFOP", "Alíquota (%)", "Total vProd", "Total vBC", "Total vICMS")
    tree = ttk.Treeview(results_window, columns=columns, show="headings", height=10)
    for col in columns:
        tree.heading(col, text=col)
        tree.column(col, anchor="center", stretch=True)
    tree.pack(fill="both", expand=True)
    
    totals_columns = ("CFOP", "Total vProd", "Total vBC", "Total vICMS")
    totals_tree = ttk.Treeview(results_window, columns=totals_columns, show="headings", height=5)
    for col in totals_columns:
        totals_tree.heading(col, text=col)
        totals_tree.column(col, anchor="center", stretch=True)
    totals_tree.pack(fill="both", expand=True)
    
    def display_results(result):
        for item in tree.get_children():
            tree.delete(item)
        
        cfop_totals = defaultdict(lambda: {'vBC': 0.0, 'vICMS': 0.0, 'vProd': 0.0})
        
        for (cfop, pICMS, modelo_nota), totals in result.items():
            tree.insert('', 'end', values=(modelo_nota, cfop, f"{pICMS:.2f}", f"{totals['vProd']:.2f}", f"{totals['vBC']:.2f}", f"{totals['vICMS']:.2f}"))
            cfop_totals[cfop]['vBC'] += totals['vBC']
            cfop_totals[cfop]['vICMS'] += totals['vICMS']
            cfop_totals[cfop]['vProd'] += totals['vProd']
        
        for item in totals_tree.get_children():
            totals_tree.delete(item)
        
        for cfop, totals in cfop_totals.items():
            totals_tree.insert('', 'end', values=(cfop, f"{totals['vProd']:.2f}", f"{totals['vBC']:.2f}", f"{totals['vICMS']:.2f}"))
        
        messagebox.showinfo("Concluído", "Processamento concluído com sucesso!")
    
    try:
        progress_var.set(0)
        root.update_idletasks()
        processing_thread = threading.Thread(target=sum_icms_by_cfop_aliquot_and_model, args=(directory, display_results))
        processing_thread.start()
    except Exception as e:
        messagebox.showerror("Erro", f"Ocorreu um erro ao processar os arquivos: {e}")

# Extrator de Registros SPED C100

def open_extrator_sped():
    sped_window = tk.Toplevel(root)
    sped_window.title("Extrator de Registros SPED C100")
    sped_window.geometry("800x600")
    
    sped_label_frame = tk.LabelFrame(sped_window, text="Seleção de Arquivos", padx=10, pady=10)
    sped_label_frame.pack(fill="x", padx=10, pady=10)
    
    tk.Label(sped_label_frame, text="Arquivo de Entrada (SPED):").grid(row=0, column=0, sticky="w")
    entry_sped_input = tk.Entry(sped_label_frame, width=50)
    entry_sped_input.grid(row=0, column=1, padx=5)
    tk.Button(sped_label_frame, text="Selecionar", command=lambda: entry_sped_input.insert(0, filedialog.askopenfilename())).grid(row=0, column=2)
    
    tk.Label(sped_label_frame, text="Arquivo de Saída (CSV):").grid(row=1, column=0, sticky="w")
    entry_sped_output = tk.Entry(sped_label_frame, width=50)
    entry_sped_output.grid(row=1, column=1, padx=5)
    tk.Button(sped_label_frame, text="Selecionar", command=lambda: entry_sped_output.insert(0, filedialog.asksaveasfilename(defaultextension=".csv"))).grid(row=1, column=2)
    
    nota_type_var = tk.StringVar(value="saida")
    nota_type_frame = tk.Frame(sped_window)
    nota_type_frame.pack(pady=10)
    tk.Label(nota_type_frame, text="Tipo de Nota:").pack(side=tk.LEFT)
    tk.Radiobutton(nota_type_frame, text="Entrada", variable=nota_type_var, value="entrada").pack(side=tk.LEFT, padx=5)
    tk.Radiobutton(nota_type_frame, text="Saída", variable=nota_type_var, value="saida").pack(side=tk.LEFT, padx=5)
    tk.Radiobutton(nota_type_frame, text="Ambas", variable=nota_type_var, value="ambas").pack(side=tk.LEFT, padx=5)
    
    tk.Button(sped_window, text="Processar SPED C100", command=lambda: process_sped_c100(entry_sped_input.get(), entry_sped_output.get(), nota_type_var.get())).pack(pady=20)

def process_sped_c100(input_filename, output_filename, nota_type):
    fields = ["Modelo", "Serie", "Numero", "Total_NF-e", "Total_BC_ICMS", "Total_ICMS", "Total_BC_ICMS_ST", "Total_ICMS_ST", "Chave_NF-e"]
    registros_c100 = []

    try:
        with open(input_filename, 'r', encoding='latin-1') as file:
            for line in file:
                if line.startswith('|C100|'):
                    parts = line.strip().split('|')
                    if len(parts) > 16:
                        if (nota_type == "saida" and parts[2] == "1") or (nota_type == "entrada" and parts[2] == "0") or (nota_type == "ambas"):
                            registro = {
                                "Modelo": parts[5],
                                "Serie": parts[7],
                                "Numero": parts[8],
                                "Total_NF-e": format_brazilian(convert_to_float(parts[12])),
                                "Total_BC_ICMS": format_brazilian(convert_to_float(parts[13])),
                                "Total_ICMS": format_brazilian(convert_to_float(parts[14])),
                                "Total_BC_ICMS_ST": format_brazilian(convert_to_float(parts[15])),
                                "Total_ICMS_ST": format_brazilian(convert_to_float(parts[16])),
                                "Chave_NF-e": parts[9]
                            }
                            registros_c100.append(registro)

        with open(output_filename, mode='w', newline='', encoding='latin-1') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fields, delimiter=';')
            writer.writeheader()
            writer.writerows(registros_c100)
        
        messagebox.showinfo("Processamento Concluído", f"Arquivo '{output_filename}' gerado com sucesso.\nTotal de registros extraídos: {len(registros_c100)}")
    except Exception as e:
        messagebox.showerror("Erro", f"Erro ao processar o arquivo: {e}")

# Comparador de Arquivos CSV

def open_comparador_csv():
    compare_window = tk.Toplevel(root)
    compare_window.title("Comparador de Arquivos CSV")
    compare_window.geometry("800x600")
    
    compare_label_frame = tk.LabelFrame(compare_window, text="Selecionar Arquivos para Comparação", padx=10, pady=10)
    compare_label_frame.pack(fill="x", padx=10, pady=10)
    
    tk.Label(compare_label_frame, text="Extrato SEFAZ:").grid(row=0, column=0, sticky="w")
    entry_file1 = tk.Entry(compare_label_frame, width=50)
    entry_file1.grid(row=0, column=1, padx=5)
    tk.Button(compare_label_frame, text="Selecionar", command=lambda: entry_file1.insert(0, filedialog.askopenfilename())).grid(row=0, column=2)
    
    tk.Label(compare_label_frame, text="Extrato HOS:").grid(row=1, column=0, sticky="w")
    entry_file2 = tk.Entry(compare_label_frame, width=50)
    entry_file2.grid(row=1, column=1, padx=5)
    tk.Button(compare_label_frame, text="Selecionar", command=lambda: entry_file2.insert(0, filedialog.askopenfilename())).grid(row=1, column=2)
    
    result_text = scrolledtext.ScrolledText(compare_window, width=90, height=20)
    result_text.pack(pady=10, padx=10, fill="both", expand=True)
    
    tk.Button(compare_window, text="Comparar", command=lambda: compare_files(entry_file1.get(), entry_file2.get(), result_text)).pack(pady=10)

def compare_files(file1_path, file2_path, result_text):
    try:
        arquivo1 = pd.read_csv(file1_path, delimiter=';', decimal=',')
        arquivo2 = pd.read_csv(file2_path, delimiter=';', decimal=',')

        arquivo1['Numero'] = arquivo1['Numero'].astype(str).str.strip()
        arquivo2['Numero'] = arquivo2['Numero'].astype(str).str.strip()
        arquivo1['Total_NF-e'] = arquivo1['Total_NF-e'].replace(',', '.', regex=True).astype(float)
        arquivo2['Total_NF-e'] = arquivo2['Total_NF-e'].replace(',', '.', regex=True).astype(float)

        comparacao = pd.merge(
            arquivo1[['Numero', 'Serie', 'Total_NF-e', 'Chave_NF-e']],
            arquivo2[['Numero', 'Serie', 'Total_NF-e', 'Chave_NF-e']],
            on='Numero', how='outer', suffixes=('_arquivo1', '_arquivo2'), indicator=True
        )

        diferencas_total_nfe = comparacao[(comparacao['_merge'] == 'both') & 
                                          (comparacao['Total_NF-e_arquivo1'] != comparacao['Total_NF-e_arquivo2'])]
        
        faltantes = comparacao[comparacao['_merge'] != 'both']
        
        result_text.delete("1.0", tk.END)
        if diferencas_total_nfe.empty and faltantes.empty:
            result_text.insert(tk.END, "Não foram encontradas divergências ou registros faltantes.\n")
        else:
            if not diferencas_total_nfe.empty:
                result_text.insert(tk.END, "=== Diferenças no Total_NF-e ===\n\n")
                for _, row in diferencas_total_nfe.iterrows():
                    result_text.insert(tk.END, f"Numero: {row['Numero']}\n")
                    result_text.insert(tk.END, f"  Total_NF-e Arquivo 1: {row['Total_NF-e_arquivo1']}\n")
                    result_text.insert(tk.END, f"  Total_NF-e Arquivo 2: {row['Total_NF-e_arquivo2']}\n")
                    result_text.insert(tk.END, f"  Chave_NF-e Arquivo 1: {row.get('Chave_NF-e_arquivo1', 'N/A')}\n")
                    result_text.insert(tk.END, f"  Chave_NF-e Arquivo 2: {row.get('Chave_NF-e_arquivo2', 'N/A')}\n")
                    result_text.insert(tk.END, "-"*40 + "\n\n")
            
            if not faltantes.empty:
                result_text.insert(tk.END, "=== Registros Faltantes ===\n\n")
                for _, row in faltantes.iterrows():
                    result_text.insert(tk.END, f"Numero: {row['Numero']}\n")
                    if pd.notna(row['Total_NF-e_arquivo1']):
                        result_text.insert(tk.END, "  Presente apenas no Arquivo 1:\n")
                        result_text.insert(tk.END, f"    Total_NF-e: {row['Total_NF-e_arquivo1']}\n")
                        result_text.insert(tk.END, f"    Chave_NF-e: {row.get('Chave_NF-e_arquivo1', 'N/A')}\n")
                    elif pd.notna(row['Total_NF-e_arquivo2']):
                        result_text.insert(tk.END, "  Presente apenas no Arquivo 2:\n")
                        result_text.insert(tk.END, f"    Total_NF-e: {row['Total_NF-e_arquivo2']}\n")
                        result_text.insert(tk.END, f"    Chave_NF-e: {row.get('Chave_NF-e_arquivo2', 'N/A')}\n")
                    result_text.insert(tk.END, "-"*40 + "\n\n")
    except Exception as e:
        messagebox.showerror("Erro", f"Ocorreu um erro ao comparar os arquivos: {e}")


# Ajuste Registro C170 COD_CTA

def open_cod_cta_adjuster():
    cod_cta_window = tk.Toplevel(root)
    cod_cta_window.title("Ajuste COD_CTA")
    cod_cta_window.geometry("800x600")
    
    cod_cta_label_frame = tk.LabelFrame(cod_cta_window, text="Seleção de Arquivos", padx=10, pady=10)
    cod_cta_label_frame.pack(fill="x", padx=10, pady=10)
    
    tk.Label(cod_cta_label_frame, text="Arquivo de Entrada (SPED):").grid(row=0, column=0, sticky="w")
    entry_input = tk.Entry(cod_cta_label_frame, width=50)
    entry_input.grid(row=0, column=1, padx=5)
    tk.Button(cod_cta_label_frame, text="Selecionar", command=lambda: entry_input.insert(0, filedialog.askopenfilename())).grid(row=0, column=2)
    
    tk.Label(cod_cta_label_frame, text="Arquivo de Saída (Ajustado):").grid(row=1, column=0, sticky="w")
    entry_output = tk.Entry(cod_cta_label_frame, width=50)
    entry_output.grid(row=1, column=1, padx=5)
    tk.Button(cod_cta_label_frame, text="Selecionar", command=lambda: entry_output.insert(0, filedialog.asksaveasfilename(defaultextension=".txt"))).grid(row=1, column=2)
    
    cod_cta_valor_frame = tk.LabelFrame(cod_cta_window, text="Configuração do Valor COD_CTA", padx=10, pady=10)
    cod_cta_valor_frame.pack(fill="x", padx=10, pady=10)
    tk.Label(cod_cta_valor_frame, text="Valor para o Campo COD_CTA:").grid(row=0, column=0, sticky="w")
    entry_cod_cta = tk.Entry(cod_cta_valor_frame, width=10)
    entry_cod_cta.grid(row=0, column=1, padx=5)
    
    tk.Button(cod_cta_window, text="Iniciar Ajuste", command=lambda: ajustar_cod_cta_c170_c175(entry_input.get(), entry_output.get(), entry_cod_cta.get())).pack(pady=20)

def ajustar_cod_cta_c170_c175(arquivo_entrada, arquivo_saida, cod_cta_valor):
    try:
        with open(arquivo_entrada, 'r', encoding='latin-1') as entrada, open(arquivo_saida, 'w', encoding='latin-1') as saida:
            for linha in entrada:
                campos = linha.strip().split('|')
                if len(campos) > 1 and campos[1] == 'C170' and len(campos) >= 36 and campos[-2] == '':
                    campos[-2] = cod_cta_valor
                elif len(campos) > 1 and campos[1] == 'C175' and len(campos) >= 20 and campos[-3] == '':
                    campos[-3] = cod_cta_valor
                linha_ajustada = '|'.join(campos)
                saida.write(linha_ajustada + '\n')
        
        messagebox.showinfo("Processamento Concluído", f"O arquivo ajustado foi salvo como '{arquivo_saida}'")
    except Exception as e:
        messagebox.showerror("Erro", f"Erro ao processar o arquivo: {e}")

# Gerador Registro A100

def open_sped_a100():
    sped_a100_window = tk.Toplevel(root)
    sped_a100_window.title("Gerador SPED A100")
    sped_a100_window.geometry("800x600")
    
    sped_label_frame = tk.LabelFrame(sped_a100_window, text="Configuração de Alíquotas", padx=10, pady=10)
    sped_label_frame.pack(fill="x", padx=10, pady=10)
    
    tk.Label(sped_label_frame, text="Alíquota de PIS (%)").grid(row=0, column=0, sticky="w")
    entry_aliquota_pis = tk.Entry(sped_label_frame, width=10)
    entry_aliquota_pis.insert(0, "0,00")
    entry_aliquota_pis.grid(row=0, column=1, padx=5)
    
    tk.Label(sped_label_frame, text="Alíquota de COFINS (%)").grid(row=1, column=0, sticky="w")
    entry_aliquota_cofins = tk.Entry(sped_label_frame, width=10)
    entry_aliquota_cofins.insert(0, "0,00")
    entry_aliquota_cofins.grid(row=1, column=1, padx=5)
    
    output_sped = scrolledtext.ScrolledText(sped_a100_window, width=90, height=15)
    output_sped.pack(pady=10, padx=10, fill="both", expand=True)
    
    tk.Button(sped_a100_window, text="Selecionar Diretório de XML e Processar", 
              command=lambda: select_directory_sped(entry_aliquota_pis.get(), entry_aliquota_cofins.get(), output_sped, progress_bar)).pack(pady=10)

def parse_xml_gerador_sped(file_path, aliquota_pis, aliquota_cofins):
    tree = ET.parse(file_path)
    root = tree.getroot()
    registros = []

    for item in root.findall('item'):
        numero_doc = item.find('numero').text
        data_emissao_raw = item.find('data_emissao').text
        try:
            data_emissao = datetime.strptime(data_emissao_raw, "%Y-%m-%d %H:%M:%S.%f").strftime("%d%m%Y")
        except ValueError:
            data_emissao = datetime.strptime(data_emissao_raw, "%Y-%m-%d %H:%M:%S").strftime("%d%m%Y")
        
        valor_servico_raw = item.find('servico/valores/valor_servico').text
        valor_servico = valor_servico_raw.replace("R$", "").replace(".", "").replace(",", ".").strip()
        
        valor_pis = round(float(valor_servico) * (aliquota_pis / 100), 2)
        valor_cofins = round(float(valor_servico) * (aliquota_cofins / 100), 2)
        
        valor_servico = format_brazilian(float(valor_servico))
        valor_pis = format_brazilian(valor_pis)
        valor_cofins = format_brazilian(valor_cofins)
        
        registro_a100 = (
            f"|A100|1|0||00|||{numero_doc}||{data_emissao}||{valor_servico}|0||{valor_servico}|"
            f"{valor_pis}|{valor_servico}|{valor_cofins}||||"
        )
        registros.append(registro_a100)
        
        registro_a170 = (
            f"|A170|1|SER0407||{valor_servico}||||01|{valor_servico}|{format_brazilian(aliquota_pis)}|{valor_pis}|01|"
            f"{valor_servico}|{format_brazilian(aliquota_cofins)}|{valor_cofins}|||"
        )
        registros.append(registro_a170)

    return registros

# Organizador de Arquivos SPED'S

def open_organizador_sped():
    organizador_window = tk.Toplevel(root)
    organizador_window.title("Organizador de Arquivos SPED")
    organizador_window.geometry("800x600")
    
    tk.Label(organizador_window, text="Organizar Arquivos SPED").pack(pady=10)
    
    tk.Button(organizador_window, text="Organizar SPED Fiscal", command=organizar_sped_fiscal).pack(pady=5)
    tk.Button(organizador_window, text="Organizar SPED Contribuições", command=organizar_sped_contribuicoes).pack(pady=5)

def process_directory_gerador_sped(directory_path, aliquota_pis, aliquota_cofins, output_widget, progress_bar):
    output_widget.delete(1.0, tk.END)
    for filename in os.listdir(directory_path):
        if filename.endswith(".xml"):
            file_path = os.path.join(directory_path, filename)
            registros = parse_xml_gerador_sped(file_path, aliquota_pis, aliquota_cofins)
            output_widget.insert(tk.END, f"Processado: {filename}\n")
            output_widget.insert(tk.END, "\n".join(registros) + "\n\n")
    messagebox.showinfo("Processo concluído", "Os registros foram gerados e exibidos na tela.")

def select_directory_sped(aliquota_pis, aliquota_cofins, output_widget, progress_bar):
    directory_path = filedialog.askdirectory()
    if directory_path:
        try:
            aliquota_pis = float(aliquota_pis.replace(",", "."))
            aliquota_cofins = float(aliquota_cofins.replace(",", "."))
            process_directory_gerador_sped(directory_path, aliquota_pis, aliquota_cofins, output_widget, progress_bar)
        except ValueError:
            messagebox.showerror("Erro", "Por favor, insira valores numéricos válidos para as alíquotas.")    

def remover_mascara(cnpj):
    return re.sub(r'\D', '', cnpj)

# Função para organizar arquivos SPED Fiscal, com opção de atualização da versão para layout 019
def organizar_sped_fiscal():
    organizar_arquivos_sped(tipo="fiscal")

# Função para organizar arquivos SPED Contribuições (sem atualização de versão)
def organizar_sped_contribuicoes():
    organizar_arquivos_sped(tipo="contribuicoes")

# Função para remover máscara do CNPJ
def remover_mascara(cnpj):
    return ''.join(filter(str.isdigit, str(cnpj)))

# Função para obter o diretório base do script ou do executável
def get_base_path():
    if getattr(sys, 'frozen', False):  # Se estiver rodando como .exe
        return os.path.dirname(sys.executable)  # Diretório do executável
    return os.path.dirname(os.path.abspath(__file__))  # Diretório do script Python

# Função principal que organiza os arquivos com base no tipo de SPED
def organizar_arquivos_sped(tipo, nova_versao=None):
    # Obter o diretório correto onde o script/executável está rodando
    current_dir = get_base_path()

    # Nome fixo do arquivo CSV na mesma pasta do script/executável
    csv_filename = "clientes_hos.csv"
    csv_path = os.path.join(current_dir, csv_filename)

    # Verificar se o arquivo CSV existe no diretório correto
    if not os.path.isfile(csv_path):
        messagebox.showerror("Erro", f"O arquivo '{csv_filename}' não foi encontrado no diretório: {current_dir}")
        return
    
    # Tentar carregar o arquivo CSV
    try:
        df = pd.read_csv(csv_path, encoding="utf-8", delimiter=";")
    except Exception as e:
        messagebox.showerror("Erro", f"Erro ao carregar o arquivo '{csv_filename}': {e}")
        return
    
    # Verificar se a coluna "CNPJ" existe no CSV
    if 'CNPJ' not in df.columns:
        messagebox.showerror("Erro", "Coluna 'CNPJ' não encontrada no arquivo CSV. Verifique o cabeçalho do CSV.")
        return

    # Remover máscara do CNPJ no CSV
    df['CNPJ'] = df['CNPJ'].apply(remover_mascara)

    # Selecionar o diretório onde as pastas serão salvas
    output_dir = easygui.diropenbox("Selecione o diretório de destino dos Arquivos SPED")
    if not output_dir:
        messagebox.showerror("Erro", "Nenhum diretório selecionado para salvar as pastas.")
        return

    # Normalizar o caminho para o padrão correto de barras no Windows/Linux
    output_dir = os.path.normpath(output_dir)

    # Selecionar múltiplos arquivos SPED (TXT)
    txt_files = easygui.fileopenbox("Selecione os arquivos SPED (TXT)", filetypes=["*.txt"], multiple=True)

    # Verificar se pelo menos um arquivo foi selecionado
    if not txt_files:
        messagebox.showerror("Erro", "Nenhum arquivo SPED selecionado.")
        return

    # Exibir os arquivos selecionados (opcional)
    easygui.msgbox(f"Arquivos SPED selecionados:\n" + "\n".join(txt_files), "Arquivos Escolhidos")

    for txt_file in txt_files:
        # Abrir e ler a primeira linha do arquivo para extrair o CNPJ, depois fechar o arquivo
        with open(txt_file, 'r', encoding='latin-1') as f:
            conteudo = f.readlines()

        # Extrair o CNPJ e versão com base no tipo de SPED
        match = None
        if tipo == "fiscal":
            match = re.search(r'\|0000\|(\d{3})\|\d\|\d+\|\d+\|[^|]+\|(\d{14})\|', conteudo[0])
        elif tipo == "contribuicoes":
            match = re.search(r'\|0000\|\d+\|\d\|\|\|\d+\|\d+\|[^|]+\|(\d{14})\|', conteudo[0])

        if match:
            versao_atual = match.group(1) if tipo == "fiscal" else None
            cnpj_sped = match.group(2) if tipo == "fiscal" else match.group(1)

            # Se a versão atual for diferente de "019", solicitar nova versão
            if tipo == "fiscal" and versao_atual != "019":
                nova_versao = simpledialog.askstring("Nova Versão", f"A versão atual é {versao_atual}. Digite a nova versão do arquivo SPED Fiscal (ex: 019):")

                if nova_versao and nova_versao.isdigit() and len(nova_versao) == 3:
                    conteudo[0] = re.sub(r'(\|0000\|)\d{3}(\|)', rf'|0000|{nova_versao}|', conteudo[0])
                else:
                    messagebox.showerror("Erro", "Versão inválida. Digite um número de 3 dígitos, por exemplo, 019.")
                    continue

            # Verificar se o CNPJ existe no CSV
            linha_cliente = df[df['CNPJ'] == cnpj_sped]
            if not linha_cliente.empty:
                codigo = linha_cliente.iloc[0]['CODIGO']
                fantasia = linha_cliente.iloc[0]['FANTASIA']
                nome_pasta = f"{codigo} - {fantasia}".strip()
                pasta_destino = os.path.join(output_dir, nome_pasta)

                # Criar a pasta no diretório selecionado
                try:
                    os.makedirs(pasta_destino, exist_ok=True)
                    messagebox.showinfo("Sucesso", f"Pasta '{nome_pasta}' criada com sucesso.")
                except Exception as e:
                    messagebox.showerror("Erro", f"Erro ao criar pasta '{nome_pasta}': {e}")
                    continue

                # Salvar o conteúdo do arquivo com a versão atualizada antes de mover
                if tipo == "fiscal" and nova_versao:
                    with open(txt_file, 'w', encoding='latin-1') as f:
                        f.writelines(conteudo)

                # Mover o arquivo TXT para a pasta criada
                try:
                    destino_arquivo = os.path.join(pasta_destino, os.path.basename(txt_file))
                    shutil.move(txt_file, destino_arquivo)
                    messagebox.showinfo("Sucesso", f"Arquivo '{os.path.basename(txt_file)}' movido para a pasta '{nome_pasta}'.")
                except Exception as e:
                    messagebox.showerror("Erro", f"Erro ao mover o arquivo '{txt_file}' para a pasta '{nome_pasta}': {e}")
            else:
                messagebox.showerror("Erro", f"CNPJ do SPED '{cnpj_sped}' não encontrado no CSV.")
        else:
            messagebox.showerror("Erro", f"Registro 0000 não encontrado ou formato inválido no arquivo {txt_file}.")

# Ajuste CNPJ do PIX - SPED

def open_adjust_cnpj():
    adjust_window = tk.Toplevel(root)
    adjust_window.title("Ajuste CNPJ do PIX - SPED")
    adjust_window.geometry("800x600")
    
    adjust_label_frame = tk.LabelFrame(adjust_window, text="Seleção de Arquivo", padx=10, pady=10)
    adjust_label_frame.pack(fill="x", padx=10, pady=10)
    
    tk.Label(adjust_label_frame, text="Arquivo de Entrada (SPED):").grid(row=0, column=0, sticky="w")
    entry_adjust_input = tk.Entry(adjust_label_frame, width=50)
    entry_adjust_input.grid(row=0, column=1, padx=5)
    tk.Button(adjust_label_frame, text="Selecionar", command=lambda: select_adjust_cnpj_file(entry_adjust_input)).grid(row=0, column=2)
    
    tk.Button(adjust_window, text="Iniciar Ajuste", command=lambda: ajustar_cnpj_cod_mun_registro_0150(entry_adjust_input.get().strip())).pack(pady=20)

def select_adjust_cnpj_file(entry_input):
    input_path = filedialog.askopenfilename(title="Selecione o arquivo de entrada", filetypes=[("Arquivos TXT", "*.txt")])
    if input_path:
        entry_input.delete(0, tk.END)
        entry_input.insert(0, input_path)

def ajustar_cnpj_cod_mun_registro_0150(caminho_arquivo):
    dados_0000 = obter_dados_registro_0000(caminho_arquivo)
    if not dados_0000:
        messagebox.showerror("Erro", "Registro 0000 não encontrado no arquivo.")
        return
    
    linhas_ajustadas = []
    with open(caminho_arquivo, mode='r', encoding='latin-1') as arquivo:
        leitor_csv = csv.reader(arquivo, delimiter='|')
        for linha in leitor_csv:
            if linha[1] == "0150" and linha[3] == "PIX":
                linha[4] = '1058'
                linha[5] = dados_0000["CNPJ"]
                linha[8] = dados_0000["COD_MUN"]
                linha[10] = '.'
            linhas_ajustadas.append(linha)
    
    diretorio_saida = os.path.dirname(caminho_arquivo)
    caminho_arquivo_ajustado = os.path.join(diretorio_saida, "SpedFiscal_Arquivo_Ajustado.txt")
    with open(caminho_arquivo_ajustado, mode='w', encoding='latin-1', newline='') as arquivo_saida:
        escritor_csv = csv.writer(arquivo_saida, delimiter='|', quoting=csv.QUOTE_NONE, escapechar='\\')
        for linha in linhas_ajustadas:
            escritor_csv.writerow(linha)
    messagebox.showinfo("Processamento Concluído", f"Arquivo ajustado com sucesso. Salvo como '{caminho_arquivo_ajustado}'.")

def obter_dados_registro_0000(caminho_arquivo):
    dados_0000 = {}
    with open(caminho_arquivo, mode='r', encoding='latin-1') as arquivo:
        leitor_csv = csv.reader(arquivo, delimiter='|')
        for linha in leitor_csv:
            if linha[1] == "0000":
                dados_0000["CNPJ"] = linha[7].strip()
                dados_0000["COD_MUN"] = linha[11].strip()
                break
    return dados_0000

# Ajustar Unidade Registro C170

def open_adjust_c170():
    """ Abre a janela para ajuste do Registro C170 """
    adjust_c170_window = tk.Toplevel(root)
    adjust_c170_window.title("Ajustar Unidade Registro C170")
    adjust_c170_window.geometry("800x600")
    
    adjust_c170_window.lista_arquivos = []  # Inicializa a lista dentro da janela
    
    frame_entrada = tk.Frame(adjust_c170_window, padx=10, pady=10, relief=tk.RIDGE, borderwidth=2)
    frame_entrada.pack(fill=tk.BOTH, padx=10, pady=5, expand=True)
    
    frame_botoes = tk.Frame(adjust_c170_window, padx=10, pady=10)
    frame_botoes.pack(fill=tk.X, padx=10, pady=5)
    
    tk.Label(frame_entrada, text="Arquivos de Entrada:", font=("Arial", 10, "bold")).pack(anchor=tk.W, padx=5, pady=5)

    frame_texto_scroll = tk.Frame(frame_entrada)
    frame_texto_scroll.pack(fill=tk.BOTH, padx=5, pady=5, expand=True)
    
    entrada_text = tk.Text(frame_texto_scroll, height=15, wrap=tk.WORD)
    entrada_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
    
    scrollbar = tk.Scrollbar(frame_texto_scroll, orient=tk.VERTICAL, command=entrada_text.yview)
    scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
    entrada_text.config(yscrollcommand=scrollbar.set)
    
    tk.Button(frame_entrada, text="Selecionar Arquivos", 
              command=lambda: selecionar_arquivos_entrada(adjust_c170_window, entrada_text), 
              width=20).pack(pady=5)
    
    tk.Button(frame_botoes, text="Processar", 
              command=lambda: processar_arquivos_multiplos(adjust_c170_window, entrada_text), 
              width=20, bg="#4CAF50", fg="white", font=("Arial", 12, "bold")).pack(pady=10)
    
def carregar_registros_0200(arquivo_entrada):
    """ Carrega os registros 0200 e retorna um dicionário {cod_produto: unidade} """
    registros_0200 = {}
    
    try:
        with open(arquivo_entrada, 'r', encoding='latin-1') as arquivo:
            for linha in arquivo:
                if linha.startswith('|0200|'):
                    campos = linha.strip().split('|')
                    if len(campos) > 6:
                        cod_produto = campos[2]  # Posição do código do produto
                        unidade = campos[6]  # Posição da unidade do produto
                        registros_0200[cod_produto] = unidade
    except Exception as e:
        messagebox.showerror("Erro", f"Erro ao carregar registros 0200: {e}")

    return registros_0200

def selecionar_arquivos_entrada(window, entrada_text):
    """ Seleciona arquivos e atualiza a lista da janela """
    arquivos = filedialog.askopenfilenames(filetypes=[("Arquivos de Texto", "*.txt")])
    if arquivos:
        window.lista_arquivos = list(arquivos)  # Atualiza a lista da janela
        atualizar_texto_entrada(window.lista_arquivos, entrada_text)

def atualizar_texto_entrada(lista_arquivos, entrada_text):
    """ Atualiza o widget de texto para exibir a lista de arquivos selecionados """
    entrada_text.delete("1.0", tk.END)
    for arquivo in lista_arquivos:
        entrada_text.insert(tk.END, arquivo + "\n")

def processar_arquivos_multiplos(window, entrada_text):
    """ Processa os arquivos selecionados e limpa a interface após concluir """
    try:
        for arquivo_entrada in window.lista_arquivos:
            registros_0200 = carregar_registros_0200(arquivo_entrada)
            pasta, nome_arquivo = os.path.split(arquivo_entrada)
            nome_arquivo_saida = os.path.splitext(nome_arquivo)[0] + '-AjustadoUN.txt'
            arquivo_saida = os.path.join(pasta, nome_arquivo_saida)

            with open(arquivo_entrada, 'r', encoding='latin-1') as entrada, open(arquivo_saida, 'w', encoding='latin-1') as saida:
                for linha in entrada:
                    if not linha.startswith('|C170|'):
                        saida.write(linha)
                    else:
                        campos = linha.strip().split('|')
                        if len(campos) > 6:
                            cod_produto = campos[3]
                            unidade_atual = campos[6]
                            unidade_correta = registros_0200.get(cod_produto)
                            if unidade_correta and unidade_correta != unidade_atual:
                                campos[6] = unidade_correta
                                linha_atualizada = '|'.join(campos) + '\n'
                                saida.write(linha_atualizada)
                            else:
                                saida.write(linha)

        # Limpar a lista dentro da janela e atualizar a interface
        window.lista_arquivos.clear()
        atualizar_texto_entrada(window.lista_arquivos, entrada_text)

        messagebox.showinfo("Concluído", "Todos os arquivos foram processados com sucesso.")
    except Exception as e:
        messagebox.showerror("Erro", f"Ocorreu um erro: {e}")

# Verificação e Ajuste CST Pis Cofins Registro C170

def selecionar_arquivo():
    global arquivo_selecionado
    arquivo_selecionado = filedialog.askopenfilename(title="Selecione o arquivo SPED", filetypes=[("Arquivos TXT", "*.txt")])
    if not arquivo_selecionado:
        messagebox.showwarning("Aviso", "Nenhum arquivo foi selecionado!")

def analisar_arquivo():
    global arquivo_selecionado
    if not arquivo_selecionado:
        messagebox.showwarning("Aviso", "Nenhum arquivo foi selecionado para análise!")
        return
    farmaceuticos, perfumaria, excecoes = verificar_e_ajustar_c170(arquivo_selecionado, ajustar=False)
    atualizar_resultado(farmaceuticos, perfumaria, excecoes)

def processar_arquivo():
    global arquivo_selecionado
    if not arquivo_selecionado:
        messagebox.showwarning("Aviso", "Nenhum arquivo foi selecionado para processamento!")
        return
    verificar_e_ajustar_c170(arquivo_selecionado, ajustar=True)
    messagebox.showinfo("Sucesso", "O arquivo foi ajustado e salvo com sucesso!")

def verificar_e_ajustar_c170(arquivo_entrada, ajustar=False):
    registros_ajustados = []
    ncm_dict = {}
    farmaceuticos, perfumaria, excecoes = [], [], []
    ncm_farmaceuticos_prefixos = {"3001", "3003", "3004", "3002101", "3002102", "3002103", "3002201", "3002202", "3006301", "3006302", "30029020", "30029092", "30029099", "30051010", "30066000"}
    ncm_excecoes_farmaceuticos = {"30039056", "30049046"}
    ncm_perfumaria_prefixos = {"3303", "3307", "34011190", "34012010", "96032100"}
    try:
        with open(arquivo_entrada, 'r', encoding='latin-1') as f:
            linhas = f.readlines()
        for linha in linhas:
            campos = linha.strip().split('|')
            if len(campos) > 1:
                if campos[1] == '0200':
                    cod_item = campos[2].strip()
                    ncm = campos[8].strip() if len(campos) > 8 else 'NCM Não Encontrado'
                    ncm_dict[cod_item] = ncm
                elif campos[1] == 'C170':
                    if len(campos) > 31:
                        cod_item = campos[3].strip()
                        cst_pis = campos[25].strip()
                        cst_cofins = campos[31].strip()
                        if cst_pis == '50' and cst_cofins == '50':
                            ncm = ncm_dict.get(cod_item, 'NCM Não Encontrado')
                            if any(ncm.startswith(prefix) for prefix in ncm_farmaceuticos_prefixos) and ncm not in ncm_excecoes_farmaceuticos:
                                farmaceuticos.append(f"COD_ITEM: {cod_item}, NCM: {ncm}")
                            elif any(ncm.startswith(prefix) for prefix in ncm_perfumaria_prefixos):
                                perfumaria.append(f"COD_ITEM: {cod_item}, NCM: {ncm}")
                            else:
                                excecoes.append(f"COD_ITEM: {cod_item}, NCM: {ncm}")
                            if ajustar:
                                campos[25] = '70'
                                campos[31] = '70'
                                campos[26] = '0'
                                campos[27] = '0'
                                campos[30] = '0'
                                campos[32] = '0'
                                campos[33] = '0'
                                campos[36] = '0'
                    linha = '|'.join(campos) + '\n'
                registros_ajustados.append(linha)
        if ajustar:
            pasta, nome_arquivo = os.path.split(arquivo_entrada)
            nome_base, extensao = os.path.splitext(nome_arquivo)
            novo_nome_arquivo = os.path.join(pasta, f"{nome_base}_AjustadoCSTPISCOFINS{extensao}")
            with open(novo_nome_arquivo, 'w', encoding='latin-1') as f:
                f.writelines(registros_ajustados)
    except Exception as e:
        messagebox.showerror("Erro", f"Erro ao processar o arquivo {arquivo_entrada}:\n{e}")
    return farmaceuticos, perfumaria, excecoes

def atualizar_resultado(farmaceuticos, perfumaria, excecoes):
    text_area.config(state=tk.NORMAL)
    text_area.delete('1.0', tk.END)
    resultado = "Produtos Farmacêuticos:\n\n" + "\n".join(farmaceuticos) + "\n\n" if farmaceuticos else "Nenhum item farmacêutico encontrado.\n\n"
    resultado += "Produtos de Perfumaria:\n\n" + "\n".join(perfumaria) + "\n\n" if perfumaria else "Nenhum item de perfumaria encontrado.\n\n"
    resultado += "Itens para analisar:\n\n" + "\n".join(excecoes) if excecoes else "Nenhum item encontrado para analisar."
    text_area.insert(tk.INSERT, resultado)
    text_area.config(state=tk.DISABLED)

def open_verificar_ncm():
    global text_area
    verificar_window = tk.Toplevel(root)
    verificar_window.title("Verificação e Ajuste CST Pis Cofins Registro C170")
    verificar_window.geometry("700x600")
    verificar_window.configure(bg="#f0f0f0")
    frame = tk.Frame(verificar_window, padx=10, pady=10, bg="#f0f0f0")
    frame.pack(fill=tk.BOTH, expand=True)
    tk.Button(frame, text="Escolher Arquivo", command=selecionar_arquivo, bg="#1976d2", fg="white", font=("Arial", 12, "bold"), width=25).pack(pady=5)
    text_area = scrolledtext.ScrolledText(frame, wrap=tk.WORD, width=80, height=25, font=("Arial", 10))
    text_area.pack(padx=10, pady=10, fill=tk.BOTH, expand=True)
    text_area.config(state=tk.DISABLED)
    tk.Button(frame, text="Analisar", command=analisar_arquivo, bg="#ff9800", fg="white", font=("Arial", 12, "bold"), width=25).pack(pady=5)
    tk.Button(frame, text="Processar", command=processar_arquivo, bg="#4CAF50", fg="white", font=("Arial", 12, "bold"), width=25).pack(pady=5)
    tk.Button(frame, text="Fechar", command=verificar_window.destroy, bg="#d32f2f", fg="white", font=("Arial", 12, "bold"), width=25).pack(pady=5)

# Verificar Números Faltantes (NF-e/NFC-e)

def open_verificar_numeros_faltantes():
    """ Abre a janela para verificar números faltantes no Firebird """
    verificar_window = tk.Toplevel(root)
    verificar_window.title("Verificar Números Faltantes")
    verificar_window.geometry("550x500")
    verificar_window.configure(bg="#f0f0f0")
    
    frame = tk.Frame(verificar_window, padx=10, pady=10, bg="#f0f0f0")
    frame.pack(fill=tk.BOTH, expand=True)
    
    tk.Label(frame, text="Período Início (Ex.01.01.2025):", font=("Arial", 10, "bold"), bg="#f0f0f0").grid(row=0, column=0, sticky="w", pady=2)
    entry_inicio = tk.Entry(frame, width=30)
    entry_inicio.grid(row=0, column=1, padx=5, pady=2)
    
    tk.Label(frame, text="Período Fim (Ex.31.01.2025):", font=("Arial", 10, "bold"), bg="#f0f0f0").grid(row=1, column=0, sticky="w", pady=2)
    entry_fim = tk.Entry(frame, width=30)
    entry_fim.grid(row=1, column=1, padx=5, pady=2)
    
    tk.Label(frame, text="Série:", font=("Arial", 10, "bold"), bg="#f0f0f0").grid(row=2, column=0, sticky="w", pady=2)
    entry_serie = tk.Entry(frame, width=30)
    entry_serie.grid(row=2, column=1, padx=5, pady=2)
    
    tk.Label(frame, text="Modelo:", font=("Arial", 10, "bold"), bg="#f0f0f0").grid(row=3, column=0, sticky="w", pady=2)
    modelo_var = tk.StringVar()
    modelo_dropdown = ttk.Combobox(frame, textvariable=modelo_var, values=["55", "65"], width=28)
    modelo_dropdown.grid(row=3, column=1, padx=5, pady=2)
    modelo_dropdown.current(0)
    
    tk.Label(frame, text="Empresa:", font=("Arial", 10, "bold"), bg="#f0f0f0").grid(row=4, column=0, sticky="w", pady=2)
    entry_empresa = tk.Entry(frame, width=30)
    entry_empresa.grid(row=4, column=1, padx=5, pady=2)
    
    tk.Label(frame, text="Banco de Dados Firebird:", font=("Arial", 10, "bold"), bg="#f0f0f0").grid(row=5, column=0, sticky="w", pady=2)
    entry_banco = tk.Entry(frame, width=35)
    entry_banco.insert(0, "C:/Mercfarma/CADASTRO.FDB")
    entry_banco.grid(row=5, column=1, padx=5, pady=2, sticky="ew")
    
    botao_banco = tk.Button(frame, text="Selecionar", command=lambda: selecionar_banco(entry_banco), 
                             bg="#1976d2", fg="white", font=("Arial", 10, "bold"), width=12)
    botao_banco.grid(row=5, column=2, padx=5, pady=2)
    
    progress_bar = ttk.Progressbar(frame, orient=tk.HORIZONTAL, length=350, mode='determinate')
    progress_bar.grid(row=6, column=0, columnspan=3, pady=10, sticky="ew")
    
    botao_verificar = tk.Button(frame, text="Verificar", command=lambda: verificar_numeros_faltantes(
        entry_inicio, entry_fim, entry_serie, modelo_var, entry_empresa, entry_banco, progress_bar),
        bg="#1976d2", fg="white", font=("Arial", 12, "bold"), width=25)
    botao_verificar.grid(row=7, column=0, columnspan=3, pady=10)

def selecionar_banco(entry_banco):
    caminho = filedialog.askopenfilename(filetypes=[("Arquivos Firebird", "*.FDB")])
    if caminho:
        entry_banco.delete(0, tk.END)
        entry_banco.insert(0, caminho)

def verificar_numeros_faltantes(entry_inicio, entry_fim, entry_serie, modelo_var, entry_empresa, entry_banco, progress_bar):
    def processar():
        try:
            periodo_inicio = entry_inicio.get()
            periodo_fim = entry_fim.get()
            serie = str(entry_serie.get())  # Conversão para string para evitar erro no SQL
            modelo = modelo_var.get()
            empresa = str(entry_empresa.get())  # Conversão para string para evitar erro no SQL
            caminho_banco = entry_banco.get()

            if not caminho_banco:
                messagebox.showerror("Erro", "Por favor, selecione o caminho do banco de dados.")
                return

            con = fdb.connect(dsn=caminho_banco, user='SYSDBA', password='masterkey')
            cur = con.cursor()
            con.begin()

            progress_bar['value'] = 10
            root.update_idletasks()

            # Definição das tabelas e colunas baseadas no modelo selecionado
            if modelo == "55":
                tabela = "CAB_NF"
                campo_numero = "NR_NOTA"
                campo_data = "DATA_EMISSAO"
                campo_serie = "SERIE"
            else:
                tabela = "CAIXA"
                campo_numero = "CUPOM"
                campo_data = "DATA"
                campo_serie = "SERIE_NFCE"

            # Busca a numeração mínima e máxima
            cur.execute(f"""
                SELECT MIN({campo_numero}), MAX({campo_numero})
                FROM {tabela}
                WHERE {campo_data} BETWEEN '{periodo_inicio}' AND '{periodo_fim}'
                  AND {campo_serie} = '{serie}'  -- Adicionando aspas para evitar erro com números
                  AND EMPRESA = '{empresa}'
            """)

            min_max = cur.fetchone()
            if not min_max or min_max[0] is None:
                messagebox.showinfo("Resultado", "Nenhuma nota encontrada no período informado.")
                return

            min_nota, max_nota = map(int, min_max)  # Convertendo os valores para inteiros
            progress_bar['value'] = 30
            root.update_idletasks()

            # Buscar números emitidos
            cur.execute(f"""
                SELECT {campo_numero} FROM {tabela}
                WHERE {campo_data} BETWEEN '{periodo_inicio}' AND '{periodo_fim}'
                  AND {campo_serie} = '{serie}'
                  AND EMPRESA = '{empresa}'
            """)

            notas_emitidas = {int(row[0]) for row in cur.fetchall()}

            progress_bar['value'] = 50
            root.update_idletasks()

            # Buscar números inutilizados
            cur.execute(f"""
                SELECT NUMERACAO_INICIAL, NUMERACAO_FINAL FROM NFE_INUTILIZACAO_NUMERACAO
                WHERE EMPRESA = '{empresa}' AND MODELO = '{modelo}'
            """)

            inutilizados = set()
            for inicio, fim in cur.fetchall():
                inutilizados.update(range(int(inicio), int(fim) + 1))

            progress_bar['value'] = 70
            root.update_idletasks()

            # Buscar números emitidos em outra data
            cur.execute(f"""
                SELECT {campo_numero}, {campo_data} FROM {tabela}
                WHERE {campo_numero} BETWEEN {min_nota} AND {max_nota}
                  AND {campo_serie} = '{serie}'
                  AND EMPRESA = '{empresa}'
                  AND {campo_data} NOT BETWEEN '{periodo_inicio}' AND '{periodo_fim}'
            """)

            notas_outra_data = {int(row[0]): row[1] for row in cur.fetchall()}

            # Identificar números faltantes
            numeros_faltantes = []
            detalhes_faltantes = []

            for num in range(min_nota, max_nota + 1):
                if num not in notas_emitidas and num not in inutilizados:
                    if num in notas_outra_data:
                        detalhes_faltantes.append(f"Número {num} emitido em outra data: {notas_outra_data[num]}")
                    else:
                        numeros_faltantes.append(num)

            con.commit()
            con.close()

            progress_bar['value'] = 100
            root.update_idletasks()

            # Exibir resultados na interface
            mostrar_tabela(numeros_faltantes, detalhes_faltantes)
        except Exception as e:
            messagebox.showerror("Erro", f"Erro ao consultar o banco de dados: {e}")
        finally:
            progress_bar['value'] = 0

    threading.Thread(target=processar).start()

def mostrar_tabela(numeros_faltantes, detalhes_faltantes):
    janela_tabela = tk.Toplevel(root)
    janela_tabela.title("Números Faltantes")
    janela_tabela.geometry("500x400")

    frame = tk.Frame(janela_tabela)
    frame.pack(fill=tk.BOTH, expand=True)

    scrollbar = tk.Scrollbar(frame)
    scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

    text_area = tk.Text(frame, wrap=tk.WORD, width=50, height=20, yscrollcommand=scrollbar.set)
    text_area.pack(padx=10, pady=10, fill=tk.BOTH, expand=True)
    scrollbar.config(command=text_area.yview)

    if numeros_faltantes or detalhes_faltantes:
        text_area.insert(tk.END, f"Números Faltantes ({len(numeros_faltantes)} registros):\n" + "\n".join(map(str, numeros_faltantes)) + "\n\n")
        text_area.insert(tk.END, "Detalhes de números emitidos em outra data:\n" + "\n".join(detalhes_faltantes))
    else:
        text_area.insert(tk.END, "Nenhum número faltante encontrado.")

# Ajustar Nr SAT Registro C116

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

def open_ajuste_c116():
    """
    Abre a janela para ajuste do campo C116.
    """
    ajuste_window = tk.Toplevel(root)
    ajuste_window.title("Ajustar Registro C116")
    ajuste_window.geometry("400x250")
    ajuste_window.configure(bg="#f0f0f0")

    frame = tk.Frame(ajuste_window, padx=10, pady=10, bg="#f0f0f0")
    frame.pack(fill=tk.BOTH, expand=True)

    tk.Label(frame, text="Ajuste de Campo C116", font=("Arial", 14, "bold"), bg="#f0f0f0").pack(pady=10)
    tk.Button(frame, text="Selecionar e Processar Arquivo", command=processar_arquivo, font=("Arial", 12), bg="#1976d2", fg="white").pack(pady=10)
    tk.Button(frame, text="Fechar", command=ajuste_window.destroy, font=("Arial", 12), bg="#d32f2f", fg="white").pack(pady=10)

# Ajustar Registros C800

def open_c800_processor():
    """ Abre a janela para processar registros C800 no SPED Fiscal """
    c800_window = tk.Toplevel(root)
    c800_window.title("Processador de Registros C800 - SPED Fiscal")
    c800_window.geometry("600x200")
    c800_window.configure(bg="#f0f0f0")

    frame = tk.Frame(c800_window, padx=10, pady=10, bg="#f0f0f0")
    frame.pack(fill=tk.BOTH, expand=True)

    tk.Label(frame, text="Arquivo de Entrada:", font=("Arial", 10, "bold"), bg="#f0f0f0").grid(row=0, column=0, sticky="w", pady=5)
    input_file_var = tk.StringVar()
    entry_input = tk.Entry(frame, textvariable=input_file_var, width=50)
    entry_input.grid(row=0, column=1, padx=5, pady=5)
    tk.Button(frame, text="Selecionar", command=lambda: select_input_file(input_file_var), bg="#1976d2", fg="white", font=("Arial", 10, "bold")).grid(row=0, column=2, padx=5, pady=5)

    tk.Button(frame, text="Processar", command=lambda: start_processing(input_file_var), bg="#2e7d32", fg="white", font=("Arial", 12, "bold"), width=20).grid(row=1, column=0, columnspan=3, pady=15)


def process_file(input_file):
    """ Processa o arquivo e remove o CNPJ_CPF do campo correspondente no registro C800 """
    try:
        # Obtém o diretório e o nome base do arquivo
        dir_name, base_name = os.path.split(input_file)
        name, ext = os.path.splitext(base_name)

        # Define o nome do arquivo de saída com o sufixo "-AjustadoRg800"
        output_file = os.path.join(dir_name, f"{name}-AjustadoRg800{ext}")

        with open(input_file, 'r', encoding='latin-1') as infile, open(output_file, 'w', encoding='latin-1') as outfile:
            for line in infile:
                if line.startswith('|C800|'):
                    fields = line.strip().split('|')
                    if len(fields) > 9:
                        fields[9] = ''  # Limpa o campo CNPJ_CPF
                    new_line = '|'.join(fields) + '\n'
                    outfile.write(new_line)
                else:
                    outfile.write(line)
        
        # Exibe mensagem de sucesso e limpa as variáveis
        messagebox.showinfo("Sucesso", f"Arquivo processado e salvo em:\n{output_file}")

    except Exception as e:
        messagebox.showerror("Erro", f"Ocorreu um erro: {e}")


def select_input_file(input_var):
    """ Seleciona o arquivo de entrada para processamento """
    input_file_path = filedialog.askopenfilename(
        title="Selecione o arquivo SPED de entrada",
        filetypes=[("Arquivos TXT", "*.txt")],
    )
    if input_file_path:
        input_var.set(input_file_path)


def start_processing(input_var):
    """ Inicia o processamento do arquivo """
    input_file = input_var.get()

    if not os.path.isfile(input_file):
        messagebox.showwarning("Aviso", "Selecione um arquivo de entrada válido!")
        return
    
    process_file(input_file)

    # Limpa o caminho do arquivo de entrada após o processamento
    input_var.set("")

# Tela de Carregamento    

# Função para simular o carregamento e mostrar a tela principal
def main_screen():
    loading_screen.destroy()

# Função para simular o carregamento e atualizar a barra de progresso
def loading():
    for i in range(101):
        progress_bar['value'] = i
        loading_screen.update_idletasks()
        time.sleep(0.03)
    main_screen()

# Configuração da tela de carregamento
loading_screen = tk.Tk()
loading_screen.title("Carregando Sistema")
loading_screen.geometry("400x200")
loading_screen.overrideredirect(True)  # Remove a barra de título

# Centralizar a janela na tela
screen_width = loading_screen.winfo_screenwidth()
screen_height = loading_screen.winfo_screenheight()
x_position = (screen_width // 2) - (400 // 2)
y_position = (screen_height // 2) - (200 // 2)
loading_screen.geometry(f"400x200+{x_position}+{y_position}")

# Define a cor de fundo da janela
loading_screen.configure(bg="#f0f0f0")

# Configuração do estilo para mudar a cor da barra de progresso
style = ttk.Style()
style.theme_use("clam")  # Tema que permite customização
style.configure("Custom.Horizontal.TProgressbar",
                troughcolor="#e0e0e0",  # Cor de fundo do trilho
                background="#1976d2",   # Cor da barra de progresso
                thickness=10)  # Espessura da barra

# Configuração do texto
loading_label = tk.Label(
    loading_screen,
    text="Carregando, por favor aguarde...",
    font=("Arial", 14, "bold"),
    bg="#f0f0f0",
    fg="#333"
)
loading_label.pack(pady=40)

# Barra de progresso personalizada
progress_bar = ttk.Progressbar(
    loading_screen,
    orient="horizontal",
    length=300,
    mode="determinate",
    style="Custom.Horizontal.TProgressbar"  # Aplica o estilo personalizado
)
progress_bar.pack(pady=20)

# Inicia o carregamento
loading_screen.after(100, loading)
loading_screen.mainloop()

# Função para gerar a senha do dia
def gerar_senha():
    data_atual = datetime.datetime.now().strftime("%Y-%m-%d")  # Obtém a data no formato YYYY-MM-DD
    hash_senha = hashlib.sha256(data_atual.encode()).hexdigest()[:6]  # Gera um hash e usa os primeiros 6 caracteres
    return hash_senha

senha_correta = gerar_senha()  # Define a senha do dia dinamicamente
tentativas = 3  # Número máximo de tentativas
sistema_pode_abrir = False

def validar_senha(event=None):  # Adiciona evento opcional para Enter
    global tentativas, sistema_pode_abrir
    senha_digitada = senha_entry.get()

    if senha_digitada == senha_correta:
        sistema_pode_abrir = True
        login_window.destroy()  # Fecha a janela de login
    else:
        tentativas -= 1
        messagebox.showerror("Erro", f"Senha incorreta! Tentativas restantes: {tentativas}")
        senha_entry.delete(0, tk.END)  # Limpa o campo de senha
        
        if tentativas == 0:
            messagebox.showwarning("Acesso Negado", "Você excedeu o número de tentativas!")
            login_window.destroy()  # Fecha a tela de login
            exit()  # Fecha o programa

# Criando a tela de login
login_window = tk.Tk()
login_window.title("Login - AuditHOS")
login_window.geometry("300x200")
login_window.resizable(False, False)

# Centralizar a janela
screen_width = login_window.winfo_screenwidth()
screen_height = login_window.winfo_screenheight()
center_x = int((screen_width - 300) / 2)
center_y = int((screen_height - 200) / 2)
login_window.geometry(f"300x200+{center_x}+{center_y}")

tk.Label(login_window, text="Digite a senha do dia:", font=("Arial", 12)).pack(pady=10)

senha_entry = tk.Entry(login_window, show="*", font=("Arial", 12))
senha_entry.pack(pady=5)
senha_entry.focus()
senha_entry.bind("<Return>", validar_senha)  # Associa a tecla Enter para validar a senha

tk.Button(login_window, text="Entrar", command=validar_senha, font=("Arial", 12, "bold"), bg="#1976d2", fg="white").pack(pady=10)

login_window.mainloop()

if sistema_pode_abrir:
    root = tk.Tk()
    progress_var = tk.DoubleVar()

    root.title("AuditHOS")
    window_width = 800
    window_height = 800
    screen_width = root.winfo_screenwidth()
    screen_height = root.winfo_screenheight()
    center_x = int((screen_width - window_width) / 2)
    center_y = int((screen_height - window_height) / 2)
    root.geometry(f"{window_width}x{window_height}+{center_x}+{center_y}")

    progress_bar = ttk.Progressbar(root, variable=progress_var, maximum=100)
    progress_bar.pack(fill="x", padx=10, pady=5)

    # Legenda
    legend_frame = tk.Frame(root)
    legend_frame.pack(pady=10)

    tk.Label(legend_frame, text="Tipos de Escrituração:", font=("Arial", 12, "bold")).grid(row=0, column=0, padx=5, pady=5)

    tk.Label(legend_frame, text="EFD ICMS IPI (FISCAL)", bg="#1976d2", fg="white", font=("Arial", 10, "bold"), padx=10, pady=5).grid(row=0, column=1, padx=5, pady=5)

    tk.Label(legend_frame, text="EFD CONTRIBUIÇÕES (PIS/COFINS)", bg="#2e7d32", fg="white", font=("Arial", 10, "bold"), padx=10, pady=5).grid(row=0, column=2, padx=5, pady=5)

    # Criando um Frame para funcionalidades
    frame_funcionalidades = tk.Frame(root)
    frame_funcionalidades.pack(pady=20)

    tk.Label(frame_funcionalidades, text="Selecione uma funcionalidade:", font=("Arial", 14, "bold")).pack()

    tk.Button(frame_funcionalidades, text="Ajustar Unidade Registro C170", command=open_adjust_c170, width=60, bg="#1976d2", fg="white", font=("Arial", 12, "bold")).pack(pady=5)
    tk.Button(frame_funcionalidades, text="Verificação e Ajuste CST Pis Cofins Registro C170", command=open_verificar_ncm, width=60, bg="#2e7d32", fg="white", font=("Arial", 12, "bold")).pack(pady=5)
    tk.Button(frame_funcionalidades, text="Ajuste Registro C170 COD_CTA", command=open_cod_cta_adjuster, width=60, bg="#2e7d32", fg="white", font=("Arial", 12, "bold")).pack(pady=5)
    tk.Button(frame_funcionalidades, text="Totalizador XML por CFOP e Alíquota", command=open_totalizador_xml, width=60, bg="#1976d2", fg="white", font=("Arial", 12, "bold")).pack(pady=5)
    tk.Button(frame_funcionalidades, text="Extrair Registros C100", command=open_extrator_sped, width=60, bg="#1976d2", fg="white", font=("Arial", 12, "bold")).pack(pady=5)
    tk.Button(frame_funcionalidades, text="Comparador de Arquivos CSV", command=open_comparador_csv, width=60, bg="#1976d2", fg="white", font=("Arial", 12, "bold")).pack(pady=5)
    tk.Button(frame_funcionalidades, text="Gerador Registro A100", command=open_sped_a100, width=60, bg="#2e7d32", fg="white", font=("Arial", 12, "bold")).pack(pady=5)
    tk.Button(frame_funcionalidades, text="Ajuste CNPJ do PIX - SPED - (Em Desenvolvimento)", command=open_adjust_cnpj, width=60, bg="#1976d2", fg="white", font=("Arial", 12, "bold")).pack(pady=5)
    tk.Button(frame_funcionalidades, text="Ajustar Nr SAT Registro C116", command=open_ajuste_c116, width=60, bg="#2e7d32", fg="white", font=("Arial", 12, "bold")).pack(pady=5)
    tk.Button(frame_funcionalidades, text="Ajustar CPF Registros C800", command=open_c800_processor, width=60, bg="#2e7d32", fg="white", font=("Arial", 12, "bold")).pack(pady=5)


    # Criando um Frame para ferramentas adicionais
    frame_ferramentas = tk.Frame(root)
    frame_ferramentas.pack(pady=20)

    tk.Label(frame_ferramentas, text="Selecione uma ferramenta:", font=("Arial", 14, "bold")).pack()

    tk.Button(frame_ferramentas, text="Organizador de Arquivos SPED'S", command=open_organizador_sped, width=60, bg="#eceff1", fg="black", font=("Arial", 12, "bold")).pack(pady=5)
    tk.Button(frame_ferramentas, text="Verificar Números Faltantes (NF-e/NFC-e)", command=open_verificar_numeros_faltantes, width=60, bg="#eceff1", fg="black", font=("Arial", 12, "bold")).pack(pady=5)

    # Rodapé
    footer_frame = tk.Frame(root, bg="#e0e0e0", relief=tk.SUNKEN, bd=2)
    footer_frame.pack(side=tk.BOTTOM, fill=tk.X, pady=5)

    footer_label = tk.Label(
        footer_frame,
        text="Desenvolvido por Cristianfer Brand",
        font=("Arial", 11, "italic"),
        fg="#333",
        bg="#e0e0e0",
        padx=10,
        pady=5
    )
    footer_label.pack()

    root.mainloop()