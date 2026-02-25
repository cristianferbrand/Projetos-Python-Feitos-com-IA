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

# Função para simular o carregamento e mostrar a tela principal
def main_screen():
    # Feche a janela de carregamento
    loading_screen.destroy()

# Função para simular o carregamento e atualizar a barra de progresso
def loading():
    for i in range(101):
        progress_bar['value'] = i
        loading_screen.update_idletasks()
        time.sleep(0.03)
    main_screen()

# Configura a tela de carregamento
loading_screen = tk.Tk()
loading_screen.title("Carregando Sistema")
loading_screen.geometry("400x200")
loading_screen.overrideredirect(True)

# Calcula a posição para centralizar a tela
screen_width = loading_screen.winfo_screenwidth()
screen_height = loading_screen.winfo_screenheight()
x_position = (screen_width // 2) - (400 // 2)
y_position = (screen_height // 2) - (200 // 2)
loading_screen.geometry(f"400x200+{x_position}+{y_position}")

# Configuração do texto e barra de progresso
loading_label = tk.Label(loading_screen, text="Carregando, por favor aguarde...", font=("Arial", 14))
loading_label.pack(pady=40)
progress_bar = ttk.Progressbar(loading_screen, orient="horizontal", length=300, mode="determinate")
progress_bar.pack(pady=20)

# Inicia o carregamento
loading_screen.after(100, loading)
loading_screen.mainloop()

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
        root.update_idletasks()  # Update the UI immediately
        # Executa o processamento em uma thread separada
        processing_thread = threading.Thread(target=sum_icms_by_cfop_aliquot_and_model, args=(directory,  display_results))
        processing_thread.start()
    except Exception as e:
        messagebox.showerror("Erro", f"Ocorreu um erro ao processar os arquivos: {e}")

# Função para extrair registros SPED C100 na Aba 2
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

# Função para comparar dois arquivos CSV na Aba 3
def compare_files(file1_path, file2_path):
    try:
        # Carregar arquivos
        arquivo1 = pd.read_csv(file1_path, delimiter=';', decimal=',')
        arquivo2 = pd.read_csv(file2_path, delimiter=';', decimal=',')

        # Limpeza e conversão
        arquivo1['Numero'] = arquivo1['Numero'].astype(str).str.strip()
        arquivo2['Numero'] = arquivo2['Numero'].astype(str).str.strip()
        arquivo1['Total_NF-e'] = arquivo1['Total_NF-e'].replace(',', '.', regex=True).astype(float)
        arquivo2['Total_NF-e'] = arquivo2['Total_NF-e'].replace(',', '.', regex=True).astype(float)

        # Comparação e união dos arquivos
        comparacao = pd.merge(
            arquivo1[['Numero', 'Serie', 'Total_NF-e', 'Chave_NF-e']],
            arquivo2[['Numero', 'Serie', 'Total_NF-e', 'Chave_NF-e']],
            on='Numero', how='outer', suffixes=('_arquivo1', '_arquivo2'), indicator=True
        )

        # Diferenças em Total_NF-e
        diferencas_total_nfe = comparacao[(comparacao['_merge'] == 'both') & 
                                          (comparacao['Total_NF-e_arquivo1'] != comparacao['Total_NF-e_arquivo2'])]
        
        # Registros presentes apenas em um dos arquivos
        faltantes = comparacao[comparacao['_merge'] != 'both']

        # Exibir resultados
        result_text.delete("1.0", tk.END)  # Limpar texto anterior
        if diferencas_total_nfe.empty and faltantes.empty:
            result_text.insert(tk.END, "Não foram encontradas divergências ou registros faltantes.\n")
        else:
            # Exibir diferenças em Total_NF-e
            if not diferencas_total_nfe.empty:
                result_text.insert(tk.END, "=== Diferenças no Total_NF-e ===\n\n")
                for _, row in diferencas_total_nfe.iterrows():
                    result_text.insert(tk.END, f"Numero: {row['Numero']}\n")
                    result_text.insert(tk.END, f"  Total_NF-e Arquivo 1: {row['Total_NF-e_arquivo1']}\n")
                    result_text.insert(tk.END, f"  Total_NF-e Arquivo 2: {row['Total_NF-e_arquivo2']}\n")
                    result_text.insert(tk.END, f"  Chave_NF-e Arquivo 1: {row['Chave_NF-e_arquivo1']}\n")
                    result_text.insert(tk.END, f"  Chave_NF-e Arquivo 2: {row['Chave_NF-e_arquivo2']}\n")
                    result_text.insert(tk.END, "-"*40 + "\n\n")

            # Exibir registros faltantes
            if not faltantes.empty:
                result_text.insert(tk.END, "=== Registros Faltantes ===\n\n")
                for _, row in faltantes.iterrows():
                    result_text.insert(tk.END, f"Numero: {row['Numero']}\n")
                    if pd.notna(row['Total_NF-e_arquivo1']):
                        result_text.insert(tk.END, "  Presente apenas no Arquivo 1:\n")
                        result_text.insert(tk.END, f"    Total_NF-e: {row['Total_NF-e_arquivo1']}\n")
                        result_text.insert(tk.END, f"    Chave_NF-e: {row['Chave_NF-e_arquivo1']}\n")
                    elif pd.notna(row['Total_NF-e_arquivo2']):
                        result_text.insert(tk.END, "  Presente apenas no Arquivo 2:\n")
                        result_text.insert(tk.END, f"    Total_NF-e: {row['Total_NF-e_arquivo2']}\n")
                        result_text.insert(tk.END, f"    Chave_NF-e: {row['Chave_NF-e_arquivo2']}\n")
                    result_text.insert(tk.END, "-"*40 + "\n\n")
    except Exception as e:
        messagebox.showerror("Erro", f"Ocorreu um erro ao comparar os arquivos: {e}")

# Aba 4 - Função para ajustar o campo COD_CTA nos registros C170 e C175
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

# Aba 5 - Função do Gerador SPED (Registro A100 e A170)
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

def process_directory_gerador_sped(directory_path, aliquota_pis, aliquota_cofins, output_widget, progress_bar):
    output_widget.delete(1.0, tk.END)
    for filename in os.listdir(directory_path):
        if filename.endswith(".xml"):
            file_path = os.path.join(directory_path, filename)
            registros = parse_xml_gerador_sped(file_path, aliquota_pis, aliquota_cofins)
            output_widget.insert(tk.END, f"Processado: {filename}\n")
            output_widget.insert(tk.END, "\n".join(registros) + "\n\n")
    messagebox.showinfo("Processo concluído", "Os registros foram gerados e exibidos na tela.")

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

# Função para selecionar o arquivo de entrada
def select_input_file(entry_input):
    input_path = filedialog.askopenfilename(title="Selecione o arquivo de entrada", filetypes=[("Text Files", "*.txt")])
    if input_path:
        entry_input.delete(0, tk.END)
        entry_input.insert(0, input_path)

# Função para iniciar o processamento
def start_processing(entry_input):
    input_file = entry_input.get()
    if not input_file:
        messagebox.showwarning("Aviso", "Por favor, selecione o arquivo de entrada.")
    else:
        ajustar_cnpj_cod_mun_registro_0150(input_file)

# Função para ajustar o CNPJ, COD_MUN, e ponto final no registro 0150
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

# Inicializar lista de arquivos
lista_arquivos = []

# Função para obter dados do registro 0000
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

# Função para carregar registros 0200
def carregar_registros_0200(arquivo_entrada):
    registros_0200 = {}
    with open(arquivo_entrada, 'r', encoding='latin-1') as entrada:
        for linha in entrada:
            if linha.startswith('|0200|'):
                campos = linha.strip().split('|')
                if len(campos) > 6:
                    cod_produto = campos[2]
                    unidade_produto = campos[6]
                    registros_0200[cod_produto] = unidade_produto
    return registros_0200

# Função para processar múltiplos arquivos
def processar_arquivos_multiplos(lista_arquivos):
    try:
        for arquivo_entrada in lista_arquivos:
            registros_0200 = carregar_registros_0200(arquivo_entrada)

            # Define nome do arquivo de saída
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
                            cod_produto = campos[3]
                            unidade_atual = campos[6]
                            unidade_correta = registros_0200.get(cod_produto)
                            if unidade_correta and unidade_correta != unidade_atual:
                                campos[6] = unidade_correta
                                linha_atualizada = '|'.join(campos) + '\n'
                                saida.write(linha_atualizada)
                            else:
                                saida.write(linha)
        messagebox.showinfo("Concluído", "Todos os arquivos foram processados com sucesso.")
    except Exception as e:
        messagebox.showerror("Erro", f"Ocorreu um erro: {e}")

# Selecionar arquivos de entrada
def selecionar_arquivos_entrada():
    caminhos = filedialog.askopenfilenames(title="Selecione os arquivos para Ajuste da UN", filetypes=[("Arquivos TXT", "*.txt")])
    if caminhos:
        lista_arquivos.extend(caminhos)
        atualizar_texto_entrada()

# Atualizar área de texto
def atualizar_texto_entrada():
    entrada_text.delete("1.0", tk.END)
    entrada_text.insert(tk.END, "\n".join(lista_arquivos))

# Interface gráfica principal
root = tk.Tk()

progress_var = tk.DoubleVar()  # Global progress variable

# Define uma barra de progresso global que permanecerá visível em todas as abas
progress_bar = ttk.Progressbar(root, variable=progress_var, maximum=100)
progress_bar.pack(fill="x", padx=10, pady=5)  # Posição no topo da janela

# Define o título da janela
root.title("AuditHOS")

# Dimensões desejadas da janela
window_width = 1280
window_height = 700

# Obtém as dimensões da tela
screen_width = root.winfo_screenwidth()
screen_height = root.winfo_screenheight()

# Calcula as coordenadas para centralizar a janela
center_x = int((screen_width - window_width) / 2)
center_y = int((screen_height - window_height) / 2)

# Define a geometria da janela com a posição centralizada
root.geometry(f"{window_width}x{window_height}+{center_x}+{center_y}")

# Adiciona um notebook (abas) à janela principal
notebook = ttk.Notebook(root)
notebook.pack(fill="both", expand=True)

# Aba 1 - Totalizador XML por CFOP e Alíquota
frame_xml = tk.Frame(notebook)
notebook.add(frame_xml, text="Totalizador XML")

tk.Button(frame_xml, text="Selecionar Diretório de XML", command=load_directory).pack(pady=10)

# Tabela para exibir os resultados detalhados
columns = ("Modelo", "CFOP", "Alíquota (%)", "Total vProd", "Total vBC", "Total vICMS")
tree = ttk.Treeview(frame_xml, columns=columns, show="headings", height=10)

for col in columns:
    tree.heading(col, text=col)
    tree.column(col, anchor="center", stretch=True)

tree.pack(fill="both", expand=True)

# Tabela para exibir os totais por CFOP
totals_label = tk.Label(frame_xml, text="Totais por CFOP", font=("Arial", 12, "bold"))
totals_label.pack(pady=5)

totals_columns = ("CFOP", "Total vProd", "Total vBC", "Total vICMS")
totals_tree = ttk.Treeview(frame_xml, columns=totals_columns, show="headings", height=5)

for col in totals_columns:
    totals_tree.heading(col, text=col)
    totals_tree.column(col, anchor="center", stretch=True)

totals_tree.pack(fill="both", expand=True)

# Aba 2 - Extrator de Registros SPED C100
frame_sped = tk.Frame(notebook)
notebook.add(frame_sped, text="Extrator de Registros SPED C100")

sped_label_frame = tk.LabelFrame(frame_sped, text="Seleção de Arquivos", padx=10, pady=10)
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
nota_type_frame = tk.Frame(frame_sped)
nota_type_frame.pack(pady=10)
tk.Label(nota_type_frame, text="Tipo de Nota:").pack(side=tk.LEFT)
tk.Radiobutton(nota_type_frame, text="Entrada", variable=nota_type_var, value="entrada").pack(side=tk.LEFT, padx=5)
tk.Radiobutton(nota_type_frame, text="Saída", variable=nota_type_var, value="saida").pack(side=tk.LEFT, padx=5)
tk.Radiobutton(nota_type_frame, text="Ambas", variable=nota_type_var, value="ambas").pack(side=tk.LEFT, padx=5)

tk.Button(frame_sped, text="Processar SPED C100", command=lambda: process_sped_c100(entry_sped_input.get(), entry_sped_output.get(), nota_type_var.get())).pack(pady=20)

# Aba 3 - Comparador de Arquivos CSV
frame_compare = tk.Frame(notebook)
notebook.add(frame_compare, text="Comparador de Arquivos CSV")

compare_label_frame = tk.LabelFrame(frame_compare, text="Selecionar Arquivos para Comparação", padx=10, pady=10)
compare_label_frame.pack(fill="x", padx=10, pady=10)

tk.Label(compare_label_frame, text="Extrato SEFAZ:").grid(row=0, column=0, sticky="w")
entry_file1 = tk.Entry(compare_label_frame, width=50)
entry_file1.grid(row=0, column=1, padx=5)
tk.Button(compare_label_frame, text="Selecionar", command=lambda: entry_file1.insert(0, filedialog.askopenfilename())).grid(row=0, column=2)

tk.Label(compare_label_frame, text="Extrato HOS:").grid(row=1, column=0, sticky="w")
entry_file2 = tk.Entry(compare_label_frame, width=50)
entry_file2.grid(row=1, column=1, padx=5)
tk.Button(compare_label_frame, text="Selecionar", command=lambda: entry_file2.insert(0, filedialog.askopenfilename())).grid(row=1, column=2)

tk.Button(frame_compare, text="Comparar", command=lambda: compare_files(entry_file1.get(), entry_file2.get())).pack(pady=10)

# Criando um frame para o texto de resultado e a barra de rolagem
result_frame = tk.Frame(frame_compare)
result_frame.pack(pady=10, padx=10, fill="both", expand=True)

# Barra de rolagem vertical
scrollbar = tk.Scrollbar(result_frame)
scrollbar.pack(side="right", fill="y")

# Widget de texto com configuração de rolagem
result_text = tk.Text(result_frame, width=100, height=15, yscrollcommand=scrollbar.set, wrap="none")
result_text.pack(fill="both", expand=True)

# Configurando a barra de rolagem para controlar o widget de texto
scrollbar.config(command=result_text.yview)

# Aba 4 - Ajustador de Campo COD_CTA para C170 e C175
frame_cod_cta = tk.Frame(notebook)
notebook.add(frame_cod_cta, text="Ajuste COD_CTA")

cod_cta_label_frame = tk.LabelFrame(frame_cod_cta, text="Seleção de Arquivos", padx=10, pady=10)
cod_cta_label_frame.pack(fill="x", padx=10, pady=10)

tk.Label(cod_cta_label_frame, text="Arquivo de Entrada (SPED):").grid(row=0, column=0, sticky="w")
entry_input = tk.Entry(cod_cta_label_frame, width=50)
entry_input.grid(row=0, column=1, padx=5)
tk.Button(cod_cta_label_frame, text="Selecionar", command=lambda: entry_input.insert(0, filedialog.askopenfilename())).grid(row=0, column=2)

tk.Label(cod_cta_label_frame, text="Arquivo de Saída (Ajustado):").grid(row=1, column=0, sticky="w")
entry_output = tk.Entry(cod_cta_label_frame, width=50)
entry_output.grid(row=1, column=1, padx=5)
tk.Button(cod_cta_label_frame, text="Selecionar", command=lambda: entry_output.insert(0, filedialog.asksaveasfilename(defaultextension=".txt"))).grid(row=1, column=2)

cod_cta_valor_frame = tk.LabelFrame(frame_cod_cta, text="Configuração do Valor COD_CTA", padx=10, pady=10)
cod_cta_valor_frame.pack(fill="x", padx=10, pady=10)
tk.Label(cod_cta_valor_frame, text="Valor para o Campo COD_CTA:").grid(row=0, column=0, sticky="w")
entry_cod_cta = tk.Entry(cod_cta_valor_frame, width=10)
entry_cod_cta.grid(row=0, column=1, padx=5)

tk.Button(frame_cod_cta, text="Iniciar Ajuste", command=lambda: ajustar_cod_cta_c170_c175(entry_input.get(), entry_output.get(), entry_cod_cta.get())).pack(pady=20)

# Aba 5 - Gerador SPED A100
frame_sped = tk.Frame(notebook)
notebook.add(frame_sped, text="Gerador SPED A100")

sped_label_frame = tk.LabelFrame(frame_sped, text="Configuração de Alíquotas", padx=10, pady=10)
sped_label_frame.pack(fill="x", padx=10, pady=10)

tk.Label(sped_label_frame, text="Alíquota de PIS (%)").grid(row=0, column=0, sticky="w")
entry_aliquota_pis = tk.Entry(sped_label_frame, width=10)
entry_aliquota_pis.insert(0, "0,00")
entry_aliquota_pis.grid(row=0, column=1, padx=5)

tk.Label(sped_label_frame, text="Alíquota de COFINS (%)").grid(row=1, column=0, sticky="w")
entry_aliquota_cofins = tk.Entry(sped_label_frame, width=10)
entry_aliquota_cofins.insert(0, "0,00")
entry_aliquota_cofins.grid(row=1, column=1, padx=5)

# Botão de seleção de diretório e barra de progresso
tk.Button(frame_sped, text="Selecionar Diretório de XML e Processar", 
          command=lambda: select_directory_sped(entry_aliquota_pis.get(), entry_aliquota_cofins.get(), output_sped, progress_bar)).pack(pady=10)

# Área de saída para exibir os registros gerados
output_sped = scrolledtext.ScrolledText(frame_sped, width=90, height=15)
output_sped.pack(pady=10, padx=10, fill="both", expand=True)

def select_directory_sped(aliquota_pis, aliquota_cofins, output_widget, progress_bar):
    directory_path = filedialog.askdirectory()
    if directory_path:
        try:
            aliquota_pis = float(aliquota_pis.replace(",", "."))
            aliquota_cofins = float(aliquota_cofins.replace(",", "."))
            process_directory_gerador_sped(directory_path, aliquota_pis, aliquota_cofins, output_widget, progress_bar)
        except ValueError:
            messagebox.showerror("Erro", "Por favor, insira valores numéricos válidos para as alíquotas.")

# Aba 6 - Ajuste de CNPJ e Código do Município
frame_adjust = tk.Frame(notebook)
notebook.add(frame_adjust, text="Ajuste CNPJ do PIX - SPED")

# LabelFrame para seleção de arquivo de entrada
adjust_label_frame = tk.LabelFrame(frame_adjust, text="Seleção de Arquivo", padx=10, pady=10)
adjust_label_frame.pack(fill="x", padx=10, pady=10)

# Campo para selecionar o arquivo de entrada
tk.Label(adjust_label_frame, text="Arquivo de Entrada (SPED):").grid(row=0, column=0, sticky="w")
entry_adjust_input = tk.Entry(adjust_label_frame, width=50)
entry_adjust_input.grid(row=0, column=1, padx=5)
tk.Button(adjust_label_frame, text="Selecionar", command=lambda: entry_adjust_input.insert(0, filedialog.askopenfilename())).grid(row=0, column=2)

# Botão para iniciar o ajuste
tk.Button(frame_adjust, text="Iniciar Ajuste", command=lambda: start_processing(entry_adjust_input)).pack(pady=20)

#Aba 7 - Organizador de Arquivos SPED
frame_sped = tk.Frame(notebook)
notebook.add(frame_sped, text="Organizador de Arquivos SPED")

tk.Label(frame_sped, text="Organizar Arquivos SPED").pack(pady=10)

# Button for organizing SPED Fiscal files
tk.Button(frame_sped, text="Organizar SPED Fiscal", command=organizar_sped_fiscal).pack(pady=5)

# Button for organizing SPED Contribuições files
tk.Button(frame_sped, text="Organizar SPED Contribuições", command=organizar_sped_contribuicoes).pack(pady=5)

# Aba 8 - Ajustar Unidade Registro C170

frame_processor = tk.Frame(notebook)
notebook.add(frame_processor, text="Ajustar Unidade Registro C170")

# Variáveis
entrada_var = tk.StringVar()
lista_arquivos = []

# Layout dentro da nova aba
frame_entrada = tk.Frame(frame_processor, padx=10, pady=10, relief=tk.RIDGE, borderwidth=2)
frame_entrada.pack(fill=tk.BOTH, padx=10, pady=5, expand=True)

frame_botoes = tk.Frame(frame_processor, padx=10, pady=10)
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

footer = tk.Label(text="Desenvolvido por Cristianfer", font=("Arial", 10))
footer.pack(pady=10)

root.mainloop()