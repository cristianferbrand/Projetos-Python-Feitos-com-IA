import os
import xml.etree.ElementTree as ET
import pandas as pd
from tkinter import Tk, filedialog, messagebox

def process_xml_to_excel(file_paths, output_directory):
    for file_path in file_paths:
        try:
            # Parse the XML file
            tree = ET.parse(file_path)
            root = tree.getroot()

            # Extract data for all EMPRESA tags (EMPRESA_1, EMPRESA_2, EMPRESA_3, etc.)
            all_data = []
            for empresa in root.findall(".//EMPRESA_1") + \
                          root.findall(".//EMPRESA_2") + \
                          root.findall(".//EMPRESA_3") + \
                          root.findall(".//EMPRESA_4") + \
                          root.findall(".//EMPRESA_99"):
                empresa_nome = empresa.attrib.get("NOME")
                endereco_ip = empresa.attrib.get("ENDERECO")
                for tabela in empresa:
                    tabela_nome = tabela.tag
                    campos = []
                    for campo in tabela:
                        campo_nome = campo.tag
                        replicar = campo.attrib.get("REPLICAR", "NÃO")
                        campos.append((campo_nome, replicar))
                    all_data.append((empresa_nome, endereco_ip, tabela_nome, campos))

            # Format data for the table with separated columns for "SIM" and "NÃO"
            split_all_data = []
            for empresa_nome, endereco_ip, tabela_nome, campos in all_data:
                campos_sim = [campo[0] for campo in campos if campo[1].upper() == "SIM"]
                campos_nao = [campo[0] for campo in campos if campo[1].upper() == "NÃO"]
                split_all_data.append({
                    "Empresa": empresa_nome,
                    "Endereço IP": endereco_ip,
                    "Tabela": tabela_nome,
                    "Campos (Replicar=SIM)": ", ".join(campos_sim),
                    "Campos (Replicar=NÃO)": ", ".join(campos_nao)
                })

            # Create a DataFrame with the split columns for all EMPRESA tags
            df_split_all = pd.DataFrame(split_all_data)

            # Generate output file name
            base_name = os.path.basename(file_path).replace(".xml", "").replace(".config", "")
            output_file_name = f"{base_name}_Tabela_Configuracoes.xlsx"
            output_file_path = os.path.join(output_directory, output_file_name)

            # Save the DataFrame to an Excel file
            df_split_all.to_excel(output_file_path, index=False)
            print(f"Arquivo processado: {file_path} -> {output_file_path}")

        except ET.ParseError as e:
            print(f"Erro ao processar o arquivo {file_path}: {e}")
            messagebox.showerror("Erro ao Processar", f"Erro no arquivo {file_path}: {e}")

def main():
    # Cria a janela principal do Tkinter
    root = Tk()
    root.withdraw()  # Oculta a janela principal

    # Selecionar múltiplos arquivos XML
    file_paths = filedialog.askopenfilenames(
        title="Selecione os arquivos XML",
        filetypes=[("Arquivos de Configuração", "*.config"), ("Todos os arquivos", "*.*")]
    )

    if not file_paths:
        messagebox.showinfo("Informação", "Nenhum arquivo selecionado.")
        return

    # Selecionar diretório de saída
    output_directory = filedialog.askdirectory(title="Selecione o diretório de saída")

    if not output_directory:
        messagebox.showinfo("Informação", "Nenhum diretório de saída selecionado.")
        return

    # Processar os arquivos selecionados
    process_xml_to_excel(file_paths, output_directory)

    # Exibir mensagem de sucesso
    messagebox.showinfo("Sucesso", "Processamento concluído com sucesso!")

if __name__ == "__main__":
    main()