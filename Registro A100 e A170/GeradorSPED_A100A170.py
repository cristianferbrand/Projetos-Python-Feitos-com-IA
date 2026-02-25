import xml.etree.ElementTree as ET
from datetime import datetime
import os
import tkinter as tk
from tkinter import filedialog, messagebox

def parse_xml(file_path, aliquota_pis, aliquota_cofins):
    # Carrega o XML e extrai os dados necessários
    tree = ET.parse(file_path)
    root = tree.getroot()
    registros = []

    for item in root.findall('item'):
        # Dados para o Registro A100
        numero_doc = item.find('numero').text
        data_emissao_raw = item.find('data_emissao').text
        
        # Ajuste para lidar com frações de segundo, se existirem
        try:
            data_emissao = datetime.strptime(data_emissao_raw, "%Y-%m-%d %H:%M:%S.%f").strftime("%d%m%Y")
        except ValueError:
            data_emissao = datetime.strptime(data_emissao_raw, "%Y-%m-%d %H:%M:%S").strftime("%d%m%Y")
        
        valor_servico_raw = item.find('servico/valores/valor_servico').text
        # Remove o símbolo de R$, pontos de milhar e substitui vírgulas por pontos
        valor_servico = valor_servico_raw.replace("R$", "").replace(".", "").replace(",", ".").strip()

        # Calcula os valores de PIS e COFINS com base no valor do serviço e nas alíquotas fornecidas
        valor_pis = round(float(valor_servico) * (aliquota_pis / 100), 2)
        valor_cofins = round(float(valor_servico) * (aliquota_cofins / 100), 2)

        # Converte os valores para string e troca ponto por vírgula
        valor_servico = str(valor_servico).replace(".", ",")
        valor_pis = str(valor_pis).replace(".", ",")
        valor_cofins = str(valor_cofins).replace(".", ",")

        # Cria o registro A100 com os valores de PIS e COFINS do A170
        registro_a100 = (
            f"|A100|1|0||00|||{numero_doc}||{data_emissao}||{valor_servico}|0||{valor_servico}|"
            f"{valor_pis}|{valor_servico}|{valor_cofins}|||||"
        )
        registros.append(registro_a100)

        # Cria o único registro A170 para cada A100
        registro_a170 = (
            f"|A170|1|SER0407||{valor_servico}||||01|{valor_servico}|{aliquota_pis:.2f}|{valor_pis}|01|"
            f"{valor_servico}|{aliquota_cofins:.2f}|{valor_cofins}|||"
        )
        registros.append(registro_a170)

    return registros

def process_directory(directory_path, aliquota_pis, aliquota_cofins):
    output_file = os.path.join(directory_path, "SPED_Registros.txt")
    with open(output_file, 'w') as f:
        for filename in os.listdir(directory_path):
            if filename.endswith(".xml"):
                file_path = os.path.join(directory_path, filename)
                registros = parse_xml(file_path, aliquota_pis, aliquota_cofins)
                f.write("\n".join(registros) + "\n")
            print(f"Processado: {filename}")
    messagebox.showinfo("Processo concluído", f"Os registros foram gerados no arquivo {output_file}")

def select_directory():
    directory_path = filedialog.askdirectory()
    if directory_path:
        try:
            aliquota_pis = float(entry_aliquota_pis.get().replace(",", "."))
            aliquota_cofins = float(entry_aliquota_cofins.get().replace(",", "."))
            process_directory(directory_path, aliquota_pis, aliquota_cofins)
        except ValueError:
            messagebox.showerror("Erro", "Por favor, insira valores numéricos válidos para as alíquotas.")

# Configuração da Interface Gráfica
root = tk.Tk()
root.title("Gerador de Registros SPED A100 e A170")
root.geometry("400x250")

label = tk.Label(root, text="Selecione a pasta com os arquivos XML:", font=("Arial", 12))
label.pack(pady=10)

select_button = tk.Button(root, text="Selecionar Pasta", command=select_directory, font=("Arial", 12), bg="lightblue")
select_button.pack(pady=5)

# Alíquota de PIS
label_aliquota_pis = tk.Label(root, text="Alíquota de PIS (%)", font=("Arial", 10))
label_aliquota_pis.pack(pady=5)
entry_aliquota_pis = tk.Entry(root, font=("Arial", 10))
entry_aliquota_pis.insert(0, "0,65")  # Valor padrão
entry_aliquota_pis.pack()

# Alíquota de COFINS
label_aliquota_cofins = tk.Label(root, text="Alíquota de COFINS (%)", font=("Arial", 10))
label_aliquota_cofins.pack(pady=5)
entry_aliquota_cofins = tk.Entry(root, font=("Arial", 10))
entry_aliquota_cofins.insert(0, "3")  # Valor padrão
entry_aliquota_cofins.pack()

# Botão de saída
exit_button = tk.Button(root, text="Sair", command=root.quit, font=("Arial", 12), bg="lightcoral")
exit_button.pack(pady=10)

root.mainloop()