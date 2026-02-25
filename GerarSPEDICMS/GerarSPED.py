import xml.etree.ElementTree as ET
import os
import time
from concurrent.futures import ThreadPoolExecutor
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from threading import Thread

def gerar_registro_0000():
    return "|0000|016|1|20250101|20250131|EMPRESA EXEMPLO|12345678000123|SP|123456789|3550308||A|0|\n"

def gerar_registro_0001():
    return "|0001|0|\n"

def gerar_registro_0150(participantes):
    for codigo, dados in participantes.items():
        yield f"|0150|{codigo}|{dados['nome']}|{dados['cpf_cnpj']}|{dados['codigo_municipio']}|||\n"

def processar_xml(xml_file):
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        namespace = {'ns': 'http://www.portalfiscal.inf.br/nfe'}
        
        registros = []
        participantes = {}
        
        for nfe in root.findall('.//ns:NFe', namespace):
            chave = nfe.find('.//ns:infNFe', namespace).get('Id')[3:]
            tipo = nfe.find('.//ns:ide/ns:tpNF', namespace).text
            modelo = nfe.find('.//ns:ide/ns:mod', namespace).text
            serie = nfe.find('.//ns:ide/ns:serie', namespace).text
            numero = nfe.find('.//ns:ide/ns:nNF', namespace).text
            data_emissao = nfe.find('.//ns:ide/ns:dhEmi', namespace).text[:10]
            valor_total = nfe.find('.//ns:total/ns:ICMSTot/ns:vNF', namespace).text
            cfop = nfe.find('.//ns:det/ns:prod/ns:CFOP', namespace).text
            emitente = nfe.find('.//ns:emit/ns:xNome', namespace).text
            cnpj_emitente = nfe.find('.//ns:emit/ns:CNPJ', namespace).text
            
            # Adicionar emitente como participante
            if cnpj_emitente not in participantes:
                participantes[cnpj_emitente] = {
                    'nome': emitente,
                    'cpf_cnpj': cnpj_emitente,
                    'codigo_municipio': "3550308",  # Ajuste conforme necessário
                }
            
            # Registro C100
            registros.append(f"|C100|0|{tipo}|{modelo}|{serie}|{numero}|{data_emissao}|{valor_total}|||||{valor_total}|0.00|0.00|\n")
            
            # Registro C170 (Itens)
            for item in nfe.findall('.//ns:det', namespace):
                num_item = item.get('nItem')
                cod_item = item.find('.//ns:prod/ns:cProd', namespace).text
                descricao = item.find('.//ns:prod/ns:xProd', namespace).text
                quantidade = item.find('.//ns:prod/ns:qCom', namespace).text
                unidade = item.find('.//ns:prod/ns:uCom', namespace).text
                valor_item = item.find('.//ns:prod/ns:vProd', namespace).text
                
                registros.append(f"|C170|{num_item}|{cod_item}|{descricao}|{quantidade}|{unidade}|{valor_item}|||{cfop}|||||\n")
            
            # Registro C190 (Resumo)
            registros.append(f"|C190|{cfop}|{valor_total}|18.00|0.00|\n")
        
        return registros, participantes
    except Exception as e:
        return [], {}

def atualizar_tempo_inicio(start_time, progress_label):
    while not processamento_concluido:
        elapsed_time = time.time() - start_time
        formatted_time = time.strftime("%H:%M:%S", time.gmtime(elapsed_time))
        progress_label.config(text=f"Tempo decorrido: {formatted_time}")
        time.sleep(1)  # Atualiza a cada 1 segundo

def executar_geracao(xml_files, output_file, progress_bar, progress_label):
    global processamento_concluido
    processamento_concluido = False
    participantes = {}
    registros_gerais = []
    
    start_time = time.time()  # Início da contagem do tempo
    Thread(target=atualizar_tempo_inicio, args=(start_time, progress_label)).start()
    
    # Processar XMLs em paralelo
    with ThreadPoolExecutor() as executor:
        resultados = list(executor.map(processar_xml, xml_files))
    
    # Consolidar resultados
    for registros, novos_participantes in resultados:
        registros_gerais.extend(registros)
        participantes.update(novos_participantes)
    
    # Criar e gravar o arquivo
    with open(output_file, 'w', encoding='latin-1') as f:
        f.write(gerar_registro_0000())
        f.write(gerar_registro_0001())
        f.writelines(registros_gerais)
        for registro in gerar_registro_0150(participantes):
            f.write(registro)
        f.write("|9999|0|\n")  # Registro de fechamento
    
    processamento_concluido = True
    elapsed_time = time.time() - start_time
    formatted_time = time.strftime("%H:%M:%S", time.gmtime(elapsed_time))
    progress_label.config(text=f"Processo concluído! Tempo total: {formatted_time}")
    messagebox.showinfo("Sucesso", f"Arquivo EFD ICMS/IPI salvo em: {output_file}\nTempo total: {formatted_time}")

def gerar_efd():
    global processamento_concluido
    processamento_concluido = False
    
    xml_files = selecionar_arquivos()
    if not xml_files:
        messagebox.showwarning("Aviso", "Nenhum arquivo XML selecionado!")
        return
    
    pasta_destino = selecionar_pasta_destino()
    if not pasta_destino:
        messagebox.showwarning("Aviso", "Nenhuma pasta de destino selecionada!")
        return
    
    output_file = os.path.join(pasta_destino, "EFD_ICMS_IPI.txt")
    
    progress_label.config(text="Processando...")
    progress_bar["value"] = 0
    progress_bar["maximum"] = len(xml_files)
    
    # Iniciar o processamento em uma nova thread
    thread = Thread(target=executar_geracao, args=(xml_files, output_file, progress_bar, progress_label))
    thread.start()

def selecionar_arquivos():
    return filedialog.askopenfilenames(filetypes=[("Arquivos XML", "*.xml")])

def selecionar_pasta_destino():
    return filedialog.askdirectory()

# Interface Gráfica
app = tk.Tk()
app.title("Gerador Completo de EFD ICMS/IPI")
app.geometry("400x250")

label = tk.Label(app, text="Selecione os arquivos XML e o destino para salvar.")
label.pack(pady=10)

btn_gerar = tk.Button(app, text="Gerar Arquivo EFD", command=gerar_efd)
btn_gerar.pack(pady=10)

progress_bar = ttk.Progressbar(app, orient="horizontal", mode="determinate", length=300)
progress_bar.pack(pady=10)

progress_label = tk.Label(app, text="")
progress_label.pack(pady=5)

app.mainloop()