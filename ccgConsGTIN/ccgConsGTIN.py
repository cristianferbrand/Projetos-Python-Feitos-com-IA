import tkinter as tk
from tkinter import messagebox, filedialog, Toplevel, scrolledtext
import requests_pkcs12
from lxml import etree

# URLs do Web Service atualizado (ccgConsGTIN)
URLS = {
    "Homologação": "https://dfe-servico-homologacao.svrs.rs.gov.br/ws/ccgConsGTIN/ccgConsGTIN.asmx",
    "Produção": "https://dfe-servico.svrs.rs.gov.br/ws/ccgConsGTIN/ccgConsGTIN.asmx"
}

# Exibe o XML enviado para debug
def exibir_xml_envio(xml):
    janela_debug = Toplevel()
    janela_debug.title("XML Enviado")
    janela_debug.geometry("600x400")
    texto = scrolledtext.ScrolledText(janela_debug, wrap=tk.WORD)
    texto.insert(tk.END, xml)
    texto.pack(fill=tk.BOTH, expand=True)

# Ação ao clicar em "Consultar"
def consultar_gtin():
    gtin = entry_gtin.get().strip()
    cert_path = entry_cert.get()
    cert_senha = entry_senha.get()
    ambiente = var_ambiente.get()

    if not gtin.isdigit():
        messagebox.showerror("Erro", "Digite um GTIN numérico válido.")
        return
    if not cert_path or not cert_senha:
        messagebox.showerror("Erro", "Informe o caminho do certificado e a senha.")
        return

    tpAmb = "2" if ambiente == "Homologação" else "1"
    url = URLS[ambiente]

    # XML de envio com namespace correto
    xml_envio = f"""
    <ccgConsGTIN xmlns="http://www.portalfiscal.inf.br/ccgConsGTIN" versao="1.00">
        <tpAmb>{tpAmb}</tpAmb>
        <GTIN>{gtin}</GTIN>
    </ccgConsGTIN>
    """

    # Envelope SOAP 1.1
    envelope = f"""<?xml version="1.0" encoding="UTF-8"?>
    <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
        <soap:Header/>
        <soap:Body>
            {xml_envio}
        </soap:Body>
    </soap:Envelope>
    """

    try:
        headers = {
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": "http://www.portalfiscal.inf.br/nfe/wsdl/ccgConsGtin/ccgConsGTIN"
        }

        # Requisição com certificado A1
        response = requests_pkcs12.post(
            url,
            data=envelope.encode("utf-8"),
            headers=headers,
            pkcs12_filename=cert_path,
            pkcs12_password=cert_senha
        )

        # Mostrar XML de envio (debug opcional)
        exibir_xml_envio(envelope)

        # Processar e exibir resposta formatada
        resposta_formatada = etree.tostring(
            etree.fromstring(response.content),
            pretty_print=True,
            encoding="unicode"
        )

        output_text.delete("1.0", tk.END)
        output_text.insert(tk.END, resposta_formatada)

    except Exception as e:
        messagebox.showerror("Erro na consulta", f"{str(e)}")

# Seleciona arquivo de certificado
def selecionar_certificado():
    caminho = filedialog.askopenfilename(filetypes=[("Certificado Digital", "*.pfx")])
    if caminho:
        entry_cert.delete(0, tk.END)
        entry_cert.insert(0, caminho)

# Interface gráfica
app = tk.Tk()
app.title("Consulta GTIN - Cadastro Centralizado (CCG)")
app.geometry("600x550")

# Campo GTIN
tk.Label(app, text="GTIN (Código de Barras):").pack(pady=5)
entry_gtin = tk.Entry(app, width=40)
entry_gtin.pack()

# Certificado Digital
tk.Label(app, text="Certificado Digital (.pfx):").pack(pady=5)
frame_cert = tk.Frame(app)
entry_cert = tk.Entry(frame_cert, width=35)
entry_cert.pack(side=tk.LEFT)
btn_cert = tk.Button(frame_cert, text="📁", command=selecionar_certificado)
btn_cert.pack(side=tk.LEFT)
frame_cert.pack()

# Senha do certificado
tk.Label(app, text="Senha do Certificado:").pack(pady=5)
entry_senha = tk.Entry(app, width=40, show="*")
entry_senha.pack()

# Ambiente
tk.Label(app, text="Ambiente de Consulta:").pack(pady=5)
var_ambiente = tk.StringVar(value="Homologação")
tk.OptionMenu(app, var_ambiente, "Homologação", "Produção").pack()

# Botão Consultar
tk.Button(app, text="Consultar", command=consultar_gtin).pack(pady=15)

# Área de resultado
tk.Label(app, text="Resposta da SEFAZ:").pack()
output_text = tk.Text(app, height=18, width=75)
output_text.pack(pady=10)

app.mainloop()