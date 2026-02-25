import tkinter as tk
from tkinter import ttk
from tkinter import messagebox

def formatar_valor_dinamico(event):
    widget = event.widget
    try:
        valor = widget.get().replace('.', '').replace(',', '')
        if valor:
            valor_formatado = f"{float(valor) / 100:,.2f}".replace('.', 'X').replace(',', '.').replace('X', ',')
            widget.delete(0, tk.END)
            widget.insert(0, valor_formatado)
    except ValueError:
        pass

def obter_valor_para_calculo(valor_str):
    return float(valor_str.replace('.', '').replace(',', '.'))

def calcular_icms_desonerado():
    campos_invalidos = []  # Lista para armazenar campos com problemas

    try:
        # Coletar e validar valores das entradas
        try:
            preco_nf = obter_valor_para_calculo(entry_preco_nf.get())
        except ValueError:
            campos_invalidos.append("Preço na Nota Fiscal")
            preco_nf = None  # Evitar cálculo com valor inválido

        try:
            aliquota = obter_valor_para_calculo(entry_aliquota.get()) / 100
        except ValueError:
            campos_invalidos.append("Alíquota")
            aliquota = None  # Evitar cálculo com valor inválido

        try:
            percentual_reducao_bc = obter_valor_para_calculo(entry_percentual_reducao_bc.get()) / 100
        except ValueError:
            # Tratar apenas se for necessário (CST 020 ou 070)
            percentual_reducao_bc = None

        # Coletar e validar CST
        cst = combo_cst.get()
        if cst not in ["020", "070", "030", "040"]:
            campos_invalidos.append("CST")

        # Verificar se percentual_reducao_bc é necessário
        if cst in ["020", "070"] and percentual_reducao_bc is None:
            campos_invalidos.append("Percentual de Redução da Base de Cálculo")

        # Se houver campos inválidos, lançar exceção
        if campos_invalidos:
            raise ValueError

        # Realizar o cálculo do ICMS desonerado
        if cst in ["020", "070"]:  # Redução de Base de Cálculo ou Alíquota
            valor_icms_desonerado = preco_nf * aliquota * percentual_reducao_bc
        elif cst in ["030", "040"]:  # Isenção ou Suspensão
            valor_icms_desonerado = preco_nf * aliquota
        else:
            raise ValueError("CST inválido para cálculo")

        # Atualizar os resultados no label
        label_resultado_icms_desonerado.config(
            text=f"R$ {valor_icms_desonerado:,.2f}".replace('.', 'X').replace(',', '.').replace('X', ',')
        )

    except ValueError:
        # Exibir mensagem com os campos inválidos
        if campos_invalidos:
            mensagem_erro = "Por favor, preencha corretamente os seguintes campos:\n" + "\n".join(campos_invalidos)
        else:
            mensagem_erro = "Erro desconhecido no cálculo. Verifique os dados inseridos."

        # Mostrar erro em uma MessageBox
        messagebox.showerror("Erro", mensagem_erro)

def limpar_campos():
    entry_preco_nf.delete(0, tk.END)
    entry_aliquota.delete(0, tk.END)
    entry_percentual_reducao_bc.delete(0, tk.END)
    combo_cst.set("")
    label_resultado_icms_desonerado.config(text="R$ 0,00")

def on_enter(event):
    event.widget.tk_focusNext().focus()
    return "break"

# Configuração da janela principal
root = tk.Tk()
root.title("Calculadora ICMS Desonerado")
root.geometry("800x400")

# Frame principal
frame = ttk.Frame(root, padding="10")
frame.pack(fill=tk.BOTH, expand=True)

# Frame para entradas
input_frame = ttk.LabelFrame(frame, text="Entradas", padding="10")
input_frame.grid(row=0, column=0, padx=10, pady=10, sticky=tk.N)

# Entradas
ttk.Label(input_frame, text="Preço na Nota Fiscal:").grid(row=0, column=0, sticky=tk.W, pady=5)
entry_preco_nf = ttk.Entry(input_frame, width=20)
entry_preco_nf.grid(row=0, column=1, sticky=tk.E, pady=5)
entry_preco_nf.bind('<KeyRelease>', formatar_valor_dinamico)
entry_preco_nf.bind('<Return>', on_enter)

ttk.Label(input_frame, text="Alíquota (%):").grid(row=1, column=0, sticky=tk.W, pady=5)
entry_aliquota = ttk.Entry(input_frame, width=20)
entry_aliquota.grid(row=1, column=1, sticky=tk.E, pady=5)
entry_aliquota.bind('<KeyRelease>', formatar_valor_dinamico)
entry_aliquota.bind('<Return>', on_enter)

ttk.Label(input_frame, text="Percentual de Redução da BC (%):").grid(row=2, column=0, sticky=tk.W, pady=5)
entry_percentual_reducao_bc = ttk.Entry(input_frame, width=20)
entry_percentual_reducao_bc.grid(row=2, column=1, sticky=tk.E, pady=5)
entry_percentual_reducao_bc.bind('<KeyRelease>', formatar_valor_dinamico)
entry_percentual_reducao_bc.bind('<Return>', on_enter)

ttk.Label(input_frame, text="CST:").grid(row=3, column=0, sticky=tk.W, pady=5)
combo_cst = ttk.Combobox(input_frame, values=["020", "030", "040", "070"], width=17)
combo_cst.grid(row=3, column=1, sticky=tk.E, pady=5)
combo_cst.set("")
combo_cst.bind('<Return>', on_enter)

# Botão Calcular
btn_calcular = ttk.Button(input_frame, text="Calcular", command=calcular_icms_desonerado)
btn_calcular.grid(row=4, column=0, pady=10)

# Botão Limpar
btn_limpar = ttk.Button(input_frame, text="Limpar", command=limpar_campos)
btn_limpar.grid(row=4, column=1, pady=10)

# Frame para resultados
result_frame = ttk.LabelFrame(frame, text="Resultado", padding="10")
result_frame.grid(row=0, column=1, padx=10, pady=10, sticky=tk.N)

# Resultados
ttk.Label(result_frame, text="Valor do ICMS Desonerado:").grid(row=0, column=0, sticky=tk.W, pady=5)
label_resultado_icms_desonerado = ttk.Label(result_frame, text="R$ 0,00", font=("Arial", 10, "bold"))
label_resultado_icms_desonerado.grid(row=0, column=1, sticky=tk.E, pady=5)

# Observações
observacoes = (
    "Na hipótese de operações com modalidades de desoneração classificadas como 'Isenção' ou 'Não Incidência' no Manual de Benefícios, serão utilizados os códigos 30 ou 40 relativos ao Código de Situação Tributária - CST, conforme o caso.\n\n"
    "Na hipótese de operações com modalidades de desoneração classificadas como 'Redução de Base de Cálculo' ou 'Redução de Alíquota' no Manual de Benefícios, serão utilizados os códigos 20 ou 70 relativos ao Código de Situação Tributária - CST."
)

observacao_label = ttk.Label(frame, text=observacoes, wraplength=760, justify="left", foreground="blue")
observacao_label.grid(row=1, column=0, columnspan=2, pady=10, sticky=tk.W)

footer = tk.Label(text="Desenvolvido por Cristianfer", font=("Arial", 10))
footer.pack(pady=10)

# Ajuste final da janela
root.mainloop()