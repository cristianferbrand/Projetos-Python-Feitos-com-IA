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

def calcular_difal():
    campos_invalidos = []

    try:
        # Coletar valores das entradas
        try:
            aliq_interna = obter_valor_para_calculo(entry_aliq_interna.get()) / 100  # Convertendo para decimal
        except ValueError:
            campos_invalidos.append("% Alíquota Interna")

        try:
            aliq_interestadual = obter_valor_para_calculo(combo_aliq_interestadual.get()) / 100  # Convertendo para decimal
        except ValueError:
            campos_invalidos.append("Alíquota Interestadual")

        try:
            fcp = obter_valor_para_calculo(combo_fcp.get()) / 100  # Convertendo para decimal
        except ValueError:
            campos_invalidos.append("% Fundo de Combate à Pobreza")

        try:
            valor_produto = obter_valor_para_calculo(entry_valor_produto.get())
        except ValueError:
            campos_invalidos.append("Valor Produto")

        try:
            valor_ipi = obter_valor_para_calculo(entry_valor_ipi.get())
        except ValueError:
            campos_invalidos.append("Valor do IPI")

        try:
            valor_frete = obter_valor_para_calculo(entry_valor_frete.get())
        except ValueError:
            campos_invalidos.append("Valor do Frete")

        try:
            outras_despesas = obter_valor_para_calculo(entry_outras_despesas.get())
        except ValueError:
            campos_invalidos.append("Outras Despesas Acessórias")

        try:
            descontos = obter_valor_para_calculo(entry_descontos.get())
        except ValueError:
            campos_invalidos.append("Descontos")

        # Se houver campos inválidos, lançar exceção
        if campos_invalidos:
            raise ValueError

        # Calcular o valor total da operação
        base_calculo = valor_produto + valor_ipi + valor_frete + outras_despesas - descontos

        # Inicializar variáveis (para evitar erros de variáveis não definidas)
        base_calculo_1 = 0
        base_calculo_2 = 0
        valor_fcp = 0
        difal = 0
        icms_interno = 0
        icms_interestadual = 0
        base_icms_uf_destino = 0
        valor_icms_uf_destino = 0
        valor_icms_uf_emitente = 0

        # Verificar o tipo de cálculo
        tipo_calculo = combo_tipo_calculo.get()

        if tipo_calculo == "Por Fora":
            # Cálculo Por Fora
            icms_interno = base_calculo * aliq_interna
            icms_interestadual = base_calculo * aliq_interestadual
            difal = icms_interno - icms_interestadual
            valor_fcp = base_calculo * fcp

            # Calcular BASE_ICMS_UF_DESTINO e VALOR_ICMS_UF_DESTINO E VALOR_ICMS_UF_EMITENTE
            base_icms_uf_destino = base_calculo
            valor_icms_uf_destino = base_icms_uf_destino * aliq_interna
            valor_icms_uf_emitente = base_calculo * aliq_interestadual

        elif tipo_calculo == "Por Dentro":
            # Cálculo Por Dentro (Base Dupla)
            icms_interestadual = base_calculo * aliq_interestadual
            base_calculo_1 = base_calculo - icms_interestadual
            base_calculo_2 = base_calculo_1 / (1 - aliq_interna)
            icms_interno = base_calculo_2 * aliq_interna
            difal = icms_interno - icms_interestadual
            valor_fcp = base_calculo * fcp

            # Calcular BASE_ICMS_UF_DESTINO e VALOR_ICMS_UF_DESTINO E VALOR_ICMS_UF_EMITENTE
            base_icms_uf_destino = base_calculo_2
            valor_icms_uf_destino = difal * (aliq_interna / (aliq_interna - aliq_interestadual))
            valor_icms_uf_emitente = icms_interestadual

        else:
            raise ValueError("Tipo de cálculo inválido")

        # Atualizar os resultados (somente variáveis relevantes)
        label_resultado_valor_operacao.config(text=f"R$ {base_calculo:,.2f}".replace('.', 'X').replace(',', '.').replace('X', ','))
        label_resultado_base_calculo_1.config(text=f"R$ {base_calculo_1:,.2f}".replace('.', 'X').replace(',', '.').replace('X', ','))
        label_resultado_base_calculo_2.config(text=f"R$ {base_calculo_2:,.2f}".replace('.', 'X').replace(',', '.').replace('X', ','))
        label_resultado_icms_interno.config(text=f"R$ {icms_interno:,.2f}".replace('.', 'X').replace(',', '.').replace('X', ','))
        label_resultado_icms_interestadual.config(text=f"R$ {icms_interestadual:,.2f}".replace('.', 'X').replace(',', '.').replace('X', ','))
        label_resultado_difal.config(text=f"R$ {difal:,.2f}".replace('.', 'X').replace(',', '.').replace('X', ','))
        label_resultado_fcp.config(text=f"R$ {valor_fcp:,.2f}".replace('.', 'X').replace(',', '.').replace('X', ','))
        label_resultado_base_icms_destino.config(text=f"R$ {base_icms_uf_destino:,.2f}".replace('.', 'X').replace(',', '.').replace('X', ','))
        label_resultado_valor_icms_destino.config(text=f"R$ {valor_icms_uf_destino:,.2f}".replace('.', 'X').replace(',', '.').replace('X', ','))
        label_resultado_valor_icms_emitente.config(text=f"R$ {valor_icms_uf_emitente:,.2f}".replace('.', 'X').replace(',', '.').replace('X', ','))

    except ValueError:
        mensagem_erro = "Por favor, preencha corretamente os seguintes campos:\n" + "\n".join(campos_invalidos)
        messagebox.showerror("Erro", mensagem_erro)

def on_enter(event):
    """
    Avança para o próximo campo ao pressionar Enter.
    """
    event.widget.tk_focusNext().focus()
    return "break"

def limpar_campos():
    entry_aliq_interna.delete(0, tk.END)
    combo_aliq_interestadual.set("4")
    combo_fcp.set("0")
    entry_valor_produto.delete(0, tk.END)
    entry_valor_ipi.delete(0, tk.END)
    entry_valor_frete.delete(0, tk.END)
    entry_outras_despesas.delete(0, tk.END)
    entry_descontos.delete(0, tk.END)
    label_resultado_valor_operacao.config(text="R$ 0,00")
    label_resultado_difal.config(text="R$ 0,00")
    label_resultado_fcp.config(text="R$ 0,00")
    label_resultado_base_calculo_1.config(text="R$ 0,00")
    label_resultado_base_calculo_2.config(text="R$ 0,00")
    label_resultado_base_icms_destino(text="R$ 0,00")
    label_resultado_icms_interno.config(text="R$ 0,00")
    label_resultado_icms_interestadual.config(text="R$ 0,00")

# Configuração da janela principal
root = tk.Tk()
root.title("Calculadora DIFAL")
root.geometry("800x600")

# Frame principal
frame = ttk.Frame(root, padding="10")
frame.pack(fill=tk.BOTH, expand=True)

# Frame para entradas
input_frame = ttk.LabelFrame(frame, text="Entradas", padding="10")
input_frame.grid(row=0, column=0, padx=10, pady=10, sticky=tk.N)

# Tipo de Cálculo
ttk.Label(input_frame, text="Tipo de Cálculo:").grid(row=0, column=0, sticky=tk.W, pady=5)
combo_tipo_calculo = ttk.Combobox(input_frame, values=["Por Fora", "Por Dentro"], width=17)
combo_tipo_calculo.grid(row=0, column=1, sticky=tk.E, pady=5)
combo_tipo_calculo.current(0)
combo_tipo_calculo.bind('<Return>', on_enter)  # Avançar com Enter

# % Alíquota Interna
ttk.Label(input_frame, text="% Alíquota Interna:").grid(row=1, column=0, sticky=tk.W, pady=5)
entry_aliq_interna = ttk.Entry(input_frame, width=20)
entry_aliq_interna.grid(row=1, column=1, sticky=tk.E, pady=5)
entry_aliq_interna.bind('<KeyRelease>', formatar_valor_dinamico)
entry_aliq_interna.bind('<Return>', on_enter)  # Avançar com Enter

# Alíquota Interestadual
ttk.Label(input_frame, text="Alíquota Interestadual:").grid(row=2, column=0, sticky=tk.W, pady=5)
combo_aliq_interestadual = ttk.Combobox(input_frame, values=["4", "7", "12"], width=17)
combo_aliq_interestadual.grid(row=2, column=1, sticky=tk.E, pady=5)
combo_aliq_interestadual.current(0)
combo_aliq_interestadual.bind('<Return>', on_enter)  # Avançar com Enter

# % Fundo de Combate à Pobreza
ttk.Label(input_frame, text="% Fundo de Combate à Pobreza:").grid(row=3, column=0, sticky=tk.W, pady=5)
combo_fcp = ttk.Combobox(input_frame, values=["0", "1", "1.6", "2"], width=17)
combo_fcp.grid(row=3, column=1, sticky=tk.E, pady=5)
combo_fcp.current(0)
combo_fcp.bind('<Return>', on_enter)  # Avançar com Enter

# Valor Produto
ttk.Label(input_frame, text="Valor Produto:").grid(row=4, column=0, sticky=tk.W, pady=5)
entry_valor_produto = ttk.Entry(input_frame, width=20)
entry_valor_produto.grid(row=4, column=1, sticky=tk.E, pady=5)
entry_valor_produto.bind('<KeyRelease>', formatar_valor_dinamico)
entry_valor_produto.bind('<Return>', on_enter)  # Avançar com Enter

# Valor do IPI
ttk.Label(input_frame, text="Valor do IPI:").grid(row=5, column=0, sticky=tk.W, pady=5)
entry_valor_ipi = ttk.Entry(input_frame, width=20)
entry_valor_ipi.grid(row=5, column=1, sticky=tk.E, pady=5)
entry_valor_ipi.bind('<KeyRelease>', formatar_valor_dinamico)
entry_valor_ipi.bind('<Return>', on_enter)  # Avançar com Enter

# Valor do Frete
ttk.Label(input_frame, text="Valor do Frete:").grid(row=6, column=0, sticky=tk.W, pady=5)
entry_valor_frete = ttk.Entry(input_frame, width=20)
entry_valor_frete.grid(row=6, column=1, sticky=tk.E, pady=5)
entry_valor_frete.bind('<KeyRelease>', formatar_valor_dinamico)
entry_valor_frete.bind('<Return>', on_enter)  # Avançar com Enter

# Outras Despesas Acessórias
ttk.Label(input_frame, text="Outras Despesas Acessórias:").grid(row=7, column=0, sticky=tk.W, pady=5)
entry_outras_despesas = ttk.Entry(input_frame, width=20)
entry_outras_despesas.grid(row=7, column=1, sticky=tk.E, pady=5)
entry_outras_despesas.bind('<KeyRelease>', formatar_valor_dinamico)
entry_outras_despesas.bind('<Return>', on_enter)  # Avançar com Enter

# Descontos
ttk.Label(input_frame, text="Descontos:").grid(row=8, column=0, sticky=tk.W, pady=5)
entry_descontos = ttk.Entry(input_frame, width=20)
entry_descontos.grid(row=8, column=1, sticky=tk.E, pady=5)
entry_descontos.bind('<KeyRelease>', formatar_valor_dinamico)
entry_descontos.bind('<Return>', on_enter)  # Avançar com Enter

# Botão Calcular
btn_calcular = ttk.Button(input_frame, text="Calcular", command=calcular_difal)
btn_calcular.grid(row=9, column=0, pady=10)
btn_calcular.bind('<Return>', lambda e: calcular_difal())  # Executar cálculo ao pressionar Enter no botão

# Botão Limpar
btn_limpar = ttk.Button(input_frame, text="Limpar", command=limpar_campos)
btn_limpar.grid(row=9, column=1, pady=10)

# Frame para resultados
result_frame = ttk.LabelFrame(frame, text="Resultados", padding="10")
result_frame.grid(row=0, column=1, padx=10, pady=10, sticky=tk.N)

# Resultados
ttk.Label(result_frame, text="Valor da Operação:").grid(row=0, column=0, sticky=tk.W, pady=5)
label_resultado_valor_operacao = ttk.Label(result_frame, text="R$ 0,00", font=("Arial", 10, "bold"))
label_resultado_valor_operacao.grid(row=0, column=1, sticky=tk.E, pady=5)

ttk.Label(result_frame, text="Base de Cálculo 1:").grid(row=1, column=0, sticky=tk.W, pady=5)
label_resultado_base_calculo_1 = ttk.Label(result_frame, text="R$ 0,00", font=("Arial", 10, "bold"))
label_resultado_base_calculo_1.grid(row=1, column=1, sticky=tk.E, pady=5)

ttk.Label(result_frame, text="Base de Cálculo 2:").grid(row=2, column=0, sticky=tk.W, pady=5)
label_resultado_base_calculo_2 = ttk.Label(result_frame, text="R$ 0,00", font=("Arial", 10, "bold"))
label_resultado_base_calculo_2.grid(row=2, column=1, sticky=tk.E, pady=5)

ttk.Label(result_frame, text="Valor DIFAL:").grid(row=3, column=0, sticky=tk.W, pady=5)
label_resultado_difal = ttk.Label(result_frame, text="R$ 0,00", font=("Arial", 10, "bold"))
label_resultado_difal.grid(row=3, column=1, sticky=tk.E, pady=5)

ttk.Label(result_frame, text="Valor Fundo de Combate à Pobreza:").grid(row=4, column=0, sticky=tk.W, pady=5)
label_resultado_fcp = ttk.Label(result_frame, text="R$ 0,00", font=("Arial", 10, "bold"))
label_resultado_fcp.grid(row=4, column=1, sticky=tk.E, pady=5)

ttk.Label(result_frame, text="ICMS Interno:").grid(row=5, column=0, sticky=tk.W, pady=5)
label_resultado_icms_interno = ttk.Label(result_frame, text="R$ 0,00", font=("Arial", 10, "bold"))
label_resultado_icms_interno.grid(row=5, column=1, sticky=tk.E, pady=5)

ttk.Label(result_frame, text="ICMS Interestadual:").grid(row=6, column=0, sticky=tk.W, pady=5)
label_resultado_icms_interestadual = ttk.Label(result_frame, text="R$ 0,00", font=("Arial", 10, "bold"))
label_resultado_icms_interestadual.grid(row=6, column=1, sticky=tk.E, pady=5)

ttk.Label(result_frame, text="Base ICMS UF Destino:").grid(row=7, column=0, sticky=tk.W, pady=5)
label_resultado_base_icms_destino = ttk.Label(result_frame, text="R$ 0,00", font=("Arial", 10, "bold"))
label_resultado_base_icms_destino.grid(row=7, column=1, sticky=tk.E, pady=5)

ttk.Label(result_frame, text="Valor ICMS UF Destino:").grid(row=8, column=0, sticky=tk.W, pady=5)
label_resultado_valor_icms_destino = ttk.Label(result_frame, text="R$ 0,00", font=("Arial", 10, "bold"))
label_resultado_valor_icms_destino.grid(row=8, column=1, sticky=tk.E, pady=5)

ttk.Label(result_frame, text="Valor ICMS UF Emitente:").grid(row=9, column=0, sticky=tk.W, pady=5)
label_resultado_valor_icms_emitente = ttk.Label(result_frame, text="R$ 0,00", font=("Arial", 10, "bold"))
label_resultado_valor_icms_emitente.grid(row=9, column=1, sticky=tk.E, pady=5)

footer = tk.Label(text="Desenvolvido por Cristianfer", font=("Arial", 10))
footer.pack(pady=10)

# Ajuste final da janela
root.mainloop()