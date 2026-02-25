import os
from tkinter import Tk, Label, Button, filedialog, messagebox
from docling.document_converter import DocumentConverter

def process_file():
    # Abrir um diálogo para selecionar o arquivo
    file_path = filedialog.askopenfilename(
        title="Selecione o arquivo",
        filetypes=(("Arquivos Word", "*.docx"), ("Todos os arquivos", "*.*"))
    )
    if not file_path:
        return  # Se o usuário cancelar, não faz nada

    try:
        # Inicializar o Docling
        converter = DocumentConverter()

        # Converter o documento
        result = converter.convert(file_path)

        # Exportar o resultado para Markdown
        output_path = os.path.join(
            os.path.dirname(file_path),
            f"tabelas_extraidas_{os.path.basename(file_path)}.md"
        )
        with open(output_path, "w", encoding="utf-8") as file:
            file.write(result.document.export_to_markdown())

        # Notificar sucesso
        messagebox.showinfo("Sucesso", f"Tabelas extraídas para: {output_path}")
    except Exception as e:
        # Notificar erro
        messagebox.showerror("Erro", f"Falha ao processar o arquivo: {e}")

# Configuração da interface gráfica
root = Tk()
root.title("Docling - Extrator de Tabelas")
root.geometry("400x200")

label = Label(root, text="Extrair tabelas de um arquivo Word (.docx)", font=("Arial", 12))
label.pack(pady=20)

button = Button(root, text="Selecionar arquivo e processar", command=process_file, font=("Arial", 12), bg="#4CAF50", fg="white")
button.pack(pady=20)

root.mainloop()