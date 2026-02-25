import dash
from dash import dcc, html, Input, Output, State
import pandas as pd
import os

# Função para carregar arquivos CSV com tratamento de erros
def carregar_csv(caminho_arquivo):
    try:
        if not os.path.exists(caminho_arquivo):
            raise FileNotFoundError(f"Arquivo não encontrado: {caminho_arquivo}")
        df = pd.read_csv(caminho_arquivo, sep=";", encoding="utf-8", on_bad_lines="skip")
        df.columns = df.columns.str.upper()  # Padronizar os nomes das colunas para maiúsculas
        if "DIAS_LIBERADOS" in df.columns:
            df["DIAS_LIBERADOS"] = pd.to_numeric(df["DIAS_LIBERADOS"], errors="coerce").fillna(0).astype(int)
        return df
    except Exception as e:
        print(f"Erro ao carregar o arquivo: {e}")
        return pd.DataFrame([{"ERRO": f"Erro ao carregar os dados: {e}"}])

# Caminho para salvar os arquivos CSV na mesma pasta do script
script_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(script_dir, 'clientes_hos.csv')

# Carregar os dados
df = carregar_csv(file_path)

# Verificar se o DataFrame contém erros ou está vazio
if not df.empty and "ERRO" not in df.columns:
    colunas = df.columns.tolist()
else:
    colunas = ["ERRO"]

# Inicializar o aplicativo Dash
app = dash.Dash(__name__)
app.title = "CRM Clientes HOS"

# Layout do aplicativo
app.layout = html.Div(
    style={"backgroundColor": "#f8f9fa", "padding": "20px"},
    children=[
        html.H1("CRM Clientes HOS", style={"textAlign": "center", "color": "#007bff"}),

        html.Div(
            style={"textAlign": "center", "marginBottom": "20px"},
            children=[
                html.Label("Pesquisa de Cliente: "),
                dcc.Input(
                    id="pesquisa-input", 
                    type="text", 
                    placeholder="Digite o termo de pesquisa", 
                    style={
                        "width": "300px",   # Largura ajustada
                        "height": "30px",   # Altura ajustada
                        "marginRight": "10px",
                        "padding": "5px",   # Espaçamento interno
                        "border": "1px solid #ced4da",  # Borda estilizada
                        "borderRadius": "5px",          # Borda arredondada
                        "fontSize": "16px"              # Tamanho do texto
                    }
                ),
                html.Button(
                    "Pesquisar", 
                    id="pesquisar-button", 
                    n_clicks=0, 
                    style={
                        "backgroundColor": "#007bff", 
                        "color": "white", 
                        "border": "none", 
                        "padding": "10px 20px", 
                        "cursor": "pointer",
                        "borderRadius": "5px"
                    }
                )
            ]
        ),

        html.Div(id="indicadores-clientes", style={"marginTop": "20px"})
    ]
)

# Callback para filtrar os dados com base na pesquisa e exibir indicadores
@app.callback(
    Output("indicadores-clientes", "children"),
    [Input("pesquisar-button", "n_clicks")],
    [State("pesquisa-input", "value")]
)
def atualizar_indicadores(n_clicks, query):
    if n_clicks is None or n_clicks == 0 or not query:
        return html.P("Digite um termo de pesquisa e clique em Pesquisar.", style={"color": "#6c757d", "textAlign": "center"})

    resultados = df[df.apply(lambda row: row.astype(str).str.contains(query, case=False).any(), axis=1)]

    if resultados.empty:
        return html.P("Nenhum resultado encontrado.", style={"color": "#dc3545", "textAlign": "center"})

    indicadores = []
    for _, cliente in resultados.iterrows():
        indicadores.append(
            html.Div(
                style={"display": "flex", "justifyContent": "space-between", "marginBottom": "20px"},
                children=[
                    html.Div(
                        style={
                            "border": "1px solid #dee2e6", 
                            "borderRadius": "5px", 
                            "padding": "10px", 
                            "width": "48%", 
                            "backgroundColor": "#ffffff"
                        },
                        children=[
                            html.H4(f"{cliente['FANTASIA']}", style={"color": "#007bff"}),
                            html.P(f"CRM: {cliente['CRM']}", style={"marginBottom": "5px"}),
                            html.P(f"CNPJ: {cliente['CNPJ']}", style={"marginBottom": "5px"}),
                            html.P(f"Telefone: {cliente['TELEFONE']}", style={"marginBottom": "5px"}),
                            html.P(f"Cidade: {cliente['CIDADE']}", style={"marginBottom": "5px"}),
                            html.P(f"Endereço: {cliente['ENDERECO']}", style={"marginBottom": "5px"}),
                            html.P(f"Bairro: {cliente['BAIRRO']}", style={"marginBottom": "5px"}),
                            html.P(f"CEP: {cliente['CEP']}", style={"marginBottom": "5px"}),
                            html.P(f"Estado: {cliente['ESTADO']}", style={"marginBottom": "5px"}),
                            html.P(f"Cliente ativo: {cliente['ATIVO']}", style={"marginBottom": "5px"}),
                            html.P(f"Dias Liberados: {cliente['DIAS_LIBERADOS']}", style={"marginBottom": "5px"}),
                        ]
                    ),
                    html.Div(
                        style={
                            "border": "1px solid #dee2e6", 
                            "borderRadius": "5px", 
                            "padding": "10px", 
                            "width": "48%", 
                            "backgroundColor": "#ffffff"
                        },
                        children=[
                            html.H4("Módulos / N° de Estações", style={"color": "#007bff"}),
                            html.Ul(
                                [html.Li(modulo.strip(), style={"marginBottom": "5px"}) for modulo in cliente['MODULOS'].split(',')]
                            )
                        ]
                    )
                ]
            )
        )

    return indicadores

# Executar o servidor
if __name__ == "__main__":
    app.run_server(debug=True, host="0.0.0.0", port=8051)