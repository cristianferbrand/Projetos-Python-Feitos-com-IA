import dash
from dash import dcc, html, dash_table, Input, Output, State, ctx
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import os

# Função para carregar arquivos CSV com tratamento de erros
def carregar_csv(caminho_arquivo, required_columns=None):
    try:
        if not os.path.exists(caminho_arquivo):
            raise FileNotFoundError(f"Arquivo não encontrado: {caminho_arquivo}")
        df = pd.read_csv(caminho_arquivo, sep=";", encoding="utf-8")
        # Verificar se as colunas obrigatórias estão presentes
        if required_columns:
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"O arquivo CSV deve conter as colunas {missing_columns}")
        return df
    except Exception as e:
        print(f"Erro ao carregar o arquivo: {e}")
        exit()

# Caminhos dos arquivos
diretorio_script = os.path.dirname(os.path.abspath(__file__))
clientes_csv = os.path.join(diretorio_script, "clientes_hos.csv")
clientes_rep_csv = os.path.join(diretorio_script, "clientes_rep.csv")
modulos_csv = os.path.join(diretorio_script, "clientes_modulos.csv")
municipios_csv = os.path.join(diretorio_script, "municipios.csv")

# Carregar os dados
clientes_hos_df = carregar_csv(clientes_csv, required_columns=["CODIGO_IBGE", "CIDADE", "ESTADO", "FANTASIA"])
clientes_rep_df = carregar_csv(clientes_rep_csv, required_columns=["COD_REP", "NOME_REP", "QTD_CLIENTES"])
modulos_csv_df = carregar_csv(modulos_csv, required_columns=["MODULO", "QUANTIDADE"])
municipios_df = pd.read_csv(municipios_csv, encoding="utf-8")

# Totalizar clientes por estado
df_totalizado_estado = clientes_hos_df.groupby("ESTADO").size().reset_index(name="Clientes")

# Adicionar a coluna de UF ao totalizado por cidade
df_totalizado_cidade = clientes_hos_df.groupby(["CIDADE", "ESTADO"]).size().reset_index(name="Clientes")
df_totalizado_cidade.rename(columns={"ESTADO": "UF"}, inplace=True)

# Lista completa de estados brasileiros
todos_estados = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA",
    "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"
]

# Encontrar estados sem clientes
estados_sem_clientes = list(set(todos_estados) - set(df_totalizado_estado["ESTADO"]))

# Criar DataFrame para estados sem clientes
df_estados_sem_clientes = pd.DataFrame({"ESTADO": estados_sem_clientes})


# Total de clientes para o indicador
total_clientes = df_totalizado_estado["Clientes"].sum()
# Criar o gráfico de indicador
fig_indicator = go.Figure()
fig_indicator.add_trace(go.Indicator(
    mode="number",
    value=total_clientes,
    title={"text": "Total de Clientes"},
    number={"font": {"size": 50}},
))
fig_indicator.update_layout(
    height=100,
    margin=dict(t=10, b=10, l=10, r=10),
    paper_bgcolor="#007bff",
    font_color="white",
)

# Combinar os dados dos clientes com as coordenadas dos municípios
merged_df = pd.merge(
    clientes_hos_df, municipios_df,
    left_on="CODIGO_IBGE", right_on="municipio",
    how="left"
)

# Ordenar os dados pelo nome do município
merged_df = merged_df.sort_values(by="CIDADE", na_position="last")

# Obter lista de estados únicos
estados = merged_df["ESTADO"].unique()

# Expandir a paleta para cobrir mais de 24 estados, se necessário
palette = px.colors.qualitative.Alphabet * ((len(estados) // 24) + 1)
color_map = {estado: palette[i] for i, estado in enumerate(estados)}

# Função para criar o mapa
def criar_mapa(estado_selecionado=None):
    fig_mapa = go.Figure()

    # Ordenar os estados pelo total de clientes em ordem decrescente
    estados_ordenados = df_totalizado_estado.sort_values(by="Clientes", ascending=False)["ESTADO"]

    for estado in estados_ordenados:
        estado_data = merged_df[merged_df["ESTADO"] == estado]
        total_clientes_estado = estado_data["FANTASIA"].notna().sum()
        fig_mapa.add_trace(go.Scattermapbox(
            lat=estado_data["lat"],
            lon=estado_data["lon"],
            mode="markers", # Define o modo de exibição como marcadores (pontos)
            marker=dict(size=10, color=color_map[estado]), # Define o tamanho dos marcadores
            text=estado_data.apply(
                lambda row: f"Município: {row['CIDADE']}",
                axis=1
            ),
            hoverinfo="text",
            name=f"{estado} ({total_clientes_estado} Fármacias)"
        ))

    # Ajustar o zoom e o centro do mapa com base no estado selecionado
    if estado_selecionado and estado_selecionado in estados:
        estado_data = merged_df[merged_df["ESTADO"] == estado_selecionado]
        center_lat = estado_data["lat"].mean()
        center_lon = estado_data["lon"].mean()
        zoom = 5
    else:
        center_lat = -16.000
        center_lon = -54.000
        zoom = 3.3

    # Configurações do layout do mapa
    fig_mapa.update_layout(
        mapbox=dict(
            style="carto-positron",  # Estilo do mapa
            zoom=zoom,  # Nível de zoom
            center=dict(lat=center_lat, lon=center_lon)  # Centralização do mapa
        ),
        height=800,  # Altura do mapa
        margin=dict(r=30, t=30, l=30, b=30),  # Margens do mapa
        showlegend=True,  # Exibir a legenda
        paper_bgcolor="rgba(0,0,0,0)",  # Fundo transparente
        legend=dict(  # Configuração da legenda
            font=dict(
                size=15,  # Tamanho da fonte da legenda
                color="black"  # Cor do texto da legenda
            ),
            orientation="v",  # Orientação vertical (ou "h" para horizontal)
            x=1,  # Posição horizontal da legenda (1 = lado direito)
            y=1,  # Posição vertical da legenda (1 = topo)
            bgcolor="rgba(255,255,255,0.8)",  # Fundo semitransparente da legenda
            bordercolor="black",  # Cor da borda da legenda
            borderwidth=1,  # Largura da borda
        )
    )

    return fig_mapa

# Criar aplicação Dash
app = dash.Dash(__name__)

# Layout do Dash
app.layout = html.Div(
    style={"backgroundColor": "#f8f9fa", "height": "100vh", "display": "flex", "flexDirection": "column"},
    children=[
        # Indicador
        html.Div(
            children=dcc.Graph(figure=fig_indicator),
            style={"padding": "10px", "backgroundColor": "#ffffff", "boxShadow": "0 4px 8px rgba(0,0,0,0.1)"}
        ),
        # Tabelas e mapa
        html.Div(
            style={"flex": "1", "display": "flex"},
            children=[
                # Tabelas na lateral esquerda
                html.Div(
                    style={"width": "30%", "padding": "20px", "borderRight": "1px solid #dee2e6"},
                    children=[
                        # Tabela de Clientes por Representante
                        html.H4("Clientes por Representante", style={"textAlign": "center"}),
                        html.Button(
                            "Expandir Tabela",
                            id="expand-tabela-representantes",
                            n_clicks=0,
                            style={
                                "marginBottom": "10px",
                                "padding": "10px",
                                "backgroundColor": "#007bff",
                                "color": "white",
                                "border": "none",
                                "cursor": "pointer",
                                "borderRadius": "5px",
                            },
                        ),                        
                        dash_table.DataTable(
                            id="tabela-representantes",
                            data=clientes_rep_df.to_dict("records"),
                            columns=[
                                {"name": "Representante", "id": "NOME_REP"},
                                {"name": "Qtd. Clientes", "id": "QTD_CLIENTES"},
                            ],
                            style_table={"height": "50vh", "overflowY": "auto"},
                            style_cell={"textAlign": "center"},
                            style_header={
                                "backgroundColor": "#f8f9fa",
                                "fontWeight": "bold",
                                "textAlign": "center",
                            },
                        ),
                        # Tabela de Estados Sem HOS
                        html.H4("Estados Sem HOS", style={"textAlign": "center", "marginTop": "20px"}),
                        dash_table.DataTable(
                            id="tabela-estados-sem-hos",
                            data=df_estados_sem_clientes.to_dict("records"),
                            columns=[{"name": "Estado", "id": "ESTADO"}],
                            style_table={"height": "30vh", "overflowY": "auto"},
                            style_cell={"textAlign": "center"},
                            style_header={
                                "backgroundColor": "#f8f9fa",
                                "fontWeight": "bold",
                                "textAlign": "center",
                            },
                        ),
                    ],
                ),
                # Mapa
                html.Div(
                    style={"width": "60%", "padding": "20px", "borderRight": "1px solid #dee2e6"},
                    children=[dcc.Graph(id="mapa-clientes", figure=criar_mapa())],
                ),
                # Tabela de Módulos e Clientes por Cidade
                html.Div(
                    style={"width": "30%", "padding": "20px", "borderRight": "1px solid #dee2e6"},
                    children=[
                        # Tabela de Módulos com botão para expandir
                        html.H4("Módulos Utilizados", style={"textAlign": "center"}),
                        html.Button(
                            "Expandir Tabela",
                            id="expand-tabela-modulos",
                            n_clicks=0,
                            style={
                                "marginBottom": "10px",
                                "padding": "10px",
                                "backgroundColor": "#007bff",
                                "color": "white",
                                "border": "none",
                                "cursor": "pointer",
                                "borderRadius": "5px",
                            },
                        ),
                        dash_table.DataTable(
                            id="tabela-modulos",
                            data=modulos_csv_df.to_dict("records"),
                            columns=[
                                {"name": "Módulo", "id": "MODULO"},
                                {"name": "Quantidade", "id": "QUANTIDADE"},
                            ],
                            style_table={"height": "30vh", "overflowY": "auto"},
                            style_cell={"textAlign": "center"},
                            style_header={
                                "backgroundColor": "#f8f9fa",
                                "fontWeight": "bold",
                                "textAlign": "center",
                            },
                        ),
                        # Tabela de Clientes por Cidade
                        html.H4("Clientes por Cidade", style={"textAlign": "center", "marginTop": "20px"}),
                        html.Button(
                            "Expandir Tabela",
                            id="expand-tabela-cidades",
                            n_clicks=0,
                            style={
                                "marginBottom": "10px",
                                "padding": "10px",
                                "backgroundColor": "#007bff",
                                "color": "white",
                                "border": "none",
                                "cursor": "pointer",
                                "borderRadius": "5px",
                            },
                        ),
                        dash_table.DataTable(
                            id="tabela-cidade",
                            data=df_totalizado_cidade.to_dict("records"),
                            columns=[
                                {"name": "UF", "id": "UF"},
                                {"name": "Cidade", "id": "CIDADE"},
                                {"name": "Clientes", "id": "Clientes"},
                            ],
                            style_table={"height": "30vh", "overflowY": "auto"},
                            style_cell={"textAlign": "center"},
                            style_header={
                                "backgroundColor": "#f8f9fa",
                                "fontWeight": "bold",
                                "textAlign": "center",
                            },
                        ),
                    ],
                ),
            ],
        ),
        # Modal para exibição dos Clientes por Representante
        html.Div(
            id="modal-tabela-representantes",
            style={
                "display": "none",  # Inicialmente escondido
                "position": "fixed",
                "top": "0",
                "left": "0",
                "width": "100%",
                "height": "100%",
                "backgroundColor": "rgba(0,0,0,0.5)",
                "zIndex": "1000",
                "justifyContent": "center",
                "alignItems": "center",
            },
            children=[
                html.Div(
                    style={
                        "width": "90%",
                        "height": "90%",
                        "backgroundColor": "white",
                        "padding": "20px",
                        "overflow": "hidden",  # Prevenir estouros
                        "boxShadow": "0 4px 8px rgba(0,0,0,0.2)",
                        "borderRadius": "10px",
                    },
                    children=[
                        html.Button(
                            "Fechar",
                            id="close-modal-representantes",
                            style={
                                "marginBottom": "10px",
                                "padding": "10px",
                                "backgroundColor": "#dc3545",
                                "color": "white",
                                "border": "none",
                                "cursor": "pointer",
                                "borderRadius": "5px",
                            },
                        ),
                        html.Div(
                            style={
                                "height": "80%",
                                "overflowY": "auto",  # Barra de rolagem
                                "border": "1px solid #ccc",
                                "padding": "10px",
                                "borderRadius": "5px",
                            },
                            children=[
                                dash_table.DataTable(
                                    data=clientes_rep_df.to_dict("records"),
                                    columns=[
                                        {"name": "Representante", "id": "NOME_REP"},
                                        {"name": "Qtd. Clientes", "id": "QTD_CLIENTES"},
                                    ],
                                    style_table={"width": "100%"},
                                    style_cell={
                                        "textAlign": "center",
                                        "padding": "5px",
                                    },
                                    style_header={
                                        "backgroundColor": "#f8f9fa",
                                        "fontWeight": "bold",
                                        "textAlign": "center",
                                    },
                                    page_action="none",  # Desativa a paginação para exibir todos os dados
                                ),
                            ],
                        ),
                    ],
                )
            ],
        ),
        # Modal para exibição dos Módulos Utilizados
        html.Div(
            id="modal-tabela-modulos",
            style={
                "display": "none",  # Inicialmente escondido
                "position": "fixed",
                "top": "0",
                "left": "0",
                "width": "100%",
                "height": "100%",
                "backgroundColor": "rgba(0,0,0,0.5)",
                "zIndex": "1000",
                "justifyContent": "center",
                "alignItems": "center",
            },
            children=[
                html.Div(
                    style={
                        "width": "90%",
                        "height": "90%",
                        "backgroundColor": "white",
                        "padding": "20px",
                        "overflow": "hidden",  # Prevenir estouros
                        "boxShadow": "0 4px 8px rgba(0,0,0,0.2)",
                        "borderRadius": "10px",
                    },
                    children=[
                        html.Button(
                            "Fechar",
                            id="close-modal-modulos",
                            style={
                                "marginBottom": "10px",
                                "padding": "10px",
                                "backgroundColor": "#dc3545",
                                "color": "white",
                                "border": "none",
                                "cursor": "pointer",
                                "borderRadius": "5px",
                            },
                        ),
                        html.Div(
                            style={
                                "height": "80%",
                                "overflowY": "auto",  # Barra de rolagem
                                "border": "1px solid #ccc",
                                "padding": "10px",
                                "borderRadius": "5px",
                            },
                            children=[
                                dash_table.DataTable(
                                    data=modulos_csv_df.to_dict("records"),
                                    columns=[
                                        {"name": "Módulo", "id": "MODULO"},
                                        {"name": "Quantidade", "id": "QUANTIDADE"},
                                    ],
                                    style_table={"width": "100%"},
                                    style_cell={
                                        "textAlign": "center",
                                        "padding": "5px",
                                    },
                                    style_header={
                                        "backgroundColor": "#f8f9fa",
                                        "fontWeight": "bold",
                                        "textAlign": "center",
                                    },
                                    page_action="none",  # Desativa a paginação para exibir todos os dados
                                ),
                            ],
                        ),
                    ],
                )
            ],
        ),
        # Modal para exibição em tela cheia com barra de rolagem
        html.Div(
            id="modal-tabela-cidade",
            style={
                "display": "none",  # Inicialmente escondido
                "position": "fixed",
                "top": "0",
                "left": "0",
                "width": "100%",
                "height": "100%",
                "backgroundColor": "rgba(0,0,0,0.5)",
                "zIndex": "1000",
                "justifyContent": "center",
                "alignItems": "center",
            },
            children=[
                html.Div(
                    style={
                        "width": "90%",
                        "height": "90%",
                        "backgroundColor": "white",
                        "padding": "20px",
                        "overflow": "hidden",  # Prevenir estouros
                        "boxShadow": "0 4px 8px rgba(0,0,0,0.2)",
                        "borderRadius": "10px",
                    },
                    children=[
                        html.Button(
                            "Fechar",
                            id="close-modal-cidades",
                            style={
                                "marginBottom": "10px",
                                "padding": "10px",
                                "backgroundColor": "#dc3545",
                                "color": "white",
                                "border": "none",
                                "cursor": "pointer",
                                "borderRadius": "5px",
                            },
                        ),
                        html.Div(
                            style={
                                "height": "80%",
                                "overflowY": "auto",  # Barra de rolagem
                                "border": "1px solid #ccc",
                                "padding": "10px",
                                "borderRadius": "5px",
                            },
                            children=[
                                dash_table.DataTable(
                                    data=df_totalizado_cidade.to_dict("records"),
                                    columns=[
                                        {"name": "UF", "id": "UF"},
                                        {"name": "Cidade", "id": "CIDADE"},
                                        {"name": "Clientes", "id": "Clientes"},
                                    ],
                                    style_table={"width": "100%"},
                                    style_cell={
                                        "textAlign": "center",
                                        "padding": "5px",
                                    },
                                    style_header={
                                        "backgroundColor": "#f8f9fa",
                                        "fontWeight": "bold",
                                        "textAlign": "center",
                                    },
                                    page_action="none",  # Desativa a paginação para exibir todos os dados
                                ),
                            ],
                        ),
                    ],
                )
            ],
        ),
        # Script JavaScript embutido para modais
        html.Script(
            """
            document.addEventListener('DOMContentLoaded', function() {
                
                // Controle para o modal Clientes por Representante
                const expandButtonRepresentantes  = document.getElementById('expand-tabela-representantes');
                const modalRepresentantes = document.getElementById('modal-tabela-representantes');
                const closeButtonRepresentantes  = document.getElementById('close-modal-representantes');
                
                expandButtonRepresentantes.addEventListener('click', function() {
                    modalRepresentantes.style.display = 'flex';
                });
                
                closeButtonRepresentantes.addEventListener('click', function() {
                    modalRepresentantes.style.display = 'none';
                });            
                // Controle para o modal de Clientes por Cidade
                const expandButtonCidade = document.getElementById('expand-tabela-cidades');
                const modalCidade = document.getElementById('modal-tabela-cidade');
                const closeButtonCidade = document.getElementById('close-modal-cidades');
                
                expandButtonCidade.addEventListener('click', function() {
                    modalCidade.style.display = 'flex';
                });
                
                closeButtonCidade.addEventListener('click', function() {
                    modalCidade.style.display = 'none';
                });

                // Controle para o modal de Módulos Utilizados
                const expandButtonModulos = document.getElementById('expand-tabela-modulos');
                const modalModulos = document.getElementById('modal-tabela-modulos');
                const closeButtonModulos = document.getElementById('close-modal-modulos');
                
                expandButtonModulos.addEventListener('click', function() {
                    modalModulos.style.display = 'flex';
                });
                
                closeButtonModulos.addEventListener('click', function() {
                    modalModulos.style.display = 'none';
                });
            });
            """
        ),
    ],
)

# Variável global para armazenar o último estado selecionado
last_selected_state = {"estado": None}

# Callback para gerenciar zoom no mapa e exibição dos modais
@app.callback(
    [
        Output("mapa-clientes", "figure"), 
        Output("modal-tabela-representantes", "style"),
        Output("modal-tabela-cidade", "style"),
        Output("modal-tabela-modulos", "style"),
    ],
    [
        Input("mapa-clientes", "restyleData"),
        Input("expand-tabela-representantes", "n_clicks"),
        Input("close-modal-representantes", "n_clicks"),        
        Input("expand-tabela-cidades", "n_clicks"),
        Input("close-modal-cidades", "n_clicks"),
        Input("expand-tabela-modulos", "n_clicks"),
        Input("close-modal-modulos", "n_clicks"),
    ],
    [
        State("mapa-clientes", "figure"),
        State("modal-tabela-representantes", "style"),        
        State("modal-tabela-cidade", "style"),
        State("modal-tabela-modulos", "style"),
    ],
)
def handle_interactions(
    restyle_data, 
    n_expand_representantes, n_close_representantes, 
    n_expand_cidade, n_close_cidade, 
    n_expand_modulos, n_close_modulos, 
    current_figure, 
    current_modal_representantes_style, 
    current_modal_cidade_style, 
    current_modal_modulos_style
):
    global last_selected_state

    # Variáveis para gerenciar os estilos dos modais
    modal_representantes_style = current_modal_representantes_style or {"display": "none"}
    modal_cidade_style = current_modal_cidade_style or {"display": "none"}
    modal_modulos_style = current_modal_modulos_style or {"display": "none"}

    # Verificar o gatilho da interação
    triggered_id = ctx.triggered_id

    # Caso o botão de expandir tabela de representantes seja clicado
    if triggered_id == "expand-tabela-representantes":
        modal_representantes_style = {
            "display": "flex",
            "position": "fixed",
            "top": "0",
            "left": "0",
            "width": "100%",
            "height": "100%",
            "backgroundColor": "rgba(0,0,0,0.5)",
            "zIndex": "1000",
            "justifyContent": "center",
            "alignItems": "center",
        }
        return current_figure, modal_representantes_style, modal_cidade_style, modal_modulos_style

    # Caso o botão de fechar modal de representantes seja clicado
    if triggered_id == "close-modal-representantes":
        modal_representantes_style = {"display": "none"}
        return current_figure, modal_representantes_style, modal_cidade_style, modal_modulos_style

    # Caso o botão de expandir tabela de cidades seja clicado
    if triggered_id == "expand-tabela-cidades":
        modal_cidade_style = {
            "display": "flex",
            "position": "fixed",
            "top": "0",
            "left": "0",
            "width": "100%",
            "height": "100%",
            "backgroundColor": "rgba(0,0,0,0.5)",
            "zIndex": "1000",
            "justifyContent": "center",
            "alignItems": "center",
        }
        return current_figure, modal_representantes_style, modal_cidade_style, modal_modulos_style

    # Caso o botão de fechar modal de cidades seja clicado
    if triggered_id == "close-modal-cidades":
        modal_cidade_style = {"display": "none"}
        return current_figure, modal_representantes_style, modal_cidade_style, modal_modulos_style

    # Caso o botão de expandir tabela de módulos seja clicado
    if triggered_id == "expand-tabela-modulos":
        modal_modulos_style = {
            "display": "flex",
            "position": "fixed",
            "top": "0",
            "left": "0",
            "width": "100%",
            "height": "100%",
            "backgroundColor": "rgba(0,0,0,0.5)",
            "zIndex": "1000",
            "justifyContent": "center",
            "alignItems": "center",
        }
        return current_figure, modal_representantes_style, modal_cidade_style, modal_modulos_style

    # Caso o botão de fechar modal de módulos seja clicado
    if triggered_id == "close-modal-modulos":
        modal_modulos_style = {"display": "none"}
        return current_figure, modal_representantes_style, modal_cidade_style, modal_modulos_style

    # Caso o zoom no mapa seja acionado via restyleData
    if triggered_id == "mapa-clientes" and restyle_data:
        if "name" in current_figure["data"][restyle_data[1][0]]:
            # Extrair o nome do trace (nome do estado)
            estado = current_figure["data"][restyle_data[1][0]]["name"].split(" ")[0]

            # Verificar se o estado clicado é o mesmo da última interação
            if last_selected_state["estado"] == estado:
                # Resetar o estado para voltar ao mapa completo
                last_selected_state["estado"] = None
                return criar_mapa(), modal_representantes_style, modal_cidade_style, modal_modulos_style

            # Atualizar o último estado selecionado e aplicar zoom
            last_selected_state["estado"] = estado
            return criar_mapa(estado), modal_representantes_style, modal_cidade_style, modal_modulos_style

    # Retornar mapa completo e estado atual dos modais caso nenhuma interação válida ocorra
    return criar_mapa(), modal_representantes_style, modal_cidade_style, modal_modulos_style

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8050)