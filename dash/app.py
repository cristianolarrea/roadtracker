from dash import Dash, html, dcc, Input, Output
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import ThemeSwitchAIO
import plotly.express as px
import pandas as pd

# =====  Inicialização do Dash  ===== #
app = Dash(__name__, 
    external_stylesheets=[dbc.themes.CYBORG, "assets/style.css"], 
    meta_tags=[{"charset": "utf-8"}, {"name": "viewport", "content": "width=device-width, initial-scale=1"}])

server = app.server

app.title = 'RoadTracker'

# =========  Layout  =========== #
app.layout = html.Div([
    dbc.Container([
        dbc.Row([
            dbc.Col([
                html.H1("Road Tracker Dashboard", className="title"),
                html.Div(className="line")
            ])
        ]),
        dbc.Row([
            dbc.Col([
                dbc.Row([
                    dbc.Col([
                        dbc.Card(
                            [html.Div("6", className="card-text"),
                            html.P("rodovias monitoradas")]
                        )
                    ], md=3, sm=6, xs=6),
                    dbc.Col([
                        dbc.Card(
                            [html.Div("132", className="card-text"),
                            html.P("veículos monitorados")]
                        )
                    ], md=3, sm=6, xs=6),
                    dbc.Col([
                        dbc.Card(
                            [html.Div("10", className="card-text"),
                            html.P("veículos acima da velocidade")]
                        )
                    ], md=3, sm=6, xs=6),
                    dbc.Col([
                        dbc.Card(
                            [html.Div("8", className="card-text"),
                            html.P("veículos com risco de colisão")]
                        )
                    ], md=3, sm=6, xs=6)
                ]),
                dbc.Row([
                    dbc.Col([
                        dbc.Card(
                            [html.H1("Veículos acima do limite de velocidade")],
                            style={"height": "38vh"}
                        )
                    ])
                ]),
                dbc.Row([
                    dbc.Col([
                        dbc.Card(
                            [html.H1("Veículos com risco de colisão")],
                            style={"height": "38vh"}
                        )
                    ])
                ]),
            ]),

            dbc.Col([
                dbc.Row([
                    dbc.Col([
                        dbc.Card(
                            [html.H1("Top 100 veículos que passaram por mais rodovias")],
                            style={"height": "48vh"}
                        )
                    ])
                ]),
                dbc.Row([
                    dbc.Col([
                        dbc.Card(
                            [html.H1("Carros proibidos de circular")],
                            style={"height": "38vh"}
                        )
                    ])
                ])
            ], md=3, sm=6),

            dbc.Col([
                dbc.Card(
                    [html.H1("Estatísticas das Rodovias")],
                    style={"height": "90vh"}
                )
            ], md=3, sm=6)
        ]),
    ], fluid=True)
])

# ========  Callbacks  ========= #

# ========  Run server  ======== #
if __name__ == '__main__':
    app.run_server(debug=False)