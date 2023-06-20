from dash import Dash, html, dcc, Input, Output, callback
import dash_bootstrap_components as dbc
from pymongo import MongoClient
import pandas as pd

MONGO_URL = 'mongodb://localhost:27017'
DATABASE_NAME = 'roadtracker'

# =====  Inicialização do Dash  ===== #
app = Dash(__name__, 
    external_stylesheets=[dbc.themes.CYBORG, "assets/style.css", dbc.icons.FONT_AWESOME], 
    meta_tags=[{"charset": "utf-8"}, {"name": "viewport", "content": "width=device-width, initial-scale=1"}])

server = app.server

app.title = 'RoadTracker'

# =========  Layout  =========== #
app.layout = html.Div([
    dbc.Container([
        dbc.Row([
            dbc.Col([ 
                html.H1([html.I(className="fa-solid fa-car"), " | Road Tracker Dashboard"], className="title"),
                html.Div(className="line")
            ])
        ]),
        dbc.Row([
            dbc.Col([
                dbc.Row([
                    dbc.Col([
                        dbc.Card(
                            [
                                html.Div(className="card-text", id='n_roads'),
                                html.P("rodovias monitoradas"),
                                dcc.Interval(
                                    id='n_roads_interval',
                                    interval=500, #ms
                                    n_intervals=0
                                )
                            ]
                        )
                    ], md=3, sm=6, xs=6),
                    dbc.Col([
                        dbc.Card(
                            [
                            html.Div(className="card-text", id='n_veiculos'),
                            html.P("veículos monitorados"),
                            dcc.Interval(
                                    id='n_veiculos_interval',
                                    interval=500, #ms
                                    n_intervals=0
                                )
                            ]
                        )
                    ], md=3, sm=6, xs=6),
                    dbc.Col([
                        dbc.Card(
                            [
                                html.Div(id='n_above_limit', className="card-text"),
                            html.P("veículos acima da velocidade"),
                            dcc.Interval(
                                    id='n_above_limit_interval',
                                    interval=500, #ms
                                    n_intervals=0
                                )
                            ]
                        )
                    ], md=3, sm=6, xs=6),
                    dbc.Col([
                        dbc.Card(
                            [html.Div(id='n_colision_risk', className="card-text"),
                            html.P("veículos com risco de colisão"),
                            dcc.Interval(
                                    id='n_colision_risk_interval',
                                    interval=500, #ms
                                    n_intervals=0
                                )
                            ]
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
                            [
                                html.div(id='vehicles_colision_risk'),
                                html.H1("Veículos com risco de colisão"),
                                dcc.Interval(
                                    id='vehicles_colision_risk_interval',
                                    interval=500, #ms
                                    n_intervals=0
                                )
                            ],
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
def getMostRecentDocument(database, collection):
    client = MongoClient(MONGO_URL)
    db = client[database]
    coll = db[collection]
    return coll.find({})

@callback(Output('n_roads', 'children'),
          Input('n_roads_interval', 'n_intervals'))
def update_n_roads(n):
    # dict = getMostRecentDocument(DATABASE_NAME, '')
    # return [html.Span(dict[''])]
    return 32

@callback(Output('n_veiculos', 'children'),
          Input('n_veiculos_interval', 'n_intervals'))
def update_n_veiculos(n):
    # df = pd.read_parquet('../results/analysis.parquet')
    # return [html.Span(df['n_veiculos'])]
    return 32

@callback(Output('n_above_limit', 'children'),
          Input('n_above_limit_interval', 'n_intervals'))
def update_n_above_limit(n):
    # df = pd.read_parquet('../results/analysis.parquet')
    # return [html.Span(df['n_above_limit'])]
    return 32

@callback(Output('n_colision_risk', 'children'),
          Input('n_colision_risk_interval', 'n_intervals'))
def update_n_colision_risk(n):
    # df = pd.read_parquet('../results/analysis.parquet')
    # return [html.Span(df['n_colision_risk'])]
    return 32

@callback(Output('vehicle_colision_risk', 'children'),
          Input('vehicle_colision_risk_interval', 'n_intervals'))
def update_vehicles_colision_risk(n):
    registers = getMostRecentDocument(MONGO_URL, 'colissionRisk.json')
    print(registers)
    return "heyhey"

# ========  Run server  ======== #
if __name__ == '__main__':
    app.run_server(debug=False)