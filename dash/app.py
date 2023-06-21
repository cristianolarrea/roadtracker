from dash import Dash, html, dcc, Input, Output, callback, dash_table
import dash_bootstrap_components as dbc
import pandas as pd
from pymongo import MongoClient

MONGO_URL = 'mongodb://localhost:27017'
database = 'roadtracker'
client = MongoClient(MONGO_URL)
db = client[database]

# =====  Inicialização do Dash  ===== #
app = Dash(__name__, 
    external_stylesheets=[dbc.themes.CYBORG, "assets/style.css", dbc.icons.FONT_AWESOME], 
    meta_tags=[{"charset": "utf-8"}, {"name": "viewport", "content": "width=device-width, initial-scale=1"}])

server = app.server
#df = pd.read_parquet('../results/analysis.parquet')

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
                            [html.H1("Veículos acima do limite de velocidade"),
                             html.Div(id='list_above_limit'),
                             dcc.Interval(
                                    id='list_above_limit_interval',
                                    interval = 500,
                                    n_intervals=0)],
                            style={"height": "36vh"}
                        )
                    ])
                ]),
                dbc.Row([
                    dbc.Col([
                        dbc.Card(
                            [html.H1("Veículos com risco de colisão"),
                             html.Div(id='list_collision_risk'),
                             dcc.Interval(
                                    id='list_collision_risk_interval',
                                    interval = 500,
                                    n_intervals=0)],
                            style={"height": "38vh"}
                        )
                    ])
                ]),
            ]),

            dbc.Col([
                dbc.Row([
                    dbc.Col([
                        dbc.Card(
                            [html.H1("Top 100 veículos que passaram por mais rodovias"),
                             html.Div(id='list_top100'),
                             dcc.Interval(
                                    id='list_top100_interval',
                                    interval = 500,
                                    n_intervals=0)],
                            style={"height": "48vh"}
                        )
                    ])
                ]),
                dbc.Row([
                    dbc.Col([
                        dbc.Card(
                            [html.H1("Carros proibidos de circular"),
                             html.Div(id='list_prohibited'),
                             dcc.Interval(
                                    id='list_prohibited_interval',
                                    interval = 500,
                                    n_intervals=0)],
                            style={"height": "38vh"}
                        )
                    ])
                ])
            ], md=3, sm=6),

            dbc.Col([
                dbc.Row([
                    dbc.Col([
                        dbc.Card(
                            [html.H1("Estatísticas das Rodovias"),
                                     html.Div(id='list_road_stats'),
                                     dcc.Interval(
                                            id='list_road_stats_interval',
                                            interval = 500,
                                            n_intervals=0)],
                            style={"height": "48vh"}
                        )
                    ])
                ]),
                dbc.Row([
                    dbc.Col([
                        dbc.Card(
                            [html.H1("Tempos das Análises"),
                             html.Div(id='list_times'),
                             dcc.Interval(
                                 id='list_times_interval',
                                 interval = 500,
                                 n_intervals=0)],
                            style={"height": "38vh"}
                        )
                    ])
                ])
            ], md=4, sm=6)
        ]),
    ], fluid=True)
])

# ========  Callbacks  ========= #

######### ANALISE 1 #########
@callback(Output('n_roads', 'children'),
          Input('n_roads_interval', 'n_intervals'))
def update_n_roads(n):
    collection = "analysis1"
    coll = db[collection]
    df = pd.DataFrame(list(coll.find()))
    return [html.Span(df['n_roads'])]

######### ANALISE 2 #########
@callback(Output('n_veiculos', 'children'),
          Input('n_veiculos_interval', 'n_intervals'))
def update_n_veiculos(n):
    collection = "analysis2"
    coll = db[collection]
    df = pd.DataFrame(list(coll.find()))
    return [html.Span(df['n_cars'])]

######### ANALISE 3 #########
@callback(Output('n_above_limit', 'children'),
          Input('n_above_limit_interval', 'n_intervals'))
def update_n_above_limit(n):
    collection = "analysis3"
    coll = db[collection]
    df = pd.DataFrame(list(coll.find()))
    return [html.Span(df['cars_over_speed_limit'])]

######### ANALISE 4 #########
@callback(Output('n_colision_risk', 'children'),
          Input('n_colision_risk_interval', 'n_intervals'))
def update_n_colision_risk(n):
    collection = "analysis4"
    coll = db[collection]
    df = pd.DataFrame(list(coll.find()))
    return [html.Span(df['cars_collision_risk'])]
def display_data_table(df,height):
    data = df.to_dict('records')
    table = dash_table.DataTable(
         data,
         [{"name": i, "id": i} for i in df.columns],
         fixed_rows={'headers': True},
         style_table={'height': height, 'overflowY': 'auto'},
         style_header={
             'backgroundColor': 'rgb(30, 30, 30)',
             'color': 'white',
             'font-size': '12px'
         },
         style_data={
             'backgroundColor': 'rgb(50, 50, 50)',
             'color': 'white',
             'font-size': '12px'
         })
    return table

######### ANALISE 5 #########
@callback(Output('list_above_limit', 'children'),
          Input('list_above_limit_interval', 'n_intervals'))
def update_n_above_limit(n):
    coll = db["analysis5"]
    df = pd.DataFrame(list(coll.find()))
    df = df.drop('_id', axis=1)
    df['collision_risk'].mask(df['collision_risk'] == 1, "Yes", inplace=True)
    df['collision_risk'].mask(df['collision_risk'] == 0, "No", inplace=True)
    return display_data_table(df, "29vh")

######### ANALISE 6 #########
@callback(Output('list_collision_risk', 'children'),
          Input('list_collision_risk_interval', 'n_intervals'))
def update_risk_collision(n):
    coll = db["analysis6"]
    df = pd.DataFrame(list(coll.find()))
    df = df.drop('_id', axis=1)
    return display_data_table(df, "32vh")

######### HISTORICA 1 #########
@callback(Output('list_top100', 'children'),
          Input('list_top100_interval', 'n_intervals'))
def update_top_100(n):
    coll = db["historical1"]
    df = pd.DataFrame(list(coll.find()))
    df = df.drop('_id', axis=1)
    return display_data_table(df, "38vh")

######### HISTORICA 2 #########
@callback(Output('list_road_stats', 'children'),
          Input('list_road_stats_interval', 'n_intervals'))
def update_roads_stats(n):
    coll = db["historical2"]
    df = pd.DataFrame(list(coll.find()))
    df = df.drop('_id', axis=1)
    df['avg_speed'] = df['avg_speed'].round(2)
    df['avg_time_to_cross'] = df['avg_time_to_cross'].round(2)
    return display_data_table(df, "40vh")

######### HISTORICA 3 #########
@callback(Output('list_prohibited', 'children'),
          Input('list_prohibited_interval', 'n_intervals'))
def update_prohibited(n):
    coll = db["historical3"]
    df = pd.DataFrame(list(coll.find()))
    df = df.drop('_id', axis=1)
    return display_data_table(df, "32vh")

######### TEMPO ANALISES #########
@callback(Output('list_times', 'children'),
          Input('list_times_interval', 'n_intervals'))
def update_times(n):
    coll = db["times"]
    df = pd.DataFrame(list(coll.find()))
    df = df.drop('_id', axis=1)
    return display_data_table(df, "32vh")

######### ANALISE ALTERNATIVA #########

# ========  Run server  ======== #
if __name__ == '__main__':
    app.run_server(debug=False)