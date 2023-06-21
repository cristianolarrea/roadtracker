from dash import Dash, html, dcc, Input, Output, callback
import dash_bootstrap_components as dbc
import pandas as pd
from pymongo import MongoClient

MONGO_URL = 'mongodb://localhost:27017'
database = 'roadtracker'
client = MongoClient(MONGO_URL)
db = client[database]
<<<<<<< HEAD
=======

collection = 'analysis3'
coll = db[collection]
df = pd.DataFrame(list(coll.find()))
print(df)
>>>>>>> 733b77a25127ef8e59ad7a2a9d762a2e9188f721

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
                            style={"height": "38vh"}
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
                dbc.Card(
                    [html.H1("Estatísticas das Rodovias"),
                             html.Div(id='list_roadstats'),
                             dcc.Interval(
                                    id='list_roadstats_interval',
                                    interval = 500,
                                    n_intervals=0)],
                    style={"height": "90vh"}
                )
            ], md=3, sm=6)
        ]),
    ], fluid=True)
])

# ========  Callbacks  ========= #

######### ANALISE 1 #########
@callback(Output('n_roads', 'children'),
          Input('n_roads_interval', 'n_intervals'))
def update_n_roads(n):
<<<<<<< HEAD
    collection = "analysis1"
    coll = db[collection]
    df = pd.DataFrame(list(coll.find()))
    return [html.Span(df['n_roads'])]
=======
    collection = 'analysis1'
    coll = db[collection]
    df = pd.DataFrame(list(coll.find()))
    #df = pd.read_parquet('../results/analysis.parquet')
    return [html.Span(df['n_roads'][0])]
>>>>>>> 733b77a25127ef8e59ad7a2a9d762a2e9188f721

######### ANALISE 2 #########
@callback(Output('n_veiculos', 'children'),
          Input('n_veiculos_interval', 'n_intervals'))
def update_n_veiculos(n):
<<<<<<< HEAD
    collection = "analysis2"
    coll = db[collection]
    df = pd.DataFrame(list(coll.find()))
    return [html.Span(df['n_cars'])]
=======
    collection = 'analysis2'
    coll = db[collection]    
    df = pd.DataFrame(list(coll.find()))
    #df = pd.read_parquet('../results/analysis.parquet')
    return [html.Span(df['n_cars'][0])]
>>>>>>> 733b77a25127ef8e59ad7a2a9d762a2e9188f721

######### ANALISE 3 #########
@callback(Output('n_above_limit', 'children'),
          Input('n_above_limit_interval', 'n_intervals'))
def update_n_above_limit(n):
<<<<<<< HEAD
    collection = "analysis3"
    coll = db[collection]
    df = pd.DataFrame(list(coll.find()))
    return [html.Span(df['cars_over_speed_limit'])]
=======
    collection = 'analysis3'
    coll = db[collection]
    df = pd.DataFrame(list(coll.find()))
    #df = pd.read_parquet('../results/analysis.parquet')
    return [html.Span(df['n_cars_over_speed_limit'][0])]
>>>>>>> 733b77a25127ef8e59ad7a2a9d762a2e9188f721

######### ANALISE 4 #########
@callback(Output('n_colision_risk', 'children'),
          Input('n_colision_risk_interval', 'n_intervals'))
def update_n_colision_risk(n):
<<<<<<< HEAD
    collection = "analysis4"
    coll = db[collection]
    df = pd.DataFrame(list(coll.find()))
    return [html.Span(df['cars_collision_risk'])]

######### ANALISE 5 #########
@callback(Output('list_above_limit', 'children'),
          Input('list_above_limit_interval', 'n_intervals'))
def update_n_above_limit(n):
    collection = "analysis5"
    coll = db[collection]
    df = pd.DataFrame(list(coll.find()))
    # remove _id column
    df = df.drop('_id', axis=1)
    df = df.to_string(index=False, header=False)
    # add line breaks
    df = df.replace('\n', "<br>")
    return [html.Span(df)]

######### ANALISE 6 #########
@callback(Output('list_collision_risk', 'children'),
          Input('list_collision_risk_interval', 'n_intervals'))
def update_n_above_limit(n):
    collection = "analysis6"
    coll = db[collection]
    df = pd.DataFrame(list(coll.find()))
    # remove _id column
    df = df.drop('_id', axis=1)
    df = df.to_string(index=False, header=False)
    # add line breaks
    df = df.replace('\n', "<br>")
    return [html.Span(df)]

######### HISTORICA 1 #########
@callback(Output('list_top100', 'children'),
          Input('list_top100_interval', 'n_intervals'))
def update_n_above_limit(n):
    collection = "historical1"
    coll = db[collection]
    df = pd.DataFrame(list(coll.find()))
    # remove _id column
    df = df.drop('_id', axis=1)
    df = df.to_string(index=False, header=False)
    # add line breaks
    df = df.replace('\n', "<br>")
    return [html.Span(df)]

######### HISTORICA 2 #########
@callback(Output('list_roadstats', 'children'),
          Input('list_roadstats_interval', 'n_intervals'))
def update_n_above_limit(n):
    collection = "historical2"
    coll = db[collection]
    df = pd.DataFrame(list(coll.find()))
    # remove _id column
    df = df.drop('_id', axis=1)
    df = df.to_string(index=False, header=False)
    # add line breaks
    df = df.replace('\n', "<br>")
    return [html.Span(df)]

######### HISTORICA 3 #########
@callback(Output('list_prohibited', 'children'),
          Input('list_prohibited_interval', 'n_intervals'))
def update_n_above_limit(n):
    collection = "historical3"
    coll = db[collection]
    df = pd.DataFrame(list(coll.find()))
    # remove _id column
    df = df.drop('_id', axis=1)
    df = df.to_string(index=False, header=False)
    # add line breaks
    df = df.replace('\n', "<br>")
    return [html.Span(df)]

######### ANALISE ALTERNATIVA #########
=======
    collection = 'analysis4'
    coll = db[collection]
    df = pd.DataFrame(list(coll.find()))
    #df = pd.read_parquet('../results/analysis.parquet')
    return [html.Span(df['n_cars_collision_risk'][0])]

>>>>>>> 733b77a25127ef8e59ad7a2a9d762a2e9188f721

# ========  Run server  ======== #
if __name__ == '__main__':
    app.run_server(debug=False)