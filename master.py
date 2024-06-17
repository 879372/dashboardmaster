import dash
from dash import html, dcc, Input, Output, State, callback_context
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import mysql.connector
from datetime import datetime, date, timedelta
import datetime
import locale
from flask import Flask
from waitress import serve
from dash_bootstrap_templates import ThemeSwitchAIO
from dash.exceptions import PreventUpdate
import threading
import hashlib
import os
from dash import dash_table
# from dotenv import load_dotenv
# load_dotenv()

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)

tab_card = {'height': '100%'}

main_config = {
    "hovermode": "x unified",
    "legend": {"yanchor":"top",
                "y":0.9,
                "xanchor":"left",
                "x":0.1,
                "title": {"text": None},
                "font" :{"color":"white"},
                "bgcolor": "rgba(0,0,0,0.5)"},
    "margin": {"l":10, "r":10, "t":50, "b":3}
}

config_graph={"displayModeBar": False, "showTips": False}

template_theme1 = "flatly"
template_theme2 = "darkly"
url_theme1 = dbc.themes.FLATLY
url_theme2 = dbc.themes.DARKLY
lock = threading.Lock()

# host = os.environ['host']
# user = os.environ['user']
# password = os.environ['senha']
# database = os.environ['database']


def obter_dados_firebird():
    conexao = mysql.connector.connect(
            host='localhost',
            user='ideia',
            password='Ideia@2017',
            database='modapay'

    )

    query = """
        SELECT DAY(c.data_pagamento) AS DIA, 
               MONTH(c.data_pagamento) AS MES, 
               YEAR(c.data_pagamento) AS ANO, 
               e.Fantasia,
               e.status as ativo,
               c.`status`,
               'PIX_IN',
               c.valor,
               c.taxa_total,
               c.valor_sem_taxa AS VALOR_MENOS_TAXA,
               ws.SALDO as saldo_atual
        FROM cobranca c
        INNER JOIN empresa e ON c.fk_empresa = e.codigo
        LEFT JOIN 
            wl_saldo ws ON c.fk_empresa = ws.codigo
        UNION ALL

        SELECT DAY(s.data_solicitacao) AS DIA, 
               MONTH(s.data_solicitacao) AS MES, 
               YEAR(s.data_solicitacao) AS ANO, 
               e.Fantasia,
               e.status as ativo,
               s.status,
               'PIX_OUT',
               s.valor_solicitado,
               s.taxa_total,
               s.valor_sem_taxa AS VALOR_MENOS_TAXA,
               ws.SALDO as saldo_atual
        FROM saque s
        INNER JOIN empresa e ON s.fk_empresa = e.codigo
        LEFT JOIN 
         wl_saldo ws ON s.fk_empresa = ws.codigo;
        """

    df = pd.read_sql(query, conexao)
    conexao.close()
    return df

def criacao():
    conexao = mysql.connector.connect(
            host='localhost',
            user='ideia',
            password='Ideia@2017',
            database='modapay'
    )

    query2 = """
    
        SELECT DAY(c.data_dia) AS DIA_CRIACAO, 
            MONTH(c.data_dia) AS MES_CRIACAO, 
            YEAR(c.data_dia) AS ANO_CRIACAO
        FROM cobranca c
        INNER JOIN empresa e ON c.fk_empresa = e.codigo

        UNION ALL

        SELECT DAY(s.data_solicitacao) AS DIA_CRIACAO, 
            MONTH(s.data_solicitacao) AS MES_CRIACAO, 
            YEAR(s.data_solicitacao) AS ANO_CRIACAO
        FROM saque s
        INNER JOIN empresa e ON s.fk_empresa = e.codigo;

        """
    df2 = pd.read_sql(query2, conexao)
    conexao.close()
    return df2

df = obter_dados_firebird()
df_cru = df
df2 = criacao()

def convert_to_text(month):
    match month:
        case 0:
            x = 'MÊS ATUAL'
        case 1:
            x = 'JAN'
        case 2:
            x = 'FEV'
        case 3:
            x = 'MAR'
        case 4:
            x = 'ABR'
        case 5:
            x = 'MAI'
        case 6:
            x = 'JUN'
        case 7:
            x = 'JUL'
        case 8:
            x = 'AGO'
        case 9:
            x = 'SET'
        case 10:
            x = 'OUT'
        case 11:
            x = 'NOV'
        case 12:
            x = 'DEZ'
    return x

def create_status_filter(status_ativo):
    if status_ativo == 'ATIVO':
        return df['ativo'] == 'ATIVO'
    elif status_ativo == 'INATIVO':
        return df['ativo'] == 'INATIVO'
    else:
        return df['ativo'].isin(['ATIVO', 'INATIVO'])


def convert_to_tipo(pix_type):
    match pix_type:
        case 'PIX_IN':
            x = 'CASH-IN'
        case 'PIX_OUT':
            x = 'CASH-OUT'
    return x

def year_filter_criacao(year_criacao):
    if year_criacao == 0:
        mask = df2['ANO_CRIACAO'].isin([datetime.datetime.now().year])
    else:
        mask = df2['ANO_CRIACAO'].isin([year_criacao])
    return mask

def month_filter_criacao(month_criacao):
    if month_criacao == 0:
        mask = df2['MES_CRIACAO'].isin([datetime.datetime.now().month])
    else:
        mask = df2['MES_CRIACAO'].isin([month_criacao])
    return mask

mes_atual = datetime.datetime.now().month
ano_atual = datetime.datetime.now().year

locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

def formatar_reais(valor):
    return locale.currency(valor, grouping=True)

def year_filter(year):
    if year == 0:
        mask = df['ANO'].isin([datetime.datetime.now().year])
    else:
       mask = df['ANO'].isin([year])
    return mask

def month_filter(month):
    if month == 0:
        mask = df['MES'].isin([datetime.datetime.now().month])
    else:
       mask = df['MES'].isin([month])
    return mask

def team_filter(team):
    if team == 0:
        mask = df['Fantasia'].isin(df['Fantasia'].unique())
    else:
        mask = df['Fantasia'].isin([team])
    return mask

def pix_filter(pix_type):
    if pix_type == 'PIX_IN':
        mask = df['PIX_IN'] == 'PIX_IN'
    elif pix_type == 'PIX_OUT':
        mask = df['PIX_IN'] == 'PIX_OUT'
    else:
        mask = df['PIX_IN'].isin(['PIX_IN', 'PIX_OUT'])
    return mask

def status_pix_filter(status_list):
    if isinstance(status_list, str): 
        status_list = [status_list]
    
    if 'Todos' in status_list:
        mask = df['status'].notnull()
    else:
        mask = df['status'].isin(status_list) 
    return mask

start_date_default = date.today().replace(day=1)
end_date_default = date.today().replace(day=1).replace(month=start_date_default.month % 12 + 1) - timedelta(days=1)

authenticated = False

center_style = {'display': 'flex', 'justify-content': 'center', 'align-items': 'center', 'height': '100vh'}
login_layout = html.Div(
    [
        html.Div(
            dbc.Card(
                dbc.CardBody(
                    [
                        html.Img(src=r'assets/logo.png', alt='logo',className='logo input1'),
                        html.Br(),
                        dbc.Input(id='username', type='text', placeholder='Usuário', className='input'),
                        html.Br(),
                        dbc.Input(id='password', type='password', placeholder='Senha', className='input'),
                        html.Br(),
                        dbc.Button("Entrar", id='login-button', style={'background-color': '#e61a55', 'border': 'none'}, className='logo input'),  
                        html.Div(id='login-output')
                    ]
                )
            ), style={'max-width': '400px'}
        )
    ], style=center_style
)


tab_graficos_fiscais = dbc.Tab(
    label="Extrato", tab_id="tab-graficos-fiscais", children=[
        dbc.Container(fluid=True, children=[
            dbc.Row([
                dbc.Col([
                    dbc.CardBody([
                        dbc.Row(dbc.Col(html.Legend('EXTRATO DIÁRIO'))),
                        dbc.Col([
                            dbc.Card([
                                dbc.CardBody([
                                    dbc.Row(children=[
                                        dbc.Col([
                                            html.H5('Selecione o Período:'),
                                            dcc.DatePickerRange(
                                                id='date-range',
                                                start_date=start_date_default,
                                                end_date=end_date_default,
                                                display_format='DD/MM/YYYY'
                                            ),
                                            dcc.Dropdown(
                                                id='empresa-dropdown',
                                                style={'backgroundColor': '#333333', 'marginTop': '20px', 'width': '286px'},
                                                placeholder='Selecione uma empresa'
                                            ),
                                        ], sm=6, lg=3),
                                        dbc.Col([
                                            html.Div(
                                                id='table-container-in-parent',
                                                style={'display': 'flex', 'justify-content': 'center', 'margin-top': '7px'},
                                                children=[
                                                    html.Div(id='table-container-in'),
                                                ]
                                            )
                                        ], sm=6, lg=6),
                                    ])
                                ])
                            ], style={'marginTop': '20px'})
                        ], sm=12, lg=12),
                    ])
                ], sm=12, lg=12),
            ], className='g-2 my-auto', style={'margin-top': '7px'}),
            dcc.Interval(
                id='intervalo-component',
                interval=5 * 60 * 1000,
                n_intervals=0
            ),
            dcc.Store(id='data-loaded', data=False)  # Armazenar o estado de carregamento dos dados
        ])
    ], style={'height': '100vh'}
)



main_layout = dbc.Container(fluid=True, children=[
        dcc.Store(id='loading-state', data=True),  # Store to keep track of initial loading state
    dcc.Loading(
        id='loading-indicator',
        type='default',
        children=html.Div(id="app-content", children=[
    dbc.Tabs(id="tabs", active_tab="tab-graficos-vendas", children=[
        dbc.Tab(label="Gráficos Vendas", tab_id="tab-graficos-vendas"),
        tab_graficos_fiscais,
    ]),
    html.Div(id="graphs-container")
        ])
    )
])
    

tab_graficos_vendas = dbc.Tab(
    label="Gráficos Vendas", id="tab-graficos-vendas", children=[
    dbc.Container(fluid=True, children=[
    dbc.Row([   html.Link(
        rel='shortcut icon',
        href='/assets/favicon.ico'
    ), 
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    dbc.Row([
                        dbc.Col([
                            html.Legend("MODAbank")
                        ], sm=8),
                        dbc.Col([
                            html.I(className='logo', style={'font-size': '80%'})
                        ], sm=4, align="center")
                    ]),
                    dbc.Row([
                        dbc.Col([
                            ThemeSwitchAIO(aio_id="theme", themes=[url_theme1, url_theme2]),
                            html.Legend("DashBoard de Vendas")
                        ])
                    ], style={'margin-top': '10px'}),
                    dbc.Row([
                    html.Div(
                        className='logo-container',
                        children=[
                            html.Img(src=r'assets/logomoda.png', alt='logo',className='logo')
                        ]),
                        html.Div(
                            className='button-container',
                            children=[
                                dbc.Button("Sair", id='logout-button', style={'background-color': '#e61a55', 'border': 'none', 'margin-top':'15px'}, className='logo input'),
                                html.Div(id='logout-output')
                            ]),
                    ], style={'margin-top': '10px'})
                ])
            ], style=tab_card)
        ], sm=4, lg=2),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    dbc.Row(
                        dbc.Col(
                            html.Legend('Top 5 Empresas')
                        )
                    ),
                    dbc.Row([
                        dbc.Col([
                            dcc.Graph(id='graph1', className='dbc', config=config_graph)
                        ], sm=12, md=7),
                        dbc.Col([
                            dcc.Graph(id='graph2', className='dbc', config=config_graph)
                        ], sm=12, lg=5)
                    ])
                ])
            ], style=tab_card)
        ], sm=12, lg=7),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    dbc.Row(
                        dbc.Col([
                            html.H5('Escolha o ANO'),
                            dbc.RadioItems(
                                id="radio-year",
                                options=[],
                                value=ano_atual if ano_atual in df['ANO'].unique() else 0,
                                inline=True,
                                labelCheckedClassName="text-success",
                                inputCheckedClassName="border border-success bg-success",
                            ),
                            html.Div(id='year-selecty', style={'text-align': 'center', 'margin-top': '20px'}, className='dbc'),
                            html.H5('Escolha o MÊS'),
                            dbc.RadioItems(
                                id="radio-month",
                                options=[],
                                value=mes_atual if mes_atual in df['MES'].unique() else 0,
                                inline=True,
                                labelCheckedClassName="text-success",
                                inputCheckedClassName="border border-success bg-success",
                            ),
                            html.Div(id='month-select', style={'text-align': 'center', 'margin-top': '20px'}, className='dbc'),
                            html.H5('Escolha o tipo de transação PIX'),
                                dbc.RadioItems(
                                id="radio-pix",
                                options=[],
                                value='Ambos', 
                                inline=True,
                                labelCheckedClassName="text-success",
                                inputCheckedClassName="border border-success bg-success",
                            ),
                            html.Div(id='status-select', style={'text-align': 'center', 'margin-top': '20px'}, className='dbc'),
                            html.H5('Selecione o status das EMPRESAS'),
                            dbc.RadioItems(
                                id="status-ativo",
                                options=[
                                    {'label': 'ATIVAS', 'value': 'ATIVO'},
                                    {'label': 'INATIVAS', 'value': 'INATIVO'},
                                    {'label': 'ATIVAS E INATIVAS', 'value': 'Ambos'}
                                ],
                                value='Ambos', 
                                inline=True,
                                labelCheckedClassName="text-success",
                                inputCheckedClassName="border border-success bg-success",
                            ),
                            html.Div(id='pix-select', style={'text-align': 'center', 'margin-top': '20px'}, className='dbc'),
                            html.Div(id='radio-status-pix'),
                            html.H5('Escolha a EMPRESA'),
                            dcc.Dropdown(
                                id="radio-team",
                                options=[],
                                value=0,
                                style={'backgroundColor': '#333333'},
                                clearable=False,
                            ),
                            html.Div(id='team-select', style={'text-align': 'center', 'margin-top': '20px'}, className='dbc'),
                            html.Div(id="output-dados"),
                        ])
                    )
                ])
            ], style=tab_card)
        ], sm=12, lg=3)
    ], className='g-2 my-auto', style={'margin-top': '7px'}),

    # Row 2
    dbc.Row([
        dbc.Col([
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                            dcc.Graph(id='graph3', className='dbc', config=config_graph)
                    ], style=tab_card)
                ])
            ]),
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                            dcc.Graph(id='graph4', className='dbc', config=config_graph)
                    ], style=tab_card)
                ])
            ], className='g-2 my-auto', style={'margin-top': '7px'})
        ], sm=12, lg=4),
        dbc.Col([
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                            dcc.Graph(id='graph5', className='dbc', config=config_graph)
                    ], style=tab_card)
                ], sm=6),
                dbc.Col([
                    dbc.Card([
                            dcc.Graph(id='graph6', className='dbc', config=config_graph)
                    ], style=tab_card)
                ], sm=6)
            ], className='g-2'),
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dcc.Graph(id='graph7', className='dbc', config=config_graph)
                    ], style=tab_card)
                ])
            ], className='g-2 my-auto', style={'margin-top': '7px'})
        ], sm=12, lg=5),
        dbc.Col([
            dbc.Card([
                dcc.Graph(id='graph8', className='dbc', config=config_graph)
            ], style=tab_card)
        ], sm=12, lg=3)
    ], className='g-2 my-auto', style={'margin-top': '7px'}),

    # Row 3
    dbc.Row([
        dbc.Col([
            dbc.Card([
                    dcc.Graph(id='graph9', className='dbc', config=config_graph)
            ], style=tab_card)
        ], sm=12, lg=3),
        dbc.Col([
            dbc.Card([
                    dcc.Graph(id='graph10', className='dbc', config=config_graph)
            ], style=tab_card)
        ], sm=12, lg=3),
        dbc.Col([
            dbc.Card([
                    dcc.Graph(id='graph11', className='dbc', config=config_graph)
            ], style=tab_card)
        ], sm=12, lg=3),
        dbc.Col([
            dbc.Card([
                    dcc.Graph(id='graph12', className='dbc', config=config_graph)
            ], style=tab_card)
        ], sm=12, lg=3),
    ], className='g-2 my-auto', style={'margin-top': '7px'}),
    # Row 4
    dbc.Row([
        dbc.Col([
            dbc.Card([
                    dcc.Graph(id='graph13', className='dbc', config=config_graph)
            ], style=tab_card)
        ], sm=12, lg=3),
        dbc.Col([
            dbc.Card([
                    dcc.Graph(id='graph15', className='dbc', config=config_graph)
            ], style=tab_card)
        ], sm=12, lg=3),
        dbc.Col([
            dbc.Card([
                    dcc.Graph(id='graph16', className='dbc', config=config_graph)
            ], style=tab_card)
        ], sm=12, lg=3),
        dbc.Col([
            dbc.Card([
                    dcc.Graph(id='graph17', className='dbc', config=config_graph)
            ], style=tab_card)
        ], sm=12, lg=3),
    ], className='g-2 my-auto', style={'margin-top': '7px'}),

        dcc.Interval(
        id='interval-component',
        interval=1 * 60 * 60 * 1000,
        n_intervals=0
    )
])], style={'height': '100vh'})


@app.callback(
    Output('graphs-container', 'children'),
    [Input('tabs', 'active_tab')]
)
def update_graphs_content(active_tab):
    if active_tab == 'tab-graficos-vendas':
        return tab_graficos_vendas
    elif active_tab == 'tab-graficos-fiscais':
        return None


def cosultaextratoin():
    conn = mysql.connector.connect(
            host='localhost',
            user='ideia',
            password='Ideia@2017',
            database='modapay'
    )

    sql_in = """
        WITH 
            cobranca_cte AS (
                SELECT 
                    e.fantasia,
                    DATE(c.data_pagamento) AS data_dia,
                    SUM(c.valor) AS valor_in,
                    COUNT(*) AS qtd_in,
                    SUM(c.taxa_total) AS taxa_in,
                    SUM(c.valor_sem_taxa) AS menos_taxa_in,
                    SUM(c.valor) / COUNT(*) AS ticket_medio_in
                FROM 
                    cobranca c
                INNER JOIN 
                    empresa e ON c.fk_empresa = e.codigo
                WHERE 
                    c.status IN ('CONCLUIDO', 'CONCLUIDA') 
                GROUP BY 
                    e.fantasia, DATE(c.data_pagamento)
            ),
            saque_cte AS (
                SELECT 
                    e.fantasia, 
                    s.data_solicitacao AS data_dia, 
                    SUM(s.valor_solicitado) AS valor_out, 
                    COUNT(*) AS qtd_out, 
                    SUM(s.taxa_total) AS taxa_out, 
                    SUM(s.valor_sem_taxa) AS menos_taxa_out,
                    SUM(s.valor_solicitado) / COUNT(*) AS ticket_medio_out
                FROM 
                    saque s
                INNER JOIN 
                    empresa e ON s.fk_empresa = e.codigo
                WHERE 
                    s.status IN ('executed', 'processing') 
                GROUP BY 
                    e.fantasia, s.data_solicitacao
            ),
            combined_cte AS (
                SELECT 
                    fantasia,
                    data_dia,
                    SUM(valor_in) AS valor_in,
                    SUM(qtd_in) AS qtd_in,
                    SUM(taxa_in) AS taxa_in,
                    SUM(menos_taxa_in) AS menos_taxa_in,
                    SUM(valor_out) AS valor_out,
                    SUM(qtd_out) AS qtd_out,
                    SUM(taxa_out) AS taxa_out,
                    SUM(menos_taxa_out) AS menos_taxa_out,
                    CASE 
                        WHEN SUM(qtd_in) > 0 THEN SUM(valor_in) / SUM(qtd_in) 
                        ELSE 0 
                    END AS ticket_medio_in,
                    CASE 
                        WHEN SUM(qtd_out) > 0 THEN SUM(valor_out) / SUM(qtd_out) 
                        ELSE 0 
                    END AS ticket_medio_out
                FROM (
                    SELECT 
                        fantasia,
                        data_dia,
                        valor_in,
                        qtd_in,
                        taxa_in,
                        menos_taxa_in,
                        0 AS valor_out,
                        0 AS qtd_out,
                        0 AS taxa_out,
                        0 AS menos_taxa_out,
                        ticket_medio_in,
                        0 AS ticket_medio_out
                    FROM 
                        cobranca_cte

                    UNION ALL

                    SELECT 
                        fantasia,
                        data_dia,
                        0 AS valor_in,
                        0 AS qtd_in,
                        0 AS taxa_in,
                        0 AS menos_taxa_in,
                        valor_out,
                        qtd_out,
                        taxa_out,
                        menos_taxa_out,
                        0 AS ticket_medio_in,
                        ticket_medio_out
                    FROM 
                        saque_cte
                ) AS subquery
                GROUP BY 
                    fantasia, data_dia
            ),
            saldo_cte AS (
                SELECT 
                    fantasia,
                    data_dia,
                    valor_in,
                    qtd_in,
                    taxa_in,
                    menos_taxa_in,
                    valor_out,
                    qtd_out,
                    taxa_out,
                    menos_taxa_out,
                    ticket_medio_in,
                    ticket_medio_out,
                    SUM(menos_taxa_in - menos_taxa_out) OVER (PARTITION BY fantasia ORDER BY data_dia) AS saldo_acumulado
                FROM
                    combined_cte
            )
        SELECT 
            *
        FROM 
            saldo_cte
        ORDER BY 
            fantasia ASC, data_dia ASC;

    """

    df_extrato_in = pd.read_sql(sql_in, conn)

    conn.close()
    return df_extrato_in



@app.callback(
    Output("output-dados", "children"),
    Input('interval-component', 'n_intervals')
)
def recarregar_dados(n_intervals):
    global df
    with lock:
        try:
            df = obter_dados_firebird()
            df2 = criacao()
            df_extrato_in = cosultaextratoin()
        except Exception as e:
            print(f"Erro recarregar_dados: {e}")
    return None

df = obter_dados_firebird()
df_extrato_in = cosultaextratoin()

@app.callback(
    Output("radio-pix", "options"),
    Output("radio-pix", "value"),
    Input('interval-component', 'n_intervals')
)
def update_radio_pix(n_intervals):
    options = [{'label': 'CASH-IN', 'value': 'PIX_IN'}, {'label': 'CASH-OUT', 'value': 'PIX_OUT'}, {'label': 'CASH-IN E CASH-OUT', 'value': 'Ambos'}]
    default_value = 'Ambos'
    return options, default_value

@app.callback(
    Output("radio-status-pix", "options"),
    Output("radio-status-pix", "value"),
    Input('interval-component', 'n_intervals'),
)
def update_radio_status_pix(n_intervals):
    unique_status = df['status'].unique()  
    options = [{'label': status, 'value': status} for status in unique_status]
    options.append({'label': 'TODOS', 'value': 'Todos'}) 
    default_value = ['Todos']
    return options, default_value


@app.callback(
    [Output('table-container-in', 'children'),
     Output('empresa-dropdown', 'options'),
     Output('empresa-dropdown', 'value')],
    [Input(ThemeSwitchAIO.ids.switch("theme"), "value"),
     Input('interval-component', 'n_intervals'),
     Input('date-range', 'start_date'),
     Input('date-range', 'end_date'),
     Input('empresa-dropdown', 'value')]  # Adicione esta entrada para capturar o valor selecionado do dropdown
)
def update_table(toggle, n_intervals, start_date, end_date, selected_empresa):  # Atualize a assinatura da função
    try:
        template = template_theme1 if toggle else template_theme2
        
        df_extrato_in = cosultaextratoin()
        
        df_extrato_in = df_extrato_in.rename(columns={
            'fantasia': 'Empresa',
            'data_dia': 'Data',
            'valor_in': 'Transações Recebidas Cash-In',
            'qtd_in': 'Qtd de Transações Cash-In',
            'taxa_in': 'Taxa Total Cash-In',
            'menos_taxa_in': 'Total Menos Taxa Cash-In',
            'ticket_medio_in': 'Ticket Médio Cash-In',
            'valor_out': 'Transações Recebidas Cash-Out',
            'qtd_out': 'Qtd de Transações Cash-Out',
            'taxa_out': 'Taxa Total Cash-Out',
            'menos_taxa_out': 'Total Mais Taxa Cash-Out',
            'ticket_medio_out': 'Ticket Médio Cash-Out',
            'saldo_acumulado': 'Saldo Acumulado'
        })
        
        df_extrato_in = df_extrato_in[['Empresa', 'Data', 'Transações Recebidas Cash-In', 'Qtd de Transações Cash-In', 'Taxa Total Cash-In', 'Total Menos Taxa Cash-In', 'Ticket Médio Cash-In', 'Saldo Acumulado', 'Transações Recebidas Cash-Out', 'Qtd de Transações Cash-Out', 'Taxa Total Cash-Out', 'Total Mais Taxa Cash-Out', 'Ticket Médio Cash-Out']]

        dropdown_options = [{'label': empresa, 'value': empresa} for empresa in df_extrato_in['Empresa'].unique()]

        df_extrato_in['Data'] = pd.to_datetime(df_extrato_in['Data'])
        mask_in = (df_extrato_in['Data'] >= pd.to_datetime(start_date)) & (df_extrato_in['Data'] <= pd.to_datetime(end_date))
        df_extrato_in = df_extrato_in.loc[mask_in].sort_values('Data').copy()


        df_extrato_in = df_extrato_in.sort_values(by='Data', ascending=False)

        df_extrato_in = df_extrato_in.fillna(0)
        
        df_extrato_in['Data'] = df_extrato_in['Data'].dt.strftime('%d/%m/%Y')
        
        # Aplicar filtro pelo valor selecionado no dropdown, se houver
        if selected_empresa:
            df_extrato_in = df_extrato_in[df_extrato_in['Empresa'] == selected_empresa]
            df_extrato_in = df_extrato_in.groupby('Data').sum().reset_index()
            df_extrato_in = df_extrato_in.sort_values(by='Data', ascending=False)


        data_combined = []
        for idx, row in df_extrato_in.iterrows():
            data_combined.extend([
                {'Descrição': 'Empresa:', 'Valor': row['Empresa']},
                {'Descrição': 'Cash-In:', 'Valor': row['Data']},
                {'Descrição': 'Recebido Cash-In', 'Valor': '{:,.2f}'.format(row['Transações Recebidas Cash-In']).replace(',', '|').replace('.', ',').replace('|', '.')},
                {'Descrição': 'Qtd. Cash-In', 'Valor': '{:,.0f}'.format(row['Qtd de Transações Cash-In']).replace(',', '|').replace('.', ',').replace('|', '.')},
                {'Descrição': 'Taxa Cash-In', 'Valor': '{:,.2f}'.format(row['Taxa Total Cash-In']).replace(',', '|').replace('.', ',').replace('|', '.')},
                {'Descrição': 'Menos a Taxa Cash-In', 'Valor': '{:,.2f}'.format(row['Total Menos Taxa Cash-In']).replace(',', '|').replace('.', ',').replace('|', '.')},
                {'Descrição': 'Ticket Médio Cash-In', 'Valor': '{:,.2f}'.format(row['Ticket Médio Cash-In']).replace(',', '|').replace('.', ',').replace('|', '.')},
                {'Descrição': 'Cash-Out:', 'Valor': ''},
                {'Descrição': 'Pago Cash-Out', 'Valor': '{:,.2f}'.format(row['Transações Recebidas Cash-Out']).replace(',', '|').replace('.', ',').replace('|', '.')},
                {'Descrição': 'Qtd. Cash-Out', 'Valor': '{:,.0f}'.format(row['Qtd de Transações Cash-Out']).replace(',', '|').replace('.', ',').replace('|', '.')},
                {'Descrição': 'Taxa Cash-Out', 'Valor': '{:,.2f}'.format(row['Taxa Total Cash-Out']).replace(',', '|').replace('.', ',').replace('|', '.')},
                {'Descrição': 'Mais a Taxa Cash-Out', 'Valor': '{:,.2f}'.format(row['Total Mais Taxa Cash-Out']).replace(',', '|').replace('.', ',').replace('|', '.')},
                {'Descrição': 'Ticket Médio Cash-Out', 'Valor': '{:,.2f}'.format(row['Ticket Médio Cash-Out']).replace(',', '|').replace('.', ',').replace('|', '.')},
                {'Descrição': 'Saldo Acumulado', 'Valor': '{:,.2f}'.format(row['Saldo Acumulado']).replace(',', '|').replace('.', ',').replace('|', '.'), 'id': 'saldo-acumulado'},
                {'Descrição': '--------------------', 'Valor': '----------'}
            ])

        table_combined = dash_table.DataTable(
            id='table-combined',
            columns=[{"name": i, "id": i} for i in ['Descrição', 'Valor']],
            data=data_combined,
            style_table={'overflowX': 'scroll', 'width': '100%', 'margin': 'auto', 'border':'1px solid gray'},
            style_cell={'textAlign': 'left', 'padding': '5px', 'whiteSpace': 'normal', 'height': 'auto', 'border-top': 'none', 'border-right': 'none'},
            style_header={'fontWeight': 'bold', 'backgroundColor': 'white', 'color': 'black', 'text-align': 'center', 'border-bottom':'1px solid gray', 'margin-top':'50px'},
            style_data={'whiteSpace': 'normal', 'height': 'auto', 'color': 'black', 'backgroundColor': 'white'},
            export_format='csv',
            style_data_conditional=[
            {
                'if': {'column_id': 'Valor'},
                'textAlign': 'right',
            },
            {

                'if': {'column_id': 'Valor', 'filter_query': '{id} eq "saldo-acumulado"'},
                'color': '#1cb49c',
                'fontWeight': 'bold'
            },
            {
                'if': {'column_id': 'Descrição', 'filter_query': '{id} eq "saldo-acumulado"'},
                'color': '#1cb49c',
                'fontWeight': 'bold'
            }

        ]
        )

        return table_combined, dropdown_options, selected_empresa  # Retorne selected_empresa como valor do dropdown

    except Exception as e:
        print(f"Erro ao obter dados da tabela: {e}")
        return html.Div(f"Erro ao carregar os dados da tabela: {e}")

@app.callback(
    Output('graph1', 'figure'),
    Output('graph2', 'figure'),
    [Input('radio-month', 'value'),
     Input('radio-year', 'value'),
     Input('radio-team', 'value'),
     Input('radio-pix', 'value'), 
     Input('radio-status-pix', 'value'), 
     Input('status-ativo', 'value'),
     Input(ThemeSwitchAIO.ids.switch("theme"), "value"),
     Input('interval-component', 'n_intervals')]  
)
def update_graph1e2(month, year, team, pix_type, status_list, status_ativo, toggle, n_intervals):
    with lock:
        try:
            template = template_theme1 if toggle else template_theme2
            mask_year = year_filter(year)
            mask_month = month_filter(month)
            mask_team = team_filter(team)
            mask_pix = pix_filter(pix_type)
            mask_ativo = create_status_filter(status_ativo)
            status_incluidos = ['CONCLUIDO', 'CONCLUIDA', 'processing', 'executed']
            mask_status = df['status'].isin(status_incluidos)

            df_filtered = df.loc[mask_year & mask_month & mask_team & mask_pix & mask_status & mask_ativo]

            df_1 = df_filtered.groupby(['Fantasia'])['valor'].sum().reset_index()
            df_1 = df_1.groupby('Fantasia').head(1).reset_index()
            df_1 = df_1.sort_values(by='valor', ascending=False)
            df_1 = df_1.head(5)
            df_1['TOTAL_VENDAS'] = df_1['valor'].map(formatar_reais)
            fig1 = go.Figure(go.Bar(x=df_1['Fantasia'], y=df_1['valor'], textposition='auto', text=df_1['TOTAL_VENDAS']))
            fig1.update_layout(main_config, height=300, template=template)

            fig2 = go.Figure(go.Pie(labels=df_1['Fantasia'] + ' - ' + df_1['Fantasia'], values=df_1['valor'], hole=.6))
            fig2.update_layout(main_config, height=300, template=template, showlegend=False)
        except Exception as e:
            print(f"Erro ao atualizar o Graph1: {e}")
            fig1 = go.Figure()
            fig2 = go.Figure()
            fig1.update_layout(main_config, height=300, template=template)
            fig2.update_layout(main_config, height=300, template=template, showlegend=False)
    return fig1, fig2

@app.callback(
    Output('graph3', 'figure'),
    [Input('radio-month', 'value'),
     Input('radio-year', 'value'),
     Input('radio-team', 'value'),
     Input('radio-pix', 'value'), 
     Input('radio-status-pix', 'value'),
     Input('status-ativo', 'value'),
     Input(ThemeSwitchAIO.ids.switch("theme"), "value"),
     Input('interval-component', 'n_intervals')]  
)
def update_graph3(month, year, team, pix_type, status_list, status_ativo, toggle, n_intervals):
    with lock:
        try:
            template = template_theme1 if toggle else template_theme2
            mask_year = year_filter(year)
            mask_month = month_filter(month)
            mask_team = team_filter(team)
            mask_pix = pix_filter(pix_type)
            mask_ativo = create_status_filter(status_ativo)

            status_incluidos = ['CONCLUIDO', 'CONCLUIDA', 'processing', 'executed']
            mask_status = df['status'].isin(status_incluidos)

            df_filtered = df.loc[mask_year & mask_month & mask_team & mask_pix & mask_status & mask_ativo]

            df_3 = df_filtered.groupby('DIA')['valor'].sum().reset_index()
            df_3['TOTAL_VENDAS'] = df_3['valor'].map(formatar_reais)
            fig3 = go.Figure(go.Scatter(x=df_3['DIA'], y=df_3['valor'], fill='tonexty', text=df_3['TOTAL_VENDAS'], hoverinfo='text'))
            fig3.add_annotation(text='Faturamento por Dia',xref="paper", yref="paper", font=dict( size=17, color='gray'), align="center", bgcolor="rgba(0,0,0,0.8)", x=0.05, y=0.85, showarrow=False)
            fig3.update_layout(main_config, height=213, template=template)

        except Exception as e:
            print(f"Erro ao atualizar o Graph3: {e}")
            fig3 = go.Figure()
            fig3.update_layout(main_config, height=213, template=template)

    return fig3


@app.callback(
    Output('graph4', 'figure'),
    [Input('radio-month', 'value'),
     Input('radio-year', 'value'),
     Input('radio-team', 'value'),
     Input('radio-pix', 'value'), 
     Input('radio-status-pix', 'value'), 
    Input('status-ativo', 'value'),
     Input(ThemeSwitchAIO.ids.switch("theme"), "value"),
     Input('interval-component', 'n_intervals')]  
)
def update_graph4(month, year, team, pix_type, status_list, status_ativo, toggle, n_intervals):
    with lock:
        try:
            template = template_theme1 if toggle else template_theme2
            mask_year = year_filter(year)
            mask_month = month_filter(month)
            mask_team = team_filter(team)
            mask_pix = pix_filter(pix_type)
            mask_ativo = create_status_filter(status_ativo)

            status_incluidos = ['CONCLUIDO', 'CONCLUIDA', 'processing', 'executed']
            mask_status = df['status'].isin(status_incluidos)

            df_filtered = df.loc[mask_year & mask_team & mask_pix & mask_status & mask_ativo]

            df_4 = df_filtered.groupby('MES')['valor'].sum().reset_index()
            df_4['TOTAL_VENDAS'] = df_4['valor'].map(formatar_reais)
            fig4 = go.Figure(go.Scatter(x=df_4['MES'], y=df_4['valor'], fill='tonexty', text=df_4['TOTAL_VENDAS'], hoverinfo='text'))
            fig4.add_annotation(text='Faturamento por Mês', xref="paper", yref="paper",font=dict( size=17, color='gray'),align="center", bgcolor="rgba(0,0,0,0.8)",x=0.05, y=0.85, showarrow=False)
            fig4.update_layout(main_config, height=213, template=template)

        except Exception as e:
            print(f"Erro ao atualizar o Graph4: {e}")
            fig4 = go.Figure()
            fig4.update_layout(main_config, height=213, template=template)

    return fig4

@app.callback(
    Output('graph5', 'figure'),
    Input('radio-month', 'value'),
    Input('radio-year', 'value'),
    Input('radio-team', 'value'),
    Input('radio-pix', 'value'), 
    Input('radio-status-pix', 'value'), 
    Input('status-ativo', 'value'),
    Input(ThemeSwitchAIO.ids.switch("theme"), "value"),
    Input('interval-component', 'n_intervals')  
)
def update_graph5(month, year, team, pix_type, status_list, status_ativo, toggle, n_intervals):
    with lock:
        try:
            template = template_theme1 if toggle else template_theme2
            mask_year = year_filter(year)
            mask_month = month_filter(month)
            mask_team = team_filter(team)
            mask_pix = pix_filter(pix_type)
            mask_ativo = create_status_filter(status_ativo)
            status_incluidos = ['CONCLUIDO', 'CONCLUIDA', 'processing', 'executed']
            mask_status = df['status'].isin(status_incluidos)

            df_filtered = df.loc[mask_year & mask_month & mask_team & mask_pix & mask_status & mask_ativo]

            # Lógica específica para o gráfico 5
            sum_values = df_filtered['valor'].sum()
            count_values = df_filtered['valor'].count()

            if count_values > 0:
                total_ticket = sum_values / count_values
                total_ticket = "{:,.2f}".format(total_ticket).replace('.', ',').replace(',', '.', 1)
            else:
                total_ticket = 0.0

            fig5 = go.Figure()
            if toggle:
                fig5.add_trace(go.Indicator(
                        mode='number',
                        title={"text": f"<span style='font-size:80%; color:#306fc1'>TICKET MÉDIO</span><br><span style='font-size:70%; color:#306fc1'>Ticket médio mensal</span><br><br><span style='font-size:150%; color:#306fc1'>R${total_ticket}</span>"},
                        number_font={'color': 'white'},
                        value=0
                ))
            else:
                fig5.add_trace(go.Indicator(
                        mode='number',
                        title={"text": f"<span style='font-size:80%; color:#306fc1'>TICKET MÉDIO</span><br><span style='font-size:70%; color:#306fc1'>Ticket médio mensal</span><br><br><span style='font-size:150%; color:#306fc1'>R${total_ticket}</span>"},
                        number_font={'color': '#303030'},
                        value=0
                ))
            fig5.update_layout(main_config, height=213, template=template)
            fig5.update_layout({"margin": {"l": 0, "r": 0, "t": 150, "b": 0}})

        except Exception as e:
            print(f"Erro ao atualizar gráficos 5: {e}")
    return fig5



@app.callback(
    Output('graph6', 'figure'),
    [Input('radio-team', 'value'),
     Input('radio-status-pix', 'value'),
     Input(ThemeSwitchAIO.ids.switch("theme"), "value"),
     Input('interval-component', 'n_intervals')]
)
def update_graph6(team, status_list, toggle, n_intervals):
    with lock:
        try:
            template = template_theme1 if toggle else template_theme2

            mask_team = team_filter(team)
            status_incluidos = ['CONCLUIDO', 'CONCLUIDA', 'processing', 'executed']
            mask_status = df['status'].isin(status_incluidos)

            df_filtered = df.loc[mask_team & mask_status]
            df_6 = df_filtered.groupby(['Fantasia', 'PIX_IN'])['VALOR_MENOS_TAXA'].sum().unstack(fill_value=0)
            df_pix_in = df_filtered[df_filtered['PIX_IN'] == 'PIX_IN']['VALOR_MENOS_TAXA'].sum()
            df_pix_out = df_filtered[df_filtered['PIX_IN'] == 'PIX_OUT']['VALOR_MENOS_TAXA'].sum()
            df_6['saldo_total'] = df_pix_in - df_pix_out
            
            df_6 = df_filtered.drop_duplicates(subset=['Fantasia'])[['Fantasia', 'saldo_atual']]
            df_6.columns = ['Fantasia', 'saldo']
            total = df_6['saldo'].sum()
            total_saldo = "{:,.2f}".format(total).replace(',', 'v').replace('.', ',').replace('v', '.')

            fig6 = go.Figure()
            if toggle:
                fig6.add_trace(go.Indicator(
                        mode='number',
                        title={"text": f"<span style='font-size:80%; color:#1cb49c'>SALDO TOTAL</span><br><span style='font-size:70%; color:#1cb49c'>Em Reais</span><br><br><span style='font-size:150%; color:#1cb49c'>R${total_saldo}</span>"},
                        number_font={'color': 'white'},
                        value=0
                ))
            else:
                fig6.add_trace(go.Indicator(
                        mode='number',
                        title={"text": f"<span style='font-size:80%; color:#1cb49c'>SALDO TOTAL</span><br><span style='font-size:70%; color:#1cb49c'>Em Reais</span><br><br><span style='font-size:150%; color:#1cb49c'>R${total_saldo}</span>"},
                        number_font={'color': '#303030'},
                        value=0
                ))
            fig6.update_layout(main_config, height=213, template=template)
            fig6.update_layout({"margin": {"l": 0, "r": 0, "t": 150, "b": 0}})

        except Exception as e:
            print(f"Erro ao atualizar gráficos 6: {e}")
            fig6 = go.Figure()  # Defina fig6 mesmo em caso de erro
    return fig6




@app.callback(
    Output('graph7', 'figure'),
    [Input('radio-year', 'value'),
     Input('radio-team', 'value'),
     Input('radio-pix', 'value'), 
     Input('radio-status-pix', 'value'), 
     Input('status-ativo', 'value'),
     Input(ThemeSwitchAIO.ids.switch("theme"), "value"),
     Input('interval-component', 'n_intervals')]  
)
def update_graph7( year, team, pix_type, status_list, status_ativo, toggle, n_intervals):
    with lock:
        try:
            template = template_theme1 if toggle else template_theme2
            mask_zero = year_filter(year)
            mask_pix = pix_filter(pix_type)
            mask_ativo = create_status_filter(status_ativo)
            df_zero = df.loc[mask_zero & mask_pix & mask_ativo]

            df_7_group = df_zero.groupby(['MES', 'Fantasia'])['valor'].sum().reset_index()
            df_7_total = df_zero.groupby('MES')['valor'].sum().reset_index()
            fig7 = px.line(df_7_group, y="valor", x="MES", color="Fantasia")
            fig7.add_trace(go.Scatter(y=df_7_total["valor"], x=df_7_total["MES"], mode='lines+markers', fill='tonexty', name='Total de Vendas'))
            fig7.update_layout(main_config, yaxis={'title': None}, xaxis={'title': None}, height=213, template=template, title='FATURAMENTO DAS EMPRESAS POR MÊS')
            fig7.update_layout({"legend": {"yanchor": "top", "y": 0.99, "font": {"color": "white", 'size': 10}}})

        except Exception as e:
            print(f"Erro ao atualizar o Graph7: {e}")
    return fig7


@app.callback(
    Output('graph8', 'figure'),
    [Input('radio-year', 'value'),
     Input('radio-team', 'value'),
     Input('radio-pix', 'value'), 
     Input('radio-status-pix', 'value'),
     Input('status-ativo', 'value'),
     Input(ThemeSwitchAIO.ids.switch("theme"), "value"),
     Input('interval-component', 'n_intervals')]  
)
def update_graph8( year, team, pix_type, status_list, status_ativo, toggle, n_intervals):
    with lock:
        try:
            template = template_theme1 if toggle else template_theme2
            mask_year = year_filter(year)
            mask_team = team_filter(team)
            mask_pix = pix_filter(pix_type)
            mask_ativo = create_status_filter(status_ativo)
            status_incluidos = ['CONCLUIDO', 'CONCLUIDA', 'processing', 'executed']
            mask_status = df['status'].isin(status_incluidos)

            df_filtered = df.loc[mask_year & mask_team & mask_pix & mask_status & mask_ativo]


            #Grafico 8
            df_8 = df_filtered.groupby('PIX_IN')['valor'].sum().reset_index()
            df_8['TOTAL_VENDAS'] = df_8['valor'].map(formatar_reais)
            df_8['PIX_IN'] = df_8['PIX_IN'].apply(convert_to_tipo)
            fig8 = go.Figure(go.Bar( x=df_8['valor'], y=df_8['PIX_IN'], orientation='h', textposition='auto', text=df_8['TOTAL_VENDAS'], hoverinfo='text',insidetextfont=dict(family='Times', size=12)))
            fig8.update_layout(main_config, height=440, title='CASH-IN E CASH-OUT POR ANO', template=template)

        except Exception as e:
            print(f"Erro ao atualizar o Graph8: {e}")
    return fig8


@app.callback(
    Output('graph9', 'figure'),
    [Input('radio-month', 'value'),
     Input('radio-year', 'value'),
     Input('radio-team', 'value'),
     Input('radio-pix', 'value'), 
     Input('radio-status-pix', 'value'),
     Input('status-ativo', 'value'),
     Input(ThemeSwitchAIO.ids.switch("theme"), "value"),
     Input('interval-component', 'n_intervals')]  
)
def update_graph9(month, year, team, pix_type, status_list, status_ativo, toggle, n_intervals):
    with lock:
        try:
            template = template_theme1 if toggle else template_theme2
            mask_year = year_filter(year)
            mask_month = month_filter(month)
            mask_team = team_filter(team)
            mask_pix = pix_filter(pix_type)
            mask_ativo = create_status_filter(status_ativo)
            status_incluidos = ['CONCLUIDO', 'CONCLUIDA', 'processing', 'executed']
            mask_status = df['status'].isin(status_incluidos)

            df_filtered = df.loc[mask_year & mask_month & mask_team & mask_pix & mask_status & mask_ativo]

            df_9 = df_filtered
            transacoes_recebidas = df_9.shape[0]
            transacoes_formatadas = "{:,.0f}".format(transacoes_recebidas).replace('.', ',').replace(',', '.', 1)
            total = df_9['valor'].sum()
            total_recebido = "{:,.2f}".format(total).replace('.', ',').replace(',', '.', 1)
            fig9 = go.Figure()
            if toggle:    
                fig9.add_trace(go.Indicator(mode='number',
                                title={"text": f"<span style='font-size:80%'>TRANSAÇÕES RECEBIDAS</span><br><span style='font-size:70%'>COBRANÇAS: {transacoes_formatadas}</span><br><br><span style='font-size:150%;'>R${total_recebido}</span>"},
                                number_font={'color': 'white'}, 
                                value=0
                ))
            else:
                fig9.add_trace(go.Indicator(mode='number',
                                title={"text": f"<span style='font-size:80%'>TRANSAÇÕES RECEBIDAS</span><br><span style='font-size:70%'>COBRANÇAS: {transacoes_formatadas}</span><br><br><span style='font-size:150%;'>R${total_recebido}</span>"},
                                number_font={'color': '#303030'}, 
                                value=0
                ))
            fig9.update_layout(main_config, height=170, template=template)
            fig9.update_layout({"margin": {"l": 0, "r": 0, "t": 125, "b": 0}})

        except Exception as e:
            print(f"Erro ao atualizar o Graph9: {e}")
    return fig9


@app.callback(
    Output('graph11', 'figure'),
    [Input('radio-month', 'value'),
     Input('radio-year', 'value'),
     Input('radio-team', 'value'),
     Input('radio-pix', 'value'), 
     Input('radio-status-pix', 'value'), 
     Input('status-ativo', 'value'),
     Input(ThemeSwitchAIO.ids.switch("theme"), "value"),
     Input('interval-component', 'n_intervals')]  
)
def update_graph11(month, year, team, pix_type, status_list, status_ativo, toggle, n_intervals):
    with lock:
        try:
            template = template_theme1 if toggle else template_theme2
            mask_year = year_filter(year)
            mask_month = month_filter(month)
            mask_team = team_filter(team)
            mask_pix = pix_filter(pix_type)
            mask_ativo = create_status_filter(status_ativo)
            status_incluidos = ['CONCLUIDO', 'CONCLUIDA', 'processing', 'executed']
            mask_status = df['status'].isin(status_incluidos)

            df_filtered = df.loc[mask_year & mask_month & mask_team & mask_pix & mask_status & mask_ativo]

            df_11 = df_filtered
            transacoes_taxa = df_11.shape[0]
            transacoes_formatadas = "{:,.0f}".format(transacoes_taxa).replace('.', ',').replace(',', '.', 1)
            total = df_11['taxa_total'].sum()
            total_taxa = "{:,.2f}".format(total).replace('.', ',').replace(',', '.', 1)
            fig11 = go.Figure()
            if toggle:
                fig11.add_trace(go.Indicator(mode='number',
                                title={"text": f"<span style='font-size:80%'>TAXA</span><br><span style='font-size:70%'>COBRANÇAS: {transacoes_formatadas}</span><br><br><span style='font-size:150%';>R${total_taxa}</span>"},
                                number_font={'color': 'white'},
                                value=0 
                ))
            else:
                fig11.add_trace(go.Indicator(mode='number',
                                title={"text": f"<span style='font-size:80%'>TAXA</span><br><span style='font-size:70%'>COBRANÇAS: {transacoes_formatadas}</span><br><br><span style='font-size:150%';>R${total_taxa}</span>"},
                                number_font={'color': '#303030'},
                                value=0 
                ))
            fig11.update_layout(main_config, height=170, template=template)
            fig11.update_layout({"margin": {"l": 0, "r": 0, "t": 125, "b": 0}})

        except Exception as e:
            print(f"Erro ao atualizar gráficos 11: {e}")
    return fig11


@app.callback(
    Output('graph12', 'figure'),
    [Input('radio-month', 'value'),
     Input('radio-year', 'value'),
     Input('radio-team', 'value'),
     Input('radio-pix', 'value'), 
     Input('radio-status-pix', 'value'),
     Input('status-ativo', 'value'),
     Input(ThemeSwitchAIO.ids.switch("theme"), "value"),
     Input('interval-component', 'n_intervals')]  
)
def update_graph12(month, year, team, pix_type, status_list, status_ativo, toggle, n_intervals):
    with lock:
        try:
            template = template_theme1 if toggle else template_theme2

            mask_team = team_filter(team)
            mask_pix = pix_filter(pix_type)
            mask_ativo = create_status_filter(status_ativo)
            status_incluidos = ['CONCLUIDO', 'CONCLUIDA', 'processing', 'executed']
            mask_status = df['status'].isin(status_incluidos)
            pix_in = ['PIX_IN']
            mask_pix_in = df['PIX_IN'].isin(pix_in)

            df_semanoemes = df.loc[mask_status & mask_pix_in & mask_team & mask_ativo]

            today = datetime.datetime.now()
            df_filtered_today = df_semanoemes[(df_semanoemes['DIA'] == today.day) & (df_semanoemes['MES'] == today.month) & (df_semanoemes['ANO'] == today.year)]
            df_12 = df_filtered_today.groupby('Fantasia')['valor'].sum()
            transacoes_diaria = df_filtered_today.shape[0]
            transacoes_formatadas = "{:,.0f}".format(transacoes_diaria).replace('.', ',').replace(',', '.', 1)
            df_12.sort_values(ascending=False, inplace=True)
            df_12 = df_12.reset_index()
            total = df_12['valor'].sum()
            total_cashin = "{:,.2f}".format(total).replace('.', ',').replace(',', '.', 1)
            fig12 = go.Figure()
            if toggle:
                fig12.add_trace(go.Indicator(
                            mode='number',
                            title={"text": f"<span style='font-size:80%; color:#1cb49c '>CASH-IN DIÁRIO</span><br><span style='font-size:70%; color:#1cb49c'>COBRANÇAS: {transacoes_formatadas}</span><br><br><span style='font-size:150%; color:#1cb49c'>R${total_cashin}</span>"},
                            number_font={'color': 'white'},
                            value=0
                ))
            else:
                fig12.add_trace(go.Indicator(
                            mode='number',
                            title={"text": f"<span style='font-size:80%; color:#1cb49c '>CASH-IN DIÁRIO</span><br><span style='font-size:70%; color:#1cb49c'>COBRANÇAS: {transacoes_formatadas}</span><br><br><span style='font-size:150%; color:#1cb49c'>R${total_cashin}</span>"},
                            number_font={'color': '#303030'},
                            value=0
                ))
            fig12.update_layout(main_config, height=170, template=template)
            fig12.update_layout({"margin": {"l": 0, "r": 0, "t": 125, "b": 0}})

        except Exception as e:
            print(f"Erro ao atualizar o Graph12: {e}")
    return fig12


@app.callback(
    Output('graph10', 'figure'),
    [Input('radio-month', 'value'),
     Input('radio-year', 'value'),
     Input('radio-team', 'value'),
     Input('radio-pix', 'value'), 
     Input('radio-status-pix', 'value'), 
     Input('status-ativo', 'value'),
     Input(ThemeSwitchAIO.ids.switch("theme"), "value"),
     Input('interval-component', 'n_intervals')]  
)
def update_graph10(month, year, team, pix_type, status_list, status_ativo, toggle, n_intervals):
    with lock:
        try:
            template = template_theme1 if toggle else template_theme2
            mask_year = year_filter(year)
            mask_month = month_filter(month)
            mask_team = team_filter(team)
            mask_pix = pix_filter(pix_type)
            mask_ativo = create_status_filter(status_ativo)
            status_incluidos = ['CONCLUIDO', 'CONCLUIDA', 'processing', 'executed']
            mask_status = df['status'].isin(status_incluidos)

            df_filtered = df.loc[mask_year & mask_month & mask_team & mask_pix & mask_status & mask_ativo]

            df_10 = df_filtered
            transacoes_menos_taxa = df_10.shape[0]
            transacoes_formatadas = "{:,.0f}".format(transacoes_menos_taxa).replace('.', ',').replace(',', '.', 1)
            total = df_10['VALOR_MENOS_TAXA'].sum()
            total_menos_taxa = "{:,.2f}".format(total).replace('.', ',').replace(',', '.', 1)
            fig10 = go.Figure()
            if toggle:
                fig10.add_trace(go.Indicator(mode='number',
                            title={"text": f"<span style='font-size:80%'>TOTAL MENOS TAXA</span><br><span style='font-size:70%'>COBRANÇAS: {transacoes_formatadas}</span><br><br><span style='font-size:150%';>R${total_menos_taxa}</span>"},
                            number_font={'color': 'white'}, 
                            value=0
                ))
            else:
                fig10.add_trace(go.Indicator(mode='number',
                            title={"text": f"<span style='font-size:80%'>TOTAL MENOS TAXA</span><br><span style='font-size:70%'>COBRANÇAS: {transacoes_formatadas}</span><br><br><span style='font-size:150%';>R${total_menos_taxa}</span>"},
                            number_font={'color': '#303030'}, 
                            value=0
                ))
            fig10.update_layout(main_config, height=170, template=template)
            fig10.update_layout({"margin": {"l": 0, "r": 0, "t": 125, "b": 0}})

        except Exception as e:
            print(f"Erro ao atualizar o graph10: {e}")
    return fig10


@app.callback(
    Output('graph13', 'figure'),
    [Input('radio-month', 'value'),
     Input('radio-year', 'value'),
     Input('radio-team', 'value'),
     Input('radio-pix', 'value'), 
     Input('radio-status-pix', 'value'), 
     Input('status-ativo', 'value'),
     Input(ThemeSwitchAIO.ids.switch("theme"), "value"),
     Input('interval-component', 'n_intervals')]  
)
def update_graph13(month_criacao, year_criacao, team, pix_type, status_list, status_ativo, toggle, n_intervals):
    with lock:
        try:
            template = template_theme1 if toggle else template_theme2
            mask_year = year_filter_criacao(year_criacao)
            mask_month = month_filter_criacao(month_criacao)
            mask_team = team_filter(team)
            mask_pix = pix_filter(pix_type)
            mask_status_pix = status_pix_filter(status_list) 
            mask_ativo = create_status_filter(status_ativo)
            status_incluidos = ['CONCLUIDO', 'CONCLUIDA', 'processing', 'executed', 'ATIVA', 'CANCELADO' ]
            mask_status = df['status'].isin(status_incluidos)

            df_filtered2 = df.loc[mask_year & mask_month & mask_team & mask_pix & mask_status_pix & mask_ativo]  

            df_13 = df_filtered2
            transacoes_todos = df_13['valor'].count()
            transacoes_formatadas = "{:,.0f}".format(transacoes_todos).replace('.', ',').replace(',', '.', 1)
            fig13 = go.Figure()
            total = df_13['valor'].sum()
            total_criado = "{:,.2f}".format(total).replace('.', ',').replace(',', '.', 1)
            if toggle:
                fig13.add_trace(go.Indicator(
                        mode='number',
                        title={"text": f"<span style='font-size:80%'>TOTAL CRIADO</span><br><span style='font-size:70%'>COBRANÇAS: {transacoes_formatadas}</span><br><br><span style='font-size:150%';>R${total_criado}</span>"},
                        number_font={'color': 'white'},
                        value=0
                ))
            else:
                fig13.add_trace(go.Indicator(
                        mode='number',
                        title={"text": f"<span style='font-size:80%'>TOTAL CRIADO</span><br><span style='font-size:70%'>COBRANÇAS: {transacoes_formatadas}</span><br><br><span style='font-size:150%';>R${total_criado}</span>"},
                        number_font={'color': '#303030'},
                        value=0
                ))
            fig13.update_layout(main_config, height=170, template=template,)
            fig13.update_layout({"margin": {"l": 0, "r": 0, "t": 125, "b": 0}})

        except Exception as e:
            print(f"Erro ao atualizar o graph13: {e}")
    return fig13


@app.callback(
    Output('graph15', 'figure'),
    [Input('radio-month', 'value'),
     Input('radio-year', 'value'),
     Input('radio-team', 'value'),
     Input('radio-pix', 'value'), 
     Input('status-ativo', 'value'),
     Input('radio-status-pix', 'value'), 
     Input(ThemeSwitchAIO.ids.switch("theme"), "value"),
     Input('interval-component', 'n_intervals')]  
)
def update_graph15(month_criacao, year_criacao, team, pix_type, status_ativo, status_list, toggle, n_intervals):
    with lock:
        try:
            template = template_theme1 if toggle else template_theme2
            mask_year = year_filter_criacao(year_criacao)
            mask_month = month_filter_criacao(month_criacao)
            mask_team = team_filter(team)
            mask_pix = pix_filter(pix_type)
            mask_ativo = create_status_filter(status_ativo)
            mask_status_pix = status_pix_filter(status_list) 
            status_incluidos = ['CONCLUIDO', 'CONCLUIDA', 'processing', 'executed', 'ATIVA', 'CANCELADO' ]
            mask_status = df['status'].isin(status_incluidos)

            df_filtered2 = df.loc[mask_year & mask_month & mask_team & mask_pix & mask_status_pix & mask_ativo]  
        
            
            df_15 = df_filtered2[df_filtered2['status'] == 'ATIVA']
            transacoes_ativas = df_15.shape[0]
            transacoes_formatadas = "{:,.0f}".format(transacoes_ativas).replace('.', ',').replace(',', '.', 1)
            fig15 = go.Figure()
            total = df_15['valor'].sum()
            total_aberto = "{:,.2f}".format(total).replace('.', ',').replace(',', '.', 1)  
            if toggle:       
                fig15.add_trace(go.Indicator(
                        mode='number',
                        title={"text": f"<span style='font-size:80%'>TOTAL ABERTO</span><br><span style='font-size:70%'>COBRANÇAS: {transacoes_formatadas}</span><br><br><span style='font-size:150%';>R${total_aberto}</span>"},
                        number_font={'color': 'white'},
                        value=0

                ))
            else:
                fig15.add_trace(go.Indicator(
                        mode='number',
                        title={"text": f"<span style='font-size:80%'>TOTAL ABERTO</span><br><span style='font-size:70%'>COBRANÇAS: {transacoes_formatadas}</span><br><br><span style='font-size:150%';>R${total_aberto}</span>"},
                        number_font={'color': '#303030'},
                        value=0

                ))
            fig15.update_layout(main_config, height=170, template=template)
            fig15.update_layout({"margin": {"l": 0, "r": 0, "t": 125, "b": 0}})

        except Exception as e:
            print(f"Erro ao atualizar o graph15: {e}")
    return fig15


@app.callback(
    Output('graph16', 'figure'),
    Input('radio-month', 'value'),
    Input('radio-year', 'value'),
    Input('radio-team', 'value'),
    Input('radio-pix', 'value'), 
    Input('status-ativo', 'value'),
    Input('radio-status-pix', 'value'), 
    Input(ThemeSwitchAIO.ids.switch("theme"), "value"),
    Input('interval-component', 'n_intervals')  
)
def update_graph16(month_criacao, year_criacao, team, pix_type, status_ativo, status_list, toggle, n_intervals):
    with lock:
        try:
            template = template_theme1 if toggle else template_theme2
            mask_year = year_filter_criacao(year_criacao)
            mask_month = month_filter_criacao(month_criacao)
            mask_team = team_filter(team)
            mask_pix = pix_filter(pix_type)
            mask_status_pix = status_pix_filter(status_list) 
            mask_ativo = create_status_filter(status_ativo)
            status_incluidos = ['CONCLUIDO', 'CONCLUIDA', 'processing', 'executed', 'ATIVA', 'CANCELADO' ]
            mask_status = df['status'].isin(status_incluidos)

            df_filtered2 = df.loc[mask_year & mask_month & mask_team & mask_pix & mask_status_pix & mask_ativo] 

            df_16 = df_filtered2[df_filtered2['status'] == 'CANCELADO']
            transacoes_canceladas = df_16.shape[0]
            transacoes_formatadas = "{:,.0f}".format(transacoes_canceladas).replace('.', ',').replace(',', '.', 1)
            total = df_16['valor'].sum()
            total_expirado = "{:,.2f}".format(total).replace('.', ',').replace(',', '.', 1)
            fig16 = go.Figure()
            if toggle:
                fig16.add_trace(go.Indicator(
                    mode='number',
                    title={"text": f"<span style='font-size:80%; color:red'>TOTAL EXPIRADO</span><br><span style='font-size:70%; color:red'>COBRANÇAS: {transacoes_formatadas}</span><br><br><span style='font-size:150%; color:red';>R${total_expirado}</span>"},
                    value=0,
                    number_font={'color': 'white'}, 
            ))
            else:
                fig16.add_trace(go.Indicator(
                    mode='number',
                    title={"text": f"<span style='font-size:80%; color:red'>TOTAL EXPIRADO</span><br><span style='font-size:70%; color:red'>COBRANÇAS: {transacoes_formatadas}</span><br><br><span style='font-size:150%; color:red';>R${total_expirado}</span>"},
                    value=0,
                    number_font={'color': '#303030'}, 
                ))

            fig16.update_layout(main_config, height=170, template=template)
            fig16.update_layout({"margin": {"l": 0, "r": 0, "t": 125, "b": 0}})
            
        except Exception as e:
            print(f"Erro ao atualizar o graph16: {e}")
    return fig16


@app.callback(
    Output('graph17', 'figure'),
     [Input('radio-team', 'value'),
     Input('radio-pix', 'value'), 
     Input('radio-status-pix', 'value'),
     Input(ThemeSwitchAIO.ids.switch("theme"), "value"),
     Input('interval-component', 'n_intervals')]  
)
def update_graph17(team, pix_type, status_list, toggle, n_intervals):
    with lock:
        try:
            template = template_theme1 if toggle else template_theme2
            mask_team = team_filter(team)
            mask_pix = pix_filter(pix_type)
            status_incluidos = ['CONCLUIDO', 'CONCLUIDA', 'processing', 'executed']
            mask_status = df['status'].isin(status_incluidos)
            

            df_filtered = df.loc[mask_team & mask_pix & mask_status ]

            df_17 = df_filtered
            transacoes_taxa = df_17.shape[0]
            transacoes_formatadas = "{:,.0f}".format(transacoes_taxa).replace('.', ',').replace(',', '.', 1)
            total = df_17['taxa_total'].sum()
            total_taxa = "{:,.2f}".format(total).replace('.', ',').replace(',', '.', 1)
            fig17 = go.Figure()
            if toggle:
                fig17.add_trace(go.Indicator(mode='number',
                                title={"text": f"<span style='font-size:80%; color:#1cb49c'>TAXA TOTAL</span><br><span style='font-size:70%; color:#1cb49c'>COBRANÇAS: {transacoes_formatadas}</span><br><br><span style='font-size:150%; color:#1cb49c'>R${total_taxa}</span>"},
                                number_font={'color': 'white'},
                                value=0
                ))
            else:
                fig17.add_trace(go.Indicator(mode='number',
                                title={"text": f"<span style='font-size:80%; color:#1cb49c'>TAXA TOTAL</span><br><span style='font-size:70%; color:#1cb49c'>COBRANÇAS: {transacoes_formatadas}</span><br><br><span style='font-size:150%; color:#1cb49c'>R${total_taxa}</span>"},
                                number_font={'color': '#303030'},
                                value=0
                ))
            fig17.update_layout(main_config, height=170, template=template)
            fig17.update_layout({"margin": {"l": 0, "r": 0, "t": 125, "b": 0}})

        except Exception as e:
            print(f"Erro ao atualizar gráficos 17: {e}")
    return fig17



@app.callback(
    Output("radio-year", "options"),
    Output("radio-year", "value"),
    Output("radio-month", "options"),
    Output("radio-month", "value"),
    Output("radio-team", "options"),
    Output("radio-team", "value"),
    Input('interval-component', 'n_intervals'),
    Input('radio-year', 'value'),
    Input('status-ativo', 'value')  # Adicionando o input para o status
)
def update_radio_buttons(n_intervals, selected_year, selected_status):
    with lock:
        try:
            mes_atual = datetime.datetime.now().month
            ano_atual = datetime.datetime.now().year
            unique_years = sorted(df['ANO'].unique(), reverse=True)
            options_year = [{'label': i, 'value': i} for i in unique_years]

            selected_year = selected_year or ano_atual

            if selected_year:
                df_filtered = df[df['ANO'] == selected_year]
                options_month = [{'label': convert_to_text(i), 'value': j} for i, j in zip(df_filtered['MES'].unique(), df_filtered['MES'].unique())]
                options_month = sorted(options_month, key=lambda x: x['value'])

                if selected_year == ano_atual:
                    default_month = mes_atual
                else:
                    default_month = options_month[0]['value']
            else:
                options_month = []
                default_month = options_month[0]['value'] if options_month else None

            options_team = [{'label': 'Todas as Empresas', 'value': 0}]
            
            # Filtrando as empresas com base no status selecionado
            if selected_status == 'ATIVO':
                df_status_filtered = df[df['ativo'] == 'ATIVO']
            elif selected_status == 'INATIVO':
                df_status_filtered = df[df['ativo'] == 'INATIVO']
            else:
                df_status_filtered = df
            
            for i in df_status_filtered['Fantasia'].unique():
                options_team.append({'label': i, 'value': i})
        except Exception as e:
            print(f"Erro ao obter dados do Firebird: {e}")

    return options_year, selected_year, options_month, default_month, options_team, 0


login_data = {
    'admin': 'admin',
    'camilo': 'moda@1010',
    'julia': 'moda@1010'
}

@app.callback(
    Output('login-output', 'children'),
    [Input('login-button', 'n_clicks')],
    [dash.dependencies.State('username', 'value'),
     dash.dependencies.State('password', 'value')]
)
def check_login(n_clicks, username, password):
    global authenticated
    
    if n_clicks:
        if username.lower() in login_data and password == login_data[username.lower()]:
            authenticated = True 
            return dcc.Location(pathname='/home', id='main_layout_redirect')
        else:
            return html.Div('Credenciais inválidas. Tente novamente.', style={'color': 'red'})


@app.callback(
    Output('url', 'pathname'),
    [Input('main_layout_redirect', 'pathname')]
)
def update_url(pathname):
    if pathname is not None:
        return pathname

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])

@app.callback(
    Output('page-content', 'children'),
    [Input('url', 'pathname')]
)
def display_page(pathname):
    global authenticated 
    if pathname == '/home' and authenticated:  
        return main_layout
    elif not authenticated:  
        return login_layout  
    else:
        return login_layout 
         
@app.callback(
    Output('logout-output', 'children'),
    [Input('logout-button', 'n_clicks')],
    [State('url', 'pathname')]
)
def update_output(n_clicks, pathname):
    if n_clicks is None:
        raise PreventUpdate
    return dcc.Location(pathname='/', id='main_layout_redirect')

mode = 'dev'

if __name__ == '__main__':
    if mode == 'dev':
        app.run(host='0.0.0.0', port='8051')
    else:
        serve(app.server, host='0.0.0.0', port='8050', threads=30)


