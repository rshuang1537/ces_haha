import numpy as np
import pandas as pd
import json
import streamlit as st
from st_aggrid import AgGrid
from st_aggrid.grid_options_builder import GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode
import plotly.express as px
from streamlit_plotly_events import plotly_events
from PIL import Image
import plotly
import plotly.figure_factory as ff
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from itertools import cycle
import requests
import io

from datetime import timedelta
# from azure.kusto.data import KustoClient, KustoConnectionStringBuilder, ClientRequestProperties
# from azure.kusto.data.exceptions import KustoServiceError
# from azure.kusto.data.helpers import dataframe_from_result_table
import warnings
warnings.filterwarnings('ignore')

st.set_page_config(layout="wide")

db = 'VisionOne'


def get_image(path, title):  
    image = Image.open(path)
    st.image(image, caption=title, use_column_width=True)

# onboard_flow = get_image(r'/Users/greenrmp/Documents/screenshot/onboard_flow.png', 'flow')
# onboard_logic = get_image(r'/Users/greenrmp/Documents/screenshot/onboard_logic_haha.png', 'logic')
#status_pic = get_image(r'/Users/alan_huang/Documents/trendmicro/DS_C1WS_Migration/migration_status_demo_new.png', 'Migration Demo')

# @st.experimental_memo(suppress_st_warning=True)
# def query_adx(db, query):
    
#     with open(r'/Users/alan_huang/auth/kusto_auth.json', 'r') as f:
#         file = json.load(f)  
#     cluster = file['cluster']
#     client_id = file['client_id']
#     authority_id = file['authority_id']

#     with open(r'/Users/alan_huang/auth/kusto_secret.json', 'r') as f:
#         file = json.load(f)  
#     client_secret = file['client_secret']

#     kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster, 
#                                                                                 client_id, 
#                                                                                 client_secret, 
#                                                                                 authority_id)
#     client = KustoClient(kcsb)
#     response = client.execute(db, query)
#     df = dataframe_from_result_table(response.primary_results[0])
#     return df

# @st.experimental_memo(suppress_st_warning=True)
# def query_adx_huge(db, query, records = 15371537, size = 1537153715):
#     '''
#     Query ADX and return to dataframe.
#     Overrides the default maximum number of records and default maximum data size a query is allowed to return to
#     '''
#     with open(r'/Users/alan_huang/auth/kusto_auth.json', 'r') as f:
#         file = json.load(f)  
#     cluster = file['cluster']
#     client_id = file['client_id']
#     authority_id = file['authority_id']

#     with open(r'/Users/alan_huang/auth/kusto_secret.json', 'r') as f:
#         file = json.load(f)  
#     client_secret = file['client_secret']

#     kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster, 
#                                                                                 client_id, 
#                                                                                 client_secret, 
#                                                                                 authority_id)
#     properties_huge = ClientRequestProperties()
#     properties_huge.set_option(properties_huge.results_defer_partial_query_failures_option_name, True)
#     properties_huge.set_option(properties_huge.request_timeout_option_name, timedelta(seconds=30 * 60))
#     properties_huge.set_option("truncationmaxrecords", records)
#     properties_huge.set_option("truncationmaxsize", size) #1.5G 啦
#     #properties_huge.set_option("notruncation", True) # 大絕最後出 

#     client = KustoClient(kcsb)
    
#     response = client.execute(db, query, properties=properties_huge)
#     df = dataframe_from_result_table(response.primary_results[0])
#     return df

st.markdown('# Customer Engagement Score 2.0')

query = f'''
            ces_exp
            | where Data_Date == datetime('2022-11-29')
            | project-reorder Data_Date, CLP_ID, Total_Score, Score_PR, Login_Score, XDR_Threat_Invest_Score, RI_Score, Workflow_and_Automation_Score, TI_Score, ZTSA_Score, Sensor_Deployment_Score
            
        '''

#df_base = query_adx_huge(db, query)

@st.experimental_memo(suppress_st_warning=True)
def load_data(path):
    #df = pd.read_csv(path, encoding = 'utf-8')
    #url = 'https://raw.githubusercontent.com/rshuang1537/ces_haha/main/ces_base.csv'
    s = requests.get(path).content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')), encoding = 'utf-8')

    for i in ['Login_details', 'XDR_Threat_Invest_details', 'RI_details', 'Workflow_and_Automation_details', 'TI_details', 'ZTSA_details', 'Sensor_Deployment_details']:
        df[i] = df[i].apply(lambda x: str(x))
        df[i] = df[i].apply(lambda x: json.loads(str(x).replace("\'", "\"").replace('None', '0')) if x != 'nan' else '')
    return df

df_base = load_data(r'https://raw.githubusercontent.com/rshuang1537/ces_haha/main/ces_base.csv') 

#AgGrid(df_base.head(1))

Total_Score = round(df_base.Total_Score.mean(), 1)
Login_Score = round(df_base.Login_Score.mean(), 1)
XDR_Threat_Invest_Score = round(df_base.XDR_Threat_Invest_Score.mean(), 1)
RI_Score = round(df_base.RI_Score.mean(), 1)
Workflow_and_Automation_Score = round(df_base.Workflow_and_Automation_Score.mean(), 1)
TI_Score = round(df_base.TI_Score.mean(), 1)
ZTSA_Score = round(df_base.ZTSA_Score.mean(), 1)
Sensor_Deployment_Score = round(df_base.Sensor_Deployment_Score.mean(), 1)

st.markdown(f'#### AVG : {Total_Score}')

details = f'''= Login_Score({Login_Score}) + RI_Score({RI_Score}) + Threat_Invest_Score({XDR_Threat_Invest_Score}) +  Workflow_Automation_Score({Workflow_and_Automation_Score}) + TI_Score({TI_Score}) + ZTSA_Score({ZTSA_Score}) + Sensor_Deployment_Score({Sensor_Deployment_Score})'''

st.markdown(f'###### {details}')

var = 'Login_Score'

colt1, colt2 = st.columns([1, 1])

with colt1:
    if st.button('Total_Score'):
        var = 'Total_Score'
    else:
        pass

with colt2:
    if st.button('Score_PR'):
        var = 'Score_PR'
    else:
        pass

st.write('----------------------------------------------------------------------------------')
st.write('Details Score')

col1, col2, col3, col4, col5, col6, col7, col8 = st.columns([1, 1.35, 1, 1.8, 1, 1, 1, 1])

with col1:
    if st.button('Login_Score'):
        var = 'Login_Score'
    else:
        pass

with col2:
    if st.button('Threat_Invest_Score'):
        var = 'XDR_Threat_Invest_Score'
    else:
        pass

with col3:
    if st.button('RI_Score'):
        var = 'RI_Score'
    else:
        pass

with col4:
    if st.button('Workflow_Automation_Score'):
        var = 'Workflow_and_Automation_Score'
    else:
        pass

with col5:
    if st.button('TI_Score'):
        var = 'TI_Score'
    else:
        pass

with col6:
    if st.button('ZTSA_Score'):
        var = 'ZTSA_Score'
    else:
        pass

with col7:
    if st.button('Sensor_Deployment_Score'):
        var = 'Sensor_Deployment_Score'
    else:
        pass

#st.write(var)


@st.experimental_memo(suppress_st_warning=True)
def fig_ces2(df = df_base, var = 'Score_PR'):
    score_avg = round(df[var].mean(), 1)
    
    df = (
        df
        .groupby(['Data_Date', var])
        .agg(CLP_cnt = ('CLP_ID', 'nunique'))
        .reset_index()
        .sort_values(by = var)
        .assign(
            accum_ttl = lambda x: x.CLP_cnt.cumsum(axis = 0),
            accumu_perc = lambda x: (100*x['accum_ttl'] / x.groupby('Data_Date')['accum_ttl'].transform('max')),
            accumu_percentage = lambda x: x.accumu_perc.apply(lambda x: round(x))
        )
    )
    
    fig1 = px.bar(df, 
                  x = var,
                  y = 'CLP_cnt')

    fig2 = px.line(df, 
                   x = var,
                   y = 'accumu_percentage')
                #markers=True)
    fig_a = make_subplots(specs=[[{"secondary_y": True}]])#specs=[[{"secondary_y": True}]] rows=2, cols=1, , shared_xaxes=True, vertical_spacing=0.01,

    #fig1.data[i].name = fig1.data[i].name + ' Customer Count'
    fig_a.add_trace(fig1.data[0], secondary_y = False)
    fig_a.add_trace(fig2.data[0], secondary_y = True)


    fig_a.update_xaxes(title_text = f'{var}')
    fig_a.update_yaxes(showgrid=False, title_text = 'Customer Count', secondary_y = False)
    fig_a.update_yaxes(gridcolor='gray', title_text = 'Accum Customer Count(%)', secondary_y = True)
    fig_a.update_yaxes(range = [0, 105], secondary_y=True)
    fig_a.update_layout(#barmode = 'stack', 
                        #legend_title_text='Channel',
                        #legend_traceorder='reversed',
                        title = f'Engagement Score 2.0 : {var} ( {score_avg} avg )', 
                        autosize=True, 
                        width=1200, 
                        height=900)

    fig_a.update_traces(#overwrite=True, 
                        #marker_color = c1_conv.view,
                        #marker={"opacity": 0.3}, 
                        marker = dict(opacity=0.5),#ß color=5, colorscale='turbo_r'),
                        #marker_line_width = 50, 
                        #selector=dict(type="bar"),
                        secondary_y=False)

    fig_a.update_traces(#overwrite=True, 
                        line={"width": 5}, 
                        #marker_line_width = 50, 
                        #selector=dict(type="bar"),
                        secondary_y=True)

    #fig_a.update_yaxes(showgrid=False)

    #fig_a.update_layout(legend=dict( orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1 ))# 開啟水平顯示
    #color = px.colors.qualitative.Plotly[i]

    fig_a.data[0].marker.color = px.colors.qualitative.Plotly[0]
    fig_a.data[1].line.color = px.colors.qualitative.Plotly[4]
    
    return fig_a

ces_distribution = fig_ces2(var = var)

st.plotly_chart(ces_distribution)


gb = GridOptionsBuilder.from_dataframe(df_base)
gb.configure_column('CLP_ID', pinned='left')
gb.configure_pagination()
gb.configure_side_bar()
gb.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
gb.configure_selection(selection_mode="single") #, use_checkbox=True

# for col in df_base.columns.values.tolist():
#     gb.configure_column(col, suppressMovable=True, suppressMenu=True)
gridOptions = gb.build()
ces_list = AgGrid(df_base, 
                gridOptions=gridOptions, 
                enable_enterprise_modules=True, 
                allow_unsafe_jscode=True,
                height=600,
                theme='balham', #'balham','alpine','material','',''
                update_mode=GridUpdateMode.SELECTION_CHANGED)


if len(ces_list['selected_rows']) > 0 :

    #Login_details = ces_list['selected_rows'][0]['Login_details']
    CLP = ces_list['selected_rows'][0]['CLP_ID']
    #st.write(Login_details)
    st.markdown(f'#### CLP_ID : {CLP}')
    #Login_details_inf = df_base[df_base.CLP_ID == CLP]['Login_details'].tolist()[0]
    #st.write(Login_details_inf)
    #st.write(ces_list)

    df_target = df_base[df_base.CLP_ID == CLP]

    col = ['Data_Date', 'CLP_ID', 'Total_Score', 
        'Login_Score', 'XDR_Threat_Invest_Score', 'RI_Score', 
        'Workflow_and_Automation_Score', 'TI_Score', 'ZTSA_Score', 'Sensor_Deployment_Score']

    df_polar = df_target[col]
    df_polar = pd.melt(df_polar, 
                    id_vars=['Data_Date', 'CLP_ID', 'Total_Score'],
                    var_name='score_flg', value_name='score'
                    )

    fig = px.line_polar(df_polar, 
                        r='score', 
                        theta='score_flg', 
                        line_close=True,
                        markers=True,
                        line_shape= 'spline',
                        template='plotly_dark',
                        custom_data = ['score_flg', 'Data_Date'],
                        width=1100, 
                        height=700
                        )
    fig.update_traces(fill='toself')
    fig.update_layout(
                        # barmode = 'stack', 
                        # legend_title_text='Channel',
                        # legend_traceorder='reversed',
                        autosize=True, 
                        font_size = 20,
                        width=1100, 
                        height=700)

    
    cola1, cola2,  = st.columns([4, 1.1])
    #st.plotly_chart(fig)
    with cola1:
        selected_points = plotly_events(fig, override_height=700)

    var_map = {
                7:'Login_details', 
                1:'XDR_Threat_Invest_details', 
                2:'RI_details', 
                3:'Workflow_and_Automation_details', 
                4:'TI_details', 
                5:'ZTSA_details', 
                6:'Sensor_Deployment_details'
            }

    if len(selected_points) > 0 :
        #st.write(selected_points)
        var_index = selected_points[0]['pointIndex']
        #st.write(var_index)
        var_name = var_map[var_index]
        #st.write(var_name)
        details_inf = df_base[df_base.CLP_ID == CLP][var_name].tolist()[0]

        with cola2:
            st.markdown(f'#### {var_name}')
            st.write(details_inf)

    else:
        pass
            
else:
    #pass
    st.markdown(f'#### 我是誰')
    #pikachu = get_image(r'/Users/greenrmp/Downloads/pikachu.jpeg', 'pikachu')
    htp="https://raw.githubusercontent.com/rshuang1537/ces_haha/main/pikachu.jpeg"
    st.image(htp, caption= 'logo', width=1000)

