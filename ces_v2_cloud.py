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
import streamlit.components.v1 as components
from pprint import pprint
from collections import defaultdict

from datetime import timedelta
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder, ClientRequestProperties
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table
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

@st.experimental_memo(suppress_st_warning=True)
def load_data(path):
    #df = pd.read_csv(path, encoding = 'utf-8')
    #url = 'https://raw.githubusercontent.com/rshuang1537/ces_haha/main/ces_base.csv'
    s = requests.get(path).content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')), encoding = 'utf-8')

    if path == 'https://raw.githubusercontent.com/rshuang1537/ces_haha/main/ces_base.csv':
        for i in ['Login_details', 'XDR_Threat_Invest_details', 'RI_details', 'Workflow_and_Automation_details', 'TI_details', 'ZTSA_details', 'Sensor_Deployment_details']:
            df[i] = df[i].apply(lambda x: str(x))
            df[i] = df[i].apply(lambda x: json.loads(str(x).replace("\'", "\"").replace('None', '0')) if x != 'nan' else '')
    else:
        pass

    return df


License_Type_haha = st.sidebar.radio('License Type', ['All', 'Trial', 'Paid', 'Essential Access'])   
Region_haha = st.sidebar.radio('Region', 
                               [    "All",
                                    "Americas",
                                    "AMEA",
                                    "Europe",
                                    "Japan",
                                    "Corporate"
                                ])  

Sub_Region_Group_haha = st.sidebar.radio('Sub Region Group', 
                                   [   "All",
                                        "APAC exclude Pan Asia",
                                        "North America",
                                        "Latin America",
                                        "MMEA",
                                        "Southern Europe",
                                        "Japan",
                                        "Germany",
                                        "Nordics",
                                        "Pan Asia",
                                        "UK Region",
                                        "Benelux",
                                        "CEE",
                                        "Alps",
                                        "Corporate"
                                    ])   

st.markdown('# Customer Engagement Score 2.0')

# query = f'''
#             ces_exp_more
#             | project-reorder Data_Date, CLP_ID, Total_Score, Score_PR, Login_Score, XDR_Threat_Invest_Score, RI_Score, Workflow_and_Automation_Score, TI_Score, ZTSA_Score, Sensor_Deployment_Score
            
#         '''

# df_base = query_adx_huge(db, query)
df_base = load_data(r'https://raw.githubusercontent.com/rshuang1537/ces_haha/main/ces_base.csv') 

@st.experimental_memo(suppress_st_warning=True)
def base_pr_process(df):
    df['Score_PR'] = (100*df['Total_Score'].rank(pct = True, method='max')).round()
    df['Login_Score_PR'] = (100*df.Login_Score.rank(pct = True, method='max')).round()
    df['XDR_Threat_Invest_Score_PR'] = (100*df.XDR_Threat_Invest_Score.rank(pct = True, method='max')).round()
    df['RI_Score_PR'] = (100*df.RI_Score.rank(pct = True, method='max')).round()
    df['Workflow_and_Automation_Score_PR'] = (100*df.Workflow_and_Automation_Score.rank(pct = True, method='max')).round()
    df['TI_Score_PR'] = (100*df.TI_Score.rank(pct = True, method='max')).round()
    df['ZTSA_Score_PR'] = (100*df.ZTSA_Score.rank(pct = True, method='max')).round()
    df['Sensor_Deployment_Score_PR'] = (100*df.Sensor_Deployment_Score.rank(pct = True, method='max')).round()

    df['Score_PR'] = df.apply(lambda x: 0 if x.Total_Score == 0 else x.Score_PR, axis = 1)
    df['Login_Score_PR'] = df.apply(lambda x: 0 if x.Login_Score == 0 else x.Login_Score_PR, axis = 1)
    df['XDR_Threat_Invest_Score_PR'] = df.apply(lambda x: 0 if x.XDR_Threat_Invest_Score == 0 else x.XDR_Threat_Invest_Score_PR, axis = 1)
    df['RI_Score_PR'] = df.apply(lambda x: 0 if x.RI_Score == 0 else x.RI_Score_PR, axis = 1)
    df['Workflow_and_Automation_Score_PR'] = df.apply(lambda x: 0 if x.Workflow_and_Automation_Score == 0 else x.Workflow_and_Automation_Score_PR, axis = 1)
    df['TI_Score_PR'] = df.apply(lambda x: 0 if x.TI_Score == 0 else x.TI_Score_PR, axis = 1)
    df['ZTSA_Score_PR'] = df.apply(lambda x: 0 if x.ZTSA_Score == 0 else x.ZTSA_Score_PR, axis = 1)
    df['Sensor_Deployment_Score_PR'] = df.apply(lambda x: 0 if x.Sensor_Deployment_Score == 0 else x.Sensor_Deployment_Score_PR, axis = 1)

    return df

df_base = base_pr_process(df_base)



if License_Type_haha == 'All':
    # df_base = df_base
    pass
else :
    df_base = df_base[df_base.License_Type == License_Type_haha]
    
if Region_haha == 'All':
    # df_base = df_base
    pass
else :
    df_base = df_base[df_base.Region == Region_haha]
    
if Sub_Region_Group_haha == 'All':
    # df_base = df_base
    pass
else :
    df_base = df_base[df_base.Sub_Region_Group == Sub_Region_Group_haha]

# df for login tree graph
# query_login_tree = f'''
    
# let haha = (
#     ces_exp_more
#     //| where Login_Score > 0 //and toint(Login_details.week_cnt) > 3
#     //| take 7
#     | project 
#         CLP_ID, 
#         week_cnt = toint(Login_details.week_cnt),
#         week_cnt_ui = toint(Login_details.week_cnt_ui),
#         week_cnt_api = toint(Login_details.week_cnt_api),
#         License_Type, 
#         Region, 
#         Sub_Region_Group
#     | project 
#         CLP_ID,
#         method = case(week_cnt_ui>0 and week_cnt_api>0, 'UI and API',
#                     week_cnt_ui>0, 'Only UI',
#                     week_cnt_api>0, 'Only API',
#                     //week_cnt == 0, 'No usage',
#                     'No usage'
#                     ),
#         week_cnt,
#         License_Type, 
#         Region, 
#         Sub_Region_Group
# );
# let base = (
#     haha
#     | project level = 1, CLP_ID, node = 'Current Onboard', method, License_Type, Region, Sub_Region_Group
#     | union ( 
#         haha
#         | project level = 2, CLP_ID, node = method, method, License_Type, Region, Sub_Region_Group
#     )
#     | union ( 
#         haha
#         | where week_cnt > 0
#         | project level = 3, CLP_ID, node = strcat('week_cnt: ', tostring(week_cnt)), method, License_Type, Region, Sub_Region_Group
#         | extend node = case(method == 'UI and API', node,
#                              method == 'Only UI', strcat('ui_', node),
#                              method == 'Only API', strcat('api_', node),
#                              'weird')
#     )
#     | project 
#         level,
#         CLP_ID,
#         node,
#         License_Type, 
#         Region, 
#         Sub_Region_Group
# );
# base
# | project-rename node_bf = node
# | join kind = inner (
#     base
#     | project CLP_ID, node_af = node, level = level-1
# ) on CLP_ID, level
# | project-away CLP_ID1
# //| summarize cnt = count() by level, node_bf, node_af

# '''

# df_login_tree_base = query_adx_huge(db, query_login_tree)

df_login_tree_raw = load_data(r'https://raw.githubusercontent.com/rshuang1537/ces_haha/main/ces_login_tree_base.csv') 

@st.experimental_memo(suppress_st_warning=True)
def df_filter(df, License_Type_haha = 'All',Region_haha = 'All', Sub_Region_Group_haha = 'All'):

    if License_Type_haha == 'All':
        # df_base = df_base
        pass
    else :
        df = df[df.License_Type == License_Type_haha]
        
    if Region_haha == 'All':
        # df_base = df_base
        pass
    else :
        df = df[df.Region == Region_haha]
        
    if Sub_Region_Group_haha == 'All':
        # df_base = df_base
        pass
    else :
        df = df[df.Sub_Region_Group == Sub_Region_Group_haha]

    return df


df_login_tree_base = df_filter(df_login_tree_raw, License_Type_haha, Region_haha, Sub_Region_Group_haha)

df_login_tree_base = df_login_tree_base.groupby(['level', 'node_bf', 'node_af']).agg(cnt = ('CLP_ID', 'nunique')).reset_index()

ttl = df_login_tree_base[df_login_tree_base.level==1].cnt.sum()
ttl_str = ['Current Onboard'+' ('+str(ttl)+') ']
mapping = df_login_tree_base.copy()
mapping['value'] = mapping.apply(lambda x: str(x.node_af)+' ('+str(x.cnt)+') ', axis=1)
mapping = mapping[['node_af', 'value']].set_index('node_af').T.to_dict('list')
mapping['Current Onboard'] = ttl_str

df_login_tree_final = df_login_tree_base.copy()
df_login_tree_final['node_bf'] = df_login_tree_final['node_bf'].apply(lambda x: mapping[x][0])
df_login_tree_final['node_af'] = df_login_tree_final['node_af'].apply(lambda x: mapping[x][0])

df = df_login_tree_final.copy()
df = df.sort_values(by = ['node_af', 'node_bf'])
lastrow = len(df)
df.loc[lastrow] = np.nan
df.loc[lastrow, 'node_af'] = ttl_str[0]
df.loc[lastrow, 'node_bf'] = '' 

df['eid'] = df['node_af']
df = df[['eid', 'node_af', 'node_bf']] #get the order right



# def show_val(title, val):
#     sep = '-' * len(title)
#     print ("\n{0}\n{1}\n{2}\n".format(sep, title, sep))
#     pprint(val)

def buildtree(t=None, parent_eid=''):
    """
    Given a parents lookup structure, construct
    a data hierarchy.
    """
    parent = parents.get(parent_eid, None)
    if parent is None:
        return t
    for eid, name, mid in parent:
        if mid == '': report = {'parent': 'null', 'name': name, 'edge_name': name }
        else : report = {'parent': mid, 'name': name, 'edge_name': name }
        if t is None:
            t = report
        else:
            reports = t.setdefault('children', [])
            reports.append(report)
        buildtree(report, eid)
    return t

people = list(df.itertuples(index=False, name=None))

parents = defaultdict(list)
for p in people:
    parents[p[2]].append(p)
tree = buildtree()



h = f"""
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">

    <title>Tree Example</title>

    <style>
	
	.node {{
		cursor: pointer;
	}}

	.node circle {{
	  fill: #fff;
	  stroke: steelblue;
	  stroke-width: 3px;
	}}

	.node text {{
	  font: 14px sans-serif;
	}}

	.link {{
	  fill: none;
	  stroke: #ccc;
	  stroke-width: 2px;
	}}
	
    </style>

  </head>

  <body>

<!-- load the d3.js library -->	
<script src="https://cdnjs.cloudflare.com/ajax/libs/d3/3.5.17/d3.min.js"></script>
	
<script>



var treeData = [
{tree}
];


// ************** Generate the tree diagram	 *****************
var margin = {{top: 20, right: 120, bottom: 20, left: 180}},
	width = 1440 - margin.right - margin.left,//960
	height = 550 - margin.top - margin.bottom;//750
	
var i = 0,
	duration = 750,
	root;

var tree = d3.layout.tree()
	.size([height, width]);

var diagonal = d3.svg.diagonal()
	.projection(function(d) {{ return [d.y, d.x]; }});

var svg = d3.select("body").append("svg")
	.attr("width", width + margin.right + margin.left)
	.attr("height", height + margin.top + margin.bottom)
  .append("g")
	.attr("transform", "translate(" + margin.left + "," + margin.top + ")");

root = treeData[0];
root.x0 = height / 2;
root.y0 = 0;
  
update(root);

d3.select(self.frameElement).style("height", "800px");//,"width", "1500" haha

function update(source) {{

  // Compute the new tree layout.
  var nodes = tree.nodes(root).reverse(),
	  links = tree.links(nodes);

  // Normalize for fixed-depth.
  nodes.forEach(function(d) {{ d.y = d.depth * 200; }});//180 haha

  // Update the nodes…
  var node = svg.selectAll("g.node")
	  .data(nodes, function(d) {{ return d.id || (d.id = ++i); }});

  // Enter any new nodes at the parent's previous position.
  var nodeEnter = node.enter().append("g")
	  .attr("class", "node")
	  .attr("transform", function(d) {{ return "translate(" + source.y0 + "," + source.x0 + ")"; }})
	  .on("click", click);

  nodeEnter.append("circle")
	  .attr("r", 1e-6)
	  .style("fill", function(d) {{ return d._children ? "lightsteelblue" : "#fff"; }});

  nodeEnter.append("text")
	  .attr("x", function(d) {{ return d.children || d._children ? -15 : 15; }})//-13 : 13
	  .attr("dy", ".35em")
	  .attr("text-anchor", function(d) {{ return d.children || d._children ? "end" : "start"; }})
	  .text(function(d) {{ return d.name; }})
	  .style("fill-opacity", 1e-6)
      .style('fill', 'gray');// haha

  // Transition nodes to their new position.
  var nodeUpdate = node.transition()
	  .duration(duration)
	  .attr("transform", function(d) {{ return "translate(" + d.y + "," + d.x + ")"; }});

  nodeUpdate.select("circle")
	  .attr("r", 10)
	  .style("fill", function(d) {{ return d._children ? "lightsteelblue" : "#fff"; }});

  nodeUpdate.select("text")
	  .style("fill-opacity", 1);

  // Transition exiting nodes to the parent's new position.
  var nodeExit = node.exit().transition()
	  .duration(duration)
	  .attr("transform", function(d) {{ return "translate(" + source.y + "," + source.x + ")"; }})
	  .remove();

  nodeExit.select("circle")
	  .attr("r", 1e-6);

  nodeExit.select("text")
	  .style("fill-opacity", 1e-6);

  // Update the links…
  var link = svg.selectAll("path.link")
	  .data(links, function(d) {{ return d.target.id; }});

  // Enter any new links at the parent's previous position.
  link.enter().insert("path", "g")
	  .attr("class", "link")
	  .attr("d", function(d) {{
		var o = {{x: source.x0, y: source.y0}};
		return diagonal({{source: o, target: o}});
	  }});

  // Transition links to their new position.
  link.transition()
	  .duration(duration)
	  .attr("d", diagonal);

  // Transition exiting nodes to the parent's new position.
  link.exit().transition()
	  .duration(duration)
	  .attr("d", function(d) {{
		var o = {{x: source.x, y: source.y}};
		return diagonal({{source: o, target: o}});
	  }})
	  .remove();

  // Stash the old positions for transition.
  nodes.forEach(function(d) {{
	d.x0 = d.x;
	d.y0 = d.y;
  }});
}}

// Toggle children on click.
function click(d) {{
  if (d.children) {{
	d._children = d.children;
	d.children = null;
  }} else {{
	d.children = d._children;
	d._children = null;
  }}
  update(d);
}}

</script>
	
  </body>
</html>
"""


Total_Score = round(df_base.Total_Score.mean(), 1)
Login_Score = round(df_base.Login_Score.mean(), 1)
XDR_Threat_Invest_Score = round(df_base.XDR_Threat_Invest_Score.mean(), 1)
RI_Score = round(df_base.RI_Score.mean(), 1)
Workflow_and_Automation_Score = round(df_base.Workflow_and_Automation_Score.mean(), 1)
TI_Score = round(df_base.TI_Score.mean(), 1)
ZTSA_Score = round(df_base.ZTSA_Score.mean(), 1)
Sensor_Deployment_Score = round(df_base.Sensor_Deployment_Score.mean(), 1)

st.markdown(f'## AVG : {Total_Score}')

details = f'''= Login_Score({Login_Score}) + RI_Score({RI_Score}) + Threat_Invest_Score({XDR_Threat_Invest_Score}) +  Workflow_Automation_Score({Workflow_and_Automation_Score}) + TI_Score({TI_Score}) + ZTSA_Score({ZTSA_Score}) + Sensor_Deployment_Score({Sensor_Deployment_Score})'''

st.markdown(f'#### {details}')

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

# show tree graph for login funnel

if var == 'Login_Score':
    components.html(h,height=600,
       #scrolling=scrolling,
       )
else:
    pass


# @st.experimental_memo(suppress_st_warning=True)
def fig_ces2(df = df_base, var = 'Score_PR'):
    score_avg = round(df[var].mean(), 1)


    # if License_Type_haha == 'All':
    #     pass
    # else :
    #     df = df[df.License_Type == License_Type_haha]
    
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
    
    fig1 = px.bar(df.assign(flg = 'Customer Count  (CLP)'), 
                  x = var,
                  y = 'CLP_cnt',
                  color = 'flg')

    fig2 = px.line(df.assign(flg = 'Accumulated Percentage'), 
                   x = var,
                   y = 'accumu_percentage',
                   color = 'flg')
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
                        width=1500, 
                        height=700)

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



CLP_ID_wish = st.text_input('make your wish  (CLP_ID)', '')

df_base_simple = df_base[['CLP_ID', 'Data_Date', 'Total_Score', 'Score_PR', 'Login_Score', 'XDR_Threat_Invest_Score', 'RI_Score', 
                          'Workflow_and_Automation_Score', 'TI_Score', 'ZTSA_Score', 'Sensor_Deployment_Score', 'Region', 'Sub_Region_Group']]

if CLP_ID_wish == '':
    pass
else:
    df_base_simple = df_base_simple[df_base_simple.CLP_ID == CLP_ID_wish]

gb = GridOptionsBuilder.from_dataframe(df_base_simple)
gb.configure_column('CLP_ID', pinned='left')
gb.configure_pagination()
gb.configure_side_bar()
gb.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
gb.configure_selection(selection_mode="single") #, use_checkbox=True

# for col in df_base.columns.values.tolist():
#     gb.configure_column(col, suppressMovable=True, suppressMenu=True)
gridOptions = gb.build()
ces_list = AgGrid(df_base_simple, 
                gridOptions=gridOptions, 
                enable_enterprise_modules=True, 
                allow_unsafe_jscode=True,
                height=400,
                theme='streamlit',#'dark', 'streamlit', #'balham','alpine','material','',''
                update_mode=GridUpdateMode.SELECTION_CHANGED)


if len(ces_list['selected_rows']) > 0:

    #Login_details = ces_list['selected_rows'][0]['Login_details']
    CLP = ces_list['selected_rows'][0]['CLP_ID']
    Region = ces_list['selected_rows'][0]['Region']
    Sub_Region_Group = ces_list['selected_rows'][0]['Sub_Region_Group']
    Score_PR = ces_list['selected_rows'][0]['Score_PR']
    #st.write(Login_details)
    st.markdown(f'#### CLP_ID : {CLP}')
    st.markdown(f'###### Score_PR : {Score_PR}')
    st.markdown(f'###### Region : {Region}')
    st.markdown(f'###### Sub Region Group : {Sub_Region_Group}')
    #Login_details_inf = df_base[df_base.CLP_ID == CLP]['Login_details'].tolist()[0]
    #st.write(Login_details_inf)
    #st.write(ces_list)

    df_target = df_base[df_base.CLP_ID == CLP]
    
    cola1, cola2,  = st.columns([4, 1.1])
    #st.plotly_chart(fig)
    with cola1:
        view = st.radio('', ('Score', 'Percentage Rank'))
        viewby = 1 if view == 'Score' else 2 if view == 'Percentage Rank' else 1
        if viewby == 1:
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
            #fig.set_ylim(0, 100)
            fig.update_traces(fill='toself')
            fig.update_polars(
                                angularaxis_categoryarray=['Login_Score', 'XDR_Threat_Invest_Score', 'RI_Score', 
                                                        'Workflow_and_Automation_Score', 'TI_Score', 'ZTSA_Score', 
                                                        'Sensor_Deployment_Score'],
                                #angularaxis_nticks=3,
                                #angularaxis_period=7,
                                #angularaxis_showgrid =False
                                #angularaxis_showline = False
                                #angularaxis_tickangle = 90 
                                #angularaxis_tickcolor = 'red' 
                                #angularaxis.tickformatstops[range(0,150)] 
                                #hole=0.1,
                                #radialaxis_angle = 270
                                radialaxis_range = [0,101],
                                angularaxis_tickfont_size=20,
                                radialaxis_tickfont_size = 15
                            )
            fig.update_layout(
                                # barmode = 'stack', 
                                # legend_title_text='Channel',
                                # legend_traceorder='reversed',
                                autosize=True, 
                                font_size = 20,
                                width=1100, 
                                height=700)

        else:
            col = ['Data_Date', 'CLP_ID', 'Total_Score', 
                   'Login_Score_PR', 'XDR_Threat_Invest_Score_PR', 'RI_Score_PR', 
                   'Workflow_and_Automation_Score_PR', 'TI_Score_PR', 'ZTSA_Score_PR', 'Sensor_Deployment_Score_PR']
            
            df_polar = df_target[col]
            df_polar = pd.melt(df_polar, 
                            id_vars=['Data_Date', 'CLP_ID', 'Total_Score'],
                            var_name='score_flg', value_name='Rank'
                            )

            fig = px.line_polar(df_polar, 
                                r='Rank', 
                                theta='score_flg', 
                                line_close=True,
                                markers=True,
                                line_shape= 'spline',
                                template='plotly_dark',
                                custom_data = ['score_flg', 'Data_Date'],
                                width=1100, 
                                height=700
                                )
            #fig.set_ylim(0, 100)
            fig.update_traces(fill='toself')
            fig.update_polars(
                                angularaxis_categoryarray=['Login_Score_PR', 'XDR_Threat_Invest_Score_PR', 'RI_Score_PR', 
                                                        'Workflow_and_Automation_Score_PR', 'TI_Score_PR', 'ZTSA_Score_PR', 
                                                        'Sensor_Deployment_Score_PR'],
                                #angularaxis_nticks=3,
                                #angularaxis_period=7,
                                #angularaxis_showgrid =False
                                #angularaxis_showline = False
                                #angularaxis_tickangle = 90 
                                #angularaxis_tickcolor = 'red' 
                                #angularaxis.tickformatstops[range(0,150)] 
                                #hole=0.1,
                                #radialaxis_angle = 270
                                radialaxis_range = [0,101],
                                angularaxis_tickfont_size=20,
                                radialaxis_tickfont_size = 15
                            )
            fig.update_layout(
                                # barmode = 'stack', 
                                # legend_title_text='Channel',
                                # legend_traceorder='reversed',
                                autosize=True, 
                                font_size = 20,
                                width=1100, 
                                height=700)
    
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




# import numpy as np
# import pandas as pd
# import json
# import streamlit as st
# from st_aggrid import AgGrid
# from st_aggrid.grid_options_builder import GridOptionsBuilder
# from st_aggrid.shared import GridUpdateMode
# import plotly.express as px
# from streamlit_plotly_events import plotly_events
# from PIL import Image
# import plotly
# import plotly.figure_factory as ff
# import plotly.graph_objects as go
# from plotly.subplots import make_subplots
# from itertools import cycle
# import requests
# import io

# from datetime import timedelta
# from azure.kusto.data import KustoClient, KustoConnectionStringBuilder, ClientRequestProperties
# from azure.kusto.data.exceptions import KustoServiceError
# from azure.kusto.data.helpers import dataframe_from_result_table
# import warnings
# warnings.filterwarnings('ignore')

# st.set_page_config(layout="wide")

# db = 'VisionOne'


# def get_image(path, title):  
#     image = Image.open(path)
#     st.image(image, caption=title, use_column_width=True)

# # onboard_flow = get_image(r'/Users/greenrmp/Documents/screenshot/onboard_flow.png', 'flow')
# # onboard_logic = get_image(r'/Users/greenrmp/Documents/screenshot/onboard_logic_haha.png', 'logic')
# #status_pic = get_image(r'/Users/alan_huang/Documents/trendmicro/DS_C1WS_Migration/migration_status_demo_new.png', 'Migration Demo')

# # @st.experimental_memo(suppress_st_warning=True)
# # def query_adx(db, query):
    
# #     with open(r'/Users/alan_huang/auth/kusto_auth.json', 'r') as f:
# #         file = json.load(f)  
# #     cluster = file['cluster']
# #     client_id = file['client_id']
# #     authority_id = file['authority_id']

# #     with open(r'/Users/alan_huang/auth/kusto_secret.json', 'r') as f:
# #         file = json.load(f)  
# #     client_secret = file['client_secret']

# #     kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster, 
# #                                                                                 client_id, 
# #                                                                                 client_secret, 
# #                                                                                 authority_id)
# #     client = KustoClient(kcsb)
# #     response = client.execute(db, query)
# #     df = dataframe_from_result_table(response.primary_results[0])
# #     return df

# # @st.experimental_memo(suppress_st_warning=True)
# # def query_adx_huge(db, query, records = 15371537, size = 1537153715):
# #     '''
# #     Query ADX and return to dataframe.
# #     Overrides the default maximum number of records and default maximum data size a query is allowed to return to
# #     '''
# #     with open(r'/Users/alan_huang/auth/kusto_auth.json', 'r') as f:
# #         file = json.load(f)  
# #     cluster = file['cluster']
# #     client_id = file['client_id']
# #     authority_id = file['authority_id']

# #     with open(r'/Users/alan_huang/auth/kusto_secret.json', 'r') as f:
# #         file = json.load(f)  
# #     client_secret = file['client_secret']

# #     kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster, 
# #                                                                                 client_id, 
# #                                                                                 client_secret, 
# #                                                                                 authority_id)
# #     properties_huge = ClientRequestProperties()
# #     properties_huge.set_option(properties_huge.results_defer_partial_query_failures_option_name, True)
# #     properties_huge.set_option(properties_huge.request_timeout_option_name, timedelta(seconds=30 * 60))
# #     properties_huge.set_option("truncationmaxrecords", records)
# #     properties_huge.set_option("truncationmaxsize", size) #1.5G 啦
# #     #properties_huge.set_option("notruncation", True) # 大絕最後出 

# #     client = KustoClient(kcsb)
    
# #     response = client.execute(db, query, properties=properties_huge)
# #     df = dataframe_from_result_table(response.primary_results[0])
# #     return df


# License_Type_haha = st.sidebar.radio('License Type', ['All', 'Trial', 'Paid', 'Essential Access'])    

# st.markdown('# Customer Engagement Score 2.0')

# # query = f'''
# #             ces_exp_more
# #             | where Data_Date == '2022-11-30'
# #             | project-reorder Data_Date, CLP_ID, Total_Score, Score_PR, Login_Score, XDR_Threat_Invest_Score, RI_Score, Workflow_and_Automation_Score, TI_Score, ZTSA_Score, Sensor_Deployment_Score
            
# #         '''
# # df_base = query_adx_huge(db, query)

# @st.experimental_memo(suppress_st_warning=True)
# def load_data(path):
#     #df = pd.read_csv(path, encoding = 'utf-8')
#     #url = 'https://raw.githubusercontent.com/rshuang1537/ces_haha/main/ces_base.csv'
#     s = requests.get(path).content
#     df = pd.read_csv(io.StringIO(s.decode('utf-8')), encoding = 'utf-8')

#     for i in ['Login_details', 'XDR_Threat_Invest_details', 'RI_details', 'Workflow_and_Automation_details', 'TI_details', 'ZTSA_details', 'Sensor_Deployment_details']:
#         df[i] = df[i].apply(lambda x: str(x))
#         df[i] = df[i].apply(lambda x: json.loads(str(x).replace("\'", "\"").replace('None', '0')) if x != 'nan' else '')
#     return df

# df_base = load_data(r'https://raw.githubusercontent.com/rshuang1537/ces_v2_haha/main/ces_v2_data.csv') 

# AgGrid(df_base.head())


# if License_Type_haha == 'All':
#     # df_base = df_base
#     pass
# else :
#     df_base = df_base[df_base.License_Type == License_Type_haha]

# Total_Score = round(df_base.Total_Score.mean(), 1)
# Login_Score = round(df_base.Login_Score.mean(), 1)
# XDR_Threat_Invest_Score = round(df_base.XDR_Threat_Invest_Score.mean(), 1)
# RI_Score = round(df_base.RI_Score.mean(), 1)
# Workflow_and_Automation_Score = round(df_base.Workflow_and_Automation_Score.mean(), 1)
# TI_Score = round(df_base.TI_Score.mean(), 1)
# ZTSA_Score = round(df_base.ZTSA_Score.mean(), 1)
# Sensor_Deployment_Score = round(df_base.Sensor_Deployment_Score.mean(), 1)

# st.markdown(f'## AVG : {Total_Score}')

# details = f'''= Login_Score({Login_Score}) + RI_Score({RI_Score}) + Threat_Invest_Score({XDR_Threat_Invest_Score}) +  Workflow_Automation_Score({Workflow_and_Automation_Score}) + TI_Score({TI_Score}) + ZTSA_Score({ZTSA_Score}) + Sensor_Deployment_Score({Sensor_Deployment_Score})'''

# st.markdown(f'#### {details}')

# var = 'Login_Score'

# colt1, colt2 = st.columns([1, 1])

# with colt1:
#     if st.button('Total_Score'):
#         var = 'Total_Score'
#     else:
#         pass

# with colt2:
#     if st.button('Score_PR'):
#         var = 'Score_PR'
#     else:
#         pass

# st.write('----------------------------------------------------------------------------------')
# st.write('Details Score')

# col1, col2, col3, col4, col5, col6, col7, col8 = st.columns([1, 1.35, 1, 1.8, 1, 1, 1, 1])

# with col1:
#     if st.button('Login_Score'):
#         var = 'Login_Score'
#     else:
#         pass

# with col2:
#     if st.button('Threat_Invest_Score'):
#         var = 'XDR_Threat_Invest_Score'
#     else:
#         pass

# with col3:
#     if st.button('RI_Score'):
#         var = 'RI_Score'
#     else:
#         pass

# with col4:
#     if st.button('Workflow_Automation_Score'):
#         var = 'Workflow_and_Automation_Score'
#     else:
#         pass

# with col5:
#     if st.button('TI_Score'):
#         var = 'TI_Score'
#     else:
#         pass

# with col6:
#     if st.button('ZTSA_Score'):
#         var = 'ZTSA_Score'
#     else:
#         pass

# with col7:
#     if st.button('Sensor_Deployment_Score'):
#         var = 'Sensor_Deployment_Score'
#     else:
#         pass

# #st.write(var)


# # @st.experimental_memo(suppress_st_warning=True)
# def fig_ces2(df = df_base, var = 'Score_PR'):
#     score_avg = round(df[var].mean(), 1)


#     # if License_Type_haha == 'All':
#     #     pass
#     # else :
#     #     df = df[df.License_Type == License_Type_haha]
    
#     df = (
#         df
#         .groupby(['Data_Date', var])
#         .agg(CLP_cnt = ('CLP_ID', 'nunique'))
#         .reset_index()
#         .sort_values(by = var)
#         .assign(
#             accum_ttl = lambda x: x.CLP_cnt.cumsum(axis = 0),
#             accumu_perc = lambda x: (100*x['accum_ttl'] / x.groupby('Data_Date')['accum_ttl'].transform('max')),
#             accumu_percentage = lambda x: x.accumu_perc.apply(lambda x: round(x))
#         )
#     )
    
#     fig1 = px.bar(df.assign(flg = 'Customer Count  (CLP)'), 
#                   x = var,
#                   y = 'CLP_cnt',
#                   color = 'flg')

#     fig2 = px.line(df.assign(flg = 'Accumulated Percentage'), 
#                    x = var,
#                    y = 'accumu_percentage',
#                    color = 'flg')
#                 #markers=True)
#     fig_a = make_subplots(specs=[[{"secondary_y": True}]])#specs=[[{"secondary_y": True}]] rows=2, cols=1, , shared_xaxes=True, vertical_spacing=0.01,

#     #fig1.data[i].name = fig1.data[i].name + ' Customer Count'
#     fig_a.add_trace(fig1.data[0], secondary_y = False)
#     fig_a.add_trace(fig2.data[0], secondary_y = True)


#     fig_a.update_xaxes(title_text = f'{var}')
#     fig_a.update_yaxes(showgrid=False, title_text = 'Customer Count', secondary_y = False)
#     fig_a.update_yaxes(gridcolor='gray', title_text = 'Accum Customer Count(%)', secondary_y = True)
#     fig_a.update_yaxes(range = [0, 105], secondary_y=True)
#     fig_a.update_layout(#barmode = 'stack', 
#                         #legend_title_text='Channel',
#                         #legend_traceorder='reversed',
#                         title = f'Engagement Score 2.0 : {var} ( {score_avg} avg )', 
#                         autosize=True, 
#                         width=1500, 
#                         height=700)

#     fig_a.update_traces(#overwrite=True, 
#                         #marker_color = c1_conv.view,
#                         #marker={"opacity": 0.3}, 
#                         marker = dict(opacity=0.5),#ß color=5, colorscale='turbo_r'),
#                         #marker_line_width = 50, 
#                         #selector=dict(type="bar"),
#                         secondary_y=False)

#     fig_a.update_traces(#overwrite=True, 
#                         line={"width": 5}, 
#                         #marker_line_width = 50, 
#                         #selector=dict(type="bar"),
#                         secondary_y=True)

#     #fig_a.update_yaxes(showgrid=False)

#     #fig_a.update_layout(legend=dict( orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1 ))# 開啟水平顯示
#     #color = px.colors.qualitative.Plotly[i]

#     fig_a.data[0].marker.color = px.colors.qualitative.Plotly[0]
#     fig_a.data[1].line.color = px.colors.qualitative.Plotly[4]
    
#     return fig_a

# ces_distribution = fig_ces2(var = var)

# st.plotly_chart(ces_distribution)

# df_base_simple = df_base[['CLP_ID', 'Data_Date', 'Total_Score', 'Score_PR', 'Login_Score', 'XDR_Threat_Invest_Score', 'RI_Score', 'Workflow_and_Automation_Score', 'TI_Score', 'ZTSA_Score', 'Sensor_Deployment_Score']]

# gb = GridOptionsBuilder.from_dataframe(df_base_simple)
# gb.configure_column('CLP_ID', pinned='left')
# gb.configure_pagination()
# gb.configure_side_bar()
# gb.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
# gb.configure_selection(selection_mode="single") #, use_checkbox=True

# # for col in df_base.columns.values.tolist():
# #     gb.configure_column(col, suppressMovable=True, suppressMenu=True)
# gridOptions = gb.build()
# ces_list = AgGrid(df_base_simple, 
#                 gridOptions=gridOptions, 
#                 enable_enterprise_modules=True, 
#                 allow_unsafe_jscode=True,
#                 height=400,
#                 theme='streamlit',#'dark', 'streamlit', #'balham','alpine','material','',''
#                 update_mode=GridUpdateMode.SELECTION_CHANGED)


# if len(ces_list['selected_rows']) > 0 :

#     #Login_details = ces_list['selected_rows'][0]['Login_details']
#     CLP = ces_list['selected_rows'][0]['CLP_ID']
#     #st.write(Login_details)
#     st.markdown(f'#### CLP_ID : {CLP}')
#     #Login_details_inf = df_base[df_base.CLP_ID == CLP]['Login_details'].tolist()[0]
#     #st.write(Login_details_inf)
#     #st.write(ces_list)

#     df_target = df_base[df_base.CLP_ID == CLP]

#     col = ['Data_Date', 'CLP_ID', 'Total_Score', 
#         'Login_Score', 'XDR_Threat_Invest_Score', 'RI_Score', 
#         'Workflow_and_Automation_Score', 'TI_Score', 'ZTSA_Score', 'Sensor_Deployment_Score']

#     df_polar = df_target[col]
#     df_polar = pd.melt(df_polar, 
#                     id_vars=['Data_Date', 'CLP_ID', 'Total_Score'],
#                     var_name='score_flg', value_name='score'
#                     )

#     fig = px.line_polar(df_polar, 
#                         r='score', 
#                         theta='score_flg', 
#                         line_close=True,
#                         markers=True,
#                         line_shape= 'spline',
#                         template='plotly_dark',
#                         custom_data = ['score_flg', 'Data_Date'],
#                         width=1100, 
#                         height=700
#                         )
#     fig.update_traces(fill='toself')
#     fig.update_layout(
#                         # barmode = 'stack', 
#                         # legend_title_text='Channel',
#                         # legend_traceorder='reversed',
#                         autosize=True, 
#                         font_size = 20,
#                         width=1100, 
#                         height=700)

    
#     cola1, cola2,  = st.columns([4, 1.1])
#     #st.plotly_chart(fig)
#     with cola1:
#         selected_points = plotly_events(fig, override_height=700)

#     var_map = {
#                 7:'Login_details', 
#                 1:'XDR_Threat_Invest_details', 
#                 2:'RI_details', 
#                 3:'Workflow_and_Automation_details', 
#                 4:'TI_details', 
#                 5:'ZTSA_details', 
#                 6:'Sensor_Deployment_details'
#             }

#     if len(selected_points) > 0 :
#         #st.write(selected_points)
#         var_index = selected_points[0]['pointIndex']
#         #st.write(var_index)
#         var_name = var_map[var_index]
#         #st.write(var_name)
#         details_inf = df_base[df_base.CLP_ID == CLP][var_name].tolist()[0]

#         with cola2:
#             st.markdown(f'#### {var_name}')
#             st.write(details_inf)

#     else:
#         pass
            
# else:
#     #pass
#     st.markdown(f'#### 我是誰')
#     # pikachu = get_image(r'/Users/greenrmp/Downloads/pikachu.jpeg', 'pikachu')
#     htp="https://raw.githubusercontent.com/rshuang1537/ces_v2_haha/main/pikachu.jpeg"
#     st.image(htp, caption= 'logo', width=1000)