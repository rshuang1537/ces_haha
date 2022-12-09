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



h = f"""
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">

    <title>Tree Example</title>

    <style>
	
	.node {
		cursor: pointer;
	}

	.node circle {
	  fill: #fff;
	  stroke: steelblue;
	  stroke-width: 3px;
	}

	.node text {
	  font: 14px sans-serif;
	}

	.link {
	  fill: none;
	  stroke: #ccc;
	  stroke-width: 2px;
	}
	
    </style>

  </head>

  <body>
    <h1></h1>TODO:
    <h1></h1>a.數值轉比例
    <h1></h1>b.加入觀測事件(特定action, behavior, result etc.)
    <h1></h1>c.Cross App haha



<!-- load the d3.js library -->	
<script src="https://cdnjs.cloudflare.com/ajax/libs/d3/3.5.17/d3.min.js"></script>
	
<script>




var treeData = [
{'parent': 'null', 'name': 'Current Onboard (12624) ', 'edge_name': 'Current Onboard (12624) ', 'children': [{'parent': 'Current Onboard (12624) ', 'name': 'ASD Page (2963) ', 'edge_name': 'ASD Page (2963) ', 'children': [{'parent': 'ASD Page (2963) ', 'name': 'Accounts (1547) ', 'edge_name': 'Accounts (1547) '}, {'parent': 'ASD Page (2963) ', 'name': 'Cloud Apps (1221) ', 'edge_name': 'Cloud Apps (1221) '}, {'parent': 'ASD Page (2963) ', 'name': 'Cloud Assets (975) ', 'edge_name': 'Cloud Assets (975) '}, {'parent': 'ASD Page (2963) ', 'name': 'Devices (2930) ', 'edge_name': 'Devices (2930) '}, {'parent': 'ASD Page (2963) ', 'name': 'Internet-Facing Assets (1677) ', 'edge_name': 'Internet-Facing Assets (1677) '}]}, {'parent': 'Current Onboard (12624) ', 'name': 'ED Page (6266) ', 'edge_name': 'ED Page (6266) ', 'children': [{'parent': 'ED Page (6266) ', 'name': 'attackOverview (2809) ', 'edge_name': 'attackOverview (2809) '}, {'parent': 'ED Page (6266) ', 'name': 'exposure (2290) ', 'edge_name': 'exposure (2290) '}, {'parent': 'ED Page (6266) ', 'name': 'riskOverview (6266) ', 'edge_name': 'riskOverview (6266) '}, {'parent': 'ED Page (6266) ', 'name': 'securityConfiguration (2428) ', 'edge_name': 'securityConfiguration (2428) '}]}, {'parent': 'Current Onboard (12624) ', 'name': 'No RI Access (6351) ', 'edge_name': 'No RI Access (6351) '}]}
];


// ************** Generate the tree diagram	 *****************
var margin = {top: 20, right: 120, bottom: 20, left: 180},
	width = 1440 - margin.right - margin.left,//960
	height = 550 - margin.top - margin.bottom;//750
	
var i = 0,
	duration = 750,
	root;

var tree = d3.layout.tree()
	.size([height, width]);

var diagonal = d3.svg.diagonal()
	.projection(function(d) { return [d.y, d.x]; });

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

function update(source) {

  // Compute the new tree layout.
  var nodes = tree.nodes(root).reverse(),
	  links = tree.links(nodes);

  // Normalize for fixed-depth.
  nodes.forEach(function(d) { d.y = d.depth * 200; });//180 haha

  // Update the nodes…
  var node = svg.selectAll("g.node")
	  .data(nodes, function(d) { return d.id || (d.id = ++i); });

  // Enter any new nodes at the parent's previous position.
  var nodeEnter = node.enter().append("g")
	  .attr("class", "node")
	  .attr("transform", function(d) { return "translate(" + source.y0 + "," + source.x0 + ")"; })
	  .on("click", click);

  nodeEnter.append("circle")
	  .attr("r", 1e-6)
	  .style("fill", function(d) { return d._children ? "lightsteelblue" : "#fff"; });

  nodeEnter.append("text")
	  .attr("x", function(d) { return d.children || d._children ? -15 : 15; })//-13 : 13
	  .attr("dy", ".35em")
	  .attr("text-anchor", function(d) { return d.children || d._children ? "end" : "start"; })
	  .text(function(d) { return d.name; })
	  .style("fill-opacity", 1e-6)
      .style('fill', 'gray');// haha

  // Transition nodes to their new position.
  var nodeUpdate = node.transition()
	  .duration(duration)
	  .attr("transform", function(d) { return "translate(" + d.y + "," + d.x + ")"; });

  nodeUpdate.select("circle")
	  .attr("r", 10)
	  .style("fill", function(d) { return d._children ? "lightsteelblue" : "#fff"; });

  nodeUpdate.select("text")
	  .style("fill-opacity", 1);

  // Transition exiting nodes to the parent's new position.
  var nodeExit = node.exit().transition()
	  .duration(duration)
	  .attr("transform", function(d) { return "translate(" + source.y + "," + source.x + ")"; })
	  .remove();

  nodeExit.select("circle")
	  .attr("r", 1e-6);

  nodeExit.select("text")
	  .style("fill-opacity", 1e-6);

  // Update the links…
  var link = svg.selectAll("path.link")
	  .data(links, function(d) { return d.target.id; });

  // Enter any new links at the parent's previous position.
  link.enter().insert("path", "g")
	  .attr("class", "link")
	  .attr("d", function(d) {
		var o = {x: source.x0, y: source.y0};
		return diagonal({source: o, target: o});
	  });

  // Transition links to their new position.
  link.transition()
	  .duration(duration)
	  .attr("d", diagonal);

  // Transition exiting nodes to the parent's new position.
  link.exit().transition()
	  .duration(duration)
	  .attr("d", function(d) {
		var o = {x: source.x, y: source.y};
		return diagonal({source: o, target: o});
	  })
	  .remove();

  // Stash the old positions for transition.
  nodes.forEach(function(d) {
	d.x0 = d.x;
	d.y0 = d.y;
  });
}

// Toggle children on click.
function click(d) {
  if (d.children) {
	d._children = d.children;
	d.children = null;
  } else {
	d.children = d._children;
	d._children = null;
  }
  update(d);
}

</script>
	
  </body>
</html>
"""




components.html(h,height=600,
#scrolling=scrolling,
)
