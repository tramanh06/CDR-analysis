__author__ = 'TramAnh'

from LP_implement import prepare_data, build_graph, set_initial_churners, add_influence_label, label_propagate

import pandas as pd
import numpy as np
import datetime as dt
import matplotlib.pyplot as plt
import networkx as nx
from sklearn.preprocessing import normalize

EVENT = {'INCOMING_CALL':0, 'OUTGOING_CALL':1, 'IDD_CALL':2, 'OUTGOING_SMS':4, 'INCOMING_SMS':5}

infile = './Data/cleaned_data_encoded.csv'

#### Main Function #####
# param data is the monthly data
def set_graph_features(data0, data1):
    data = pd.concat([data0, data1])
    # Aggregate call data
    call_data = data[(data['EVENT_TYPE']==EVENT['OUTGOING_CALL']) | (data['EVENT_TYPE']==EVENT['INCOMING_CALL'])]

    # Construct graph
    G = build_graph(call_data)

    # Add churner attribute to nodes
    churners = set_initial_churners(data0, data1)
    for n in G.nodes():
        if n in churners:
            G.add_node(n, churner=1)
        else:
            G.add_node(n, churner=0)

    # Add influence attribute to nodes
    add_influence_label(G)

    # Label Propagation
    Y = label_propagate(G)

    # Combine Y with A_NUMBER -> return df
    df = pd.DataFrame(Y, columns=['churn_prob', 'influence_prob'])
    df['A_NUMBER'] = pd.Series(G.nodes())
    df = df.set_index('A_NUMBER')
    return df

if __name__=='__main__':
    monthly_data = prepare_data(infile)

    churn_period = monthly_data[2][monthly_data[2]['EVENT_DATE'].dt.day <16]

    # print 'Creating features for training'
    # feature_data = feature_engineer(monthly_data[1])

    print 'Add graph features. Build graph from CDR and add churner&influence label'
    graph_data = set_graph_features(monthly_data[0], monthly_data[1])

    # Create training set
    print 'Graph data:'
    print graph_data

    # print 'Feature data:'
    # print feature_data

    # feature_data = feature_data.join(graph_data)
    #
    # print 'Determine churner label'
    # train_data = set_label(feature_data, churn_period)
    #
    # print 'Write to csv'
    # train_data.to_csv('Month2_LP.csv', index=False)