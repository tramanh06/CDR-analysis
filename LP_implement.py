__author__ = 'TramAnh'

import pandas as pd
import numpy as np
import datetime as dt
import matplotlib.pyplot as plt
import networkx as nx
from sklearn.preprocessing import normalize

EVENT = {'INCOMING_CALL':0, 'OUTGOING_CALL':1, 'IDD_CALL':2, 'OUTGOING_SMS':4, 'INCOMING_SMS':5}

infile = './Data/cleaned_data_2.csv'

def prepare_data(input_file):
    # read in csv
    # Convert 'EVENT_DATE' column to Timestamp
    # Convert 'DURATION' to timedelta
    raw_data = pd.read_csv(infile, sep='|', parse_dates=['EVENT_DATE'])
    raw_data['DURATION'] = pd.to_timedelta(raw_data['DURATION'])

    # Split into months
    first_month = 10
    monthly_data = []
    for i in range(6):
        month = (first_month + i - 1) % 12 + 1
        monthly_data.append(raw_data[raw_data['EVENT_DATE'].dt.month == month])

    return monthly_data

def feature_engineer(data):
    ### Return aggreate feature in the form of dataframe
    group = data.groupby('A_NUMBER')
    return group.apply(aggregations)

def set_label(feature_data, churn_data):
    ### feature_data is df, churn_data is raw CDR for churn period
    ### Return df with churner column
    halfmonth_group = churn_data.groupby(['A_NUMBER'])
    halfmonth_agg = halfmonth_group.apply(nonchurns)

    # join features and churn
    joinchurn = feature_data.join(halfmonth_agg)

    # Set churner=1 for people who do not make any activities in churn_data
    joinchurn['churner'] = joinchurn['churner'].fillna(1)

    polish_data(joinchurn)
    return joinchurn

def aggregations(x):
    # Pure Social KPI
    out_degree_call = len(pd.unique(x[(x['EVENT_TYPE']==EVENT['OUTGOING_CALL'])]['B_NUMBER']))
    out_degree_sms = len(pd.unique(x[(x['EVENT_TYPE']==EVENT['OUTGOING_SMS'])]['B_NUMBER']))
    in_degree_call = len(pd.unique(x[(x['EVENT_TYPE']==EVENT['INCOMING_CALL'])]['B_NUMBER']))
    in_degree_sms = len(pd.unique(x[(x['EVENT_TYPE']==EVENT['INCOMING_SMS'])]['B_NUMBER']))

    first_recds = x['EVENT_DATE'].min()
    last_recds = x['EVENT_DATE'].max()
#     total_recds = len(x)

    num_out_calls = len(x[x['EVENT_TYPE']==EVENT['OUTGOING_CALL']])
    total_out_call_duration = x[x['EVENT_TYPE']==EVENT['OUTGOING_CALL']]['DURATION'].sum()
    total_out_call_duration_sec = total_out_call_duration/np.timedelta64(1,'s')

    num_in_calls = len(x[x['EVENT_TYPE']==EVENT['INCOMING_CALL']])
    total_in_call_duration = x[x['EVENT_TYPE']==EVENT['INCOMING_CALL']]['DURATION'].sum()
    total_in_call_duration_sec = total_in_call_duration/np.timedelta64(1,'s')

    num_IDD_calls = len(x[x['EVENT_TYPE']==EVENT['IDD_CALL']])

    num_out_sms = len(x[x['EVENT_TYPE']==EVENT['OUTGOING_SMS']])
    num_in_sms = len(x[x['EVENT_TYPE']==EVENT['INCOMING_SMS']])

    # Last KPI
    last_call = x[x['EVENT_TYPE']==EVENT['OUTGOING_CALL']]['EVENT_DATE'].max()
    last_sms = x[x['EVENT_TYPE']==EVENT['OUTGOING_SMS']]['EVENT_DATE'].max()
    last_idd =  x[x['EVENT_TYPE']==EVENT['IDD_CALL']]['EVENT_DATE'].max()
    last_activity = max([pd.to_datetime(last_call), pd.to_datetime(last_sms), pd.to_datetime(last_idd)])

    # Churner identifying -- warning: will not work for label propagation, because churner label is only identified next month
    # TODO ChurnerOutDegree, ChurnerInDegree

    attr_list = [out_degree_call, out_degree_sms, in_degree_call, in_degree_sms
                ,first_recds, last_recds,
                num_out_calls, total_out_call_duration_sec,
                num_in_calls, total_in_call_duration_sec,
                num_IDD_calls,
                num_out_sms, num_in_sms,
                last_call, last_sms, last_idd, last_activity]

    headers_list = ['out degree call', 'out degree sms', 'in degree call', 'in degree sms'
                    ,'first recds', 'last recds',
                    'num outgoing calls', 'total out call duration in sec',
                    'num incoming calls', 'total in call duration in sec',
                    'num IDD calls',
                    'num outgoing sms', 'num incoming sms',
                    'last call', 'last sms', 'last idd', 'last activity']

    return pd.Series(attr_list, index=headers_list)

def nonchurns(x):
    churner = 0
    return pd.Series([churner], index=['churner'])

# Convert datetime to integer + fill in missing data
def polish_data(data):
    for column in data:
        if data[column].dtypes == '<M8[ns]':    # '<M8[ns]' is datetime
            data[column] = data[column].dt.day
            data[column] = data[column].fillna(0)

def build_graph(data):
    ### Construct graph G from df
    # Adding the weight to prepare for nx
    df = data.groupby(['A_NUMBER', 'B_NUMBER'])['DURATION'].sum().reset_index()
    df['DURATION_SEC'] = df['DURATION'] / np.timedelta64(1, 's')
    G = nx.from_pandas_dataframe(df,'A_NUMBER', 'B_NUMBER', ['DURATION_SEC'])
    return G

def set_initial_churners(data0, data1):
    # Return list of churners
    a0 = pd.unique(data0.A_NUMBER.ravel())
    a1 = pd.unique(data1[data1['EVENT_DATE'].dt.day <16].A_NUMBER.ravel())
    churners = list(set(a0) - set(a1))

    return churners

# Remove lone nodes
def remove_lone_nodes(G):
    degree_array = nx.degree(G)
    for n in G.nodes():
        if degree_array[n]==1:
            G.remove_node(n)
    G.number_of_nodes()

# Connect 2 A_NUMBER that shares the same B_NUMBER
def connect_edges_from_node(G, b, neighbors):
    neighbors_len = len(neighbors)
    for i in range(neighbors_len):
        for j in range(i+1, neighbors_len):
            a1 = neighbors[i]
            a2 = neighbors[j]
            added_duration = G[a1][b]['DURATION_SEC'] + G[a2][b]['DURATION_SEC']
            if G.has_edge(a1,a2):
                G[a1][a2]['DURATION_SEC'] += added_duration
            else:
                G.add_edge(a1, a2, DURATION_SEC=added_duration)

def connect_edges(G):
    for n in G.nodes():
        if type(n) is str:
            connect_edges_from_node(G, n, G.neighbors(n))

# remove B_NUMBER nodes
def remove_BNUMBER_nodes(G):
    for n in G.nodes():
        if type(n) is str:
            G.remove_node(n)

def add_influence_label(G):
    degree_list = G.degree().values()
    sorted_degree = sorted(degree_list, reverse=True)[:50]
    threshold = sorted_degree[-1]

    # Add label
    for n in G.nodes():
        if G.degree(n) < threshold:
            G.add_node(n, influence=0)
        else:
            G.add_node(n, influence=1)

# Graph functions
def build_Y(G):
    arr = []
    for n in G.nodes():
        arr.append([G.node[n]['churner'], G.node[n]['influence']])
    return np.matrix(arr)

def fix_labels(G, Y):
    for i, n in enumerate(G.nodes()):
        if G.node[n]['churner']==1:
            Y[i,0]=1.0
            Y[i,1]=0.0
        elif G.node[n]['influence']==1:
            Y[i,1]=1.0
            Y[i,0]=0.0
    return Y

def label_propagate(G):
    A = nx.adjacency_matrix(G, weight='DURATION_SEC')
#     T = normalize(A, axis=0, norm='l1')
    T = A
    Y = build_Y(G)

    while(True):
        Y1 = T*Y
        Y1 = normalize(Y1, axis=1, norm='l1') # Row normalize Y
        Y1 = fix_labels(G, Y1)
        Y1 = np.matrix(Y1)
        if np.allclose(Y1, Y, atol=1e-6):
            break
        else:
            Y=Y1

    return Y


#### Main Function #####
# param data is the monthly data
def set_graph_features(data0, data1):
    data = pd.concat([data0, data1])
    # Aggregate call data
    call_data = data[(data['EVENT_TYPE']==EVENT['OUTGOING_CALL']) | (data['EVENT_TYPE']==EVENT['INCOMING_CALL'])]

    # Construct graph
    G = build_graph(call_data)

    # Preprocess
    remove_lone_nodes(G)
    connect_edges(G)
    remove_BNUMBER_nodes(G)

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

# Main program

if __name__=='__main__':
    monthly_data = prepare_data(infile)

    churn_period = monthly_data[2][monthly_data[2]['EVENT_DATE'].dt.day <16]

    print 'Creating features for training'
    feature_data = feature_engineer(monthly_data[1])

    print 'Add graph features. Build graph from CDR and add churner&influence label'
    graph_data = set_graph_features(monthly_data[0], monthly_data[1])

    # Create training set
    print 'Graph data:'
    print graph_data

    print 'Feature data:'
    print feature_data

    feature_data = feature_data.join(graph_data)

    print 'Determine churner label'
    train_data = set_label(feature_data, churn_period)

    print 'Write to csv'
    train_data.to_csv('Month2_LP.csv', index=False)