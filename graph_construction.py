__author__ = 'TramAnh'
from LP_implement import prepare_data, build_graph, EVENT, add_churner_label, add_influence_label
from LP_with_encoded import infile

import cPickle as pickle
import pandas as pd

graph_file = 'G_model.pkl'

def construct_graph(data0, data1):
    data = pd.concat([data0, data1])

    print 'Aggregating call data...'
    call_data = data[(data['EVENT_TYPE']==EVENT['OUTGOING_CALL']) | (data['EVENT_TYPE']==EVENT['INCOMING_CALL'])]

    print 'Constructing graph...'
    G = build_graph(call_data)
    return G


def serialize_graph():
    monthly_data = prepare_data(infile)

    data0 = monthly_data[0]
    data1 = monthly_data[1]

    G = construct_graph(data0, data1)

    print 'Add churner attribute to nodes...'
    add_churner_label(G, data0, data1)

    print 'Add influence attribute to nodes...'
    add_influence_label(G)

    # Save serialized grapg
    pickle.dump(G, open(graph_file, 'wb' ))

