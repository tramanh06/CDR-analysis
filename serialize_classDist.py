__author__ = 'TramAnh'

import cPickle as pickle
from graph_construction import graph_file
import numpy as np
from mrjob.protocol import JSONProtocol

G = pickle.load( open( graph_file, 'rb' ))

def build_classDistribution(G):
    dict = {}
    for n in G.nodes():
        dict[n] = [G.node[n]['churner'], G.node[n]['influence']]
    return dict

def encode_node(node_id, value):
    x=JSONProtocol()
    return x.write(node_id, value) + '\n'

prob_distribution_dict = build_classDistribution(G)     # prob_distribution_dict is dictionary {int: [float, float]}
# pickle.dump(prob_distribution_dict, open('prob_distribution.pkl', 'wb'))

# Write to json
with open ('result_json.txt','w') as fo:
    for key in prob_distribution_dict:
        fo.write(encode_node(key,prob_distribution_dict[key]))