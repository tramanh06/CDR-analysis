__author__ = 'TramAnh'

import cPickle as pickle
from graph_construction import graph_file
import numpy as np
from mrjob.protocol import JSONProtocol

G = pickle.load( open( graph_file, 'rb' ))

def build_classDistribution(G):
    dict = {}
    for n in G.nodes():
        dict[n] = np.array([G.node[n]['churner'], G.node[n]['influence']])
    return dict

def encode_node(node_id, links=None, score=1):
    node = {}
    if links:
        node['links'] = sorted(links.items())
    node['score'] = score
    x=JSONProtocol()
    return x.write(node_id, node) + '\n'

prob_distribution_dict = build_classDistribution(G)
# pickle.dump(prob_distribution_dict, open('prob_distribution.pkl', 'wb'))

# Write to json
with open ('result.txt','w') as fo:
    for key in dict:
        temp={}
        for val in d[key]:
            temp[val]=(val,1.0/len(d[key]))
        fo.write(encode_node(key,temp))