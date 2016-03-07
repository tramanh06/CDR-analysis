__author__ = 'TramAnh'

import pandas as pd
import mincemeat
import cPickle as pickle

from graph_construction import graph_file




G = pickle.load( open( graph_file, 'rb' ))

datasource = dict(enumerate(G.nodes()))


def mapfn(k, v):
    '''
    Map function for map reduce
    :param k: index from enumerate(nodes)
    :param v: node number
    :return: updated class distribution
    '''
    # TODO: optimize to make use of nodes for key
    def build_classDistribution(G):
        dict = {}
        for n in G.nodes():
            dict[n] = [G.node[n]['churner'], G.node[n]['influence']]
        return dict

    prob_distribution_dict = build_classDistribution(G)
    
    def get_neighbors(G, n):
        return G.neighbors(n)

    d = 0
    neighbors = get_neighbors(G,v)

    for neighbor in neighbors:
        w = G[v][neighbor]['DURATION_SEC']
        d += w * prob_distribution_dict[neighbor]

    norm = [float(i)/sum(d) for i in d]
    yield v, norm

def reducefn(k, vs):
    result = vs
    return result

s = mincemeat.Server()
s.datasource = datasource
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="changeme")
print results

