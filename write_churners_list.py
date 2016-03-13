__author__ = 'TramAnh'

import cPickle as pickle
from graph_construction import graph_file

G = pickle.load( open( graph_file, 'rb' ))

churners=[]
influence=[]
for n in G.nodes():
    if G.node[n]['churner']:
        churners.append(n)
    elif G.node[n]['influence']:
        influence.append(n)

pickle.dump(churners, open('churners.pkl', 'wb'))
pickle.dump(influence, open('influence.pkl', 'wb'))
