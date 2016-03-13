__author__ = 'TramAnh'

import cPickle as pickle

G = pickle.load(open('G_model.pkl', 'rb'))

nodes = G.nodes()
print len(nodes)
# with open('nodes.txt', 'wb') as txtfile:
#     for node in nodes:
#         txtfile.write(str(node)+"\n")
