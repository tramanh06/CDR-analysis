#!/usr/bin/env python

from mrjob.job import MRJob
import cPickle as pickle
from graph_construction import graph_file
import numpy as np
from mrjob.protocol import JSONProtocol
from mrjob.step import MRStep

G = pickle.load( open( graph_file, 'rb' ))
prob_distribution_dict = pickle.load( open( 'prob_distribution.pkl', 'rb'))

class LabelProp(MRJob):
    """ A  map-reduce job that calculates the density """
    INPUT_PROTOCOL = JSONProtocol  # read the same format we write

    def configure_options(self):
        super(LabelProp, self).configure_options()

        self.add_passthrough_option(
            '--iterations', dest='iterations', default=10, type='int',
            help='number of iterations to run')

        # self.add_passthrough_option(
        #     '--damping-factor', dest='damping_factor', default=0.85,
        #     type='float',
        #     help='probability a web surfer will continue clicking on links')


    def mapper(self, _, node):
        d=np.array([0.0, 0.0])
        node_int = int(node)
        neighbors =  G.neighbors(node_int)

        for neighbor in neighbors:
            w = G[node_int][neighbor]['DURATION_SEC']
            d += float(w) * prob_distribution_dict[neighbor]

        if sum(d):
            norm = np.array([float(i)/sum(d) for i in d])
        else:
            norm = np.zeros(2)
        yield node_int, norm.tolist()



        # for neighbor in neighbors:
        #     yield neighbor, 1
    #
    # def reducer(self, key, val):
    #     yield (key, sum(val))

if __name__ == '__main__':
    LabelProp.run()