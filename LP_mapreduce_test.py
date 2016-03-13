__author__ = 'TramAnh'

#/usr/bin/env python

from mrjob.job import MRJob
import cPickle as pickle
from LP_implement import g_test_file, churners_test_file, influence_test_file
import numpy as np
from mrjob.protocol import JSONProtocol
from mrjob.step import MRStep

G = pickle.load( open( g_test_file, 'rb' ))
churners_list = pickle.load( open( churners_test_file, 'rb'))
influence_list = pickle.load( open( influence_test_file, 'rb'))

ITERATION = 3

class LabelPropTest(MRJob):
    INPUT_PROTOCOL = JSONProtocol  # read the same format we write

    def configure_options(self):
        super(LabelPropTest, self).configure_options()

        self.add_passthrough_option(
            '--iterations', dest='iterations', default=ITERATION, type='int',
            help='number of iterations to run')


    def map_task(self, node, prob):
        node_int = int(node)
        neighbors =  G.neighbors(node_int)
        p = np.array(prob)

        for neighbor in neighbors:
            w = G[node_int][neighbor]['DURATION_SEC']
            result = w*p
            yield neighbor, result.tolist()

    def reduce_task(self, key, val):
        # Check if key is a churner or influence - Clamp the label
        node_int = int(key)
        if node_int in churners_list:       # Clamp label
            yield key, [1.0, 0.0]
        elif node_int in influence_list:
            yield key, [0.0, 1.0]
        else:
            # Defensive style
            total = np.zeros(2)
            for each in val:
                total += np.array(each)

            if sum(total):      # To prevent divide by 0
                norm = [float(i)/sum(total) for i in total]
            else:
                norm = np.zeros(2).tolist()

            yield key, norm     # reduce returns [node, [churners_prob, influence_prob]]

    def steps(self):
        return ([self.mr(mapper=self.map_task, reducer=self.reduce_task)] *
                self.options.iterations)

if __name__ == '__main__':
    LabelPropTest.run()