#!/usr/bin/env python

from LP_mapreduce import LabelProp
import numpy as np
import cPickle as pickle
import collections
import os.path

prob_file = 'prob_distribution.pkl'

def is_convergent(results):
    ' Extract values to compare for convergent'
    # if not os.path.isfile(prob_file):
    #     return False

    prob_dist = pickle.load(open(prob_file, 'rb'))    # a dict of key=node and value=np.array[,]

    prob_dist_od = create_ordered_dict(prob_dist)       # use ordered dict so we can sort the key and get values
                                                        # at the same order everytime
    results_od = create_ordered_dict(results)

    prob_matrix = np.array(prob_dist_od.values())
    results_matrix = np.array(results_od.values())

    pickle.dump(prob_matrix, open('prob_matrix.pkl', 'wb'))
    pickle.dump(results_matrix, open('results_matrix.pkl', 'wb'))

    return np.allclose(prob_matrix, results_matrix, atol=1e-6)

def create_ordered_dict(dict):
    return collections.OrderedDict(sorted(dict.items()))


writer = open('runner_output.txt', 'wb')

count=0

for i in range(2):
# while True:
    results = {}
    mr_job = LabelProp(args=['nodes.txt'])
    with mr_job.make_runner() as runner:
        print 'Running iteration # %d'%(count)
        runner.run()
        for line in runner.stream_output():
            key, value = mr_job.parse_output_line(line)
            writer.write('{0}: {1}\n'.format(key, value))
            results[key]= np.array(value)     # value is list of 2 numbers
        print 'Ending stream output'

    print 'Checking convergent'
    # if is_convergent(results):
    #     break
    print 'Done checking convergent'

    pickle.dump(results, open('prob_file_{0}.pkl'.format(count), 'wb'))
    count += 1


# print results