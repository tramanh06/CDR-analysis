__author__ = 'TramAnh'

from LP_mapreduce import LabelProp
from LP_mapreduce_test import LabelPropTest
import numpy as np
from controller import json_train, json_test
import pandas as pd

def runner_train():
    mr_job = LabelProp(args=[json_train])
    result=[]
    with mr_job.make_runner() as runner:
        runner.run()
        for line in runner.stream_output():
            key, value = mr_job.parse_output_line(line)
            tokens = value.split()          # value=[.., ...]
            churn = np.float64(tokens[0][1:-1])
            infl = np.float64(tokens[1][:-1])
            result.append([key, churn, infl])
        print 'Ending stream output'

    df = pd.DataFrame(result, columns=['node', 'churner_prob', 'influence_prob'])
    return df

def runner_test():
    mr_job = LabelPropTest(args=[json_test])
    result=[]
    with mr_job.make_runner() as runner:
        runner.run()
        for line in runner.stream_output():
            key, value = mr_job.parse_output_line(line)
            tokens = value.split()          # value=[.., ...]
            churn = np.float64(tokens[0][1:-1])
            infl = np.float64(tokens[1][:-1])
            result.append([key, churn, infl])
        print 'Ending stream output'

    df = pd.DataFrame(result, columns=['node', 'churner_prob', 'influence_prob'])
    return df

