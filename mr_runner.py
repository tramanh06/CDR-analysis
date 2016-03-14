__author__ = 'TramAnh'

from LP_mapreduce import LabelProp
from LP_mapreduce_test import LabelPropTest
import numpy as np
import pandas as pd

json_train = 'json_train.txt'
json_test = 'json_test.txt'

def runner_train():
    mr_job = LabelProp(args=[json_train])
    result=[]
    with mr_job.make_runner() as runner:
        runner.run()
        for line in runner.stream_output():
            key, value = mr_job.parse_output_line(line)
            churn = np.float64(value[0])
            infl = np.float64(value[1])
            result.append([key, churn, infl])
        print 'Ending stream output'

    df = pd.DataFrame(result, columns=['node', 'churner_prob', 'influence_prob'])
    df.set_index('node')
    return df

def runner_test():
    mr_job = LabelPropTest(args=[json_test])
    result=[]
    with mr_job.make_runner() as runner:
        runner.run()
        for line in runner.stream_output():
            key, value = mr_job.parse_output_line(line)
            churn = np.float64(value[0])
            infl = np.float64(value[1])
            result.append([key, churn, infl])
        print 'Ending stream output'

    df = pd.DataFrame(result, columns=['node', 'churner_prob', 'influence_prob'])
    df.set_index('node')
    return df

