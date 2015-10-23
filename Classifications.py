__author__ = 'TramAnh'

import pandas as pd
import numpy as np
import datetime as dt
import matplotlib.pyplot as plt

EVENT = {'INCOMING_CALL':0, 'OUTGOING_CALL':1, 'IDD_CALL':2, 'OUTGOING_SMS':4, 'INCOMING_SMS':5}

def churn_boolean(x):
    churn_timedelta = pd.to_timedelta(churn_duration_str)
    end_date = pd.to_datetime(end_date_str)
    if x['last activity'] < (end_date - churn_timedelta):
        return 1
    else:
        return 0

def add_churner_column(agg_data):
    agg_data['churner'] = agg_data.apply(churn_boolean, axis=1)

if __name__ == "__main__":
    infile = './AggregateData/agg_original.csv'
    churn_duration_str = '30 days'
    end_date_str = '2015-4-1'

    print 'Read in '+infile+' ...'
    agg_data = pd.read_csv(infile, parse_dates=['first recds', 'last recds', 'last call',
                                                'last sms', 'last idd', 'last activity'])
    # Churners
    add_churner_column(agg_data)

    # Classification
