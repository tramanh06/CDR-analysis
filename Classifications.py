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

def plot_attribute_graph():
    # fig = plt.figure()
    agg_data.hist(bins=50)
    plt.show()

def write_to_csv(outfile):
    agg_data.to_csv(outfile, index=False)

if __name__ == "__main__":
    infile = './AggregateData/agg_original.csv'
    outfile = './AggregateData/agg_original_wLabel.csv'
    churn_duration_str = '30 days'
    end_date_str = '2015-4-1'
    remove_CC = True

    if remove_CC:
        infile = './AggregateData/agg_withoutCallCentre.csv'
        outfile = './AggregateData/agg_withoutCallCentre_label.csv'

    print 'Read in '+infile+' ...'
    agg_data = pd.read_csv(infile, parse_dates=['first recds', 'last recds', 'last call',
                                                'last sms', 'last idd', 'last activity'])
    # Churners  NOTE: NOT NEEDED IF USE agg_data_avgCallDuration.csv
    add_churner_column(agg_data)

    # Show distribution graph
    plot_attribute_graph()

    # Write to csv
    write_to_csv(outfile)

    # Classification
