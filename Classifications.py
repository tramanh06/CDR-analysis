__author__ = 'TramAnh'

import pandas as pd
import numpy as np
import datetime as dt
import matplotlib.pyplot as plt

EVENT = {'INCOMING_CALL':0, 'OUTGOING_CALL':1, 'IDD_CALL':2, 'OUTGOING_SMS':4, 'INCOMING_SMS':5}

def process_agg_data(agg_data):
    agg_data['avg duration/outcall'] = agg_data['out duration in sec']/agg_data['num outgoing calls']
    agg_data['avg duration/incall'] = agg_data['in duration in sec']/agg_data['num incoming calls']
    agg_data['record duration'] = (agg_data['last recds'] - agg_data['first recds']).dt.days
    agg_data['avg outgoing calls/day'] = agg_data['num outgoing calls']/agg_data['record duration']
    agg_data['avg incoming calls/day'] = agg_data['num incoming calls']/agg_data['record duration']

    # Remove timedelta columns
    agg_data.drop('in duration', axis=1, inplace=True)
    agg_data.drop('out duration', axis=1, inplace=True)

    # Set Churner Column
    churn_timedelta = pd.to_timedelta(churn_duration_str)
    end_date = pd.to_datetime(end_date_str)

    def churn_boolean(x):
        if x['last recds'] < (end_date - churn_timedelta):
            return 1
        else: return 0

    agg_data['churner'] = agg_data.apply(churn_boolean, axis=1)

def plot_attribute_graph():
    agg_data.hist(bins=50)
    plt.show()

def write_to_csv(outfile):
    agg_data.to_csv(outfile, index=False)

if __name__ == "__main__":
    '''
    Declare parameters
    '''
    infile = './AggregateData/agg_original.csv'
    outfile = './AggregateData/agg_original_wLabel_15days.csv'
    churn_duration_str = '15 days'
    end_date_str = '2015-4-1'
    remove_CC = True
    if remove_CC:
        infile = './AggregateData/agg_withoutCallCentre.csv'
        outfile = './AggregateData/agg_withoutCallCentre_label_15days.csv'


    print 'Read in '+infile+' ...'
    dates_attr = ['first recds', 'last recds', 'last call', 'last sms', 'last idd', 'last activity']
    agg_data = pd.read_csv(infile, parse_dates=dates_attr)

    process_agg_data(agg_data)

    # Show distribution graph
    # plot_attribute_graph()

    # Write to csv
    write_to_csv(outfile)

    # Classification
