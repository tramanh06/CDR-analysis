__author__ = 'TramAnh'

from LP_implement import *

import pandas as pd
import numpy as np
import datetime as dt
import matplotlib.pyplot as plt
import networkx as nx
from sklearn.preprocessing import normalize

EVENT = {'INCOMING_CALL':0, 'OUTGOING_CALL':1, 'IDD_CALL':2, 'OUTGOING_SMS':4, 'INCOMING_SMS':5}

infile = './Data/cleaned_data_encoded.csv'

if __name__=='__main__':
    # read in csv
    # Convert 'EVENT_DATE' column to Timestamp
    # Convert 'DURATION' to timedelta
    raw_data = pd.read_csv(infile, sep='|', parse_dates=['EVENT_DATE'])
    raw_data['DURATION'] = pd.to_timedelta(raw_data['DURATION'])

    # Split into months
    first_month = 10
    monthly_data = []
    for i in range(6):
        month = (first_month + i - 1) % 12 + 1
        monthly_data.append(raw_data[raw_data['EVENT_DATE'].dt.month == month])

    churn_period = monthly_data[2][monthly_data[2]['EVENT_DATE'].dt.day <16]

    print 'Creating features for training'
    feature_data = feature_engineer(monthly_data[1])

    print 'Add graph features. Build graph from CDR and add churner&influence label'
    graph_data = set_graph_features(monthly_data[0], monthly_data[1])

    # Create training set
    print 'Graph data:'
    print graph_data

    print 'Feature data:'
    print feature_data

    feature_data = feature_data.join(graph_data)

    print 'Determine churner label'
    train_data = set_label(feature_data, churn_period)

    print 'Write to csv'
    train_data.to_csv('Month2_LP.csv', index=False)