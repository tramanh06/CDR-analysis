__author__ = 'TramAnh'

import pandas as pd
import numpy as np
import datetime as dt
import matplotlib.pyplot as plt

EVENT = {'INCOMING_CALL':0, 'OUTGOING_CALL':1, 'IDD_CALL':2, 'OUTGOING_SMS':4, 'INCOMING_SMS':5}

def convert_duration_totimedelta(raw_data):
    raw_data['DURATION'] = pd.to_timedelta(raw_data['DURATION'])

def remove_CallCentre_records(raw_data):
    return raw_data[raw_data['B_NUMBER'].str.len() < 20]

def aggregations(x):
    first_recds = x['EVENT_DATE'].min()
    last_recds = x['EVENT_DATE'].max()
    total_recds = len(x)

    num_out_calls = len(x[x['EVENT_TYPE']==EVENT['OUTGOING_CALL']])
    out_duration = x[x['EVENT_TYPE']==EVENT['OUTGOING_CALL']]['DURATION'].sum()
    out_duration_sec = out_duration/np.timedelta64(1,'s')

    num_in_calls = len(x[x['EVENT_TYPE']==EVENT['INCOMING_CALL']])
    in_duration = x[x['EVENT_TYPE']==EVENT['INCOMING_CALL']]['DURATION'].sum()
    in_duration_sec = in_duration/np.timedelta64(1,'s')

    num_IDD_calls = len(x[x['EVENT_TYPE']==EVENT['IDD_CALL']])
    num_out_sms = len(x[x['EVENT_TYPE']==EVENT['OUTGOING_SMS']])
    num_in_sms = len(x[x['EVENT_TYPE']==EVENT['INCOMING_SMS']])

    last_call = x[x['EVENT_TYPE']==EVENT['OUTGOING_CALL']]['EVENT_DATE'].max()
    last_sms = x[x['EVENT_TYPE']==EVENT['OUTGOING_SMS']]['EVENT_DATE'].max()
    last_idd =  x[x['EVENT_TYPE']==EVENT['IDD_CALL']]['EVENT_DATE'].max()
    last_activity = max([pd.to_datetime(last_call), pd.to_datetime(last_sms), pd.to_datetime(last_idd)])


    attr_list = [first_recds, last_recds, total_recds,
                num_out_calls, out_duration, out_duration_sec,
                num_in_calls, in_duration, in_duration_sec,
                num_IDD_calls,
                num_out_sms, num_in_sms,
                last_call, last_sms, last_idd, last_activity]

    headers_list = ['first recds', 'last recds', 'total records',
                    'num outgoing calls', 'out duration', 'out duration in sec',
                    'num incoming calls', 'in duration','in duration in sec',
                    'num IDD calls',
                    'num outgoing sms', 'num incoming sms',
                    'last call', 'last sms', 'last idd', 'last activity']

    return pd.Series(attr_list, index=headers_list)

def write_summary_tofile(summary, outfile):
    summary.to_csv(outfile)

if __name__ == "__main__":

    infile = './Data/cleaned_data_2.csv'
    outfile = './AggregateData/agg_original.csv'
    remove_CC = True    # Remove Call Centre records

    # read in csv. Convert 'EVENT_DATE' column to Timestamp
    print 'Reading in csv file...'
    raw_data = pd.read_csv(infile, sep='|', parse_dates=['EVENT_DATE'])
    convert_duration_totimedelta(raw_data)

    # If want to remove Call Centre records
    if remove_CC:
        raw_data = remove_CallCentre_records(raw_data)
        outfile = './AggregateData/agg_withoutCallCentre.csv'

    print 'Aggregating CDR data...'
    group = raw_data.groupby('A_NUMBER')
    summary = group.apply(aggregations)

    print 'Write summary data to file...'
    write_summary_tofile(summary, outfile)
