__author__ = 'TramAnh'

import pandas as pd
import datetime as dt

def clean_header():
    ###
    # Cleaning the CDR Raw Data
    # remove all the header "A_NUMBER||'|'||...." that appears once in a while
    ###

    input_file = '../../CDR Raw Data/masked_data-20May2015.txt'

    ### Clean repeated header
    output_file = 'cleaned_header.csv'
    keyword = 'A_NUMBER'
    has_passed_header = False

    with open(input_file) as f:
        lines = f.readlines()

    for line in lines:
        if keyword.lower() not in line.lower() or has_passed_header == False:
            with open(output_file, 'a') as myfile:
                myfile.write(line)
            has_passed_header = True


def clean_duration_data():
    ###
    # 1. Parse EVENT_DATE as Timestamp object
    # 2. Clean DURATION as some data are integers, some are NaN, some are in timedelta
    # -> convert DURATION to timedelta
    # 3. Write changes to cleaned_data.csv
    ###

    # Convert 'EVENT_DATE' column to Timestamp
    timeformat = '%d-%b-%y %I.%M.%S.000000 %p'
    infile = 'cleaned_header.csv'
    raw_data = convert_to_timestamp(infile, timeformat)

    # Clean 'DURATION' to datetime format
    print 'Starting to clean duration data'
    raw_data['DURATION'] = raw_data['DURATION'].map(clean_duration)

    # Save to file
    print 'Saving to file'
    outfile = 'cleaned_data_2.csv'
    raw_data.to_csv(outfile, index=False, sep='|')

def convert_to_timestamp(infile, timeformat):
    # Convert 'EVENT_DATE' column to Timestamp
    dateparse = lambda x: pd.datetime.strptime(x, timeformat)
    raw_data = pd.read_csv(infile, sep='|', parse_dates=['EVENT_DATE'], date_parser=dateparse)
    print 'Done parsing data'
    return raw_data

def clean_duration (x):
    # Convert duration to Timedelta
        if pd.isnull(x):
            x = dt.timedelta(seconds=0)
        elif x.isdigit():
            x = dt.timedelta(seconds=int(x))
        return x

if __name__ == "__main__":
    # clean_header()
    clean_duration_data()