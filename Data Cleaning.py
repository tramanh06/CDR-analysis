__author__ = 'TramAnh'

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

    import pandas as pd
    import datetime as dt

    # Convert 'EVENT_DATE' column to Timestamp
    dateparse = lambda x: pd.datetime.strptime(x, '%d-%b-%y %I.%M.%S.000000 %p')
    infile = 'cleaned_header.csv'
    raw_data = pd.read_csv(infile, sep='|', parse_dates=['EVENT_DATE'], date_parser=dateparse)
    print 'Done parsing data'

    # Clean 'DURATION' to datetime format
    print 'Starting to clean duration data'
    def clean_duration (x):
        if pd.isnull(x):
            x = dt.timedelta(seconds=0)
        elif x.isdigit():
            x = dt.timedelta(seconds=int(x))
        return x
    raw_data['DURATION'] = raw_data['DURATION'].map(clean_duration)

    # Save to file
    print 'Saving to file'
    outfile = 'cleaned_data_2.csv'
    raw_data.to_csv(outfile, index=False, sep='|')

clean_duration_data()