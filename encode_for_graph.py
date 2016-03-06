__author__ = 'TramAnh'

import csv

outfile = "Data/cleaned_data_encoded.csv"
indexfile = "Data/index.txt"
datafile = 'Data/cleaned_data_3.csv'

print 'Prepare index to dict...'
caller_dict = {}
with open(indexfile, "rb") as txtfile:
    for line in txtfile:
        if line.startswith("index"):
            continue
        tokens = line.split("|")
        i = tokens[0].strip()
        caller = tokens[1].strip()
        caller_dict[caller]=i

print 'Encode CDR with index...'
with open(datafile, 'rb') as csvfile:
    csvreader = csv.reader(csvfile, delimiter="|")
    with open(outfile, 'wb') as csvwrite:
        fieldname = ["A_NUMBER","B_NUMBER","EVENT_TYPE","EVENT_DATE","DURATION","EVENT_COST"]
        writer = csv.writer(csvwrite, quoting=csv.QUOTE_NONE, delimiter="|")
        writer.writerow(fieldname)
        for line in csvreader:
            if 'A_NUMBER' in line:
                continue
            if line[0] and line[1]:
                a, b, event, date, duration, cost = line[0], line[1], line[2], \
                                                    line[3], line[4], line[5]
                a_encode = caller_dict[a]
                b_encode = caller_dict[b]
                writer.writerow([a_encode, b_encode, event, date, duration, cost])



