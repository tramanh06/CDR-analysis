__author__ = 'TramAnh'

outfile = "Data/rawdata_encoded.txt"

print 'Prepare index to dict...'
caller_dict = {}
with open("Data/index.txt", "rb") as txtfile:
    for line in txtfile:
        if line.startswith("index"):
            continue
        tokens = line.split("|")
        i = tokens[0].strip()
        caller = tokens[1].strip()
        caller_dict[caller]=i

print 'Encode CDR with index...'
with open('RawData/masked_data-20May2015.txt', 'rb') as rawfile:
    writer = open(outfile, 'wb')
    for line in rawfile:
        if line.startswith("A_NUMBER"):
            writer.write(line)
            continue
        tokens = line.split("|")
        if tokens[0] and tokens[1]:
            a, b, event, date, duration, cost = tokens[0], tokens[1], tokens[2], \
                                                tokens[3], tokens[4], tokens[5]
            a_encode = caller_dict[a]
            b_encode = caller_dict[b]
            writer.write("{0}|{1}|{2}|{3}|{4}|{5}".format(a_encode, b_encode,
                                                          event, date, duration, cost))

    writer.close()
    

