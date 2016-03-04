import csv

f = open('masked_data-20May2015.txt', 'r')
a = set()
labels = {}
count = 1
for line in f:
    if line.startswith("A_NUMBER"):
        continue
    tokens = line.split("|")
    if tokens[0]:
        a1 = tokens[0].strip()
        if a1 not in labels:
            labels[a1] = count
            count += 1
        a.add(a1)
print 'set a completed'

f = open('masked_data-20May2015.txt', 'r')
for line in f:
    if line.startswith("A_NUMBER"):
        continue
    tokens = line.split("|")
    if tokens[1]:
        b1 = tokens[1].strip()
        if b1 in a:
            print b1
            print line
            break
