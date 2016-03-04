__author__ = 'TramAnh'


f = open('RawData/masked_data-20May2015.txt', 'rb')
callers = set()
for line in f:
    if line.startswith('A_NUMBER'):
        continue
    tokens = line.split("|")
    if tokens[0] and tokens[1]:
        a = tokens[0].strip()
        b = tokens[1].strip()
        callers.add(a)
        callers.add(b)

print 'complete A and B_NUMBER'
print 'Length=%d' %(len(callers))

with open("index.txt", "w") as textfile:
    textfile.write("index|caller\n")
    for i, caller in enumerate(callers):
        textfile.write("{0}|{1}\n".format(i,caller))