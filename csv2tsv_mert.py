import csv
import sys

with open(sys.argv[1] + ".csv", 'r', encoding='utf-8') as csvin, open(sys.argv[1] + ".tsv", 'w', newline='',
                                                                       encoding='utf-8') as tsvout:
    csvin = csv.reader(csvin)
    tsvout = csv.writer(tsvout, delimiter='\t')

    for row in csvin:
        tsvout.writerow(row)
