import unicodecsv
import codecs

rows = [row for row in unicodecsv.reader(codecs.open('data/2014-04-04-Bibsysmatch.csv', 'r'), delimiter=';')]
q = [row[1] + row[2] for row in rows]

dups = unicodecsv.writer(codecs.open('data/dups.csv', 'w'))
uniq = unicodecsv.writer(codecs.open('data/uniq.csv', 'w'))

for row in rows:
    qq = row[1] + row[2]
    if q.count(qq) > 1:
        dups.writerow(row)
    else:
        uniq.writerow(row)
