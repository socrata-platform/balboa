import struct 

from lazyboy import *
from lazyboy.key import Key

connection.add_pool("Metrics", ["localhost:9160"])

current = 18000000 

class SummaryKey(Key):
    def __init__(self):
        global current
        current += 1000
        Key.__init__(self, keyspace="Metrics", column_family="realtime", key="bugs-bugs", super_column=struct.pack(">q", current))

class Summary(record.Record):
    def __init__(self, *args, **kwargs):
        record.Record.__init__(self, *args, **kwargs)
        self.key = SummaryKey()

def populate():
    data = {"narf": "1", "jess": "2"}

    records = RecordSet()
    for i in xrange(0, 1000000):
        records.append(Summary(data))
        if (i % 50000) == 0:
            print "Inserting %d" % i
            records.save()
            records = RecordSet()
    records.save()

if __name__ == "__main__":
    populate()
