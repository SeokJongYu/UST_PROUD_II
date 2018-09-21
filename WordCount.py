import sys
from pyspark import SparkContext

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print >> sys.stderr, "Usage: WordCount.py <file>"
        exit(-1)
sc = SparkContext()
counts = sc.textFile(sys.argv[1]).flatMap(lambda line: line.split()).map(lambda word: (word, 1)).reduceByKey(lambda v1,v2: v1+v2)

for pair in counts.take(5) : 
    print pair
sc.stop()

