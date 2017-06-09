from pyspark import SparkContext
import sys

if __name__ == '__main__':
    sc = SparkContext()
    if len(sys.argv) == 4:
        with open('path.txt', 'r') as fi:
            files = fi.readlines()
        path = files[0] + files[sys.argv[1]]
        f = sc.textFile(path, use_unicode=False).cache()
        head = f.select(sys.argv[3])
        with open(sys.argv[2], 'wb') as fo:
            for h in head:
                fo.write('%s'%h)