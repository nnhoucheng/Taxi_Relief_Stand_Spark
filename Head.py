from pyspark import SparkContext
import sys

if __name__ == '__main__':
    sc = SparkContext()        
    head = sys.argv
    rdd = sc.parallelize(head)
    rdd.saveAsTextFile('tmp')