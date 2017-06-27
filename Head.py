from pyspark import SparkContext
import sys

if __name__ == '__main__':
    sc = SparkContext()
    if len(sys.argv) == 2:        
        path = '/gws/projects/project-taxi_capstone_2016/data/TLC/TPEP2015/BreadCrumb_CMT.csv'
        f = sc.textFile(path, use_unicode=False).cache()
        head = f.take(10)
        rdd = sc.parallelize(head)
        rdd.saveAsTextFile(sys.argv[1])