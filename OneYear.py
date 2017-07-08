from pyspark import SparkContext
import os
import pandas as pd
import numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt
import geopy.distance
from datetime import datetime
from scipy.ndimage.interpolation import shift
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


if __name__ == '__main__':
    sc = SparkContext()
    
    # Step 0: Read Data
    ## Breagcrumb Data: 5 columns
    ### 0: shl_number or medallion
    ### 1: timestamp in yymmddhhmmss
    ### 2: longitude in crs 2263
    ### 3: latitude in crs 2263
    ### 4: number of passengers in taxi
    path = '/gws/projects/project-taxi_capstone_2016/data/breadcrumb_nad83'
    Breadcrumb = sc.textFile(path, use_unicode=False).cache()
    
    # Step 1: Filter out customer trip and time period
    def parseCSV(list_of_records):
        import csv
        reader = csv.reader(list_of_records)
        for row in reader:
            ## Filter out N/A
            if len(row) == 5:
                ## Filter out customer trip
                if row[-1] == '0':
                    ## from '2015-01-01' to '2015-01-31'
                    if row[1][:4] == '1501'
                        yield (row[0], (row[1], row[2], row[3]))
            
    BC = Breadcrumb.mapPartitions(parseCSV)
    rdd = sc.parallelize(BC.take(10))
    rdd.saveAsTextFile('tmp')