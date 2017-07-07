from pyspark import SparkContext
import os
import pandas as pd
import numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt
import geopy.distance
from datetime import datetime
from scipy.ndimage.interpolation import shift



if __name__ == '__main__':
    sc = SparkContext()
    path = '/gws/projects/project-taxi_capstone_2016/data/breadcrumb_nad83'
    f = sc.textFile(path, use_unicode=False).cache()
    head = f.take(10)
    rdd = sc.parallelize(head)
    rdd.saveAsTextFile('tmp')