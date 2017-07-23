from pyspark import SparkContext
import csv
import numpy as np
import rtree
import geopandas as gpd
from fiona.crs import from_epsg
import shapely.geometry as geom

def parse(records):
    reader = csv.reader(records)
    
    distance = 1/.3048*50 # setup the distance parameter
    #relief_path = 'new_york_city_taxi_relief_stations.geojson'
    #relief_path = 'relief_stands_23July.geojson'
    relief_path = 'TRS_149.geojson'
    relief = gpd.GeoDataFrame.from_file(relief_path)
    relief.crs = from_epsg(4326)
    relief = relief.to_crs(epsg=2263)
    #relief.drop_duplicates(subset=["location"], inplace=True)
    relief["buffer"] = relief.apply(lambda x: x.geometry.buffer(distance), axis=1)
    relief = relief.set_geometry("buffer")
    ##
    index = rtree.Rtree()
    for idx, geometry in zip(relief.index.values, relief.geometry):
        index.insert(idx, geometry.bounds)
    
    for row in reader:
        date = row[1][:6]
        x,y = int(row[3]), int(row[4])
        potentialMatches = index.intersection((x, y, x, y))
        p = geom.Point(x,y)
        
        match = None
        for idx in potentialMatches:
            if relief.geometry[idx].contains(p):
                match = idx
                break
        if match != None:
            yield ((match, date), int(row[2]))

def stat(values):
    time_usage = 0
    car_usage = 0
    for v in values:
        car_usage += 1
        time_usage += v
    return (car_usage, time_usage)

def saveformat(kvs):
    return ','.join(map(str, kvs[0])) + ',' + ','.join(map(str, kvs[1]))
            
if __name__ == '__main__':
    sc = SparkContext()
    
    path = '/user/ch3019/capstone/idles_2015'
    idles = sc.textFile(path, use_unicode=False).cache()
    
    usage = idles.mapPartitions(parse).groupByKey().mapValues(stat)
    usgae_column = sc.parallelize(["relief_stand_idx,date,car_usage,time_usage"])
    #usgae_column.union(usage.sortByKey().map(saveformat)).saveAsTextFile('capstone/usage_2015_July23')
    usgae_column.union(usage.sortByKey().map(saveformat)).saveAsTextFile('capstone/usage_2015_TRS_149')
    
# end{main}