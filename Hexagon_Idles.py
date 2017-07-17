from pyspark import SparkContext
import csv
import numpy as np
import rtree
import geopandas as gpd
import shapely.geometry as geom
import pyproj

def parseIdles(records):
    reader = csv.reader(records)
    hexagon = gpd.GeoDataFrame.from_file('Hexagon_clipped.geojson')
    
    counts = {}
    index = rtree.Rtree()
    for idx, geometry in zip(hexagon.index.values, hexagon.geometry):
        index.insert(idx, geometry.bounds)
    
    for row in reader:
        date = row[1][:6]
        x,y = int(row[3]), int(row[4])
        potentialMatches = index.intersection((x, y, x, y))
        p = geom.Point(x,y)
        
        match = None
        for idx in potentialMatches:
            if hexagon.geometry[idx].contains(p):
                match = idx
                break
        if match:
            k = (hexagon.GRID_ID[match], date)
            v = counts.get(k,(0,0))
            counts[k] = (v[0]+1, v[1]+int(row[2]))
            #yield ((match, date), int(row[2]))
    return counts.items()   

def reducer(x,y):
    return (x[0]+y[0], x[1]+y[1])

def saveformat(kvs):
    return ','.join(map(str, kvs[0])) + ',' + ','.join(map(str, kvs[1]))

if __name__ == '__main__':
    sc = SparkContext()
    
    idles = sc.textFile('/user/ch3019/capstone/idles_2015', use_unicode=False).cache()
    Hex = idles.mapPartitions(parseIdles).reduceByKey(reducer).sortByKey()    
    
    columns = sc.parallelize(["GRID,date,idle_count,idle_time_count"])
    columns.union(Hex.map(saveformat)).saveAsTextFile('capstone/hexagon_2015')
# end{main}