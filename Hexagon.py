from pyspark import SparkContext
import csv
import numpy as np
import rtree
import geopandas as gpd
import shapely.geometry as geom

def parseIdles(records):
    reader = csv.reader(records)
    hexagon = gpd.GeoDataFrame.from_file('hexagon.geojson')
    
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
            if relief.geometry[idx].contains(p):
                match = idx
                break
        if match:
            k = (hexagon.GRID_ID[match], date)
            v = count.get(k,(0,0,0,0))
            count[k] = (v[0]+1, v[1]+int(row[2]), 0, 0)
            #yield ((match, date), int(row[2]))
    return count.items()   

def parseGreen(records):
    reader = csv.reader(records):    
    for row in reader:
        if len(row) == 37:
            pickup_date = row[2][8:10] + row[2][:2] + row[2][3:5]
            dropoff_date = row[13][8:10] + row[13][:2] + row[13][3:5]
            pickup_lng = int(row[29])
            pickup_lat = int(row[30])
            dropoff_lng = int(row[32])
            dropoff_lat = int(row[33])
            yield(pickup_date, pickup_lng, pickup_lat, dropoff_date, dropoff_lng, dropoff_lat)

def parseYellow(records):
    reader = csv.reader(records):    
    for row in reader:
        if len(row) == 38:
            pickup_date = row[0][8:10] + row[0][:2] + row[0][3:5]
            dropoff_date = row[11][8:10] + row[11][:2] + row[11][3:5]
            pickup_lng = int(row[30])
            pickup_lat = int(row[31])
            dropoff_lng = int(row[33])
            dropoff_lat = int(row[34])
            yield(pickup_date, pickup_lng, pickup_lat, dropoff_date, dropoff_lng, dropoff_lat)

def tr_hex(records):
    hexagon = gpd.GeoDataFrame.from_file('hexagon.geojson')
    
    count = {}
    index = rtree.Rtree()
    for idx, geometry in zip(hexagon.index.values, hexagon.geometry):
        index.insert(idx, geometry.bounds)
    
    for row in records:
        ## pickup
        x,y = int(row[1]), int(row[2])
        potentialMatches = index.intersection((x, y, x, y))
        p= geom.Point(x,y)        
        match = None
        for idx in potentialMatches1:
            if hexagon.geometry[idx].contains(p):
                match = idx
                break
        if match:
            k = (hexagon.GRID_ID[match], row[0])
            v = count.get(k,(0,0,0,0))
            count[k] = (0, 0, v[0]+1, v[1])
            #yield ((hexagon['GRID_ID'][match], row[0]), (1,0))
            
        ## dropoff
        x,y = int(row[4]), int(row[5])
        potentialMatches = index.intersection((x, y, x, y))
        p= geom.Point(x,y)        
        match = None
        for idx in potentialMatches1:
            if hexagon.geometry[idx].contains(p):
                match = idx
                break
        if match:
            k = (hexagon.GRID_ID[match], row[3])
            v = count.get(k,(0,0,0,0))
            count[k] = (0, 0, v[0], v[1]+1)
            #yield ((hexagon['GRID_ID'][match], row[3]), (0,1))
    return count.items()

def reducer(x,y):
    return [x[i]+y[i] for i in range(4)]

def saveformat(kvs):
    return kvs[0] + ',' + ','.join(map(str, kvs[1]))

if __name__ == '__main__':
    sc = SparkContext()
    
    idles = sc.textFile('/user/ch3019/capstone/idles', use_unicode=False).cache()
    hex_idles = idles.mapPartitions(parseIdles)
    
    ## LPEP TripRecord Data: 37 columns
    ### 2: lpep_pickup_datetime
    ### 13: lpep_dropoff_datetime
    ### 29: pickup_longitude
    ### 30: pickup_latitude
    ### 32: dropoff_longitude
    ### 33: dropoff_latitude
    LPEP_path = '/gws/projects/project-taxi_capstone_2016/data/TLC/LPEP201/TripRecords_'
    green = sc.textFile(LPEP_path+'CMT.csv', use_unicode=False).union(
            sc.textFile(LPEP_path+'VTS.csv', use_unicode=False))
    green_tr = green.mapPartitions(parseGreen).mapPartitions(tr_hex)
    
    ## TPEP TripRecord Data 38 columns
    ### 0: tpep_pickup_datetime
    ### 11: tpep_dropoff_datetime
    ### 30: pickup_longitude
    ### 31: pickup_latitude
    ### 33: dropoff_longitude
    ### 34: dropoff_latitude
    TPEP_path = '/gws/projects/project-taxi_capstone_2016/data/TLC/LPEP201/TripRecords_'
    yellow = sc.textFile(TPEP_path+'CMT.csv', use_unicode=False).union(
             sc.textFile(TPEP_path+'VTS.csv', use_unicode=False))
    yellow_tr = yellow.mapPartitions(parseYellow).mapPartitions(tr_hex)
    
    ##
    Hex = hex_idles.union(green_tr).union(yellow_tr).reduceByKey(reducer).sortByKey()
    
    columns = sc.parallelize(["GRID,date,idle_count,idle_time_count,pickup_count,dropoff_count"])
    columns.union(Hex.map(saveformat)).saveAsTextFile('capstone/hexagon')
# end{main}