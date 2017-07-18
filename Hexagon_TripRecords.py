from pyspark import SparkContext
import csv
import numpy as np
import rtree
import geopandas as gpd
import shapely.geometry as geom
import pyproj

def parseGreen(records):
    reader = csv.reader(records) 
    for row in reader:
        if len(row) == 37:
            if len(row[2]) == 19 and len(row[13]) == 19:
                if row[2][6:10] == '2015' and row[13][6:10] == '2015':
                    pickup_date = row[2][8:10] + row[2][:2] + row[2][3:5]
                    dropoff_date = row[13][8:10] + row[13][:2] + row[13][3:5]
                    pickup_lng = float(row[29])
                    pickup_lat = float(row[30])
                    dropoff_lng = float(row[32])
                    dropoff_lat = float(row[33])
                    yield(pickup_date, pickup_lng, pickup_lat, dropoff_date, dropoff_lng, dropoff_lat)

def parseYellow(records):
    reader = csv.reader(records)
    for row in reader:
        if len(row) == 38:
            if len(row[0]) == 19 and len(row[11]) == 19:
                if row[0][6:10] == '2015' and row[11][6:10] == '2015':
                    pickup_date = row[0][8:10] + row[0][:2] + row[0][3:5]
                    dropoff_date = row[11][8:10] + row[11][:2] + row[11][3:5]
                    pickup_lng = float(row[30])
                    pickup_lat = float(row[31])
                    dropoff_lng = float(row[33])
                    dropoff_lat = float(row[34])
                    yield(pickup_date, pickup_lng, pickup_lat, dropoff_date, dropoff_lng, dropoff_lat)

def tr_hex(records):
    hexagon = gpd.GeoDataFrame.from_file('Hexagon_clipped.geojson')
    proj = pyproj.Proj(init='epsg:2263', preserve_units=True)
    
    counts = {}
    index = rtree.Rtree()
    for idx, geometry in zip(hexagon.index.values, hexagon.geometry):
        index.insert(idx, geometry.bounds)
    
    for row in records:
        ## pickup
        p = geom.Point(proj(row[1], row[2]))
        potentialMatches = index.intersection((p.x, p.y, p.x, p.y))
        match = None
        for idx in potentialMatches:
            if hexagon.geometry[idx].contains(p):
                match = idx
                break
        if match:
            k = (hexagon.GRID_ID[match], row[0])
            v = counts.get(k,(0,0))
            counts[k] = (v[0]+1, v[1])
            #yield ((hexagon['GRID_ID'][match], row[0]))
            
        ## dropoff
        p = geom.Point(proj(row[4], row[5]))
        potentialMatches = index.intersection((p.x, p.y, p.x, p.y))
        match = None
        for idx in potentialMatches:
            if hexagon.geometry[idx].contains(p):
                match = idx
                break
        if match:
            k = (hexagon.GRID_ID[match], row[3])
            v = counts.get(k,(0,0))
            counts[k] = (v[0], v[1]+1)
            #yield ((hexagon['GRID_ID'][match], row[3]))
    return counts.items()

def reducer(x,y):
    return (x[0]+y[0], x[1]+y[1])

def saveformat(kvs):
    return ','.join(map(str, kvs[0])) + ',' + ','.join(map(str, kvs[1]))

if __name__ == '__main__':
    sc = SparkContext()
    
    ## LPEP TripRecord Data: 37 columns
    ### 2: lpep_pickup_datetime
    ### 13: lpep_dropoff_datetime
    ### 29: pickup_longitude
    ### 30: pickup_latitude
    ### 32: dropoff_longitude
    ### 33: dropoff_latitude
    LPEP_path = '/gws/projects/project-taxi_capstone_2016/data/TLC/LPEP2015/TripRecord_'
    green = sc.textFile(LPEP_path+'CMT.csv', use_unicode=False).union(
            sc.textFile(LPEP_path+'VTS.csv', use_unicode=False))
    green_tr = green.mapPartitions(parseGreen).mapPartitions(tr_hex)
    #green.mapPartitions(parseGreen).map(lambda x:','.join(map(str,x))).saveAsTextFile('capstone/green')
    
    ## TPEP TripRecord Data 38 columns
    ### 0: tpep_pickup_datetime
    ### 11: tpep_dropoff_datetime
    ### 30: pickup_longitude
    ### 31: pickup_latitude
    ### 33: dropoff_longitude
    ### 34: dropoff_latitude
    TPEP_path = '/gws/projects/project-taxi_capstone_2016/data/TLC/TPEP2015/TripRecord_'
    yellow = sc.textFile(TPEP_path+'CMT.csv', use_unicode=False).union(
             sc.textFile(TPEP_path+'VTS.csv', use_unicode=False))
    yellow_tr = yellow.mapPartitions(parseYellow).mapPartitions(tr_hex)
    #yellow.mapPartitions(parseYellow).map(lambda x:','.join(map(str,x))).saveAsTextFile('capstone/yellow')
    
    ##
    Hex = yellow_tr.union(green_tr).reduceByKey(reducer).sortByKey()
    columns = sc.parallelize(["GRID,date,pickup_count,dropoff_count"])
    columns.union(Hex.map(saveformat)).saveAsTextFile('capstone/hexagon_pd')
# end{main}