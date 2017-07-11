from pyspark import SparkContext
import os

def parseCSV(list_of_records):
    import csv
    reader = csv.reader(list_of_records)
    for row in reader:
        ## Filter out N/A
        if len(row) == 5:
            ## Filter out customer trip
            if row[-1] == '0':
                ## from '2015-01-01' to '2015-01-31'
                if row[1][:4] == '1501':
                    yield ((row[0], row[1][:6]), (row[1][6:], row[2], row[3]))
# end{parseCSV}

def findidle(values):
    import numpy as np
    ## Transform timestamp to seconds
    def trans(timestamp):
        return int(timestamp[:2])*3600 + int(timestamp[2:4])*60 + int(timestamp[4:])

    ## After grouping by (vehicle, day), extract and sort values by time
    rows = []
    for v in values:
        rows.append([trans(v[0]), int(v[1]), int(v[2])]) 
    rows = np.array(sorted(rows, key=lambda x:x[0]))
    
    ## Check if the speed within the threshold
    def checkspeed(row1, row2, speed_threshold = 10*3.28084/120):
        speed = 1.*np.sqrt((row1[1]-row2[1])**2 + (row1[2]-row2[2])**2) / (row2[0]-row1[0])
        if speed <= speed_threshold:
            return True
        else:
            return False

    idles = []
    time_thresold = 30*60 # time thershold
    flag = False
    start = 0
    for i,row in enumerate(rows):
        if i == 0:
            continue
        if checkspeed(rows[i-1], row) and not flag:
            flag = True
            start = i-1
        elif not checkspeed(rows[i-1], row) and flag:
            flag = False
            time = rows[i-1][0] - rows[start][0]
            if time >= time_thresold:
                idles.append((np.mean(rows[start:i,1]), np.mean(rows[start:i,2]), rows[start][0], time))
    return idles
# end{findidle}                  

def mapback(list_of_records):
    def trans_(time):
        hour = time//3600
        time = time%3600
        minute = time//60
        second = time%60
        return ('0'+str(hour))[-2:] + ('0'+str(minute))[-2:] + ('0'+str(second))[-2:]
    
    for kvs in list_of_records:
        k, vs = kvs
        output = k[0] + ',' + k[1]
        for v in vs:
            yield output + trans_(v[2]) + ',' + str(v[3]) + ',' + str(int(round(v[0]))) + ',' + str(int(round(v[1])))
# end{mapback}
            
            
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
    BC = Breadcrumb.mapPartitions(parseCSV)
    #rdd = sc.parallelize(BC.take(10))
    #rdd.saveAsTextFile('capstone/p1')
    # Step 2: Find idle points    
    Idles = BC.groupByKey().mapValues(findidle)
    #rdd = sc.parallelize(Idles.take(10))
    #rdd.saveAsTextFile('capstone/p2')
    # Step 3: Save idle points
    Idles.mapPartitions(mapback).saveAsTextFile('capstone/tmp')
# end{main}    
    