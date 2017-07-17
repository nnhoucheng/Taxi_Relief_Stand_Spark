from pyspark import SparkContext
import csv
import numpy as np

def trans(timestamp):
    return int(timestamp[:2])*3600 + int(timestamp[2:4])*60 + int(timestamp[4:])
    
def trans_(time):
    hour = time//3600
    time = time%3600
    minute = time//60
    second = time%60
    return ('0'+str(hour))[-2:] + ('0'+str(minute))[-2:] + ('0'+str(second))[-2:]

def checkspeed(row1, row2, speed_threshold = 10*3.28084/120):
    speed = 1.*np.sqrt((row1[1]-row2[1])**2 + (row1[2]-row2[2])**2) / (row2[0]-row1[0])
    if speed <= speed_threshold:
        return True
    else:
        return False    
    
def parse(records):
    # local variables
    time_thresold = 30*60
    ## flag
    sumlng = 0
    sumlat = 0
    countpoints = 0
    sumtime = 0
    ## 
    vehicleID = ''
    date = ''
    ##
    currentRow = None
    lastRow = None
    
    reader = csv.reader(records)
    
    for row in reader:
        ## Filter out N/A
        if len(row) == 5:
            ## Filter out customer trip
            if row[-1] == '0':
                ## Points in 2015
                if row[1][:2] == '15':
                    cvID = row[0]
                    cdate = row[1][:6]
                    currentRow = [trans(row[1][6:]), int(row[2]), int(row[3])]
                    
                    if cvID == vehicleID and cdate == date:
                        if checkspeed(lastRow, currentRow):
                            countpoints += 1
                            sumtime += currentRow[0] - lastRow[0]
                            sumlng += currentRow[1]
                            sumlat += currentRow[2]
                        elif countpoints > 0:
                            if sumtime >= time_thresold:
                                yield (vehicleID, date+trans_(lastRow[0]-sumtime), sumtime, 
                                       int(round(1.*sumlng/countpoints)), 
                                       int(round(1.*sumlat/countpoints)))
                            countpoints = 0
                            sumlng = 0
                            sumlat = 0
                            sumtime = 0
                    else:
                        if countpoints > 0:
                            if sumtime >= time_thresold:
                                yield (vehicleID, date+trans_(lastRow[0]-sumtime), sumtime, 
                                       int(round(1.*sumlng/countpoints)), 
                                       int(round(1.*sumlat/countpoints)))
                            countpoints = 0
                            sumlng = 0
                            sumlat = 0
                            sumtime = 0
                        vehicleID = cvID
                        date = cdate
                    
                    lastRow = currentRow
                       
def saveformat(kvs):
    return kvs[0] + ',' + ','.join(map(str, kvs[1]))

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
    ## Vehichle ID Data:
    ### shl_number for green, medallion for yellow
    #green_path = '/user/ch3019/capstone/shl_number.csv'
    #yellow_path = '/user/ch3019/capstone/medallion.csv'
    #green = sc.textFile(green_path, use_unicode=False).cache()
    #yellow = sc.textFile(yellow_path, use_unicode=False).cache()
    
    # Step 1: Find Idle points
    idles = Breadcrumb.mapPartitions(parse)
    ## merge with green and yellow taxi vehicle ID
    #g_ = idles.map(lambda x: (x[0], x[1:])).join(green.map(lambda x: (x, "g")))
    #y_ = idles.map(lambda x: (x[0], x[1:])).join(yellow.map(lambda x: (x, "y")))
    #columns_g = sc.parallelize(["shl_number,start_datetime,duration,longitude,latitude,classifer"])
    #columns_y = sc.parallelize(["medallion,start_datetime,duration,longitude,latitude,classifer"])
    ## Save to file
    idles.map(lambda x: ','.join(map(str, x))).saveAsTextFile('capstone/idles_2015')
    #columns_g.union(g_.map(saveformat)).saveAsTextFile('capstone/green')
    #columns_y.union(y_.map(saveformat)).saveAsTextFile('capstone/yellow')
        
# end{main}