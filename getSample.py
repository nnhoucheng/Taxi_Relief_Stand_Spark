from pyspark import SparkContext
import csv
import sys

def find(records):
    reader = csv.reader(records)
    for row in reader:
        ## Filter out N/A
        if len(row) == 5:
            ## check VID and DATE
            if row[0] == VID and row[1][:6] == DATE:
                yield([row[1][6:], row[2], row[3], row[4]])

if __name__ == '__main__':
    sc = SparkContext()
    
    if len(sys.argv) == 3:
        VID = sys.argv[1]
        DATE = sys.argv[2]
    else:
        VID = 'V105336832'
        DATE = '150102'
    
    # Step 0: Read Data
    ## Breagcrumb Data: 5 columns
    ### 0: shl_number or medallion
    ### 1: timestamp in yymmddhhmmss
    ### 2: longitude in crs 2263
    ### 3: latitude in crs 2263
    ### 4: number of passengers in taxi
    path = '/gws/projects/project-taxi_capstone_2016/data/breadcrumb_nad83'
    Breadcrumb = sc.textFile(path, use_unicode=False).cache()
    
    allrows = Breadcrumb.mapPartitions(find)
    column = sc.parallelize(["time,longitude,latitude,num_customer"])
    column.union(allrows.map(lambda x:','.join(x))).saveAsTextFile('capstone/sample')
        
# end{main}