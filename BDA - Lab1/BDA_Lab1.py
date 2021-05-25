from pyspark import SparkContext
import pyspark.sql.functions as F
from operator import add
import sys

# Set up Spark Context
sc = SparkContext(appName = "BDA Lab1")


# Reading Temperature data
rdd_tempReadings = sc.textFile("file:///home/x_kesma/Lab1/input_data/temperature-readings.csv") \
                            .map(lambda line: line.split(";"))

# Reading Precipitation data
rdd_precReadings = sc.textFile("file:///home/x_kesma/Lab1/input_data/precipitation-readings.csv") \
                            .map(lambda line: line.split(";"))

# Reading Ostergotland Stations data
rdd_OstStations = sc.textFile("file:///home/x_kesma/Lab1/input_data/stations-Ostergotland.csv")\
                            .map(lambda line: line.split(";"))\
                            .map(lambda line:int(line[0]))


# 1
print("Executing Q1")
rdd_filtered_1 = rdd_tempReadings.filter(lambda line: (int(line[1][0:4]))>=1950 and int(line[1][0:4])<=2014) \
                            .map(lambda line: (line[1][0:4],(float(line[3]))))

# Maximum Temp for each year
max_Temp = (rdd_filtered_1.reduceByKey(max)\
            .sortBy(keyfunc=lambda k: k[0],ascending = False))

# Minimum Temp for each year
min_Temp = (rdd_filtered_1.reduceByKey(min)\
            .sortBy(keyfunc=lambda k: k[0],ascending = False))

# Save the output
max_Temp.repartition(1).saveAsTextFile('file:///home/x_kesma/Lab1/input_data/results/BDA_LAB1/Q1_1/')
min_Temp.repartition(1).saveAsTextFile('file:///home/x_kesma/Lab1/input_data/results/BDA_LAB1/Q1_2/')

# 2_1 Count the number of readings for each month in the period of 1950-2014 which are higher than 10 degrees 
print("Executing Q2_1")
rdd_filtered_2_1 = rdd_tempReadings.filter(lambda line: ((int(line[1][0:4]))>=1950\
                                                       and int(line[1][0:4])<=2014)\
                                                       and float(line[3]) >10 )\
                                .map(lambda line: ((line[1][0:4], line[1][5:7]),(line[0],float(line[3]))))\
                                .countByKey()

out_2_1 = sc.parallelize(sorted(rdd_filtered_2_1.items(), key = lambda v:v[1], reverse = True))

out_2_1.repartition(1).saveAsTextFile('file:///home/x_kesma/Lab1/input_data/results/BDA_LAB1/Q2_1/')


# 2_2 Repeat the exercise,this time taking only distinct readings from each station.
# That is, if a station reported a reading above 10 degrees in some month, then itappears only
# once in the count for that month

print("Executing Q2_2")
rdd_filtered_2_2 = rdd_tempReadings.filter(lambda line: ((int(line[1][0:4]))>=1950\
                                                       and int(line[1][0:4])<=2014)\
                                                       and float(line[3]) >10 )\
                                .map(lambda line: (line[1][0:4], line[1][5:7],line[0]))\
                                .distinct()\
                                .map(lambda line: ((line[0],line[1]),(line[2])))\
                                .countByKey()
out_2_2 = sc.parallelize(sorted(rdd_filtered_2_2.items(), key = lambda v:v[1], reverse = True))

out_2_2.repartition(1).saveAsTextFile('file:///home/x_kesma/Lab1/input_data/results/BDA_LAB1/Q2_2/')

# 3 Find the average monthly temperature for each available station in Sweden. Your result
#should include average temperature for each station for each month in the period of 1960-
#2014. Bear in mind that not every station has the readings for each month in this timeframe.

print("Executing Q3")
rdd_filtered_3 = rdd_tempReadings.filter(lambda line: (int(line[1][0:4]))>=1950 and int(line[1][0:4])<=2014) \
                            .map(lambda line: ((line[1][0:4], line[1][5:7], line[0]),(float(line[3]))))\
                            .groupByKey()\
                            .mapValues(lambda val: sum(val)/len(val))
out_3 = (rdd_filtered_3\
            .sortBy(keyfunc=lambda k: (k[0][2],k[0][0],k[0][1]),ascending = False))

out_3.repartition(1).saveAsTextFile('file:///home/x_kesma/Lab1/input_data/results/BDA_LAB1/Q3/')

# 4 Provide a list of stations with their associated maximum measured temperatures and
# maximum measured daily precipitation. Show only those stations where the maximum
# temperature is between 25 and 30 degrees and maximum daily precipitation is between 100mm and 200mm.

print("Executing Q4")
rdd_filter_4 = rdd_precReadings\
                            .filter(lambda line: float(line[3])>=100 and float(line[3])<=200)\
                            .map(lambda line: (line[0],line[3]))\
                            .reduceByKey(max)
        

rdd_tempReadings_4 = rdd_tempReadings.filter(lambda line: float(line[3])>=25 and float(line[3]<=30) )\
                                     .map(lambda line: (line[0],line[3]))\
                                     .reduceByKey(max)

rdd_result = rdd_tempReadings_4.join(rdd_filter_4)

rdd_result.repartition(1).saveAsTextFile('file:///home/x_kesma/Lab1/input_data/results/BDA_LAB1/Q4/')


### 5 Calculate the average monthly precipitation for the Ostergotland region (list of stations is provided in the separate file)
### for the period 1993-2016. In order to do this, you will first need to calculate the totalmonthly precipitation for each 
### station before calculating the monthly average (by averaging over stations).

print("Executing Q5")
list_OstStations = rdd_OstStations.collect()

broadcastVar = sc.broadcast(list_OstStations)

rdd_filter_5 = rdd_precReadings.filter(lambda line: (int(line[0]) in broadcastVar.value) and\
                                                    (int(line[1][0:4])>=1993 and int(line[1][0:4])<=2016))\
                            .map(lambda line: ((line[1][0:4], line[1][5:7], line[0]),(float(line[3]))))\
                            .reduceByKey(add)\
                            .map(lambda line:((line[0][0], line[0][1]),(line[1])))\
                            .groupByKey()\
                            .mapValues(lambda val:sum(val)/len(val))

rdd_filter_5 = (rdd_filter_5\
            .sortBy(keyfunc=lambda k: (k[0][0],k[0][1]),ascending = False))

rdd_filter_5.repartition(1).saveAsTextFile('file:///home/x_kesma/Lab1/input_data/results/BDA_LAB1/Q5/')

sys.exit(0)
