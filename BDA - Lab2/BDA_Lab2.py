from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from operator import add
import sys

# Set up Spark Context
sc = SparkContext(appName = "BDA Lab2")
spark = SparkSession.builder.getOrCreate()

# Reading Data
df_tempReadings = spark.read.csv("file:///home/x_kesma/Lab1/input_data/temperature-readings.csv", header = False, sep = ';' )
df_tempReadings = df_tempReadings.withColumnRenamed("_c0", "stationNumber")\
                                 .withColumnRenamed("_c1", "date")\
                                 .withColumnRenamed("_c2", "time")\
                                 .withColumnRenamed("_c3", "airTemperature")\
                                 .withColumnRenamed("_c4", "quality")

df_precipitation = spark.read.csv("file:///home/x_kesma/Lab1/input_data/precipitation-readings.csv", header = False, sep = ';' )
df_precipitation = df_precipitation.withColumnRenamed("_c0", "stationNumber")\
                                 .withColumnRenamed("_c1", "date")\
                                 .withColumnRenamed("_c2", "time")\
                                 .withColumnRenamed("_c3", "precipitation")\
                                 .withColumnRenamed("_c4", "quality")

rdd_OstStations = sc.textFile("file:///home/x_kesma/Lab1/input_data/stations-Ostergotland.csv")\
                            .map(lambda line: line.split(";"))\
                            .map(lambda line:line[0])


# Assignment 1: What are the lowest and highest temperatures measured each year for the period 1950-2014.
# Using Dataframes


df_filtered_1 = df_tempReadings.select("stationNumber", F.year(F.col('date')).alias("Year"),\
                                     F.col("airTemperature").cast("float"))\
                              .filter((F.col("Year")>=1950) & ((F.col("Year")<=2014)))

out = df_filtered_1.groupBy("Year")\
            .agg(F.min('airTemperature').alias('MinTemp'),F.max('airTemperature').alias('MaxTemp'))\
            .orderBy("Year")
            
out.repartition(1).write.csv("file:///home/x_kesma/Lab1/input_data/results/BDA_LAB2/Q1",sep=",", header=True)

# 2_1 Count the number of readings for each month in the period of 1950-2014 which are higher than 10 degrees

df_filtered_2 = df_tempReadings.select("stationNumber", F.year(F.col('date')).alias("Year"),\
                                     F.month(F.col("date")).alias("Month"),\
                                     F.col("airTemperature").cast("float"))\
                              .filter(((F.col("Year")>=1950) & ((F.col("Year")<=2014))) &(F.col("airTemperature")>10))
                                      
out = df_filtered_2.groupBy("Year", "Month")\
             .agg(F.count("stationNumber").alias("Value"))\
             .orderBy("value",ascending=False)

out.repartition(1).write.csv("file:///home/x_kesma/Lab1/input_data/results/BDA_LAB2/Q2_1",sep=",", header=True)

# 2_2 Repeat the exercise,this time taking only distinct readings from each station.
# That is, if a station reported a reading above 10 degrees in some month, then itappears only
# once in the count for that month

out = df_filtered_2.groupBy("Year", "Month")\
             .agg(F.countDistinct("stationNumber").alias("Value"))\
             .orderBy("value",ascending=False)

out.repartition(1).write.csv("file:///home/x_kesma/Lab1/input_data/results/BDA_LAB2/Q2_2",sep=",", header=True)

# 3 Find the average monthly temperature for each available station in Sweden. Your result
#should include average temperature for each station for each month in the period of 1960-
#2014. Bear in mind that not every station has the readings for each month in this timeframe.

df_filtered_3 = df_tempReadings.select("stationNumber", F.year(F.col('date')).alias("Year"),\
                                     F.month(F.col("date")).alias("Month"),\
                                     F.col("airTemperature").cast("float"))\
                              .filter((F.col("Year")>=1960) & ((F.col("Year")<=2014)))

out = df_filtered_3.groupBy("stationNumber","Year", "Month")\
             .agg(F.avg("airTemperature").alias("avgMonthlyTemperature"))\
             .orderBy("stationNumber","Year", "Month",ascending=False)

out.repartition(1).write.csv("file:///home/x_kesma/Lab1/input_data/results/BDA_LAB2/Q3",sep=",", header=True)

# 4 Provide a list of stations with their associated maximum measured temperatures and
# maximum measured daily precipitation. Show only those stations where the maximum
# temperature is between 25 and 30 degrees and maximum daily precipitation is between 100mm and 200mm.



df_filtered_temp = df_tempReadings.select("stationNumber",\
                                     F.col("airTemperature").cast("float"))\
                              .filter((F.col("airTemperature")>=25) & ((F.col("airTemperature")<=30)))

df_filtered_preci = df_precipitation.select("stationNumber",\
                                     F.col("precipitation").cast("float"))\
                              .filter((F.col("precipitation")>=100) & ((F.col("precipitation")<=200)))

df_join = df_filtered_temp.alias("a").join(df_filtered_preci.alias("b"),
                                           F.col("a.stationNumber")==F.col("b.stationNumber"),"inner")\
                                     .select("a.stationNumber", "a.airTemperature", "b.precipitation")

out = df_join.groupBy("stationNumber")\
             .agg(F.max("airTemperature").alias("maxTemp"),F.max("precipitation").alias("maxDailyPrecipitation"))\
             .orderBy("stationNumber",ascending=False)

out.repartition(1).write.csv("file:///home/x_kesma/Lab1/input_data/results/BDA_LAB2/Q4",sep=",", header=True)

# 5 Calculate the average monthly precipitation for the Ostergotland region (list of stations is provided in the separate file)
# for the period 1993-2016. In order to do this, you will first need to calculate the totalmonthly precipitation for each 
# station before calculating the monthly average (by averaging over stations).

list_OstStations = rdd_OstStations.collect()

broadcastVar = sc.broadcast(list_OstStations)

df_filtered_preci_5 = df_precipitation.select("stationNumber",\
                                     F.year(F.col('date')).alias("Year"),\
                                     F.month(F.col("date")).alias("Month"),\
                                     F.col("precipitation").cast("float"))\
                              .filter(((F.col("Year")>=1993) & ((F.col("Year")<=2016))) & (F.col("stationNumber").isin(broadcastVar.value)))

out = df_filtered_preci_5.groupBy("Year", "Month","stationNumber")\
             .agg(F.sum("precipitation").alias("Sum"))\
             .groupBy("Year", "Month")\
             .agg(F.avg("Sum").alias("avgMonthlyPrecipitation"))\
             .orderBy("year","Month",ascending=False)

out.repartition(1).write.csv("file:///home/x_kesma/Lab1/input_data/results/BDA_LAB2/Q5",sep=",", header=True)

sys.exit(0)