from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()

schema = StructType([ 
                    StructField("stationID", StringType(), True),
                    StructField("date", IntegerType(), True),
                    StructField("measure_type", StringType(), True),    
                    StructField("temperature", FloatType(), True)    
                    ])

# read the file as a dataframe
df = spark.read.schema(schema).csv("file:///SparkCourse/1800.csv")
df.printSchema()

# filter out all but the TMIN entries
minTemps = df.filter(df.measure_type == 'TMIN')

# select on the stationID and temperature
stationTemps = minTemps.select("stationID", "temperature")

# aggregate to find the minimum temperature for each station
minTempsByStation = stationTemps.groupBy("stationID").min("temperature")
minTempsByStation = minTempsByStation.withColumnRenamed("min(temperature)", "temperature_c")
minTempsByStation.show()

minTempsByStationF = minTempsByStation.withColumn("temperature_f", func.round(func.col("temperature_c") * .1 * 1.8 + 32, 2)).sort("temperature_c")

# collect the results
results = minTempsByStationF.collect()

for result in results:
    print(result[0]+ f"\t{result[1]:.2f}C" + f"\t{result[2]:.2f}F")
