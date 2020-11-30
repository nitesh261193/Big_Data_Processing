# --------------------------------------------------------
#
# PYTHON PROGRAM DEFINITION
#
# The knowledge a computer has of Python can be specified in 3 levels:
# (1) Prelude knowledge --> The computer has it by default.
# (2) Borrowed knowledge --> The computer gets this knowledge from 3rd party libraries defined by others
#                            (but imported by us in this program).
# (3) Generated knowledge --> The computer gets this knowledge from the new functions defined by us in this program.
#
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer first processes this PYTHON PROGRAM DEFINITION section of the file.
# On it, our computer enhances its Python knowledge from levels (2) and (3) with the imports and new functions
# defined in the program. However, it still does not execute anything.
#
# --------------------------------------------------------

import pyspark
import pyspark.sql.functions
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.functions import date_format


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------


def my_main(spark, my_dataset_dir, bus_stop, bus_line, hours_list):
    # 1. We define the Schema of our DF.

    my_schema = pyspark.sql.types.StructType(
        [pyspark.sql.types.StructField("date", pyspark.sql.types.StringType(), False),
         pyspark.sql.types.StructField("busLineID", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("busLinePatternID", pyspark.sql.types.StringType(), False),
         pyspark.sql.types.StructField("congestion", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("longitude", pyspark.sql.types.FloatType(), False),
         pyspark.sql.types.StructField("latitude", pyspark.sql.types.FloatType(), False),
         pyspark.sql.types.StructField("delay", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("vehicleID", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("closerStopID", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("atStop", pyspark.sql.types.IntegerType(), False)
         ])

    # 2. Operation C2: 'read' to create the DataFrame from the dataset and the schema
    try:
        ## read data from file
        inputDF = spark.read.format("csv") \
            .option("delimiter", ",") \
            .option("quote", "") \
            .option("header", "false") \
            .schema(my_schema) \
            .load(my_dataset_dir)

        ## hour is fetched with date column and stored in new column named "hour"
        inputDF = inputDF.withColumn("hour", format_string("%02d", hour(F.to_timestamp("date", "yyyy-MM-dd HH:mm:ss"))))

        ## weekday is fetched with date coulmn and stored in new column "is weekday"
        # weekday will be in three char and take bool operation with ["Sat", "Sun"] and return 1 or 0
        inputDF = inputDF.withColumn("is_weekend", date_format("date", 'EEE').isin(["Sat", "Sun"]).cast("int"))

        ## dataframe rows are filtered where busStop, weekday, Busline, AtStop, hourlist is matched
        filterdf = inputDF.filter((inputDF["closerStopID"] == bus_stop) & (inputDF["is_weekend"] == 0) & (
                inputDF["busLineID"] == bus_line) & (inputDF["atStop"] == 1) & (inputDF["hour"].isin(hours_list)))

        ## Same hours are grouped and calculate avg delay
        solutionDF = filterdf.groupBy("hour").avg("delay")

        ## create new dataframe with column hour and avg delay
        solutionDF = solutionDF.select("hour", round("avg(delay)", 2).alias('averageDelay'))

        ## Solution is sorted with delay column
        solutionDF = solutionDF.orderBy("averageDelay", ascending=True)

        ## soultion is saved
        solutionDF.persist()

        ## Solution dataframe is collected
        resVAL = solutionDF.collect()
        for item in resVAL:
            print(item)
    except Exception as e:
        print(e)


# --------------------------------------------------------
#
# PYTHON PROGRAM EXECUTION
#
# Once our computer has finished processing the PYTHON PROGRAM DEFINITION section its knowledge is set.
# Now its time to apply this knowledge.
#
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer finally processes this PYTHON PROGRAM EXECUTION section, which:
# (i) Specifies the function F to be executed.
# (ii) Define any input parameter such this function F has to be called with.
#
# --------------------------------------------------------
if __name__ == '__main__':
    # 1. We use as many input arguments as needed
    bus_stop = 279
    bus_line = 40
    hours_list = ["07", "08", "09"]

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset and my_result
    my_local_path = "D://MS_CIT/BD/New_folder/"
    my_databricks_path = "/"

    my_dataset_dir = "my_dataset_complete/"

    if not local_False_databricks_True:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    my_main(spark, my_dataset_dir, bus_stop, bus_line, hours_list)
