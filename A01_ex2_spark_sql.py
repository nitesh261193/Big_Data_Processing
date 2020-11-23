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
import pyspark.sql.functions as f
from pyspark.sql import functions as F
from pyspark.sql.functions import *


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark, my_dataset_dir, vehicle_id):
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
        inputDF = spark.read.format("csv") \
            .option("delimiter", ",") \
            .option("quote", "") \
            .option("header", "false") \
            .schema(my_schema) \
            .load(my_dataset_dir)

        # TO BE COMPLETED
        # print(inputDF.count())
        inputDF = inputDF.withColumn("day", format_string("%02d",
                                                          dayofmonth(F.to_date("date", "yyyy-MM-dd HH:mm:ss"))))
        filterDf = inputDF.filter(inputDF["vehicleID"] == vehicle_id)
        dropDisDF = filterDf.groupBy(["day", "busLineID"]).count()
        dropDisDF = dropDisDF.groupBy('day').count().select('day', f.col('count').alias('n'))
        max_n = dropDisDF.orderBy(dropDisDF.n.desc()).first().n
        MaxBusInDay = dropDisDF.filter(dropDisDF["n"] == max_n)
        # MaxBusInDay.show()
        l1 = []
        for i in range(MaxBusInDay.count()):
            l1.append(MaxBusInDay.collect()[i].asDict("day")["day"])
        dropDisDF = filterDf.groupBy(["day", "busLineID"]).count()
        DF = dropDisDF.filter(dropDisDF.day.isin(l1)).select('day', 'busLineID')
        solutionDF = DF.groupBy('day').agg(F.collect_list("busLineID").alias("sortedBusLineID"))
        # Operation A1: 'collect' to get all results
        resVAL = sorted(solutionDF.collect())
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
    vehicle_id = 33145

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset and my_result
    my_local_path = "D://MS_CIT/BD/New_folder/"
    my_databricks_path = "/"

    # my_dataset_complete
    # test_database
    my_dataset_dir = "my_dataset_complete/"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    my_main(spark, my_dataset_dir, vehicle_id)
