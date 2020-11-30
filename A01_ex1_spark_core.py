# --------------------------------------------------------
#
# PYTHON PROGRAM DEFINITION
#
# The knowledge a computer has of Python can be specified in 3 levels:
# (1) Prelude knowledge --> The computer has it by default.
# (2) Borrowed knowledge --> The computer gets this knowledge from 3rd party libraries defined by others
#                            (but imported by us in this program).
# (3) Generated knowledge --> The computer gets this knowledge from the new functions defined by us in this program.
# r
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer first processes this PYTHON PROGRAM DEFINITION section of the file.
# On it, our computer enhances its Python knowledge from levels (2) and (3) with the imports and new functions
# defined in the program. However, it still does not execute anything.
#
# --------------------------------------------------------

import datetime

import pyspark


# ------------------------------------------
# FUNCTION process_line
# ------------------------------------------
def process_line(line):
    # 1. We create the output variable
    res = ()

    # 2. We get the parameter list from the line
    params_list = line.strip().split(",")

    # (00) Date => The date of the measurement. String <%Y-%m-%d %H:%M:%S> (e.g., "2013-01-01 13:00:02").
    # (01) Bus_Line => The bus line. Int (e.g., 120).
    # (02) Bus_Line_Pattern => The pattern of bus stops followed by the bus. String (e.g., "027B1001"). It can be empty (e.g., "").
    # (03) Congestion => On whether the bus is at a traffic jam (No -> 0 and Yes -> 1). Int (e.g., 0).
    # (04) Longitude => Longitude position of the bus. Float (e.g., -6.269634).
    # (05) Latitude = > Latitude position of the bus. Float (e.g., 53.360504).
    # (06) Delay => Delay of the bus in seconds (negative if ahead of schedule). Int (e.g., 90).
    # (07) Vehicle => An identifier for the bus vehicle. Int (e.g., 33304)
    # (08) Closer_Stop => An idenfifier for the closest bus stop given the current bus position. Int (e.g., 7486). It can be no bus stop, in which case it takes value -1 (e.g., -1).
    # (09) At_Stop => On whether the bus is currently at the bus stop (No -> 0 and Yes -> 1). Int (e.g., 0).

    # 3. If the list contains the right amount of parameters
    if (len(params_list) == 10):
        # 3.1. We set the right type for the parameters
        params_list[1] = int(params_list[1])
        params_list[3] = int(params_list[3])
        params_list[4] = float(params_list[4])
        params_list[5] = float(params_list[5])
        params_list[6] = int(params_list[6])
        params_list[7] = int(params_list[7])
        params_list[8] = int(params_list[8])
        params_list[9] = int(params_list[9])

        # 3.2. We assign res
        res = tuple(params_list)

    # 4. We return res
    return res


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------

## Method for fetching hour from date column
def parse_hour(date_str):
    try:
        return datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").strftime("%H")
    except:
        print(f"Error in value: {date_str}")
        return datetime.datetime.today().hour


## Method for fetching day from date column
## day will be given in integer e.g ['mon -1 Tue -2 ...Sat-6 Sun -7]
## Methid will return integer value as per weekday
def parse_weekday(date_str):
    try:
        return datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").isoweekday()
    except:
        print(f"Error in value: {date_str}")
        return datetime.datetime.today().isoweekday()


## Method is for calculating average
def get_average(rows):
    delays = list(map(lambda line: line[6], rows))
    return sum(delays) / len(delays)


## Method is designed for counting of same entity
def get_count(rows):
    count = list(map(lambda line: rows))
    return len(count)


## Method is created for fetching day of month from date coloumn
def parse_day(date_str):
    try:
        return datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").strftime("%d")
    except:
        print(f"Error in value: {date_str}")
        return datetime.datetime.today().day


## Main Funtion
def my_main(sc, my_dataset_dir, bus_stop, bus_line, hours_list):
    # 1. Operation C1: 'textFile' to load the dataset into an RDD

    try:
        ## read data from file
        inputRDD = sc.textFile(my_dataset_dir)

        ## process_line fn to get all the lines from data and stored in tuple
        inputRDD = inputRDD.map(process_line)

        ## hour is fetched from date coloumn and added in each respective tuple.
        # (Here line[0] represents date column )
        hourRDD = inputRDD.map(lambda line: tuple([*line, (parse_hour(line[0]))]))

        ## weekday is fetched from date column and added in tuple
        # (Here line[0] represents date column )
        dateRDD = hourRDD.map(lambda line: tuple([*line, (parse_weekday(line[0]))]))

        ## filtered rows where given busstop is matched
        #(Here line[8] refers busStop that means bus is stopped at given busstop)
        busStopRDD = dateRDD.filter(lambda line: bus_stop == line[8])

        ## filtered rows where atstpo is 1
        # (Here line[9] refers 1 that means whether bus stops or not at given busstop )
        atStopRDD = busStopRDD.filter(lambda line: line[9]==1)

        ## Filtered row where busline is matched
        # Here line[1] refers busLine
        buslineRDD = atStopRDD.filter(lambda line: bus_line == line[1])

        ## filtered rows where given hours is matched
        # Here .. Line[10] refers hours column that is added in tuple in step 3
        hourfilterRDD = buslineRDD.filter(lambda line: line[10] in hours_list)

        ## filtered rows where date should be weekday
        # Here.. line[11] refers day column that is added in tuple on step 4
        isweekdayRDD = hourfilterRDD.filter(lambda line: line[11] not in [6, 7])

        ## Same hours rows is grouped and calculate avg
        solutionRDD = isweekdayRDD.groupBy(lambda line: line[10]).map(
            lambda line: (line[0], round(get_average(line[1]), 2)))

        ## Stored RDD is sorted out
        solutionRDD = solutionRDD.sortBy(lambda line: line[1])

        ## RDD values are colleced in this step
        resVAL = solutionRDD.collect()
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

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    my_main(sc, my_dataset_dir, bus_stop, bus_line, hours_list)
