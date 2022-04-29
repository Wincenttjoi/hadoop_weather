# check if input files exist in HDFS

# load input files into HDFS
hdfs dfs -mkdir input-stats
#hdfs dfs -put input/* input-stats
hdfs dfs -put ../download/resources/raw_data/* input-stats

# start mapreduce job
hadoop jar ../statistics.jar Main input-stats output-stats

# display output
hdfs dfs -cat output-stats/part-r-00000