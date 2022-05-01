# load input files into HDFS
hdfs dfs -mkdir input-stats
hdfs dfs -put input/* input-stats
