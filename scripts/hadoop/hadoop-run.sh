# start mapreduce job
hadoop jar ../kmeans.jar KMeans input-kmeans output-kmeans

# display output
hdfs dfs -cat output-kmeans/part-r-00000


