import mapreduce.KMeansMapper;
import mapreduce.KMeansReducer;
import mapreduce.LocationMapper;
import mapreduce.LocationReducer;
import model.Centroid;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.PropertyConfigurator;
import utils.DistanceUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class Main {
    /**
     * @param args <input> <output> <finaloutput>
     * @throws IOException
     * @throws NoSuchFieldException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    public static void main(String[] args) throws IOException, NoSuchFieldException, InterruptedException, ClassNotFoundException {

        // job 1 - preprocessing of data
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        String log4jConfPath = "/src/resources/log4j.properties";
        PropertyConfigurator.configure(log4jConfPath);
        if (remainingArgs.length != 3) {
            System.out.println("arguments: <input> <output> <finaloutput>");
            System.exit(1);
        }
//        // Parameter settings
        final String INPUT = remainingArgs[0];
        final String OUTPUT = remainingArgs[1];
        final String FINAL_OUTPUT = remainingArgs[2];

            Job job = Job.getInstance(conf, "location");
            job.setJarByClass(Main.class);
            job.setMapperClass(LocationMapper.class);
            job.setReducerClass(LocationReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(INPUT));
            FileOutputFormat.setOutputPath(job, new Path(OUTPUT));
//            job.setInputFormatClass(TextInputFormat.class);
//            job.setOutputFormatClass(TextOutputFormat.class);


        job.waitForCompletion(true);

        // job 2 - k_means algorithm
        Configuration conf2 = new Configuration();
        conf2.setInt("k", 3);
        final int k = conf2.getInt("k", 3);

        CentroidController centroidController = new CentroidController(OUTPUT, conf);
        List<Centroid> centroidList = centroidController.generateInitialCentroids(k);
//
        for (int i = 0; i < k; i++) {
            conf2.set("centroid." + i, Base64.getEncoder().encodeToString(SerializationUtils.serialize(
                    (Serializable) centroidList.get(i))));
        }
//
        boolean stop = false;
        boolean succeed = true;
        int i = 0;

        while (!stop) {
            i++;

            Job job2 = Job.getInstance(conf2, "iteration: " + i);
            job2.setJarByClass(Main.class);
            job2.setMapperClass(KMeansMapper.class);
            job2.setReducerClass(KMeansReducer.class);
            job2.setNumReduceTasks(k);
            job2.setOutputKeyClass(IntWritable.class);
            job2.setOutputValueClass(Centroid.class);
            FileInputFormat.addInputPath(job2, new Path(OUTPUT));
            FileOutputFormat.setOutputPath(job2, new Path(FINAL_OUTPUT));
            job2.setInputFormatClass(TextInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);


            succeed = job2.waitForCompletion(true);

            if (!succeed) {
                System.out.println("Iteration + " + i + " has failed. Please try again");
                System.exit(2);
            }

            List<Centroid> newCentroidList = readFromConf(conf2, k, FINAL_OUTPUT);

            if (stopCluster(centroidList, newCentroidList, i)) {
                // stop the function and write to output
                finalize(conf2, newCentroidList, "finaloutput");
                System.out.println("IM DONE WITH FIRST JOB");
                break;
            } else {
                centroidList = new ArrayList<>(newCentroidList);
                                for (int j = 0; j < k; j++) {
                    conf2.unset("centroid." + j);
//                    conf2.set("centroid." + j, newCentroidList.get(j).toString());
                                    conf2.set("centroid." + j, Base64.getEncoder()
                                                                     .encodeToString(SerializationUtils.serialize(
                                                                             newCentroidList.get(j))));
                }
                deleteHdfsFile(conf2, FINAL_OUTPUT);
            }
        }
    }


    /**
     * Read centroids from configuration file
     * @param conf configuration file
     * @param k number of clusters
     * @param output outfile directory name
     * @return list of centroids
     * @throws IOException
     */
    private static List<Centroid> readFromConf(Configuration conf, int k, String output) throws IOException {
        List<Centroid> centroidList = new ArrayList<>();

        FileSystem fileSystem = FileSystem.get(conf);
        FileStatus[] status = fileSystem.listStatus(new Path(output));

        for (int i = 0; i < status.length; i++) {
            if (!status[i].getPath()
                          .toString()
                          .endsWith("_SUCCESS")) {
                BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(status[i].getPath())));
                String[] string = br.readLine().split("\t");
                int centroidId = Integer.parseInt(string[0]);
//                String[] centroid = string[1].split(",");
//                centroidList.add(new Centroid(centroid));
                centroidList.add(SerializationUtils.deserialize(Base64.getDecoder()
                                                                      .decode(string[1])));
                br.close();
            }
        }

        return centroidList;
    }


    /**
     * Stops clusters if centroids difference is less than a specified threshold or if above certain iteration
     * @param old old list of centroids
     * @param news new list of centroids
     * @param iterations number of iterations
     * @return boolean value of whether to stop the cluster
     */
    private static boolean stopCluster(List<Centroid> old, List<Centroid> news, int iterations) {
        for (int i = 0; i < old.size(); i++) {
            double distance = DistanceUtils.calculateDistance(old.get(i).getAttributes(),
                                                              news.get(i).getAttributes());
            // check threshold
            if (distance < 1) {
                return true;
            }
        }
        if (iterations > 30) {
            return true;
        }
        return false;
    }

    /**
     * Writes centroids into a txt file
     * @param conf configuration file
     * @param centroids final centroids to be written
     * @param output output directory path name
     * @throws IOException
     */
    private static void finalize(Configuration conf, List<Centroid> centroids, String output) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        FSDataOutputStream dos = hdfs.create(new Path(output + "/centroids.txt"), true);
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(dos));

        //Write the result in a unique file
        for (Centroid centroid : centroids) {
            br.write(centroid.toString());
            br.newLine();
        }

        br.close();
        hdfs.close();
    }

    /**
     * Deletes the hdfs output files so that map reduce job can be run again
     * @param conf configuration file
     * @param output output directory path name
     * @throws IOException
     */
    private static void deleteHdfsFile(Configuration conf, String output) throws IOException {
        FileSystem fileSystem = FileSystem.get(conf);
        fileSystem.delete(new Path(output), true);
    }


}
