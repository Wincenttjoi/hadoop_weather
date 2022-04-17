import mapreduce.KMeansMapper;
import mapreduce.KMeansReducer;
import model.Centroid;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.List;

public class Main {
//    public static void main(String[] args) throws CsvValidationException {
//        CentroidController centroidController = new CentroidController("test.csv");
//
//        try {
//            List<Centroid> centroidList = centroidController.generateInitialCentroids(3);
//
//            for (Centroid centroid : centroidList
//            ) {
//                System.out.println(centroid.toString());
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//
//            Field[] fields = Statistics.class.getDeclaredFields();
//            for (Field field : fields
//            ) {
//                System.out.println(field.getName());
//                System.out.println(field.get(fileUtils.getBeans().get(0)));
//            }
//    }

    public static void main(String[] args) throws IOException, NoSuchFieldException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();

        if (remainingArgs.length != 2) {
            System.out.println("Usage: <input> <output>");
            System.exit(1);
        }
        // Parameter settings
        final String INPUT = remainingArgs[0];
        final String OUTPUT = remainingArgs[1];
        final int DISTANCE = conf.getInt("distance", 2);
        final int K = conf.getInt("k", 3);

        CentroidController centroidController = new CentroidController(INPUT);
        List<Centroid> centroidList = centroidController.generateInitialCentroids(K);

        for (int i = 0; i < K; i++) {
            conf.set("centroid." + i, centroidList.get(i)
                                                  .toString());
        }

        // workflow

        boolean stop = false;
        boolean succeed = true;
        int i = 0;

        while (!stop) {
            i++;

            // job configuration
            Job job = Job.getInstance(conf, "iteration: " + i);
            job.setJarByClass(Main.class);
            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);
            job.setNumReduceTasks(K);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
//            FileInputFormat.addInputPath(job, new Path(INPUT));
//            FileOutputFormat.setOutputPath(job, new Path(OUTPUT));

            succeed = job.waitForCompletion(true);

            if (!succeed) {
                System.out.println("Iteration + " + i + " has failed. Please try again");
                System.exit(2);
            }
        }

    }
}
