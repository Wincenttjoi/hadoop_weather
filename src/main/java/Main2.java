import mapreduce.LocationMapper;
import mapreduce.LocationReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;

public class Main2 {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
//        String log4jConfPath = "/src/resources/log4j.properties";
//        PropertyConfigurator.configure(log4jConfPath);
//        conf.addResource(new Path("config.xml"));
//
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();

        if (remainingArgs.length != 2) {
            System.out.println("arguments: <input> <output>");
            System.exit(1);
        }
        // Parameter settings
        final String INPUT = remainingArgs[0];
        final String OUTPUT = remainingArgs[1];

        Job job = Job.getInstance(conf, "location");
        job.setJarByClass(Main.class);
        job.setMapperClass(LocationMapper.class);
        job.setReducerClass(LocationReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(INPUT));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT));
//            job.setInputFormatClass(TextInputFormat.class);
//            job.setOutputFormatClass(TextOutputFormat.class);


        try {
            System.exit(job.waitForCompletion(true) ?
                        0 :
                        1);
        } catch (InterruptedException e) {
            System.out.println("interrupted");
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            System.out.println("class not found");
            throw new RuntimeException(e);
        }
    }
}
