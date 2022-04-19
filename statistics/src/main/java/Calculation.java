import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import utils.InputUtils;

import java.io.IOException;

public class WordCount {
    private static final int STATION = 0;
    private static final int DATE = 1;
    private static final int ELEVATION = 2;
    private static final int TEMPERATURE_C = 3;
    private static final int HUMIDITY = 4;
    private static final int WIND_DIRECTION = 5;
    private static final int WIND_SPEED = 6;

    public static void main(String[] args) throws Exception {
        String maxTemperatureOutput = "output/maxTemperature";

        /* Max Temperature */
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "MaxTemperature");
        job1.setJarByClass(WordCount.class);
        job1.setMapperClass(TokenizerMapper.class);
        job1.setCombinerClass(FloatMaxReducer.class);
        job1.setReducerClass(FloatMaxReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(maxTemperatureOutput));
        j
        // System.exit(job1.waitForCompletion(true) ? 0 : 1);

        /* Min Temperature */

    }

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, FloatWritable> {

        private final FloatWritable floatValue = new FloatWritable();
        private final Text stationMonth = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String record = value.toString();
            if (!record.equals("station,valid,elevation,tmpc,relh,drct,sped")) {
                String[] tokens = record.split(",");
                String station = tokens[STATION];
                String date = tokens[DATE];
                String month = InputUtils.getMonth(date);
                String temperature = tokens[TEMPERATURE_C];

                stationMonth.set(station + month);
                floatValue.set(Float.parseFloat(temperature));
                context.write(stationMonth, floatValue);
            }
        }
    }

    public static class FloatMaxReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        private final FloatWritable result = new FloatWritable();

        public void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            float max = Float.MIN_VALUE;
            for (FloatWritable val : values) {
                max = Float.max(max, val.get());
            }
            result.set(max);
            context.write(key, result);
        }
    }
}