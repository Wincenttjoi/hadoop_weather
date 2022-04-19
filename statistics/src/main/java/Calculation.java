import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import utils.InputUtils;

import java.io.IOException;

public class Calculation {
    private static final int STATION = 0;
    private static final int DATE = 1;
    private static final int ELEVATION = 2;
    private static final int TEMPERATURE_C = 3;
    private static final int HUMIDITY = 4;
    private static final int WIND_DIRECTION = 5;
    private static final int WIND_SPEED = 6;

    public static void main(String[] args) throws Exception {
        String maxTemperatureOutput = "output/maxTemperature";
        String minTemperatureOutput = "output/minTemperature";
        String maxHumidityOutput = "output/maxHumidity";
        String minHumidityOutput = "output/minHumidity";

        /* Max Temperature */
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "MaxTemperature");
        job1.setJarByClass(Calculation.class);
        job1.setMapperClass(TemperatureMapper.class);
        job1.setCombinerClass(FloatMaxReducer.class);
        job1.setReducerClass(FloatMaxReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(maxTemperatureOutput));

        job1.waitForCompletion(true);

        /* Min Temperature */
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "MinTemperature");
        job2.setJarByClass(Calculation.class);
        job2.setMapperClass(TemperatureMapper.class);
        job2.setCombinerClass(FloatMinReducer.class);
        job2.setReducerClass(FloatMinReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(minTemperatureOutput));

        job2.waitForCompletion(true);

        /* Max Humidity */
        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3, "MaxHumidity");
        job3.setJarByClass(Calculation.class);
        job3.setMapperClass(HumidityMapper.class);
        job3.setCombinerClass(FloatMaxReducer.class);
        job3.setReducerClass(FloatMaxReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job3, new Path(args[0]));
        FileOutputFormat.setOutputPath(job3, new Path(maxHumidityOutput));

        job3.waitForCompletion(true);

        /* Min Humidity */
        Configuration conf4 = new Configuration();
        Job job4 = Job.getInstance(conf4, "MinHumidity");
        job4.setJarByClass(Calculation.class);
        job4.setMapperClass(HumidityMapper.class);
        job4.setCombinerClass(FloatMinReducer.class);
        job4.setReducerClass(FloatMinReducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job4, new Path(args[0]));
        FileOutputFormat.setOutputPath(job4, new Path(minHumidityOutput));

        job4.waitForCompletion(true);
    }

    public static class TemperatureMapper
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
                if (!temperature.equals("M")) {
                    floatValue.set(Float.parseFloat(temperature));
                } else {
                    floatValue.set(Float.NaN);
                }
                context.write(stationMonth, floatValue);
            }
        }
    }

    public static class HumidityMapper
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
                String humidity = tokens[HUMIDITY];

                stationMonth.set(station + month);
                if (!humidity.equals("M")) {
                    floatValue.set(Float.parseFloat(humidity));
                } else {
                    floatValue.set(Float.NaN);
                }
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

    public static class FloatMinReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        private final FloatWritable result = new FloatWritable();

        public void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            float min = Float.MAX_VALUE;
            for (FloatWritable val : values) {
                min = Float.min(min, val.get());
            }
            result.set(min);
            context.write(key, result);
        }
    }
}