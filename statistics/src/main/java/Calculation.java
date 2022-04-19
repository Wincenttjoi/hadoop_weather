import org.apache.avro.generic.GenericData;
import org.apache.commons.collections.IteratorUtils;
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

import javax.lang.model.element.Element;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
        String meanTemperatureOutput = "output/meanTemperature";
        String meanHumidityOutput = "output/meanHumidity";
        String stdTemperatureOutput = "output/stdTemperature";
        String stdHumidityOutput = "output/stdHumidity";

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

        /* Mean Temperature */
        Configuration conf5 = new Configuration();
        Job job5 = Job.getInstance(conf5, "MeanTemperature");
        job5.setJarByClass(Calculation.class);
        job5.setMapperClass(TemperatureMapper.class);
        job5.setCombinerClass(FloatMeanReducer.class);
        job5.setReducerClass(FloatMeanReducer.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job5, new Path(args[0]));
        FileOutputFormat.setOutputPath(job5, new Path(meanTemperatureOutput));

        job5.waitForCompletion(true);

        /* Mean Humidity */
        Configuration conf6 = new Configuration();
        Job job6 = Job.getInstance(conf6, "MeanHumidity");
        job6.setJarByClass(Calculation.class);
        job6.setMapperClass(HumidityMapper.class);
        job6.setCombinerClass(FloatMeanReducer.class);
        job6.setReducerClass(FloatMeanReducer.class);
        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job6, new Path(args[0]));
        FileOutputFormat.setOutputPath(job6, new Path(meanHumidityOutput));

        job6.waitForCompletion(true);

        /* STD Temperature */
        Configuration conf7 = new Configuration();
        Job job7 = Job.getInstance(conf7, "StdTemperature");
        job7.setJarByClass(Calculation.class);
        job7.setMapperClass(TemperatureMapper.class);
        job7.setCombinerClass(FloatStdReducer.class);
        job7.setReducerClass(FloatStdReducer.class);
        job7.setOutputKeyClass(Text.class);
        job7.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job7, new Path(args[0]));
        FileOutputFormat.setOutputPath(job7, new Path(stdTemperatureOutput));

        job7.waitForCompletion(true);

        /* STD Humidity */
        Configuration conf8 = new Configuration();
        Job job8 = Job.getInstance(conf8, "StdHumidity");
        job8.setJarByClass(Calculation.class);
        job8.setMapperClass(HumidityMapper.class);
        job8.setCombinerClass(FloatStdReducer.class);
        job8.setReducerClass(FloatStdReducer.class);
        job8.setOutputKeyClass(Text.class);
        job8.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job8, new Path(args[0]));
        FileOutputFormat.setOutputPath(job8, new Path(stdHumidityOutput));

        job8.waitForCompletion(true);
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

    public static class FloatMeanReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        private final FloatWritable result = new FloatWritable();

        public void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            float sum = 0;
            float count = 0;
            for (FloatWritable val : values) {
                sum += val.get();
                count += 1;
            }
            result.set(sum / count);
            context.write(key, result);
        }
    }

    public static class FloatStdReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        private final FloatWritable result = new FloatWritable();

        public void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {

            ArrayList<Float> arrayValues = new ArrayList<>();
            for (FloatWritable val : values) {
                arrayValues.add(val.get());
            }

            float sum = 0;
            for (float val : arrayValues) {
                sum += val;
            }
            float mean = sum / arrayValues.size();

            float squareSum = 0;
            for (float val : arrayValues) {
                squareSum += Math.pow((val - mean), 2);
            }

            float std = (float) Math.sqrt(squareSum / arrayValues.size());

            result.set(std);
            context.write(key, result);
        }
    }
}