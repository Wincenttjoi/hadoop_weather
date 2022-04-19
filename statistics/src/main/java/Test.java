import java.io.IOException;
import java.util.Arrays;
import java.util.StringJoiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import utils.InputUtils;

public class Test {
    private static final int STATION = 0;
    private static final int DATE = 1;
    private static final int ELEVATION = 2;
    private static final int TEMPERATURE_C = 3;
    private static final int HUMIDITY = 4;
    private static final int WIND_DIRECTION = 5;
    private static final int WIND_SPEED = 6;

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        private final Text values = new Text();
        private final Text stationMonth = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String record = value.toString();
            if (!record.equals("station,valid,elevation,tmpc,relh,drct,sped")) {
                String[] tokens = record.split(",");
                String station = tokens[STATION];
                String date = tokens[DATE];
                String month = InputUtils.getMonth(date);
                stationMonth.set(station + month);
                context.write(stationMonth, value);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

        public enum Attribute {
            Elevation,
            MaxTemp, MinTemp, MeanTemp, StdTemp,
            MaxHum, MinHum, MeanHum, StdHum,
            Dir1, Dir2, Dir3,
            MaxSpeed, MinSpeed, MeanSpeed, StdSpeed
        }

        private final float[] attributes = new float[16];

        private void processElevation(float value) {
            attributes[Attribute.Elevation.ordinal()] = value;
        }

        private void processTemperature(float value) {
            attributes[Attribute.MaxTemp.ordinal()] = Float.max(attributes[Attribute.MaxTemp.ordinal()], value);
            attributes[Attribute.MinTemp.ordinal()] = Float.max(attributes[Attribute.MinTemp.ordinal()], value);
            attributes[Attribute.MeanTemp.ordinal()] = Float.max(attributes[Attribute.MeanTemp.ordinal()], value);
        }

        private void processHumidity(float value) {
            attributes[Attribute.MaxHum.ordinal()] = Float.max(attributes[Attribute.MaxHum.ordinal()], value);
            attributes[Attribute.MinHum.ordinal()] = Float.max(attributes[Attribute.MinHum.ordinal()], value);
            attributes[Attribute.MeanHum.ordinal()] = Float.max(attributes[Attribute.MeanHum.ordinal()], value);
        }

        private void processDirection(float value) {
            // TODO: implement
        }

        private void processSpeed(float value) {
            attributes[Attribute.MaxSpeed.ordinal()] = Float.max(attributes[Attribute.MaxSpeed.ordinal()], value);
            attributes[Attribute.MinSpeed.ordinal()] = Float.max(attributes[Attribute.MinSpeed.ordinal()], value);
            attributes[Attribute.MeanSpeed.ordinal()] = Float.max(attributes[Attribute.MeanSpeed.ordinal()], value);
        }

        private void calculateTempMean(int n) {
            attributes[Attribute.MeanTemp.ordinal()] /= n;
        }

        private void calculateHumMean(int n) {
            attributes[Attribute.MeanHum.ordinal()] /= n;
        }

        private void calculateSpeedMean(int n) {
            attributes[Attribute.MeanSpeed.ordinal()] /= n;
        }

        private void accTempStd(float value) {
            attributes[Attribute.StdTemp.ordinal()] += Math.pow((attributes[Attribute.MeanTemp.ordinal()] - value), 2);
        }

        private void accHumStd(float value) {
            attributes[Attribute.StdHum.ordinal()] += Math.pow((attributes[Attribute.MeanHum.ordinal()] - value), 2);
        }

        private void accSpeedStd(float value) {
            attributes[Attribute.StdSpeed.ordinal()] += Math.pow((attributes[Attribute.MeanSpeed.ordinal()] - value), 2);
        }

        private void calculateTempStd(int n) {
            attributes[Attribute.StdTemp.ordinal()] /= n;
            attributes[Attribute.StdTemp.ordinal()] = (float) Math.sqrt(attributes[Attribute.StdTemp.ordinal()]);
        }

        private void calculateHumStd(int n) {
            attributes[Attribute.StdHum.ordinal()] /= n;
            attributes[Attribute.StdHum.ordinal()] = (float) Math.sqrt(attributes[Attribute.StdHum.ordinal()]);
        }

        private void calculateSpeedStd(int n) {
            attributes[Attribute.StdSpeed.ordinal()] /= n;
            attributes[Attribute.StdSpeed.ordinal()] = (float) Math.sqrt(attributes[Attribute.StdSpeed.ordinal()]);
        }

        public int firstProcess(Iterable<Text> values) {
            int n = 0;
            for (Text val : values) {
                String[] tokens = Arrays.toString(val.getBytes()).split(",");
                if (Arrays.asList(tokens).contains("M")) {
                    continue;
                }

                float elevation = Float.parseFloat(tokens[ELEVATION]);
                float temperature = Float.parseFloat(tokens[TEMPERATURE_C]);
                float humidity = Float.parseFloat(tokens[HUMIDITY]);
                float direction = Float.parseFloat(tokens[WIND_DIRECTION]);
                float speed = Float.parseFloat(tokens[WIND_SPEED]);

                processElevation(elevation);
                processTemperature(temperature);
                processHumidity(humidity);
                processDirection(direction);
                processSpeed(speed);

                n += 1;
            }
            return n;
        }
        public void calculateMean(int n) {
            calculateTempMean(n);
            calculateHumMean(n);
            calculateSpeedMean(n);
        }
        public void secondProcess(Iterable<Text> values) {
            for (Text val : values) {
                String[] tokens = Arrays.toString(val.getBytes()).split(",");
                if (Arrays.asList(tokens).contains("M")) {
                    continue;
                }

                float temperature = Float.parseFloat(tokens[TEMPERATURE_C]);
                float humidity = Float.parseFloat(tokens[HUMIDITY]);
                float speed = Float.parseFloat(tokens[WIND_SPEED]);

                accTempStd(temperature);
                accHumStd(humidity);
                accSpeedStd(speed);
            }
        }
        public void calculateStd(int n) {
            calculateTempStd(n);
            calculateHumStd(n);
            calculateSpeedStd(n);
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int n = firstProcess(values);
            calculateMean(n);
            secondProcess(values);
            calculateStd(n);
            StringBuilder sb = new StringBuilder();
            for (float attribute : attributes) {
                sb.append(attribute).append(",");
            }
            context.write(new Text("key"), new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}