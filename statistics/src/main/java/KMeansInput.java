import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KMeansInput {
    private static final int STATION = 0;
    private static final int DATE = 1;
    private static final int ELEVATION = 2;
    private static final int TEMPERATURE_C = 3;
    private static final int HUMIDITY = 4;
    private static final int WIND_DIRECTION = 5;
    private static final int WIND_SPEED = 6;

    /**
     * Map record into <station_month, values>.
     * For example, record: ZUUU,2012-01-01 07:00,508.00,7.00,81.20,M,2.24 will be mapped into
     * <"ZUUU,2012-01-01", "ZUUU,2012-01-01 07:00,508.00,7.00,81.20,M,2.24">.
     */
    public static class IdentityMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String record = value.toString();
            if (!record.equals("station,valid,elevation,tmpc,relh,drct,sped")) {
                String stationMonth = record.substring(0, 12);
                context.write(new Text(stationMonth), new Text(value));
            }
        }
    }

    public static class StatsReducer extends Reducer<Text, Text, Text, Text> {

        public enum Attribute {
            Elevation,
            MaxTemp, MinTemp, MeanTemp, StdTemp,
            MaxHum, MinHum, MeanHum, StdHum,
            Dir1, Dir2, Dir3,
            MaxSpeed, MinSpeed, MeanSpeed, StdSpeed
        }

        private float[] attributes = new float[16];
        private HashMap<InputUtils.WindDirection, Integer> windDirectionCounter = new HashMap<>();

        private void processElevation(float value) {
            attributes[Attribute.Elevation.ordinal()] = value;
        }

        private void processTemperature(float value) {
            attributes[Attribute.MaxTemp.ordinal()] = Float.max(attributes[Attribute.MaxTemp.ordinal()], value);
            attributes[Attribute.MinTemp.ordinal()] = Float.min(attributes[Attribute.MinTemp.ordinal()], value);
            attributes[Attribute.MeanTemp.ordinal()] += value;
        }

        private void processHumidity(float value) {
            attributes[Attribute.MaxHum.ordinal()] = Float.max(attributes[Attribute.MaxHum.ordinal()], value);
            attributes[Attribute.MinHum.ordinal()] = Float.min(attributes[Attribute.MinHum.ordinal()], value);
            attributes[Attribute.MeanHum.ordinal()] += value;
        }

        private void processDirection(float value) {
            InputUtils.WindDirection windDirection = InputUtils.getWindDirection(value);
            windDirectionCounter.put(windDirection, windDirectionCounter.getOrDefault(windDirection, 0) + 1);
        }

        private void processSpeed(float value) {
            attributes[Attribute.MaxSpeed.ordinal()] = Float.max(attributes[Attribute.MaxSpeed.ordinal()], value);
            attributes[Attribute.MinSpeed.ordinal()] = Float.min(attributes[Attribute.MinSpeed.ordinal()], value);
            attributes[Attribute.MeanSpeed.ordinal()] += value;
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
                String[] tokens = val.toString().split(",");
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

        public void calculateTopThree() {
            Map<InputUtils.WindDirection, Integer> topThree = windDirectionCounter
                    .entrySet().stream()
                    .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                    .limit(3)
                    .collect(Collectors.toMap(Map.Entry::getKey,
                            Map.Entry::getValue,
                            (e1, e2) -> e1, LinkedHashMap::new));
            int i = Attribute.Dir1.ordinal();
            for (InputUtils.WindDirection dir : topThree.keySet()) {
                attributes[i] = (float) dir.ordinal();
                i += 1;
            }
        }

        private void initAttributes() {
            attributes[Attribute.MaxTemp.ordinal()] = Float.MIN_VALUE;
            attributes[Attribute.MaxSpeed.ordinal()] = Float.MIN_VALUE;
            attributes[Attribute.MaxHum.ordinal()] = Float.MIN_VALUE;

            attributes[Attribute.MinTemp.ordinal()] = Float.MAX_VALUE;
            attributes[Attribute.MinSpeed.ordinal()] = Float.MAX_VALUE;
            attributes[Attribute.MinHum.ordinal()] = Float.MAX_VALUE;

            attributes[Attribute.MeanTemp.ordinal()] = 0;
            attributes[Attribute.MeanSpeed.ordinal()] = 0;
            attributes[Attribute.MeanHum.ordinal()] = 0;

            attributes[Attribute.StdTemp.ordinal()] = 0;
            attributes[Attribute.StdSpeed.ordinal()] = 0;
            attributes[Attribute.StdHum.ordinal()] = 0;
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            initAttributes();
            int n = firstProcess(values);
            calculateMean(n);
            secondProcess(values);
            calculateStd(n);
            calculateTopThree();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < attributes.length; i++) {
                sb.append(attributes[i]);
                if (i != attributes.length - 1) {
                    sb.append(",");
                }
            }
            context.write(key, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "stats");
        job.setJarByClass(KMeansInput.class);
        job.setMapperClass(IdentityMapper.class);
//        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(StatsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}