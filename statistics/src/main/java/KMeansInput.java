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
import org.apache.hadoop.util.GenericOptionsParser;

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
     * Sample: BOS,2012-01	9.0,15.0,-14.4,1.3710173,0.0,100.0,27.77,65.43557,0.0,6.0,7.0,0.0,32.2,0.0,11.287558,0.0
     *
     * Output: <location-month> <...>
     */
    public static class IdentityMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String record = value.toString();
            String[] tokens = record.split("\t");

            String stationYearMonth = tokens[0];
            String station = stationYearMonth.split(",")[0];
            String month = stationYearMonth.split("-")[1];
            String stationMonth = station + "," + month;
            context.write(new Text(stationMonth), new Text(tokens[1]));
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
        // month: <direction, count>
        private HashMap<Integer, HashMap<InputUtils.WindDirection, Integer>> windDirectionCounter = new HashMap<>();

        public int firstProcess(Iterable<Text> values) {
            int n = 0;
            for (Text val : values) {
                String[] tokens = val.toString().split(",");
                for (int i = 0; i < tokens.length; i++) {

                    if (i == 0) {
                        attributes[i] = Float.parseFloat(tokens[i]);
                        continue;
                    }

                    if (i == Attribute.Dir1.ordinal() || i == Attribute.Dir2.ordinal() || i == Attribute.Dir3.ordinal()) {
                        attributes[i] = Float.parseFloat(tokens[i]);
                        continue;
                        // TODO: calculate
                    }

                    attributes[i] += Float.parseFloat(tokens[i]);
                }
                n += 1;
            }
            return n;
        }

        public void calculateMean(int n) {
            for (int i = 1; i < attributes.length; i++) {
                if (i == Attribute.Dir1.ordinal() || i == Attribute.Dir2.ordinal() || i == Attribute.Dir3.ordinal()) {
                    continue;
                    // TODO: calculate
                }
                attributes[i] /= n;
            }
        }

        public void secondProcess(Iterable<Text> values) {

        }



        private void initAttributes() {
            attributes[Attribute.MaxTemp.ordinal()] = 0;
            attributes[Attribute.MaxSpeed.ordinal()] = 0;
            attributes[Attribute.MaxHum.ordinal()] = 0;

            attributes[Attribute.MinTemp.ordinal()] = 0;
            attributes[Attribute.MinSpeed.ordinal()] = 0;
            attributes[Attribute.MinHum.ordinal()] = 0;

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
//        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//        if (otherArgs.length < 2) {
//            System.err.println("Usage: kmeans-input <in> [<in>...] <out>");
//            System.exit(2);
//        }
//        // set up configuration
//        conf.set("key", "value");

        Job job = Job.getInstance(conf, "k-means input");
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