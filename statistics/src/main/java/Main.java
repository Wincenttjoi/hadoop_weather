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

public class Main {
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
                if (stationMonth.endsWith("-")) {
                    stationMonth = stationMonth.substring(0, 11);
                }
                context.write(new Text(stationMonth), new Text(value));
            }
        }
    }

    public static class StatsReducer extends Reducer<Text, Text, Text, Text> {

        /**
         * enum for the statistics to be calculated
         */
        public enum Attribute {
            Elevation,
            MaxTemp, MinTemp, MeanTemp, StdTemp,
            MaxHum, MinHum, MeanHum, StdHum,
            Dir1, Dir2, Dir3,
            MaxSpeed, MinSpeed, MeanSpeed, StdSpeed
        }

        private float[] attributes = new float[16];
        private HashMap<InputUtils.WindDirection, Integer> windDirectionCounter = new HashMap<>();

        /**
         * Set elevation in the attributes array
         * Note that the elevation for a station is a constant. Therefore no additional calculation is required
         * @param value elevation
         */
        private void processElevation(float value) {
            attributes[Attribute.Elevation.ordinal()] = value;
        }

        /**
         * Process temperature by updating the current maxTemp, minTemp, and meanTemp (sum of all temp at this stage)
         * @param value temperature
         */
        private void processTemperature(float value) {
            attributes[Attribute.MaxTemp.ordinal()] = Float.max(attributes[Attribute.MaxTemp.ordinal()], value);
            attributes[Attribute.MinTemp.ordinal()] = Float.min(attributes[Attribute.MinTemp.ordinal()], value);
            attributes[Attribute.MeanTemp.ordinal()] += value;
        }

        /**
         * Process humidity by updating the current maxHum, minHum, and meanHum (sum of all hum at this stage)
         * @param value humidity
         */
        private void processHumidity(float value) {
            attributes[Attribute.MaxHum.ordinal()] = Float.max(attributes[Attribute.MaxHum.ordinal()], value);
            attributes[Attribute.MinHum.ordinal()] = Float.min(attributes[Attribute.MinHum.ordinal()], value);
            attributes[Attribute.MeanHum.ordinal()] += value;
        }

        /**
         * Process direction by calling the util method in InputUtils and update the direction counter
         * @param value wind direction in degrees
         */
        private void processDirection(float value) {
            InputUtils.WindDirection windDirection = InputUtils.getWindDirection(value);
            windDirectionCounter.put(windDirection, windDirectionCounter.getOrDefault(windDirection, 0) + 1);
        }

        /**
         * Process wind speed by updating the current maxSpeed, minSpeed, and meanSpeed (sum of all speed at this stage)
         * @param value wind speed
         */
        private void processSpeed(float value) {
            attributes[Attribute.MaxSpeed.ordinal()] = Float.max(attributes[Attribute.MaxSpeed.ordinal()], value);
            attributes[Attribute.MinSpeed.ordinal()] = Float.min(attributes[Attribute.MinSpeed.ordinal()], value);
            attributes[Attribute.MeanSpeed.ordinal()] += value;
        }

        /**
         * Get mean temperature by dividing the sum by n, which is the number of records
         * @param n number of records
         */
        private void calculateTempMean(int n) {
            attributes[Attribute.MeanTemp.ordinal()] /= n;
        }

        /**
         * Get mean humidity by divdiing the sum by n, which is the number of records
         * @param n number of records
         */
        private void calculateHumMean(int n) {
            attributes[Attribute.MeanHum.ordinal()] /= n;
        }

        /**
         * Get mean wind speed by dividing the sum by n, which is the number of records
         * @param n number of records
         */
        private void calculateSpeedMean(int n) {
            attributes[Attribute.MeanSpeed.ordinal()] /= n;
        }

        /**
         * Accumulate the square of the difference between the temperature of the current record and the mean temperature
         * to prepare for the calculation of standard deviation
         * @param value temperature
         */
        private void accTempStd(float value) {
            attributes[Attribute.StdTemp.ordinal()] += Math.pow((attributes[Attribute.MeanTemp.ordinal()] - value), 2);
        }

        /**
         * Accumulate the square of the difference between the humidity of the current record and the mean humidity
         * to prepare for the calculation of standard deviation
         * @param value humidity
         */
        private void accHumStd(float value) {
            attributes[Attribute.StdHum.ordinal()] += Math.pow((attributes[Attribute.MeanHum.ordinal()] - value), 2);
        }

        /**
         * Accumulate the square of the difference between the speed of the current record and the mean speed
         * to prepare for the calculation of standard deviation
         * @param value speed
         */
        private void accSpeedStd(float value) {
            attributes[Attribute.StdSpeed.ordinal()] += Math.pow((attributes[Attribute.MeanSpeed.ordinal()] - value), 2);
        }

        /**
         * Calculate standard deviation of temperature
         * @param n number of records
         */
        private void calculateTempStd(int n) {
            attributes[Attribute.StdTemp.ordinal()] /= n;
            attributes[Attribute.StdTemp.ordinal()] = (float) Math.sqrt(attributes[Attribute.StdTemp.ordinal()]);
        }

        /**
         * Calculate standard deviation of humidity
         * @param n number of records
         */
        private void calculateHumStd(int n) {
            attributes[Attribute.StdHum.ordinal()] /= n;
            attributes[Attribute.StdHum.ordinal()] = (float) Math.sqrt(attributes[Attribute.StdHum.ordinal()]);
        }

        /**
         * Calculate standard deviation of wind speed
         * @param n number of records
         */
        private void calculateSpeedStd(int n) {
            attributes[Attribute.StdSpeed.ordinal()] /= n;
            attributes[Attribute.StdSpeed.ordinal()] = (float) Math.sqrt(attributes[Attribute.StdSpeed.ordinal()]);
        }

        /**
         * First pass of statistics calculation to calculate max, min, and sum of temperature, humidity and speed
         * Set elevation, parse and count wind direction
         * @param values value containing raw data
         * @return an array list containing the values processed
         */
        public ArrayList<Text> firstProcess(Iterable<Text> values) {
            ArrayList<Text> valuesCopy = new ArrayList<>();
            for (Text val : values) {
                valuesCopy.add(val);
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

            }
            return valuesCopy;
        }

        /**
         * Calculate mean values
         * @param n number of records
         */
        public void calculateMean(int n) {
            calculateTempMean(n);
            calculateHumMean(n);
            calculateSpeedMean(n);
        }

        /**
         * Second pass of the statistics calculation to accumulate standard deviation after getting mean values
         * of temperature, humidity and speed
         * @param values values
         */
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

        /**
         * Calculate standard deviation
         * @param n number of records
         */
        public void calculateStd(int n) {
            calculateTempStd(n);
            calculateHumStd(n);
            calculateSpeedStd(n);
        }

        /**
         * Calculate the top 3 most common wind speed by sorting the windDirectionCounter and obtaining the top 3
         */
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

        /**
         * Initialise attributes to be either 0 or extreme values for maximum and minimum calculation
         */
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

        /**
         * Redude
         * @param key key of the hadoop <key-value> pair
         * @param values value of the hadoop <key-value> pair
         * @param context context
         * @throws IOException IOException
         * @throws InterruptedException InterruptException
         */
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            initAttributes();
            ArrayList<Text> valuesCopy = firstProcess(values);
            int n = valuesCopy.size();
            calculateMean(n);
            secondProcess(valuesCopy);
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

    /**
     * Configure the hadoop job and run
     * @param args args for input and output path
     * @throws Exception Exception
     */
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