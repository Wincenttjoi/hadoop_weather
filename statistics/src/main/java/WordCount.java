import com.google.common.collect.Iterables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.jetbrains.annotations.NotNull;
import utils.InputUtils;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

public class WordCount {
    private static final int STATION = 0;
    private static final int DATE = 1;
    private static final int ELEVATION = 2;
    private static final int TEMPERATURE_C = 3;
    private static final int HUMIDITY = 4;
    private static final int WIND_DIRECTION = 5;
    private static final int WIND_SPEED = 6;

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "max temp");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(FloatMaxReducer.class);
        job.setReducerClass(FloatMaxReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class FloatArrayWritable extends ArrayWritable {

        public FloatArrayWritable(FloatWritable[] floatWritables) {
            super(FloatWritable.class);
        }

        @Override
        public FloatWritable[] get() {
            return (FloatWritable[]) super.get();
        }

        @Override
        public void write(DataOutput arg0) throws IOException {
            for (FloatWritable data : get()) {
                data.write(arg0);
            }
        }
    }


    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        private final Text stationMonth = new Text();
        private final Text values = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//            String record = value.toString();
//            if (!record.equals("station,valid,elevation,tmpc,relh,drct,sped")) {
//                String[] tokens = record.split(",");
//                String station = tokens[STATION];
//                String date = tokens[DATE];
//
//                String month = InputUtils.getMonth(tokens[DATE]);
//                String elevation = tokens[ELEVATION];
//                String temperature = tokens[TEMPERATURE_C];
//                String humidity = tokens[HUMIDITY];
//                String direction = tokens[WIND_DIRECTION];
//                String speed = tokens[WIND_SPEED];
//
//                values.set("1,2,3,4");
//                stationMonth.set(station + month);
//                context.write(stationMonth, values);
//            }
            context.write(new Text("key"), new Text("val"));
        }
    }

    public static class FloatMaxReducer extends Reducer<Text, Text, Text, Text> {
        private final Text result = new Text();
//        private float[] floatValues = new float[16];

//        public void process(int index, float value) {
//            int maxIndex = index * 4, minIndex = maxIndex + 1, meanIndex = maxIndex + 2;
//            floatValues[maxIndex] = Float.max(floatValues[maxIndex], value);
//            floatValues[minIndex] = Float.min(floatValues[minIndex], value);
//            floatValues[meanIndex] += value;
//        }
//
//        public void getSTD(int index, float mean, float value) {
//            int stdIndex = index * 4 + 3;
//            floatValues[stdIndex] += Math.pow((mean - value), 2);
//        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            // max, min, mean
//            for (int i = 0; i < 4; i++) {
//                int maxIndex = i * 4, minIndex = maxIndex + 1, meanIndex = maxIndex + 2, stdIndex = maxIndex + 3;
//                // init array
//                floatValues[maxIndex] = Float.MIN_VALUE;
//                floatValues[minIndex] = Float.MAX_VALUE;
//                floatValues[meanIndex] = 0;
//                floatValues[stdIndex] = 0;
//            }
//
//            for (ArrayWritable val : values) {
//                for (int i = 0; i < 3; i++) {
//                    process(i, ((FloatWritable)val.get()[i]).get());
//                }
//            }
//
//            for (int i = 0; i < 4; i++) {
//                floatValues[i * 4 + 2] /= Iterables.size(values);
//            }
//
//            for (ArrayWritable val : values) {
//                for (int i = 0; i < 3; i++) {
//                    getSTD(i, floatValues[i * 4 + 2], ((FloatWritable)val.get()[i]).get());
//                }
//            }
//            FloatWritable[] writableValues = new FloatWritable[floatValues.length];
//
//            for (int i = 0; i < floatValues.length; i++) {
//                writableValues[i] = new FloatWritable(floatValues[i]);
//            }
//
//            for (int i = 0; i < 4; i++) {
//                int stdIndex = i * 4 + 3;
//                floatValues[stdIndex] /= Iterables.size(values);
//                floatValues[stdIndex] = (float) Math.sqrt(floatValues[stdIndex]);
//            }

//            result.set(writableValues);
            context.write(new Text("key"), new Text("val"));
        }
    }
}