package mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;

/**
 * Map record into <station, values>
 */
public class LocationMapper extends Mapper<LongWritable, Text, Text, Text> {

    /**
     * @param key key
     * @param val feature values of data
     * @param context context
     * @throws IOException IOException
     * @throws InterruptedException InterruptedException
     */
    public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
        String record = val.toString();
        String stats;

        if (!record.startsWith("station")) {
            try {
                stats = getStats(record);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
            context.write(new Text(record.split("\t")[0].split(",")[0]), new Text(stats));
        }

    }

    /**
     * Gets the formatted feature values
     * @param record Feature string
     * @return formatted feature values
     * @throws ParseException ParseException
     */
    private String getStats(String record) throws ParseException {
        String[] stats = record.split("\t");
        StringBuilder builder = new StringBuilder();
        String month = stats[0].split(",")[1];

        builder.append(Integer.parseInt(month) - 1)
               .append(",")
               .append(stats[1].split(",")[3])
               .append(",")
               .append(stats[1].split(",")[7]);

        return builder.toString();
    }
}
