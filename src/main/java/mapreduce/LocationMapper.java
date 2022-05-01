package mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class LocationMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void setup(Context context) {

    }


    public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
        String record = val.toString();
        String stats = "";
        String year = "";

        if (!record.startsWith("station")) {
            try {
                stats = getStats(record);
//                year = getYear(record);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
            context.write(new Text(record.split("\t")[0].split(",")[0]), new Text(stats));
        }

    }

    private String getStats(String record) throws ParseException {
        String[] stats = record.split("\t");
        StringBuilder builder = new StringBuilder();
//        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM");
//        Calendar cal = Calendar.getInstance();
//        cal.setTime(df.parse(stats[1].split(" ")[0]));
//        String month = String.valueOf(cal.get(Calendar.MONTH));
        String month = stats[0].split(",")[1];

        builder.append(Integer.parseInt(month) - 1)
               .append(",")
               .append(stats[1].split(",")[3])
               .append(",")
               .append(stats[1].split(",")[7]);

        return builder.toString();
    }

    private String getYear(String record) throws ParseException {
        String[] stats = record.split(",");
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM");
        Calendar cal = Calendar.getInstance();
        cal.setTime(df.parse(stats[1].split(" ")[0]));
        return String.valueOf(cal.get(Calendar.YEAR));
    }
}
