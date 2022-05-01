package mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Objects;

public class LocationReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text station, Iterable<Text> stats, Context context) {
        String stat = getString(stats);

        try {
            context.write(station, new Text(stat));
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Gets a string containing all monthly values of features of a particular station
     * @param stats feature value list
     * @return String containing all monthly values of features
     */
    private String getString(Iterable<Text> stats) {
        String[] monthlyStats = new String[12];
        for (Text text : stats
        ) {
            StringBuilder builder = new StringBuilder();
            String[] stat = text.toString()
                                .split(",");
            if (Objects.equals(stat[0], "")) {
                System.out.println("DEBUGGING");
            }
            Integer month = Integer.parseInt(stat[0]);
            for (int i = 1; i < stat.length; i++) {
                builder.append(stat[i]);
                if (i != stat.length - 1) {
                    builder.append(":");
                }
            }
            monthlyStats[month] = builder.toString();
        }
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < monthlyStats.length; i++) {
            builder.append(monthlyStats[i] == null ?
                           "M" :
                           monthlyStats[i]);
            if (i != monthlyStats.length - 1) {
                builder.append(",");
            }
        }

        return builder.toString();
    }
}
