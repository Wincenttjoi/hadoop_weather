package mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Objects;

public class LocationReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text station, Iterable<Text> stats, Context context) {
        String stat = getString(stats);
//        System.out.println("station reducer is " + station.toString());
//        System.out.println("stat reducer is " + stat);

        try {
            context.write(station, new Text(stat));
        } catch (IOException e) {
            System.out.println("IO EXCEPTION");
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            System.out.println("INTERRUPTED EXCEPTION");
            throw new RuntimeException(e);
        }
    }

    private String getString(Iterable<Text> stats) {
        String[] monthlyStats = new String[12];
        for (Text text : stats
        ) {
            if (text.toString()
                    .isEmpty()) {
                continue;
            }
            String[] stat = text.toString()
                                .split(",");
            if (Objects.equals(stat[0], "")) {
                System.out.println("DEBUGGING");
            }
            Integer month = Integer.parseInt(stat[0]);
            StringBuilder builder = new StringBuilder();
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
