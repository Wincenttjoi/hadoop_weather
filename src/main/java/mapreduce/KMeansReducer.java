package mapreduce;

import model.Centroid;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.stream.StreamSupport;

public class KMeansReducer extends Reducer<IntWritable, Centroid, Text, Text> {

    private final Text centroidId = new Text();
    private final Text centroidValue = new Text();

    public void reduce(IntWritable centroidPosition, Centroid centroid, Iterable<Centroid> points,
                       Context context) throws IOException, InterruptedException {

        Centroid average = average(centroid, points);

        centroidId.set(centroidPosition.toString());
        centroidValue.set(centroid.toString());
        context.write(centroidId, centroidValue);
    }

    // get average of features in centroid
    private Centroid average(Centroid centroid, Iterable<Centroid> points) {
        if (points == null || !points.iterator()
                                     .hasNext()) {
            System.out.println("NTH");
        }

        Map<String, Double> hashMap = centroid.getRelevantAttributes();

        StreamSupport.stream(points.spliterator(), false)
                     .flatMap(c -> c.getRelevantAttributes()
                                    .keySet()
                                    .stream())
                     .forEach(k -> hashMap.put(k, 0.0));

        for (Centroid point : points) {
            point.getRelevantAttributes()
                 .forEach((key, value) -> hashMap.compute(key, (key1, currentValue) -> value + currentValue));
        }

        hashMap.forEach((key, value) -> hashMap.put(key, value / (StreamSupport.stream(points.spliterator(), false)
                                                                               .count())));

        return new Centroid(hashMap);
    }
}
