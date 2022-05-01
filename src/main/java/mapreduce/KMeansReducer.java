package mapreduce;

import model.Centroid;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.DistanceUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

public class KMeansReducer extends Reducer<IntWritable, Centroid, Text, Text> {

    private final Text centroidId = new Text();
    private final Text location = new Text();

    @Override
    public void reduce(IntWritable centroidPosition, Iterable<Centroid> points,
                       Context context) throws IOException, InterruptedException {

        System.out.println("hello: " + centroidPosition);
        String c = context.getConfiguration()
                                 .get("centroid." + centroidPosition.get());
        Centroid centroid = SerializationUtils.deserialize(Base64.getDecoder()
                                                          .decode(c));
        System.out.println("centroid: " + centroid.getAttributes());
//        System.out.println("centroid reduce : " + centroid.toString());
        List<String> places = new ArrayList<>();
        Centroid average = average(centroid, points, places);
        System.out.println("AVERAGE IS " + average.toString());
//        relocateCentroid(context, centroidPosition, average);


        StringBuilder sb = new StringBuilder();
        places.stream()
              .distinct()
              .forEach(place -> sb.append(place)
                                  .append(","));
//        System.out.println("average reduce centroid: " + average.toString());

        centroidId.set(centroidPosition.toString());

        System.out.println(sb.toString());

        context.write(centroidId, new Text(Base64.getEncoder().encodeToString(SerializationUtils.serialize(average))));
        context.write(new Text("places"), new Text(sb.toString()));
    }

    /**
     * Gets relocated centroid based on data assigned to it
     * @param centroid previous centroid
     * @param points points assigned to this centroid
     * @param places station names assigned to this centroid
     * @return relocated centroid
     */
    private Centroid average(Centroid centroid, Iterable<Centroid> points, List<String> places) {
        if (points == null || !points.iterator()
                                     .hasNext()) {
            System.out.println("NTH");
        }


        Map<String, String> hashMap = centroid.getAttributes();

        int count = 1;

        for (Centroid point : points) {
            places.add(point.getAttributes()
                            .get("station"));
            point.getAttributes()
                 .forEach((key, value) -> {
                     if (!key.equals("station")) {
                         hashMap.compute(key,
                                         (key1, currentValue) -> DistanceUtils.calculateTotal(value, currentValue));
                     }
                 });
            count++;
        }

        int finalCount = count;
        hashMap.forEach((key, value) ->  hashMap.compute(key, (key1, value1) ->  DistanceUtils.calculateAverage(value1, finalCount)) );

        return new Centroid(hashMap);
    }
}
