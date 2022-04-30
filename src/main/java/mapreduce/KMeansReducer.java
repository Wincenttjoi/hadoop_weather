package mapreduce;

import model.Centroid;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KMeansReducer extends Reducer<IntWritable, Centroid, Text, Text> {

    private final Text centroidId = new Text();
    private final Text location = new Text();

    public void reduce(IntWritable centroidPosition, Iterable<Centroid> points,
                       Context context) throws IOException, InterruptedException {

        System.out.println("hello: " + centroidPosition);
        Centroid centroid = new Centroid(context.getConfiguration()
                                                .getStrings("centroid." + centroidPosition));
//        System.out.println("centroid reduce : " + centroid.toString());
        List<String> places = new ArrayList<>();
        Centroid average = average(centroid, points, places);
//        relocateCentroid(context, centroidPosition, average);


        StringBuilder sb = new StringBuilder();
        places.stream()
              .distinct()
              .forEach(place -> sb.append(place)
                                  .append(","));
//        System.out.println("average reduce centroid: " + average.toString());

        centroidId.set(centroidPosition.toString());

        System.out.println(sb.toString());

        context.write(centroidId, new Text(average.toString()));
        context.write(new Text("places"), new Text(sb.toString()));
    }

    // get average of features in centroid
    private Centroid average(Centroid centroid, Iterable<Centroid> points, List<String> places) {
        if (points == null || !points.iterator()
                                     .hasNext()) {
            System.out.println("NTH");
        }


        Map<String, String> hashMap = centroid.getRelevantAttributes();
        hashMap.put("temperature", "0.0");
        hashMap.put("humidity", "0.0");


//        StreamSupport.stream(points.spliterator(), false)
//                     .flatMap(c -> c.getRelevantAttributes()
//                                    .keySet()
//                                    .stream())
//                     .forEach(k -> hashMap.put(k, 0.0));
        int count = 0;

        for (Centroid point : points) {
            places.add(point.getAttributes()
                            .get("station"));
            point.getRelevantAttributes()
                 .forEach((key, value) -> {
//                     point.getAttributes()
//                          .forEach((k, v) -> {
//                              System.out.println("key is " + k + " value is " + v);
//                          });
                     hashMap.compute(key, (key1, currentValue) -> String.valueOf(Double.parseDouble(value) + Double.parseDouble(currentValue)));
                 });

            count++;
        }

        int finalCount = count;
        hashMap.forEach((key, value) -> hashMap.put(key, String.valueOf(Double.parseDouble(value) / finalCount)));

//        hashMap.forEach((k, v) -> System.out.println("key is " + k + " value is : " + v));

        return new Centroid(hashMap);
    }

    private void relocateCentroid(Context context, IntWritable centroidPosition, Centroid centroid) {
        context.getConfiguration()
               .unset("centroid." + centroidPosition.get());
        System.out.println("average centroid: " + centroid.toString());
        context.getConfiguration().set("centroid." + centroidPosition.get(), centroid.toString());
    }
}
