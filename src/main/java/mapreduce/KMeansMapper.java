package mapreduce;

import model.Centroid;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.DistanceUtils;

import java.io.IOException;
import java.util.*;

/**
 * Maps record into <station, monthlyValues>
 */
public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Centroid> {

    private Centroid[] centroids;

    /**
     * Initializes the centroids to be used for map reduce function
     * @param context context
     */
    public void setup(Context context) {
        int k = Integer.parseInt(context.getConfiguration()
                                        .get("k"));

        this.centroids = new Centroid[k];
        for (int i = 0; i < k; i++) {
            String centroid = context.getConfiguration()
                                       .get("centroid." + i);
            Centroid c = SerializationUtils.deserialize(Base64.getDecoder()
                                                              .decode(centroid));

            this.centroids[i] = c;
            System.out.println("mapper centroid: "+ c.toString());
        }
    }

    public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {

        if (!val.toString()
                .startsWith("station")) {
            String[] pointString = val.toString()
                                      .split("\t");

            Centroid point = new Centroid(getRelevant(pointString));



            double minDist = Double.MAX_VALUE;
            int position = -1;

            // find the nearest centroid
            for (int i = 0; i < centroids.length; i++) {
                double distance = DistanceUtils.calculateDistance(point.getAttributes(),
                                                           centroids[i].getAttributes());

                if (distance < minDist) {
                    position = i;
                    minDist = distance;
                }
            }

            point.getAttributes()
                 .put("station", pointString[0].split(",")[0]);
            // nearest centroid position and the point features
            context.write(new IntWritable(position), point);
        }


    }

    /**
     * Get the relevant features to be input to k means
     * @param attributes feature list
     * @return HashMap containing the feature and its values
     */
    private Map<String, String> getRelevant(String[] attributes) {
        Map<String, String> hashMap = new HashMap<>();
        String[] features = attributes[1].split(",");
        List<String> temperature = new ArrayList<>();
        List<String> humidity = new ArrayList<>();
        for (String feature : features
        ) {
            if (!feature.equals("M")) {
                String temp = feature.split(":")[0];
                String humid = feature.split(":")[1];
                temperature.add(temp);
                humidity.add(humid);
            } else {
                temperature.add("M");
                humidity.add("M");
            }
        }

        hashMap.put("temperature", String.join(",", temperature));
        hashMap.put("humidity", String.join(",", humidity));

        return hashMap;
    }
}
