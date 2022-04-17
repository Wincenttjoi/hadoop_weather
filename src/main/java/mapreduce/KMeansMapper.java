package mapreduce;

import model.Centroid;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.DistanceUtils;

import java.io.IOException;

public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Centroid> {

    private Centroid[] centroids;
    private double distance;
    private final IntWritable centroidPosition = new IntWritable();

    public void init(Context context) {
        int distance = Integer.parseInt(context.getConfiguration()
                                               .get("distance"));
        int k = Integer.parseInt(context.getConfiguration()
                                        .get("k"));

        this.centroids = new Centroid[k];
        for (int i = 0; i < k; i++) {
            String[] centroid = context.getConfiguration()
                                       .getStrings("centroid." + i);
            this.centroids[i] = new Centroid(centroid);
        }
    }

    public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {

        String[] pointString = val.toString()
                                  .split(",");

        Centroid point = new Centroid(pointString);


        double minDist = Double.MAX_VALUE;
        int position = -1;

        // find the nearest centroid
        for (int i = 0; i < centroids.length; i++) {
            distance = DistanceUtils.calculateDistance(point.getRelevantAttributes(), centroids[i].getRelevantAttributes());

            if (distance < minDist) {
                position = i;
                minDist = distance;
            }
        }

        centroidPosition.set(position);
        // nearest centroid position and the point features
        context.write(centroidPosition, point);
    }
}
