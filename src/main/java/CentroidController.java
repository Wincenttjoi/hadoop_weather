import model.Centroid;
import model.Statistics;
import utils.DistanceUtils;
import utils.FileUtils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

public class CentroidController {

    List<Statistics> list;

    public CentroidController(String fileName) {
        try {
            FileUtils fileUtils = new FileUtils(fileName);
            this.list = fileUtils.getBeans();
        } catch (URISyntaxException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    // find the nearest centroid
    public Centroid getNearestCentroid(Centroid point, List<Centroid> centroids) {
        double distance = Double.MAX_VALUE;
        Centroid nearestCentroid = null;
        for (Centroid centroid : centroids
        ) {
            double tempDistance = DistanceUtils.calculateDistance(point.getAttributes(), centroid.getAttributes());

            if (tempDistance < distance) {
                distance = tempDistance;
                nearestCentroid = centroid;
            }
        }

        return nearestCentroid;
    }

    // returns a list of k random centroids
    public List<Centroid> generateInitialCentroids(int k) throws NoSuchFieldException {
        List<Centroid> centroids = new ArrayList<>();
        Map<String, Double> maxList = new HashMap<>();
        Map<String, Double> minList = new HashMap<>();

        Random random = new Random();

        // get min max values to generate a centroid
        getMaxMinValue(maxList, minList);

        for (int i = 0; i < k ; i++) {
            Map<String, Double> centroid = new HashMap<>();
            for (String attribute : maxList.keySet()
            ) {
                double max = maxList.get(attribute);
                double min = minList.get(attribute);
                centroid.put(attribute, random.nextDouble() * (max - min) + min);
            }
            centroids.add(new Centroid(centroid));
        }
        return centroids;
    }

    private void getMaxMinValue(Map<String, Double> maxList, Map<String, Double> minList) throws NoSuchFieldException {
        for (Statistics stats : list) {
            Arrays.stream(stats.getRelevantFields())
                  .forEach((key) -> {
                      maxList.compute(key.getName(), (key1, max) -> {
                          try {
                              return max == null || Double.parseDouble(key.get(stats)
                                                                          .toString()) > max ?
                                     Double.parseDouble(key.get(stats)
                                                           .toString()) :
                                     max;
                          } catch (IllegalAccessException e) {
                              throw new RuntimeException(e);
                          }
                      });

                      minList.compute(key.getName(), (key1, min) -> {
                          try {
                              return min == null || Double.parseDouble(key.get(stats)
                                                                          .toString()) < min ?
                                     Double.parseDouble(key.get(stats)
                                                           .toString()) :
                                     min;
                          } catch (IllegalAccessException e) {
                              throw new RuntimeException(e);
                          }
                      });
                  });
        }
    }

    public Centroid[] readCentroid() {
        return null;
    }
}
