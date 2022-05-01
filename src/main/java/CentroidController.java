import model.Centroid;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import utils.DistanceUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Collectors;

public class CentroidController {

    List<String> list;
    FileSystem fileSystem;

    public CentroidController(String fileName, Configuration configuration) throws IOException {
        this.list = new ArrayList<>();
        this.fileSystem = FileSystem.get(configuration);
        FileStatus[] status = fileSystem.listStatus(new Path(fileName));
        FileStatus status1 = status[0];


        if (!status1.getPath()
                    .toString()
                    .endsWith("_SUCCESS")) {
            BufferedReader bufferedReader = new BufferedReader(
                    new InputStreamReader(fileSystem.open(status1.getPath())));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }
                String[] string = line.split("\t");
                list.add(string[1]);
            }
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
        Map<String, Double[]> maxList = new HashMap<>();
        Map<String, Double[]> minList = new HashMap<>();

        Random random = new Random();

        // get min max values to generate a centroid
        test(maxList, minList);
//        for (String s : maxList.keySet()
//        ) {
//            maxList.forEach((key, v) -> System.out.println("max: " + Arrays.toString(v)));
//            minList.forEach((key, v) -> System.out.println("min: " + Arrays.toString(v)));
//        }

        for (int i = 0; i < k ; i++) {
            Map<String, String> centroid = new HashMap<>();
            for (String attribute : maxList.keySet()
            ) {
                Double[] max = maxList.get(attribute);
                Double[] min = minList.get(attribute);
                String[] result = new String[maxList.get(attribute).length];
                Arrays.setAll(result, index -> String.valueOf(random.nextDouble() * (max[index] - min[index]) + min[index]));
                centroid.put(attribute, Arrays.stream(result).map(String::valueOf).collect(Collectors.joining(",")));
            }
            centroids.add(new Centroid(centroid));
        }
        return centroids;
    }

//    private void getMaxMinValue(Map<String, Double> maxList, Map<String, Double> minList) throws NoSuchFieldException {
//        for (String stats : list) {
//            Arrays.stream(stats)
//                  .forEach((key) -> {
//                      maxList.compute(key.getName(), (key1, max) -> {
//                          try {
//                              return max == null || Double.parseDouble(key.get(stats)
//                                                                          .toString()) > max ?
//                                     Double.parseDouble(key.get(stats)
//                                                           .toString()) :
//                                     max;
//                          } catch (IllegalAccessException e) {
//                              throw new RuntimeException(e);
//                          }
//                      });
//
//                      minList.compute(key.getName(), (key1, min) -> {
//                          try {
//                              return min == null || Double.parseDouble(key.get(stats)
//                                                                          .toString()) < min ?
//                                     Double.parseDouble(key.get(stats)
//                                                           .toString()) :
//                                     min;
//                          } catch (IllegalAccessException e) {
//                              throw new RuntimeException(e);
//                          }
//                      });
//                  });
//        }
//    }

    private void test(Map<String, Double[]> maxList, Map<String, Double[]> minList) {
        initHashMap(maxList, minList);
        for (int i = 0; i < list.size(); i++) {
            String[] features = list.get(i).split(",");
            setMinMaxFeatures(features, maxList, minList);
        }
    }

    private void setMinMaxFeatures(String[] features, Map<String, Double[]> maxList, Map<String, Double[]> minList) {
        Double[] maxTemp = maxList.getOrDefault("temperature", new Double[12]);
        Double[] maxHumidity = maxList.getOrDefault("humidity", new Double[12]);
        Double[] maxElevation = maxList.getOrDefault("elevation", new Double[12]);

        Double[] minTemp = minList.getOrDefault("temperature", new Double[12]);
        Double[] minHumidity = minList.getOrDefault("humidity", new Double[12]);
        Double[] minElevation = minList.getOrDefault("elevation", new Double[12]);

        for (int i = 0; i < features.length; i++) {
            if (!features[i].equals("M")) {
                String elevation = features[i].split(":")[0];
                String temperature = features[i].split(":")[1];
                String humidity = features[i].split(":")[2];
                if (temperature.equals("NaN")) {
                    temperature = "0.0";
                }
                if (humidity.equals("NaN")) {
                    humidity = "0.0";
                }
                maxElevation[i] = Math.max(maxElevation[i], Double.parseDouble(elevation));
                minElevation[i] = Math.min(minElevation[i], Double.parseDouble(elevation));
                maxTemp[i] = Math.max(maxTemp[i], Double.parseDouble(temperature));
                minTemp[i] = Math.min(minTemp[i], Double.parseDouble(temperature));
                maxHumidity[i] = Math.max(maxHumidity[i], Double.parseDouble(humidity));
                minHumidity[i] = Math.min(minHumidity[i], Double.parseDouble(humidity));
            }
        }

        maxList.put("temperature", maxTemp);
        minList.put("temperature", minTemp);
        maxList.put("elevation", maxElevation);
        minList.put("elevation", minElevation);
        maxList.put("humidity", maxHumidity);
        minList.put("humidity", minHumidity);
    }

    private void initHashMap(Map<String, Double[]> maxList, Map<String, Double[]> minList) {
        Double[] maxTemp = new Double[12];
        Double[] minTemp = new Double[12];
        Double[] maxHumidity = new Double[12];
        Double[] minHumidity = new Double[12];
        Double[] maxElevation = new Double[12];
        Double[] minElevation = new Double[12];


        Arrays.fill(maxTemp, Double.MIN_VALUE);
//        Arrays.fill(maxTemp, 0.0);
        Arrays.fill(minTemp, Double.MAX_VALUE);
//        Arrays.fill(minTemp, 0.0);
        Arrays.fill(maxHumidity, Double.MIN_VALUE);
//        Arrays.fill(maxHumidity, 0.0);
        Arrays.fill(minHumidity, Double.MAX_VALUE);
//        Arrays.fill(minHumidity, 0.0);
        Arrays.fill(maxElevation, Double.MIN_VALUE);
        Arrays.fill(minElevation, Double.MAX_VALUE);

        maxList.put("temperature", maxTemp);
        maxList.put("humidity", maxHumidity);
        minList.put("temperature", minTemp);
        minList.put("humidity", minHumidity);
        maxList.put("elevation", maxElevation);
        minList.put("elevation", minElevation);
    }
}
