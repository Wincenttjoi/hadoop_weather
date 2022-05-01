package utils;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

public class DistanceUtils {


    /**
     * Calculates Euclidean distance between 2 points
     * @param p1 features of a point
     * @param p2 features of a centroid
     * @return Euclidean distance between the 2 points
     */
    public static double calculateDistance(Map<String, String> p1, Map<String, String> p2) {
        double distance = 0;

        for (String attribute : p1.keySet()) {

            String[] features1 = p1.get(attribute)
                                   .split(",");
            String[] features2 = p2.get(attribute)
                                   .split(",");


            for (int i = 0; i < features1.length; i++) {
                if (!features1[i].equals("M")) {
                    Double value1 = Double.parseDouble(features1[i]);
                    Double value2 = Double.parseDouble(features2[i]);
                    if (!Double.isNaN(value1) && !Double.isNaN(value2)) {
                        distance += Math.pow(value1 - value2, 2);
                    }
                }
            }
        }

        return Math.sqrt(distance);
    }

    /**
     * Adds the total value for features
     * @param p1 feature list of point
     * @param p2 feature list of another point
     * @return total value added from p1 and p2
     */
    public static String calculateTotal(String p1, String p2) {
        String[] features1 = p1.split(",");
        String[] features2 = p2.split(",");


        String[] result = new String[features1.length];

        Arrays.setAll(result, index -> String.valueOf(Objects.equals(features1[index], "M") ?
                                                      0.0 :
                                                      Double.isNaN(Double.parseDouble(features1[index])) ?
                                                      0.0 :
                                                      Double.parseDouble(features1[index]) +
                                                      Double.parseDouble(features2[index])));

        return String.join(",", result);

    }

    /**
     * Gets the average features for centroid
     * @param feature feature list of centroid
     * @param denominator number to be divided by
     * @return feature list for relocated centroid
     */
    public static String calculateAverage(String feature, int denominator) {
        String[] features = feature.split(",");
        Arrays.setAll(features, index -> String.valueOf(Double.parseDouble(features[index]) / denominator));

        return String.join(",", features);
    }
}
