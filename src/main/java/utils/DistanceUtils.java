package utils;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class DistanceUtils {

    // Euclidean Distance
    public static double calculateDistance(Map<String, String> p1, Map<String, String> p2) {
        double distance = 0;

        for (String attribute : p1.keySet()
        ) {

            String[] features1 = p1.get(attribute).split(",");
            String[] features2 = p2.get(attribute).split(",");

            for (int i = 0; i < features1.length; i++) {
                Double value1 = Double.parseDouble(features1[i]);
                Double value2 = Double.parseDouble(features2[i]);

                if (value1 != null && value2 != null) {
                    distance += Math.pow(value1 - value2, 2);
                }
            }
        }

        return Math.sqrt(distance);
    }

    public static String calculateTotal(String p1, String p2) {
        String[] features1 = p1.split(",");
        String[] features2 = p2.split(",");


        String[] result = new String[features1.length];

        Arrays.setAll(result, index -> String.valueOf(Double.parseDouble(features1[index]) + Double.parseDouble(features2[index])));

        return String.join(",", result);

    }

    public static String calculateAverage(String feature, int denominator) {
        String[] features = feature.split(",");
        System.out.println("feature: " + feature);
        System.out.println("denominator: " + denominator);
        Arrays.setAll(features, index -> String.valueOf(Double.parseDouble(features[index]) / denominator));

        return String.join(",", features);
    }
}
