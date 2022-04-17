package utils;

import java.util.Map;

public class DistanceUtils {

    // Euclidean Distance
    public static double calculateDistance(Map<String, Double> p1, Map<String, Double> p2) {
        double distance = 0;

        for (String attribute : p1.keySet()
        ) {
            Double value1 = p1.get(attribute);
            Double value2 = p2.get(attribute);

            if (value1 != null && value2 != null) {
                distance += Math.pow(value1 - value2, 2);
            }
        }

        return Math.sqrt(distance);
    }
}
