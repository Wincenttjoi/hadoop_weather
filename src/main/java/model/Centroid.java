package model;

import constants.Constants;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class Centroid {

    private Map<String, Double> attributes;

    public Centroid() {

    }

    public Centroid(Map<String, Double> attributes) {
        this.attributes = attributes;
    }

    public Centroid(String[] attributes) {
        this.attributes = new HashMap<>();
        for (int i = 0; i < attributes.length ; i += 2) {
            this.attributes.put(attributes[i], Double.parseDouble(attributes[i + 1]));
        }
    }

    public Map<String, Double> getAttributes() {
        return attributes;
    }

    public Map<String, Double> getRelevantAttributes() {
        return attributes.entrySet()
                         .stream()
                         .filter(entry -> Arrays.stream(Constants.RELEVANT_ATTRIBUTES)
                                                .anyMatch(attribute -> attribute.equals(entry.getKey())))
                         .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public void setAttributes(Map<String, Double> attributes) {
        this.attributes = attributes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Centroid centroid = (Centroid) o;
        return Objects.equals(attributes, centroid.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(attributes);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        for (Map.Entry<String, Double> x : attributes.entrySet()
        ) {
            sb.append(String.format("Key: %s, Value: %f\n", x.getKey(), x.getValue()));
        }

        return sb.toString();
    }
}
