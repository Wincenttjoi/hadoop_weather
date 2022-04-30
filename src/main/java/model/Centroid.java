package model;

import constants.Constants;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class Centroid implements Writable, Serializable {

    private Map<String, String> attributes;

    public Centroid() {

    }

    public Centroid(Map<String, String> attributes) {
        this.attributes = attributes;
    }

    public Centroid(String[] attributes) {
        this.attributes = new HashMap<>();
        for (int i = 0; i < attributes.length ; i += 2) {
            this.attributes.put(attributes[i], attributes[i + 1]);
        }
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public Map<String, String> getRelevantAttributes() {
        return attributes.entrySet()
                         .stream()
                         .filter(entry -> Arrays.stream(Constants.RELEVANT_ATTRIBUTES)
                                                .anyMatch(attribute -> attribute.equals(entry.getKey())))
                         .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public void setAttributes(Map<String, String> attributes) {
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

        for (Map.Entry<String, String> x : attributes.entrySet()
        ) {
            sb.append(String.format("%s:%s|", x.getKey(), x.getValue()));
        }

        return sb.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        byte[] data = SerializationUtils.serialize((Serializable) this.attributes);
        out.writeInt(data.length);
        out.write(data);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        byte[] data = new byte[size];

        in.readFully(data);
        this.attributes = SerializationUtils.deserialize(data);
    }
}
