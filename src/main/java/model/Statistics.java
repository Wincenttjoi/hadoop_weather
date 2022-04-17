package model;

import com.opencsv.bean.CsvBindByName;
import com.opencsv.bean.CsvDate;
import constants.Constants;

import java.lang.reflect.Field;
import java.util.Date;

public class Statistics {

    @CsvBindByName
    public String station;

    @CsvBindByName(column = "valid")
    @CsvDate("yyyy-MM-dd")
    public Date date;

    @CsvBindByName
    public String elevation;

    @CsvBindByName(column = "tmpc")
    public String temperature;

    @CsvBindByName(column = "relh")
    public String humidity;

    @CsvBindByName(column = "drct")
    public String windDirection;

    @CsvBindByName(column = "sped")
    public String windSpeed;

    public Field[] getRelevantFields() throws NoSuchFieldException {
        String[] relevantAttributes = Constants.RELEVANT_ATTRIBUTES;
        Field[] fields = new Field[relevantAttributes.length];

        for (int i = 0; i < relevantAttributes.length; i++) {
            fields[i] = Statistics.class.getDeclaredField(relevantAttributes[i]);
        }

        return fields;
    }
}
