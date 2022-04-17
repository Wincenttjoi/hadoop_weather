package utils;

import com.opencsv.CSVReader;
import com.opencsv.bean.CsvToBeanBuilder;
import com.opencsv.exceptions.CsvValidationException;
import model.Statistics;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class FileUtils {

    CSVReader csvReader;
    List<Statistics> beans;

    public FileUtils(String pathName) throws URISyntaxException, IOException {
         this.beans = new CsvToBeanBuilder(
                Files.newBufferedReader(Paths.get(ClassLoader.getSystemResource(pathName)
                                                             .toURI()))).withType(Statistics.class)
                                                                        .build()
                                                                        .parse();
    }

    public void readLine() throws CsvValidationException, IOException {
        System.out.println(beans.get(0).humidity);

    }

    public List<Statistics> getBeans() {
        return beans;
    }

    public void setBeans(List<Statistics> beans) {
        this.beans = beans;
    }
}
