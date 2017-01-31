package parser;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class InterventionParser {

    public static void main(String[] args){

    }

    InterventionParser(File inputFile){

        List<String> rawInterventions = getStoredInterventions(inputFile);

    }

    private List<String> getStoredInterventions(File inputFile){
        List<String> rawInterventions = new ArrayList<>();
        CSVParser parser = CSVParser.parse(inputFile, CSVFormat.Excel);
        for(CSVRecord record : parser){
            rawInterventions.add(record);
        }
        return rawInterventions;
    }
}
