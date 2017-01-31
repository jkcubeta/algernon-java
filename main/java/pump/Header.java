package pump;

import java.util.List;
import java.util.Map;

public class Header {

    private String tableName;
    private List<String> columnNames;
    private Map<String,String> dataTypes;

    public Header(String tableName, List<String> columnNames, Map<String,String> dataTypes){
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.dataTypes = dataTypes;
    }

    String getDataType(String columnName){
        return dataTypes.get(columnName);
    }

    Boolean containsColumnName(String columnName){
        return columnNames.contains(columnName);
    }

    List<String> getColumnNames(){
        return columnNames;
    }
}
