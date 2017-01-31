package pump;

import org.jdom2.Element;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class TableRow {

    private PrimaryKey primaryKey;
    private Header header;
    Map<String, String> rowData;

    public TableRow(PrimaryKey primaryKey, Header header, Map<String,String> seedData){
        this.header = header;
        this.primaryKey = primaryKey;
        this.rowData = seedData;
    }

    TableRow(Header header, List<String> pkColumnNames, ResultSet algyRow) throws SQLException{
        this.header = header;
        this.rowData = new HashMap<>();
        LinkedHashMap<String,Integer> pkValues = new LinkedHashMap<>();
        ResultSetMetaData meta = algyRow.getMetaData();
        for(int q = 1;q<=meta.getColumnCount();q++){
            String columnName = meta.getColumnName(q);
            String entry = algyRow.getString(q);
            if(pkColumnNames.contains(columnName)){
                pkValues.put(columnName,algyRow.getInt(q));
            }
            if(!(entry == null)) {
                if (meta.getColumnType(q) == -7) {
                    if (entry.equals("0")) {
                        entry = "false";
                    } else {
                        entry = "true";
                    }
                } else if (meta.getColumnType(q) == 93) {
                    SimpleDateFormat isoDf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
                    SimpleDateFormat algyDf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.s");
                    try {
                        Date newDate = algyDf.parse(entry);
                        entry = isoDf.format(newDate);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                }
            }
            if(pkColumnNames.size()>1){
                this.primaryKey = new MultiColumnPK(pkColumnNames,pkValues);
            }else{
                String pkName = pkColumnNames.get(0);
                Integer pkValue = pkValues.get(pkName);
                this.primaryKey = new SingleColumnPK(pkName,pkValue);
            }
            rowData.put(columnName,entry);
        }

    }

    public TableRow(Header header, List<String> pkColumnNames, List<Element> credibleRow){
        rowData = new LinkedHashMap<>();
        Map<String,String> credibleValues = new HashMap<>();
        Set<String> columnNames = new LinkedHashSet<>(header.getColumnNames());
        LinkedHashMap<String,Integer> pkValues = new LinkedHashMap<>();
        //iterate over all returned values and add them to the row
        for(Element child : credibleRow){
            String entry = child.getText();
            String columnName = child.getName();
            if (pkColumnNames.contains(columnName)) {
                pkValues.put(columnName,Integer.valueOf(entry));
            }
            credibleValues.put(columnName,entry);
        }
        for(String columnName : columnNames){
            rowData.put(columnName,credibleValues.get(columnName));
        }
        //since null values are not reliably returned through http, check the child elements against the header and add nulls as needed
        for (String columnName : header.getColumnNames()) {
            if(!rowData.containsKey(columnName)){
                rowData.put(columnName,null);
            }
        }
        if(pkColumnNames.size()>1){
            this.primaryKey = new MultiColumnPK(pkColumnNames,pkValues);
        }else{
            String pkColumnName = pkColumnNames.get(0);
            Integer pkValue = pkValues.get(pkColumnName);
            this.primaryKey = new SingleColumnPK(pkColumnName,pkValue);
        }
    }

    public PrimaryKey getPrimaryKey(){
        return primaryKey;
    }

    public Header getHeader(){
        return header;
    }

}
