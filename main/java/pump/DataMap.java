package pump;

import java.sql.*;

public class DataMap {
    public DataMap(){

    }

    String mapName(String columnName){
        if(columnName.equals("order")){
            columnName = columnName.replace("order","ordinal_position");
        }else if(columnName.length()>=64) {
            columnName = columnName.substring(0, 64);
        }else if(columnName.contains("condition")||columnName.equals("condition")){
            columnName = columnName.replace("condition","medical_condition");
        }else if(columnName.contains("-")){
            columnName = columnName.replace("-","_");
        }else if(columnName.equals("Range")) {
            columnName = columnName.replace("Range", "result_range");
        }else if(columnName.equals("Status")) {
            columnName = columnName.replace("Status", "current_status");
        }else if(columnName.equals("inout")) {
            columnName = columnName.replace("inout", "in_out");
        }else if(columnName.equals("value")) {
            columnName = columnName.replace("value", "field_value");
        }else if(columnName.equals("key")) {
            columnName = columnName.replace("key", "unique_key");
        }else if(columnName.contains(" ")) {
            columnName = columnName.replace(" ", "_");
        }else if(columnName.equals("release")) {
            columnName = columnName.replace("release", "release_roi");
        }else if(columnName.equals("repeat")) {
            columnName = columnName.replace("repeat", "num_repeat");
        }else if(columnName.equals("column_name")){
            columnName = columnName.replace("column_name","column_name_value");
        }
        return columnName;
    }

    String standardizeData(String dataField){
        switch (dataField) {
            case "FALSE":
                return "0";
            case "TRUE":
                return "1";
            default:
                return dataField;
        }
    }

    void mapDataType(String dataType, PreparedStatement updateStatement, Integer q, String fieldValue) throws SQLException {
        if (!(fieldValue == null)) {
            switch (dataType) {
                case "smallint":
                    updateStatement.setInt(q, Integer.parseInt(fieldValue));
                    break;
                case "tinyint":
                    updateStatement.setBoolean(q, Boolean.parseBoolean(fieldValue));
                    break;
                case "datetime":
                    Timestamp timeStamp = new Timestamp(javax.xml.bind.DatatypeConverter.parseDateTime(fieldValue).getTimeInMillis());
                    updateStatement.setTimestamp(q, timeStamp);
                    break;
                case "time":
                    updateStatement.setTime(q, Time.valueOf(fieldValue));
                    break;
                case "date":
                    //credible seems to export date values with time = 0 for date fields
                    fieldValue = fieldValue.split("T")[0];
                    updateStatement.setDate(q, Date.valueOf(fieldValue));
                    break;
                case "long":
                    updateStatement.setLong(q, Long.parseLong(fieldValue));
                    break;
                case "float":
                    updateStatement.setFloat(q, Float.parseFloat(fieldValue));
                    break;
                case "double":
                    updateStatement.setDouble(q, Double.parseDouble(fieldValue));
                    break;
                case "decimal":
                    updateStatement.setDouble(q, Double.parseDouble(fieldValue));
                    break;
                case "int":
                    updateStatement.setInt(q, Integer.parseInt(fieldValue));
                    break;
                case "bigint":
                    updateStatement.setInt(q, Integer.parseInt(fieldValue));
                    break;
                default:
                    updateStatement.setString(q, fieldValue);
            }
        } else {
            switch (dataType) {
                case "smallint":
                    updateStatement.setNull(q, 5);
                    break;
                case "tinyint":
                    updateStatement.setNull(q, -6);
                    break;
                case "datetime":
                    updateStatement.setNull(q, 93);
                    break;
                case "time":
                    updateStatement.setNull(q, 92);
                    break;
                case "date":
                    updateStatement.setNull(q, 91);
                    break;
                case "long":
                    updateStatement.setNull(q, 6);
                    break;
                case "float":
                    updateStatement.setNull(q, 6);
                    break;
                case "double":
                    updateStatement.setNull(q, 8);
                    break;
                case "decimal":
                    updateStatement.setNull(q, 3);
                    break;
                case "int":
                    updateStatement.setNull(q, 4);
                    break;
                case "bigint":
                    updateStatement.setNull(q, -5);
                    break;
                default:
                    updateStatement.setNull(q, 12);
            }
        }
    }
}
