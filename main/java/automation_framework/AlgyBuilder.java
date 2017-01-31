package automation_framework;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;

class AlgyBuilder {

    private Properties storedQueriesProperties;
    private DataMap dataMap;

    AlgyBuilder(){
        dataMap = new DataMap();
        storedQueriesProperties = buildQueries();
    }

    private Properties buildQueries(){
        Properties storedQueries  = new Properties();
        ClassLoader loader = this.getClass().getClassLoader();
        InputStream in = loader.getResourceAsStream("stored_queries.properties");
        try {
            storedQueriesProperties.load(in);
            in.close();
        }catch(IOException e){
            throw new RuntimeException("can't locate the files in the rsc folder, verify their presence and try again");
        }
        return storedQueries;
    }

    String getInsertQuery(String tableName, Set<String> columnNames){
        if(storedQueriesProperties.contains(tableName+"_insert")) {
            return storedQueriesProperties.getProperty(tableName+"_insert");
        }else{
            buildInsertQuery(tableName,columnNames);
            return storedQueriesProperties.getProperty(tableName+"_insert");
        }
    }

    String getUpdateQuery(String tableName, Set<String> columnNames, Set<String> pkColumnNames){
        if(storedQueriesProperties.contains(tableName+"_update")){
            return storedQueriesProperties.getProperty(tableName+"_update");
        }else{
            buildUpdateQuery(tableName,columnNames,pkColumnNames);
            return storedQueriesProperties.getProperty(tableName+"_update");
        }
    }

    private void buildInsertQuery(String tableName, Set<String> columnNames){
        StringBuilder builder = new StringBuilder();
        String sqlStarter = "INSERT INTO "+tableName+" (";
        builder.append(sqlStarter);
        for(String columnName : columnNames){
            builder.append(dataMap.mapName(columnName)).append(",");
        }
        builder.deleteCharAt(builder.length()-1);
        builder.append(") VALUES (");
        for(String columnName : columnNames){
            builder.append("?,");
        }
        builder.deleteCharAt(builder.length()-1);
        builder.append(");");
        storedQueriesProperties.setProperty(tableName+"_insert",builder.toString());
    }

    private void buildUpdateQuery(String tableName, Set<String> columnNames, Set<String> pkColumnNames){
        StringBuilder builder = new StringBuilder("UPDATE "+tableName+" SET ");
        for(String columnName : columnNames){
            builder.append(dataMap.mapName(columnName)).append(" = ?,");
        }
        builder.deleteCharAt(builder.length()).append(" WHERE ");
        for(String pkColumn : pkColumnNames){
            builder.append(dataMap.mapName(pkColumn)).append(" = ?").append(" AND ");
        }
        builder.delete(builder.length()-4,builder.length());
        storedQueriesProperties.setProperty(tableName+"_update",builder.toString());
    }

}
