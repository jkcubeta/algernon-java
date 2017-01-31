package pump;

import algernon.AlgyPumpWhisperer;
import algernon.RemoteDatabaseConnectionError;

import java.io.*;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.Set;

class AlgyBuilder {

    private Properties storedQueriesProperties;
    private DataMap dataMap;

    AlgyBuilder(AlgyPumpWhisperer whisperer){
        dataMap = new DataMap();
        ClassLoader loader = this.getClass().getClassLoader();
        storedQueriesProperties = loadQueries(loader);
        Set<String> tableNames;
        try {
            tableNames = whisperer.pullReplicatedTableNames();
            for(String tableName : tableNames){
                Header header = whisperer.getTableHeader(tableName);
               if(!storedQueriesProperties.containsKey(tableName+"_insert")){
                   buildInsertQuery(tableName,header.getColumnNames());
               }
               if(!storedQueriesProperties.containsKey(tableName+"_update")){
                   buildUpdateQuery(tableName,header.getColumnNames(),whisperer.getPks(tableName));
               }
                if(!storedQueriesProperties.containsKey(tableName+"_delete")){
                    buildDeleteQuery(tableName,whisperer.getPks(tableName));
                }
            }
        }catch(SQLException e){
            throw new RemoteDatabaseConnectionError();
        }
        storeQueries(loader, storedQueriesProperties);
    }

    private Properties loadQueries(ClassLoader loader){
        Properties storedQueries  = new Properties();
        InputStream in = loader.getResourceAsStream("stored_queries.properties");
        try {
            storedQueries.load(in);
            in.close();
        }catch(IOException e){
            throw new RuntimeException("can't locate the files in the rsc folder, verify their presence and try again");
        }
        return storedQueries;
    }

    private void storeQueries(ClassLoader loader, Properties properties){
        try {
            File propertiesFile = new File(loader.getResource("stored_queries.properties").getFile());
            PrintWriter writer = new PrintWriter(propertiesFile);
            properties.store(writer,"--no comments--");
            writer.flush();
            writer.close();
        }catch (IOException e){
            throw new RuntimeException("can't locate the files in the rsc folder, verify their presence and try again");
        }

    }

    String getInsertQuery(String tableName){
        return storedQueriesProperties.getProperty(tableName+"_insert");
    }

    String getUpdateQuery(String tableName){
        return storedQueriesProperties.getProperty(tableName+"_update");
    }

    String getDeleteQuery(String tableName){
        return storedQueriesProperties.getProperty(tableName+"_delete");
    }

    private void buildInsertQuery(String tableName, List<String> columnNames){
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

    private void buildUpdateQuery(String tableName, List<String> columnNames, List<String> pkColumnNames){
        StringBuilder builder = new StringBuilder("UPDATE "+tableName+" SET ");
        for(String columnName : columnNames){
            builder.append(dataMap.mapName(columnName)).append(" = ?,");
        }
        builder.deleteCharAt(builder.length()-1).append(" WHERE ");
        for(String pkColumn : pkColumnNames){
            builder.append(dataMap.mapName(pkColumn)).append(" = ?").append(" AND ");
        }
        builder.delete(builder.length()-5,builder.length()-1).append(";");
        storedQueriesProperties.setProperty(tableName+"_update",builder.toString());
    }

    private void buildDeleteQuery(String tableName, List<String> pkColumnNames){

        StringBuilder sql = new StringBuilder();
        sql.append("DELETE FROM ").append(tableName).append(" WHERE ");
        for(String pkName : pkColumnNames){
            sql.append(tableName).append(".").append(pkName).append(" = ? AND ");
        }
        sql.delete(sql.length()-5,sql.length()-1);
        sql.append(";");
        storedQueriesProperties.setProperty(tableName+"_delete",sql.toString());
    }

}
