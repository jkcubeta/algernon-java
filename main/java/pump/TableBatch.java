package pump;

import algernon.RemoteDatabaseConnectionError;
import algernon.ReplicationDatabaseConnection;
import security.DatabaseCredentials;
import algernon.AlgyPumpWhisperer;

import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

class TableBatch{

    private String tableName;
    private Header header;
    private PreparedStatement updateStatement;
    private PreparedStatement insertStatement;
    private PreparedStatement deleteStatement;
    private DataMap dataMap;
    private Log log;
    private Integer maxRowCount = 1000;
    private Integer updateCount = 0;
    private Integer insertCount = 0;
    private Integer deleteCount = 0;

    TableBatch(String tableName, Header header, AlgyPumpWhisperer whisperer, DatabaseCredentials credentials){
        this.tableName = tableName;
        this.header = header;
        this.dataMap = new DataMap();
        this.log = new Log();
        AlgyBuilder builder = new AlgyBuilder(whisperer);
        ReplicationDatabaseConnection connection = new ReplicationDatabaseConnection(credentials);
        try {
            this.updateStatement = connection.createPreparedStatement(builder.getUpdateQuery(tableName));
            this.insertStatement = connection.createPreparedStatement(builder.getInsertQuery(tableName));
            this.deleteStatement = connection.createPreparedStatement(builder.getDeleteQuery(tableName));
        }catch(SQLException e){
            throw new RemoteDatabaseConnectionError();
        }
    }

    Boolean executeInsertBatch() throws SQLException{
        try {
            log.log("firing insert batch for table "+tableName);
            insertStatement.executeBatch();
            return true;
        }catch(BatchUpdateException e){
            throw new RemoteDatabaseConnectionError(e);
        }catch(Exception e){
            return false;
        }
    }

    Boolean executeUpdateBatch() throws SQLException{
        try {
            log.log("firing update batch for table "+tableName);
            updateStatement.executeBatch();
            return true;
        }catch(BatchUpdateException e){
            throw new RemoteDatabaseConnectionError(e);
        }catch(Exception e){
            return false;
        }
    }

    Boolean executeDeleteBatch() throws SQLException{
        try {
            log.log("firing delete batch for table "+tableName);
            deleteStatement.executeBatch();
            return true;
        }catch(BatchUpdateException e){
            throw new RemoteDatabaseConnectionError(e);
        }catch(Exception e){
            return false;
        }
    }

    Boolean addInsertRowAndBatch(TableRow tableRow) throws SQLException{
        addInsertRow(tableRow);
        if(insertCount >= maxRowCount){
            log.log("firing insert batch for table "+tableName+" "+maxRowCount+" records in batch");
            try {
                insertStatement.executeBatch();
            }catch (BatchUpdateException e){
                throw new RemoteDatabaseConnectionError(e);
            }
            insertCount = 0;
            return true;
        }else{
            return false;
        }
    }

    Boolean addUpdateRowAndBatch(TableRow tableRow) throws SQLException{
        addUpdateRow(tableRow);
        if(updateCount >= maxRowCount){
            log.log("firing update batch for table "+tableName+" "+ maxRowCount+" records in batch");
            try {
                updateStatement.executeBatch();
            }catch(BatchUpdateException e){
                throw new RemoteDatabaseConnectionError(e);
            }
            updateCount = 0;
            return true;
        }else{
            return false;
        }
    }

    Boolean addDeleteRowAndBatch(PrimaryKey primaryKey) throws SQLException{
        addDeleteRow(primaryKey);
        if(deleteCount >= maxRowCount){
            log.log("firing delete batch for table "+tableName+" "+ maxRowCount+" records in batch");
            try {
                deleteStatement.executeBatch();
            }catch (BatchUpdateException e){
                throw new RemoteDatabaseConnectionError(e);
            }
            deleteCount = 0;
            return true;
        }else{
            return false;
        }
    }

    private PreparedStatement addInsertRow(TableRow tableRow) throws SQLException{
        Map<String,String> rowData = tableRow.rowData;
        Integer q = 1;
        for(String columnName : rowData.keySet()) {
            dataMap.mapDataType(header.getDataType(columnName), insertStatement, q, rowData.get(columnName));
            q++;
        }
        insertStatement.addBatch();
        insertCount++;
        return insertStatement;
    }

    private PreparedStatement addUpdateRow(TableRow tableRow) throws SQLException{
        Map<String,String> rowData = tableRow.rowData;
        PrimaryKey primaryKey = tableRow.getPrimaryKey();
        Integer q = 1;
        for(String columnName : rowData.keySet()){
            dataMap.mapDataType(header.getDataType(columnName),updateStatement,q,rowData.get(columnName));
            q++;
        }
        String testClass = primaryKey.getClass().getSimpleName();
        if(testClass.equals("SingleColumnPK")) {
            updateStatement.setInt(q, (Integer) primaryKey.getKeyValue());
        }else if(testClass.equals("MultiColumnPK")){
            Map<String, Integer> pk = (Map<String, Integer>) primaryKey.getKeyValue();
            for(String keyColumnName : pk.keySet()){
                updateStatement.setInt(q,pk.get(keyColumnName));
                q++;
            }
        }else{
            throw new RuntimeException("format of object used as primary key is unknown");
        }
        updateStatement.addBatch();
        updateCount++;
        return updateStatement;
    }

    private PreparedStatement addDeleteRow(PrimaryKey primaryKey) throws SQLException{
        Integer q = 1;
        String testClass = primaryKey.getClass().getSimpleName();
        if(testClass.equals("SingleColumnPK")) {
            deleteStatement.setInt(q, (Integer) primaryKey.getKeyValue());
        }else if(testClass.equals("MultiColumnPK")){
            Map<String, Integer> pk = (Map<String, Integer>) primaryKey.getKeyValue();
            for(String keyColumnName : pk.keySet()){
                deleteStatement.setInt(q,pk.get(keyColumnName));
                q++;
            }
        }else{
            throw new RuntimeException("format of object used as primary key is unknown");
        }
        deleteStatement.addBatch();
        deleteCount++;
        return deleteStatement;
    }

}