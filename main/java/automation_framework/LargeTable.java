package automation_framework;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

class LargeTable implements CredibleTable {
    private Log logger;
    private String tableName;
    private AlgyWhisperer algyWhisperer;
    private ArrayList<String> pkColumnNames;
    private Map<Map<String,Integer>, TableRow> tableData;
    private Map<String, String> header;

    LargeTable(String tableName){
        this.logger = new Log();
        this.algyWhisperer = new AlgyWhisperer(Robot.dbUsername,Robot.dbPassword,Robot.address,Robot.dbName);
        this.tableName = tableName;
        this.tableData = new HashMap<>();
        EncryptedKeys keys = new EncryptedKeys();
        PrimaryKey topLocalPk;
        String encryptedDataPullKey;
        try {
            this.header = algyWhisperer.getTableHeader(tableName);
            this.pkColumnNames = algyWhisperer.getPks(tableName);
            topLocalPk = algyWhisperer.getTopPk(tableName);
            encryptedDataPullKey = algyWhisperer.getEncryptedKeyByName(tableName+"_table_puller",Robot.internalKey,keys);
        }catch(SQLException e){
            e.printStackTrace();
            throw new ReplicationDatabaseConnectionError();
        }

        //generate new table rows based on PK not in database already
        try{
            tableData.putAll(buildNewData(encryptedDataPullKey,topLocalPk));
            //update all table rows indicated in ChangeLog
            tableData.putAll(buildModifiedData(algyWhisperer.getLastUpdateDate(tableName)));
        }catch(Exception e){
            e.printStackTrace();
            throw new RemoteDatabaseConnectionError();
        }
    }

    private Map<Map<String,Integer>,TableRow> buildNewData(String encryptedKey,PrimaryKey topLocalPK) throws Exception{
        Map<Map<String,Integer>,TableRow> outputData = new HashMap<>();
        Data newData = new Data(encryptedKey,topLocalPK.getSingleKeyValue(),null);
        Map<String, Map<String,String>> mapData = newData.getMapData();
        PreparedStatement insertStatement = algyWhisperer.startInsertBatch(tableName,header);
        Integer batchCount = 0;
        for(String key : mapData.keySet()) {
            Map<String, String> data = mapData.get(key);
            Map<String, Integer> pk = new HashMap<>();
            pk.put(pkColumnNames.get(0), Integer.valueOf(data.get(pkColumnNames.get(0))));
            TableRow newRow = new TableRow(pk, data);
            outputData.put(pk,newRow);
            algyWhisperer.addInsertRowToBatch(insertStatement,header,newRow);
            batchCount++;
            if(batchCount>=10000){
                insertStatement.executeBatch();
            }
        }
        insertStatement.executeBatch();
        logger.log("retrieved a total of "+outputData.size()+" new rows from the table "+tableName);
        return outputData;
    }

    private Map<Map<String,Integer>,TableRow> buildModifiedData(String updateDate) throws Exception{
        AtomicInteger updateCount = new AtomicInteger(0);
        Map<Map<String,Integer>,TableRow> updateRows = new HashMap<>();
        ChangeLog log = new ChangeLog(tableName,updateDate);
        List<String> changes = log.getChanges();
        logger.log("table "+tableName+" has "+changes.size()+" entries to be updated");
        algyWhisperer.startUpdateBatch(tableName,header,pkColumnNames.get(0));
        for(String change : changes){
            try {
                Data changeData = new Data(algyWhisperer.getEncryptedKeyByName(tableName + "_multipull", Robot.internalKey, new EncryptedKeys()), change, null);
                Map<String, String> changeLine = changeData.getMapData().get(change);
                Map<String, String> keyValue = new HashMap<>();
                keyValue.put(pkColumnNames.get(0), changeLine.get(pkColumnNames.get(0)));
                PrimaryKey pk = new PrimaryKey(tableName, pkColumnNames, keyValue);
                TableRow changeRow = new TableRow(pk, changeLine);
                updateRows.put(pk.getKeyValueStrict(), changeRow);
                updateCount.getAndIncrement();
                logger.log("retrieved remote row with PK "+pk+", "+updateCount+"/"+changes.size());
            }catch(RemoteDatabaseRecordDeletedError e){
                System.out.println(e.getMessage());
            }catch(Exception e){
                e.printStackTrace();
            }
        }
        logger.log("retrieved a total of "+updateRows.size()+" rows to be updated from the table "+tableName);
        return updateRows;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public Map<String, String> getHeader() {
        return header;
    }

    @Override
    public Map<Map<String, Integer>, TableRow> getTableRows() {
        return tableData;
    }
}
