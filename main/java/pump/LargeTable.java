package pump;

import algernon.*;
import org.jdom2.Element;
import security.DatabaseCredentials;

import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.util.*;

class LargeTable implements CredibleTable {
    private Log logger;
    private String tableName;
    private AlgyWhisperer whisperer;
    private List<String> pkColumnNames;
    private Map<PrimaryKey, TableRow> tableData;
    private Header header;
    private DatabaseCredentials credentials;
    private TableBatch batch;

    LargeTable(String tableName, DatabaseCredentials credentials){
        this.logger = new Log();
        this.credentials = credentials;
        AlgyPumpWhisperer algyWhisperer = new AlgyPumpWhisperer(credentials);
        this.whisperer = new AlgyWhisperer(credentials);
        this.tableName = tableName;
        this.tableData = new HashMap<>();
        PrimaryKey topLocalPk;
        try {
            this.header = algyWhisperer.getTableHeader(tableName);
            this.pkColumnNames = algyWhisperer.getPks(tableName);
            topLocalPk = algyWhisperer.getTopPk(tableName);
        }catch(SQLException e){
            e.printStackTrace();
            throw new ReplicationDatabaseConnectionError();
        }

        this.batch = new TableBatch(tableName,header, algyWhisperer,credentials);

        //generate new table rows based on PK not in database already
        try {
            tableData.putAll(buildNewData(tableName, topLocalPk));
            //update all table rows indicated in ChangeLog
            tableData.putAll(buildModifiedData(algyWhisperer.getLastUpdateDate(tableName)));
            System.out.println("finished synch of "+tableName);
        }catch(BatchUpdateException e){
            throw new RemoteDatabaseConnectionError(e);
        }catch(Exception e){
            e.printStackTrace();
            throw new RemoteDatabaseConnectionError();
        }
    }

    private Map<PrimaryKey, TableRow> buildNewData(String tableName, PrimaryKey topLocalPK) throws Exception{
        Map<PrimaryKey, TableRow> outputData = new HashMap<>();
        logger.log("beginning data pull for new table rows");
        List<Element> tables;
        try {
            RemoteData newData = whisperer.getNewLargeTableData(tableName,topLocalPK);
            tables = newData.getTables();
        }catch(RemoteDatabaseRecordDeletedError e){
            logger.log(e.getMessage());
            tables = new ArrayList<>();
        }
        for(Element table : tables) {
            TableRow newRow = new TableRow(header,pkColumnNames,table.getChildren());
            outputData.put(newRow.getPrimaryKey(),newRow);
            logger.log("extracted a new large table row with PK "+newRow.getPrimaryKey().getFirstKeyValue().toString());
            batch.addInsertRowAndBatch(newRow);
        }
        batch.executeInsertBatch();
        logger.log("retrieved a total of "+outputData.size()+" new rows from the table "+tableName);
        return outputData;
    }

    private Map<PrimaryKey, TableRow> buildModifiedData(String updateDate) throws Exception{
        Map<PrimaryKey, TableRow> updateRows = new LinkedHashMap<>();
        ChangeLog log = new ChangeLog(tableName,updateDate,credentials);
        List<String> changes = log.getChanges();
        Captain updateCaptain = new Captain(tableName,credentials, pkColumnNames,header);
        logger.log("table "+tableName+" has "+changes.size()+" entries to be updated");
        for(SingleColumnPK pk : log.getPkChanges()){
            updateCaptain.orderSingleWork(pk);
        }

        try{
            updateRows = updateCaptain.workWithBatch(batch);
        }catch(RemoteDatabaseRecordDeletedError e){
            System.out.println(e.getMessage());
        }catch(Exception e){
            e.printStackTrace();
        }

        batch.executeUpdateBatch();
        logger.log("retrieved a total of "+updateCaptain.getTotalWorkCount()+" rows to be updated from the table "+tableName);
        return updateRows;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public Header getHeader() {
        return header;
    }

    @Override
    public Map<PrimaryKey, TableRow> getTableRows() {
        return tableData;
    }
}
