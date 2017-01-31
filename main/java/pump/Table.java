package pump;

import algernon.AlgyPumpWhisperer;
import algernon.AlgyWhisperer;
import security.DatabaseCredentials;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

class Table implements CredibleTable {

    private Log log;
    private AlgyPumpWhisperer algyPumpWhisperer;
    private AlgyWhisperer whisperer;
    String tableName;
    private List<String> pkColumnNames;
    private Map<PrimaryKey, TableRow> tableData;
    private Header header;

    Table(String tableName, Set<PrimaryKey>foreignKeys, DatabaseCredentials credentials) {
        log = new Log();
        this.tableName = tableName;
        this.tableData = new HashMap<>();
        this.algyPumpWhisperer = new AlgyPumpWhisperer(credentials);
        try {
            pkColumnNames = algyPumpWhisperer.getPks(tableName);
            header = algyPumpWhisperer.getTableHeader(tableName);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            ResultSet algyTable = algyPumpWhisperer.dumpLocalTable(tableName,foreignKeys);
            if(algyTable.isBeforeFirst()) {
                while (algyTable.next()) {
                    TableRow algyRow = new TableRow(header,pkColumnNames,algyTable);
                    tableData.put(algyRow.getPrimaryKey(), algyRow);
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        List<Integer> pkValues = new ArrayList<>();
        for(PrimaryKey pk : foreignKeys){
            pkValues.add(pk.getFirstKeyValue());
        }
        Collections.sort(pkValues);
        log.log("got algy version of "+tableName+" ("+pkValues.get(0)+" - "+pkValues.get(pkValues.size()-1));
    }

    Table(String tableName, Boolean synched, DatabaseCredentials credentials) {
        Log log = new Log();
        Captain captain;
        this.algyPumpWhisperer = new AlgyPumpWhisperer(credentials);
        this.whisperer = new AlgyWhisperer(credentials);
        this.log = new Log();
        this.tableName = tableName;
        this.tableData = new HashMap<>();
        Integer tableChunkCount;
        List<PrimaryKey> remotePks;
        try {
            this.header = algyPumpWhisperer.getTableHeader(tableName);
            this.pkColumnNames = algyPumpWhisperer.getPks(tableName);
            captain = new Captain(tableName, credentials, pkColumnNames, header);
            remotePks = getRemotePks(tableName, synched);
            if (!synched) {
                captain.organizeWork(remotePks);
            } else {
                for (PrimaryKey remoteKey : remotePks) {
                    captain.orderSingleWork(remoteKey);
                }
            }
            tableChunkCount = captain.getTotalWorkCount();
            log.log("this table " + tableName + " has " + tableChunkCount + " chunks (chunk = 10000 row items)) in it");
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("can't connect to replication database, check connections and try again");
        } catch (Exception e){
            e.printStackTrace();
            throw new RuntimeException("this is embarrassing, but it seems we have an unknown error, consult the stacktrace and give it a shot");
        }
        tableData = captain.work();
        Set<PrimaryKey> keys = tableData.keySet();
        log.log("extracted all the remote keys for table "+tableName+" ("+keys.size()+"), removing extraneous values ");
        if(synched) {
            keys.retainAll(remotePks);
        }
        if(!(tableData.size() == remotePks.size())){
            log.log("this table "+tableName+" done fucked up. Can't guarantee we got all the data, got "+tableData.size()+" rows out of "+remotePks.size());
        }
        log.log("got remote version of "+tableName);
    }

    private List<PrimaryKey> getRemotePks(String tableName, Boolean synched) throws Exception{
        String primeDate;
        if(synched) {
            primeDate = algyPumpWhisperer.getLastUpdateDate(tableName);
        }else{
            primeDate = "no date";
        }
        String dateParam;
        if(primeDate.equals("no date")){
            dateParam = "";
        }else{
            dateParam = primeDate.replace(" ","T");
        }
        pkColumnNames = algyPumpWhisperer.getPks(tableName);
        return whisperer.getRemotePksData(tableName,header,pkColumnNames,dateParam);
    }

    @Override
    public Map<PrimaryKey, TableRow> getTableRows() {
        return tableData;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public Header getHeader() {
        return header;
    }
}

