package pump;

import security.DatabaseCredentials;
import algernon.AlgyPumpWhisperer;

import java.sql.SQLException;
import java.util.Map;
import java.util.TreeMap;

class TableWorker implements Runnable{
    private Log logger;
    private AlgyPumpWhisperer algyPumpWhisperer;
    private DatabaseCredentials credentials;

    TableWorker(DatabaseCredentials credentials){
        logger = new Log();
        this.credentials = credentials;
        algyPumpWhisperer = new AlgyPumpWhisperer(credentials);
    }

    public static void main(String[] args){
        DatabaseCredentials credentials = new DatabaseCredentials(args[0]);
        TableWorker worker = new TableWorker(credentials);
        worker.run();
    }

	@Override
	public void run(){
	    String oldestTableName;
        Boolean changeTracked;
        Table algyTable;
        CredibleTable remoteTable;
		try {
            String[] oldestTablePacket = algyPumpWhisperer.getOldestTable();
            oldestTableName = oldestTablePacket[0];
            System.out.println("starting synch of "+oldestTableName);
            logger.log("starting synch of "+oldestTableName);
            changeTracked = oldestTablePacket[1].equals("1");
            logger.log("getting remote version of "+oldestTableName);
            if(oldestTableName.equals("ClientVisit")){
                remoteTable = new LargeTable(oldestTablePacket[0],credentials);
            }else {
                remoteTable = new Table(oldestTableName,changeTracked,credentials);
            }
            logger.log("getting algy version of "+oldestTableName);
            if(changeTracked && remoteTable.getTableRows().isEmpty()){
                logger.log("didn't bother to get algy version of "+oldestTableName+", as the remote version shows no changes");
            }else if(oldestTableName.equals("ClientVisit")) {
                logger.log("didn't bother to get algy version of "+oldestTableName+", as large tables are synched independently");
            }else{
                algyTable = new Table(oldestTableName, remoteTable.getTableRows().keySet(),credentials);
                compare(algyTable, remoteTable, changeTracked);
            }
            algyPumpWhisperer.finishUpdateEntry(remoteTable.getTableName());
        }catch(Exception e){
			e.printStackTrace();
		}
    }

    private void compare(Table algyTable,CredibleTable remoteTable, Boolean synched) throws SQLException{
        TableBatch batch = new TableBatch(algyTable.tableName,algyTable.getHeader(),algyPumpWhisperer,credentials);
	    logger.log("starting comparison of "+algyTable.tableName);
        Map<PrimaryKey, TableRow> algyRows = algyTable.getTableRows();
        Map<PrimaryKey, TableRow> remoteRows = remoteTable.getTableRows();
        if(!algyRows.equals(remoteRows)){
            //check for changes in remote fields or new remote rows
            for(PrimaryKey remotePrimaryKey : remoteRows.keySet()) {
                TableRow algyRow = algyRows.get(remotePrimaryKey);
                TableRow remoteRow = remoteRows.get(remotePrimaryKey);
                //row not found in algernon, add it from the remote server
                if(algyRow == null){
                    try {
                        batch.addInsertRowAndBatch(remoteRow);
                        logger.log("added row to "+algyTable.tableName+" with id "+remotePrimaryKey.getKeyValue().toString());
                    }catch(SQLException e){
                        e.printStackTrace();
                    }
                //row is found, check for differences
                }else{
                    Map<String,String>  updateFields = new TreeMap<>();
                    //get column names and values for both tables
                    Map<String,String> remoteColumns = remoteRow.rowData;
                    Map<String,String> algyColumns = algyRow.rowData;
                    //compare the tables looking for differences
                    for(String remoteColumnName : remoteColumns.keySet()){
                        String remoteValue = remoteColumns.get(remoteColumnName);
                        String algyValue = algyColumns.get(remoteColumnName);
                        if(remoteValue == null || algyValue==null) {
                            if (remoteValue == null && !(algyValue == null)) {
                                updateFields.put(remoteColumnName, null);
                            } else if (!(remoteValue == null)) {
                                updateFields.put(remoteColumnName, remoteValue);
                            }
                        }else if(!algyValue.equals(remoteValue)){
                            updateFields.put(remoteColumnName,remoteValue);
                        }
                    }
                    try {
                        //if any differences are found, update the row
                        if(!updateFields.isEmpty()) {
                            batch.addUpdateRowAndBatch(remoteRow);
                            logger.log("updated row to "+algyTable.tableName+" with id "+remotePrimaryKey.getKeyValue().toString());
                        }
                    }catch(Exception e){
                        e.printStackTrace();
                    }
                }
            }
            batch.executeUpdateBatch();
            batch.executeInsertBatch();
            //if the table is unsynched(that is it doesn't track changes) delete rows that are not on the remote
            if(!synched){
                for(PrimaryKey algyPrimaryKey : algyRows.keySet()){
                    if(!remoteRows.keySet().contains(algyPrimaryKey)){
                        try {
                            batch.addDeleteRowAndBatch(algyPrimaryKey);
                            logger.log("deleted row from "+algyTable.tableName);
                        }catch(SQLException e){
                            e.printStackTrace();
                        }
                    }

                }
            }
        }
        batch.executeDeleteBatch();
        System.out.println("finished synch of "+algyTable.tableName);
    }
}