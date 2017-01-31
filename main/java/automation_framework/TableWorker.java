package automation_framework;

import java.sql.SQLException;
import java.util.Map;
import java.util.TreeMap;

class TableWorker implements Runnable{
    private Log logger;
	@Override
	public void run(){
        logger = new Log();
	    String oldestTableName;
        Boolean changeTracked;
        Table algyTable;
        CredibleTable remoteTable;
		try {
            String[] oldestTablePacket = Robot.algyWhisperer.getOldestTable();
            oldestTableName = oldestTablePacket[0];
            System.out.println("starting synch of "+oldestTableName);
            logger.log("starting synch of "+oldestTableName);
            changeTracked = oldestTablePacket[1].equals("1");
            logger.log("getting remote version of "+oldestTableName);
            if(oldestTableName.equals("ClientVisit")){
                remoteTable = new LargeTable(oldestTablePacket[0]);
            }else {
                remoteTable = new Table(oldestTableName, changeTracked);
            }
            logger.log("getting algy version of "+oldestTableName);
            if(changeTracked && remoteTable.getTableRows().isEmpty()){
                logger.log("didn't bother to get algy version of "+oldestTableName+", as the remote version shows no changes");
            }else if(oldestTableName.equals("ClientVisit")) {
                logger.log("didn't bother to get algy version of "+oldestTableName+", as large tables are synched independently");
            }else{
                algyTable = new Table(oldestTableName, remoteTable.getTableRows().keySet());
                compare(algyTable, remoteTable, changeTracked);
            }
            Robot.algyWhisperer.finishUpdateEntry(remoteTable.getTableName());
        }catch(Exception e){
			e.printStackTrace();
		}
    }

    private void compare(Table algyTable,CredibleTable remoteTable, Boolean synched) throws SQLException{
        logger.log("starting comparison of "+algyTable.tableName);
        Map<Map<String,Integer>,TableRow> algyRows = algyTable.getTableRows();
        Map<Map<String,Integer>,TableRow> remoteRows = remoteTable.getTableRows();
        if(!algyRows.equals(remoteRows)){
            //check for changes in remote fields or new remote rows
            for(Map<String,Integer> remotePrimaryKey : remoteRows.keySet()) {
                TableRow algyRow = algyRows.get(remotePrimaryKey);
                TableRow remoteRow = remoteRows.get(remotePrimaryKey);
                //row not found in algernon, add it from the remote server
                if(algyRow == null){
                    try {
                        Robot.algyWhisperer.addAlgernonTableRow(algyTable.header,remoteTable.getTableName(), remoteRow.rowData);
                        logger.log("added row to "+algyTable.tableName+" with id "+remotePrimaryKey.toString());
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
                            Robot.algyWhisperer.updateAlgernonTableRow(remotePrimaryKey, remoteTable.getTableName(), updateFields,remoteTable.getHeader());
                            logger.log("updated row to "+algyTable.tableName+" with id "+remotePrimaryKey.toString());
                        }
                    }catch(Exception e){
                        e.printStackTrace();
                    }
                }
            }
            //if the table is unsynched(that is it doesn't track changes) delete rows that are not on the remote
            if(!synched){
                for(Map<String,Integer> algyPrimaryKey : algyRows.keySet()){
                    if(!remoteRows.keySet().contains(algyPrimaryKey)){
                        try {
                            Robot.algyWhisperer.removeAlgernonTableRow(algyPrimaryKey, remoteTable.getTableName());
                            logger.log("deleted row from "+algyTable.tableName);
                        }catch(SQLException e){
                            e.printStackTrace();
                        }
                    }

                }
            }
        }
        System.out.println("finished synch of "+algyTable.tableName);
    }
}