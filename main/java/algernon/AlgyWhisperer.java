package algernon;

import writer.Intervention;
import org.jdom2.Element;
import pump.Header;
import pump.Log;
import pump.PrimaryKey;
import pump.TableRow;
import security.DatabaseCredentials;
import security.EncryptedKeys;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class AlgyWhisperer {
    private Log log;
    private String internalKey;
    private CallableStatement getKeyStatement;
    private EncryptedKeys keys;

    public AlgyWhisperer(DatabaseCredentials credentials){
        log = new Log();
        internalKey = credentials.getInternalKey();
        ReplicationDatabaseConnection connection = new ReplicationDatabaseConnection(credentials);
        keys = new EncryptedKeys();
        try {
            getKeyStatement = connection.createCallableStatement("call algernon_cloud.getKey(?)");
        }catch(SQLException e){
            throw new ReplicationDatabaseConnectionError();
        }
    }

    public synchronized RemoteData getNewLargeTableData(String tableName,PrimaryKey primaryKey){
        RemoteData returnedData;
        String param1;
        String param2;
        String key = getEncryptedKeyByName(tableName+"_table_puller");
        String testClass = primaryKey.getClass().getSimpleName();
        if(testClass.equals("SingleColumnPK")) {
            param1 = String.valueOf(primaryKey.getKeyValue());
            param2 = "";
        }else{
            throw new RuntimeException("format of object used as primary key is unknown");
        }
        try{
            returnedData = new RemoteData(key,param1,param2,"");
        }catch(RemoteDatabaseConnectionError e){
            throw new RemoteDatabaseRecordDeletedError(param1);
        }catch(Exception e){
            throw new RemoteDatabaseConnectionError();
        }
        return returnedData;
    }

    public synchronized Map<PrimaryKey, TableRow> getModifiedLargeTableData(String tableName, Header header, List<String> pkColumnNames, List<PrimaryKey> keyChunk){
        Map<PrimaryKey, TableRow> chunk = new HashMap<>();
        String param1;
        String param2;
        String key = getEncryptedKeyByName(tableName+"_multipull");
        if(keyChunk.size()>1){
            List<Integer> collectedKeyValues = new ArrayList<>();
            for(PrimaryKey pk : keyChunk){
                collectedKeyValues.add(pk.getFirstKeyValue());
            }
            Collections.sort(collectedKeyValues);
            param1 = String.valueOf(collectedKeyValues.get(0));
            param2 = String.valueOf(collectedKeyValues.get(collectedKeyValues.size()-1));
        }else {
            Integer paramValue1 = keyChunk.get(0).getFirstKeyValue();
            param1 = String.valueOf(paramValue1);
            param2 = param1;
        }
        RemoteData data = new RemoteData(key,param1,param2,"");
        List<Element> tables = data.getTables();
        for (Element table : tables) {
            TableRow tableRow = new TableRow(header,pkColumnNames,table.getChildren());
            chunk.put(tableRow.getPrimaryKey(), tableRow);
        }
        return chunk;
    }

    public synchronized RemoteData getChangeLogData(String type, String startDate){
        String key = getEncryptedKeyByName(type+"changelog");
        return new RemoteData(key,startDate,"","");
    }

    public synchronized List<PrimaryKey> getRemotePksData(String tableName, Header header, List<String> pkColumnNames, String dateParam){
        Integer progressCount = 0;
        List<PrimaryKey> pkStack = new ArrayList<>();
        String key = tableName+"_key_puller_key";
        String keyId = getEncryptedKeyByName(key);
        RemoteData pksData = new RemoteData(keyId,"","",dateParam);
        for(Element table : pksData.getTables()){
            TableRow tableRow = new TableRow(header,pkColumnNames,table.getChildren());
            progressCount = progressCount +1;
            log.log("parsed a row from the key response for table "+tableName + "------"+progressCount+"/"+pksData.getTables().size());
            pkStack.add(tableRow.getPrimaryKey());
        }
        log.log("finished gathering remote keys for table "+tableName);
        return pkStack;
    }

    public synchronized RemoteData getRawTxPlusParents(Intervention intervention){
        String key = getEncryptedKeyByName("caseload_intervention_parents");
        return new RemoteData(key,String.valueOf(intervention.getInterventionId()),"","");
    }

    public synchronized RemoteData getTxPlusTimeData(String plannerId){
        String key = getEncryptedKeyByName("caseload_planner_times");
        return new RemoteData(key,plannerId,"","");
    }

    public synchronized RemoteData getPlannerClientId(String plannerId){
        String key = getEncryptedKeyByName("caseload_planner_clientid");
        return new RemoteData(key,plannerId,"","");
    }

    public synchronized String getTempvisitId(String plannerId){
        String key = getEncryptedKeyByName("caseload_planner_tempvisitid");
        String tempvisitId = "";
        RemoteData data = new RemoteData(key,plannerId,"","");
        if(data.getData().size()>1){
            throw new RemoteDatabaseConnectionError();
        }
        for(Map<String,String> tableRow: data.getData()){
            tempvisitId = tableRow.get("visittemp_id");
        }
        return tempvisitId;
    }

    private synchronized String getEncryptedKeyByName(String keyName) {
        String decryptedString;
        try {
            getKeyStatement.setString(1, keyName);
            ResultSet getKeyRs = getKeyStatement.executeQuery();
            getKeyRs.next();
            String encryptedString = getKeyRs.getString(1);
            String salt = getKeyRs.getString(2);
            decryptedString=  keys.decryptString(encryptedString, salt, internalKey);
        }catch (SQLException e){
            throw new ReplicationDatabaseConnectionError();
        }
        return decryptedString;
    }
}
