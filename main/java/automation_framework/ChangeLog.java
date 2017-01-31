package automation_framework;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class ChangeLog {
    private String type;
    private List<String> changes;

    ChangeLog(String type, String startDate){
        changes = new ArrayList<>();
        AlgyWhisperer algyWhisperer = new AlgyWhisperer(Robot.dbUsername,Robot.dbPassword,Robot.address,Robot.dbName);
        String encryptedChangeLogKey;
        Map<String,Map<String,String>> changeLogMap;
        try{
            encryptedChangeLogKey = algyWhisperer.getEncryptedKeyByName(type+"_changelog",Robot.internalKey,new EncryptedKeys());
            Data changeLogData = new Data(encryptedChangeLogKey, startDate, null);
            changeLogMap = changeLogData.getMapData();
        }catch(SQLException e){
            e.printStackTrace();
            throw new ReplicationDatabaseConnectionError();
        }catch(Exception e){
            e.printStackTrace();
            throw new RemoteDatabaseConnectionError();
        }

        for (String key : changeLogMap.keySet()){
            Map<String,String> value = changeLogMap.get(key);
            changes.add(value.get("clientvisit_id"));
        }
    }

    List<String> getChanges(){
        return changes;
    }
}