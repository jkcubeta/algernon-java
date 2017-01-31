package pump;

import algernon.*;
import security.DatabaseCredentials;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class ChangeLog {

    private List<String> changes;
    private List<SingleColumnPK> pkChanges;

    ChangeLog(String type, String startDate, DatabaseCredentials credentials){
        changes = new ArrayList<>();
        pkChanges = new ArrayList<>();
        AlgyPumpWhisperer algyWhisperer = new AlgyPumpWhisperer(credentials);
        AlgyWhisperer whisperer = new AlgyWhisperer(credentials);
        String pkColumnName;
        Map<String,Map<String,String>> changeLogMap;
        try {
            pkColumnName = algyWhisperer.getPks(type).get(0);
        }catch(SQLException e){
            e.printStackTrace();
            throw new ReplicationDatabaseConnectionError();
        }
        try{
            RemoteData changeLogData = whisperer.getChangeLogData(type,startDate);
            changeLogMap = changeLogData.getMapData();
        }catch (RemoteDatabaseRecordDeletedError e){
            changeLogMap = new LinkedHashMap<>();
        }catch(Exception e){
            e.printStackTrace();
            throw new RemoteDatabaseConnectionError();
        }

        for (String key : changeLogMap.keySet()){
            Map<String,String> value = changeLogMap.get(key);
            changes.add(value.get(pkColumnName));
            pkChanges.add(new SingleColumnPK(pkColumnName,Integer.valueOf(value.get(pkColumnName))));
        }
    }

    List<String> getChanges(){
        return changes;
    }

    List<SingleColumnPK> getPkChanges(){
        return pkChanges;
    }
}