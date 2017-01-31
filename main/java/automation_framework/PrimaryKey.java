package automation_framework;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PrimaryKey {
    private String tableName;
    private List<String> pkColumnNames;
    private Map<String,String> keyValue;

    PrimaryKey(String tableName, List<String> pkColumnNames, Map<String,String> keyValue){
        this.tableName = tableName;
        this.pkColumnNames = pkColumnNames;
        this.keyValue = keyValue;
    }

    String getTableName(){
        return tableName;
    }

    List<String> getPkColumnNames(){
        return pkColumnNames;
    }

    Map<String,String> getKeyValue(){
        return keyValue;
    }

    String getSingleKeyValue(){
        if(keyValue.size()>1){
            throw new RuntimeException("can't get single key value from multi-key table");
        }else{
            return keyValue.get(pkColumnNames.get(0));
        }
    }

    Map<String,Integer> getKeyValueStrict(){
        Map<String,Integer> strictKey = new HashMap<>();
        for(String key : keyValue.keySet()){
            strictKey.put(key,Integer.valueOf(keyValue.get(key)));
        }
        return strictKey;
    }

}
