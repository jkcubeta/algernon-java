package algernon;

import eligibility.DataMap;
import security.DatabaseCredentials;
import security.EncryptedKeys;

import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class AlgyEligibilityWhisperer {

    private ReplicationDatabaseConnection con;
    private String internalKey;
    Map<String,PreparedStatement> triggers;
    Map<String,PreparedStatement> queries;
    Map<String,String> queriesSql;
    Map<String,PreparedStatement> tables;
    Map<String,PreparedStatement> fks;
    Map<String,PreparedStatement> databases;

    private String dbName = "algernon_cloud";
    private DataMap dataMap;

    public AlgyEligibilityWhisperer(DatabaseCredentials credentials){
        this.internalKey = credentials.getInternalKey();
        this.con = new ReplicationDatabaseConnection(credentials);
        triggers = new HashMap<>();
        this.queries = new HashMap<>();
        tables = new HashMap<>();
        fks = new HashMap<>();
        queriesSql = new HashMap<>();
        databases = new HashMap<>();
        dataMap = new DataMap();

        buildPreparedStatements();
    }

    public synchronized String getEncryptedKeyByName(String keyName, EncryptedKeys keys) throws SQLException{
        PreparedStatement getKey = queries.get("get_key_by_name");
        getKey.setString(1,keyName);
        ResultSet getKeyRs = getKey.executeQuery();
        getKeyRs.next();
        String encryptedString = getKeyRs.getString(1);
        String salt = getKeyRs.getString(2);
        return keys.decryptString(encryptedString,salt,internalKey);
    }

    private synchronized void buildPreparedStatements(){
        try {
            ClassLoader loader = this.getClass().getClassLoader();
            InputStream in = loader.getResourceAsStream("algy_local.properties");
            Properties properties = new Properties();
            properties.load(in);
            for(Object storedQueryKey : properties.keySet()) {
                String storedQueryName = (String) storedQueryKey;
                String type = storedQueryName.split("_")[0];
                String name = storedQueryName.substring(type.length()+1);
                String query = (String) properties.get(storedQueryKey);
                query = query.replace("@dbName", dbName);
                query = query.replace("@dbName2", "algernon");
                queriesSql.put(name,query);
                PreparedStatement statement = con.createPreparedStatement(query);
                switch (type) {
                    case "table":
                        tables.put(name, statement);
                        break;
                    case "sniffer":
                        queries.put(name, statement);
                        break;
                    case "FKS":
                        fks.put(name, statement);
                        break;
                    case "trigger":
                        triggers.put(name, statement);
                        break;
                    case "database":
                        databases.put(name,statement);
                        break;
                }
            }
            in.close();
        }catch(IOException e) {
            throw new RuntimeException("troubles loading the tables file, check the rsc folder to confirm");
        }catch(SQLException e) {
            throw new RuntimeException("cannot connect to replication database, check configuration and try again");
        }
    }

    public synchronized PreparedStatement buildScrubHistoryInsert() throws SQLException{
        return con.createPreparedStatement("call algernon_cloud.insertEligibility(?,?,?,?);");
    }

    public synchronized ArrayList<Map<String,String>> getAllEligibility(){
        ArrayList<Map<String, String>> output = new ArrayList<>();
        try {
            ResultSet rs = queries.get("get_all_eligibility_clients").executeQuery();
            while (rs.next()) {
                Map<String, String> outputRecord = new HashMap<>();
                outputRecord.put("credible_id", String.valueOf(rs.getInt(1)));
                outputRecord.put("medicaid_id", String.valueOf(rs.getInt(2)));
                output.add(outputRecord);
            }
        }catch(SQLException e){
            throw new RuntimeException("can't connect to replication database, check connections and try again");
        }
        return output;
    }

    public synchronized ArrayList<Map<String,String>> getEligibilityBatch() throws SQLException{
        ArrayList<Map<String, String>> output = new ArrayList<>();
        ResultSet rs = queries.get("get_oldest_client_batch").executeQuery();
        while(rs.next()){
            Map<String,String> outputRecord = new HashMap<>();
            outputRecord.put("credible_id",String.valueOf(rs.getInt(1)));
            outputRecord.put("medicaid_id",String.valueOf(rs.getInt(2)));
            output.add(outputRecord);
        }
        return output;
    }

}
