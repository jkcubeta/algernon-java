package algernon;

import writer.Intervention;
import security.DatabaseCredentials;
import security.EncryptedKeys;

import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class AlgyRobotWhisperer {

    private ReplicationDatabaseConnection con;
    private String internalKey;
    Map<String,PreparedStatement> triggers;
    Map<String,PreparedStatement> queries;
    Map<String,String> queriesSql;
    Map<String,PreparedStatement> tables;
    Map<String,PreparedStatement> fks;
    Map<String,PreparedStatement> databases;

    private String dbName = "algernon_test";

    public AlgyRobotWhisperer(DatabaseCredentials credentials){
        this.internalKey = credentials.getInternalKey();
        this.con = new ReplicationDatabaseConnection(credentials);
        triggers = new HashMap<>();
        this.queries = new HashMap<>();
        tables = new HashMap<>();
        fks = new HashMap<>();
        queriesSql = new HashMap<>();
        databases = new HashMap<>();

        buildPreparedStatements();
    }

    public synchronized String getRawPresentation(String plannerId) throws SQLException{
        StringBuilder presentationBuilder = new StringBuilder();
        PreparedStatement ps = con.createPreparedStatement("call algernon_test.getStoredPresentationByPlannerId(?)");
        ps.setInt(1,Integer.valueOf(plannerId));
        ResultSet rs = ps.executeQuery();
        while(rs.next()){
            presentationBuilder.append(rs.getString("presentation"));
        }
        return presentationBuilder.toString();
    }

    public synchronized List<String> getPendingNotes(String empId) throws SQLException{
        List<String> returnedPlannerIds = new ArrayList<>();
        PreparedStatement ps = con.createPreparedStatement("call algernon_test.getPendingNotes(?)");
        ps.setInt(1,Integer.valueOf(empId));
        ResultSet rs = ps.executeQuery();
        while(rs.next()){
            returnedPlannerIds.add(rs.getString("plan_id"));
        }
        return returnedPlannerIds;
    }

    public synchronized void closeNote(String noteId) throws SQLException{
        PreparedStatement ps = con.createPreparedStatement("call algernon_test.closeNote(?)");
        ps.setInt(1,Integer.valueOf(noteId));
        ps.execute();
    }

    public synchronized List<Intervention> getRawInterventions(String plannerId) throws SQLException{
        List<Intervention> rawInterventions = new ArrayList<>();
        PreparedStatement ps = con.createPreparedStatement("call algernon_test.getInterventions(?)");
        ps.setInt(1,Integer.valueOf(plannerId));
        ResultSet rs = ps.executeQuery();
        while(rs.next()){
            Integer interventionId = rs.getInt("intervention_id");
            Integer duration = rs.getInt("duration");
            String description = rs.getString("description");
            String response = rs.getString("response");
            Intervention intervention = new Intervention(interventionId,duration,description,response);
            rawInterventions.add(intervention);
        }
        return rawInterventions;
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

}
