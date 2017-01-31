package algernon;

import pump.*;
import security.DatabaseCredentials;
import security.EncryptedKeys;

import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;

public class AlgyPumpWhisperer {

    private ReplicationDatabaseConnection con;
    private String internalKey;
    Map<String,PreparedStatement> triggers;
    Map<String,PreparedStatement> queries;
    Map<String,String> queriesSql;
    Map<String,PreparedStatement> tables;
    Map<String,PreparedStatement> fks;
    Map<String,PreparedStatement> databases;

    private String dbName = "algernon_cloud";
    DataMap dataMap;

    public AlgyPumpWhisperer(DatabaseCredentials credentials){
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

    public synchronized Set<String> pullReplicatedTableNames() throws SQLException{
        Set<String> output = new HashSet<>();
        ResultSet rs = queries.get("pull_synched_tables").executeQuery();
        while(rs.next()){
            output.add(rs.getString(1));
        }
        return output;
    }

    public synchronized PrimaryKey getTopPk(String tableName) throws SQLException{
        Map<String,String> keyValue = new HashMap<>();
        StringBuilder builder = new StringBuilder("SELECT ");
        List<String> pkColumnNames = getPks(tableName);
        if(pkColumnNames.size()>1){
            throw new RuntimeException("attempting to perform single key update on multiple key table");
        }
        builder.append("MAX(").append(pkColumnNames.get(0)).append(") ");
        builder.append(" FROM ").append(tableName).append(";");
        PreparedStatement ps = con.createPreparedStatement(builder.toString());
        ResultSet rs = ps.executeQuery();
        rs.next();
        keyValue.put(pkColumnNames.get(0),String.valueOf(rs.getInt(1)));
        String columnName = pkColumnNames.get(0);
        Integer pkValue = Integer.valueOf(keyValue.get(columnName));
        return new SingleColumnPK(columnName,pkValue);
    }

    public synchronized String getLastUpdateDate(String tableName) throws SQLException {
        PreparedStatement lastTableUpdateDate = queries.get("last_update_date");
        lastTableUpdateDate.setString(1, tableName);
        ResultSet rs = lastTableUpdateDate.executeQuery();
        rs.next();
        return String.valueOf(rs.getString(1));
    }

    public synchronized ResultSet dumpLocalTable(String tableName, Set<PrimaryKey> matchingKeys) throws SQLException{
        StringBuilder sql = new StringBuilder("SELECT * FROM "+dbName+"."+tableName+" WHERE ");
        String matchingKeyColumnName = getPks(tableName).get(0);
        sql.append(matchingKeyColumnName).append(" IN (");
        for(PrimaryKey key : matchingKeys){
            sql.append(key.getFirstKeyValue().toString()).append(", ");
        }
        sql.delete(sql.length()-2,sql.length());
        sql.append(");");
        PreparedStatement ps = con.createPreparedStatement(sql.toString());
        return ps.executeQuery();
    }

    public synchronized List<String> getPks(String tableName) throws SQLException{
        ArrayList<String> output = new ArrayList<>();
        PreparedStatement ps = queries.get("pull_pks");
        ps.setString(1,tableName);
        ResultSet rs = ps.executeQuery();
        if(rs.isBeforeFirst()){
            while(rs.next()){
                output.add(rs.getString(1));
            }
        }else{
            throw new RuntimeException("tried to get the pk of a table with none listed");
        }
        return output;
    }

    public synchronized Header getTableHeader(String tableName) throws SQLException{
        Map<String,String> header = new HashMap<>();
        List<String> columnNames = new ArrayList<>();
        PreparedStatement ps = queries.get("table_header");
        ps.setString(1,tableName);
        ResultSet rs = ps.executeQuery();
        while(rs.next()){
            String columnName = rs.getString(1);
            String dataType = rs.getString(2);
            columnNames.add(columnName);
            header.put(columnName,dataType);
        }
        return new Header(tableName,columnNames,header);
    }

    synchronized String getEncryptedKeyByName(String keyName, EncryptedKeys keys) throws SQLException{
        PreparedStatement getKey = queries.get("get_key_by_name");
        getKey.setString(1,keyName);
        ResultSet getKeyRs = getKey.executeQuery();
        getKeyRs.next();
        String encryptedString = getKeyRs.getString(1);
        String salt = getKeyRs.getString(2);
        return keys.decryptString(encryptedString,salt,internalKey);
    }

    synchronized void removeAlgernonTableRow(Map<String,Integer> rowId, String tableName) throws SQLException{
        Integer q = 1;
        StringBuilder sql = new StringBuilder();
        sql.append("DELETE FROM ").append(tableName).append(" WHERE ");
        for(String pkName : rowId.keySet()){
            sql.append(tableName).append(".").append(pkName).append(" = ").append(rowId.get(pkName));
            if(!(rowId.keySet().size() == q)){
                sql.append(" AND ");
                q++;
            }
        }
        sql.append(";");
        con.createPreparedStatement(sql.toString()).execute();
    }

    public synchronized void finishUpdateEntry(String tableName) throws SQLException{
        Integer tableId = getTableId(tableName);
        PreparedStatement scrubAdd = queries.get("finish_synch_history");
        scrubAdd.setTimestamp(1,new Timestamp(new java.util.Date().getTime()));
        scrubAdd.setInt(2,tableId);
        scrubAdd.execute();
    }

    public synchronized String[] getOldestTable() throws SQLException{
        ResultSet rs = queries.get("get_oldest_table").executeQuery();
        rs.next();
        String [] tablePacket = new String[]{rs.getString(1),String.valueOf(rs.getInt(2))};
        startUpdateEntry(tablePacket[0]);
        return tablePacket;
    }

    private synchronized void startUpdateEntry(String tableName) throws SQLException{
        Integer tableId = getTableId(tableName);
        Timestamp startTime = new Timestamp(new java.util.Date().getTime());
        PreparedStatement scrubAdd = queries.get("start_update_entry");
        scrubAdd.setInt(1,tableId);
        scrubAdd.setTimestamp(2,startTime);
        scrubAdd.execute();
    }

    private synchronized Integer getTableId(String tableName) throws SQLException{
        PreparedStatement idGet = queries.get("get_table_id");
        idGet.setString(1,tableName);
        ResultSet tableId = idGet.executeQuery();
        tableId.next();
        return tableId.getInt(1);
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
