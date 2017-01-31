package automation_framework;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;

class AlgyWhisperer {

    private Connection con;
    Map<String,PreparedStatement> triggers;
    Map<String,PreparedStatement> queries;
    Map<String,String> queriesSql;
    Map<String,PreparedStatement> tables;
    Map<String,PreparedStatement> fks;
    Map<String,PreparedStatement> databases;

    private String dbName;
    private DataMap dataMap;

    AlgyWhisperer(String username, String password, String remoteAddress, String dbName){
        this.dbName = dbName;
        triggers = new HashMap<>();
        queries = new HashMap<>();
        tables = new HashMap<>();
        fks = new HashMap<>();
        queriesSql = new HashMap<>();
        databases = new HashMap<>();
        dataMap = new DataMap();

        try {
            String address = "jdbc:mysql://"+remoteAddress+"?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC&generateSimpleParameterMetaData=true";
            Class.forName("com.mysql.cj.jdbc.Driver");
            con = DriverManager.getConnection(address,username,password);
        }catch(ClassNotFoundException e){
            e.printStackTrace();
        }catch(SQLException e){
            e.printStackTrace();
            throw new RuntimeException("cannot connect to remote database, check configuration and try again");
        }

        try {
            if (!sniffDatabase()) {
                createDatabase(username, password, remoteAddress, dbName);
            }else {
                con.setCatalog(dbName);
            }
        }catch(SQLException e){
            e.printStackTrace();
            throw new RuntimeException("cannot connect to replication database, check configuration and try again");
        }

        try {
            ClassLoader loader = AlgyWhisperer.class.getClassLoader();
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
                PreparedStatement statement = con.prepareStatement(query);
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

    private synchronized Boolean sniffDatabase() throws SQLException{
        String sql = "SHOW DATABASES LIKE '"+dbName+"'";
        ResultSet rs = con.prepareStatement(sql).executeQuery();
        return rs.isBeforeFirst();
    }

    synchronized Boolean sniffTrigger(String triggerName) throws SQLException{
        PreparedStatement ps = queries.get("check_trigger");
        ps.setString(1,triggerName);
        ResultSet rs = ps.executeQuery();
        return rs.isBeforeFirst();

    }

    synchronized void addTrigger(String triggerName) throws SQLException{
        triggers.get(triggerName).execute();
    }

    private synchronized void createDatabase(String username, String password, String remoteAddress, String dbName) throws SQLException{
        String sql = "CREATE DATABASE "+dbName+";";
        con.prepareStatement(sql).execute();
        con.close();
        try{
            String address = "jdbc:mysql://"+remoteAddress+"/"+dbName;
            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection(address,username,password);
            con.setCatalog(dbName);
        }catch(ClassNotFoundException e){
            e.printStackTrace();
        }catch(SQLException e){
            throw new RuntimeException("cannot connect to remote database, check configuration and try again");
        }
    }

    synchronized String getEncryptedKeyByName(String keyName, String password, EncryptedKeys keys) throws SQLException{
        PreparedStatement getKey = queries.get("get_key_by_name");
        getKey.setString(1,keyName);
        ResultSet getKeyRs = getKey.executeQuery();
        getKeyRs.next();
        String encryptedString = getKeyRs.getString(1);
        String salt = getKeyRs.getString(2);
        return keys.decryptString(encryptedString,salt,password);
    }

    synchronized Boolean  sniffTable(String tableName) throws SQLException{
        PreparedStatement ps = queries.get("check_table");
        ps.setString(1,tableName);
        ResultSet rs = ps.executeQuery();
        return rs.isBeforeFirst();
    }

    synchronized void addTable(String tableName) throws SQLException{
        tables.get(tableName).execute();
    }

    synchronized Boolean sniffFk(String fkName) throws SQLException{
        PreparedStatement ps = queries.get("check_fk");
        ps.setString(1,fkName);
        ResultSet rs = ps.executeQuery();
        return rs.isBeforeFirst();
    }

    synchronized void addFk(String fkName) throws SQLException{
        fks.get(fkName).execute();
    }

    synchronized Boolean sniffKey(String keyName) throws SQLException{
        PreparedStatement ps = queries.get("check_key");
        ps.setString(1,keyName);
        ResultSet rs = ps.executeQuery();
        return rs.isBeforeFirst();
    }

    synchronized Set<String> pullReplicatedTableNames() throws SQLException{
        Set<String> output = new HashSet<>();
        ResultSet rs = queries.get("pull_replicated_tables").executeQuery();
        while(rs.next()){
            output.add(rs.getString(1));
        }
        return output;
    }

    synchronized void addLocalKey(String keyName, String reportName, String encryptedKeyValue, String encodedSalt) throws SQLException{
        PreparedStatement ps = queries.get("add_local_key");
        ps.setString(1, keyName);
        ps.setString(2, reportName);
        ps.setString(3, encryptedKeyValue);
        ps.setString(4, encodedSalt);
        ps.execute();
    }

    synchronized void addRemoteKey(String tableName, String keyName, String reportName, String encryptedKeyValue, String encodedSalt) throws SQLException{
        PreparedStatement ps = queries.get("add_remote_key");
        Integer tableId = getTableId(tableName);
        ps.setInt(1,tableId);
        ps.setString(2, keyName);
        ps.setString(3, reportName);
        ps.setString(4, encryptedKeyValue);
        ps.setString(5, encodedSalt);
        ps.execute();
    }

    synchronized PreparedStatement startParameterizedBatch(String sql) throws SQLException{
        return con.prepareStatement(sql);
    }

    synchronized Statement startGeneralBatch() throws SQLException{
        return con.createStatement();
    }

    synchronized PreparedStatement startInsertBatch(String tableName, Map<String,String> header) throws SQLException{
        StringBuilder builder = new StringBuilder();
        String sqlStarter = "INSERT INTO " + dbName + "." + tableName + " (";
        builder.append(sqlStarter);
        for (String columnName : header.keySet()) {
            if (!(header.get(columnName) == null)) {
                builder.append(dataMap.mapName(columnName)).append(",");
            }
        }
        builder.deleteCharAt(builder.length() - 1);
        builder.append(") VALUES (");
        for (String columnName : header.keySet()) {
            if (!(header.get(columnName) == null)) {
                builder.append("?,");
            }
        }
        builder.deleteCharAt(builder.length() - 1);
        builder.append(");");
        return con.prepareStatement(builder.toString());
    }

    synchronized PreparedStatement startUpdateBatch(String tableName, Map<String,String> header, String primaryKey) throws SQLException{
        StringBuilder builder = new StringBuilder("INSERT INTO "+dbName+"."+tableName+" (");
        for(String columnName : header.keySet()){
            builder.append(columnName).append(",");
        }
        builder.delete(builder.length()-1,builder.length());
        builder.append(")").append(" VALUES (");
        for(int j=0;j<header.size();j++){
            builder.append("?,");
        }
        builder.delete(builder.length()-1,builder.length());
        builder.append(") WHERE ");
        builder.append(primaryKey).append(" = ").append("?").append(";");
        return con.prepareStatement(builder.toString());
    }

    synchronized void addUpdateBatch(Map<String,String> header, Map<String,String> addFields, PreparedStatement batch) throws SQLException{
        int q = 1;
        //assign parameters based on interpretation of string value
        for (String columnName : addFields.keySet()) {
            String dataType = header.get(dataMap.mapName(columnName));
            String fieldValue = addFields.get(dataMap.mapName(columnName));
            dataMap.mapDataType(dataType, batch, q, fieldValue);
            q++;
        }
        batch.addBatch();
    }

    synchronized void buildRemoteTableLocally(PreparedStatement logBatch, Statement createBatch, String tableName, Set<String> keyPull, Map<String,Map<String,String>> columnPull) throws SQLException{
        StringBuilder builder = new StringBuilder("CREATE TABLE "+tableName+"(");
        Boolean changeTracked = columnPull.keySet().contains("date_updated");
        for(String columnName : columnPull.keySet()){
            String dataType = columnPull.get(columnName).get("mapped_data_type");
            columnName = dataMap.mapName(columnName);
            if(!columnName.contains("controller__")) {
                builder.append(columnName).append(" ").append(dataType).append(",");
            }
        }
        if(!keyPull.isEmpty()){
            builder.append("PRIMARY KEY (");
            for (String keyName : keyPull) {
                keyName = dataMap.mapName(keyName);
                builder.append(keyName).append(",");
            }
            builder.deleteCharAt(builder.length()-1);
            builder.append(")");
        }else{
            builder.deleteCharAt(builder.length()-1);
        }
        builder.append(");");
        createBatch.addBatch(builder.toString());
        logBatch.setString(1,tableName);
        logBatch.setBoolean(2,changeTracked);
        logBatch.setBoolean(3,false);
        logBatch.addBatch();
    }

    synchronized String buildTablePuller(String tableName) throws SQLException{
        ArrayList<String> pks = getPks(tableName);
        return "SELECT * FROM " + tableName + " WHERE" + " " + pks.get(0) + " >= @param1 AND " + pks.get(0) + " <= @param2;";
    }

    synchronized String buildKeyPuller(String tableName, Boolean changeTracked) throws SQLException{
        ArrayList<String> pks = getPks(tableName);
        StringBuilder keyPuller = new StringBuilder("SELECT ");
        for(String pk : pks){
            keyPuller.append(pk).append(", ");
        }
        keyPuller.delete(keyPuller.length()-2,keyPuller.length());
        keyPuller.append(" FROM ").append(tableName);
        if(changeTracked) {
            keyPuller.append(" WHERE datediff(d,date_updated,@param3) <= 0");
        }
        keyPuller.append(";");
        return keyPuller.toString();
    }

    synchronized Map<String,Map<String,Object>> pullUnkeyedSynchedTables() throws SQLException{
        Map<String,Map<String,Object>> output = new HashMap<>();
        ResultSet rs = queries.get("pull_unkeyed_synched_tables").executeQuery();
        if(rs.isBeforeFirst()){
            while(rs.next()) {
                Map<String,Object> tableRow = new HashMap<>();
                tableRow.put("table_id",rs.getInt(1));
                tableRow.put("table_name",rs.getString(2));
                tableRow.put("is_change_tracked",rs.getBoolean(3));
                output.put(rs.getString(2),tableRow);
            }
        }
        return output;
    }

    synchronized void fireBatch(Statement batchedQueries) throws SQLException{
        batchedQueries.executeBatch();
    }

    synchronized String[] getOldestTable() throws SQLException{
        ResultSet rs = queries.get("get_oldest_table").executeQuery();
        rs.next();
        String [] tablePacket = new String[]{rs.getString(1),String.valueOf(rs.getInt(2))};
        startUpdateEntry(tablePacket[0]);
        return tablePacket;
    }

    synchronized String getLastUpdateDate(String tableName) throws SQLException{
        PreparedStatement lastTableUpdateDate = queries.get("last_update_date");
        lastTableUpdateDate.setString(1,tableName);
        ResultSet rs = lastTableUpdateDate.executeQuery();
        rs.next();
        return String.valueOf(rs.getString(1));
    }

    synchronized void updateAlgernonTableRow (Map<String,Integer> rowId, String tableName, Map<String,String> updateFields, Map<String, String> header) throws SQLException{
        Integer j = 1;
        StringBuilder builder = new StringBuilder();
        builder.append("UPDATE ").append(dbName).append(".`").append(tableName).append("` SET ");
        for(String updateField : updateFields.keySet()) {
            if (!(updateFields.get(updateField) == null)) {
                builder.append(updateField).append("=?,");
            }
        }
        builder.deleteCharAt(builder.length() - 1);
        builder.append(" WHERE ");
        for (String pkName : rowId.keySet()) {
            builder.append(tableName).append(".").append(pkName).append(" = ").append(rowId.get(pkName));
            if(!(rowId.keySet().size() == j)){
                builder.append(" AND ");
                j++;
            }
        }
        PreparedStatement updateStatement = con.prepareStatement(builder.toString());
        int q = 1;
        //assign parameters based on interpretation of string value
        for (String columnName : updateFields.keySet()) {
            String dataType = header.get(columnName);
            String fieldValue = updateFields.get(columnName);
            if (!(fieldValue == null)) {
                dataMap.mapDataType(dataType,updateStatement,q,fieldValue);
                q++;
            }
        }
        try {
            updateStatement.execute();
        }catch(Exception e){
            throw new RuntimeException("this id caused us trouble :"+ rowId.toString());
        }
    }

    synchronized PreparedStatement addBatchAlgernonTableRow(Map<String,String> header, String tableName, Map<String,String> addFields, PreparedStatement batch) throws SQLException {
        StringBuilder builder = new StringBuilder();
        String sqlStarter = "INSERT INTO " + dbName + "." + tableName + " (";
        builder.append(sqlStarter);
        for (String columnName : addFields.keySet()) {
            if (!(addFields.get(columnName) == null)) {
                builder.append(dataMap.mapName(columnName)).append(",");
            }
        }
        builder.deleteCharAt(builder.length() - 1);
        builder.append(") VALUES (");
        for (String columnName : addFields.keySet()) {
            if (!(addFields.get(columnName) == null)) {
                builder.append("?,");
            }
        }
        builder.deleteCharAt(builder.length() - 1);
        builder.append(");");
        PreparedStatement insertStatement = con.prepareStatement(builder.toString());
        int q = 1;
        //assign parameters based on interpretation of string value
        for (String columnName : addFields.keySet()) {
            String dataType = header.get(dataMap.mapName(columnName));
            String fieldValue = addFields.get(dataMap.mapName(columnName));
            if (!(fieldValue == null)) {
                try {
                    dataMap.mapDataType(dataType, insertStatement, q, fieldValue);
                    q++;
                } catch (NullPointerException e) {
                    e.printStackTrace();
                }
            }
        }
        return insertStatement;
    }

    synchronized void addAlgernonTableRow(Map<String,String> header,String tableName, Map<String,String> addFields) throws SQLException{
        StringBuilder builder = new StringBuilder();
        String sqlStarter = "INSERT INTO "+dbName+"."+tableName+" (";
        builder.append(sqlStarter);
        for(String columnName : addFields.keySet()){
            if(!(addFields.get(columnName) == null)) {
                builder.append(dataMap.mapName(columnName)).append(",");
            }
        }
        builder.deleteCharAt(builder.length()-1);
        builder.append(") VALUES (");
        for(String columnName : addFields.keySet()){
            if(!(addFields.get(columnName) == null)) {
                builder.append("?,");
            }
        }
        builder.deleteCharAt(builder.length()-1);
        builder.append(");");
        PreparedStatement insertStatement = con.prepareStatement(builder.toString());
        int q = 1;
        //assign parameters based on interpretation of string value
        for (String columnName : addFields.keySet()) {
            String dataType = header.get(dataMap.mapName(columnName));
            String fieldValue = addFields.get(dataMap.mapName(columnName));
            if (!(fieldValue == null)) {
                try {
                    dataMap.mapDataType(dataType, insertStatement, q, fieldValue);
                    q++;
                }catch(NullPointerException e){
                    e.printStackTrace();
                }
            }
        }
        insertStatement.execute();
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
        con.prepareStatement(sql.toString()).execute();
    }

    private synchronized void startUpdateEntry(String tableName) throws SQLException{
        Integer tableId = getTableId(tableName);
        Timestamp startTime = new Timestamp(new java.util.Date().getTime());
        PreparedStatement scrubAdd = queries.get("start_update_entry");
        scrubAdd.setInt(1,tableId);
        scrubAdd.setTimestamp(2,startTime);
        scrubAdd.execute();
    }

    synchronized void finishUpdateEntry(String tableName) throws SQLException{
        Integer tableId = getTableId(tableName);
        PreparedStatement scrubAdd = con.prepareStatement("UPDATE synch_history SET end_time = ? WHERE table_id = ? ORDER BY scrub_date DESC LIMIT 1;");
        scrubAdd.setTimestamp(1,new Timestamp(new java.util.Date().getTime()));
        scrubAdd.setInt(2,tableId);
        scrubAdd.execute();
    }

    synchronized void createTemporaryTable(String tableName) throws SQLException{
        String ps = queriesSql.get("build_temporary_table");
        ps = ps.replace("&tableName1","temp_"+tableName);
        ps = ps.replace("&tableName2",tableName);
        con.prepareStatement(ps).execute();
    }

    synchronized ResultSet dumpLocalTable(String tableName, Set<Map<String,Integer>> matchingKeys) throws SQLException{
        StringBuilder sql = new StringBuilder("SELECT * FROM "+dbName+"."+tableName+" WHERE ");
        String matchingKeyColumnName = getPks(tableName).get(0);
        sql.append(matchingKeyColumnName).append(" IN (");
        for(Map<String,Integer> key : matchingKeys){
            sql.append(key.get(matchingKeyColumnName)).append(", ");
        }
        sql.delete(sql.length()-2,sql.length());
        sql.append(");");
        PreparedStatement ps = con.prepareStatement(sql.toString());
        //if(isChangeTracked(tableName)){
            /*String sql = queriesSql.get("dump_local_tracked_table");
            sql = sql.replace("&tableName", tableName);
            ps = con.prepareStatement(sql);
            String pk = getPks(tableName).get(0);
            ps.setString(1,pk);
            ps.setArray(2,con.createArrayOf("INTEGER",matchingKeys.toArray()));
        //}else {
          //  String sql = queriesSql.get("dump_local_table");
            //sql = sql.replace("&tableName", tableName);
            //ps = con.prepareStatement(sql);
        //}*/
        return ps.executeQuery();
    }

    private synchronized Boolean isChangeTracked(String tableName) throws SQLException{
        PreparedStatement ps = queries.get("is_change_tracked");
        ps.setString(1,tableName);
        ResultSet rs = ps.executeQuery();
        rs.next();
        return rs.getBoolean(1);
    }

    private synchronized Integer getTableId(String tableName) throws SQLException{
        PreparedStatement idGet = queries.get("get_table_id");
        idGet.setString(1,tableName);
        ResultSet tableId = idGet.executeQuery();
        tableId.next();
        return tableId.getInt(1);
    }

    synchronized ArrayList<String> getPks(String tableName) throws SQLException{
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

    synchronized Map<String,String> getTableHeader(String tableName) throws SQLException{
        Map<String,String> header = new HashMap<>();
        PreparedStatement ps = queries.get("table_header");
        ps.setString(1,tableName);
        ResultSet rs = ps.executeQuery();
        while(rs.next()){
            String columnName = rs.getString(1);
            String dataType = rs.getString(2);
            header.put(columnName,dataType);
        }
        return header;
    }

    synchronized ArrayList<Map<String,String>> getEligibilityBatch() throws SQLException{
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

    synchronized ArrayList<Map<String,String>> getAllEligibility(){
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

    synchronized void addScrubEntry(HtmlOutputRecord results) throws SQLException{
        PreparedStatement ps = queries.get("add_scrub_entry");
        ps.setString(1,results.getTableHtml());
        ps.setTimestamp(2, new Timestamp(new java.util.Date().getTime()));
    }

    synchronized List<PrimaryKey> getAllPks(String tableName) throws SQLException{
        ArrayList<String> pks = getPks(tableName);
        Integer pkCount = pks.size();
        List<PrimaryKey> returnedKeys = new ArrayList<>();
        String keyPuller = buildKeyPuller(tableName,false);
        PreparedStatement ps = con.prepareStatement(keyPuller);
        ResultSet rs = ps.executeQuery();
        while(rs.next()){
            Map<String,String> keyValues = new HashMap<>();
            for(int q = 0; q<pkCount; q++){
                keyValues.put(pks.get(q),String.valueOf(rs.getInt(q)));
            }
            PrimaryKey pk = new PrimaryKey(tableName,pks,keyValues);
            returnedKeys.add(pk);
        }
        return returnedKeys;
    }

    synchronized PrimaryKey getTopPk(String tableName) throws SQLException{
        Map<String,String> keyValue = new HashMap<>();
        StringBuilder builder = new StringBuilder("SELECT ");
        List<String> pkColumnNames = getPks(tableName);
        if(pkColumnNames.size()>1){
            throw new RuntimeException("attempting to perform single key update on multiple key table");
        }
        builder.append("MAX(").append(pkColumnNames.get(0)).append(") ");
        builder.append(" FROM ").append(tableName).append(";");
        PreparedStatement ps = con.prepareStatement(builder.toString());
        ResultSet rs = ps.executeQuery();
        rs.next();
        keyValue.put(pkColumnNames.get(0),String.valueOf(rs.getInt(1)));
        return new PrimaryKey(tableName,pkColumnNames,keyValue);
    }

    synchronized PreparedStatement addInsertRowToBatch(PreparedStatement batch, Map<String,String> header, TableRow row) throws SQLException{
        Map<String,String> rowData = row.rowData;
        Integer q = 0;
        for(String columnName : header.keySet()){
            dataMap.mapDataType(header.get(columnName),batch,q,rowData.get(columnName));
            q++;
        }
        return batch;
    }

    synchronized PreparedStatement addUpdateRowToBatch(PreparedStatement batch, Map<String,String> header, TableRow row) throws SQLException{
        Map<String,String> rowData = row.rowData;
        Integer q = 0;
        for(String columnName : header.keySet()){
            dataMap.mapDataType(header.get(columnName),batch,q,rowData.get(columnName));
            q++;
        }
        Map<String,Integer> pk = row.primaryKey;
        Integer pkValue = 0;
        if(pk.size()>1){
            throw new RuntimeException("large table currently limited to single PK tables");
        }else{
            for(String columnName : pk.keySet()){
                pkValue = pk.get(columnName);
            }
        }
        batch.setInt(q,pkValue);
        return batch;
    }

}
