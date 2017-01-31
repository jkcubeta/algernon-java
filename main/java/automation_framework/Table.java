package automation_framework;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

class Table implements CredibleTable{

    Log log;
    private AlgyWhisperer algyWhisperer;
    static ExecutorService tableBoss;
    String tableName;
    private ArrayList<String> pkColumnNames;
    private Map<Map<String,Integer>, TableRow> tableData;
    static Integer tableChunkCount;
    final AtomicInteger count = new AtomicInteger(1);
    Map<String, String> header;

    Table(String tableName, Set<Map<String,Integer>>foreignKeys) {
        log = new Log();
        this.tableName = tableName;
        this.tableData = new HashMap<>();
        this.algyWhisperer = new AlgyWhisperer(Robot.dbUsername,Robot.dbPassword,Robot.address,Robot.dbName);
        try {
            pkColumnNames = algyWhisperer.getPks(tableName);
            header = algyWhisperer.getTableHeader(tableName);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            ResultSet algyTable = algyWhisperer.dumpLocalTable(tableName,foreignKeys);
            if(algyTable.isBeforeFirst()) {
                while (algyTable.next()) {
                    TableRow algyRow = new TableRow(algyTable, pkColumnNames);
                    tableData.put(algyRow.primaryKey, algyRow);
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        log.log("got algy version of "+tableName);
    }

    Table(String tableName, Boolean synched) {
        Log log = new Log();
        Integer numChunkWorkers = 8;
        List<Map<String,String>> tableRows;
        tableBoss = Executors.newFixedThreadPool(numChunkWorkers);
        String keyId;
        List<ChunkWorker> workers = new ArrayList<>();
        Queue<List<Map<String,String>>> stackedKeyChunks;
        this.algyWhisperer = new AlgyWhisperer(Robot.dbUsername,Robot.dbPassword,Robot.address,Robot.dbName);
        this.log = new Log();
        this.tableName = tableName;
        this.tableData = new HashMap<>();
        try {
            this.header = algyWhisperer.getTableHeader(tableName);
            this.pkColumnNames = algyWhisperer.getPks(tableName);
            keyId = algyWhisperer.getEncryptedKeyByName(tableName + "_table_puller",Robot.internalKey,Robot.keys);
            List<Map<String,String>> remotePks = getRemotePks(tableName,synched);
            tableRows = remotePks;
            if(!remotePks.isEmpty()) {
                stackedKeyChunks = chunkKeySet(remotePks);
                tableChunkCount = stackedKeyChunks.size();
                log.log("this table "+tableName+" has "+tableChunkCount+" chunks (chunk = 10000 row items)) in it");
            }else{
                stackedKeyChunks = new LinkedBlockingQueue<>();
                tableChunkCount = 0;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("can't connect to replication database, check connections and try again");
        } catch (Exception e){
            e.printStackTrace();
            throw new RuntimeException("this is embarrassing, but it seems we have an unknown error, consult the stacktrace and give it a shot");
        }
            for (List<Map<String, String>> keyChunk : stackedKeyChunks) {
                workers.add(new ChunkWorker(count, tableChunkCount, keyId, keyChunk, pkColumnNames, header));
            }
            try {
                List<Future<Map<Map<String, Integer>, TableRow>>> pendingTable = tableBoss.invokeAll(workers, 2, TimeUnit.HOURS);
                for (Future<Map<Map<String, Integer>, TableRow>> chunk : pendingTable) {
                    tableData.putAll(chunk.get());
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        if(!(tableData.size() == tableRows.size())){
            log.log("this table "+tableName+" done fucked up. Can't guarantee we got all the data, got "+tableData.size()+"rows out of "+tableRows.size());
        }
        log.log("got remote version of "+tableName);
    }

    private void checkWork(List<Future<HashMap<HashMap<String,Integer>,TableRow>>> pendingResults){
        do{
            for(Future<HashMap<HashMap<String,Integer>,TableRow>> pendingResult : pendingResults) {
                if (!(pendingResult == null)) {
                    if(pendingResult.isDone()) {
                        try {
                            PreparedStatement addStatement = algyWhisperer.startInsertBatch("temp_"+tableName,header);
                            HashMap<HashMap<String,Integer>,TableRow> results = pendingResult.get();
                            for(HashMap<String,Integer> primaryKey : results.keySet()){
                                TableRow tableRow = results.get(primaryKey);
                                algyWhisperer.addUpdateBatch(header,tableRow.rowData,addStatement);
                            }
                            addStatement.executeBatch();
                        } catch (Exception e) {
                            e.printStackTrace();
                            throw new RuntimeException("something went terribly wrong and I don't know what is");
                        }
                    }
                }
            }
        }while(!checkWorkCompletion(pendingResults));
    }

    synchronized void waitForWork(Boolean workDone){
        while(!workDone){
            try{
                wait();
            }catch(InterruptedException e){
                e.printStackTrace();
            }
        }
    }

    private Queue<List<Map<String,String>>> chunkKeySet(List<Map<String,String>> stackedKeys){
        Queue<List<Map<String,String>>> stackedKeyChunks = new ConcurrentLinkedQueue<>();
        Integer first = Integer.valueOf(stackedKeys.get(0).get(pkColumnNames.get(0)));
        Integer last = Integer.valueOf(stackedKeys.get(stackedKeys.size()-1).get(pkColumnNames.get(0)));
        if(stackedKeys.size() >10000){
            if(last-first > 10000) {
                Integer keyOne;
                Integer keyTwo;
                Integer q = 0;
                while (q < stackedKeys.size()) {
                    List<Map<String, String>> chunk = new ArrayList<>();
                    try {
                        chunk.add(stackedKeys.get(q));
                        keyOne = Integer.valueOf(stackedKeys.get(q).get(pkColumnNames.get(0)));
                        keyTwo = Integer.valueOf(stackedKeys.get(q + 1).get(pkColumnNames.get(0)));
                        while (keyTwo - keyOne < 10000) {
                            chunk.add(stackedKeys.get(q + 1));
                            q = q + 1;
                            keyTwo = Integer.valueOf(stackedKeys.get(q + 1).get(pkColumnNames.get(0)));
                        }
                        chunk.add(stackedKeys.get(q + 1));
                        q = q + 1;
                        stackedKeyChunks.add(chunk);
                    }catch(IndexOutOfBoundsException e){
                        chunk.add(stackedKeys.get(q));
                        break;
                    }
                }
            }else {
                Integer q = 0;
                Integer numChunks = stackedKeys.size() / 10000;
                for (Integer j = 0; j < numChunks; j++) {
                    List<Map<String, String>> keyChunk = stackedKeys.subList(q, q + 10000);
                    stackedKeyChunks.add(keyChunk);
                    q = q + 10000;
                }
                List<Map<String, String>> keyChunk = stackedKeys.subList(q, stackedKeys.size());
                stackedKeyChunks.add(keyChunk);
            }
        }else {
            if(last-first > 10000) {
                Integer keyOne;
                Integer keyTwo;
                Integer q = 0;
                while (q < stackedKeys.size()) {
                    List<Map<String, String>> chunk = new ArrayList<>();
                    try {
                        chunk.add(stackedKeys.get(q));
                        keyOne = Integer.valueOf(stackedKeys.get(q).get(pkColumnNames.get(0)));
                        keyTwo = Integer.valueOf(stackedKeys.get(q + 1).get(pkColumnNames.get(0)));
                        while (keyTwo - keyOne < 10000) {
                            chunk.add(stackedKeys.get(q + 1));
                            q = q + 1;
                            keyTwo = Integer.valueOf(stackedKeys.get(q + 1).get(pkColumnNames.get(0)));
                        }
                        chunk.add(stackedKeys.get(q + 1));
                        q = q + 1;
                        stackedKeyChunks.add(chunk);
                    }catch(IndexOutOfBoundsException e){
                        chunk.add(stackedKeys.get(q));
                        break;
                    }
                }
            }else {
                List<Map<String, String>> keyChunk = stackedKeys.subList(0, stackedKeys.size());
                stackedKeyChunks.add(keyChunk);
            }
        }
        return stackedKeyChunks;
    }

    private Boolean checkWorkCompletion(List<Future<HashMap<HashMap<String,Integer>,TableRow>>> pendingResults){
        Boolean workDone = true;
        for(Future<HashMap<HashMap<String,Integer>,TableRow>> result : pendingResults){
            if(!result.isDone()){
                workDone = false;
            }
        }
        return workDone;
    }

    private Map<Map<String,Integer>, TableRow> getTableChunk(String keyId, Integer startValue, Integer endValue, String primeDate) throws Exception{
        Map<Map<String,Integer>, TableRow> chunk = new HashMap<>();
        URI uri = null;
        try {
            if(startValue == 0) {
                uri = new URIBuilder()
                        .setScheme("http")
                        .setHost("reportservices.crediblebh.com")
                        .setPath("/reports/ExportService.asmx/ExportXML")
                        .setParameter("connection", keyId)
                        .setParameter("start_date", "")
                        .setParameter("end_date", "")
                        .setParameter("custom_param1", "")
                        .setParameter("custom_param2", "")
                        .setParameter("custom_param3", "")
                        .build();
            }else{
                uri = new URIBuilder()
                        .setScheme("http")
                        .setHost("reportservices.crediblebh.com")
                        .setPath("/reports/ExportService.asmx/ExportXML")
                        .setParameter("connection", keyId)
                        .setParameter("start_date", "")
                        .setParameter("end_date", "")
                        .setParameter("custom_param1", String.valueOf(startValue))
                        .setParameter("custom_param2", String.valueOf(endValue))
                        .setParameter("custom_param3", primeDate.split(" ")[0])
                        .build();
            }
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        HttpGet httpGet = new HttpGet(uri);
        CloseableHttpClient httpclient = HttpClients.createDefault();
        CloseableHttpResponse response1 = null;
        try {
            response1 = httpclient.execute(httpGet);
        } catch (IOException e) {
            e.printStackTrace();
        }
        HttpEntity entity1 = response1.getEntity();
        SAXBuilder saxBuilder = new SAXBuilder();
        Document document;
        Document document2 = null;
        try {
            document = saxBuilder.build(entity1.getContent());
        } catch (IOException | UnsupportedOperationException | JDOMException e) {
            throw new Exception("credible server down, again...");
        }
        Element root = document.getRootElement();
        try {
            document2 = saxBuilder.build(new StringReader(root.getText()));
        } catch (IOException | UnsupportedOperationException | JDOMException e) {
            e.printStackTrace();
        }
        Element root2 = document2.getRootElement();
        List<Element> children = root2.getChildren();
        for (Element child : children) {
            TableRow tableRow = new TableRow(child.getChildren(),pkColumnNames,header);
            chunk.put(tableRow.primaryKey, tableRow);
        }
        System.out.println("gathered remote row chunk");
        return chunk;
    }

    private ArrayList<Map<String,String>> getRemotePks(String tableName, Boolean synched) throws Exception{
        String primeDate;
        ArrayList<Map<String,String>> pkStack = new ArrayList<>();
        if(synched) {
            primeDate = algyWhisperer.getLastUpdateDate(tableName);
        }else{
            primeDate = "no date";
        }
        String keyPullKey = algyWhisperer.getEncryptedKeyByName(tableName+"_key_puller_key",Robot.internalKey,Robot.keys);
        pkColumnNames = algyWhisperer.getPks(tableName);
        String dateParam;
        if(primeDate.equals("no date")){
            dateParam = "";
        }else{
            dateParam = primeDate.replace(" ","T");
        }
        URI uri = new URIBuilder()
                .setScheme("http")
                .setHost("reportservices.crediblebh.com")
                .setPath("/reports/ExportService.asmx/ExportXML")
                .setParameter("connection", keyPullKey)
                .setParameter("start_date", "")
                .setParameter("end_date", "")
                .setParameter("custom_param1", "")
                .setParameter("custom_param2", "")
                .setParameter("custom_param3", dateParam)
                .build();
        HttpGet httpPost = new HttpGet(uri);
        CloseableHttpClient httpclient = HttpClients.createDefault();
        CloseableHttpResponse response;
        HttpEntity entity;
        try {
            log.log("starting the key pull for table "+tableName);
            response = httpclient.execute(httpPost);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("IOException while building primary key query for table "+tableName);
        }
        if (response != null) {
            entity = response.getEntity();
        }else{
            throw new RuntimeException("empty result set returned, check query");
        }
        log.log("extracted the data for the key pull of table "+tableName+ "from remote source");
        SAXBuilder saxBuilder = new SAXBuilder();
        Document document2;
        try {
            Document document = saxBuilder.build(entity.getContent());
            Element root = document.getRootElement();
            document2 = saxBuilder.build(new StringReader(root.getText()));
        } catch (IOException | UnsupportedOperationException | JDOMException e) {
            e.printStackTrace();
            throw new Exception("credible server down, again...");
        }
        Element dataSet = document2.getRootElement();
        List<Element> tables = dataSet.getChildren();
        Integer rowCount = tables.size();
        Integer progressCount = 0;
        log.log("table "+tableName+" has " + rowCount + " rows in it");
        for(Element table : tables){
            HashMap<String,String> rowData = new HashMap<>();
            for(Element row : table.getChildren()) {
                String value = row.getContent().get(0).getValue();
                String columnName = row.getName();
                rowData.put(columnName, value);
            }
            progressCount = progressCount +1;
            log.log("parsed a row from the key response for table "+tableName + "------"+progressCount+"/"+rowCount);
            pkStack.add(rowData);
        }
        log.log("finished gathering remote keys for table "+tableName);
        return pkStack;
    }

    @Override
    public Map<Map<String, Integer>, TableRow> getTableRows() {
        return tableData;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public Map<String,String> getHeader() {
        return header;
    }
}

class ChunkWorker implements Callable<Map<Map<String,Integer>,TableRow>>{
        private AtomicInteger chunkCount;
        private Integer tableChunkCount;
        private String keyId;
        private ArrayList<String> pkColumnNames;
        private Map<String,String> header;
        private List<Map<String,String>> keyChunk;

        ChunkWorker(AtomicInteger chunkCount, Integer tableChunkCount, String keyId, List<Map<String,String>> keyChunk, ArrayList<String> pkColumnNames,Map<String,String> header){
            this.chunkCount = chunkCount;
            this.tableChunkCount = tableChunkCount;
            this.keyId = keyId;
            this.pkColumnNames = pkColumnNames;
            this.header = header;
            this.keyChunk = keyChunk;
        }

        @Override
        public Map<Map<String, Integer>, TableRow> call() throws Exception {
            Log logger = new Log();
            Runtime run = Runtime.getRuntime();
            String startValue = keyChunk.get(0).get(pkColumnNames.get(0));
            String endValue = keyChunk.get(keyChunk.size() - 1).get(pkColumnNames.get(0));
            Map<Map<String, Integer>, TableRow> chunk = new HashMap<>();
            URI uri = null;
            try {
                uri = new URIBuilder()
                        .setScheme("http")
                        .setHost("reportservices.crediblebh.com")
                        .setPath("/reports/ExportService.asmx/ExportXML")
                        .setParameter("connection", keyId)
                        .setParameter("start_date", "")
                        .setParameter("end_date", "")
                        .setParameter("custom_param1", startValue)
                        .setParameter("custom_param2", endValue)
                        .setParameter("custom_param3", "")
                        .build();
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }
            HttpGet httpGet = new HttpGet(uri);
            CloseableHttpClient httpclient = HttpClients.createDefault();
            CloseableHttpResponse response1 = null;
            try {
                response1 = httpclient.execute(httpGet);
            } catch (IOException e) {
                e.printStackTrace();
            }
            HttpEntity entity1 = response1.getEntity();
            SAXBuilder saxBuilder = new SAXBuilder();
            Document document;
            Document document2 = null;
            try {
                document = saxBuilder.build(entity1.getContent());
            } catch (IOException | UnsupportedOperationException | JDOMException e) {
                e.printStackTrace();
                throw new Exception("credible server down, again...");
            }
            Element root = document.getRootElement();
            try {
                document2 = saxBuilder.build(new StringReader(root.getText()));
            } catch (IOException | UnsupportedOperationException | JDOMException e) {
                e.printStackTrace();
            }
            Element root2 = document2.getRootElement();
            List<Element> children = root2.getChildren();
            for (Element child : children) {
                TableRow tableRow = new TableRow(child.getChildren(), pkColumnNames, header);
                chunk.put(tableRow.primaryKey, tableRow);
            }
            logger.log("gathered remote row {"+startValue+"-"+endValue+"} "+chunkCount.getAndIncrement()+"/"+tableChunkCount+"------------"+Thread.currentThread().toString()+"------------"+new Timestamp(new Date().getTime())+"------------"+run.availableProcessors()+"------"+run.freeMemory()/(1024*1024));
            return chunk;
        }

}

class TableRow {
        Map<String,Integer> primaryKey;
        Map<String, String> rowData;
        DataMap map;

        TableRow(ResultSet algyRow, ArrayList<String> pkColumnNames) throws SQLException{
            rowData = new TreeMap<>();
            primaryKey = new HashMap<>();
            ResultSetMetaData meta = algyRow.getMetaData();
            for(int q = 1;q<=meta.getColumnCount();q++){
                String columnName = meta.getColumnName(q);
                String entry = algyRow.getString(q);
                if(pkColumnNames.contains(columnName)){
                    primaryKey.put(columnName,algyRow.getInt(q));
                }
                if(!(entry == null)) {
                    if (meta.getColumnType(q) == -7) {
                        if (entry.equals("0")) {
                            entry = "false";
                        } else {
                            entry = "true";
                        }
                    } else if (meta.getColumnType(q) == 93) {
                        SimpleDateFormat isoDf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
                        SimpleDateFormat algyDf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.s");
                        try {
                            Date newDate = algyDf.parse(entry);
                            entry = isoDf.format(newDate);
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                    }
                }
                rowData.put(columnName,entry);
            }
        }

        TableRow(List<Element> children,ArrayList<String> pkColumnNames, Map<String, String> header) {
            rowData = new TreeMap<>();
            primaryKey = new HashMap<>();
            //iterate over all returned values and add them to the row
            for(Element child : children){
                String entry = child.getText();
                String columnName = child.getName();
                if (pkColumnNames.contains(columnName)) {
                    primaryKey.put(columnName,Integer.valueOf(entry));
                }
                rowData.put(columnName,entry);
            }
            //since null values are not reliably returned through http, check the child elements against the header and add nulls as needed
            for (String columnName : header.keySet()) {
                if(!rowData.containsKey(columnName)){
                    rowData.put(columnName,null);
                }
            }
        }

        TableRow(Map<String,Integer> primaryKey, Map<String,String> seedData){
            this.primaryKey = primaryKey;
            this.rowData = seedData;
        }

        TableRow(PrimaryKey primaryKey,Map<String,String> rowData){
            this.primaryKey = primaryKey.getKeyValueStrict();
            map = new DataMap();
            Map<String,String> cleanedData = new HashMap<>();
            for(String key : rowData.keySet()){
                String rawData = rowData.get(key);
                cleanedData.put(map.mapName(key),map.stnadardizeData(rawData));
            }
            this.rowData = cleanedData;
        }

    }