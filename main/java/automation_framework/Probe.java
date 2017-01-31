package automation_framework;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxBinary;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.firefox.FirefoxProfile;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.Select;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

class Probe {

    private static Properties remoteQueries;
    private static AlgyWhisperer algyWhisperer;
    private static EncryptedKeys encryptedKeys;
    private String password;

    Probe(String internalKey, String dbUsername, String dbPassword, String address, String dbName){
        List<String> userDefinedKeys = Arrays.asList("root_username","root_password","domain");
        encryptedKeys = new EncryptedKeys();
        password = internalKey;
        loadProperties();
        //load scripts
        algyWhisperer = new AlgyWhisperer(dbUsername,dbPassword,address,dbName);
        //create instance of remote database
        //confirm presence of key table, with username, password, domain
        try {
            //add missing tables to schema
            for(String tableName : algyWhisperer.tables.keySet()){
                investigateTable(tableName);
            }
            //add missing foreign keys to starter tables
            for(String fkName : algyWhisperer.fks.keySet()){
                investigateFK(fkName);
            }
            //add missing triggers to starter tables
            for(String triggerName : algyWhisperer.triggers.keySet()) {
                investigateTrigger(triggerName);
            }
            //add missing local keys to key table
            Scanner console = new Scanner(System.in);
            for(String userDefinedKeyName : userDefinedKeys) {
                investigateLocalKey(userDefinedKeyName,password,console);
            }
            console.close();
            //add missing remote keys to key table
            if(!probeLaunchPrecheck(remoteQueries.keySet()).isEmpty()) {
                ProbeWriter writer = new ProbeWriter();
                for (String remoteKeyName : probeLaunchPrecheck(remoteQueries.keySet())){
                    String remoteKey = writer.writeReport(remoteKeyName,(String)remoteQueries.get(remoteKeyName));
                    String[] encryptedKeyPackage = encryptedKeys.encryptString(remoteKey,password);
                    algyWhisperer.addLocalKey(remoteKeyName,remoteKeyName.replace("_"," "),encryptedKeyPackage[0],encryptedKeyPackage[1]);
                }
                writer.killProbeWriter();
            }
            //confirm that tables table has all remote tables present
            //confirm presence of synch history table
            investigateSynchTable();
            //confirm that remote database has queries listed for all synched tables
            Map<String,Map<String,Object>> unkeyedSynchedTables = algyWhisperer.pullUnkeyedSynchedTables();
            if(!unkeyedSynchedTables.isEmpty()) {
                ProbeWriter writer = new ProbeWriter();
                for (String tableName : unkeyedSynchedTables.keySet()) {
                    if (!investigateTablePuller(tableName)) {
                        String reportName = tableName+"_table_puller";
                        String sql = algyWhisperer.buildTablePuller(tableName);
                        String remoteKey = writer.writeReport(reportName,sql);
                        String[] encryptedKeyPackage = encryptedKeys.encryptString(remoteKey,password);
                        algyWhisperer.addRemoteKey(tableName,reportName,reportName.replace("_"," "),encryptedKeyPackage[0],encryptedKeyPackage[1]);
                    }
                    if(!investigateKeyPuller(tableName)){
                        String reportName = tableName+"_key_puller_key";
                        String sql = algyWhisperer.buildKeyPuller(tableName,(Boolean)unkeyedSynchedTables.get(tableName).get("is_change_tracked"));
                        String remoteKey = writer.writeReport(reportName,sql);
                        String[] encryptedKeyPackage = encryptedKeys.encryptString(remoteKey,password);
                        algyWhisperer.addRemoteKey(tableName,reportName,reportName.replace("_"," "),encryptedKeyPackage[0],encryptedKeyPackage[1]);
                    }
                }
                writer.killProbeWriter();
            }
        }catch(SQLException e){
            e.printStackTrace();
            throw new RuntimeException("can't connect to replication database, check connectivity and try again");
        }
    }

    private void loadProperties(){
        remoteQueries = new Properties();
        ClassLoader loader = this.getClass().getClassLoader();
        InputStream in = loader.getResourceAsStream("algy_remote.properties");
        try {
            remoteQueries.load(in);
            in.close();
        }catch(IOException e){
            throw new RuntimeException("can't locate the files in the rsc folder, verify their presence and try again");
        }
    }

    private synchronized void investigateFK(String fkName) throws SQLException{
        if(!algyWhisperer.sniffFk(fkName)){
            algyWhisperer.addFk(fkName);
        }
    }

    private synchronized void investigateTable(String tableName) throws SQLException{
        if(!algyWhisperer.sniffTable(tableName)) {
           algyWhisperer.addTable(tableName);
        }
    }

    private synchronized void investigateTrigger(String triggerName) throws SQLException{
        if(!algyWhisperer.sniffTrigger(triggerName)) {
            algyWhisperer.addTrigger(triggerName);
        }
    }

    private synchronized void investigateLocalKey(String keyName,String password, Scanner inputConsole) throws SQLException{
        if(!algyWhisperer.sniffKey(keyName)){
            String reportName = keyName.replace("_"," ");
            String verifiedText = getVerifiedText(reportName,inputConsole);
            String[] encryptedKeySet = encryptedKeys.encryptString(verifiedText,password);
            algyWhisperer.addLocalKey(keyName,reportName,encryptedKeySet[0],encryptedKeySet[1]);
        }
    }

    private synchronized String getVerifiedText(String fieldName, Scanner console){
        String finalText = null;
        Boolean match = false;
        do {
            System.out.println("please input the "+fieldName+" for your domain");
            String text1 = console.nextLine();
            System.out.println("confirm "+fieldName);
            String text2 = console.nextLine();
            if(text1.equals(text2)){
                finalText = text1;
                match = true;
            }else{
                System.out.println("fields do not match, try again");
            }
        }while(!match);
        return finalText;
    }

    //launching the probe writer is resource intensive, so check that there are reports missing first
    private synchronized List<String> probeLaunchPrecheck(Set<Object> reportNames) throws SQLException{
        List<String> output = new ArrayList<>();
        for(Object reportName : reportNames) {
            if(!algyWhisperer.sniffKey((String)reportName)){
                output.add((String)reportName);
            }
        }
        return output;
    }

    private synchronized void investigateSynchTable() throws SQLException{
        Integer batchCount = 0;
        String tableSnifferKey = algyWhisperer.getEncryptedKeyByName("table_sniffer",password,encryptedKeys);
        Set<String> remoteTableNames = new HashSet<>();
        Set<String> algyTableNames = algyWhisperer.pullReplicatedTableNames();
        try {
            Data remoteTableData = new Data(tableSnifferKey, "", "");
            remoteTableNames.addAll(remoteTableData.getMapData().keySet());
        }catch(Exception e){
            e.printStackTrace();
            throw new RuntimeException("can't connect to remote database, check connectivity and try again");
        }
        if(!remoteTableNames.equals(algyTableNames)){
            PreparedStatement insertBatch = algyWhisperer.startParameterizedBatch(algyWhisperer.queriesSql.get("log_new_synched_table"));
            Statement createBatch = algyWhisperer.startGeneralBatch();
            remoteTableNames.removeAll(algyTableNames);
            for(String tableName : remoteTableNames){
                Set<String> keyPull;
                String columnPullKey = algyWhisperer.getEncryptedKeyByName("column_sniffer",password,encryptedKeys);
                String keyPullKey = algyWhisperer.getEncryptedKeyByName("key_sniffer",password,encryptedKeys);
                try {
                    Map<String,Map<String,String>> columnPull = new Data(columnPullKey, tableName, "").getMapData();
                    try {
                        keyPull = new Data(keyPullKey, tableName, "").getMapData().keySet();
                    }catch(IndexOutOfBoundsException e){
                        keyPull = new HashSet<>();
                    }
                    algyWhisperer.buildRemoteTableLocally(insertBatch,createBatch,tableName,keyPull,columnPull);
                    if(batchCount++ >100){
                        algyWhisperer.fireBatch(insertBatch);
                        algyWhisperer.fireBatch(createBatch);
                        batchCount = 0;
                    }
                }catch(SQLException e){
                    e.printStackTrace();
                    throw new RuntimeException("can't connect to replication database, check connectivity and try again");
                }catch(Exception e){
                    e.printStackTrace();
                    throw new RuntimeException("can't connect to remote database, check connectivity and try again");
                }finally {
                    algyWhisperer.fireBatch(insertBatch);
                    algyWhisperer.fireBatch(createBatch);
                }
            }
        }
    }

    private synchronized Boolean investigateTablePuller(String tableName) throws SQLException{
        String tablePuller = tableName+"_table_puller";
        return algyWhisperer.sniffKey(tablePuller);
    }

    private synchronized Boolean investigateKeyPuller(String tableName) throws SQLException{
        String keyPuller = tableName+"_key_puller_key";
        return algyWhisperer.sniffKey(keyPuller);
    }

    private class ProbeWriter{
        private WebDriver driver;
        private Integer longWait = 120;
        private String url = "https://crediblebh.com";

        ProbeWriter(){
            ClassLoader loader = this.getClass().getClassLoader();
            InputStream in = loader.getResourceAsStream("config.properties");
            Properties properties = new Properties();
            try {
                properties.load(in);
            }catch(IOException e){
                throw new RuntimeException("can't find the config.properties file, check again");
            }
            String firefoxBinaryPath;
            String os = System.getProperty("os.name").toLowerCase();
            if(os.contains("win")) {
                firefoxBinaryPath = properties.getProperty("firefox_binary_path_windows");
            }else if(os.contains("nux")) {
                firefoxBinaryPath = properties.getProperty("firefox_binary_path_linux");
            }else{
                System.out.println(os);
                throw new RuntimeException("your operating system is not supported");
            }
            FirefoxBinary bin = new FirefoxBinary(new File(firefoxBinaryPath));
            driver = new FirefoxDriver(bin,new FirefoxProfile());
            try {
                knock();
            }catch(Exception e){
                e.printStackTrace();
            }
        }

        private void knock() throws Exception {
            driver.get(url);
            WebElement loginButton = new WebDriverWait(driver, longWait).until(ExpectedConditions.presenceOfElementLocated(By.name("btnLogin")));
            WebElement usernameField = new WebDriverWait(driver, longWait).until(ExpectedConditions.presenceOfElementLocated(By.name("UserName")));
            WebElement passwordField = new WebDriverWait(driver, longWait).until(ExpectedConditions.presenceOfElementLocated(By.name("Password")));
            WebElement domainField = new WebDriverWait(driver, longWait).until(ExpectedConditions.presenceOfElementLocated(By.name("DomainName")));
            usernameField.sendKeys(algyWhisperer.getEncryptedKeyByName("root_username",password,encryptedKeys));
            passwordField.sendKeys(algyWhisperer.getEncryptedKeyByName("root_password",password,encryptedKeys));
            domainField.sendKeys(algyWhisperer.getEncryptedKeyByName("domain",password,encryptedKeys));
            loginButton.click();
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
        }

        private String writeReport(String reportName, String sql) throws SQLException{
            Map<String,String> parameterMap = new HashMap<>();
            if(sql.contains("param")){
                for(Integer j = -1; (j=sql.indexOf("@",j+1)) != -1;){
                    String parameter = sql.substring(j+1,j+7);
                    String position = parameter.substring(parameter.length()-1);
                    parameterMap.put(parameter,position);
                }
            }
            String reportsTab = "/reports/export_list.asp";
            driver.get(url + reportsTab);
            WebElement reportCreateButton = new WebDriverWait(driver, longWait).until(ExpectedConditions.presenceOfElementLocated(By.name("new_export")));
            WebElement nameField = new WebDriverWait(driver, longWait).until(ExpectedConditions.presenceOfElementLocated(By.name("export_name")));
            nameField.sendKeys(reportName);
            WebElement typeDropdown = new WebDriverWait(driver, longWait).until(ExpectedConditions.presenceOfElementLocated(By.name("form_id")));
            Select mySelect= new Select(typeDropdown);
            mySelect.selectByValue("CUSTOMADHOC");
            reportCreateButton.click();
            WebElement textArea = new WebDriverWait(driver, longWait).until(ExpectedConditions.presenceOfElementLocated(By.name("custom_query")));
            textArea.sendKeys(sql);
            for(int q = 0;q<2;q++) {
                WebElement reportTypeDropdown = new WebDriverWait(driver, longWait).until(ExpectedConditions.presenceOfElementLocated(By.name("output_format")));
                Select formatSelect= new Select(reportTypeDropdown);
                formatSelect.selectByValue("WS");
                WebElement checkBox = new WebDriverWait(driver, longWait).until(ExpectedConditions.presenceOfElementLocated(By.xpath("//*[@id=\"treeroot\"]/input")));
                checkBox.click();
                if(!parameterMap.isEmpty()) {
                    for(String param : parameterMap.keySet()) {
                        WebElement paramField = new WebDriverWait(driver, longWait).until(ExpectedConditions.presenceOfElementLocated(By.name("custom_param" + parameterMap.get(param))));
                        paramField.sendKeys(param);
                    }
                }
                WebElement reportFinishButton = new WebDriverWait(driver, longWait).until(ExpectedConditions.presenceOfElementLocated(By.name("submit")));
                reportFinishButton.click();
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
            String newReportUrl = driver.getCurrentUrl();
            String reportNumber = newReportUrl.split("=")[1];
            driver.get("https://reports.crediblebh.com/reports/web_services_export.aspx?exportbuilder_id="+reportNumber+"&columnlist=Column%5FName%2CData%5Ftype");
            WebElement tableElement = driver.findElement(By.name("aspnetForm"));
            String innerHtml = tableElement.getAttribute("innerHTML");
            org.jsoup.nodes.Document doc2 = Jsoup.parse(innerHtml);
            Elements children = doc2.getAllElements();
            Element encryptedHeaderLine = children.get(12);
            String rawEncryptedLine = encryptedHeaderLine.toString();
            return rawEncryptedLine.split("<br>")[4];
        }

        private void killProbeWriter(){
            driver.close();
        }

    }
}