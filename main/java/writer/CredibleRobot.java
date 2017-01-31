package writer;

import algernon.*;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.htmlunit.HtmlUnitDriver;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import security.DatabaseCredentials;
import security.EncryptedKeys;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class CredibleRobot {
    private WebDriver driver;
    private String url = "https://crediblebh.com";
    private EncryptedKeys keys;

    public static void main(String[] args){
        String empId = args[1];
        String secretKey = args[0];
        DatabaseCredentials creds = new DatabaseCredentials(secretKey);
        CredibleRobot robot = new CredibleRobot(empId,creds);
    }

    CredibleRobot(String empId, DatabaseCredentials credentials){
        keys = new EncryptedKeys();
        System.setProperty("webdriver.chrome.driver","src\\main\\resources\\chromedriver.exe");
        driver = new ChromeDriver();
        //driver = new HtmlUnitDriver();
        AlgyRobotWhisperer robotWhisperer = new AlgyRobotWhisperer(credentials);
        AlgyWhisperer whisperer = new AlgyWhisperer(credentials);
        List<String> plannerIds;
        try {
            plannerIds = robotWhisperer.getPendingNotes(empId);
        }catch (SQLException e){
            throw new ReplicationDatabaseConnectionError();
        }
        knock(robotWhisperer);
        try {
            Thread.sleep(5000);
        }catch(InterruptedException e){
            e.printStackTrace();
        }
        for(String plannerId : plannerIds) {
            String visittempId = startService(plannerId,whisperer);
            String clientId = getClientId(plannerId, whisperer);
            NoteInformation information = new NoteInformation(plannerId,clientId, robotWhisperer, whisperer);
            populateService(visittempId, information);
        }

    }

    private void knock(AlgyRobotWhisperer robotWhisperer){
        driver.get(url);
        int longWait = 120;
        WebElement loginButton = new WebDriverWait(driver, longWait).until(ExpectedConditions.presenceOfElementLocated(By.name("btnLogin")));
        WebElement usernameField = new WebDriverWait(driver, longWait).until(ExpectedConditions.presenceOfElementLocated(By.name("UserName")));
        WebElement passwordField = new WebDriverWait(driver, longWait).until(ExpectedConditions.presenceOfElementLocated(By.name("Password")));
        WebElement domainField = new WebDriverWait(driver, longWait).until(ExpectedConditions.presenceOfElementLocated(By.name("DomainName")));
        try{
            usernameField.sendKeys(robotWhisperer.getEncryptedKeyByName("root_username",keys));
            passwordField.sendKeys(robotWhisperer.getEncryptedKeyByName("root_password",keys));
            domainField.sendKeys(robotWhisperer.getEncryptedKeyByName("domain",keys));
        }catch(SQLException e){
            throw new ReplicationDatabaseConnectionError();
        }
        loginButton.click();

    }

    private String startService(String plannerId,AlgyWhisperer whisperer) {
        driver.get(url + "/planner/plan.asp?plan_id=" + plannerId + "&noredirect=");
        WebElement startServiceButton = new WebDriverWait(driver, 120).until(ExpectedConditions.presenceOfElementLocated(By.xpath("/html/body/form/table/tbody/tr/td/table/tbody/tr[13]/td/input[2]")));
        startServiceButton.submit();
        return whisperer.getTempvisitId(plannerId);
    }

    private void populateService(String visittempId, NoteInformation information){
        String clientId = information.getClientId();
        driver.get(url+"/webforms/left.asp?visittemp_id="+visittempId+"&blnempform=0&blntemp=True&blnfromtx=0&incomplete=&client_id="+clientId+"&actualTimeFlag=&");
        String noteFields = "/webforms/category.asp?category_id=62859&visittemp_id=" + visittempId + "&blnempform=0&blntemp=True&blnfromtx=0&actualTimeFlag=&blneditfullvisit=";
        driver.get(url+noteFields);
        WebElement presentationCheckbox = new WebDriverWait(driver, 120).until(ExpectedConditions.presenceOfElementLocated(new By.ById("q_514015")));
        WebElement goalCheckbox = new WebDriverWait(driver, 120).until(ExpectedConditions.presenceOfElementLocated(new By.ById("q_514016")));
        WebElement objectiveCheckbox = new WebDriverWait(driver, 120).until(ExpectedConditions.presenceOfElementLocated(new By.ById("q_514017")));
        WebElement interventionCheckbox = new WebDriverWait(driver, 120).until(ExpectedConditions.presenceOfElementLocated(new By.ById("q_514018")));
        WebElement responseCheckbox = new WebDriverWait(driver, 120).until(ExpectedConditions.presenceOfElementLocated(new By.ById("q_514019")));
        WebElement presentationNotes = new WebDriverWait(driver, 120).until(ExpectedConditions.presenceOfElementLocated(new By.ById("qnotes_514015")));
        WebElement goalNotes = new WebDriverWait(driver, 120).until(ExpectedConditions.presenceOfElementLocated(new By.ById("qnotes_514016")));
        WebElement objectiveNotes = new WebDriverWait(driver, 120).until(ExpectedConditions.presenceOfElementLocated(new By.ById("qnotes_514017")));
        WebElement interventionNotes = new WebDriverWait(driver, 120).until(ExpectedConditions.presenceOfElementLocated(new By.ById("qnotes_514018")));
        WebElement responseNotes = new WebDriverWait(driver, 120).until(ExpectedConditions.presenceOfElementLocated(new By.ById("qnotes_514019")));
        WebElement submitBox = new WebDriverWait(driver, 120).until(ExpectedConditions.presenceOfElementLocated(new By.ByName("Complete")));

        while(!presentationNotes.isDisplayed()) {
            presentationCheckbox.click();
        }
        presentationNotes.clear();
        presentationNotes.sendKeys(information.getPresentation());
        while(!goalNotes.isDisplayed()) {
            goalCheckbox.click();
        }
        goalNotes.clear();
        goalNotes.sendKeys(information.getGoals());
        while(!objectiveNotes.isDisplayed()) {
            objectiveCheckbox.click();
        }
        objectiveNotes.clear();
        objectiveNotes.sendKeys(information.getObjectives());
        if(!interventionNotes.isDisplayed()) {
            interventionCheckbox.click();
        }
        interventionNotes.clear();
        interventionNotes.sendKeys(information.getInterventions());
        if(!responseNotes.isDisplayed()) {
            responseCheckbox.click();
        }
        responseNotes.clear();
        responseNotes.sendKeys(information.getResponse());
        submitBox.submit();
    }

    private String getClientId(String plannerId, AlgyWhisperer whisperer){
        String clientId = "";
        try {
            RemoteData clientIdPull = whisperer.getPlannerClientId(plannerId);
            if(clientIdPull.getData().size()>1){
                throw new RemoteDatabaseConnectionError("error while getting the client id from the planner id, planner id: "+plannerId);
            }
            for(Map<String,String> tableRow : clientIdPull.getData()){
                clientId = tableRow.get("client_id");
            }
        }catch(Exception e){
            throw new RemoteDatabaseConnectionError();
        }
        return clientId;
    }
}
