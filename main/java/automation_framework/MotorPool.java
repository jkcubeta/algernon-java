package automation_framework;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.htmlunit.HtmlUnitDriver;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedQueue;

class MotorPool
{
    private ConcurrentLinkedQueue<WebDriver> fleet;
    private Boolean driverUnavailable;
    private static String eligibilityURL = "https://www.dc-medicaid.com/dcwebportal/inquiry/eligiblityInquiry";


    MotorPool(Integer numOfDrivers){
        this.fleet = new ConcurrentLinkedQueue<>();
        for(Integer j = 0; j<numOfDrivers; j++){
            WebDriver driver;
            try {
                driver = new HtmlUnitDriver();
                //driver = new FirefoxDriver();
            }catch(Exception e) {
                e.printStackTrace();
                throw new RuntimeException("troubles with the drivers");
                //driver = new FirefoxDriver(bin, new FirefoxProfile());
            }
            try {
                knock(driver);
            }catch(Exception e){
                e.printStackTrace();
                throw new RuntimeException("can't reach remote server, check connection and try again");
            }
            fleet.add(driver);
        }
        this.driverUnavailable = false;
    }

    synchronized Boolean checkRoster(){
        return driverUnavailable;
    }

    synchronized WebDriver enlistDriver(){
        while(driverUnavailable){
            try{
                wait();
            }catch(InterruptedException e){
                e.printStackTrace();
                throw new RuntimeException("Interrupted while waiting for driver");
            }
        }
        notifyAll();
        return signOutDriver();
    }

    synchronized void returnDriver(WebDriver driver){
        driver.get(eligibilityURL);
        fleet.add(driver);
        driverUnavailable = false;
        notifyAll();

    }

    private synchronized WebDriver signOutDriver(){
        WebDriver signedDriver = fleet.poll();
        if(fleet.isEmpty()){
            driverUnavailable = true;
        }
        return signedDriver;
    }

    private void knock(WebDriver driver) throws Exception {
        Integer longWait = 120;
        String login = Robot.algyWhisperer.getEncryptedKeyByName("medicaid_username", Robot.internalKey, Robot.keys);
        String password = Robot.algyWhisperer.getEncryptedKeyByName("medicaid_password", Robot.internalKey, Robot.keys);
        String targetURL = "http://www.dc-medicaid.com/dcwebportal/home";
        String userNameFieldName = "j_username";
        String passwordFieldName = "j_password";
        String loginFieldName = "loginForm";
        try {
            driver.get(targetURL);
            WebElement userNameField = new WebDriverWait(driver, longWait).until(ExpectedConditions.presenceOfElementLocated(By.name(userNameFieldName)));
            WebElement passwordField = new WebDriverWait(driver, longWait).until(ExpectedConditions.presenceOfElementLocated(By.name(passwordFieldName)));
            WebElement loginField = new WebDriverWait(driver, longWait).until(ExpectedConditions.presenceOfElementLocated(By.name(loginFieldName)));
            userNameField.sendKeys(login);
            passwordField.sendKeys(password);
            loginField.submit();
            driver.get(eligibilityURL);
        } catch (NoSuchElementException nse1) {
            throw new RuntimeException("can't find webportal, check connection");
        }
    }
}
