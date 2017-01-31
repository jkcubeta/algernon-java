package automation_framework;

import org.openqa.selenium.*;
import org.openqa.selenium.remote.UnreachableBrowserException;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.util.Map;
import java.util.concurrent.Callable;

class HtmlScrubWorker implements Callable<HtmlOutputRecord> {
    private MotorPool drivers;
    private int longWait = 60;
    private Map<String, String> inputSeed;

    HtmlScrubWorker(Map<String, String> inputSeed, MotorPool motorPool){
        this.drivers = motorPool;
        this.inputSeed = inputSeed;
    }

    @Override
    public HtmlOutputRecord call() {
        InputRecord inputRecord = new InputRecord(inputSeed);
        String credibleId = inputRecord.getField("credible_id");
        Robot.access.startHtmlScrubEntry(new ScrubEntry(credibleId));
        WebDriver driver = drivers.enlistDriver();
        HtmlOutputRecord outputRecord = verify(inputRecord,driver);
        drivers.returnDriver(driver);
        return outputRecord;
    }

    private synchronized HtmlOutputRecord verify(InputRecord inputRecord, WebDriver driver) {
        String credibleId = inputRecord.getField("credible_id");
        HtmlOutputRecord outputRecord;
        if (testMedicaidId(inputRecord.getField("medicaid_id"))) {
            try {
                String medicaidFieldXpath = "//*[@id=\"textfield\"]";
                String medicaidFieldSubmitXpath = "/html/body/div[3]/form/div[2]/div[3]/table[2]/tbody/tr/td[1]/input";
                String userFoundBannerXpath = "/html/body/div[3]/form/div[2]/div[2]/div/div/div";
                String userFound = "Eligibility Inquiry Result";
                WebElement medicaidField = new WebDriverWait(driver, longWait).until(ExpectedConditions.presenceOfElementLocated(By.xpath(medicaidFieldXpath)));
                medicaidField.sendKeys(inputRecord.getField("medicaid_id"));
                WebElement medicaidFieldSubmit = new WebDriverWait(driver, longWait).until(ExpectedConditions.presenceOfElementLocated(By.xpath(medicaidFieldSubmitXpath)));
                medicaidFieldSubmit.submit();
                WebElement userFoundBanner = new WebDriverWait(driver, longWait).until(ExpectedConditions.presenceOfElementLocated(By.xpath(userFoundBannerXpath)));
                if (userFound.equals(userFoundBanner.getText())) {
                    WebElement eligibilityTable = new WebDriverWait(driver, longWait).until(ExpectedConditions.presenceOfElementLocated(By.id("sandbox_content")));
                    String innerHtml = eligibilityTable.getAttribute("innerHTML");
                    outputRecord = new HtmlOutputRecord(credibleId,innerHtml);
                } else {
                    outputRecord = new HtmlOutputRecord(credibleId);
                }
            } catch (TimeoutException toe1) {
                throw new RuntimeException("driver timed out " + Thread.currentThread().getName());
            } catch (StaleElementReferenceException stre1) {
                throw new RuntimeException("stale reference in " + Thread.currentThread().getName());
            } catch (UnhandledAlertException uae1) {
                throw new RuntimeException("alert message appears in " + Thread.currentThread().getName());
            } catch (UnreachableBrowserException ure1) {
                throw new RuntimeException("browser has died " + Thread.currentThread().getName());
            }
        } else {
            outputRecord = new HtmlOutputRecord(credibleId);
        }
        return outputRecord;
    }

    private synchronized Boolean testMedicaidId(String medicaidNumber) {
        Boolean valid = true;
        Integer medicaidIdNumber = Integer.parseInt(medicaidNumber);
        if (medicaidIdNumber >= 99999999 || medicaidIdNumber <= 10000000) {
            valid = false;
        }
        return valid;
    }
}
