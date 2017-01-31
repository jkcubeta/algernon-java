package automation_framework;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.openqa.selenium.*;
import org.openqa.selenium.remote.UnreachableBrowserException;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.util.Map;
import java.util.concurrent.Callable;

class ScrubWorker implements Callable<OutputRecord> {
    private MotorPool drivers;
    private int longWait = 60;
    private Map<String, String> inputSeed;

    ScrubWorker(Map<String, String> inputSeed, MotorPool motorPool){
        this.drivers = motorPool;
        this.inputSeed = inputSeed;
    }

    @Override
    public OutputRecord call() {
        InputRecord inputRecord = new InputRecord(inputSeed);
        String credibleId = inputRecord.getField("credible_id");
        Robot.access.startScrubEntry(new ScrubEntry(credibleId));
        WebDriver driver = drivers.enlistDriver();
        OutputRecord outputRecord = verify(inputRecord,driver);
        if (outputRecord != null) {
            if (compare(outputRecord)) {
                outputRecord = new OutputRecord(credibleId);
                //Robot.access.addRecord(outputRecord);
            }
        } else {
            outputRecord = new OutputRecord(credibleId);
        }
        Robot.access.finishScrubEntry(inputRecord.getField("credible_id"));
        drivers.returnDriver(driver);
        return outputRecord;
    }

    private synchronized OutputRecord verify(InputRecord inputRecord, WebDriver driver) {
        Integer q = 0;
        RecordPart recordPart = new RecordPart("all");
        String credibleId = inputRecord.getField("credible_id");
        recordPart.setCredibleId(credibleId);
        OutputRecord outputRecord = null;
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
                    Document doc2 = Jsoup.parse(innerHtml);
                    Elements tables = doc2.getElementsByTag("tbody");
                    Element demoTable = tables.get(1);
                    setDemographicData(demoTable, recordPart);
                    Element medicaidTable = tables.get(2);
                    String medicaidCheckLine = medicaidTable.child(1).child(0).text();
                    if (!medicaidCheckLine.contains("N/A")) {
                        setMedicaidData(medicaidTable, recordPart);
                        Element mcoTable = tables.get(4);
                        String mcoCheckLine = mcoTable.child(1).child(0).text();
                        if (!mcoCheckLine.contains("N/A")) {
                            setMCOData(mcoTable, recordPart);
                            q++;
                        }
                        q++;
                    }
                    Element medicareTable = tables.get(4 + q);
                    String medicareCheckLine = medicareTable.child(1).child(0).text();
                    if (!medicareCheckLine.contains("N/A")) {
                        setMedicareData(medicareTable, recordPart);
                        recordPart.setField("medicaid_check", null);
                        recordPart.setField("medicaid_start_date", null);
                        recordPart.setField("medicaid_end_date", null);
                    }
                    Element tplTable = tables.get(6 + q);
                    String tplCheckLine = tplTable.child(1).child(0).text();
                    if (!tplCheckLine.isEmpty()) {
                        setTplData(tplTable, recordPart);
                    }
                    outputRecord = new OutputRecord(recordPart);
                } else {
                    outputRecord = new OutputRecord(inputRecord.getField("credible_id"));
                }
            } catch (TimeoutException toe1) {
                System.out.print("driver timed out " + Thread.currentThread().getName());
            } catch (StaleElementReferenceException stre1) {
                System.out.println("stale reference in " + Thread.currentThread().getName());
            } catch (UnhandledAlertException uae1) {
                System.out.println("alert message appears in " + Thread.currentThread().getName());
            } catch (UnreachableBrowserException ure1) {
                System.out.println("browser has died " + Thread.currentThread().getName());
            }
        } else {
            outputRecord = new OutputRecord(inputRecord.getField("credible_id"));
        }
        return outputRecord;
    }

    private synchronized Boolean compare(OutputRecord newRecord) {
        Boolean equal = null;
        try {
            OutputRecord currentRecord = Robot.access.getRecord(newRecord.getCredibleId());
            equal = newRecord.equals(currentRecord);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return equal;
    }

    private synchronized void setDemographicData(Element demoTable, RecordPart recordPart) {
        String medicaidName = demoTable.child(1).child(1).text();
        String medicaidGender = demoTable.child(5).child(1).text();
        String medicaidDob = demoTable.child(6).child(1).text();
        recordPart.setField("medicaid_name", medicaidName);
        recordPart.setField("medicaid_gender", medicaidGender);
        recordPart.setField("medicaid_dob", medicaidDob);
    }

    private synchronized void setMedicaidData(Element medicaidTable, RecordPart recordPart) {
        String medicaidCheck = medicaidTable.child(3).child(1).text();
        String medicaidStartDate = medicaidTable.child(4).child(1).text();
        String medicaidEndDate = medicaidTable.child(5).child(1).text();
        recordPart.setField("medicaid_check", medicaidCheck);
        recordPart.setField("medicaid_start_date", medicaidStartDate);
        recordPart.setField("medicaid_end_date", medicaidEndDate);
        String qmbCode = medicaidTable.child(6).child(1).text();
        recordPart.setField("qmb_code", qmbCode);
    }

    private synchronized void setMedicareData(Element medicareTable, RecordPart recordPart) {
        String medicareCheck = "ACTIVE";
        String medicareStartDate = medicareTable.child(3).child(1).text();
        String medicareEndDate = medicareTable.child(4).child(1).text();
        recordPart.setField("medicare_check", medicareCheck);
        recordPart.setField("medicare_start_date", medicareStartDate);
        recordPart.setField("medicare_end_date", medicareEndDate);
    }

    private synchronized void setMCOData(Element mcoTable, RecordPart recordPart) {
        String mcoName = mcoTable.child(4).child(1).text();
        String mcoStartDate = mcoTable.child(2).child(1).text();
        String mcoEndDate = mcoTable.child(3).child(1).text();
        recordPart.setField("mco_name", mcoName);
        recordPart.setField("mco_start_date", mcoStartDate);
        recordPart.setField("mco_end_date", mcoEndDate);
    }

    private synchronized void setTplData(Element tplTable, RecordPart recordPart) {
        String tplName = tplTable.child(2).child(0).text();
        recordPart.setField("tpl_name", tplName);
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
